"""
bot/handlers/cuad_activar.py
----------------------------
Handler de los callbacks de "cambiar OT activa" en la bandeja de
cuadrilla.

CALLBACKS QUE MANEJA:
    cuad_cambiar_menu
        La cuadrilla toco [🔄 Cambiar OT activa] en /bandeja.
        Muestra el menu de seleccion con los botones 1️⃣ 2️⃣ ... ⬅️ Cancelar.

    cuad_activar|<asignacion_id>
        La cuadrilla toco un boton del menu para activar una OT.
        Hace UPDATE en cuadrillas.ot_activa_id, registra interaccion,
        edita el mensaje del menu a una confirmacion.

NO HACE (todavia):
    - No pinea ni despinea mensajes en Telegram. Eso es del Bloque 4
      (Mecanismo B), que se agregara despues a la funcion
      _ejecutar_activacion.

REGLAS DE SEGURIDAD:
    - Las dos funciones validan que quien presiona el boton este en
      el grupo registrado de una cuadrilla. Si el chat no esta
      registrado, se ignora la accion.
    - El callback cuad_activar valida que la asignacion_id pertenezca
      a una OT activa de esa cuadrilla. Esto evita que alguien
      manipule el callback_data para activar OTs de otra cuadrilla.
"""

import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from bot.services.cuadrillas import identificar_cuadrilla_por_chat_id
from bot.services.bandeja import obtener_ots_activas_cuadrilla
from bot.services.ot_activa import (
    activar_ot,
    obtener_ot_activa,
    actualizar_datos_pin,
    obtener_datos_pin_anterior,
)
from bot.services.interacciones import registrar_interaccion
from integrations.postgres.client import obtener_conexion, cerrar_conexion
from core.config import PINEAR_OT_ACTIVA

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# Emojis numericos para botones 1..9
# ═══════════════════════════════════════════════════════════════════
EMOJI_NUMEROS = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣"]


def _emoji_numero(i):
    """
    Devuelve el emoji para la posicion i (1-indexed). Si pasa de 9,
    devuelve un texto entre parentesis '(10)', '(11)'... Esto solo
    aplicaria si una cuadrilla tiene 10+ OTs simultaneas (improbable).
    """
    if 1 <= i <= 9:
        return EMOJI_NUMEROS[i - 1]
    return f"({i})"


def _abreviar_departamento(depto):
    """Misma logica que en bandeja.py: 'VALLE DEL CAUCA' -> 'VALLE'."""
    if depto is None or not isinstance(depto, str) or not depto.strip():
        return "—"
    palabras = depto.strip().upper().split()
    saltar = {"LA", "EL", "LOS", "LAS", "DE", "DEL"}
    for palabra in palabras:
        if palabra not in saltar:
            return palabra
    return palabras[0]


def _norm(valor):
    if valor is None:
        return "—"
    if isinstance(valor, str) and valor.strip() == "":
        return "—"
    return valor


def _truncar(texto, maximo=100):
    if texto is None or (isinstance(texto, str) and texto.strip() == ""):
        return "—"
    t = str(texto).strip()
    return (t[:maximo].rstrip() + "…") if len(t) > maximo else t


# ═══════════════════════════════════════════════════════════════════
# RAMA 1 — Mostrar menu de seleccion
# ═══════════════════════════════════════════════════════════════════

async def mostrar_menu_cambiar_ot(query, update, context):
    """
    callback_data: cuad_cambiar_menu

    Construye el menu con la lista de OTs disponibles + botones de
    activacion. Edita el mensaje original (el de /bandeja) para
    mostrar el menu.

    Si despues de ver la bandeja la cuadrilla ya solo tiene 1 OT no
    activa (raro pero posible), igual mostramos el menu con esa
    sola opcion. La consistencia es mejor que tratar casos especiales.
    """
    chat_id = query.message.chat.id

    cuadrilla = identificar_cuadrilla_por_chat_id(chat_id)
    if cuadrilla is None:
        await query.answer(
            text="Este grupo no esta registrado.",
            show_alert=True,
        )
        return

    # Traer todas las OTs activas de la cuadrilla
    ots = obtener_ots_activas_cuadrilla(cuadrilla["cuadrilla_id"])

    if not ots:
        await query.edit_message_text(
            text=(
                "ℹ️ Ya no tienes OTs en bandeja para activar.\n\n"
                "Escribe /bandeja para refrescar."
            )
        )
        return

    # Necesitamos saber cual es la actualmente activa para excluirla
    # de las opciones de "activar" (a la activa no se le puede
    # "activar de nuevo", no tiene sentido).
    conn = obtener_conexion()
    if conn is None:
        await query.answer(
            text="Error de conexion a BD.",
            show_alert=True,
        )
        return

    try:
        ot_activa_actual = obtener_ot_activa(cuadrilla["cuadrilla_id"], conn)
        activa_asig_id = ot_activa_actual["asignacion_id"] if ot_activa_actual else None
    finally:
        cerrar_conexion(conn)

    # Filtrar las que se pueden activar (todas menos la actual)
    candidatas = [ot for ot in ots if ot["asignacion_id"] != activa_asig_id]

    if not candidatas:
        # No hay ninguna otra opcion. Esto solo pasaria si la cuadrilla
        # toco el boton pero entre /bandeja y el toque, alguna OT
        # desaparecio del listado (carrera). Mensaje claro y se acabo.
        await query.edit_message_text(
            text=(
                "ℹ️ No hay otras OTs disponibles para activar en este "
                "momento.\n\nEscribe /bandeja para refrescar."
            )
        )
        return

    # Construir el texto del menu
    lineas = []
    lineas.append("🔄 *¿Cuál OT quieres activar?*\n")

    if ot_activa_actual:
        lineas.append(f"_Actualmente activa: {ot_activa_actual['wonum']}_\n")
    else:
        lineas.append("_No tienes OT activa todavía._\n")

    for i, ot in enumerate(candidatas, start=1):
        emoji = _emoji_numero(i)
        marca_fallida = (
            "\n   🚩 *VISITA FALLIDA - pendiente CGR*"
            if ot.get("visita_fallida") else ""
        )
        lineas.append(
            f"{emoji} *{_norm(ot['wonum'])}* — "
            f"{_abreviar_departamento(ot['departamento'])}/{_norm(ot['ciudad'])}\n"
            f"   {_truncar(ot['descripcion'])}"
            f"{marca_fallida}"
        )
        lineas.append("")  # separador entre opciones

    texto = "\n".join(lineas).rstrip()

    # Construir los botones (opcion A: uno por fila, cancelar al final)
    keyboard = []
    for i, ot in enumerate(candidatas, start=1):
        emoji = _emoji_numero(i)
        keyboard.append([
            InlineKeyboardButton(
                text=f"{emoji}  Activar {ot['wonum']}",
                callback_data=f"cuad_activar|{ot['asignacion_id']}",
            )
        ])
    keyboard.append([
        InlineKeyboardButton(
            text="⬅️  Cancelar",
            callback_data="cancelar_operacion",
        )
    ])

    await query.edit_message_text(
        text=texto,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


# ═══════════════════════════════════════════════════════════════════
# HELPER — Gestionar pin del mensaje de OT activa en Telegram
# ═══════════════════════════════════════════════════════════════════

async def _gestionar_pin_ot_activa(context, chat_id, cuadrilla_id, ot, conn):
    """
    Mecanismo B: Despinea el pin anterior (si lo habia) y pinea un
    mensaje nuevo con los datos de la OT activa en el grupo.

    Si la config PINEAR_OT_ACTIVA esta en False, no hace nada.

    Errores en operaciones de Telegram (sin permisos, mensaje viejo
    inalcanzable, etc.) se loguean pero NO interrumpen el flujo: la
    activacion en BD ya quedo bien y eso es lo importante.

    Despues de pinear exitosamente, guarda en BD el message_id y el
    chat_id del nuevo pin para poder despinearlo la proxima vez.
    """
    if not PINEAR_OT_ACTIVA:
        logger.info(
            f"[PinOTActiva] PINEAR_OT_ACTIVA=False, saltando pin para "
            f"cuadrilla {cuadrilla_id}."
        )
        return

    # 1) Despinear el anterior si lo habia
    pin_anterior = obtener_datos_pin_anterior(cuadrilla_id, conn)
    if pin_anterior:
        try:
            await context.bot.unpin_chat_message(
                chat_id=pin_anterior["pin_message_chat_id"],
                message_id=pin_anterior["pin_message_id"],
            )
            logger.info(
                f"[PinOTActiva] Pin anterior despineado "
                f"(message_id={pin_anterior['pin_message_id']}) "
                f"en chat {pin_anterior['pin_message_chat_id']}."
            )
        except Exception as e:
            # Casos comunes: mensaje borrado, sin permisos, mensaje > 48h.
            # No es bloqueante: lo logueamos y seguimos.
            logger.info(
                f"[PinOTActiva] No se pudo despinear el pin anterior "
                f"(message_id={pin_anterior['pin_message_id']}): {e}"
            )

    # 2) Construir mensaje nuevo y enviarlo al grupo
    marca_fallida = (
        "\n🚩 *VISITA FALLIDA - pendiente CGR*"
        if ot.get("visita_fallida") else ""
    )
    texto_pin = (
        f"📌 *Trabajando: {_norm(ot['wonum'])}* · "
        f"{_norm(ot['worktype'])} · Sev {_norm(ot['severity'])} · "
        f"{_abreviar_departamento(ot['departamento'])}/{_norm(ot['ciudad'])}\n"
        f"{_truncar(ot['descripcion'], 200)}\n"
        f"🏢 {_norm(ot['operador_fo'])}"
        f"{marca_fallida}"
    )

    try:
        mensaje_pin = await context.bot.send_message(
            chat_id=chat_id,
            text=texto_pin,
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.warning(
            f"[PinOTActiva] No se pudo enviar el mensaje a pinear "
            f"en chat {chat_id}: {e}"
        )
        # Limpiar referencias viejas para no quedar con pin colgado
        actualizar_datos_pin(cuadrilla_id, None, None, conn)
        return

    # 3) Pinear el mensaje recien enviado
    try:
        await context.bot.pin_chat_message(
            chat_id=chat_id,
            message_id=mensaje_pin.message_id,
            disable_notification=True,
        )
        logger.info(
            f"[PinOTActiva] Pin nuevo creado en chat {chat_id} "
            f"(message_id={mensaje_pin.message_id}) para "
            f"OT {ot['wonum']}."
        )
    except Exception as e:
        # Sin permisos de pin o similar: el mensaje quedo enviado pero
        # no pineado. Limpiamos referencias en BD para no intentar
        # despinearlo despues (porque no esta pineado).
        logger.warning(
            f"[PinOTActiva] No se pudo pinear el mensaje "
            f"(message_id={mensaje_pin.message_id}) en chat {chat_id}: {e}"
        )
        actualizar_datos_pin(cuadrilla_id, None, None, conn)
        return

    # 4) Guardar referencia del nuevo pin en BD
    actualizar_datos_pin(
        cuadrilla_id=cuadrilla_id,
        pin_message_id=mensaje_pin.message_id,
        pin_message_chat_id=chat_id,
        conn=conn,
    )


# ═══════════════════════════════════════════════════════════════════
# RAMA 2 — Ejecutar la activacion de la OT seleccionada
# ═══════════════════════════════════════════════════════════════════

async def ejecutar_activacion(query, partes, update, context):
    """
    callback_data: cuad_activar|<asignacion_id>

    Activa la OT en la cuadrilla (UPDATE de ot_activa_id), registra
    la interaccion, y edita el mensaje del menu con la confirmacion.

    Validaciones de seguridad:
        - El chat debe estar registrado como grupo de una cuadrilla.
        - La asignacion_id debe pertenecer a una OT en bandeja activa
          de esa cuadrilla (evita manipulacion del callback_data).
    """
    _, asignacion_id = partes
    try:
        asignacion_id = int(asignacion_id)
    except (TypeError, ValueError):
        await query.answer(text="Callback invalido.", show_alert=True)
        return

    chat_id = query.message.chat.id
    chat_title = query.message.chat.title or ""
    user = update.effective_user

    cuadrilla = identificar_cuadrilla_por_chat_id(chat_id)
    if cuadrilla is None:
        await query.answer(
            text="Este grupo no esta registrado.",
            show_alert=True,
        )
        return

    # Validar que la asignacion pertenezca a una OT activa de esta
    # cuadrilla (defensa contra manipulacion del callback_data).
    ots = obtener_ots_activas_cuadrilla(cuadrilla["cuadrilla_id"])
    ot_target = next(
        (o for o in ots if o["asignacion_id"] == asignacion_id),
        None,
    )
    if ot_target is None:
        await query.edit_message_text(
            text=(
                "⚠️ Esa OT ya no esta en tu bandeja o no pertenece a tu "
                "cuadrilla.\n\nEscribe /bandeja para refrescar."
            ),
        )
        return

    # Ejecutar el UPDATE
    conn = obtener_conexion()
    if conn is None:
        await query.answer(
            text="Error de conexion a BD.",
            show_alert=True,
        )
        return

    try:
        ok = activar_ot(
            cuadrilla_id=cuadrilla["cuadrilla_id"],
            asignacion_id=asignacion_id,
            conn=conn,
        )

        if not ok:
            conn.rollback()
            await query.edit_message_text(
                text="❌ No se pudo activar la OT. Intenta de nuevo."
            )
            return

        # Registrar interaccion
        registrar_interaccion(
            tipo_interaccion="cuadrilla_activa_ot",
            direccion="entrante",
            actor_tipo="cuadrilla",
            actor_id=cuadrilla["cuadrilla_id"],
            cuadrilla_id=cuadrilla["cuadrilla_id"],
            wonum=ot_target["wonum"],
            asignacion_id=asignacion_id,
            telegram_chat_id=chat_id,
            telegram_chat_title=chat_title,
            telegram_message_id=query.message.message_id,
            telegram_user_id=user.id,
            telegram_username=user.username,
            contenido_texto=f"Activo OT {ot_target['wonum']}",
            metadata={
                "accion": "activar_ot",
                "asignacion_id": asignacion_id,
                "wonum": ot_target["wonum"],
            },
        )

        conn.commit()

        # Mecanismo B: gestionar pin en Telegram (despinear anterior,
        # pinear nuevo, guardar message_id). Es best-effort: errores
        # se loguean pero no interrumpen el flujo.
        try:
            await _gestionar_pin_ot_activa(
                context=context,
                chat_id=chat_id,
                cuadrilla_id=cuadrilla["cuadrilla_id"],
                ot=ot_target,
                conn=conn,
            )
            conn.commit()
        except Exception as e:
            logger.warning(
                f"[CuadActivar] Falla inesperada en gestion de pin "
                f"para cuadrilla {cuadrilla['cuadrilla_id']}: {e}",
                exc_info=True,
            )

        # Editar el mensaje del menu a una confirmacion
        marca_fallida = (
            "\n  🚩 *VISITA FALLIDA - pendiente CGR*"
            if ot_target.get("visita_fallida") else ""
        )
        texto_confirm = (
            f"✅ *OT {ot_target['wonum']} activada*\n\n"
            f"🔧 *{_norm(ot_target['wonum'])}* · "
            f"{_norm(ot_target['worktype'])} · "
            f"Sev {_norm(ot_target['severity'])} · "
            f"{_abreviar_departamento(ot_target['departamento'])}/"
            f"{_norm(ot_target['ciudad'])}\n"
            f"  {_truncar(ot_target['descripcion'])}\n"
            f"  🏢 {_norm(ot_target['operador_fo'])}\n"
            f"  Estado: _{_norm(ot_target['estado_descripcion'])}_ / "
            f"Fase: _{_norm(ot_target['fase_descripcion'])}_"
            f"{marca_fallida}\n\n"
            f"_Los comandos siguientes actuarán sobre esta OT._"
        )
        try:
            await query.edit_message_text(
                text=texto_confirm,
                parse_mode="Markdown",
            )
        except Exception as e:
            logger.warning(
                f"[CuadActivar] No se pudo editar el mensaje del menu: {e}"
            )

        logger.info(
            f"[CuadActivar] Cuadrilla {cuadrilla['cuadrilla_id']} activo "
            f"OT {ot_target['wonum']} (asig={asignacion_id})"
        )

    except Exception as e:
        conn.rollback()
        logger.error(
            f"[CuadActivar] Error activando asig={asignacion_id} "
            f"para cuadrilla {cuadrilla['cuadrilla_id']}: {e}",
            exc_info=True,
        )
        try:
            await query.edit_message_text(
                text="❌ Error al activar la OT. Revisa los logs."
            )
        except Exception:
            pass
    finally:
        cerrar_conexion(conn)