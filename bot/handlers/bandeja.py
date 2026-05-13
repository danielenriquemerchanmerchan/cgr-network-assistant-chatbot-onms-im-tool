"""
bot/handlers/bandeja.py
-----------------------
Handler del comando /bandeja.

Detecta el contexto:
    - Grupo (cuadrilla): muestra las OTs activas de esa cuadrilla.
    - Chat privado (coordinador): muestra las OTs del coord agrupadas
      en pendientes-de-asignar + asignadas-a-cuadrilla.
    - Cualquier otro caso: mensaje informativo.

El mismo comando, distinta respuesta segun quien lo escriba.
"""

import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from bot.services.cuadrillas import identificar_cuadrilla_por_chat_id
from bot.services.bandeja import obtener_ots_activas_cuadrilla
from bot.services.bandeja_coord import (
    identificar_coordinador_por_telegram_user_id,
    obtener_ots_del_coord,
)
from bot.services.ot_activa import obtener_ot_activa
from bot.services.interacciones import registrar_interaccion
from integrations.postgres.client import obtener_conexion, cerrar_conexion

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════

async def handle(update, context):
    chat_type = update.effective_chat.type
    logger.info(
        f"/bandeja recibido en chat {update.effective_chat.id} ({chat_type})"
    )

    if chat_type in ("group", "supergroup"):
        await _bandeja_cuadrilla(update, context)
    elif chat_type == "private":
        await _bandeja_coordinador(update, context)
    else:
        await update.message.reply_text(
            "ℹ️ El comando /bandeja solo funciona en grupos de cuadrilla "
            "o en chat privado con un coordinador."
        )


# ═══════════════════════════════════════════════════════════════════
# RAMA CUADRILLA — diseno con OT activa destacada + en espera
# ═══════════════════════════════════════════════════════════════════

async def _bandeja_cuadrilla(update, context):
    """
    Muestra la bandeja de la cuadrilla con el diseno de Mecanismo A:
        - Si hay una OT activa: la muestra arriba destacada (todos los
          campos) bajo el encabezado "✅ TRABAJANDO AHORA".
        - Las demas OTs ("en espera") salen abajo con info reducida en
          cursiva.
        - Si no hay OT activa todavia: aviso explicito + todas las OTs
          listadas como "en espera".

    Boton inline al final:
        - "🔄 Cambiar OT activa" si hay >=1 OT distinta a la activa.
        - No aparece si solo hay 1 OT y ya es la activa.
        - No aparece si la cuadrilla no tiene OTs.
    """
    chat_id    = update.effective_chat.id
    chat_title = update.effective_chat.title or ""
    user       = update.effective_user

    cuadrilla = identificar_cuadrilla_por_chat_id(chat_id)
    if cuadrilla is None:
        await update.message.reply_text(
            f"⚠️ Este grupo no esta registrado.\n`chat_id = {chat_id}`",
            parse_mode="Markdown",
        )
        return

    # Lista completa de OTs activas (en bandeja)
    ots = obtener_ots_activas_cuadrilla(cuadrilla["cuadrilla_id"])

    if not ots:
        await update.message.reply_text(
            f"ℹ️ La bandeja de *{cuadrilla['nombre']}* esta vacia.\n\n"
            f"No hay OTs activas asignadas en este momento.",
            parse_mode="Markdown",
        )
        # Registrar y salir
        registrar_interaccion(
            tipo_interaccion="mensaje_libre",
            direccion="entrante",
            actor_tipo="cuadrilla",
            actor_id=cuadrilla["cuadrilla_id"],
            cuadrilla_id=cuadrilla["cuadrilla_id"],
            telegram_chat_id=chat_id,
            telegram_chat_title=chat_title,
            telegram_message_id=update.message.message_id,
            telegram_user_id=user.id,
            telegram_username=user.username,
            contenido_texto="/bandeja",
            metadata={"comando": "/bandeja", "ots_listadas": 0},
        )
        logger.info(
            f"Bandeja consultada por {cuadrilla['cuadrilla_id']}: 0 OTs"
        )
        return

    # Necesitamos la OT activa actual para destacarla.
    conn = obtener_conexion()
    if conn is None:
        await update.message.reply_text(
            "❌ Error de conexion a BD. Intenta de nuevo en unos minutos."
        )
        return

    try:
        ot_activa = obtener_ot_activa(cuadrilla["cuadrilla_id"], conn)
    finally:
        cerrar_conexion(conn)

    activa_id = ot_activa["asignacion_id"] if ot_activa else None

    # Separar lista en activa vs en espera (preservando orden original
    # para las en espera).
    ots_en_espera = [ot for ot in ots if ot["asignacion_id"] != activa_id]
    # La activa la sacamos de la lista de candidatas a destacar SI
    # esta efectivamente en la bandeja (defensa: si por carrera
    # quedo apuntando a algo desactivado, ot_activa sera None).
    ot_destacada = None
    if ot_activa:
        ot_destacada = next(
            (ot for ot in ots if ot["asignacion_id"] == activa_id), None
        )

    # Armar texto
    lineas = []
    lineas.append(f"📋 *Bandeja de {cuadrilla['nombre']}*")
    lineas.append(f"_{len(ots)} OT(s) activa(s)_\n")

    if ot_destacada is not None:
        lineas.append("✅ *TRABAJANDO AHORA*")
        lineas.append(_formatear_ot_cuadrilla_destacada(ot_destacada))
    else:
        # No hay OT activa todavia (caso al inicio del dia, o la activa
        # quedo apuntando a algo que ya no esta)
        lineas.append("⚠️ *No has activado ninguna OT todavía*")
        lineas.append(
            "_Cuando actives una, los comandos como /midiendo, /cierre, "
            "etc., actuarán sobre ella._"
        )

    if ots_en_espera:
        lineas.append("\n───────────────────────")
        lineas.append("🔘 *En espera*\n")
        for ot in ots_en_espera:
            lineas.append(_formatear_ot_cuadrilla_en_espera(ot))
            lineas.append("")

    texto = "\n".join(lineas).rstrip()

    # Boton inline "Cambiar OT activa". Aparece si hay >=1 OT en espera
    # (es decir, hay algo distinto a la activa a lo que cambiarse).
    reply_markup = None
    if ots_en_espera:
        keyboard = [[
            InlineKeyboardButton(
                text="🔄 Cambiar OT activa",
                callback_data="cuad_cambiar_menu",
            )
        ]]
        reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        texto,
        parse_mode="Markdown",
        reply_markup=reply_markup,
    )

    registrar_interaccion(
        tipo_interaccion="mensaje_libre",
        direccion="entrante",
        actor_tipo="cuadrilla",
        actor_id=cuadrilla["cuadrilla_id"],
        cuadrilla_id=cuadrilla["cuadrilla_id"],
        telegram_chat_id=chat_id,
        telegram_chat_title=chat_title,
        telegram_message_id=update.message.message_id,
        telegram_user_id=user.id,
        telegram_username=user.username,
        contenido_texto="/bandeja",
        metadata={
            "comando": "/bandeja",
            "ots_listadas": len(ots),
            "tiene_ot_activa": ot_destacada is not None,
        },
    )

    logger.info(
        f"Bandeja consultada por {cuadrilla['cuadrilla_id']}: {len(ots)} OTs "
        f"(activa: {ot_destacada['wonum'] if ot_destacada else 'ninguna'})"
    )


# ═══════════════════════════════════════════════════════════════════
# RAMA COORD
# ═══════════════════════════════════════════════════════════════════

async def _bandeja_coordinador(update, context):
    user = update.effective_user
    chat_id = update.effective_chat.id

    conn = obtener_conexion()
    if conn is None:
        await update.message.reply_text(
            "❌ Error de conexion a BD. Intenta de nuevo en unos minutos."
        )
        return

    try:
        coord = identificar_coordinador_por_telegram_user_id(user.id, conn)
        if coord is None:
            await update.message.reply_text(
                "ℹ️ El comando /bandeja en chat privado solo funciona "
                "para coordinadores registrados."
            )
            return

        datos = obtener_ots_del_coord(coord["coordinador_id"], conn)
        texto = _formatear_bandeja_coord(coord, datos)
        reply_markup = _construir_botones_bandeja_coord(datos)

        await update.message.reply_text(
            texto, parse_mode="Markdown", reply_markup=reply_markup,
        )

        registrar_interaccion(
            tipo_interaccion="mensaje_libre",
            direccion="entrante",
            actor_tipo="coordinador",
            actor_id=coord["coordinador_id"],
            telegram_chat_id=chat_id,
            telegram_chat_title="",
            telegram_message_id=update.message.message_id,
            telegram_user_id=user.id,
            telegram_username=user.username,
            contenido_texto="/bandeja",
            metadata={
                "comando": "/bandeja",
                "rol": "coordinador",
                "ots_total": datos["total"],
                "pendientes": len(datos["pendientes_asignar"]),
                "asignadas": len(datos["asignadas_a_cuadrilla"]),
            },
        )

        logger.info(
            f"Bandeja consultada por coord {coord['coordinador_id']}: "
            f"{datos['total']} OTs"
        )

    finally:
        cerrar_conexion(conn)


# ═══════════════════════════════════════════════════════════════════
# HELPERS DE FORMATO
# ═══════════════════════════════════════════════════════════════════

def _norm(valor):
    if valor is None:
        return "—"
    if isinstance(valor, str) and valor.strip() == "":
        return "—"
    return valor


def _fmt_fecha(dt):
    if dt is None:
        return "—"
    try:
        return dt.strftime("%Y-%m-%d %H:%M")
    except AttributeError:
        return str(dt)


def _fmt_hace_cuanto(dt):
    if dt is None:
        return "—"
    try:
        from datetime import datetime, timezone
        ahora = datetime.now(timezone.utc) if dt.tzinfo else datetime.now()
        delta = ahora - dt
        segundos = int(delta.total_seconds())
        if segundos < 60:
            return "hace unos segundos"
        if segundos < 3600:
            return f"hace {segundos // 60} min"
        if segundos < 86400:
            return f"hace {segundos // 3600}h"
        return f"hace {segundos // 86400}d"
    except Exception:
        return _fmt_fecha(dt)


def _truncar_desc(texto, maximo=100):
    if texto is None or (isinstance(texto, str) and texto.strip() == ""):
        return "—"
    t = str(texto).strip()
    return (t[:maximo].rstrip() + "…") if len(t) > maximo else t


def _abreviar_departamento(depto):
    if depto is None or not isinstance(depto, str) or not depto.strip():
        return "—"
    palabras = depto.strip().upper().split()
    saltar = {"LA", "EL", "LOS", "LAS", "DE", "DEL"}
    for palabra in palabras:
        if palabra not in saltar:
            return palabra
    return palabras[0]


def _agrupar_ots_por_cuadrilla(ots):
    grupos = {}
    orden = []
    for ot in ots:
        nombre = ot.get("cuadrilla_nombre") or ot.get("cuadrilla_id")
        if not nombre:
            nombre = "Sin cuadrilla (?)"
        if nombre not in grupos:
            grupos[nombre] = []
            orden.append(nombre)
        grupos[nombre].append(ot)
    return [(nombre, grupos[nombre]) for nombre in orden]


def _formatear_ot_cuadrilla_destacada(ot):
    """
    Formato de la OT activa (TRABAJANDO AHORA) en /bandeja de cuadrilla.
    Info completa, sin cursiva, con el emoji 🔧.
    """
    marca_fallida = (
        "\n  🚩 *VISITA FALLIDA - pendiente CGR*"
        if ot.get("visita_fallida") else ""
    )
    urgencia = " 🚨" if ot.get("marcada_urgente_cgr") else ""
    nota_urgencia = (
        f"\n  ⚠️ {ot['nota_urgencia_cgr']}"
        if ot.get("nota_urgencia_cgr") else ""
    )
    return (
        f"🔧 *{_norm(ot['wonum'])}*{urgencia} · "
        f"{_norm(ot['worktype'])} · Sev {_norm(ot['severity'])} · "
        f"{_abreviar_departamento(ot['departamento'])}/{_norm(ot['ciudad'])}\n"
        f"  {_truncar_desc(ot['descripcion'])}\n"
        f"  🏢 {_norm(ot['operador_fo'])}\n"
        f"  Estado: _{_norm(ot['estado_descripcion'])}_ / "
        f"Fase: _{_norm(ot['fase_descripcion'])}_"
        f"{marca_fallida}"
        f"{nota_urgencia}"
    )


def _formatear_ot_cuadrilla_en_espera(ot):
    """
    Formato de OT en espera en /bandeja de cuadrilla.
    Info reducida, todo en cursiva, con bullet 🔹.
    """
    marca_fallida = (
        "\n   🚩 *VISITA FALLIDA - pendiente CGR*"
        if ot.get("visita_fallida") else ""
    )
    return (
        f"🔹 _{_norm(ot['wonum'])} · "
        f"{_norm(ot['worktype'])} · Sev {_norm(ot['severity'])} · "
        f"{_abreviar_departamento(ot['departamento'])}/{_norm(ot['ciudad'])}_\n"
        f"   _{_truncar_desc(ot['descripcion'])}_"
        f"{marca_fallida}"
    )


def _formatear_ot_pendiente(ot):
    return (
        f"• `{_norm(ot['wonum'])}` · {_norm(ot['worktype'])} · "
        f"Sev {_norm(ot['severity'])}\n"
        f"  📍 {_norm(ot['departamento'])} · {_norm(ot['ciudad'])}\n"
        f"  📝 {_truncar_desc(ot['description'])}\n"
        f"  🏢 {_norm(ot['operador_fo'])}\n"
        f"  └─ Acusada {_fmt_hace_cuanto(ot['notificacion_coordinador_recibida_at'])}, "
        f"sin cuadrilla aun"
    )


def _formatear_ot_asignada(ot):
    marca_fallida = (
        "\n  🚩 *VISITA FALLIDA - pendiente CGR*"
        if ot.get("visita_fallida") else ""
    )
    return (
        f"• `{_norm(ot['wonum'])}` · {_norm(ot['worktype'])} · "
        f"Sev {_norm(ot['severity'])} · "
        f"{_abreviar_departamento(ot['departamento'])}/{_norm(ot['ciudad'])}\n"
        f"  {_truncar_desc(ot['description'])}\n"
        f"  🏢 {_norm(ot['operador_fo'])}\n"
        f"  Estado: _{_norm(ot['estado'])}_ / "
        f"Fase: _{_norm(ot['fase_operativa'])}_"
        f"{marca_fallida}"
    )


def _formatear_bandeja_coord(coord, datos):
    pendientes = datos["pendientes_asignar"]
    asignadas  = datos["asignadas_a_cuadrilla"]

    lineas = []
    lineas.append(f"📋 *Bandeja del Coordinador {coord['nombre_completo']}*\n")

    if datos["total"] == 0:
        lineas.append("_Tu bandeja esta vacia. No tienes OTs activas en este momento._")
        return "\n".join(lineas)

    if pendientes:
        lineas.append(f"⚠️ *{len(pendientes)} OT(s) PENDIENTE(S) DE ASIGNAR*")
        lineas.append("─────────────────────────────────")
        for ot in pendientes:
            lineas.append(_formatear_ot_pendiente(ot))
            lineas.append("")

    if asignadas:
        lineas.append(f"📌 *{len(asignadas)} OT(s) asignadas a cuadrillas*")
        lineas.append("─────────────────────────────────")

        grupos = _agrupar_ots_por_cuadrilla(asignadas)
        for idx_grupo, (nombre_cuadrilla, ots_grupo) in enumerate(grupos):
            if idx_grupo > 0:
                lineas.append("─────────────────────────────────")
            n = len(ots_grupo)
            sufijo = "OTs" if n != 1 else "OT"
            lineas.append(f"🛠 *{nombre_cuadrilla}* ({n} {sufijo})")
            for ot in ots_grupo:
                lineas.append(_formatear_ot_asignada(ot))
                lineas.append("")

    return "\n".join(lineas)


def _construir_botones_bandeja_coord(datos):
    pendientes = datos.get("pendientes_asignar", [])
    asignadas  = datos.get("asignadas_a_cuadrilla", [])

    if not pendientes and not asignadas:
        return None

    keyboard = []
    for ot in pendientes:
        keyboard.append([
            InlineKeyboardButton(
                text=f"🔧 Asignar {ot['wonum']}",
                callback_data=f"asig_iniciar|{ot['asignacion_id']}",
            )
        ])

    if asignadas:
        keyboard.append([
            InlineKeyboardButton(
                text="🔄 Reasignar OT...",
                callback_data="reasig_menu",
            )
        ])

    return InlineKeyboardMarkup(keyboard)