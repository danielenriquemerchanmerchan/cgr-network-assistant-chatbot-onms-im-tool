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
from bot.services.interacciones import registrar_interaccion
from integrations.postgres.client import obtener_conexion, cerrar_conexion

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# ENTRY POINT — router por tipo de chat
# ═══════════════════════════════════════════════════════════════════

async def handle(update, context):
    """
    Punto de entrada del comando /bandeja.

    Detecta el tipo de chat y delega:
        - group/supergroup -> bandeja de cuadrilla (logica original)
        - private          -> bandeja de coordinador (logica nueva)
        - otro             -> mensaje informativo
    """
    chat_type = update.effective_chat.type

    logger.info(
        f"/bandeja recibido en chat {update.effective_chat.id} "
        f"({chat_type})"
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
# RAMA 1 — BANDEJA DE CUADRILLA (logica original, sin cambios)
# ═══════════════════════════════════════════════════════════════════

async def _bandeja_cuadrilla(update, context):
    chat_id    = update.effective_chat.id
    chat_title = update.effective_chat.title or ""
    user       = update.effective_user

    # Identificar la cuadrilla
    cuadrilla = identificar_cuadrilla_por_chat_id(chat_id)
    if cuadrilla is None:
        await update.message.reply_text(
            f"⚠️ Este grupo no esta registrado.\n"
            f"`chat_id = {chat_id}`",
            parse_mode="Markdown",
        )
        return

    # Consultar las OTs activas
    ots = obtener_ots_activas_cuadrilla(cuadrilla["cuadrilla_id"])

    if not ots:
        await update.message.reply_text(
            f"ℹ️ La bandeja de *{cuadrilla['nombre']}* esta vacia.\n\n"
            f"No hay OTs activas asignadas en este momento.",
            parse_mode="Markdown",
        )
    else:
        # Construir respuesta legible
        lineas = [f"📋 *Bandeja de {cuadrilla['nombre']}*"]
        lineas.append(f"_Total: {len(ots)} OT(s) activa(s)_\n")

        for i, ot in enumerate(ots, start=1):
            urgencia = " 🚨" if ot["marcada_urgente_cgr"] else ""
            marca_fallida = (
                "\n  🚩 *VISITA FALLIDA - pendiente CGR*"
                if ot.get("visita_fallida") else ""
            )
            lineas.append(
                f"*{i}. OT {ot['wonum']}*{urgencia}\n"
                f"  📍 {ot['descripcion'] or '(sin descripcion)'}\n"
                f"  🔧 Tipo: {ot['tipo_tramo'] or 'N/D'}\n"
                f"  📊 Estado: _{ot['estado_descripcion']}_\n"
                f"  ⚙️ Fase: _{ot['fase_descripcion']}_"
                f"{marca_fallida}"
            )
            if ot["nota_urgencia_cgr"]:
                lineas.append(f"  ⚠️ {ot['nota_urgencia_cgr']}")
            lineas.append("")

        texto = "\n".join(lineas)
        await update.message.reply_text(texto, parse_mode="Markdown")

    # Registrar la consulta de bandeja en cualquier caso
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
        },
    )

    logger.info(
        f"Bandeja consultada por {cuadrilla['cuadrilla_id']}: "
        f"{len(ots)} OTs"
    )


# ═══════════════════════════════════════════════════════════════════
# RAMA 2 — BANDEJA DE COORDINADOR (nueva)
# ═══════════════════════════════════════════════════════════════════

async def _bandeja_coordinador(update, context):
    """
    Muestra la bandeja del coordinador. Solo lectura (sin botones).

    Formato:
        📋 Bandeja del Coordinador {nombre}

        ⚠️ N OT(s) PENDIENTE(S) DE ASIGNAR
        ─────────────────────────────────
        [detalle de cada una]

        📌 M OT(s) asignadas a cuadrillas
        ─────────────────────────────────
        [detalle de cada una]
    """
    user = update.effective_user
    chat_id = update.effective_chat.id

    conn = obtener_conexion()
    if conn is None:
        await update.message.reply_text(
            "❌ Error de conexion a BD. Intenta de nuevo en unos minutos."
        )
        return

    try:
        # 1) Identificar si el user_id es un coord registrado
        coord = identificar_coordinador_por_telegram_user_id(user.id, conn)
        if coord is None:
            await update.message.reply_text(
                "ℹ️ El comando /bandeja en chat privado solo funciona "
                "para coordinadores registrados.\n\n"
                "Si eres una cuadrilla, escribe /bandeja en el grupo de "
                "tu cuadrilla."
            )
            return

        # 2) Traer las OTs activas del coord
        datos = obtener_ots_del_coord(coord["coordinador_id"], conn)

        # 3) Formatear texto + construir botones (asignar pendientes + reasignar)
        texto = _formatear_bandeja_coord(coord, datos)
        reply_markup = _construir_botones_bandeja_coord(datos)

        await update.message.reply_text(
            texto,
            parse_mode="Markdown",
            reply_markup=reply_markup,
        )

        # 4) Registrar la consulta
        registrar_interaccion(
            tipo_interaccion="mensaje_libre",
            direccion="entrante",
            actor_tipo="coordinador",
            actor_id=coord["coordinador_id"],
            telegram_chat_id=chat_id,
            telegram_chat_title="",  # chat privado, sin titulo
            telegram_message_id=update.message.message_id,
            telegram_user_id=user.id,
            telegram_username=user.username,
            contenido_texto="/bandeja",
            metadata={
                "comando": "/bandeja",
                "rol": "coordinador",
                "ots_total":      datos["total"],
                "pendientes":     len(datos["pendientes_asignar"]),
                "asignadas":      len(datos["asignadas_a_cuadrilla"]),
            },
        )

        logger.info(
            f"Bandeja consultada por coord {coord['coordinador_id']}: "
            f"{datos['total']} OTs "
            f"({len(datos['pendientes_asignar'])} pendientes, "
            f"{len(datos['asignadas_a_cuadrilla'])} asignadas)"
        )

    finally:
        cerrar_conexion(conn)


# ═══════════════════════════════════════════════════════════════════
# FORMATEO DEL TEXTO PARA LA BANDEJA DEL COORD
# ═══════════════════════════════════════════════════════════════════

def _norm(valor):
    """Reemplaza NULL/vacio por '—'."""
    if valor is None:
        return "—"
    if isinstance(valor, str) and valor.strip() == "":
        return "—"
    return valor


def _fmt_fecha(dt):
    """Formatea timestamp a 'YYYY-MM-DD HH:MM' o devuelve '—'."""
    if dt is None:
        return "—"
    try:
        return dt.strftime("%Y-%m-%d %H:%M")
    except AttributeError:
        return str(dt)


def _fmt_hace_cuanto(dt):
    """Devuelve algo legible tipo 'hace 15 min', 'hace 2h', 'hace 1d'.
    Si dt es None, devuelve '—'."""
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
    """
    Trunca un texto largo agregando '…' si se truncó.
    Si es None o vacío, devuelve '—'.
    """
    if texto is None or (isinstance(texto, str) and texto.strip() == ""):
        return "—"
    t = str(texto).strip()
    return (t[:maximo].rstrip() + "…") if len(t) > maximo else t


def _formatear_ot_pendiente(ot):
    """Bloque de detalle para una OT pendiente de asignar (sin cuadrilla aun)."""
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
    """Bloque de detalle para una OT ya asignada a cuadrilla."""
    cuadrilla = _norm(ot["cuadrilla_nombre"]) if ot.get("cuadrilla_nombre") \
        else _norm(ot["cuadrilla_id"])
    marca_fallida = (
        "\n  🚩 *VISITA FALLIDA - pendiente CGR*"
        if ot.get("visita_fallida") else ""
    )
    return (
        f"• `{_norm(ot['wonum'])}` · {_norm(ot['worktype'])} · "
        f"Sev {_norm(ot['severity'])}\n"
        f"  📍 {_norm(ot['departamento'])} · {_norm(ot['ciudad'])}\n"
        f"  📝 {_truncar_desc(ot['description'])}\n"
        f"  🏢 {_norm(ot['operador_fo'])}\n"
        f"  🔧 {cuadrilla}\n"
        f"  📊 Estado: _{_norm(ot['estado'])}_ / "
        f"Fase: _{_norm(ot['fase_operativa'])}_"
        f"{marca_fallida}"
    )


def _formatear_bandeja_coord(coord, datos):
    """
    Arma el texto completo de la bandeja del coordinador.
    """
    pendientes = datos["pendientes_asignar"]
    asignadas  = datos["asignadas_a_cuadrilla"]

    lineas = []
    lineas.append(f"📋 *Bandeja del Coordinador {coord['nombre_completo']}*\n")

    # Caso bandeja vacia
    if datos["total"] == 0:
        lineas.append(
            "_Tu bandeja esta vacia. No tienes OTs activas en este momento._"
        )
        return "\n".join(lineas)

    # Seccion 1: pendientes de asignar (resaltada)
    if pendientes:
        lineas.append(
            f"⚠️ *{len(pendientes)} OT(s) PENDIENTE(S) DE ASIGNAR*"
        )
        lineas.append("─────────────────────────────────")
        for ot in pendientes:
            lineas.append(_formatear_ot_pendiente(ot))
            lineas.append("")  # separador entre OTs

    # Seccion 2: asignadas a cuadrilla
    if asignadas:
        lineas.append(
            f"📌 *{len(asignadas)} OT(s) asignadas a cuadrillas*"
        )
        lineas.append("─────────────────────────────────")
        for ot in asignadas:
            lineas.append(_formatear_ot_asignada(ot))
            lineas.append("")

    return "\n".join(lineas)


# ═══════════════════════════════════════════════════════════════════
# BOTONES INLINE PARA LAS OTs DE LA BANDEJA DEL COORD
# ═══════════════════════════════════════════════════════════════════

def _construir_botones_bandeja_coord(datos):
    """
    Construye el InlineKeyboardMarkup con dos tipos de botones:

    1. Para cada OT PENDIENTE de asignar (los dejamos uno por OT
       porque tipicamente son pocas y es accion urgente):
       [🔧 Asignar XXXXX]   ->  asig_iniciar|<asig>

    2. UN solo botón [🔄 Reasignar OT...] que abre menu de seleccion
       si hay 1+ OTs asignadas:
                            ->  reasig_menu

    Si no hay OTs activas, retorna None.
    """
    pendientes = datos.get("pendientes_asignar", [])
    asignadas  = datos.get("asignadas_a_cuadrilla", [])

    if not pendientes and not asignadas:
        return None

    keyboard = []

    # Botones de asignar (uno por OT pendiente)
    for ot in pendientes:
        keyboard.append([
            InlineKeyboardButton(
                text=f"🔧 Asignar {ot['wonum']}",
                callback_data=f"asig_iniciar|{ot['asignacion_id']}",
            )
        ])

    # Boton unico para reasignar (abre menu si hay asignadas)
    if asignadas:
        keyboard.append([
            InlineKeyboardButton(
                text="🔄 Reasignar OT...",
                callback_data="reasig_menu",
            )
        ])

    return InlineKeyboardMarkup(keyboard)