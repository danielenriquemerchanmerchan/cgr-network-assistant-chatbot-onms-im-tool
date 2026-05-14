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
from bot.handlers._markdown import md, md_or_dash, md_trunc, md_dept_abbr
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
# RAMA CUADRILLA
# ═══════════════════════════════════════════════════════════════════

async def _bandeja_cuadrilla(update, context):
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

    ots = obtener_ots_activas_cuadrilla(cuadrilla["cuadrilla_id"])

    if not ots:
        await update.message.reply_text(
            f"ℹ️ La bandeja de *{md(cuadrilla['nombre'])}* esta vacia.\n\n"
            f"No hay OTs activas asignadas en este momento.",
            parse_mode="Markdown",
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
            metadata={"comando": "/bandeja", "ots_listadas": 0},
        )
        return

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

    ots_en_espera = [ot for ot in ots if ot["asignacion_id"] != activa_id]
    ot_destacada = None
    if ot_activa:
        ot_destacada = next(
            (ot for ot in ots if ot["asignacion_id"] == activa_id), None
        )

    lineas = []
    lineas.append(f"📋 *Bandeja de {md(cuadrilla['nombre'])}*")
    lineas.append(f"_{len(ots)} OT(s) activa(s)_\n")

    if ot_destacada is not None:
        lineas.append("✅ *TRABAJANDO AHORA*")
        lineas.append(_formatear_ot_cuadrilla_destacada(ot_destacada))
    else:
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

    finally:
        cerrar_conexion(conn)


# ═══════════════════════════════════════════════════════════════════
# HELPERS DE FORMATO
# ═══════════════════════════════════════════════════════════════════

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
    marca_fallida = (
        "\n  🚩 *VISITA FALLIDA - pendiente CGR*"
        if ot.get("visita_fallida") else ""
    )
    urgencia = " 🚨" if ot.get("marcada_urgente_cgr") else ""
    nota_urgencia = (
        f"\n  ⚠️ {md(ot['nota_urgencia_cgr'])}"
        if ot.get("nota_urgencia_cgr") else ""
    )
    return (
        f"🔧 *{md_or_dash(ot['wonum'])}*{urgencia} · "
        f"{md_or_dash(ot['worktype'])} · Sev {md_or_dash(ot['severity'])} · "
        f"{md_dept_abbr(ot['departamento'])}/{md_or_dash(ot['ciudad'])}\n"
        f"  {md_trunc(ot['descripcion'])}\n"
        f"  🏢 {md_or_dash(ot['operador_fo'])}\n"
        f"  Estado: _{md_or_dash(ot['estado_descripcion'])}_ / "
        f"Fase: _{md_or_dash(ot['fase_descripcion'])}_"
        f"{marca_fallida}"
        f"{nota_urgencia}"
    )


def _formatear_ot_cuadrilla_en_espera(ot):
    marca_fallida = (
        "\n   🚩 *VISITA FALLIDA - pendiente CGR*"
        if ot.get("visita_fallida") else ""
    )
    return (
        f"🔹 _{md_or_dash(ot['wonum'])} · "
        f"{md_or_dash(ot['worktype'])} · Sev {md_or_dash(ot['severity'])} · "
        f"{md_dept_abbr(ot['departamento'])}/{md_or_dash(ot['ciudad'])}_\n"
        f"   _{md_trunc(ot['descripcion'])}_"
        f"{marca_fallida}"
    )


def _formatear_ot_pendiente(ot):
    return (
        f"• `{md_or_dash(ot['wonum'])}` · {md_or_dash(ot['worktype'])} · "
        f"Sev {md_or_dash(ot['severity'])}\n"
        f"  📍 {md_or_dash(ot['departamento'])} · {md_or_dash(ot['ciudad'])}\n"
        f"  📝 {md_trunc(ot['description'])}\n"
        f"  🏢 {md_or_dash(ot['operador_fo'])}\n"
        f"  └─ Acusada {_fmt_hace_cuanto(ot['notificacion_coordinador_recibida_at'])}, "
        f"sin cuadrilla aun"
    )


def _formatear_ot_asignada(ot):
    marca_fallida = (
        "\n  🚩 *VISITA FALLIDA - pendiente CGR*"
        if ot.get("visita_fallida") else ""
    )
    # Usamos las descripciones legibles si vienen del JOIN; si no,
    # caemos al codigo crudo. Las descripciones no tienen guiones bajos,
    # asi que no chocan con Markdown.
    estado_render = ot.get("estado_descripcion") or ot.get("estado")
    fase_render = ot.get("fase_descripcion") or ot.get("fase_operativa")
    return (
        f"• `{md_or_dash(ot['wonum'])}` · {md_or_dash(ot['worktype'])} · "
        f"Sev {md_or_dash(ot['severity'])} · "
        f"{md_dept_abbr(ot['departamento'])}/{md_or_dash(ot['ciudad'])}\n"
        f"  {md_trunc(ot['description'])}\n"
        f"  🏢 {md_or_dash(ot['operador_fo'])}\n"
        f"  Estado: _{md_or_dash(estado_render)}_ / "
        f"Fase: _{md_or_dash(fase_render)}_"
        f"{marca_fallida}"
    )


def _formatear_bandeja_coord(coord, datos):
    pendientes = datos["pendientes_asignar"]
    asignadas  = datos["asignadas_a_cuadrilla"]

    lineas = []
    lineas.append(
        f"📋 *Bandeja del Coordinador {md(coord['nombre_completo'])}*\n"
    )

    if datos["total"] == 0:
        lineas.append(
            "_Tu bandeja esta vacia. No tienes OTs activas en este momento._"
        )
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
            lineas.append(f"🛠 *{md(nombre_cuadrilla)}* ({n} {sufijo})")
            lineas.append("")
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