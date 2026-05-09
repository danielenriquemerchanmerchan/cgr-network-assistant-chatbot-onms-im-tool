"""
bot/handlers/bandeja.py
-----------------------
Handler del comando /bandeja.

Muestra las OTs activas de la cuadrilla y registra la consulta.
"""

import logging

from bot.services.cuadrillas import identificar_cuadrilla_por_chat_id
from bot.services.bandeja import obtener_ots_activas_cuadrilla
from bot.services.interacciones import registrar_interaccion

logger = logging.getLogger(__name__)


async def handle(update, context):
    chat_id    = update.effective_chat.id
    chat_title = update.effective_chat.title or ""
    chat_type  = update.effective_chat.type
    user       = update.effective_user

    logger.info(f"/bandeja recibido en chat {chat_id} ({chat_type})")

    # /bandeja solo tiene sentido en grupos de cuadrilla
    if chat_type not in ("group", "supergroup"):
        await update.message.reply_text(
            "ℹ️ El comando /bandeja solo funciona en el grupo de la cuadrilla."
        )
        return

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
            lineas.append(
                f"*{i}. OT {ot['wonum']}*{urgencia}\n"
                f"  📍 {ot['descripcion'] or '(sin descripcion)'}\n"
                f"  🔧 Tipo: {ot['tipo_tramo'] or 'N/D'}\n"
                f"  📊 Estado: _{ot['estado_descripcion']}_\n"
                f"  ⚙️ Fase: _{ot['fase_descripcion']}_"
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

    logger.info(f"Bandeja consultada por {cuadrilla['cuadrilla_id']}: {len(ots)} OTs")