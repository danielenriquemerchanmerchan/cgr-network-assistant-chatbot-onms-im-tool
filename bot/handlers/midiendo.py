"""
bot/handlers/midiendo.py
------------------------
Handler del comando /midiendo.

La cuadrilla reporta que comenzo a medir. Cambia:
- fase_operativa: cualquiera -> 'midiendo'

Tipo de interaccion: fase_midiendo (genera worklog en MAXIMO).
"""

import logging

from bot.services.cuadrillas import identificar_cuadrilla_por_chat_id
from bot.services.bandeja import (
    obtener_asignacion_activa_unica,
    obtener_ots_activas_cuadrilla,
    actualizar_fase_operativa,
)
from bot.services.interacciones import registrar_interaccion

logger = logging.getLogger(__name__)


async def handle(update, context):
    chat_id    = update.effective_chat.id
    chat_title = update.effective_chat.title or ""
    chat_type  = update.effective_chat.type
    user       = update.effective_user

    logger.info(f"/midiendo recibido en chat {chat_id} ({chat_type})")

    if chat_type not in ("group", "supergroup"):
        await update.message.reply_text(
            "ℹ️ El comando /midiendo solo funciona en el grupo de la cuadrilla."
        )
        return

    cuadrilla = identificar_cuadrilla_por_chat_id(chat_id)
    if cuadrilla is None:
        await update.message.reply_text(
            f"⚠️ Este grupo no esta registrado.\n`chat_id = {chat_id}`",
            parse_mode="Markdown",
        )
        return

    ot = obtener_asignacion_activa_unica(cuadrilla["cuadrilla_id"])
    if ot is None:
        ots_todas = obtener_ots_activas_cuadrilla(cuadrilla["cuadrilla_id"])
        if not ots_todas:
            await update.message.reply_text(
                "ℹ️ No tienes OTs activas en bandeja."
            )
        else:
            wonums = ", ".join(o["wonum"] for o in ots_todas)
            await update.message.reply_text(
                f"⚠️ Tienes varias OTs activas: {wonums}\n"
                f"_(Selección por OT específica - próximas versiones)_",
                parse_mode="Markdown",
            )
        return

    fase_anterior = ot["fase_operativa"]

    ok = actualizar_fase_operativa(
        asignacion_id=ot["asignacion_id"],
        nueva_fase="midiendo",
        # NO cambiamos estado - debe estar ya en 'en_progreso'
    )

    if not ok:
        await update.message.reply_text("❌ No se pudo registrar la medición.")
        logger.error(f"Fallo actualizar_fase para asignacion {ot['asignacion_id']}")
        return

    registrar_interaccion(
        tipo_interaccion="fase_midiendo",
        direccion="entrante",
        actor_tipo="cuadrilla",
        actor_id=cuadrilla["cuadrilla_id"],
        cuadrilla_id=cuadrilla["cuadrilla_id"],
        wonum=ot["wonum"],
        asignacion_id=ot["asignacion_id"],
        telegram_chat_id=chat_id,
        telegram_chat_title=chat_title,
        telegram_message_id=update.message.message_id,
        telegram_user_id=user.id,
        telegram_username=user.username,
        contenido_texto="/midiendo",
        metadata={
            "comando": "/midiendo",
            "fase_anterior": fase_anterior,
            "fase_nueva": "midiendo",
        },
    )

    await update.message.reply_text(
        f"📡 Medición iniciada en *OT {ot['wonum']}*\n\n"
        f"Cuando localicen falla, escriban /avance con el detalle.",
        parse_mode="Markdown",
    )

    logger.info(
        f"Midiendo registrado: {cuadrilla['cuadrilla_id']} en OT {ot['wonum']} "
        f"(de {fase_anterior} a midiendo)"
    )