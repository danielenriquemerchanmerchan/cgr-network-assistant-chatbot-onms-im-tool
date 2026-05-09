"""
bot/handlers/avance.py
----------------------
Handler del comando /avance.

La cuadrilla reporta avance libre con texto descriptivo.
NO cambia estado ni fase, solo registra la interaccion.

Tipo de interaccion: avance_libre (genera worklog en MAXIMO).

Uso: /avance Texto del avance...
"""

import logging

from bot.services.cuadrillas import identificar_cuadrilla_por_chat_id
from bot.services.bandeja import (
    obtener_asignacion_activa_unica,
    obtener_ots_activas_cuadrilla,
)
from bot.services.interacciones import registrar_interaccion

logger = logging.getLogger(__name__)


async def handle(update, context):
    chat_id    = update.effective_chat.id
    chat_title = update.effective_chat.title or ""
    chat_type  = update.effective_chat.type
    user       = update.effective_user

    # Extraer el texto que viene despues del comando
    args = context.args
    texto_avance = " ".join(args).strip()

    logger.info(f"/avance recibido en chat {chat_id} (texto: {len(texto_avance)} chars)")

    # Validar que hay texto
    if not texto_avance:
        await update.message.reply_text(
            "✏️ Por favor escribe el avance después del comando.\n\n"
            "Ejemplo:\n"
            "`/avance Localizamos corte por obra civil km 0.85`",
            parse_mode="Markdown",
        )
        return

    # Validaciones de chat type y cuadrilla (igual que los anteriores)
    if chat_type not in ("group", "supergroup"):
        await update.message.reply_text(
            "ℹ️ El comando /avance solo funciona en el grupo de la cuadrilla."
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

    # Registrar el avance (no modificamos ot_bandeja, solo bot_interacciones)
    registrar_interaccion(
        tipo_interaccion="avance_libre",
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
        contenido_texto=texto_avance,
        metadata={
            "comando": "/avance",
            "longitud_caracteres": len(texto_avance),
        },
    )

    await update.message.reply_text(
        f"✅ Avance registrado en *OT {ot['wonum']}*",
        parse_mode="Markdown",
    )

    logger.info(
        f"Avance registrado: {cuadrilla['cuadrilla_id']} en OT {ot['wonum']} "
        f"({len(texto_avance)} caracteres)"
    )