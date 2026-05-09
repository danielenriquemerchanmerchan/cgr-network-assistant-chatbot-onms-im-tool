"""
bot/handlers/start.py
---------------------
Handler del comando /start.
Registra cada interaccion en onms.bot_interacciones.
"""

import logging

from bot.services.cuadrillas import identificar_cuadrilla_por_chat_id
from bot.services.interacciones import registrar_interaccion

logger = logging.getLogger(__name__)


async def handle(update, context):
    nombre    = update.effective_user.first_name or "amigo"
    chat_id   = update.effective_chat.id
    chat_title = update.effective_chat.title or ""
    chat_type = update.effective_chat.type
    user      = update.effective_user

    logger.info(f"/start recibido de {nombre} en chat {chat_id} ({chat_type})")

    # Caso 1: chat privado
    if chat_type == "private":
        await update.message.reply_text(
            f"👋 Hola {nombre}!\n\n"
            f"Soy el bot ONMS. Estoy listo para hablar contigo en privado "
            f"(escenario de coordinador) o en un grupo (escenario de cuadrilla).\n\n"
            f"Tu user_id es: {user.id}"
        )
        # Registrar la interaccion (no sabemos si es coordinador, lo dejamos como sistema)
        registrar_interaccion(
            tipo_interaccion="mensaje_libre",
            direccion="entrante",
            actor_tipo="sistema",            # aun no podemos identificar al usuario
            telegram_chat_id=chat_id,
            telegram_message_id=update.message.message_id,
            telegram_user_id=user.id,
            telegram_username=user.username,
            contenido_texto="/start",
            metadata={"comando": "/start", "chat_type": chat_type},
        )
        return

    # Caso 2 y 3: grupo (registrado o no)
    cuadrilla = identificar_cuadrilla_por_chat_id(chat_id)

    if cuadrilla is not None:
        # Grupo registrado a una cuadrilla
        await update.message.reply_text(
            f"👋 Hola, equipo de *{cuadrilla['nombre']}*!\n\n"
            f"Contratista: {cuadrilla['contratista']}\n"
            f"Departamento: {cuadrilla['departamento']}\n\n"
            f"Listo para recibir comandos.",
            parse_mode="Markdown",
        )
        logger.info(f"Cuadrilla identificada: {cuadrilla['cuadrilla_id']}")

        # Registrar la interaccion
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
            contenido_texto="/start",
            metadata={"comando": "/start", "chat_type": chat_type},
        )
    else:
        # Grupo no registrado
        await update.message.reply_text(
            f"⚠️ Este grupo no esta registrado en el sistema ONMS.\n\n"
            f"Si crees que es un error, contacta al administrador con este dato:\n"
            f"`chat_id = {chat_id}`",
            parse_mode="Markdown",
        )
        logger.warning(f"Grupo no registrado: chat_id = {chat_id}")

        # Registrar como mensaje no clasificable (auditoria)
        registrar_interaccion(
            tipo_interaccion="mensaje_no_clasificable",
            direccion="entrante",
            actor_tipo="sistema",            # aun no identificamos
            telegram_chat_id=chat_id,
            telegram_chat_title=chat_title,
            telegram_message_id=update.message.message_id,
            telegram_user_id=user.id,
            telegram_username=user.username,
            contenido_texto="/start",
            metadata={
                "comando": "/start",
                "chat_type": chat_type,
                "razon": "grupo_no_registrado",
            },
        )