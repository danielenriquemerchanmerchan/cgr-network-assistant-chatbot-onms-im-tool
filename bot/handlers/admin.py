"""
bot/handlers/admin.py
---------------------
Handlers de comandos administrativos del bot.

ALCANCE:
    - /admin_recargar : recarga los caches de catalogos desde Postgres
                        sin reiniciar el bot.

REGLAS DE SEGURIDAD:
    Todos los comandos definidos aqui son privados: solo se ejecutan si
    el chat de origen coincide con ADMIN_CHAT_ID (definido en .env).

    Si un chat NO autorizado los invoca, el bot:
        - Loguea el intento (con chat_id, user_id, username)
        - Responde con SILENCIO TOTAL (no responde nada)

    El silencio es deliberado: no queremos exponer la existencia de
    comandos admin a chats que no los conocen. Si respondieramos con
    "no autorizado", cualquier persona descubriria que el comando existe.

NOTAS:
    Este modulo se importa desde main.py. Si en el futuro hay mas
    comandos admin (/admin_estado, /admin_logs, etc.), van todos aqui
    y comparten la verificacion de chat_id.
"""

import logging

from telegram import Update
from telegram.ext import ContextTypes

from core.config import ADMIN_CHAT_ID
from bot.services.cache_catalogos import recargar_caches, estado_cache

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════

def _es_admin(update: Update) -> bool:
    """
    True si el mensaje proviene del chat admin configurado.

    Si ADMIN_CHAT_ID esta en 0 (default cuando no se definio en .env),
    NINGUN chat es admin. Esto evita que en un deploy mal configurado
    cualquier chat pueda ejecutar comandos admin.
    """
    if ADMIN_CHAT_ID == 0:
        logger.warning(
            "[admin] ADMIN_CHAT_ID no configurado en .env — "
            "todos los comandos admin estan deshabilitados"
        )
        return False

    chat_id = update.effective_chat.id if update.effective_chat else None
    return chat_id == ADMIN_CHAT_ID


def _log_intento_no_autorizado(update: Update, comando: str):
    """
    Loguea un intento de uso de comando admin desde un chat no
    autorizado. Util para auditoria si alguien empieza a tantear.
    """
    chat = update.effective_chat
    user = update.effective_user
    logger.warning(
        f"[admin] Intento NO autorizado de {comando} "
        f"chat_id={chat.id if chat else '?'} "
        f"chat_title={getattr(chat, 'title', None) if chat else '?'} "
        f"user_id={user.id if user else '?'} "
        f"username={user.username if user else '?'}"
    )


# ══════════════════════════════════════════════════════════════
# /admin_recargar
# ══════════════════════════════════════════════════════════════

async def recargar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /admin_recargar
    ---------------
    Recarga todos los catalogos del bot desde Postgres a memoria.

    Casos de uso tipicos:
        - Operaciones edita un mensaje en cat_mensaje y queremos que
          se aplique sin reiniciar.
        - Se agregan nuevas filas a un catalogo (causales nuevas, fases
          nuevas, etc.) y queremos verlas activas ya.

    Comportamiento:
        - Solo ADMIN_CHAT_ID puede ejecutarlo. Otros chats: silencio.
        - Si la recarga es exitosa: mensaje con conteo de catalogos.
        - Si la recarga falla: mensaje con el error y aclaracion de
          que los catalogos VIEJOS siguen activos (no hubo
          reemplazo a medias).
    """
    if not _es_admin(update):
        _log_intento_no_autorizado(update, "/admin_recargar")
        return  # silencio total

    logger.info(
        f"[admin] /admin_recargar invocado por chat_id={update.effective_chat.id}"
    )

    try:
        recargar_caches()
    except Exception as e:
        logger.error(f"[admin] Error en recarga: {e}", exc_info=True)
        await update.message.reply_text(
            f"❌ *Error recargando catálogos*\n\n"
            f"`{type(e).__name__}: {e}`\n\n"
            f"Los catálogos en memoria *no se modificaron*. "
            f"El bot sigue operando con los valores anteriores.",
            parse_mode="Markdown",
        )
        return

    # Exito: armar mensaje con conteos
    estado = estado_cache()
    lineas = ["✅ *Catálogos recargados desde BD*", ""]
    for nombre, cantidad in estado.items():
        lineas.append(f"• `{nombre}`: {cantidad} entradas")
    lineas.append("")
    lineas.append("Los cambios ya están activos.")

    await update.message.reply_text(
        "\n".join(lineas),
        parse_mode="Markdown",
    )