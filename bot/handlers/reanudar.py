"""
bot/handlers/reanudar.py
------------------------
Handler del comando /reanudar.

Cierra la parada de reloj activa de la asignacion. Restaura la fase
operativa de la OT a 'midiendo' (o la fase que pasemos por contexto).

NO recibe argumentos: detecta automaticamente la parada activa de la
unica OT activa en bandeja de la cuadrilla.
"""

import logging

from bot.services.cuadrillas import identificar_cuadrilla_por_chat_id
from bot.services.bandeja import (
    obtener_asignacion_activa_unica,
    obtener_ots_activas_cuadrilla,
)
from bot.services.paradas import obtener_parada_activa, cerrar_parada
from bot.services.interacciones import registrar_interaccion

logger = logging.getLogger(__name__)


async def handle(update, context):
    chat_id    = update.effective_chat.id
    chat_title = update.effective_chat.title or ""
    chat_type  = update.effective_chat.type
    user       = update.effective_user

    logger.info(f"/reanudar recibido en chat {chat_id} ({chat_type})")

    # Solo en grupos
    if chat_type not in ("group", "supergroup"):
        await update.message.reply_text(
            "ℹ️ El comando /reanudar solo funciona en el grupo de la cuadrilla."
        )
        return

    # Identificar cuadrilla
    cuadrilla = identificar_cuadrilla_por_chat_id(chat_id)
    if cuadrilla is None:
        await update.message.reply_text(
            f"⚠️ Este grupo no esta registrado.\n`chat_id = {chat_id}`",
            parse_mode="Markdown",
        )
        return

    # Buscar la OT activa unica de la cuadrilla
    ot = obtener_asignacion_activa_unica(cuadrilla["cuadrilla_id"])
    if ot is None:
        ots_todas = obtener_ots_activas_cuadrilla(cuadrilla["cuadrilla_id"])
        if not ots_todas:
            await update.message.reply_text("ℹ️ No tienes OTs activas en bandeja.")
        else:
            wonums = ", ".join(o["wonum"] for o in ots_todas)
            await update.message.reply_text(
                f"⚠️ Tienes varias OTs activas: {wonums}\n"
                f"No es claro cuál reanudar."
            )
        return

    # Buscar la parada activa de la OT
    parada = obtener_parada_activa(ot["asignacion_id"])
    if parada is None:
        await update.message.reply_text(
            f"ℹ️ La OT {ot['wonum']} no tiene paradas activas.\n"
            f"Si quieres declarar una nueva parada, escribe /bloqueo"
        )
        return

    # Cerrar la parada
    resultado = cerrar_parada(
        parada_id=parada["parada_id"],
        cerrada_por_chat_id=chat_id,
        cerrada_por_telegram_user_id=user.id,
    )

    if resultado is None:
        await update.message.reply_text(
            "❌ Error cerrando la parada. Revisa los logs del bot."
        )
        return

    # Registrar la interaccion
    registrar_interaccion(
        tipo_interaccion="parada_reloj_fin",
        direccion="entrante",
        actor_tipo="cuadrilla",
        actor_id=cuadrilla["cuadrilla_id"],
        cuadrilla_id=cuadrilla["cuadrilla_id"],
        wonum=ot["wonum"],
        asignacion_id=ot["asignacion_id"],
        parada_id=parada["parada_id"],
        telegram_chat_id=chat_id,
        telegram_chat_title=chat_title,
        telegram_message_id=update.message.message_id,
        telegram_user_id=user.id,
        telegram_username=user.username,
        contenido_texto="/reanudar",
        metadata={
            "comando": "/reanudar",
            "parada_id_cerrada": parada["parada_id"],
            "causal_original": parada["causal"],
            "duracion_real": str(resultado["duracion_real"]),
        },
    )

    # Formatear duracion bonita: HH:MM:SS o "X minutos"
    duracion_str = _formatear_duracion(resultado["duracion_real"])

    await update.message.reply_text(
        f"✅ *Trabajo reanudado*\n\n"
        f"OT: `{ot['wonum']}`\n"
        f"Parada cerrada: `{parada['parada_id']}`\n"
        f"Duración: *{duracion_str}*\n\n"
        f"La OT volvió a fase _midiendo_.",
        parse_mode="Markdown",
    )


def _formatear_duracion(timedelta_obj):
    """
    Convierte un timedelta de Python a un string legible.
    Ej: 1:23:45 -> "1h 23min"
        0:05:30 -> "5min"
        2 days, 1:00:00 -> "49h"
    """
    total_segundos = int(timedelta_obj.total_seconds())
    horas = total_segundos // 3600
    minutos = (total_segundos % 3600) // 60

    if horas == 0:
        return f"{minutos} min"
    elif minutos == 0:
        return f"{horas}h"
    else:
        return f"{horas}h {minutos}min"