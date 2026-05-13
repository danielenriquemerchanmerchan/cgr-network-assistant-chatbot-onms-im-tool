"""
bot/handlers/cierre.py
----------------------
Handler del comando /cierre.

La cuadrilla declara que terminó el trabajo. Cambia:
- estado: en_progreso -> 'declarada_terminada'
- fase_operativa: * -> 'validacion'
- fecha_declaracion_cierre: NOW()

NO cierra la OT definitivamente. Solo declara que terminaron desde su lado.
La OT pasa a estar en bandeja CGR esperando validacion.

Tipo de interaccion: declaracion_cierre (genera worklog + atencion CGR).
"""

import logging

from bot.services.cuadrillas import identificar_cuadrilla_por_chat_id
from bot.services.bandeja import (
    obtener_asignacion_activa_unica,
    obtener_ots_activas_cuadrilla,
)
from bot.services.cierre import declarar_cierre
from bot.services.interacciones import registrar_interaccion
from integrations.postgres.client import obtener_conexion, cerrar_conexion

logger = logging.getLogger(__name__)


async def handle(update, context):
    chat_id    = update.effective_chat.id
    chat_title = update.effective_chat.title or ""
    chat_type  = update.effective_chat.type
    user       = update.effective_user

    logger.info(f"/cierre recibido en chat {chat_id} ({chat_type})")

    if chat_type not in ("group", "supergroup"):
        await update.message.reply_text(
            "ℹ️ El comando /cierre solo funciona en el grupo de la cuadrilla."
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
                f"⚠️ Tienes varias OTs activas: {wonums}",
            )
        return

    # Cambiar estado a declarada_terminada (patron B: handler dueno
    # de la transaccion).
    conn = obtener_conexion()
    if conn is None:
        await update.message.reply_text(
            "❌ Error de conexion a BD. Intenta de nuevo en unos minutos."
        )
        return

    try:
        ok = declarar_cierre(ot["asignacion_id"], conn)
        if not ok:
            conn.rollback()
            await update.message.reply_text("❌ No se pudo declarar el cierre.")
            return

        # Registrar la interaccion en bot_interacciones (patron B:
        # handler registra log, servicio solo toca estado de negocio).
        registrar_interaccion(
            tipo_interaccion="declaracion_cierre",
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
            contenido_texto="/cierre",
            metadata={
                "comando": "/cierre",
                "fase_al_cierre": ot["fase_operativa"],
            },
        )

        conn.commit()

        await update.message.reply_text(
            f"✅ Cierre declarado para *OT {ot['wonum']}*\n\n"
            f"_CGR validará el cierre en breve. Mantengan el sitio "
            f"hasta confirmación._",
            parse_mode="Markdown",
        )

        logger.info(
            f"Cierre declarado: {cuadrilla['cuadrilla_id']} en OT "
            f"{ot['wonum']} (fase al cierre: {ot['fase_operativa']})"
        )

    except Exception as e:
        conn.rollback()
        logger.error(f"Error declarando cierre: {e}", exc_info=True)
        await update.message.reply_text("❌ No se pudo declarar el cierre.")
    finally:
        cerrar_conexion(conn)