"""
bot/handlers/llegada.py
-----------------------
Handler del comando /llegada.

La cuadrilla reporta que llego al sitio. Cambia:
- estado: aceptada -> en_progreso
- fase_operativa: cualquiera -> en_sitio
- fecha_inicio_progreso: NULL -> NOW() (si era NULL)

Tipo de interaccion: fase_llegada (genera worklog en MAXIMO).
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

    logger.info(f"/llegada recibido en chat {chat_id} ({chat_type})")

    # /llegada solo en grupos
    if chat_type not in ("group", "supergroup"):
        await update.message.reply_text(
            "ℹ️ El comando /llegada solo funciona en el grupo de la cuadrilla."
        )
        return

    # Identificar la cuadrilla
    cuadrilla = identificar_cuadrilla_por_chat_id(chat_id)
    if cuadrilla is None:
        await update.message.reply_text(
            f"⚠️ Este grupo no esta registrado.\n`chat_id = {chat_id}`",
            parse_mode="Markdown",
        )
        return

    # Buscar OT activa unica
    ot = obtener_asignacion_activa_unica(cuadrilla["cuadrilla_id"])

    if ot is None:
        # Caso: 0 OTs o multiples OTs activas
        ots_todas = obtener_ots_activas_cuadrilla(cuadrilla["cuadrilla_id"])
        
        if not ots_todas:
            await update.message.reply_text(
                "ℹ️ No tienes OTs activas en bandeja. No hay donde reportar llegada."
            )
        else:
            # Hay varias, no podemos auto-detectar
            wonums_lista = ", ".join(o["wonum"] for o in ots_todas)
            await update.message.reply_text(
                f"⚠️ Tienes varias OTs activas: {wonums_lista}\n\n"
                f"_(Selecciona una OT especifica - funcionalidad en proximas versiones)_",
                parse_mode="Markdown",
            )
        return

    # Hay UNA OT activa: actualizar fase
    fase_anterior = ot["fase_operativa"]
    estado_anterior = ot["estado"]
    
    ok = actualizar_fase_operativa(
        asignacion_id=ot["asignacion_id"],
        nueva_fase="en_sitio",
        nuevo_estado="en_progreso",
    )

    if not ok:
        await update.message.reply_text(
            "❌ No se pudo registrar la llegada. Intenta de nuevo o contacta soporte."
        )
        logger.error(f"Fallo actualizar_fase_operativa para asignacion {ot['asignacion_id']}")
        return

    # Registrar interaccion (tipo fase_llegada -> genera worklog en MAXIMO)
    registrar_interaccion(
        tipo_interaccion="fase_llegada",
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
        contenido_texto="/llegada",
        metadata={
            "comando": "/llegada",
            "fase_anterior": fase_anterior,
            "fase_nueva": "en_sitio",
            "estado_anterior": estado_anterior,
            "estado_nuevo": "en_progreso",
        },
    )

    # Responder al técnico
    await update.message.reply_text(
        f"✅ Llegada registrada en *OT {ot['wonum']}*\n"
        f"_{ot['descripcion'] or '(sin descripcion)'}_\n\n"
        f"Cuando comiencen a medir, escriban /midiendo",
        parse_mode="Markdown",
    )

    logger.info(
        f"Llegada registrada: {cuadrilla['cuadrilla_id']} en OT {ot['wonum']} "
        f"(de fase {fase_anterior} a en_sitio)"
    )