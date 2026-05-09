"""
bot/handlers/bloqueo.py
-----------------------
Handler del comando /bloqueo.

Inicia el flujo de declaracion de parada de reloj. Muestra los grupos de
causales (espera/bloqueo/riesgo/otro) como botones.

IMPORTANTE: Los grupos se leen de la tabla onms.cat_grupo_causal_parada
en cada invocacion. Cualquier cambio en BD se refleja inmediatamente
sin reiniciar el bot.
"""

import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from bot.services.cuadrillas import identificar_cuadrilla_por_chat_id
from bot.services.bandeja import (
    obtener_asignacion_activa_unica,
    obtener_ots_activas_cuadrilla,
)
from bot.services.catalogos import obtener_grupos_causal_activos

logger = logging.getLogger(__name__)


async def handle(update, context):
    chat_id   = update.effective_chat.id
    chat_type = update.effective_chat.type

    logger.info(f"/bloqueo recibido en chat {chat_id} ({chat_type})")

    if chat_type not in ("group", "supergroup"):
        await update.message.reply_text(
            "ℹ️ El comando /bloqueo solo funciona en el grupo de la cuadrilla."
        )
        return

    cuadrilla = identificar_cuadrilla_por_chat_id(chat_id)
    if cuadrilla is None:
        await update.message.reply_text(
            f"⚠️ Este grupo no esta registrado.\n`chat_id = {chat_id}`",
            parse_mode="Markdown",
        )
        return

    # Buscar la OT activa unica
    ot = obtener_asignacion_activa_unica(cuadrilla["cuadrilla_id"])
    if ot is None:
        ots_todas = obtener_ots_activas_cuadrilla(cuadrilla["cuadrilla_id"])
        if not ots_todas:
            await update.message.reply_text("ℹ️ No tienes OTs activas en bandeja.")
        else:
            wonums = ", ".join(o["wonum"] for o in ots_todas)
            await update.message.reply_text(f"⚠️ Tienes varias OTs activas: {wonums}")
        return

    # Leer grupos desde BD (NO hardcodeados)
    grupos = obtener_grupos_causal_activos()

    if not grupos:
        await update.message.reply_text(
            "⚠️ No hay grupos de causales configurados en el sistema. "
            "Contacta al administrador."
        )
        logger.error("cat_grupo_causal_parada esta vacio o sin grupos activos")
        return

    # Construir botones de los grupos
    asignacion_id = ot["asignacion_id"]
    keyboard = []
    for grupo in grupos:
        texto_boton = f"{grupo['emoji']} {grupo['descripcion_corta']}"
        boton = InlineKeyboardButton(
            text=texto_boton,
            callback_data=f"bloq_grupo|{grupo['codigo']}|{asignacion_id}",
        )
        keyboard.append([boton])

    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"🚧 *Declarar parada de reloj*\n"
        f"OT: {ot['wonum']}\n\n"
        f"¿Qué tipo de bloqueo están reportando?",
        reply_markup=reply_markup,
        parse_mode="Markdown",
    )

    logger.info(
        f"Menu de grupos mostrado para asignacion {asignacion_id} "
        f"(cuadrilla {cuadrilla['cuadrilla_id']}, {len(grupos)} grupos)"
    )