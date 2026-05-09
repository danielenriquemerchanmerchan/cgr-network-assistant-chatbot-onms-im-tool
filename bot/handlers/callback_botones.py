"""
bot/handlers/callback_botones.py
--------------------------------
Handler de callbacks de botones inline para el flujo /bloqueo.

Maneja 4 tipos de callback:
1. bloq_grupo|<grupo>|<asig>             -> mostrar causales del grupo
2. bloq_causal|<causal>|<asig>           -> mostrar estimados de duracion
3. bloq_estimado|<causal>|<est>|<asig>   -> crear parada en BD
4. bloq_volver_grupos|<asig>             -> regresar a menu de grupos
5. bloq_volver_causales|<grupo>|<asig>   -> regresar a menu de causales

Toda la informacion de catalogos se lee de BD en cada callback.
"""

import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from bot.services.catalogos import (
    obtener_grupos_causal_activos,
    obtener_causales_de_grupo,
    obtener_estimados_duracion_activos,
)
from bot.services.cuadrillas import identificar_cuadrilla_por_chat_id
from bot.services.bandeja import obtener_asignacion_activa_unica
from bot.services.paradas import crear_parada
from bot.services.interacciones import registrar_interaccion

logger = logging.getLogger(__name__)


# ======================================================================
# ENTRY POINT — dispatcher por tipo de callback
# ======================================================================

async def handle(update, context):
    """
    Recibe cualquier callback de botones y lo enruta segun el prefijo.
    """
    query = update.callback_query
    await query.answer()  # Quita el spinner del boton en Telegram

    data = query.data
    logger.info(f"Callback recibido: {data}")

    # Parsear el callback_data
    partes = data.split("|")
    accion = partes[0]

    try:
        if accion == "bloq_grupo":
            await _mostrar_causales(query, partes)
        elif accion == "bloq_causal":
            await _mostrar_estimados(query, partes)
        elif accion == "bloq_estimado":
            await _crear_parada(query, partes, update)
        elif accion == "bloq_volver_grupos":
            await _volver_a_grupos(query, partes)
        elif accion == "bloq_volver_causales":
            await _volver_a_causales(query, partes)
        else:
            logger.warning(f"Callback desconocido: {data}")
            await query.edit_message_text(
                "⚠️ Acción no reconocida. Por favor inicia de nuevo con /bloqueo"
            )
    except Exception as e:
        logger.error(f"Error procesando callback {data}: {e}", exc_info=True)
        await query.edit_message_text(
            f"❌ Error procesando la acción.\n"
            f"Por favor inicia de nuevo con /bloqueo"
        )


# ======================================================================
# RAMA 1 — mostrar causales del grupo seleccionado
# ======================================================================

async def _mostrar_causales(query, partes):
    """
    callback_data: bloq_grupo|<grupo>|<asig>
    """
    _, grupo_codigo, asignacion_id = partes
    asignacion_id = int(asignacion_id)

    # Si el usuario eligio "otro", paramos aqui (esta causal requiere detalle textual)
    if grupo_codigo == "otro":
        await query.edit_message_text(
            "⚠️ *Causal 'Otro' requiere detalle textual*\n\n"
            "Esta opción aún no está implementada en el bot. "
            "Por favor selecciona otra causal.\n\n"
            "Para volver, escribe /bloqueo de nuevo.",
            parse_mode="Markdown",
        )
        return

    causales = obtener_causales_de_grupo(grupo_codigo)
    if not causales:
        await query.edit_message_text(
            f"⚠️ No hay causales activas para el grupo *{grupo_codigo}*.\n"
            f"Contacta al administrador.",
            parse_mode="Markdown",
        )
        return

    # Construir botones de causales
    keyboard = []
    for causal in causales:
        texto = f"{causal['emoji']} {causal['descripcion']}"
        keyboard.append([
            InlineKeyboardButton(
                text=texto,
                callback_data=f"bloq_causal|{causal['codigo']}|{asignacion_id}",
            )
        ])

    # Boton volver
    keyboard.append([
        InlineKeyboardButton(
            text="⬅️ Volver",
            callback_data=f"bloq_volver_grupos|{asignacion_id}",
        )
    ])

    # Recuperar el emoji del grupo para el header
    grupos = obtener_grupos_causal_activos()
    grupo = next((g for g in grupos if g["codigo"] == grupo_codigo), None)
    header_emoji = grupo["emoji"] if grupo else "🚧"
    header_texto = grupo["descripcion_corta"] if grupo else grupo_codigo

    await query.edit_message_text(
        f"{header_emoji} *{header_texto}*\n\n"
        f"¿Qué están esperando exactamente?",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


# ======================================================================
# RAMA 2 — mostrar estimados de duracion (paso nuevo)
# ======================================================================

async def _mostrar_estimados(query, partes):
    """
    callback_data: bloq_causal|<causal>|<asig>
    """
    _, causal_codigo, asignacion_id = partes
    asignacion_id = int(asignacion_id)

    estimados = obtener_estimados_duracion_activos()
    if not estimados:
        await query.edit_message_text(
            "⚠️ No hay estimados de duración configurados.\n"
            "Contacta al administrador.",
        )
        return

    # Encontrar el grupo al que pertenece la causal (para el boton volver)
    grupo_de_causal = _encontrar_grupo_de_causal(causal_codigo)

    # Construir botones de estimados con marca de "atencion CGR"
    keyboard = []
    for est in estimados:
        texto = est["descripcion"]
        if est["requiere_atencion"]:
            texto += " ⚠️"
        keyboard.append([
            InlineKeyboardButton(
                text=texto,
                callback_data=f"bloq_estimado|{causal_codigo}|{est['codigo']}|{asignacion_id}",
            )
        ])

    # Boton volver
    if grupo_de_causal:
        keyboard.append([
            InlineKeyboardButton(
                text="⬅️ Volver",
                callback_data=f"bloq_volver_causales|{grupo_de_causal}|{asignacion_id}",
            )
        ])

    await query.edit_message_text(
        f"⏱️ *¿Cuánto estiman que dure?*\n\n"
        f"_⚠️ marca duraciones que requieren atención del CGR._",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


# ======================================================================
# RAMA 3 — crear parada en BD (final del flujo)
# ======================================================================

async def _crear_parada(query, partes, update):
    """
    callback_data: bloq_estimado|<causal>|<estimado>|<asig>
    """
    _, causal_codigo, estimado_codigo, asignacion_id = partes
    asignacion_id = int(asignacion_id)

    chat_id = query.message.chat.id
    chat_title = query.message.chat.title or ""
    user = update.effective_user

    # Re-identificar la cuadrilla y la OT (defensivo: el estado pudo cambiar)
    cuadrilla = identificar_cuadrilla_por_chat_id(chat_id)
    if cuadrilla is None:
        await query.edit_message_text(
            f"⚠️ Este grupo ya no esta registrado.\n`chat_id = {chat_id}`",
            parse_mode="Markdown",
        )
        return

    ot = obtener_asignacion_activa_unica(cuadrilla["cuadrilla_id"])
    if ot is None or ot["asignacion_id"] != asignacion_id:
        await query.edit_message_text(
            "⚠️ La OT cambió de estado mientras seleccionabas. "
            "Por favor inicia de nuevo con /bloqueo"
        )
        return

    # Obtener el flag requiere_atencion del estimado seleccionado
    estimados = obtener_estimados_duracion_activos()
    estimado = next((e for e in estimados if e["codigo"] == estimado_codigo), None)
    if estimado is None:
        await query.edit_message_text(
            f"⚠️ Estimado '{estimado_codigo}' no es valido.",
        )
        return

    requiere_atencion = estimado["requiere_atencion"]

    # Crear la parada via servicio
    parada_id = crear_parada(
        asignacion_id=asignacion_id,
        wonum=ot["wonum"],
        cuadrilla_id=cuadrilla["cuadrilla_id"],
        causal=causal_codigo,
        estimado_duracion=estimado_codigo,
        requiere_atencion_exhaustiva=requiere_atencion,
        declarada_por_chat_id=chat_id,
        declarada_por_telegram_user_id=user.id,
    )

    if parada_id is None:
        await query.edit_message_text(
            "❌ Error registrando la parada en la base de datos. "
            "Revisa los logs del bot.",
        )
        return

    # Registrar la interaccion
    registrar_interaccion(
        tipo_interaccion="parada_reloj_inicio",
        direccion="entrante",
        actor_tipo="cuadrilla",
        actor_id=cuadrilla["cuadrilla_id"],
        cuadrilla_id=cuadrilla["cuadrilla_id"],
        wonum=ot["wonum"],
        asignacion_id=asignacion_id,
        parada_id=parada_id,
        telegram_chat_id=chat_id,
        telegram_chat_title=chat_title,
        telegram_user_id=user.id,
        telegram_username=user.username,
        contenido_texto=f"Parada por causal '{causal_codigo}', estimado '{estimado_codigo}'",
        metadata={
            "comando": "/bloqueo",
            "causal": causal_codigo,
            "estimado_duracion": estimado_codigo,
            "requiere_atencion_exhaustiva": requiere_atencion,
            "parada_id_creada": parada_id,
        },
    )

    # Mensaje de confirmacion
    aviso_atencion = (
        "\n\n⚠️ _CGR sera notificado para monitorear esta parada._"
        if requiere_atencion else ""
    )

    await query.edit_message_text(
        f"🚧 *Parada registrada*\n\n"
        f"OT: `{ot['wonum']}`\n"
        f"Causal: `{causal_codigo}`\n"
        f"Estimado: `{estimado_codigo}`\n"
        f"Parada ID: `{parada_id}`\n\n"
        f"La OT pasó a fase *pausada*."
        f"{aviso_atencion}\n\n"
        f"Cuando reanuden, escriban /reanudar",
        parse_mode="Markdown",
    )


# ======================================================================
# RAMA 4 — volver al menu de grupos
# ======================================================================

async def _volver_a_grupos(query, partes):
    """
    callback_data: bloq_volver_grupos|<asig>
    """
    _, asignacion_id = partes
    asignacion_id = int(asignacion_id)

    grupos = obtener_grupos_causal_activos()
    keyboard = []
    for grupo in grupos:
        texto = f"{grupo['emoji']} {grupo['descripcion_corta']}"
        keyboard.append([
            InlineKeyboardButton(
                text=texto,
                callback_data=f"bloq_grupo|{grupo['codigo']}|{asignacion_id}",
            )
        ])

    await query.edit_message_text(
        "🚧 *Declarar parada de reloj*\n\n"
        "¿Qué tipo de bloqueo están reportando?",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


# ======================================================================
# RAMA 5 — volver al menu de causales del grupo
# ======================================================================

async def _volver_a_causales(query, partes):
    """
    callback_data: bloq_volver_causales|<grupo>|<asig>
    """
    _, grupo_codigo, asignacion_id = partes
    # Reusar la logica de mostrar causales
    await _mostrar_causales(query, ["bloq_grupo", grupo_codigo, asignacion_id])


# ======================================================================
# Helper privado
# ======================================================================

def _encontrar_grupo_de_causal(causal_codigo):
    """
    Dado un codigo de causal, busca a que grupo pertenece.
    Lo necesitamos para construir el boton "volver" en el menu de estimados.

    Retorna el codigo del grupo, o None si no se encuentra.
    """
    grupos = obtener_grupos_causal_activos()
    for grupo in grupos:
        causales = obtener_causales_de_grupo(grupo["codigo"])
        if any(c["codigo"] == causal_codigo for c in causales):
            return grupo["codigo"]
    return None