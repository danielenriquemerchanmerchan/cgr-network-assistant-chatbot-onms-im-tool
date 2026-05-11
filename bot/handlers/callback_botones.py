"""
bot/handlers/callback_botones.py
--------------------------------
Handler de callbacks de botones inline.

Maneja los siguientes tipos de callback:

Flujo /bloqueo (parada de reloj):
1. bloq_grupo|<grupo>|<asig>             -> mostrar causales del grupo
2. bloq_causal|<causal>|<asig>           -> mostrar estimados de duracion
3. bloq_estimado|<causal>|<est>|<asig>   -> crear parada en BD
4. bloq_volver_grupos|<asig>             -> regresar a menu de grupos
5. bloq_volver_causales|<grupo>|<asig>   -> regresar a menu de causales

Flujo de acuse del coordinador (notificacion de OT nueva):
6. ot_recibida|<asig>                    -> coord acusa + muestra cuadrillas
7. ot_rechazada|<asig>                   -> coord rechaza ("no es de mi zona")

Flujo de asignacion a cuadrilla:
8. asig_elegir|<asig>|<cuadrilla>        -> mostrar pantalla de confirmacion
9. asig_confirmar|<asig>|<cuadrilla>     -> ejecutar asignacion + notificar
10. asig_volver|<asig>                   -> volver a lista de cuadrillas
11. asig_iniciar|<asig>                  -> iniciar asignacion desde /bandeja

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
from bot.services.acuse_ot import (
    obtener_asignacion,
    procesar_acuse_recibida,
    procesar_acuse_rechazada,
)
from bot.services.asignar_cuadrilla import (
    obtener_cuadrillas_del_coord,
    obtener_datos_ot_para_grupo,
    asignar_a_cuadrilla,
)
from bot.services.cache_catalogos import obtener_mensaje
from integrations.postgres.client import obtener_conexion, cerrar_conexion

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
        elif accion == "ot_recibida":
            await _procesar_recibida(query, partes, update)
        elif accion == "ot_rechazada":
            await _procesar_rechazada(query, partes, update)
        elif accion == "asig_elegir":
            await _mostrar_confirmar_asignacion(query, partes, update)
        elif accion == "asig_confirmar":
            await _ejecutar_asignacion(query, partes, update, context)
        elif accion == "asig_volver":
            await _volver_a_cuadrillas(query, partes, update)
        elif accion == "asig_iniciar":
            await _iniciar_asignacion_desde_bandeja(query, partes, update)
        else:
            logger.warning(f"Callback desconocido: {data}")
            await query.edit_message_text(
                "⚠️ Acción no reconocida. Por favor inicia de nuevo con /bloqueo"
            )
    except Exception as e:
        logger.error(f"Error procesando callback {data}: {e}", exc_info=True)
        # Para callbacks de bloqueo damos instruccion clara. Para otros
        # mantenemos generico.
        if accion.startswith("bloq_"):
            await query.edit_message_text(
                f"❌ Error procesando la acción.\n"
                f"Por favor inicia de nuevo con /bloqueo"
            )
        else:
            await query.edit_message_text(
                f"❌ Error procesando la acción. Revisa los logs del bot."
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


# ======================================================================
# RAMA 6 — coord acusa [Recibida] de una OT recien notificada
# ======================================================================

async def _procesar_recibida(query, partes, update):
    """
    callback_data: ot_recibida|<asignacion_id>

    Flujo:
        1. Validar que quien presiona sea el coord asignado a la OT.
        2. Actualizar BD (estado y fase nuevos + recibida_at = NOW()).
        3. Editar el mensaje original: quitar botones, agregar linea con
           el visto bueno y timestamp.
        4. Enviar mensaje aparte con el texto de confirmacion del cache.
    """
    _, asignacion_id = partes
    asignacion_id = int(asignacion_id)
    user_id = update.effective_user.id

    conn = obtener_conexion()
    if conn is None:
        await query.edit_message_text(
            "❌ Error de conexion a BD. Intenta de nuevo en unos minutos."
        )
        return

    try:
        # 1) Leer datos de la asignacion
        asig = obtener_asignacion(asignacion_id, conn)
        if asig is None:
            await query.edit_message_text(
                f"⚠️ La asignacion {asignacion_id} no existe en BD."
            )
            return

        wonum = asig["wonum"]

        # 2) Validar que quien presiona el boton sea el coord asignado
        if asig["coord_telegram_user_id"] != user_id:
            logger.warning(
                f"[Acuse] Usuario {user_id} intento responder OT {wonum} "
                f"pero el coord asignado es {asig['coordinador_asignado_id']} "
                f"(tg={asig['coord_telegram_user_id']}). Ignorando."
            )
            await query.answer(
                text="No eres el coordinador asignado a esta OT.",
                show_alert=True,
            )
            return

        # 3) Si ya fue acusada antes, no procesar de nuevo
        if asig["notificacion_coordinador_recibida_at"] is not None:
            logger.info(
                f"[Acuse] OT {wonum} (asig={asignacion_id}) ya fue acusada "
                f"previamente. Ignorando click duplicado."
            )
            await query.answer(text="Esta OT ya fue procesada.", show_alert=False)
            return

        # 4) Actualizar BD
        ok = procesar_acuse_recibida(asignacion_id, conn)
        conn.commit()
        if not ok:
            await query.answer(
                text="No se pudo procesar la OT en este momento.",
                show_alert=True,
            )
            return

        # 5) Editar el mensaje original: quitar botones, agregar marca
        from datetime import datetime
        marca = datetime.now().strftime("%d/%m/%Y %H:%M")
        texto_original = query.message.text_markdown or query.message.text or ""
        nuevo_texto = (
            f"{texto_original}\n\n"
            f"✅ *Recibida* — {marca}"
        )
        try:
            await query.edit_message_text(
                text=nuevo_texto,
                parse_mode="Markdown",
                # reply_markup omitido -> quita los botones
            )
        except Exception as e:
            # Si la edicion falla (ej. mensaje muy viejo, > 48h),
            # al menos la BD ya quedo bien.
            logger.warning(
                f"[Acuse] No se pudo editar el mensaje original "
                f"(asig={asignacion_id}): {e}. La BD si fue actualizada."
            )

        # 6) Enviar mensaje aparte con botones de cuadrillas para asignar
        await _enviar_botones_cuadrillas(
            chat=query.message.chat,
            asignacion_id=asignacion_id,
            wonum=wonum,
            coordinador_id=asig["coordinador_asignado_id"],
            conn=conn,
        )

    finally:
        cerrar_conexion(conn)


# ======================================================================
# RAMA 7 — coord rechaza con [No es de mi zona]
# ======================================================================

async def _procesar_rechazada(query, partes, update):
    """
    callback_data: ot_rechazada|<asignacion_id>

    Mismo flujo que _procesar_recibida pero con:
        - SQL distinto (estado='rechazada_por_coordinador', asignacion_activa=false)
        - Marca en el mensaje con ❌
        - Texto de confirmacion 'notificacion.ot_acuse_rechazada'
    """
    _, asignacion_id = partes
    asignacion_id = int(asignacion_id)
    user_id = update.effective_user.id

    conn = obtener_conexion()
    if conn is None:
        await query.edit_message_text(
            "❌ Error de conexion a BD. Intenta de nuevo en unos minutos."
        )
        return

    try:
        asig = obtener_asignacion(asignacion_id, conn)
        if asig is None:
            await query.edit_message_text(
                f"⚠️ La asignacion {asignacion_id} no existe en BD."
            )
            return

        wonum = asig["wonum"]

        if asig["coord_telegram_user_id"] != user_id:
            logger.warning(
                f"[Acuse] Usuario {user_id} intento rechazar OT {wonum} "
                f"pero no es el coord asignado. Ignorando."
            )
            await query.answer(
                text="No eres el coordinador asignado a esta OT.",
                show_alert=True,
            )
            return

        if asig["notificacion_coordinador_recibida_at"] is not None:
            await query.answer(text="Esta OT ya fue procesada.", show_alert=False)
            return

        ok = procesar_acuse_rechazada(asignacion_id, conn)
        conn.commit()
        if not ok:
            await query.answer(
                text="No se pudo procesar la OT en este momento.",
                show_alert=True,
            )
            return

        from datetime import datetime
        marca = datetime.now().strftime("%d/%m/%Y %H:%M")
        texto_original = query.message.text_markdown or query.message.text or ""
        nuevo_texto = (
            f"{texto_original}\n\n"
            f"❌ *Rechazada* (no es de mi zona) — {marca}"
        )
        try:
            await query.edit_message_text(
                text=nuevo_texto,
                parse_mode="Markdown",
            )
        except Exception as e:
            logger.warning(
                f"[Acuse] No se pudo editar el mensaje original "
                f"(asig={asignacion_id}): {e}. La BD si fue actualizada."
            )

        texto_confirm = obtener_mensaje(
            "notificacion.ot_acuse_rechazada",
            wonum=wonum,
        )
        await query.message.reply_text(
            text=texto_confirm,
            parse_mode="Markdown",
        )

    finally:
        cerrar_conexion(conn)

# ======================================================================
# HELPER — Mostrar botones de cuadrillas al coord
# ======================================================================

async def _enviar_botones_cuadrillas(chat, asignacion_id, wonum, coordinador_id, conn):
    """
    Envia un mensaje nuevo al coord con los botones de sus cuadrillas
    activas. Cada boton muestra cuadrilla_id + cantidad de OTs activas.

    callback_data por boton: asig_elegir|<asig>|<cuadrilla_id>
    """
    cuadrillas = obtener_cuadrillas_del_coord(coordinador_id, conn)

    if not cuadrillas:
        # Caso defensivo: el coord no tiene cuadrillas activas asociadas.
        # No deberia pasar en este piloto (COORD_TEST_DM tiene 2), pero
        # protegemos.
        await chat.send_message(
            text=(
                f"⚠️ No tienes cuadrillas activas asociadas. "
                f"Contacta al administrador."
            )
        )
        return

    # Texto introductorio (editable desde cat_mensaje)
    texto = obtener_mensaje(
        "asignacion.elegir_cuadrilla",
        wonum=wonum,
    )

    # Botones: uno por cuadrilla. Cada uno con su conteo de OTs activas.
    keyboard = []
    for cu in cuadrillas:
        ots = cu["ots_activas"]
        etiqueta = f"🔧 {cu['cuadrilla_id']} ({ots})"
        keyboard.append([
            InlineKeyboardButton(
                text=etiqueta,
                callback_data=f"asig_elegir|{asignacion_id}|{cu['cuadrilla_id']}",
            )
        ])

    await chat.send_message(
        text=texto,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


# ======================================================================
# RAMA 8 — coord eligio cuadrilla, mostrar pantalla de confirmacion
# ======================================================================

async def _mostrar_confirmar_asignacion(query, partes, update):
    """
    callback_data: asig_elegir|<asignacion_id>|<cuadrilla_id>

    Edita el mensaje (que tenia los botones de cuadrillas) para mostrar
    la pantalla de confirmacion con [Si confirmar] / [Volver].
    """
    _, asignacion_id, cuadrilla_id = partes
    asignacion_id = int(asignacion_id)

    conn = obtener_conexion()
    if conn is None:
        await query.edit_message_text(
            "❌ Error de conexion a BD. Intenta de nuevo en unos minutos."
        )
        return

    try:
        # Necesitamos el wonum y el nombre amigable de la cuadrilla
        asig = obtener_asignacion(asignacion_id, conn)
        if asig is None:
            await query.edit_message_text(
                f"⚠️ La asignacion {asignacion_id} no existe en BD."
            )
            return

        # Validar identidad
        if asig["coord_telegram_user_id"] != update.effective_user.id:
            await query.answer(
                text="No eres el coordinador de esta OT.",
                show_alert=True,
            )
            return

        wonum = asig["wonum"]

        # Buscar el nombre legible de la cuadrilla
        cuadrillas = obtener_cuadrillas_del_coord(
            asig["coordinador_asignado_id"], conn
        )
        cu = next((c for c in cuadrillas if c["cuadrilla_id"] == cuadrilla_id), None)
        if cu is None:
            await query.edit_message_text(
                f"⚠️ La cuadrilla {cuadrilla_id} no esta asociada a ti."
            )
            return

        texto = obtener_mensaje(
            "asignacion.confirmar_seleccion",
            wonum=wonum,
            cuadrilla_nombre=cu.get("nombre") or cu["cuadrilla_id"],
        )

        keyboard = [
            [InlineKeyboardButton(
                text="✅ Sí, confirmar",
                callback_data=f"asig_confirmar|{asignacion_id}|{cuadrilla_id}",
            )],
            [InlineKeyboardButton(
                text="⬅️ Volver a cuadrillas",
                callback_data=f"asig_volver|{asignacion_id}",
            )],
        ]

        await query.edit_message_text(
            text=texto,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    finally:
        cerrar_conexion(conn)


# ======================================================================
# RAMA 9 — coord confirmo, ejecutar asignacion y notificar grupo
# ======================================================================

async def _ejecutar_asignacion(query, partes, update, context):
    """
    callback_data: asig_confirmar|<asignacion_id>|<cuadrilla_id>

    Flujo:
        1. Validar identidad.
        2. UPDATE ot_bandeja con cuadrilla, estado=aceptada, fase=asignada.
        3. Editar el mensaje del coord con la confirmacion final.
        4. Enviar mensaje informativo (sin botones) al grupo Telegram
           de la cuadrilla.
    """
    _, asignacion_id, cuadrilla_id = partes
    asignacion_id = int(asignacion_id)

    conn = obtener_conexion()
    if conn is None:
        await query.edit_message_text(
            "❌ Error de conexion a BD. Intenta de nuevo en unos minutos."
        )
        return

    try:
        asig = obtener_asignacion(asignacion_id, conn)
        if asig is None:
            await query.edit_message_text(
                f"⚠️ La asignacion {asignacion_id} no existe en BD."
            )
            return

        if asig["coord_telegram_user_id"] != update.effective_user.id:
            await query.answer(
                text="No eres el coordinador de esta OT.",
                show_alert=True,
            )
            return

        wonum = asig["wonum"]

        # 1) Ejecutar UPDATE en BD
        ok = asignar_a_cuadrilla(asignacion_id, cuadrilla_id, conn)
        conn.commit()

        if not ok:
            await query.edit_message_text(
                f"⚠️ La OT {wonum} ya no puede asignarse (cambio de estado). "
                f"Probablemente ya fue procesada."
            )
            return

        # 2) Traer datos completos para construir el mensaje al grupo
        datos = obtener_datos_ot_para_grupo(asignacion_id, conn)
        if datos is None:
            logger.error(
                f"[Asignacion] No se pudieron recuperar datos de la OT "
                f"asignacion_id={asignacion_id} despues del UPDATE"
            )
            await query.edit_message_text(
                f"⚠️ OT asignada pero no se pudo notificar al grupo. "
                f"Revisar logs."
            )
            return

        # 3) Editar el mensaje del coord con la confirmacion final
        texto_coord = obtener_mensaje(
            "asignacion.asignada_ok",
            wonum=wonum,
            cuadrilla_nombre=datos["cuadrilla_nombre"] or cuadrilla_id,
        )
        try:
            await query.edit_message_text(
                text=texto_coord,
                parse_mode="Markdown",
                # reply_markup omitido -> quita los botones
            )
        except Exception as e:
            logger.warning(
                f"[Asignacion] No se pudo editar el mensaje del coord "
                f"(asig={asignacion_id}): {e}. La BD si fue actualizada."
            )

        # 4) Enviar mensaje al grupo Telegram de la cuadrilla
        chat_id_grupo = datos["telegram_chat_id_cuadrilla"]
        if chat_id_grupo is None:
            logger.error(
                f"[Asignacion] Cuadrilla {cuadrilla_id} no tiene "
                f"telegram_chat_id. La OT quedo asignada en BD pero no "
                f"se notifico al grupo."
            )
            # No interrumpimos el flujo; al menos quedo en BD.
            return

        # Normalizar campos para el mensaje (mismo patron del notificador)
        def _norm(v):
            if v is None:
                return "—"
            if isinstance(v, str) and v.strip() == "":
                return "—"
            return v

        def _fecha(dt):
            if dt is None:
                return "—"
            try:
                return dt.strftime("%Y-%m-%d %H:%M")
            except AttributeError:
                return str(dt)

        def _descripcion(t):
            if t is None or (isinstance(t, str) and t.strip() == ""):
                return "—"
            t = str(t).strip()
            return (t[:200].rstrip() + "…") if len(t) > 200 else t

        texto_grupo = obtener_mensaje(
            "asignacion.notificacion_grupo",
            wonum=_norm(datos["wonum"]),
            worktype=_norm(datos["worktype"]),
            departamento=_norm(datos["departamento"]),
            ciudad=_norm(datos["ciudad"]),
            operador_fo=_norm(datos["operador_fo"]),
            severity=_norm(datos["severity"]),
            creation_date=_fecha(datos["creation_date"]),
            description=_descripcion(datos["description"]),
            cinum=_norm(datos["cinum"]),
            tipo_tramo=_norm(datos["tipo_tramo"]),
            direccion=_norm(datos["direccion"]),
            coord_nombre=_norm(datos["coord_nombre"]),
        )

        try:
            await context.bot.send_message(
                chat_id=chat_id_grupo,
                text=texto_grupo,
                parse_mode="Markdown",
            )
            logger.info(
                f"[Asignacion] OT {wonum} -> cuadrilla {cuadrilla_id}: "
                f"mensaje enviado al grupo chat_id={chat_id_grupo}"
            )
        except Exception as e:
            logger.error(
                f"[Asignacion] Falla al enviar mensaje al grupo de "
                f"cuadrilla {cuadrilla_id} (chat_id={chat_id_grupo}): {e}",
                exc_info=True,
            )
            # No revertimos: la asignacion en BD si quedo, solo el aviso fallo.

    finally:
        cerrar_conexion(conn)


# ======================================================================
# RAMA 10 — coord oprimio "Volver", regresar a lista de cuadrillas
# ======================================================================

async def _volver_a_cuadrillas(query, partes, update):
    """
    callback_data: asig_volver|<asignacion_id>

    Vuelve a mostrar los botones de cuadrillas (reusando la misma
    helper). Edita el mensaje (que tenia la confirmacion) para
    reemplazar su contenido con la lista nueva.
    """
    _, asignacion_id = partes
    asignacion_id = int(asignacion_id)

    conn = obtener_conexion()
    if conn is None:
        await query.edit_message_text(
            "❌ Error de conexion a BD. Intenta de nuevo en unos minutos."
        )
        return

    try:
        asig = obtener_asignacion(asignacion_id, conn)
        if asig is None:
            await query.edit_message_text(
                f"⚠️ La asignacion {asignacion_id} no existe en BD."
            )
            return

        if asig["coord_telegram_user_id"] != update.effective_user.id:
            await query.answer(
                text="No eres el coordinador de esta OT.",
                show_alert=True,
            )
            return

        cuadrillas = obtener_cuadrillas_del_coord(
            asig["coordinador_asignado_id"], conn
        )

        texto = obtener_mensaje(
            "asignacion.elegir_cuadrilla",
            wonum=asig["wonum"],
        )

        keyboard = []
        for cu in cuadrillas:
            ots = cu["ots_activas"]
            etiqueta = f"🔧 {cu['cuadrilla_id']} ({ots})"
            keyboard.append([
                InlineKeyboardButton(
                    text=etiqueta,
                    callback_data=f"asig_elegir|{asignacion_id}|{cu['cuadrilla_id']}",
                )
            ])

        await query.edit_message_text(
            text=texto,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    finally:
        cerrar_conexion(conn)


# ======================================================================
# RAMA 11 — coord oprimio "Asignar XXXXX" desde /bandeja
# ======================================================================

async def _iniciar_asignacion_desde_bandeja(query, partes, update):
    """
    callback_data: asig_iniciar|<asignacion_id>

    Punto de entrada al flujo de asignacion DESDE el comando /bandeja
    del coord (no desde el botón [Recibida] de una notificacion).

    Cubre el caso de OTs que estan en pendiente_asignacion_coordinador
    porque el coord las acuso pero todavia no las asigno a una cuadrilla.

    Reusa el helper _enviar_botones_cuadrillas() que ya existe.
    """
    _, asignacion_id = partes
    asignacion_id = int(asignacion_id)

    conn = obtener_conexion()
    if conn is None:
        await query.answer(
            text="❌ Error de conexion a BD. Intenta de nuevo.",
            show_alert=True,
        )
        return

    try:
        asig = obtener_asignacion(asignacion_id, conn)
        if asig is None:
            await query.answer(
                text=f"⚠️ La asignacion {asignacion_id} no existe en BD.",
                show_alert=True,
            )
            return

        # Validar identidad
        if asig["coord_telegram_user_id"] != update.effective_user.id:
            await query.answer(
                text="No eres el coordinador de esta OT.",
                show_alert=True,
            )
            return

        # Validacion defensiva: solo se puede asignar si esta en el
        # estado correcto.
        if asig["estado"] != "pendiente_asignacion_coordinador":
            await query.answer(
                text=(
                    f"Esta OT no esta pendiente de asignar "
                    f"(estado: {asig['estado']})."
                ),
                show_alert=True,
            )
            return

        # Mostrar los botones de cuadrillas (reusando el helper existente).
        # Se envia como mensaje aparte para no destruir el mensaje
        # original del /bandeja.
        await _enviar_botones_cuadrillas(
            chat=query.message.chat,
            asignacion_id=asignacion_id,
            wonum=asig["wonum"],
            coordinador_id=asig["coordinador_asignado_id"],
            conn=conn,
        )

    finally:
        cerrar_conexion(conn)