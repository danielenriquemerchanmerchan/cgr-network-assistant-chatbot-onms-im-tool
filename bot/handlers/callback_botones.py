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

Flujo de reasignacion de cuadrilla (cambio de cuadrilla en OT ya asignada):
12. reasig_menu                          -> mostrar lista de OTs para escoger
13. reasig_iniciar|<asig>                -> mostrar cuadrillas disponibles
14. reasig_elegir|<asig>|<cuadrilla>     -> mostrar pantalla de confirmacion
15. reasig_confirmar|<asig>|<cuadrilla>  -> ejecutar reasignacion + notificar a 2 grupos
16. reasig_volver|<asig>                 -> volver a la lista (cancelar)

Genericos:
17. cancelar_operacion                   -> cierra el menu y muestra "cancelado"

Flujo de visita fallida:
18. visfall_elegir|<asig>                -> coord eligio OT cuando habia varias
19. visfall_confirmar|<asig>             -> ejecuta visita fallida + worklog

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
from bot.services.reasignar_cuadrilla import (
    obtener_datos_para_reasignacion,
    obtener_datos_cuadrilla,
    reasignar_a_cuadrilla,
)
from bot.services.visita_fallida import (
    obtener_ot_para_visita_fallida,
    marcar_visita_fallida,
)
from bot.services.cache_catalogos import obtener_mensaje
from integrations.postgres.client import obtener_conexion, cerrar_conexion

logger = logging.getLogger(__name__)


# ======================================================================
# HELPER: escapar caracteres especiales de Telegram Markdown V1
# ======================================================================

def _escapar_md(texto):
    """
    Escapa caracteres reservados de Markdown V1 para que texto del
    usuario no rompa el formato del mensaje al renderizar.
    """
    if not texto:
        return ""
    s = str(texto)
    for ch in ("\\", "*", "_", "`", "[", "]"):
        s = s.replace(ch, "\\" + ch)
    return s


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
        elif accion == "reasig_menu":
            await _mostrar_menu_reasignar(query, update)
        elif accion == "reasig_iniciar":
            await _iniciar_reasignacion(query, partes, update)
        elif accion == "reasig_elegir":
            await _mostrar_confirmar_reasignacion(query, partes, update)
        elif accion == "reasig_confirmar":
            await _ejecutar_reasignacion(query, partes, update, context)
        elif accion == "reasig_volver":
            await _iniciar_reasignacion(query, partes, update)
        elif accion == "cancelar_operacion":
            await _cancelar_operacion(query)
        elif accion == "visfall_elegir":
            await _visfall_elegir_ot(query, partes, update, context)
        elif accion == "visfall_confirmar":
            await _visfall_confirmar(query, partes, update, context)
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

        # 4.b) Registrar la interaccion en bot_interacciones
        registrar_interaccion(
            tipo_interaccion="acuse_ot",
            direccion="entrante",
            actor_tipo="coordinador_contratista",
            actor_id=asig["coordinador_asignado_id"],
            wonum=wonum,
            asignacion_id=asignacion_id,
            telegram_chat_id=update.effective_chat.id,
            telegram_chat_title=(update.effective_chat.title or ""),
            telegram_message_id=query.message.message_id,
            telegram_user_id=user_id,
            telegram_username=update.effective_user.username,
            contenido_texto="[Recibida]",
            metadata={"accion": "recibida"},
        )

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

        # Registrar la interaccion en bot_interacciones
        registrar_interaccion(
            tipo_interaccion="acuse_ot",
            direccion="entrante",
            actor_tipo="coordinador_contratista",
            actor_id=asig["coordinador_asignado_id"],
            wonum=wonum,
            asignacion_id=asignacion_id,
            telegram_chat_id=update.effective_chat.id,
            telegram_chat_title=(update.effective_chat.title or ""),
            telegram_message_id=query.message.message_id,
            telegram_user_id=user_id,
            telegram_username=update.effective_user.username,
            contenido_texto="[No es de mi zona]",
            metadata={"accion": "rechazada"},
        )

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

        # 1.b) Registrar la interaccion en bot_interacciones
        registrar_interaccion(
            tipo_interaccion="asignacion_cuadrilla",
            direccion="entrante",
            actor_tipo="coordinador_contratista",
            actor_id=asig["coordinador_asignado_id"],
            cuadrilla_id=cuadrilla_id,
            wonum=wonum,
            asignacion_id=asignacion_id,
            telegram_chat_id=update.effective_chat.id,
            telegram_chat_title=(update.effective_chat.title or ""),
            telegram_message_id=query.message.message_id,
            telegram_user_id=update.effective_user.id,
            telegram_username=update.effective_user.username,
            contenido_texto=f"Asigno OT a cuadrilla {cuadrilla_id}",
            metadata={"cuadrilla_id": cuadrilla_id},
        )

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


# ======================================================================
# RAMA 12 — coord oprimio [Reasignar XXXXX] desde /bandeja
# ======================================================================

async def _iniciar_reasignacion(query, partes, update):
    """
    callback_data: reasig_iniciar|<asignacion_id>
                   reasig_volver|<asignacion_id>   (reusa esta misma funcion)

    Muestra la lista de cuadrillas del coord para escoger la nueva.
    La actual aparece en la lista pero etiquetada como "(actual)" y al
    presionarla no hace nada (de hecho la quitamos para evitar confusion).
    """
    _, asignacion_id = partes
    asignacion_id = int(asignacion_id)

    conn = obtener_conexion()
    if conn is None:
        await query.answer(
            text="❌ Error de conexion a BD.",
            show_alert=True,
        )
        return

    try:
        datos = obtener_datos_para_reasignacion(asignacion_id, conn)
        if datos is None:
            await query.answer(
                text=f"⚠️ La asignacion {asignacion_id} no existe.",
                show_alert=True,
            )
            return

        # Validar identidad
        if datos["coord_telegram_user_id"] != update.effective_user.id:
            await query.answer(
                text="No eres el coordinador de esta OT.",
                show_alert=True,
            )
            return

        # Validacion: la OT tiene que estar asignada y activa
        if not datos["asignacion_activa"]:
            await query.answer(
                text="Esta OT ya no esta activa.",
                show_alert=True,
            )
            return
        if datos["cuadrilla_anterior_id"] is None:
            await query.answer(
                text="Esta OT no tiene cuadrilla asignada todavia.",
                show_alert=True,
            )
            return

        wonum = datos["wonum"]
        cuadrilla_anterior_nombre = (
            datos["cuadrilla_anterior_nombre"]
            or datos["cuadrilla_anterior_id"]
        )

        # Traer todas las cuadrillas del coord (con conteo de OTs activas)
        cuadrillas = obtener_cuadrillas_del_coord(
            datos["coordinador_id"], conn
        )

        # Filtrar la cuadrilla actual (no la mostramos como opcion)
        cuadrillas_disponibles = [
            cu for cu in cuadrillas
            if cu["cuadrilla_id"] != datos["cuadrilla_anterior_id"]
        ]

        if not cuadrillas_disponibles:
            # El coord no tiene otras cuadrillas a las que pueda mover
            await query.edit_message_text(
                text=(
                    f"⚠️ No hay otras cuadrillas disponibles para "
                    f"reasignar la OT {wonum}.\n\n"
                    f"Solo tienes la cuadrilla *{cuadrilla_anterior_nombre}* "
                    f"asociada."
                ),
                parse_mode="Markdown",
            )
            return

        # Texto introductorio
        texto = obtener_mensaje(
            "reasignacion.elegir_cuadrilla_nueva",
            wonum=wonum,
            cuadrilla_anterior_nombre=cuadrilla_anterior_nombre,
        )

        # Botones: uno por cuadrilla disponible
        keyboard = []
        for cu in cuadrillas_disponibles:
            ots = cu["ots_activas"]
            etiqueta = f"🔧 {cu['cuadrilla_id']} ({ots})"
            keyboard.append([
                InlineKeyboardButton(
                    text=etiqueta,
                    callback_data=(
                        f"reasig_elegir|{asignacion_id}|{cu['cuadrilla_id']}"
                    ),
                )
            ])

        # Boton de cancelar (cierra el menu sin accion)
        keyboard.append([
            InlineKeyboardButton(
                text="⬅️ Cancelar",
                callback_data="cancelar_operacion",
            )
        ])

        # Diferencia clave: si vinimos de "reasig_iniciar" (boton desde
        # /bandeja), enviamos mensaje nuevo. Si vinimos de "reasig_volver",
        # editamos el mensaje actual.
        accion_origen = partes[0]
        if accion_origen == "reasig_volver":
            await query.edit_message_text(
                text=texto,
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
        else:
            await query.message.chat.send_message(
                text=texto,
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )

    finally:
        cerrar_conexion(conn)


# ======================================================================
# RAMA 13 — coord eligio cuadrilla nueva, mostrar confirmacion
# ======================================================================

async def _mostrar_confirmar_reasignacion(query, partes, update):
    """
    callback_data: reasig_elegir|<asignacion_id>|<cuadrilla_nueva_id>

    Edita el mensaje (el que tenia los botones de cuadrillas) para
    mostrar la pantalla de confirmacion.
    """
    _, asignacion_id, cuadrilla_nueva_id = partes
    asignacion_id = int(asignacion_id)

    conn = obtener_conexion()
    if conn is None:
        await query.edit_message_text(
            "❌ Error de conexion a BD."
        )
        return

    try:
        datos = obtener_datos_para_reasignacion(asignacion_id, conn)
        if datos is None:
            await query.edit_message_text(
                f"⚠️ La asignacion {asignacion_id} no existe."
            )
            return

        if datos["coord_telegram_user_id"] != update.effective_user.id:
            await query.answer(
                text="No eres el coordinador de esta OT.",
                show_alert=True,
            )
            return

        # Buscar el nombre legible de la cuadrilla nueva
        cuadrilla_nueva = obtener_datos_cuadrilla(cuadrilla_nueva_id, conn)
        if cuadrilla_nueva is None:
            await query.edit_message_text(
                f"⚠️ La cuadrilla {cuadrilla_nueva_id} no existe o esta inactiva."
            )
            return

        wonum = datos["wonum"]
        nombre_anterior = (
            datos["cuadrilla_anterior_nombre"]
            or datos["cuadrilla_anterior_id"]
        )
        nombre_nueva = (
            cuadrilla_nueva["nombre"] or cuadrilla_nueva["cuadrilla_id"]
        )

        texto = obtener_mensaje(
            "reasignacion.confirmar",
            wonum=wonum,
            cuadrilla_anterior_nombre=nombre_anterior,
            cuadrilla_nueva_nombre=nombre_nueva,
        )

        keyboard = [
            [InlineKeyboardButton(
                text="✅ Sí, reasignar",
                callback_data=(
                    f"reasig_confirmar|{asignacion_id}|{cuadrilla_nueva_id}"
                ),
            )],
            [InlineKeyboardButton(
                text="⬅️ Volver",
                callback_data=f"reasig_volver|{asignacion_id}",
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
# RAMA 14 — coord confirmo reasignacion, ejecutar
# ======================================================================

async def _ejecutar_reasignacion(query, partes, update, context):
    """
    callback_data: reasig_confirmar|<asignacion_id>|<cuadrilla_nueva_id>

    Flujo:
        1. Leer datos actuales.
        2. UPDATE BD (cambia cuadrilla_id + fecha_asignacion_cuadrilla).
        3. Editar mensaje del coord con confirmacion.
        4. Enviar mensaje al grupo de la cuadrilla anterior (aviso).
        5. Enviar mensaje al grupo de la cuadrilla nueva (notificacion completa).
    """
    _, asignacion_id, cuadrilla_nueva_id = partes
    asignacion_id = int(asignacion_id)

    conn = obtener_conexion()
    if conn is None:
        await query.edit_message_text(
            "❌ Error de conexion a BD."
        )
        return

    try:
        # 1) Traer datos ANTES del UPDATE (necesitamos chat_id de la
        #    cuadrilla anterior para avisarle).
        datos = obtener_datos_para_reasignacion(asignacion_id, conn)
        if datos is None:
            await query.edit_message_text(
                f"⚠️ La asignacion {asignacion_id} no existe."
            )
            return

        if datos["coord_telegram_user_id"] != update.effective_user.id:
            await query.answer(
                text="No eres el coordinador de esta OT.",
                show_alert=True,
            )
            return

        wonum = datos["wonum"]
        nombre_anterior = (
            datos["cuadrilla_anterior_nombre"]
            or datos["cuadrilla_anterior_id"]
        )
        chat_id_anterior = datos["chat_id_cuadrilla_anterior"]

        # Datos de la cuadrilla nueva
        cuadrilla_nueva = obtener_datos_cuadrilla(cuadrilla_nueva_id, conn)
        if cuadrilla_nueva is None:
            await query.edit_message_text(
                f"⚠️ La cuadrilla {cuadrilla_nueva_id} no existe."
            )
            return
        nombre_nueva = (
            cuadrilla_nueva["nombre"] or cuadrilla_nueva["cuadrilla_id"]
        )
        chat_id_nueva = cuadrilla_nueva["telegram_chat_id"]

        # 2) Ejecutar el UPDATE
        ok = reasignar_a_cuadrilla(asignacion_id, cuadrilla_nueva_id, conn)
        conn.commit()

        if not ok:
            await query.edit_message_text(
                text=(
                    f"⚠️ No se pudo reasignar la OT {wonum}. "
                    f"Probablemente la cuadrilla escogida es la misma actual."
                ),
                parse_mode="Markdown",
            )
            return

        # 2.b) Registrar la interaccion en bot_interacciones
        registrar_interaccion(
            tipo_interaccion="reasignacion_cuadrilla",
            direccion="entrante",
            actor_tipo="coordinador_contratista",
            actor_id=datos["coordinador_id"],
            cuadrilla_id=cuadrilla_nueva_id,
            wonum=wonum,
            asignacion_id=asignacion_id,
            telegram_chat_id=update.effective_chat.id,
            telegram_chat_title=(update.effective_chat.title or ""),
            telegram_message_id=query.message.message_id,
            telegram_user_id=update.effective_user.id,
            telegram_username=update.effective_user.username,
            contenido_texto=(
                f"Reasigno OT de {datos['cuadrilla_anterior_id']} "
                f"a {cuadrilla_nueva_id}"
            ),
            metadata={
                "cuadrilla_anterior": datos["cuadrilla_anterior_id"],
                "cuadrilla_nueva": cuadrilla_nueva_id,
            },
        )

        # 3) Editar mensaje del coord con confirmacion
        texto_coord = obtener_mensaje(
            "reasignacion.asignada_ok",
            wonum=wonum,
            cuadrilla_anterior_nombre=nombre_anterior,
            cuadrilla_nueva_nombre=nombre_nueva,
        )
        try:
            await query.edit_message_text(
                text=texto_coord,
                parse_mode="Markdown",
            )
        except Exception as e:
            logger.warning(
                f"[Reasignacion] No se pudo editar el mensaje del coord "
                f"(asig={asignacion_id}): {e}. La BD si fue actualizada."
            )

        # 4) Enviar aviso a la cuadrilla ANTERIOR (si tiene chat_id)
        if chat_id_anterior is not None:
            try:
                texto_anterior = obtener_mensaje(
                    "reasignacion.aviso_cuadrilla_anterior",
                    wonum=wonum,
                    cuadrilla_nueva_nombre=nombre_nueva,
                )
                await context.bot.send_message(
                    chat_id=chat_id_anterior,
                    text=texto_anterior,
                    parse_mode="Markdown",
                )
                logger.info(
                    f"[Reasignacion] aviso enviado a cuadrilla anterior "
                    f"chat_id={chat_id_anterior}"
                )
            except Exception as e:
                logger.error(
                    f"[Reasignacion] Falla al avisar a cuadrilla anterior "
                    f"(chat_id={chat_id_anterior}): {e}"
                )
        else:
            logger.warning(
                f"[Reasignacion] Cuadrilla anterior "
                f"{datos['cuadrilla_anterior_id']} no tiene chat_id, "
                f"no se pudo avisar."
            )

        # 5) Enviar notificacion a la cuadrilla NUEVA
        if chat_id_nueva is None:
            logger.error(
                f"[Reasignacion] Cuadrilla nueva {cuadrilla_nueva_id} "
                f"no tiene chat_id. La OT quedo reasignada pero no se "
                f"notifico al grupo."
            )
            return

        # Normalizar campos (mismo patron del notificador)
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

        texto_nueva = obtener_mensaje(
            "reasignacion.aviso_cuadrilla_nueva",
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
            cuadrilla_anterior_nombre=nombre_anterior,
            coord_nombre=_norm(datos["coord_nombre"]),
        )

        try:
            await context.bot.send_message(
                chat_id=chat_id_nueva,
                text=texto_nueva,
                parse_mode="Markdown",
            )
            logger.info(
                f"[Reasignacion] OT {wonum} -> cuadrilla nueva "
                f"{cuadrilla_nueva_id}: mensaje enviado al grupo "
                f"chat_id={chat_id_nueva}"
            )
        except Exception as e:
            logger.error(
                f"[Reasignacion] Falla al notificar a cuadrilla nueva "
                f"(chat_id={chat_id_nueva}): {e}",
                exc_info=True,
            )

    finally:
        cerrar_conexion(conn)


# ======================================================================
# CANCELAR — borra los botones y muestra mensaje de cancelacion
# ======================================================================

async def _cancelar_operacion(query):
    """
    callback_data: cancelar_operacion

    Generico para cualquier menu que tenga un boton [Cancelar].
    Edita el mensaje a texto plano "Operacion cancelada" sin botones.
    """
    try:
        await query.edit_message_text(
            text=(
                "ℹ️ _Operación cancelada._\n\n"
                "Para ver tu bandeja, escribe /bandeja."
            ),
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.warning(f"[Cancelar] No se pudo editar el mensaje: {e}")
        # Si la edicion falla, al menos respondemos al callback
        await query.answer(text="Operacion cancelada.", show_alert=False)


# ======================================================================
# MENU DE REASIGNAR — listado de OTs asignadas como botones
# ======================================================================

async def _mostrar_menu_reasignar(query, update):
    """
    callback_data: reasig_menu

    Cuando el coord presiona [🔄 Reasignar OT...] en su /bandeja,
    se le muestra un menu con la lista de sus OTs asignadas. Al
    presionar una, se llama al flujo existente reasig_iniciar.
    """
    from bot.services.bandeja_coord import (
        identificar_coordinador_por_telegram_user_id,
        obtener_ots_del_coord,
    )

    user_id = update.effective_user.id

    conn = obtener_conexion()
    if conn is None:
        await query.answer(
            text="❌ Error de conexion a BD.",
            show_alert=True,
        )
        return

    try:
        # Validar identidad: solo coords pueden ver esto
        coord = identificar_coordinador_por_telegram_user_id(user_id, conn)
        if coord is None:
            await query.answer(
                text="No estas registrado como coordinador.",
                show_alert=True,
            )
            return

        # Traer las OTs activas del coord
        datos = obtener_ots_del_coord(coord["coordinador_id"], conn)
        asignadas = datos["asignadas_a_cuadrilla"]

        if not asignadas:
            await query.edit_message_text(
                text=(
                    "ℹ️ No tienes OTs asignadas a cuadrillas para reasignar."
                ),
            )
            return

        # Construir botones: uno por cada OT asignada + cancelar
        keyboard = []
        for ot in asignadas:
            # Nombre legible de la cuadrilla actual
            cu_nombre = (
                ot.get("cuadrilla_nombre")
                or ot.get("cuadrilla_id")
                or "—"
            )
            # Etiqueta del boton: wonum + cuadrilla actual (truncada si es larga)
            cu_corto = cu_nombre[:25] + "…" if len(cu_nombre) > 25 else cu_nombre
            etiqueta = f"🔄 {ot['wonum']} · {cu_corto}"
            keyboard.append([
                InlineKeyboardButton(
                    text=etiqueta,
                    callback_data=f"reasig_iniciar|{ot['asignacion_id']}",
                )
            ])

        # Boton de cancelar al final
        keyboard.append([
            InlineKeyboardButton(
                text="⬅️ Cancelar",
                callback_data="cancelar_operacion",
            )
        ])

        texto = (
            "🔄 *Reasignar OT*\n\n"
            "Selecciona la OT que quieres reasignar:"
        )

        # Enviamos como mensaje nuevo (no editamos el de /bandeja para
        # conservarlo)
        await query.message.chat.send_message(
            text=texto,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    finally:
        cerrar_conexion(conn)


# ======================================================================
# RAMA 18 — cuadrilla escogio cual OT (cuando habia varias)
# ======================================================================

async def _visfall_elegir_ot(query, partes, update, context):
    """
    callback_data: visfall_elegir|<asignacion_id>

    Caso: la cuadrilla tenia varias OTs activas y escogio una del menu
    de seleccion. Reusamos la pantalla de confirmacion estandar.

    La razon esta guardada en context.user_data (la guardo el handler
    cuando construyo el menu).
    """
    _, asignacion_id = partes
    asignacion_id = int(asignacion_id)

    razon = (context.user_data or {}).get("visita_fallida_razon", "")
    if not razon:
        await query.edit_message_text(
            text=(
                "⚠️ Se perdio la razon de la visita fallida. "
                "Vuelve a escribir /visita_fallida [razon]."
            ),
        )
        return

    conn = obtener_conexion()
    if conn is None:
        await query.answer(
            text="❌ Error de conexion a BD.",
            show_alert=True,
        )
        return

    try:
        datos = obtener_ot_para_visita_fallida(asignacion_id, conn)
        if datos is None:
            await query.edit_message_text(
                text=f"⚠️ La asignacion {asignacion_id} no existe."
            )
            return

        if datos["visita_fallida"]:
            await query.edit_message_text(
                text=(
                    f"ℹ️ La OT {datos['wonum']} ya fue declarada como "
                    f"visita fallida anteriormente."
                ),
            )
            return

        # Editar el mensaje a la pantalla de confirmacion
        texto = obtener_mensaje(
            "visita_fallida.confirmar",
            wonum=datos["wonum"],
            razon=_escapar_md(razon),
        )

        keyboard = [
            [InlineKeyboardButton(
                text="✅ Sí, declarar fallida",
                callback_data=f"visfall_confirmar|{asignacion_id}",
            )],
            [InlineKeyboardButton(
                text="⬅️ Cancelar",
                callback_data="cancelar_operacion",
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
# RAMA 19 — cuadrilla confirmo la declaracion
# ======================================================================

async def _visfall_confirmar(query, partes, update, context):
    """
    callback_data: visfall_confirmar|<asignacion_id>

    Ejecuta el UPDATE + INSERT worklog, edita el mensaje con el
    resultado final, y limpia user_data.
    """
    _, asignacion_id = partes
    asignacion_id = int(asignacion_id)

    razon = (context.user_data or {}).get("visita_fallida_razon", "")

    conn = obtener_conexion()
    if conn is None:
        await query.answer(
            text="❌ Error de conexion a BD.",
            show_alert=True,
        )
        return

    try:
        datos = obtener_ot_para_visita_fallida(asignacion_id, conn)
        if datos is None:
            await query.edit_message_text(
                text=f"⚠️ La asignacion {asignacion_id} no existe."
            )
            return

        wonum = datos["wonum"]
        cuadrilla_id = datos["cuadrilla_id"]
        cuadrilla_nombre = datos["cuadrilla_nombre"] or cuadrilla_id

        # Si por algun motivo NO hay razon en user_data (ej. la cuadrilla
        # era unica y el handler la paso directo, sin guardarla),
        # intentamos rescatarla del texto del mensaje (entre comillas).
        if not razon:
            texto_actual = query.message.text or ""
            # Buscamos la razon entre comillas dobles del mensaje de
            # confirmacion. Patron: '...registrada: "RAZON" ...'
            import re
            match = re.search(r'"([^"]+)"', texto_actual)
            if match:
                razon = match.group(1)

        if not razon:
            await query.edit_message_text(
                text=(
                    "⚠️ No pude recuperar la razon. "
                    "Vuelve a escribir /visita_fallida [razon]."
                ),
            )
            return

        if datos["visita_fallida"]:
            await query.edit_message_text(
                text=(
                    f"ℹ️ La OT {wonum} ya fue declarada como visita "
                    f"fallida anteriormente."
                ),
            )
            return

        if not datos["asignacion_activa"]:
            await query.edit_message_text(
                text=f"⚠️ La OT {wonum} ya no esta activa."
            )
            return

        # Ejecutar la marcacion
        # OJO: razon SIN _escapar_md (eso es solo para display Markdown).
        # En BD guardamos el texto original que escribio la cuadrilla.
        msg = query.message
        ok = marcar_visita_fallida(
            asignacion_id=asignacion_id,
            razon=razon,
            cuadrilla_id=cuadrilla_id,
            conn=conn,
            telegram_chat_id=update.effective_chat.id,
            telegram_chat_title=(update.effective_chat.title or ""),
            telegram_user_id=update.effective_user.id,
            telegram_username=update.effective_user.username,
            telegram_message_id=msg.message_id if msg else None,
        )
        conn.commit()

        if not ok:
            await query.edit_message_text(
                text=(
                    f"⚠️ No se pudo marcar la OT {wonum}. "
                    f"Probablemente ya estaba declarada."
                ),
            )
            return

        # Mensaje final
        texto = obtener_mensaje(
            "visita_fallida.declarada",
            wonum=wonum,
            razon=_escapar_md(razon),
            cuadrilla_nombre=cuadrilla_nombre,
        )
        try:
            await query.edit_message_text(
                text=texto,
                parse_mode="Markdown",
            )
        except Exception as e:
            logger.warning(
                f"[VisitaFallida] No se pudo editar mensaje: {e}"
            )

        # Limpiar user_data
        if context.user_data is not None:
            context.user_data.pop("visita_fallida_razon", None)

    finally:
        cerrar_conexion(conn)