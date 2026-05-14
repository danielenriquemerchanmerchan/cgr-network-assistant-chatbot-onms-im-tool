"""
bot/handlers/reportar.py
------------------------
Handler del comando /reportar.

/reportar es el punto único de entrada del Momento 2 (Seguimiento).
La cuadrilla escribe /reportar y el bot le muestra un menú dinámico
con las acciones disponibles según la fase actual de su OT activa.

DISEÑO:
- Solo aplica a la OT que la cuadrilla tiene marcada como activa
  (Mecanismo A). Si no hay activa, el bot orienta a usar /bandeja.
- Solo funciona en grupos de cuadrilla. Chat privado o canal: mensaje
  informativo.
- El menú principal muestra dos secciones:
    a) Avance libre (siempre disponible).
    b) Ver más opciones (botón que expande las acciones específicas
       de la fase actual y el resto).

CAPTURA DE TEXTO LIBRE EN GRUPO:
- Usa Patrón 1 (privacy OFF + filtro por user_id).
- Cuando la cuadrilla pulsa [✏️ Avance libre], el bot guarda en
  `context.chat_data` una conversación activa con: user_id (quien
  toco), wonum, asignacion_id, timestamp de inicio.
- Solo el user_id que toco el boton puede responder.
- Los mensajes de otros miembros del grupo se ignoran.
- A los 3 min sin respuesta, el bot envia un recordatorio.
- A los 5 min, la conversacion se cierra automaticamente.

CALLBACKS QUE EXPONE:
    rep_avance_libre     -> iniciar captura de avance libre.
    rep_ver_mas          -> expandir menú con todas las opciones
                            según fase (stub por ahora).
"""

import logging
from datetime import datetime, timezone

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from bot.services.cuadrillas import identificar_cuadrilla_por_chat_id
from bot.services.ot_activa import obtener_ot_activa
from bot.services.interacciones import registrar_interaccion
from bot.services.fases import cambiar_fase, asegurar_estado_en_progreso
from integrations.postgres.client import obtener_conexion, cerrar_conexion

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# Helpers de formato (compartidos con bandeja)
# ═══════════════════════════════════════════════════════════════════

def _norm(valor):
    if valor is None:
        return "—"
    if isinstance(valor, str) and valor.strip() == "":
        return "—"
    return valor


def _abreviar_departamento(depto):
    if depto is None or not isinstance(depto, str) or not depto.strip():
        return "—"
    palabras = depto.strip().upper().split()
    saltar = {"LA", "EL", "LOS", "LAS", "DE", "DEL"}
    for palabra in palabras:
        if palabra not in saltar:
            return palabra
    return palabras[0]


def _primer_nombre(user):
    """
    Devuelve el primer nombre o username del usuario para personalizar
    mensajes ('Listo Pedro, ...').
    """
    if user is None:
        return "Listo"
    if user.first_name:
        return user.first_name.split()[0]
    if user.username:
        return f"@{user.username}"
    return "Listo"


def _escapar_md(texto):
    """
    Escapa caracteres reservados de Markdown V1 para que texto del
    usuario (nombres, usernames, descripciones) no rompa el formato
    del mensaje al renderizar.
    """
    if not texto:
        return ""
    s = str(texto)
    for ch in ("\\", "*", "_", "`", "[", "]"):
        s = s.replace(ch, "\\" + ch)
    return s


# ═══════════════════════════════════════════════════════════════════
# ENTRY POINT — Comando /reportar
# ═══════════════════════════════════════════════════════════════════

async def handle(update, context):
    """
    Handler del comando /reportar.

    Flujo:
        1. Validar que sea un grupo (cuadrilla).
        2. Identificar la cuadrilla.
        3. Leer la OT activa de la cuadrilla.
        4. Si no hay activa, orientar a /bandeja.
        5. Si hay activa, mostrar menú dinámico según fase.
    """
    chat_type = update.effective_chat.type
    chat_id = update.effective_chat.id
    chat_title = update.effective_chat.title or ""
    user = update.effective_user

    logger.info(
        f"/reportar recibido en chat {chat_id} ({chat_type})"
    )

    if chat_type not in ("group", "supergroup"):
        await update.message.reply_text(
            "ℹ️ El comando /reportar solo funciona en grupos de "
            "cuadrilla."
        )
        return

    cuadrilla = identificar_cuadrilla_por_chat_id(chat_id)
    if cuadrilla is None:
        await update.message.reply_text(
            f"⚠️ Este grupo no esta registrado.\n"
            f"`chat_id = {chat_id}`",
            parse_mode="Markdown",
        )
        return

    conn = obtener_conexion()
    if conn is None:
        await update.message.reply_text(
            "❌ Error de conexion a BD. Intenta de nuevo en unos minutos."
        )
        return

    try:
        ot_activa = obtener_ot_activa(cuadrilla["cuadrilla_id"], conn)
    finally:
        cerrar_conexion(conn)

    if ot_activa is None:
        await update.message.reply_text(
            "ℹ️ No tienes una OT activa todavía.\n\n"
            "Escribe /bandeja para ver tus OTs y activar una. "
            "Los reportes siempre se hacen sobre la OT activa.",
            parse_mode="Markdown",
        )
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
            contenido_texto="/reportar",
            metadata={"comando": "/reportar", "resultado": "sin_ot_activa"},
        )
        return

    texto = _construir_texto_menu(ot_activa)
    reply_markup = _construir_botones_menu(ot_activa)

    await update.message.reply_text(
        texto,
        parse_mode="Markdown",
        reply_markup=reply_markup,
    )

    registrar_interaccion(
        tipo_interaccion="mensaje_libre",
        direccion="entrante",
        actor_tipo="cuadrilla",
        actor_id=cuadrilla["cuadrilla_id"],
        cuadrilla_id=cuadrilla["cuadrilla_id"],
        wonum=ot_activa["wonum"],
        asignacion_id=ot_activa["asignacion_id"],
        telegram_chat_id=chat_id,
        telegram_chat_title=chat_title,
        telegram_message_id=update.message.message_id,
        telegram_user_id=user.id,
        telegram_username=user.username,
        contenido_texto="/reportar",
        metadata={
            "comando": "/reportar",
            "fase_actual": ot_activa.get("fase_operativa"),
            "wonum": ot_activa["wonum"],
        },
    )

    logger.info(
        f"/reportar mostrado a {cuadrilla['cuadrilla_id']} para "
        f"OT {ot_activa['wonum']} (fase: "
        f"{ot_activa.get('fase_operativa')})"
    )


# ═══════════════════════════════════════════════════════════════════
# CONSTRUCCION DEL MENU
# ═══════════════════════════════════════════════════════════════════

def _construir_texto_menu(ot):
    fase_desc = _norm(ot.get("fase_descripcion"))
    return (
        f"📋 *Reportar para OT {_norm(ot['wonum'])}*\n"
        f"{_norm(ot.get('worktype'))} · "
        f"Sev {_norm(ot.get('severity'))} · "
        f"{_abreviar_departamento(ot.get('departamento'))}/"
        f"{_norm(ot.get('ciudad'))}\n\n"
        f"Estás en fase: *{fase_desc}*\n\n"
        f"¿Qué reportas?"
    )


def _construir_botones_menu(ot):
    keyboard = [
        [InlineKeyboardButton(
            text="✏️ Avance libre",
            callback_data="rep_avance_libre",
        )],
        [InlineKeyboardButton(
            text="▾ Ver más opciones",
            callback_data="rep_ver_mas",
        )],
    ]
    return InlineKeyboardMarkup(keyboard)


# ═══════════════════════════════════════════════════════════════════
# CALLBACK — Iniciar captura de avance libre
# ═══════════════════════════════════════════════════════════════════

# Constantes de timeout
RECORDATORIO_SEGUNDOS = 180   # 3 min
TIMEOUT_SEGUNDOS = 300        # 5 min


async def callback_avance_libre(query, update, context):
    """
    callback_data: rep_avance_libre

    Inicia la captura de texto libre:
    1. Valida que la cuadrilla siga teniendo OT activa.
    2. Guarda en context.chat_data['avance_libre_pendiente'] la info
       de quien debe responder.
    3. Edita el mensaje del menu con instrucciones claras.
    4. Programa dos jobs: recordatorio (3min) y timeout (5min).
    """
    chat_id = query.message.chat.id
    chat_title = query.message.chat.title or ""
    user = update.effective_user

    # Validar cuadrilla y OT activa
    cuadrilla = identificar_cuadrilla_por_chat_id(chat_id)
    if cuadrilla is None:
        await query.edit_message_text(
            text="⚠️ Este grupo ya no esta registrado."
        )
        return

    conn = obtener_conexion()
    if conn is None:
        await query.edit_message_text(
            text="❌ Error de conexion a BD. Intenta de nuevo."
        )
        return
    try:
        ot_activa = obtener_ot_activa(cuadrilla["cuadrilla_id"], conn)
    finally:
        cerrar_conexion(conn)

    if ot_activa is None:
        await query.edit_message_text(
            text=(
                "⚠️ Ya no tienes una OT activa.\n\n"
                "Escribe /bandeja para activar una."
            ),
        )
        return

    # Si ya hay una conversacion de avance libre activa, avisamos
    # (no rompemos la anterior; se sobreescribe).
    pendiente_anterior = (context.chat_data or {}).get("avance_libre_pendiente")
    if pendiente_anterior:
        logger.info(
            f"[AvanceLibre] Sobrescribiendo conversacion previa en "
            f"chat {chat_id} (usuario anterior: "
            f"{pendiente_anterior.get('user_id')})"
        )
        # Cancelar jobs anteriores si existen
        _cancelar_jobs_pendientes(context, chat_id)

    # Guardar la conversacion en chat_data
    if context.chat_data is None:
        context.chat_data = {}
    context.chat_data["avance_libre_pendiente"] = {
        "user_id": user.id,
        "username": user.username,
        "primer_nombre": _primer_nombre(user),
        "wonum": ot_activa["wonum"],
        "asignacion_id": ot_activa["asignacion_id"],
        "cuadrilla_id": cuadrilla["cuadrilla_id"],
        "iniciado_en": datetime.now(timezone.utc).isoformat(),
    }

    # Editar el mensaje del menu con instrucciones
    nombre = _primer_nombre(user)
    nombre_md = _escapar_md(nombre)
    await query.edit_message_text(
        text=(
            f"✏️ *Avance libre — OT {_norm(ot_activa['wonum'])}*\n\n"
            f"Listo {nombre_md}, escribe tu avance cuando puedas.\n\n"
            f"_No te apures aunque se atraviesen otros mensajes en el "
            f"grupo. Tienes 5 minutos._"
        ),
        parse_mode="Markdown",
    )

    # Programar jobs de recordatorio y timeout
    jq = context.application.job_queue
    if jq is None:
        logger.warning(
            "[AvanceLibre] JobQueue no disponible; no se programan "
            "recordatorio ni timeout. La conversacion no se "
            "auto-cerrara."
        )
    else:
        jq.run_once(
            _job_recordatorio,
            when=RECORDATORIO_SEGUNDOS,
            chat_id=chat_id,
            name=f"avance_libre_recordatorio_{chat_id}",
            data={"user_id": user.id, "nombre": nombre},
        )
        jq.run_once(
            _job_timeout,
            when=TIMEOUT_SEGUNDOS,
            chat_id=chat_id,
            name=f"avance_libre_timeout_{chat_id}",
            data={"user_id": user.id, "nombre": nombre},
        )

    logger.info(
        f"[AvanceLibre] Iniciada captura en chat {chat_id} para "
        f"user {user.id} ({nombre}), OT {ot_activa['wonum']}"
    )


# ═══════════════════════════════════════════════════════════════════
# MESSAGE HANDLER — Recibir texto libre del usuario que toco el boton
# ═══════════════════════════════════════════════════════════════════

async def recibir_texto_avance_libre(update, context):
    """
    Handler de mensajes de texto en grupos. Filtra:
    - Solo procesa si existe conversacion 'avance_libre_pendiente'
      en chat_data.
    - Solo procesa si el user_id que escribe es el que inicio la
      conversacion.
    - Ignora silenciosamente todo lo demas (chat libre normal en
      el grupo).

    Esto se conecta en main.py como MessageHandler con
    filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND.
    """
    pendiente = (context.chat_data or {}).get("avance_libre_pendiente")
    if not pendiente:
        return  # No hay conversacion activa, ignorar

    user = update.effective_user
    if user is None or user.id != pendiente["user_id"]:
        return  # No es el usuario que inicio, ignorar

    texto_avance = (update.message.text or "").strip()
    if not texto_avance:
        return  # Mensaje vacio (raro), ignorar

    chat_id = update.effective_chat.id
    chat_title = update.effective_chat.title or ""
    wonum = pendiente["wonum"]
    asignacion_id = pendiente["asignacion_id"]
    cuadrilla_id = pendiente["cuadrilla_id"]

    # Cancelar los jobs (recordatorio y timeout) — ya respondio
    _cancelar_jobs_pendientes(context, chat_id)

    # Limpiar conversacion del chat_data
    context.chat_data.pop("avance_libre_pendiente", None)

    # Registrar la interaccion en BD
    registrar_interaccion(
        tipo_interaccion="avance_libre",
        direccion="entrante",
        actor_tipo="cuadrilla",
        actor_id=cuadrilla_id,
        cuadrilla_id=cuadrilla_id,
        wonum=wonum,
        asignacion_id=asignacion_id,
        telegram_chat_id=chat_id,
        telegram_chat_title=chat_title,
        telegram_message_id=update.message.message_id,
        telegram_user_id=user.id,
        telegram_username=user.username,
        contenido_texto=texto_avance,
        metadata={
            "comando": "/reportar > avance_libre",
            "wonum": wonum,
        },
    )

    # Confirmar al usuario (mensaje corto, no invasivo)
    await update.message.reply_text(
        text=(
            f"✅ Avance registrado para OT *{_norm(wonum)}*."
        ),
        parse_mode="Markdown",
    )

    logger.info(
        f"[AvanceLibre] Avance registrado para cuadrilla "
        f"{cuadrilla_id}, OT {wonum} (user {user.id}): "
        f"{texto_avance[:80]}{'...' if len(texto_avance) > 80 else ''}"
    )


# ═══════════════════════════════════════════════════════════════════
# JOBS — recordatorio y timeout
# ═══════════════════════════════════════════════════════════════════

async def _job_recordatorio(context):
    """
    Se ejecuta a los 3 minutos. Si la conversacion sigue pendiente,
    envia un recordatorio al grupo.
    """
    job = context.job
    chat_id = job.chat_id
    user_id = job.data.get("user_id")
    nombre = job.data.get("nombre", "")

    pendiente = (context.chat_data or {}).get("avance_libre_pendiente")
    if not pendiente or pendiente.get("user_id") != user_id:
        # Ya se cerro la conversacion (respondio o fue sobreescrita)
        return

    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"⏰ {_escapar_md(nombre)}, sigo esperando tu avance.\n"
                f"_Quedan 2 minutos._"
            ),
            parse_mode="Markdown",
        )
        logger.info(
            f"[AvanceLibre] Recordatorio enviado en chat {chat_id} "
            f"para user {user_id}"
        )
    except Exception as e:
        logger.warning(
            f"[AvanceLibre] No se pudo enviar recordatorio en "
            f"chat {chat_id}: {e}"
        )


async def _job_timeout(context):
    """
    Se ejecuta a los 5 minutos. Si la conversacion sigue pendiente,
    la cierra y avisa.
    """
    job = context.job
    chat_id = job.chat_id
    user_id = job.data.get("user_id")
    nombre = job.data.get("nombre", "")

    pendiente = (context.chat_data or {}).get("avance_libre_pendiente")
    if not pendiente or pendiente.get("user_id") != user_id:
        # Ya se cerro la conversacion
        return

    # Cerrar
    context.chat_data.pop("avance_libre_pendiente", None)

    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"⏱️ {nombre}, cerré la espera porque pasó mucho "
                f"tiempo. Si todavía quieres reportar tu avance, "
                f"toca /reportar otra vez."
            ),
        )
        logger.info(
            f"[AvanceLibre] Timeout aplicado en chat {chat_id} "
            f"para user {user_id}"
        )
    except Exception as e:
        logger.warning(
            f"[AvanceLibre] No se pudo enviar mensaje de timeout en "
            f"chat {chat_id}: {e}"
        )


def _cancelar_jobs_pendientes(context, chat_id):
    """
    Cancela los jobs de recordatorio y timeout que esten programados
    para este chat. Util cuando la conversacion termino exitosamente
    o cuando se sobreescribe.
    """
    jq = context.application.job_queue
    if jq is None:
        return
    for nombre_job in (
        f"avance_libre_recordatorio_{chat_id}",
        f"avance_libre_timeout_{chat_id}",
    ):
        for job in jq.get_jobs_by_name(nombre_job):
            job.schedule_removal()


# ═══════════════════════════════════════════════════════════════════
# CALLBACK — Ver más opciones (menu expandido)
# ═══════════════════════════════════════════════════════════════════

async def callback_ver_mas(query, update, context):
    """
    callback_data: rep_ver_mas

    Expande el menu con todas las acciones disponibles para reportar.
    Por ahora solo tiene /en_desplazamiento. Conforme codeemos cada
    comando del Momento 2, vamos agregando botones aqui.
    """
    chat_id = query.message.chat.id

    # Validar que sigue habiendo OT activa
    cuadrilla = identificar_cuadrilla_por_chat_id(chat_id)
    if cuadrilla is None:
        await query.edit_message_text(
            text="⚠️ Este grupo ya no esta registrado."
        )
        return

    conn = obtener_conexion()
    if conn is None:
        await query.edit_message_text(
            text="❌ Error de conexion a BD. Intenta de nuevo."
        )
        return
    try:
        ot_activa = obtener_ot_activa(cuadrilla["cuadrilla_id"], conn)
    finally:
        cerrar_conexion(conn)

    if ot_activa is None:
        await query.edit_message_text(
            text=(
                "⚠️ Ya no tienes una OT activa.\n\n"
                "Escribe /bandeja para activar una."
            ),
        )
        return

    texto = (
        f"📋 *Reportar para OT {_norm(ot_activa['wonum'])}*\n\n"
        f"Selecciona la acción:"
    )

    keyboard = [
        [InlineKeyboardButton(
            text="🚐 En desplazamiento",
            callback_data="rep_en_desplazamiento",
        )],
        [InlineKeyboardButton(
            text="📍 Ya llegamos a sitio",
            callback_data="rep_ya_llegamos_a_sitio",
        )],
        [InlineKeyboardButton(
            text="📡 Midiendo",
            callback_data="rep_midiendo",
        )],
        [InlineKeyboardButton(
            text="✏️ Avance libre",
            callback_data="rep_avance_libre",
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


# ═══════════════════════════════════════════════════════════════════
# CALLBACK — /en_desplazamiento (tipo A, cambia fase)
# ═══════════════════════════════════════════════════════════════════

async def callback_en_desplazamiento(query, update, context):
    """
    callback_data: rep_en_desplazamiento

    Cambia la fase de la OT activa a 'desplazamiento' y registra
    la interaccion 'fase_desplazamiento'.
    """
    await _ejecutar_cambio_fase(
        query=query,
        update=update,
        context=context,
        nueva_fase="desplazamiento",
        tipo_interaccion="fase_desplazamiento",
        contenido_texto="En desplazamiento",
        emoji="🚐",
        texto_confirmacion="En desplazamiento al sitio",
        nombre_comando="en_desplazamiento",
    )


# ═══════════════════════════════════════════════════════════════════
# CALLBACK — /ya_llegamos_a_sitio (tipo A, cambia fase)
# ═══════════════════════════════════════════════════════════════════

async def callback_ya_llegamos_a_sitio(query, update, context):
    """
    callback_data: rep_ya_llegamos_a_sitio

    Cambia la fase de la OT activa a 'en_sitio' y registra
    la interaccion 'fase_llegada'.
    """
    await _ejecutar_cambio_fase(
        query=query,
        update=update,
        context=context,
        nueva_fase="en_sitio",
        tipo_interaccion="fase_llegada",
        contenido_texto="Llegamos a sitio",
        emoji="📍",
        texto_confirmacion="Cuadrilla en sitio",
        nombre_comando="ya_llegamos_a_sitio",
    )


# ═══════════════════════════════════════════════════════════════════
# CALLBACK — /midiendo (tipo A, cambia fase)
# ═══════════════════════════════════════════════════════════════════

async def callback_midiendo(query, update, context):
    """
    callback_data: rep_midiendo

    Cambia la fase de la OT activa a 'midiendo' y registra
    la interaccion 'fase_midiendo'.
    """
    await _ejecutar_cambio_fase(
        query=query,
        update=update,
        context=context,
        nueva_fase="midiendo",
        tipo_interaccion="fase_midiendo",
        contenido_texto="Midiendo",
        emoji="📡",
        texto_confirmacion="Midiendo",
        nombre_comando="midiendo",
    )


# ═══════════════════════════════════════════════════════════════════
# HELPER — Ejecutar cambio de fase + registrar interaccion + confirmar
# ═══════════════════════════════════════════════════════════════════

async def _ejecutar_cambio_fase(
    query, update, context,
    nueva_fase,
    tipo_interaccion,
    contenido_texto,
    emoji,
    texto_confirmacion,
    nombre_comando,
):
    """
    Logica comun para comandos tipo A del Momento 2 (los que cambian
    fase). Reutilizada por en_desplazamiento, ya_llegamos_a_sitio, y
    todos los proximos comandos del flujo normal.

    Args:
        nueva_fase: codigo de la fase nueva en cat_fase_operativa.
        tipo_interaccion: codigo del tipo de interaccion a registrar.
        contenido_texto: texto que va al campo contenido_texto de
                         bot_interacciones (lo que aparece en logs y
                         reportes). Lenguaje corto.
        emoji: emoji para la confirmacion visual.
        texto_confirmacion: texto descriptivo despues del wonum
                            (ej: "En desplazamiento al sitio").
        nombre_comando: usado en metadata y logs (ej: "en_desplazamiento").
    """
    chat_id = query.message.chat.id
    chat_title = query.message.chat.title or ""
    user = update.effective_user

    cuadrilla = identificar_cuadrilla_por_chat_id(chat_id)
    if cuadrilla is None:
        await query.edit_message_text(
            text="⚠️ Este grupo ya no esta registrado."
        )
        return

    conn = obtener_conexion()
    if conn is None:
        await query.edit_message_text(
            text="❌ Error de conexion a BD. Intenta de nuevo."
        )
        return

    try:
        # Releer OT activa por si cambio entre que abrio el menu
        # y toco el boton
        ot_activa = obtener_ot_activa(cuadrilla["cuadrilla_id"], conn)
        if ot_activa is None:
            await query.edit_message_text(
                text=(
                    "⚠️ Ya no tienes una OT activa.\n\n"
                    "Escribe /bandeja para activar una."
                ),
            )
            return

        fase_anterior = ot_activa.get("fase_operativa")

        # Ejecutar cambio de fase
        ok = cambiar_fase(
            asignacion_id=ot_activa["asignacion_id"],
            nueva_fase=nueva_fase,
            conn=conn,
        )

        if not ok:
            conn.rollback()
            await query.edit_message_text(
                text="❌ No se pudo actualizar la fase. Intenta de nuevo."
            )
            return

        # Si la OT estaba en estado 'aceptada' (no habia empezado),
        # ahora paso a 'en_progreso'. Es idempotente; si ya estaba en
        # otro estado, no toca nada.
        paso_a_en_progreso = asegurar_estado_en_progreso(
            asignacion_id=ot_activa["asignacion_id"],
            conn=conn,
        )

        # Registrar la interaccion
        metadata = {
            "comando": f"/reportar > {nombre_comando}",
            "fase_anterior": fase_anterior,
            "fase_nueva": nueva_fase,
            "wonum": ot_activa["wonum"],
        }
        if paso_a_en_progreso:
            metadata["estado_anterior"] = "aceptada"
            metadata["estado_nuevo"] = "en_progreso"

        registrar_interaccion(
            tipo_interaccion=tipo_interaccion,
            direccion="entrante",
            actor_tipo="cuadrilla",
            actor_id=cuadrilla["cuadrilla_id"],
            cuadrilla_id=cuadrilla["cuadrilla_id"],
            wonum=ot_activa["wonum"],
            asignacion_id=ot_activa["asignacion_id"],
            telegram_chat_id=chat_id,
            telegram_chat_title=chat_title,
            telegram_message_id=query.message.message_id,
            telegram_user_id=user.id,
            telegram_username=user.username,
            contenido_texto=contenido_texto,
            metadata=metadata,
        )

        conn.commit()

        # Confirmar al usuario
        await query.edit_message_text(
            text=(
                f"{emoji} *OT {_norm(ot_activa['wonum'])}* — "
                f"{texto_confirmacion}"
            ),
            parse_mode="Markdown",
        )

        logger.info(
            f"[{nombre_comando}] Cuadrilla {cuadrilla['cuadrilla_id']} "
            f"-> fase '{nueva_fase}' en OT {ot_activa['wonum']}"
            f"{' (paso a en_progreso)' if paso_a_en_progreso else ''}"
        )

    except Exception as e:
        conn.rollback()
        logger.error(
            f"[{nombre_comando}] Error en chat {chat_id}: {e}",
            exc_info=True,
        )
        try:
            await query.edit_message_text(
                text="❌ Error al actualizar la fase. Revisa los logs."
            )
        except Exception:
            pass
    finally:
        cerrar_conexion(conn)