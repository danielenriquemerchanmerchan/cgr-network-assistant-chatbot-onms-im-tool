"""
bot/services/notificador_ot.py
------------------------------
Job que envia por Telegram las notificaciones de OTs nuevas pendientes.

PROPOSITO:
    Tomar las filas de ot_bandeja con notificacion_coordinador_enviada_at
    IS NULL y enviarles al coordinador correspondiente un mensaje con
    los detalles de la OT y dos botones inline:
        [ ✅ Recibida ]   [ ❌ No es de mi zona ]

NO HACE:
    - No procesa los botones que el coord presione (eso es trabajo de
      callback_botones.py).
    - No asigna OTs nuevas a ot_bandeja (eso lo hace el asignador desde
      el ETL).
    - No tiene su propio scheduler — se registra como JobQueue del bot
      en main.py.

DISENO:
    - Cada ciclo procesa hasta NOTIFICADOR_MAX_POR_CICLO OTs.
    - Entre mensajes pausa NOTIFICADOR_PAUSA_ENTRE_MSG_SEG segundos
      para no saturar Telegram.
    - Tras NOTIFICADOR_MAX_INTENTOS fallidos sobre la misma OT, esta
      deja de aparecer en el SELECT (queda con intentos>=max).
    - Conexion a BD se abre y cierra POR CICLO. Mas simple que mantener
      una conexion viva.

CONTRATO:
    notificar_pendientes(bot) -> dict con stats
        bot: instancia de telegram.Bot ya autenticada (viene del context
             del JobQueue).
        retorna:
            {
                'pendientes_total': N,    # cuantas habia
                'enviadas_ok':       N,   # se mando con exito
                'fallidas':          N,   # Telegram dijo no
                'sin_coord':         N,   # OT sin coord asignado, se salta
                'sin_telegram':      N,   # coord sin telegram_user_id
            }

TEXTO DEL MENSAJE:
    El texto vive en cat_mensaje (codigo 'notificacion.ot_nueva_coordinador').
    Editable desde BD + /admin_recargar. Ver bot/services/cache_catalogos.py.
"""

import logging
import asyncio

from core.config import (
    NOTIFICADOR_MAX_POR_CICLO,
    NOTIFICADOR_MAX_INTENTOS,
    NOTIFICADOR_PAUSA_ENTRE_MSG_SEG,
)
from integrations.postgres.client import obtener_conexion, cerrar_conexion
from bot.services.cache_catalogos import obtener_mensaje

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError, Forbidden, BadRequest

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# CONSTANTES
# ═══════════════════════════════════════════════════════════════════

# Codigo del mensaje en cat_mensaje. Editable desde BD.
CODIGO_MSG_NOTIFICACION = "notificacion.ot_nueva_coordinador"

# Como se muestran los campos NULL en el mensaje.
PLACEHOLDER_NULL = "—"

# Maximo de caracteres para el campo description. Si es mas largo se trunca.
MAX_LARGO_DESCRIPTION = 200


# ═══════════════════════════════════════════════════════════════════
# 1. SELECT DE PENDIENTES (con JOIN para traer todo lo necesario)
# ═══════════════════════════════════════════════════════════════════

def _listar_pendientes(conn, limite):
    """
    Devuelve hasta `limite` filas de ot_bandeja pendientes de notificar.
    Hace JOIN con work_orders y coordinadores_contratista para traer en
    una sola query todo lo necesario para el mensaje.

    Filtros:
        - notificacion_coordinador_enviada_at IS NULL  (no se ha notificado)
        - notificacion_coordinador_intentos < MAX      (no agotada de intentos)
        - asignacion_activa = true                     (asignacion vigente)

    Retorna:
        list[dict] con campos: asignacion_id, wonum, coordinador_id,
        telegram_user_id, y todos los campos de la OT que se usan en
        el mensaje.

    Si una OT tiene coord sin telegram_user_id, igual se trae — el
    llamador decide saltarla (incrementando intentos) y loguear.
    """
    sql = """
        SELECT
            ob.asignacion_id,
            ob.wonum,
            ob.coordinador_asignado_id,
            ob.notificacion_coordinador_intentos,
            co.telegram_user_id,
            -- Campos de la OT para el mensaje
            wo.worktype,
            wo.departamento,
            wo.ciudad,
            wo.operador_fo,
            wo.severity,
            wo.creation_date,
            wo.description,
            wo.cinum,
            wo.tipo_tramo,
            wo.direccion
        FROM onms.ot_bandeja ob
        LEFT JOIN onms.coordinadores_contratista co
               ON ob.coordinador_asignado_id = co.coordinador_id
        LEFT JOIN onms.work_orders wo
               ON ob.wonum = wo.wonum
        WHERE ob.notificacion_coordinador_enviada_at IS NULL
          AND ob.notificacion_coordinador_intentos < %s
          AND ob.asignacion_activa = true
        ORDER BY ob.fecha_asignacion_cgr ASC
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (NOTIFICADOR_MAX_INTENTOS, limite))
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ═══════════════════════════════════════════════════════════════════
# 2. UPDATES POST-ENVIO
# ═══════════════════════════════════════════════════════════════════

def _marcar_enviada(asignacion_id, conn):
    """Marca la OT como notificada exitosamente: enviada_at = NOW()."""
    sql = """
        UPDATE onms.ot_bandeja
           SET notificacion_coordinador_enviada_at = NOW()
         WHERE asignacion_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (asignacion_id,))


def _incrementar_intentos(asignacion_id, conn):
    """
    Incrementa el contador de intentos. Cuando alcance NOTIFICADOR_MAX_INTENTOS,
    la OT deja de aparecer en _listar_pendientes() (porque el filtro es
    intentos < max).
    """
    sql = """
        UPDATE onms.ot_bandeja
           SET notificacion_coordinador_intentos =
               notificacion_coordinador_intentos + 1
         WHERE asignacion_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (asignacion_id,))


# ═══════════════════════════════════════════════════════════════════
# 3. ARMADO DEL MENSAJE
# ═══════════════════════════════════════════════════════════════════

def _normalizar_campo(valor):
    """
    Convierte NULL/None y strings vacios al placeholder definido.
    Pasa el resto tal cual.
    """
    if valor is None:
        return PLACEHOLDER_NULL
    if isinstance(valor, str) and valor.strip() == "":
        return PLACEHOLDER_NULL
    return valor


def _formatear_fecha(dt):
    """
    Formatea creation_date a 'YYYY-MM-DD HH:MM'. Si es None devuelve
    el placeholder.
    """
    if dt is None:
        return PLACEHOLDER_NULL
    try:
        return dt.strftime("%Y-%m-%d %H:%M")
    except AttributeError:
        return str(dt)


def _truncar_description(texto):
    """
    Trunca description a MAX_LARGO_DESCRIPTION chars, agregando '…' si
    se trunco. Devuelve placeholder si es None/vacio.
    """
    if texto is None or (isinstance(texto, str) and texto.strip() == ""):
        return PLACEHOLDER_NULL
    texto = str(texto).strip()
    if len(texto) > MAX_LARGO_DESCRIPTION:
        return texto[:MAX_LARGO_DESCRIPTION].rstrip() + "…"
    return texto


def _construir_texto_mensaje(fila):
    """
    Toma un dict de _listar_pendientes() y devuelve el texto formateado
    listo para enviar por Telegram (con Markdown).

    Llama a obtener_mensaje() del cache de catalogos. Si el codigo
    'notificacion.ot_nueva_coordinador' no existe en cat_mensaje, el
    propio cache devuelve un fallback visible — no rompe el bot.
    """
    variables = {
        "wonum":          _normalizar_campo(fila["wonum"]),
        "worktype":       _normalizar_campo(fila["worktype"]),
        "departamento":   _normalizar_campo(fila["departamento"]),
        "ciudad":         _normalizar_campo(fila["ciudad"]),
        "operador_fo":    _normalizar_campo(fila["operador_fo"]),
        "severity":       _normalizar_campo(fila["severity"]),
        "creation_date":  _formatear_fecha(fila["creation_date"]),
        "description":    _truncar_description(fila["description"]),
        "cinum":          _normalizar_campo(fila["cinum"]),
        "tipo_tramo":     _normalizar_campo(fila["tipo_tramo"]),
        "direccion":      _normalizar_campo(fila["direccion"]),
    }
    return obtener_mensaje(CODIGO_MSG_NOTIFICACION, **variables)


def _construir_botones(asignacion_id):
    """
    Construye el InlineKeyboardMarkup con los 2 botones.

    callback_data:
        ot_recibida|<asignacion_id>     -> coord acepta la OT
        ot_rechazada|<asignacion_id>    -> coord dice 'no es de mi zona'

    El '|' como separador es la convencion del proyecto (ya usada en
    callback_botones.py para los flujos de bloqueo).
    """
    keyboard = [[
        InlineKeyboardButton(
            text="✅ Recibida",
            callback_data=f"ot_recibida|{asignacion_id}",
        ),
        InlineKeyboardButton(
            text="❌ No es de mi zona",
            callback_data=f"ot_rechazada|{asignacion_id}",
        ),
    ]]
    return InlineKeyboardMarkup(keyboard)


# ═══════════════════════════════════════════════════════════════════
# 4. ENVIO INDIVIDUAL
# ═══════════════════════════════════════════════════════════════════

async def _enviar_a_coord(bot, fila):
    """
    Envia el mensaje al coordinador via Telegram.

    Retorna:
        ('ok', None)              -> envio exitoso
        ('sin_telegram', None)    -> coord no tiene telegram_user_id
        ('forbidden', detalle)    -> coord nunca abrio chat con el bot
        ('error', detalle)        -> otro error de Telegram

    No actualiza BD — el llamador decide que hacer con el resultado.
    """
    telegram_user_id = fila["telegram_user_id"]
    if telegram_user_id is None:
        return ("sin_telegram", None)

    texto = _construir_texto_mensaje(fila)
    botones = _construir_botones(fila["asignacion_id"])

    try:
        await bot.send_message(
            chat_id=telegram_user_id,
            text=texto,
            reply_markup=botones,
            parse_mode="Markdown",
        )
        return ("ok", None)
    except Forbidden as e:
        # Coord nunca inicio chat con el bot, o lo bloqueo.
        return ("forbidden", str(e))
    except BadRequest as e:
        # Markdown mal formado, mensaje muy largo, etc.
        return ("error", f"BadRequest: {e}")
    except TelegramError as e:
        return ("error", f"TelegramError: {e}")


# ═══════════════════════════════════════════════════════════════════
# 5. ORQUESTADOR PUBLICO
# ═══════════════════════════════════════════════════════════════════

async def notificar_pendientes(bot):
    """
    Ciclo del notificador. Lo invoca el JobQueue del bot cada
    NOTIFICADOR_INTERVALO_SEG segundos.

    Argumentos:
        bot: instancia de telegram.Bot (viene de context.bot del job).

    Retorna dict con conteos para logging.
    """
    stats = {
        "pendientes_total": 0,
        "enviadas_ok":      0,
        "fallidas":         0,
        "sin_coord":        0,
        "sin_telegram":     0,
    }

    conn = obtener_conexion()
    if conn is None:
        logger.error("[Notificador] No se pudo abrir conexion a Postgres")
        return stats

    try:
        pendientes = _listar_pendientes(conn, NOTIFICADOR_MAX_POR_CICLO)
        stats["pendientes_total"] = len(pendientes)

        if not pendientes:
            return stats  # nada que hacer este ciclo

        logger.info(
            f"[Notificador] {len(pendientes)} OT(s) pendiente(s) "
            f"de notificar (max este ciclo: {NOTIFICADOR_MAX_POR_CICLO})"
        )

        for i, fila in enumerate(pendientes):
            asignacion_id = fila["asignacion_id"]
            wonum = fila["wonum"]
            coord_id = fila["coordinador_asignado_id"]

            # Caso: OT sin coord asignado (NULL). El asignador la dejo
            # asi por la politica acordada. Incrementamos intentos y
            # seguimos.
            if coord_id is None:
                logger.warning(
                    f"[Notificador] OT {wonum} (asig={asignacion_id}) "
                    f"sin coordinador asignado. Salto e incremento intentos."
                )
                _incrementar_intentos(asignacion_id, conn)
                conn.commit()
                stats["sin_coord"] += 1
                continue

            # Enviar mensaje
            resultado, detalle = await _enviar_a_coord(bot, fila)

            if resultado == "ok":
                _marcar_enviada(asignacion_id, conn)
                conn.commit()
                stats["enviadas_ok"] += 1
                logger.info(
                    f"[Notificador] OT {wonum} -> coord {coord_id} "
                    f"(asig={asignacion_id}) ✓"
                )

            elif resultado == "sin_telegram":
                _incrementar_intentos(asignacion_id, conn)
                conn.commit()
                stats["sin_telegram"] += 1
                logger.warning(
                    f"[Notificador] OT {wonum} - coord {coord_id} no "
                    f"tiene telegram_user_id. No se puede notificar."
                )

            elif resultado == "forbidden":
                _incrementar_intentos(asignacion_id, conn)
                conn.commit()
                stats["fallidas"] += 1
                logger.error(
                    f"[Notificador] OT {wonum} - coord {coord_id} bloqueo "
                    f"al bot o nunca lo inicio. Detalle: {detalle}"
                )

            else:  # 'error'
                _incrementar_intentos(asignacion_id, conn)
                conn.commit()
                stats["fallidas"] += 1
                logger.error(
                    f"[Notificador] OT {wonum} - falla envio Telegram. "
                    f"Detalle: {detalle}"
                )

            # Pausa entre mensajes (excepto despues del ultimo)
            if i < len(pendientes) - 1:
                await asyncio.sleep(NOTIFICADOR_PAUSA_ENTRE_MSG_SEG)

        logger.info(
            f"[Notificador] Ciclo cerrado: "
            f"total={stats['pendientes_total']} | "
            f"ok={stats['enviadas_ok']} | "
            f"sin_coord={stats['sin_coord']} | "
            f"sin_telegram={stats['sin_telegram']} | "
            f"fallidas={stats['fallidas']}"
        )

    finally:
        cerrar_conexion(conn)

    return stats


# ═══════════════════════════════════════════════════════════════════
# 6. CALLBACK PARA EL JOBQUEUE
# ═══════════════════════════════════════════════════════════════════

async def notificar_pendientes_job(context):
    """
    Callback que ejecuta el JobQueue de python-telegram-bot.
    El JobQueue le pasa un context con .bot ya autenticado.

    Esta funcion es la que se registra en main.py.
    """
    try:
        await notificar_pendientes(context.bot)
    except Exception as e:
        logger.exception(f"[Notificador] Error inesperado en el job: {e}")