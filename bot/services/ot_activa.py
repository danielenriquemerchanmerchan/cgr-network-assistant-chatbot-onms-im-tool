"""
bot/services/ot_activa.py
-------------------------
Servicio para gestionar la OT activa de cada cuadrilla.

CONCEPTO:
    Una cuadrilla puede tener varias OTs en bandeja, pero solo trabaja
    sobre UNA a la vez (la OT activa). Los comandos operativos
    (/midiendo, /cierre, etc.) actuan sobre esa OT activa.

    La cuadrilla escoge cual es la OT activa desde /bandeja con el
    boton [▶️ Trabajar esta OT]. El handler/callback que maneja ese
    boton llama a las funciones de este servicio.

MODELO TRANSACCIONAL (Modelo B — caller dueno de la transaccion):
    Las funciones reciben conn y NO commitean. El handler decide
    cuando commitear, normalmente despues de registrar la
    interaccion en bot_interacciones. Consistente con el resto de
    servicios del mundo handler (visita_fallida, cierre, acuse_ot).

NO HACE:
    - No envia mensajes Telegram. El handler decide cuando pinear
      o despinear, y solo despues llama a actualizar_datos_pin().
    - No valida que la asignacion_id exista o sea valida. El caller
      ya debio chequearlo en su flujo.
    - No registra interacciones en bot_interacciones. Eso es del
      handler.

CONTRATOS PUBLICOS:
    activar_ot(cuadrilla_id, asignacion_id, conn) -> bool
        Pone ot_activa_id en la cuadrilla. NO toca pin (el handler
        lo hace despues con actualizar_datos_pin).

    obtener_ot_activa(cuadrilla_id, conn) -> dict | None
        Devuelve los datos de la OT activa con los mismos campos que
        retorna obtener_asignacion_activa_unica (drop-in replacement).
        None si no hay OT activa.

    limpiar_ot_activa(cuadrilla_id, conn) -> bool
        Pone ot_activa_id = NULL y limpia datos de pin. Se usa cuando
        la cuadrilla termina la OT.

    actualizar_datos_pin(cuadrilla_id, pin_message_id, pin_message_chat_id, conn) -> bool
        Guarda el message_id y chat_id del mensaje pineado en Telegram,
        para poder despinearlo despues.

    obtener_datos_pin_anterior(cuadrilla_id, conn) -> dict | None
        Antes de pinear nuevo, el handler consulta esto para saber
        que mensaje despinear. Retorna dict con pin_message_id y
        pin_message_chat_id, o None si no hay pin anterior.
"""

import logging

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# ACTIVAR / LIMPIAR OT ACTIVA
# ═══════════════════════════════════════════════════════════════════

def activar_ot(cuadrilla_id, asignacion_id, conn):
    """
    Pone ot_activa_id = asignacion_id en la cuadrilla.

    NO toca pin_message_id ni pin_message_chat_id. Eso lo hace el
    handler con actualizar_datos_pin() despues de pinear en Telegram.

    Retorna True si actualizo una fila, False si la cuadrilla no
    existe.
    """
    sql = """
        UPDATE onms.cuadrillas
           SET ot_activa_id = %s
         WHERE cuadrilla_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (asignacion_id, cuadrilla_id))
        if cur.rowcount == 0:
            logger.warning(
                f"[OTActiva] activar_ot: cuadrilla {cuadrilla_id!r} "
                f"no existe."
            )
            return False

    logger.info(
        f"[OTActiva] Cuadrilla {cuadrilla_id} activa ahora "
        f"asignacion_id={asignacion_id}"
    )
    return True


def limpiar_ot_activa(cuadrilla_id, conn):
    """
    Limpia ot_activa_id, pin_message_id y pin_message_chat_id de la
    cuadrilla.

    Se usa cuando:
        - La cuadrilla termina la OT (cierre confirmado por CGR).
        - La OT se marca visita_fallida = true.
        - El ETL desactiva la asignacion (huerfana).

    Retorna True si actualizo una fila, False si la cuadrilla no
    existe.
    """
    sql = """
        UPDATE onms.cuadrillas
           SET ot_activa_id        = NULL,
               pin_message_id      = NULL,
               pin_message_chat_id = NULL
         WHERE cuadrilla_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (cuadrilla_id,))
        if cur.rowcount == 0:
            logger.warning(
                f"[OTActiva] limpiar_ot_activa: cuadrilla "
                f"{cuadrilla_id!r} no existe."
            )
            return False

    logger.info(f"[OTActiva] Cuadrilla {cuadrilla_id} sin OT activa.")
    return True


# ═══════════════════════════════════════════════════════════════════
# LEER OT ACTIVA (drop-in replacement de obtener_asignacion_activa_unica)
# ═══════════════════════════════════════════════════════════════════

def obtener_ot_activa(cuadrilla_id, conn):
    """
    Devuelve los datos de la OT activa de la cuadrilla, con los mismos
    campos que hoy retorna obtener_asignacion_activa_unica() para que
    los handlers de fase puedan migrarse sin cambios estructurales.

    Retorna dict con:
        asignacion_id, wonum, estado, fase_operativa,
        marcada_urgente_cgr, nota_urgencia_cgr, visita_fallida,
        descripcion, tipo_tramo, worktype, severity, departamento,
        ciudad, operador_fo, estado_descripcion, fase_descripcion

    Retorna None si:
        - La cuadrilla no tiene ot_activa_id (NULL).
        - La asignacion fue desactivada (asignacion_activa = false).
          En este caso, sirve como senal al caller para limpiar la
          referencia obsoleta.
    """
    sql = """
        SELECT ob.asignacion_id,
               ob.wonum,
               ob.estado,
               ob.fase_operativa,
               ob.marcada_urgente_cgr,
               ob.nota_urgencia_cgr,
               ob.visita_fallida,
               wo.description     AS descripcion,
               wo.tipo_tramo,
               wo.worktype,
               wo.severity,
               wo.departamento,
               wo.ciudad,
               wo.operador_fo,
               ce.descripcion     AS estado_descripcion,
               cf.descripcion     AS fase_descripcion
          FROM onms.cuadrillas cu
          JOIN onms.ot_bandeja ob
                 ON ob.asignacion_id = cu.ot_activa_id
                AND ob.asignacion_activa = true
          LEFT JOIN onms.work_orders wo
                 ON wo.wonum = ob.wonum
          LEFT JOIN onms.cat_estado_ot_bandeja ce
                 ON ce.codigo = ob.estado
          LEFT JOIN onms.cat_fase_operativa cf
                 ON cf.codigo = ob.fase_operativa
         WHERE cu.cuadrilla_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (cuadrilla_id,))
        row = cur.fetchone()
        if row is None:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))


# ═══════════════════════════════════════════════════════════════════
# GESTION DEL PIN EN TELEGRAM
# ═══════════════════════════════════════════════════════════════════

def actualizar_datos_pin(cuadrilla_id, pin_message_id, pin_message_chat_id, conn):
    """
    Guarda el message_id y chat_id del mensaje pineado en Telegram.

    El handler llama a esta funcion DESPUES de haber pineado el
    mensaje exitosamente en Telegram. Si Telegram devolvio error en
    el pin, el handler simplemente no llama a esto y los campos
    quedan en NULL (no hay pin que despinear despues).

    Para registrar 'sin pin actual', el handler puede pasar
    pin_message_id=None y pin_message_chat_id=None.

    Retorna True si actualizo una fila, False si la cuadrilla no
    existe.
    """
    sql = """
        UPDATE onms.cuadrillas
           SET pin_message_id      = %s,
               pin_message_chat_id = %s
         WHERE cuadrilla_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (pin_message_id, pin_message_chat_id, cuadrilla_id))
        if cur.rowcount == 0:
            logger.warning(
                f"[OTActiva] actualizar_datos_pin: cuadrilla "
                f"{cuadrilla_id!r} no existe."
            )
            return False

    if pin_message_id is None:
        logger.info(f"[OTActiva] Cuadrilla {cuadrilla_id}: datos de pin limpiados.")
    else:
        logger.info(
            f"[OTActiva] Cuadrilla {cuadrilla_id}: pin actualizado a "
            f"message_id={pin_message_id} en chat_id={pin_message_chat_id}"
        )
    return True


def obtener_datos_pin_anterior(cuadrilla_id, conn):
    """
    Devuelve los datos del pin actual de la cuadrilla, para que el
    handler pueda despinearlo antes de pinear uno nuevo.

    Retorna dict con keys 'pin_message_id', 'pin_message_chat_id'.
    Retorna None si no hay pin actual (ambos campos NULL) o si la
    cuadrilla no existe.
    """
    sql = """
        SELECT pin_message_id,
               pin_message_chat_id
          FROM onms.cuadrillas
         WHERE cuadrilla_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (cuadrilla_id,))
        row = cur.fetchone()
        if row is None:
            return None
        pin_message_id, pin_message_chat_id = row
        if pin_message_id is None or pin_message_chat_id is None:
            return None
        return {
            "pin_message_id":      pin_message_id,
            "pin_message_chat_id": pin_message_chat_id,
        }