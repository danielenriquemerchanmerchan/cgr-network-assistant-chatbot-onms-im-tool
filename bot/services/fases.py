"""
bot/services/fases.py
---------------------
Servicio de cambio de fase operativa de una OT.

Punto único de cambio de `ot_bandeja.fase_operativa`. Reutilizable
por todos los handlers tipo A del Momento 2 (los que cambian fase):
/en_desplazamiento, /llegada, /midiendo, /busqueda, /hallazgo,
/empalmando, /normalizado, /nos_retiramos.

PATRON B (caller commitea):
- La funcion recibe la conexion abierta.
- NO hace conn.commit() ni conn.rollback().
- NO inserta en bot_interacciones (eso lo hace el handler con
  registrar_interaccion).
- El handler que llama es quien commitea o revierte.
"""

import logging

logger = logging.getLogger(__name__)


def cambiar_fase(asignacion_id, nueva_fase, conn):
    """
    Actualiza `ot_bandeja.fase_operativa` a la nueva fase.

    Args:
        asignacion_id: ID de la asignacion (FK a ot_bandeja).
        nueva_fase: Codigo de la fase nueva (debe existir en
                    cat_fase_operativa y estar activa).
        conn: Conexion abierta. El caller commitea.

    Returns:
        True si actualizo una fila (la asignacion existe y la
        operacion fue OK), False si no actualizo nada.

    No valida transiciones (la cuadrilla puede ir de cualquier fase
    a cualquier otra). La validacion del codigo nueva_fase la hace
    el FK de la tabla; si el codigo no existe en cat_fase_operativa,
    el UPDATE fallara con excepcion y el caller maneja.
    """
    sql = """
        UPDATE onms.ot_bandeja
           SET fase_operativa = %s
         WHERE asignacion_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (nueva_fase, asignacion_id))
        rowcount = cur.rowcount

    if rowcount > 0:
        logger.info(
            f"[Fases] Asignacion {asignacion_id} -> "
            f"fase '{nueva_fase}'"
        )
    else:
        logger.warning(
            f"[Fases] No se actualizo asignacion {asignacion_id} "
            f"(no existe o ya tenia esa fase)"
        )

    return rowcount > 0


def asegurar_estado_en_progreso(asignacion_id, conn):
    """
    Si la OT esta en estado='aceptada', la pasa a 'en_progreso'.
    Es idempotente: si ya esta en otro estado (en_progreso,
    declarada_terminada, etc.) no hace nada.

    Se llama desde el helper generico que ejecuta comandos tipo A
    del Momento 2. La primera vez que la cuadrilla reporta cualquier
    avance, la OT pasa de 'aceptada' (no ha empezado) a 'en_progreso'
    (trabajando activamente).

    Args:
        asignacion_id: ID de la asignacion (FK a ot_bandeja).
        conn: Conexion abierta. El caller commitea.

    Returns:
        True si efectivamente actualizo (paso de aceptada a en_progreso).
        False si no toco nada (ya estaba en otro estado).
    """
    sql = """
        UPDATE onms.ot_bandeja
           SET estado = 'en_progreso',
               fecha_inicio_progreso = COALESCE(fecha_inicio_progreso, NOW())
         WHERE asignacion_id = %s
           AND estado = 'aceptada'
    """
    with conn.cursor() as cur:
        cur.execute(sql, (asignacion_id,))
        rowcount = cur.rowcount

    if rowcount > 0:
        logger.info(
            f"[Fases] Asignacion {asignacion_id} -> "
            f"estado 'aceptada' a 'en_progreso'"
        )
    return rowcount > 0