"""
bot/services/reasignar_cuadrilla.py
-----------------------------------
Logica de BD para el flujo de REASIGNACION de OT entre cuadrillas.

DIFERENCIA con asignar_cuadrilla.py:
    asignar_cuadrilla.py: primera asignacion. cuadrilla_id pasa de NULL a X.
    reasignar_cuadrilla.py: cambio de cuadrilla. cuadrilla_id pasa de X a Y.

CONTRATOS PUBLICOS:
    obtener_datos_para_reasignacion(asignacion_id, conn) -> dict | None
        Trae datos suficientes para mostrar el menu de reasignacion al
        coord y luego notificar a ambos grupos.

    reasignar_a_cuadrilla(asignacion_id, cuadrilla_nueva_id, conn) -> bool
        Ejecuta el UPDATE. Idempotente: si la cuadrilla nueva es igual
        a la actual, no hace nada y retorna False.
"""

import logging

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# 1. DATOS COMPLETOS PARA LA REASIGNACION
# ═══════════════════════════════════════════════════════════════════

def obtener_datos_para_reasignacion(asignacion_id, conn):
    """
    Trae todos los datos necesarios para el flujo de reasignacion:
        - Datos de la OT (para el mensaje a la cuadrilla nueva)
        - Datos de la cuadrilla actual (chat_id para avisar que pierde la OT)
        - Identidad del coord (para validar y para mostrar quien reasigno)

    Retorna dict o None si la asignacion no existe.

    Campos:
        asignacion_id, wonum, coordinador_id, coord_telegram_user_id,
        coord_nombre, estado, asignacion_activa,
        cuadrilla_anterior_id, cuadrilla_anterior_nombre,
        chat_id_cuadrilla_anterior,
        worktype, departamento, ciudad, operador_fo, severity,
        creation_date, description, cinum, tipo_tramo, direccion
    """
    sql = """
        SELECT ob.asignacion_id,
               ob.wonum,
               ob.coordinador_asignado_id      AS coordinador_id,
               co.telegram_user_id              AS coord_telegram_user_id,
               co.nombre_completo               AS coord_nombre,
               ob.estado,
               ob.asignacion_activa,
               ob.cuadrilla_id                  AS cuadrilla_anterior_id,
               cu_ant.nombre                    AS cuadrilla_anterior_nombre,
               cu_ant.telegram_chat_id          AS chat_id_cuadrilla_anterior,
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
          LEFT JOIN onms.cuadrillas cu_ant
                 ON ob.cuadrilla_id = cu_ant.cuadrilla_id
          LEFT JOIN onms.work_orders wo
                 ON ob.wonum = wo.wonum
         WHERE ob.asignacion_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (asignacion_id,))
        row = cur.fetchone()
        if row is None:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))


# ═══════════════════════════════════════════════════════════════════
# 2. DATOS DE LA CUADRILLA NUEVA (chat_id y nombre)
# ═══════════════════════════════════════════════════════════════════

def obtener_datos_cuadrilla(cuadrilla_id, conn):
    """
    Devuelve dict con nombre y telegram_chat_id de la cuadrilla.
    None si no existe.
    """
    sql = """
        SELECT cuadrilla_id, nombre, telegram_chat_id, contratista
          FROM onms.cuadrillas
         WHERE cuadrilla_id = %s
           AND activa = true
    """
    with conn.cursor() as cur:
        cur.execute(sql, (cuadrilla_id,))
        row = cur.fetchone()
        if row is None:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))


# ═══════════════════════════════════════════════════════════════════
# 3. EJECUTAR LA REASIGNACION (UPDATE en BD)
# ═══════════════════════════════════════════════════════════════════

def reasignar_a_cuadrilla(asignacion_id, cuadrilla_nueva_id, conn):
    """
    Cambia el cuadrilla_id de la OT.

    Politica acordada con el usuario:
        - NO se cambia estado ni fase_operativa (sigue donde iba).
        - NO se crea historial separado (solo se sobreescribe).
        - SI se actualiza fecha_asignacion_cuadrilla a NOW() para
          marcar el momento del cambio.

    Idempotencia:
        - Si la cuadrilla nueva es la misma que la actual, no hace nada.
        - Si la asignacion no esta activa, no hace nada.

    Retorna True si exitoso, False en cualquier otro caso.
    """
    sql = """
        UPDATE onms.ot_bandeja
           SET cuadrilla_id = %s,
               fecha_asignacion_cuadrilla = NOW()
         WHERE asignacion_id = %s
           AND asignacion_activa = true
           AND cuadrilla_id IS NOT NULL
           AND cuadrilla_id <> %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (cuadrilla_nueva_id, asignacion_id, cuadrilla_nueva_id))
        filas = cur.rowcount

    if filas == 0:
        logger.warning(
            f"[Reasignacion] reasignar_a_cuadrilla no actualizo nada "
            f"asignacion_id={asignacion_id}, cuadrilla_nueva={cuadrilla_nueva_id}. "
            f"Probablemente: cuadrilla igual a la actual, OT inactiva o "
            f"sin cuadrilla previa."
        )
        return False

    logger.info(
        f"[Reasignacion] asignacion_id={asignacion_id} -> "
        f"nueva cuadrilla {cuadrilla_nueva_id}"
    )
    return True