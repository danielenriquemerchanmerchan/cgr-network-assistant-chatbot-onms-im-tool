"""
bot/services/asignar_cuadrilla.py
----------------------------------
Logica de BD para el flujo de asignacion de OT a una cuadrilla del
coordinador.

PROPOSITO:
    Cuando el coord presiona [Recibida] y luego escoge una cuadrilla,
    este servicio:
        1. Lista las cuadrillas del coord (con conteo de OTs activas).
        2. Obtiene los datos completos de la OT a asignar (para el
           mensaje al grupo).
        3. Actualiza ot_bandeja con cuadrilla_id, estado='aceptada',
           fase='asignada', fecha_asignacion_cuadrilla=NOW().
        4. Obtiene el telegram_chat_id del grupo de la cuadrilla
           (para que el handler envie el mensaje).

NO HACE:
    - No envia mensajes de Telegram (eso es trabajo del handler).
    - No registra interacciones (lo hara el handler).

CONTRATOS PUBLICOS:
    obtener_cuadrillas_del_coord(coordinador_id, conn) -> list[dict]
        Cuadrillas activas del coord con conteo de OTs activas.

    obtener_datos_ot_para_grupo(asignacion_id, conn) -> dict | None
        Datos de la OT para construir el mensaje al grupo.

    asignar_a_cuadrilla(asignacion_id, cuadrilla_id, conn) -> bool
        Ejecuta el UPDATE. Idempotente por estado origen.

    obtener_telegram_chat_id_cuadrilla(cuadrilla_id, conn) -> int | None
        Devuelve el chat_id del grupo Telegram de la cuadrilla.
"""

import logging

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# 1. LISTAR CUADRILLAS DEL COORD (con conteo de OTs activas)
# ═══════════════════════════════════════════════════════════════════

def obtener_cuadrillas_del_coord(coordinador_id, conn):
    """
    Devuelve las cuadrillas activas del coord con su conteo de OTs
    actualmente en su bandeja.

    "OTs activas" = filas en ot_bandeja con asignacion_activa=true
    asociadas a la cuadrilla. Las rechazadas o terminadas no cuentan.

    Retorna:
        list[dict] con campos:
            cuadrilla_id, nombre, contratista, ots_activas
        Ordenado alfabeticamente por cuadrilla_id.

    Si el coord no tiene cuadrillas, retorna lista vacia.
    """
    sql = """
        SELECT cu.cuadrilla_id,
               cu.nombre,
               cu.contratista,
               COALESCE(ots.cantidad, 0) AS ots_activas
          FROM onms.cuadrillas cu
          LEFT JOIN (
                SELECT cuadrilla_id, COUNT(*) AS cantidad
                  FROM onms.ot_bandeja
                 WHERE asignacion_activa = true
                   AND cuadrilla_id IS NOT NULL
                 GROUP BY cuadrilla_id
          ) ots ON ots.cuadrilla_id = cu.cuadrilla_id
         WHERE cu.coordinador_id = %s
           AND cu.activa = true
         ORDER BY cu.cuadrilla_id
    """
    with conn.cursor() as cur:
        cur.execute(sql, (coordinador_id,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ═══════════════════════════════════════════════════════════════════
# 2. DATOS DE LA OT PARA EL MENSAJE AL GRUPO
# ═══════════════════════════════════════════════════════════════════

def obtener_datos_ot_para_grupo(asignacion_id, conn):
    """
    Devuelve los datos completos necesarios para construir el mensaje
    que se envia al grupo Telegram de la cuadrilla.

    Hace JOIN entre ot_bandeja, work_orders, coordinadores y cuadrillas
    para traer todo en una sola query.

    Retorna dict | None.
    Campos: wonum, worktype, departamento, ciudad, operador_fo, severity,
            creation_date, description, cinum, tipo_tramo, direccion,
            coord_nombre, cuadrilla_nombre, telegram_chat_id_cuadrilla
    """
    sql = """
        SELECT ob.asignacion_id,
               ob.wonum,
               wo.worktype,
               wo.departamento,
               wo.ciudad,
               wo.operador_fo,
               wo.severity,
               wo.creation_date,
               wo.description,
               wo.cinum,
               wo.tipo_tramo,
               wo.direccion,
               co.nombre_completo AS coord_nombre,
               cu.nombre          AS cuadrilla_nombre,
               cu.telegram_chat_id AS telegram_chat_id_cuadrilla
          FROM onms.ot_bandeja ob
          LEFT JOIN onms.work_orders wo
                 ON ob.wonum = wo.wonum
          LEFT JOIN onms.coordinadores_contratista co
                 ON ob.coordinador_asignado_id = co.coordinador_id
          LEFT JOIN onms.cuadrillas cu
                 ON ob.cuadrilla_id = cu.cuadrilla_id
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
# 3. EJECUTAR LA ASIGNACION
# ═══════════════════════════════════════════════════════════════════

def asignar_a_cuadrilla(asignacion_id, cuadrilla_id, conn):
    """
    Ejecuta el UPDATE en ot_bandeja para asignar la OT a la cuadrilla.

    Transicion de estados (segun acordamos):
        estado:                            pendiente_asignacion_coordinador -> aceptada
        fase_operativa:                    vista_por_coordinador            -> asignada
        cuadrilla_id:                      NULL                              -> X
        fecha_asignacion_cuadrilla:        NULL                              -> NOW()
        fecha_aceptacion:                  NULL                              -> NOW()

    Nota: ponemos fecha_aceptacion = NOW() porque en el modelo simplificado
    no hay paso de aceptacion explicito de la cuadrilla. Es como si
    aceptaran al recibir.

    Idempotencia: el WHERE incluye el estado origen, asi un doble-click
    no vuelve a procesar. Retorna False si 0 filas afectadas.
    """
    sql = """
        UPDATE onms.ot_bandeja
           SET cuadrilla_id = %s,
               estado = 'aceptada',
               fase_operativa = 'asignada',
               fecha_asignacion_cuadrilla = NOW(),
               fecha_aceptacion = NOW()
         WHERE asignacion_id = %s
           AND estado = 'pendiente_asignacion_coordinador'
           AND asignacion_activa = true
    """
    with conn.cursor() as cur:
        cur.execute(sql, (cuadrilla_id, asignacion_id))
        filas_afectadas = cur.rowcount

    if filas_afectadas == 0:
        logger.warning(
            f"[Asignacion] asignar_a_cuadrilla no actualizo nada para "
            f"asignacion_id={asignacion_id}. Probablemente ya fue asignada."
        )
        return False

    logger.info(
        f"[Asignacion] asignacion_id={asignacion_id} -> "
        f"cuadrilla {cuadrilla_id}"
    )
    return True