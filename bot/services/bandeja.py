"""
bot/services/bandeja.py
-----------------------
Servicios para consultar la bandeja de OTs de una cuadrilla.
"""

import logging

from integrations.postgres.client import obtener_conexion, cerrar_conexion

logger = logging.getLogger(__name__)


def obtener_ots_activas_cuadrilla(cuadrilla_id):
    """
    Retorna las OTs activas en la bandeja de una cuadrilla.

    "Activa" = asignacion_activa=true Y estado IN (pendiente_aceptacion,
                aceptada, en_progreso, declarada_terminada)
    """
    conn = None
    try:
        conn = obtener_conexion()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 
                    ob.asignacion_id,
                    ob.wonum,
                    wo.description AS descripcion,
                    wo.tipo_tramo,
                    ob.estado,
                    ce.descripcion AS estado_descripcion,
                    ob.fase_operativa,
                    cf.descripcion AS fase_descripcion,
                    ob.fecha_asignacion_cuadrilla,
                    ob.marcada_urgente_cgr,
                    ob.nota_urgencia_cgr,
                    ob.visita_fallida,
                    wo.departamento,
                    wo.ciudad,
                    wo.worktype,
                    wo.severity,
                    wo.operador_fo
                FROM onms.ot_bandeja ob
                LEFT JOIN onms.work_orders wo ON wo.wonum = ob.wonum
                JOIN onms.cat_estado_ot_bandeja ce ON ce.codigo = ob.estado
                JOIN onms.cat_fase_operativa cf ON cf.codigo = ob.fase_operativa
                WHERE ob.cuadrilla_id = %s
                  AND ob.asignacion_activa = true
                  AND ob.estado IN (
                      'pendiente_aceptacion', 
                      'aceptada', 
                      'en_progreso', 
                      'declarada_terminada'
                  )
                ORDER BY 
                  ob.marcada_urgente_cgr DESC,
                  ob.fecha_asignacion_cuadrilla
                """,
                (cuadrilla_id,),
            )
            rows = cur.fetchall()
            return [
                {
                    "asignacion_id":               row[0],
                    "wonum":                       row[1],
                    "descripcion":                 row[2],
                    "tipo_tramo":                  row[3],
                    "estado":                      row[4],
                    "estado_descripcion":          row[5],
                    "fase_operativa":              row[6],
                    "fase_descripcion":            row[7],
                    "fecha_asignacion_cuadrilla":  row[8],
                    "marcada_urgente_cgr":         row[9],
                    "nota_urgencia_cgr":           row[10],
                    "visita_fallida":              row[11],
                    "departamento":                row[12],
                    "ciudad":                      row[13],
                    "worktype":                    row[14],
                    "severity":                    row[15],
                    "operador_fo":                 row[16],
                }
                for row in rows
            ]
    except Exception as e:
        logger.error(f"Error obteniendo bandeja de cuadrilla {cuadrilla_id}: {e}")
        return []
    finally:
        if conn:
            cerrar_conexion(conn)


def obtener_asignacion_activa_unica(cuadrilla_id):
    """
    Si la cuadrilla tiene UNA SOLA OT activa en estado 'aceptada' o 'en_progreso',
    la retorna. Si tiene varias o ninguna, retorna None.
    """
    ots = obtener_ots_activas_cuadrilla(cuadrilla_id)
    activas = [
        ot for ot in ots
        if ot["estado"] in ("aceptada", "en_progreso")
    ]
    if len(activas) == 1:
        return activas[0]
    return None


def actualizar_fase_operativa(asignacion_id, nueva_fase, nuevo_estado=None):
    """
    Actualiza la fase operativa (y opcionalmente el estado) de una asignacion.
    """
    conn = None
    try:
        conn = obtener_conexion()
        with conn.cursor() as cur:
            if nuevo_estado:
                cur.execute(
                    """
                    UPDATE onms.ot_bandeja
                    SET fase_operativa = %s,
                        estado = %s,
                        fecha_inicio_progreso = COALESCE(fecha_inicio_progreso, NOW())
                    WHERE asignacion_id = %s
                    """,
                    (nueva_fase, nuevo_estado, asignacion_id),
                )
            else:
                cur.execute(
                    """
                    UPDATE onms.ot_bandeja
                    SET fase_operativa = %s
                    WHERE asignacion_id = %s
                    """,
                    (nueva_fase, asignacion_id),
                )
            conn.commit()
            return cur.rowcount > 0
    except Exception as e:
        logger.error(f"Error actualizando fase de asignacion {asignacion_id}: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            cerrar_conexion(conn)