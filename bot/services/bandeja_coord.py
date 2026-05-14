"""
bot/services/bandeja_coord.py
-----------------------------
Logica de BD para la bandeja del coordinador (/bandeja en chat privado).
"""

import logging

logger = logging.getLogger(__name__)


ESTADOS_ACTIVOS_COORD = (
    "pendiente_asignacion_coordinador",
    "aceptada",
    "en_progreso",
)


def identificar_coordinador_por_telegram_user_id(telegram_user_id, conn):
    """
    Busca un coordinador activo por su telegram_user_id.
    Retorna dict con coordinador_id, nombre_completo, contratista.
    None si no se encuentra.
    """
    sql = """
        SELECT coordinador_id, nombre_completo, contratista
          FROM onms.coordinadores_contratista
         WHERE telegram_user_id = %s
           AND activo = true
         LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (telegram_user_id,))
        row = cur.fetchone()
        if row is None:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))


def obtener_ots_del_coord(coordinador_id, conn):
    """
    Trae las OTs activas del coord, separadas en dos grupos.

    Incluye JOINs a cat_estado_ot_bandeja y cat_fase_operativa para
    obtener las descripciones legibles (estado_descripcion,
    fase_descripcion). Estas descripciones se usan en el render del
    handler para mostrar texto humano en lugar de codigos crudos
    (que ademas contienen guiones bajos que rompen Markdown).
    """
    sql = """
        SELECT ob.asignacion_id,
               ob.wonum,
               ob.estado,
               ob.fase_operativa,
               ob.cuadrilla_id,
               ob.fecha_asignacion_cuadrilla,
               ob.notificacion_coordinador_recibida_at,
               ob.visita_fallida,
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
               cu.nombre          AS cuadrilla_nombre,
               ce.descripcion     AS estado_descripcion,
               cf.descripcion     AS fase_descripcion
          FROM onms.ot_bandeja ob
          LEFT JOIN onms.work_orders wo
                 ON ob.wonum = wo.wonum
          LEFT JOIN onms.cuadrillas cu
                 ON ob.cuadrilla_id = cu.cuadrilla_id
          LEFT JOIN onms.cat_estado_ot_bandeja ce
                 ON ce.codigo = ob.estado
          LEFT JOIN onms.cat_fase_operativa cf
                 ON cf.codigo = ob.fase_operativa
         WHERE ob.coordinador_asignado_id = %s
           AND ob.asignacion_activa = true
           AND ob.estado = ANY(%s)
         ORDER BY ob.notificacion_coordinador_recibida_at ASC NULLS LAST,
                  ob.fecha_asignacion_cuadrilla ASC NULLS LAST,
                  ob.wonum
    """

    with conn.cursor() as cur:
        cur.execute(sql, (coordinador_id, list(ESTADOS_ACTIVOS_COORD)))
        cols = [d[0] for d in cur.description]
        filas = [dict(zip(cols, row)) for row in cur.fetchall()]

    pendientes_asignar = [
        f for f in filas
        if f["estado"] == "pendiente_asignacion_coordinador"
    ]
    asignadas_a_cuadrilla = [
        f for f in filas
        if f["estado"] != "pendiente_asignacion_coordinador"
    ]

    return {
        "pendientes_asignar":    pendientes_asignar,
        "asignadas_a_cuadrilla": asignadas_a_cuadrilla,
        "total":                 len(filas),
    }