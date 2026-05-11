"""
bot/services/bandeja_coord.py
-----------------------------
Logica de BD para la bandeja del coordinador (/bandeja en chat privado).

PROPOSITO:
    Listar las OTs del coord segun el filtro acordado: activas
    (asignacion_activa=true) Y vivas (estado distinto a rechazada).
    Eso incluye:
        - OTs acusadas con [Recibida] pero aun sin cuadrilla asignada
          (estado='pendiente_asignacion_coordinador'). PRIORIDAD ALTA.
        - OTs ya asignadas a cuadrilla (estado='aceptada' o cualquier
          fase posterior). EN CURSO.
    Excluye:
        - rechazadas_por_coordinador (asignacion_activa=false)
        - pendiente_acuse_coordinador (aun no las acusa, no son su trabajo)
        - terminadas/cerradas (en futuro)

NO HACE:
    - No envia mensajes (eso es del handler).
    - No detecta quien es coord (eso es del handler).
    - No registra interacciones.

CONTRATOS PUBLICOS:
    obtener_ots_del_coord(coordinador_id, conn) -> dict
        Estructura del dict retornado:
            {
                'pendientes_asignar': [list de dicts OT],
                'asignadas_a_cuadrilla': [list de dicts OT],
                'total': int,
            }

        Cada dict OT contiene:
            asignacion_id, wonum, worktype, departamento, ciudad,
            operador_fo, severity, creation_date, description, cinum,
            tipo_tramo, direccion, estado, fase_operativa, cuadrilla_id,
            cuadrilla_nombre, fecha_asignacion_cuadrilla,
            notificacion_coordinador_recibida_at
"""

import logging

logger = logging.getLogger(__name__)


# Estados que cuentan como "trabajo activo del coord"
# - pendiente_asignacion_coordinador: acuso [Recibida] pero no ha asignado cuadrilla
# - aceptada: asignada a cuadrilla, sin actividad iniciada
# - en_progreso: cuadrilla esta trabajando
# (en el futuro pueden agregarse otros estados intermedios)
ESTADOS_ACTIVOS_COORD = (
    "pendiente_asignacion_coordinador",
    "aceptada",
    "en_progreso",
)


# ═══════════════════════════════════════════════════════════════════
# IDENTIFICACION DE COORDINADOR
# ═══════════════════════════════════════════════════════════════════

def identificar_coordinador_por_telegram_user_id(telegram_user_id, conn):
    """
    Busca un coordinador activo por su telegram_user_id.

    Retorna dict con campos: coordinador_id, nombre_completo, contratista.
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
    Trae las OTs activas del coord, separadas en dos grupos segun la
    decision de diseno acordada (pendientes-de-asignar primero, luego
    asignadas-a-cuadrilla).

    Una sola query con JOIN; el split en grupos se hace en Python.
    """
    sql = """
        SELECT ob.asignacion_id,
               ob.wonum,
               ob.estado,
               ob.fase_operativa,
               ob.cuadrilla_id,
               ob.fecha_asignacion_cuadrilla,
               ob.notificacion_coordinador_recibida_at,
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
               cu.nombre AS cuadrilla_nombre
          FROM onms.ot_bandeja ob
          LEFT JOIN onms.work_orders wo
                 ON ob.wonum = wo.wonum
          LEFT JOIN onms.cuadrillas cu
                 ON ob.cuadrilla_id = cu.cuadrilla_id
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

    # Particionar en dos grupos
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