"""
bot/services/acuse_ot.py
------------------------
Logica de BD para procesar el acuse del coordinador sobre una OT
notificada (botones [Recibida] / [No es de mi zona]).

PROPOSITO:
    Mantener callback_botones.py como un router delgado. La logica de
    actualizar BD, validar permisos y leer datos de la OT vive aqui.

CONTRATOS PUBLICOS:
    obtener_asignacion(asignacion_id, conn) -> dict | None
        Devuelve datos minimos de la asignacion para validacion y mensajes.
        None si no existe.

    procesar_acuse_recibida(asignacion_id, conn) -> bool
        Marca la OT como acusada y aceptada por el coord.
        Retorna True si exitoso, False si la OT no estaba en estado valido.

    procesar_acuse_rechazada(asignacion_id, conn) -> bool
        Marca la OT como acusada y rechazada por el coord.
        Retorna True si exitoso, False si la OT no estaba en estado valido.

DECISIONES DE DISENO:
    - Las funciones reciben conn como parametro (el llamador la maneja).
    - El UPDATE tiene WHERE de estado para idempotencia: si el coord
      hace doble-click y de alguna forma el segundo click llega antes
      de que el primero edite el mensaje, el segundo UPDATE no hace
      nada (0 filas afectadas) y la funcion devuelve False.
    - El llamador (callback_botones.py) decide que hacer con el False:
      por simplicidad, ignorar silenciosamente.
"""

import logging

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# LECTURA: datos minimos de la asignacion
# ═══════════════════════════════════════════════════════════════════

def obtener_asignacion(asignacion_id, conn):
    """
    Devuelve dict con los campos necesarios para validar y formatear
    mensajes de respuesta. None si la asignacion no existe.

    Campos retornados:
        asignacion_id, wonum, coordinador_asignado_id,
        coord_telegram_user_id, estado, asignacion_activa,
        notificacion_coordinador_recibida_at
    """
    sql = """
        SELECT ob.asignacion_id,
               ob.wonum,
               ob.coordinador_asignado_id,
               co.telegram_user_id  AS coord_telegram_user_id,
               ob.estado,
               ob.asignacion_activa,
               ob.notificacion_coordinador_recibida_at
          FROM onms.ot_bandeja ob
          LEFT JOIN onms.coordinadores_contratista co
                 ON ob.coordinador_asignado_id = co.coordinador_id
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
# ESCRITURA: acuse [Recibida]
# ═══════════════════════════════════════════════════════════════════

def procesar_acuse_recibida(asignacion_id, conn):
    """
    Marca la OT como acusada (Recibida).

    Transicion:
        estado:            pendiente_acuse_coordinador -> pendiente_asignacion_coordinador
        fase_operativa:    sin_asignar                 -> vista_por_coordinador
        notificacion_coordinador_recibida_at:  NULL    -> NOW()

    Idempotencia: el WHERE incluye el estado origen, asi un doble-click
    no vuelve a procesar. Retorna False si 0 filas afectadas.

    NO toca cuadrilla_id (sigue NULL hasta que el coord la asigne en
    una sesion futura).
    """
    sql = """
        UPDATE onms.ot_bandeja
           SET estado = 'pendiente_asignacion_coordinador',
               fase_operativa = 'vista_por_coordinador',
               notificacion_coordinador_recibida_at = NOW()
         WHERE asignacion_id = %s
           AND estado = 'pendiente_acuse_coordinador'
           AND asignacion_activa = true
    """
    with conn.cursor() as cur:
        cur.execute(sql, (asignacion_id,))
        filas_afectadas = cur.rowcount

    if filas_afectadas == 0:
        logger.warning(
            f"[Acuse] procesar_acuse_recibida no actualizo nada para "
            f"asignacion_id={asignacion_id}. Probablemente ya fue procesada."
        )
        return False

    logger.info(
        f"[Acuse] asignacion_id={asignacion_id} -> recibida por coord"
    )
    return True


# ═══════════════════════════════════════════════════════════════════
# ESCRITURA: acuse [No es de mi zona]
# ═══════════════════════════════════════════════════════════════════

def procesar_acuse_rechazada(asignacion_id, conn):
    """
    Marca la OT como rechazada por el coord.

    Transicion:
        estado:            pendiente_acuse_coordinador -> rechazada_por_coordinador
        notificacion_coordinador_recibida_at:  NULL    -> NOW()
        asignacion_activa:                     true    -> false

    Apagar asignacion_activa saca la OT del flujo: ya no aparecera en
    el notificador ni en bandejas futuras. Pero la fila queda en BD
    para auditoria (cuando, quien, etc.).

    No tocamos fase_operativa: queda en sin_asignar porque la OT nunca
    llego a verla un coord operativo. El estado rechazada_por_coordinador
    es lo suficientemente expresivo.
    """
    sql = """
        UPDATE onms.ot_bandeja
           SET estado = 'rechazada_por_coordinador',
               notificacion_coordinador_recibida_at = NOW(),
               asignacion_activa = false
         WHERE asignacion_id = %s
           AND estado = 'pendiente_acuse_coordinador'
           AND asignacion_activa = true
    """
    with conn.cursor() as cur:
        cur.execute(sql, (asignacion_id,))
        filas_afectadas = cur.rowcount

    if filas_afectadas == 0:
        logger.warning(
            f"[Acuse] procesar_acuse_rechazada no actualizo nada para "
            f"asignacion_id={asignacion_id}. Probablemente ya fue procesada."
        )
        return False

    logger.info(
        f"[Acuse] asignacion_id={asignacion_id} -> rechazada por coord"
    )
    return True