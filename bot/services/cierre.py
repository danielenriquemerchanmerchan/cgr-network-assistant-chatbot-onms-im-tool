"""
bot/services/cierre.py
----------------------
Logica de BD para declarar cierre de OT por la cuadrilla.

PROPOSITO:
    Cuando la cuadrilla declara que termino el trabajo (/cierre), este
    servicio actualiza la OT en ot_bandeja:
        - estado -> 'declarada_terminada'
        - fase_operativa -> 'validacion'
        - fecha_declaracion_cierre -> NOW()

    NO cierra la OT en Maximo. Solo declara que la cuadrilla termino.
    CGR valida despues desde su tablero.

MODELO TRANSACCIONAL (Modelo B — handler dueno de la transaccion):
    Este servicio NO hace commit. El handler lo decide despues de
    registrar la interaccion en bot_interacciones. Consistente con
    el resto de servicios llamados desde handlers de Telegram
    (visita_fallida, acuse_ot, asignar_cuadrilla, reasignar_cuadrilla).

NO HACE:
    - No escribe en bot_interacciones (lo hace el handler).
    - No envia mensajes Telegram (eso es del handler).
    - No hace commit ni rollback (el caller decide).

CONTRATO PUBLICO:
    declarar_cierre(asignacion_id, conn) -> bool
        True si el UPDATE afecto una fila, False si la asignacion
        no existe.

DEUDA CONOCIDA (a resolver en T3):
    - No valida estado/fase actual antes del UPDATE. Si /cierre se
      ejecuta sobre una OT ya en 'declarada_terminada', se vuelve
      a pisar la fecha. Se uniformiza con las guardas del resto de
      handlers de fase cuando se hagan en T3.
    - No bloquea si la OT esta marcada visita_fallida=true. Tambien
      se uniformiza en T3.
"""

import logging

logger = logging.getLogger(__name__)


def declarar_cierre(asignacion_id, conn):
    """
    Marca la OT como declarada_terminada en ot_bandeja.

    Solo toca estado de negocio. El handler hace commit y registra
    la interaccion en bot_interacciones (patron B).

    Transicion:
        estado:                   * -> declarada_terminada
        fase_operativa:           * -> validacion
        fecha_declaracion_cierre: * -> NOW()

    Retorna True si el UPDATE afecto una fila, False si no existe
    la asignacion.
    """
    sql = """
        UPDATE onms.ot_bandeja
           SET estado = 'declarada_terminada',
               fase_operativa = 'validacion',
               fecha_declaracion_cierre = NOW()
         WHERE asignacion_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (asignacion_id,))
        if cur.rowcount == 0:
            logger.warning(
                f"[Cierre] No se actualizo nada para "
                f"asignacion_id={asignacion_id}. No existe."
            )
            return False

    logger.info(
        f"[Cierre] asignacion_id={asignacion_id} declarada_terminada."
    )
    return True