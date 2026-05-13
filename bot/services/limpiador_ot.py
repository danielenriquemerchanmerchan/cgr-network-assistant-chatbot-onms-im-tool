"""
bot/services/limpiador_ot.py
----------------------------
Limpia las filas de ot_bandeja cuya OT ya no esta en work_orders.

PROPOSITO:
    El ETL de bandeja_o_gesfo trae solo OTs del grupo O_GESFO en
    Maximo. Cuando una OT cambia de grupo (a O_GESTRA, etc.) o se
    cierra (COMP/CLOSE/CAN), desaparece de work_orders. Pero la fila
    en ot_bandeja queda activa, generando huerfanas.

    Este modulo detecta esas huerfanas y:
        1. Marca asignacion_activa = false (UPDATE).
        2. Commitea la transaccion (atomiza su propio trabajo).
        3. Devuelve la lista de OTs marcadas para que el ETL (o el
           bot) avise a las cuadrillas afectadas.

MODELO TRANSACCIONAL (Modelo A — servicio dueno de la transaccion):
    El servicio commitea internamente. Toda la limpieza es una sola
    transaccion: o se desactivan todas las huerfanas detectadas, o
    ninguna. Si algo falla, rollback y el proximo ciclo reintentara.
    El llamador (ETL) NO necesita hacer commit ni rollback.

NO HACE:
    - No borra filas. Solo cambia el flag (preserva historial).
    - No envia Telegram (eso es del llamador, ya que el ETL no tiene
      acceso al bot).

CONTRATO PUBLICO:
    limpiar_huerfanas(conn) -> list[dict]
        Marca las huerfanas, commitea, y devuelve la lista para que
        el llamador decida que hacer con cada una.

        Cada dict tiene:
            asignacion_id, wonum, cuadrilla_id, telegram_chat_id (de la cuadrilla)
"""

import logging

logger = logging.getLogger(__name__)


def limpiar_huerfanas(conn):
    """
    Marca como inactivas las filas de ot_bandeja cuya OT ya no existe
    en work_orders. Devuelve la lista de filas afectadas.

    Modelo A — atomiza su propio trabajo:
        - Ejecuta el UPDATE.
        - Si OK, commit y retorna la lista de filas.
        - Si falla algo, rollback y propaga la excepcion.

    El UPDATE es atomico (una sola query con WHERE y RETURNING): o se
    desactivan todas las huerfanas detectadas, o ninguna.

    Retorna list[dict] (puede ser []).
    Propaga excepciones de BD para que el llamador las pueda loguear.
    """
    sql = """
        WITH huerfanas AS (
            SELECT ob.asignacion_id,
                   ob.wonum,
                   ob.cuadrilla_id,
                   cu.telegram_chat_id AS chat_id_cuadrilla
              FROM onms.ot_bandeja ob
              LEFT JOIN onms.cuadrillas cu
                     ON ob.cuadrilla_id = cu.cuadrilla_id
             WHERE ob.asignacion_activa = true
               AND NOT EXISTS (
                    SELECT 1 FROM onms.work_orders wo
                     WHERE wo.wonum = ob.wonum
               )
        )
        UPDATE onms.ot_bandeja
           SET asignacion_activa = false
          FROM huerfanas
         WHERE onms.ot_bandeja.asignacion_id = huerfanas.asignacion_id
        RETURNING onms.ot_bandeja.asignacion_id,
                  onms.ot_bandeja.wonum,
                  onms.ot_bandeja.cuadrilla_id,
                  huerfanas.chat_id_cuadrilla
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            filas = cur.fetchall()
            if not filas:
                resultado = []
            else:
                cols = [d[0] for d in cur.description]
                resultado = [dict(zip(cols, row)) for row in filas]
        conn.commit()
        return resultado
    except Exception:
        conn.rollback()
        raise