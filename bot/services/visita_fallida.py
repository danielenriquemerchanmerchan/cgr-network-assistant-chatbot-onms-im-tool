"""
bot/services/visita_fallida.py
------------------------------
Logica de BD para declarar visita fallida sobre una OT.

PROPOSITO:
    Cuando la cuadrilla declara que la visita al sitio fue fallida
    (ej. el problema no era de fibra, era del cliente, etc.), este
    servicio marca la OT como visita_fallida=true en ot_bandeja.

REGLAS:
    - Solo se permite si la OT esta activa.
    - Solo se permite si visita_fallida ya no era true (idempotencia).
    - Despues de declarada, los comandos de avance se bloquean (eso lo
      controla el handler de cada comando, no este servicio).

NO HACE (patron B: servicio solo toca estado de negocio):
    - No escribe en bot_interacciones (lo hace el handler).
    - No envia mensajes Telegram (eso es del handler).
    - No valida estructura del texto (la razon es texto libre).

CONTRATOS PUBLICOS:
    obtener_ot_para_visita_fallida(asignacion_id, conn) -> dict | None
        Datos minimos para validar y mostrar confirmacion.

    marcar_visita_fallida(asignacion_id, conn) -> str | None
        Hace el UPDATE en ot_bandeja. Idempotente: si ya estaba
        marcada o la OT no esta activa, retorna None. Si exitoso,
        retorna el wonum.

    ot_esta_bloqueada_por_visita_fallida(asignacion_id, conn) -> bool
        Helper para que los handlers de avance verifiquen si la OT
        esta bloqueada.
"""

import logging

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# LECTURA: datos minimos de la OT
# ═══════════════════════════════════════════════════════════════════

def obtener_ot_para_visita_fallida(asignacion_id, conn):
    """
    Devuelve dict con wonum, cuadrilla_id, cuadrilla_nombre, visita_fallida.
    None si no existe la asignacion.
    """
    sql = """
        SELECT ob.asignacion_id,
               ob.wonum,
               ob.cuadrilla_id,
               cu.nombre AS cuadrilla_nombre,
               ob.visita_fallida,
               ob.asignacion_activa
          FROM onms.ot_bandeja ob
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
# ESCRITURA: declarar visita fallida
# ═══════════════════════════════════════════════════════════════════

def marcar_visita_fallida(asignacion_id, conn):
    """
    Marca la OT como visita_fallida=true en ot_bandeja.

    Solo toca estado de negocio. El log conversacional en
    bot_interacciones lo registra el handler (patron B).

    Transicion:
        visita_fallida:  false -> true

    NO cambia estado ni fase_operativa: la OT sigue activa hasta
    que el CGR la mueva en Maximo y el ETL la limpie.

    Idempotencia: si ya estaba marcada (visita_fallida=true) o la
    OT no esta activa, no hace nada y retorna None.

    NO escribe en onms.worklogs porque esa tabla es una replica/cache
    de Maximo (todas las filas tienen worklog_id UNIQUE NOT NULL que
    asigna Maximo). Un proceso ETL inverso (PENDIENTE de implementar)
    leera bot_interacciones donde sincronizado_maximo=false y
    tipo_interaccion tiene genera_worklog_maximo=true en
    cat_tipo_interaccion, y se encargara de crear el worklog en
    Maximo. Cuando se sincronice, el ETL normal lo traera de vuelta
    a onms.worklogs con su worklog_id.

    Parametros:
        asignacion_id : PK de la fila en ot_bandeja
        conn          : conexion abierta (no hace commit; el caller decide)

    Retorna:
        wonum (str) si se actualizo.
        None si no se pudo (ya estaba marcada o OT inactiva).
    """
    sql_update = """
        UPDATE onms.ot_bandeja
           SET visita_fallida = true
         WHERE asignacion_id = %s
           AND asignacion_activa = true
           AND visita_fallida = false
        RETURNING wonum
    """
    with conn.cursor() as cur:
        cur.execute(sql_update, (asignacion_id,))
        row = cur.fetchone()
        if row is None:
            logger.warning(
                f"[VisitaFallida] No se actualizo nada para "
                f"asignacion_id={asignacion_id}. Probablemente ya "
                f"estaba declarada o la OT no esta activa."
            )
            return None
        wonum = row[0]

    logger.info(
        f"[VisitaFallida] asignacion_id={asignacion_id} (wonum={wonum}) "
        f"marcada visita_fallida=true."
    )
    return wonum


# ═══════════════════════════════════════════════════════════════════
# CONSULTA: ¿la OT esta bloqueada?
# ═══════════════════════════════════════════════════════════════════

def ot_esta_bloqueada_por_visita_fallida(asignacion_id, conn):
    """
    Helper para que cualquier handler de avance verifique antes de
    actuar: ¿esta OT esta bloqueada por una declaracion de visita
    fallida previa?

    Retorna True si la OT esta bloqueada.
    """
    sql = """
        SELECT visita_fallida
          FROM onms.ot_bandeja
         WHERE asignacion_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (asignacion_id,))
        row = cur.fetchone()
        if row is None:
            return False  # no existe la OT, que falle por otra parte
        return bool(row[0])