"""
bot/services/visita_fallida.py
------------------------------
Logica de BD para declarar visita fallida sobre una OT.

PROPOSITO:
    Cuando la cuadrilla declara que la visita al sitio fue fallida
    (ej. el problema no era de fibra, era del cliente, etc.), este
    servicio:
        1. Marca la OT como visita_fallida=true en ot_bandeja.
        2. Crea un worklog tipo 'visita_fallida' con la razon.
        3. Notifica al grupo de la cuadrilla.

REGLAS:
    - Solo se permite si la OT esta activa.
    - Solo se permite si visita_fallida ya no era true (idempotencia).
    - Despues de declarada, los comandos de avance se bloquean (eso lo
      controla el handler de cada comando, no este servicio).

NO HACE:
    - No envia mensajes Telegram (eso es del handler).
    - No valida estructura del texto (la razon es texto libre).

CONTRATOS PUBLICOS:
    obtener_ot_para_visita_fallida(asignacion_id, conn) -> dict | None
        Datos minimos para validar y mostrar confirmacion.

    marcar_visita_fallida(asignacion_id, razon, cuadrilla_id, conn) -> bool
        Hace el UPDATE + INSERT worklog. Idempotente: si ya estaba
        marcada, retorna False.

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

def marcar_visita_fallida(
    asignacion_id,
    razon,
    cuadrilla_id,
    conn,
    telegram_chat_id=None,
    telegram_chat_title=None,
    telegram_user_id=None,
    telegram_username=None,
    telegram_message_id=None,
):
    """
    Marca la OT como visita_fallida y crea el registro conversacional.

    Hace DOS cosas:
        1) UPDATE en ot_bandeja (visita_fallida=true).
        2) INSERT en bot_interacciones (log conversacional + datos
           para sincronizacion futura con Maximo).

    NO escribe en onms.worklogs porque esa tabla es una replica/cache
    de Maximo (todas las filas tienen worklog_id UNIQUE NOT NULL que
    asigna Maximo). Un proceso ETL inverso (PENDIENTE de implementar)
    leera bot_interacciones donde sincronizado_maximo=false y
    tipo_interaccion tiene genera_worklog_maximo=true en cat_tipo_interaccion,
    y se encargara de crear el worklog en Maximo. Cuando se sincronice,
    el ETL normal lo traera de vuelta a onms.worklogs con su worklog_id.

    Transicion:
        visita_fallida:  false -> true

    NO cambia estado ni fase_operativa: la OT sigue activa hasta
    que el CGR la mueva en Maximo y el ETL la limpie.

    Idempotencia: si ya estaba marcada (visita_fallida=true), no hace
    nada y retorna False.

    Parametros:
        asignacion_id        : PK de la fila en ot_bandeja
        razon                : texto libre que escribio la cuadrilla
        cuadrilla_id         : codigo de la cuadrilla que declara
        telegram_*           : datos del mensaje de Telegram para el
                               CHECK constraint chk_telegram_coherente.

    Retorna True si exitoso, False si no.
    """
    # 1) UPDATE en ot_bandeja con condicion para idempotencia
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
                f"asignacion_id={asignacion_id}. Probablemente ya estaba "
                f"declarada o la OT no esta activa."
            )
            return False
        wonum = row[0]

    # 2) INSERT en onms.bot_interacciones (log conversacional + fuente
    #    de verdad para sincronizacion futura con Maximo).
    #    Incluye todos los campos de Telegram para satisfacer el CHECK
    #    chk_telegram_coherente.
    sql_botint = """
        INSERT INTO onms.bot_interacciones (
            fecha_hora,
            asignacion_id,
            wonum,
            cuadrilla_id,
            tipo_interaccion,
            direccion,
            nivel_urgencia,
            actor_tipo,
            actor_id,
            telegram_chat_id,
            telegram_chat_title_snapshot,
            telegram_message_id,
            telegram_user_id,
            telegram_username,
            contenido_texto,
            metadata,
            estado_procesamiento
        )
        VALUES (
            NOW(),
            %s,
            %s,
            %s,
            'visita_fallida',
            'entrante',
            'alta',
            'cuadrilla',
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            'procesado'
        )
    """
    import json
    with conn.cursor() as cur:
        cur.execute(sql_botint, (
            asignacion_id,
            wonum,
            cuadrilla_id,
            cuadrilla_id,                       # actor_id
            telegram_chat_id,
            telegram_chat_title,
            telegram_message_id,
            telegram_user_id,
            telegram_username,
            f"/visita_fallida {razon}",         # contenido_texto
            json.dumps({                        # metadata
                "razon": razon,
                "description_long": razon,      # para uso del ETL inverso
            }),
        ))

    logger.info(
        f"[VisitaFallida] asignacion_id={asignacion_id} (wonum={wonum}) "
        f"marcada por cuadrilla={cuadrilla_id}. "
        f"bot_interaccion creado (pendiente sync Maximo)."
    )
    return True


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