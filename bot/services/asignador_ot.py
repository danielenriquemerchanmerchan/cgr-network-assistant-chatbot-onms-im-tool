"""
bot/services/asignador_ot.py
----------------------------
Asignador automatico de OTs nuevas a ot_bandeja.

PROPOSITO:
    Cuando el ETL trae OTs nuevas de Maximo a work_orders, este modulo
    decide cuales deben pasar a ot_bandeja y a que coordinador, e inserta
    las filas correspondientes.

NO HACE:
    - No envia mensajes de Telegram. Eso es trabajo del notificador
      (bot/services/notificador_ot.py).
    - No modifica work_orders. Solo lee.
    - No tiene scheduler propio. Se invoca desde el ETL al final de
      cada corrida de sincronizacion (etl/bandeja_o_gesfo.py).

REGLAS DE NEGOCIO ACTUALES:
    1. Solo se procesan OTs en estado 'INPRG' (en progreso). Las
       COMP/CLOSE/CAN no se asignan a coordinador (ya estan terminadas).
    2. Una OT entra a ot_bandeja solo si NO tiene asignacion activa
       (asignacion_activa=true) actualmente. Esto evita duplicados.
    3. Filtro opcional por cinum (config.ASIGNADOR_FILTRO_CINUM_LIKE).
       En piloto = None (todas las INPRG entran). Mas adelante = 'RBHFO%'.
    4. Coordinador se decide consultando onms.coordinador_zona:
        - Reglas con departamento especifico ganan sobre la default
          (menor prioridad = mayor precedencia).
        - Si ninguna regla calza (ni siquiera la default), se loguea
          warning y la OT entra a ot_bandeja con coordinador_id NULL.
          Politica acordada: registrar de todas formas para auditoria.
    5. Las filas nuevas usan el default de la columna estado
       (pendiente_acuse_coordinador) y notificacion_coordinador_*=NULL.
       El notificador las recogera en su proximo ciclo.

CONTRATO:
    asignar_ots_pendientes(conn) -> dict con stats
        conn: conexion de Postgres ABIERTA. El llamador la maneja.
        retorna: dict con conteos para que el ETL los logue
            {
                'evaluadas': N,        # OTs INPRG candidatas en work_orders
                'ya_en_bandeja': N,    # ya tenian asignacion activa
                'asignadas_ok': N,     # nueva fila en ot_bandeja con coord
                'sin_coordinador': N,  # nueva fila pero sin coord (warning)
                'errores': N,          # filas que no se pudieron insertar
            }
"""

import logging

from core.config import ASIGNADOR_FILTRO_CINUM_LIKE

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# 1. RESOLVER COORDINADOR PARA UNA OT
# ═══════════════════════════════════════════════════════════════════

def obtener_coordinador_para_ot(departamento, conn):
    """
    Consulta onms.coordinador_zona para decidir que coordinador atiende
    una OT segun su departamento.

    Logica de evaluacion (ya armada en la tabla):
        - Reglas activas con departamento = X tienen prioridad menor
          (10 por convencion) y ganan si la OT es del departamento X.
        - La regla default tiene departamento=NULL y prioridad alta (999).
          Aplica como catch-all cuando ninguna especifica calza.
        - Si una OT viene SIN departamento (NULL), las reglas especificas
          no aplican (porque NULL != 'Cundinamarca'), y solo gana la
          default. Esto es deseable: OTs sin departamento van al fallback.

    Argumentos:
        departamento : str | None — departamento de la OT (puede ser NULL)
        conn         : conexion Postgres abierta

    Retorna:
        str | None — coordinador_id; None si no hay regla aplicable
                     activa (caso raro pero posible si alguien desactivo
                     todas las reglas).
    """
    sql = """
        SELECT coordinador_id
          FROM onms.coordinador_zona
         WHERE activo = true
           AND (departamento IS NULL OR departamento = %s)
         ORDER BY prioridad ASC
         LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (departamento,))
        row = cur.fetchone()
        return row[0] if row else None


# ═══════════════════════════════════════════════════════════════════
# 2. LISTAR OTs CANDIDATAS A ASIGNAR
# ═══════════════════════════════════════════════════════════════════

def _listar_ots_candidatas(conn):
    """
    Devuelve las OTs en work_orders que:
      - estan en estado INPRG, y
      - NO tienen una asignacion activa en ot_bandeja, y
      - opcionalmente: cumplen el filtro cinum_like de config.

    Retorna lista de tuplas (wonum, departamento) — los unicos campos
    que el asignador necesita. Mas datos no hacen falta porque la fila
    de ot_bandeja se crea con NULLs en lo demas; el notificador y los
    handlers leen el resto desde work_orders cuando los necesiten.
    """
    sql_base = """
        SELECT wo.wonum, wo.departamento
          FROM onms.work_orders wo
         WHERE wo.status = 'INPRG'
           AND NOT EXISTS (
                SELECT 1
                  FROM onms.ot_bandeja ob
                 WHERE ob.wonum = wo.wonum
                 -- Sin filtro de asignacion_activa: si la OT ya tuvo
                 -- alguna asignacion (activa o no), no la volvemos a
                 -- asignar automaticamente. Las rechazadas por el coord
                 -- quedan en BD esperando reasignacion manual desde
                 -- el tablero del CGR (caso excepcional).
           )
    """
    params = []

    if ASIGNADOR_FILTRO_CINUM_LIKE:
        sql_base += " AND wo.cinum LIKE %s"
        params.append(ASIGNADOR_FILTRO_CINUM_LIKE)

    sql_base += " ORDER BY wo.creation_date ASC NULLS LAST"

    with conn.cursor() as cur:
        cur.execute(sql_base, params)
        return cur.fetchall()


# ═══════════════════════════════════════════════════════════════════
# 3. INSERTAR FILA EN ot_bandeja
# ═══════════════════════════════════════════════════════════════════

def _insertar_en_bandeja(wonum, coordinador_id, conn):
    """
    Inserta una fila nueva en ot_bandeja para la OT indicada.

    Notas:
        - estado se omite -> usa el DEFAULT 'pendiente_acuse_coordinador'.
        - asignacion_activa se omite -> usa el DEFAULT true.
        - fecha_asignacion_cgr se omite -> usa el DEFAULT NOW().
        - notificacion_coordinador_enviada_at queda NULL.
        - notificacion_coordinador_intentos queda 0 (default).
        - coordinador_asignado_id puede ser NULL si ninguna regla aplico.

    El UNIQUE INDEX idx_ot_bandeja_wonum_activa garantiza que no haya
    dos asignaciones activas para el mismo wonum. Si por alguna race
    condition se intentara insertar duplicado, falla con IntegrityError
    y el llamador lo cuenta como error.
    """
    sql = """
        INSERT INTO onms.ot_bandeja (wonum, coordinador_asignado_id)
        VALUES (%s, %s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (wonum, coordinador_id))


# ═══════════════════════════════════════════════════════════════════
# 4. ORQUESTADOR PUBLICO
# ═══════════════════════════════════════════════════════════════════

def asignar_ots_pendientes(conn):
    """
    Funcion principal. La invoca el ETL al final de cada corrida.

    Flujo:
        1. Lista OTs INPRG en work_orders sin asignacion activa.
        2. Para cada una: resuelve coordinador con coordinador_zona.
        3. INSERT en ot_bandeja.
        4. Commit por OT individual: si una falla, las anteriores
           ya quedaron persistidas. Esto es importante porque el ETL
           corre cada 10 min y queremos avanzar lo que se pueda.

    Retorna dict con conteos para que el ETL logue al final.
    """
    stats = {
        "evaluadas": 0,
        "asignadas_ok": 0,
        "sin_coordinador": 0,
        "errores": 0,
    }

    candidatas = _listar_ots_candidatas(conn)
    stats["evaluadas"] = len(candidatas)

    if not candidatas:
        logger.info("[Asignador] No hay OTs candidatas a asignar")
        return stats

    logger.info(f"[Asignador] {len(candidatas)} OT(s) candidata(s) a asignar")

    for wonum, departamento in candidatas:
        try:
            coord_id = obtener_coordinador_para_ot(departamento, conn)

            if coord_id is None:
                logger.warning(
                    f"[Asignador] OT {wonum} (dep={departamento!r}) sin "
                    f"coordinador asignable. Se inserta SIN coordinador "
                    f"(coordinador_asignado_id=NULL). Revisar reglas en "
                    f"onms.coordinador_zona."
                )
                stats["sin_coordinador"] += 1

            _insertar_en_bandeja(wonum, coord_id, conn)
            conn.commit()

            if coord_id is not None:
                stats["asignadas_ok"] += 1
                logger.info(
                    f"[Asignador] OT {wonum} (dep={departamento!r}) "
                    f"-> coord {coord_id}"
                )

        except Exception as e:
            conn.rollback()
            stats["errores"] += 1
            logger.error(
                f"[Asignador] Error asignando OT {wonum}: "
                f"{type(e).__name__}: {e}",
                exc_info=True,
            )

    return stats