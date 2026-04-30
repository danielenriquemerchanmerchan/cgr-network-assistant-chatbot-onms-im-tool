"""
client.py (Postgres del dashboard ONMS)
---------------------------------------
Funciones para operar sobre el schema 'onms' en Postgres.

PROPOSITO:
    Punto unico de acceso a la BD del dashboard. Encapsula UPSERT, deteccion
    de salidas, gestion de worklogs y limpieza de OTs viejas.

NO HACE:
    - No habla con Maximo (eso es de integrations/maximo/rest_api.py)
    - No transforma datos (eso es de domain/transformers/)
    - No orquesta nada (eso es de etl/bandeja_o_gesfo.py)

PATRON:
    Funciones sueltas que reciben la conexion como parametro. La conexion
    la maneja el orquestador (etl/bandeja_o_gesfo.py).
    Cada funcion hace su trabajo y deja un estado consistente.
"""

import logging
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import RealDictCursor

from core.config import PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DATABASE
from core.config import UMBRAL_FRESCA, UMBRAL_TIBIA, UMBRAL_ANTIGUA

# ════════════════════════════════════════════════════════════════════
# CONFIGURACION
# ════════════════════════════════════════════════════════════════════

SCHEMA = "onms"

# Campos de work_orders que el ETL escribe (sin metadata)
# Estos son los que se comparan en el UPSERT para detectar cambios
CAMPOS_WORK_ORDER = [
    # Identificadores y enriquecimiento
    "cinum", "ci_description", "description",
    # Estado y clasificacion
    "status", "etom_phase", "woclass", "worktype", "classstructureid",
    # Personas
    "reported_by", "assigned_to", "owner_group", "persongroup",
    # Ubicacion
    "location", "nom_ubicacion", "direccion",
    # Severidad
    "severity",
    # Fechas
    "creation_date", "actual_start",
    # SPEC_CAMPOS (en minusculas, igual que en BD)
    "eecc_cuadrilla_fo", "tipo_cuadrilla_fo", "operador_fo",
    "numero_caso_fo", "coordinador_red_fo", "lider_de_zona_fo",
    "responsable_zona_nivel3_fo", "persona_que_reporta", "area_que_reporta_fo",
    "numero_ot_gestot", "provisional", "iru_afectado", "outage_asociado",
    "tipo_tramo", "tipo_operacion_fo", "dist_optica", "origen_medida",
    "tipo_causa", "observ_cierre", "coordenada_corte_long", "coordenada_corte_lat",
    "parada_reloj", "tiempo_efect",
    # Metadata derivada
    "cant_worklogs", "clasificacion_operativa",
]

# Campos de worklogs que se insertan
CAMPOS_WORKLOG = [
    "wonum", "worklog_id",
    "createdate", "createby",
    "logtype", "logtype_description",
    "description", "description_long",
    "modifydate", "modifyby",
    "clientviewable",
]


# ════════════════════════════════════════════════════════════════════
# 1. CONEXION
# ════════════════════════════════════════════════════════════════════

def obtener_conexion():
    """
    Abre una conexion a Postgres usando las credenciales del .env.

    Retorna:
        psycopg2.connection o None si falla.
    """
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DATABASE,
        )
        # autocommit=False para tener control explicito de los commits
        conn.autocommit = False
        logging.info(f"[Postgres] Conexion abierta a {PG_DATABASE}@{PG_HOST}:{PG_PORT}")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"[Postgres] Error de conexion: {e}")
        return None


def cerrar_conexion(conn):
    """Cierra la conexion limpiamente."""
    if conn and not conn.closed:
        conn.close()
        logging.info("[Postgres] Conexion cerrada")


# ════════════════════════════════════════════════════════════════════
# 2. WORK ORDERS - LECTURA
# ════════════════════════════════════════════════════════════════════

def obtener_wonums_activos(conn):
    """
    Retorna un set con los wonums que actualmente estan activa=true en la BD.

    Sirve para hacer el "diff" con Maximo: las OTs en BD que ya no aparecen
    en Maximo son candidatas a marcarse como salidas.
    """
    sql = f"SELECT wonum FROM {SCHEMA}.work_orders WHERE activa = true"
    with conn.cursor() as cur:
        cur.execute(sql)
        return {row[0] for row in cur.fetchall()}
    
    
def obtener_wonums_operativas(conn):
    """
    Retorna un set con los wonums que estan activa=true y dentro de la
    ventana operativa (creadas o cerradas en los ultimos DIAS_VENTANA_OPERATIVA dias).

    Usado por el ETL operativo para detectar salidas: las OTs que estan en
    BD dentro de la ventana pero ya no aparecen en Maximo se marcan como
    salidas (cambio de ownergroup, status pasado a CAN, etc.).

    NO incluye OTs MUY_ANTIGUA (ya fuera de la ventana). Esas siguen en BD
    pero no se evaluan para detectar salidas en el operativo.

    Criterios de inclusion segun status:
        INPRG: reportdate >= hace DIAS_VENTANA_OPERATIVA dias
        COMP:  changedate >= hace DIAS_VENTANA_OPERATIVA dias
        CLOSE: actfinish  >= hace DIAS_VENTANA_OPERATIVA dias
    """
    sql = f"""
        SELECT wonum FROM {SCHEMA}.work_orders
        WHERE activa = true
          AND clasificacion_operativa IN ('FRESCA', 'TIBIA', 'ANTIGUA', 'SOLUCIONADO', 'DOCUMENTADO')
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return {row[0] for row in cur.fetchall()}


def reclasificar_envejecidas(conn):
    """
    Recalcula clasificacion_operativa para TODAS las OTs activas segun
    su edad actual. Se ejecuta al inicio de cada corrida del ETL para
    mantener la BD al dia con las categorias correctas.

    Reglas (umbrales en core/config.py):
        INPRG con < UMBRAL_FRESCA dias    → FRESCA
        INPRG con < UMBRAL_TIBIA dias     → TIBIA
        INPRG con < UMBRAL_ANTIGUA dias   → ANTIGUA
        INPRG con >= UMBRAL_ANTIGUA dias  → MUY_ANTIGUA
        COMP                               → SOLUCIONADO
        CLOSE                              → DOCUMENTADO

    Solo actualiza filas donde la categoria cambio realmente, para
    evitar UPDATEs innecesarios.

    Retorna: cantidad de OTs reclasificadas.
    """
    sql = f"""
        UPDATE {SCHEMA}.work_orders
        SET clasificacion_operativa = calc.nueva_categoria,
            ultima_actualizacion = NOW()
        FROM (
            SELECT wonum,
                CASE
                    WHEN status = 'COMP'  THEN 'SOLUCIONADO'
                    WHEN status = 'CLOSE' THEN 'DOCUMENTADO'
                    WHEN status = 'INPRG' THEN
                        CASE
                            WHEN EXTRACT(EPOCH FROM (NOW() - creation_date)) / 86400 < {UMBRAL_FRESCA}  THEN 'FRESCA'
                            WHEN EXTRACT(EPOCH FROM (NOW() - creation_date)) / 86400 < {UMBRAL_TIBIA}   THEN 'TIBIA'
                            WHEN EXTRACT(EPOCH FROM (NOW() - creation_date)) / 86400 < {UMBRAL_ANTIGUA} THEN 'ANTIGUA'
                            ELSE 'MUY_ANTIGUA'
                        END
                    ELSE clasificacion_operativa
                END AS nueva_categoria
            FROM {SCHEMA}.work_orders
            WHERE activa = true
        ) calc
        WHERE {SCHEMA}.work_orders.wonum = calc.wonum
          AND {SCHEMA}.work_orders.clasificacion_operativa IS DISTINCT FROM calc.nueva_categoria
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.rowcount


# ════════════════════════════════════════════════════════════════════
# 3. WORK ORDERS - UPSERT
# ════════════════════════════════════════════════════════════════════

def upsert_work_order(registro, conn):
    """
    Inserta o actualiza una OT en work_orders.

    Logica:
        - Si la OT no existe -> INSERT
        - Si existe y algo cambio -> UPDATE (solo cambia ultima_actualizacion)
        - Si existe y nada cambio -> nada
        - Siempre marca activa=true y salio_bandeja_at=NULL (porque acaba de
          aparecer en Maximo, asi que esta operacionalmente viva)

    Retorna:
        'INSERTED' | 'UPDATED' | 'UNCHANGED'
    """
    wonum = registro["wonum"]

    # Construir lista de columnas y placeholders dinamicamente
    columnas = ["wonum"] + CAMPOS_WORK_ORDER + ["activa", "ultima_actualizacion", "salio_bandeja_at"]
    valores = [wonum] + [registro.get(c) for c in CAMPOS_WORK_ORDER] + [True, datetime.now(), None]

    placeholders = ", ".join(["%s"] * len(columnas))
    columnas_sql = ", ".join(columnas)

    # Para el UPDATE en caso de conflicto, construimos el SET dinamicamente
    set_clauses = ", ".join([f"{c} = EXCLUDED.{c}" for c in columnas if c != "wonum"])

    # WHERE para detectar cambios reales: solo actualiza si algun campo difiere
    # (excluimos ultima_actualizacion del check porque siempre cambia)
    diff_check = " OR ".join([
        f"{SCHEMA}.work_orders.{c} IS DISTINCT FROM EXCLUDED.{c}"
        for c in CAMPOS_WORK_ORDER + ["activa", "salio_bandeja_at"]
    ])

    sql = f"""
        INSERT INTO {SCHEMA}.work_orders ({columnas_sql})
        VALUES ({placeholders})
        ON CONFLICT (wonum) DO UPDATE SET {set_clauses}
        WHERE {diff_check}
        RETURNING (xmax = 0) AS inserted
    """

    with conn.cursor() as cur:
        cur.execute(sql, valores)
        result = cur.fetchone()

        if result is None:
            # No hubo cambios (el WHERE bloqueo el UPDATE)
            return "UNCHANGED"
        elif result[0]:
            # xmax = 0 significa INSERT
            return "INSERTED"
        else:
            # xmax != 0 significa UPDATE
            return "UPDATED"


# ════════════════════════════════════════════════════════════════════
# 4. WORK ORDERS - SALIDAS DE BANDEJA
# ════════════════════════════════════════════════════════════════════

def marcar_salidas_bandeja(wonums_que_salieron, conn):
    """
    Marca las OTs que ya no aparecen en Maximo como salidas:
        activa = false
        salio_bandeja_at = NOW() (solo si era NULL, no se sobrescribe)

    Solo afecta OTs que actualmente tienen activa=true.

    Retorna: cantidad de OTs marcadas.
    """
    if not wonums_que_salieron:
        return 0

    sql = f"""
        UPDATE {SCHEMA}.work_orders
        SET activa = false,
            salio_bandeja_at = COALESCE(salio_bandeja_at, NOW()),
            ultima_actualizacion = NOW()
        WHERE wonum = ANY(%s)
          AND activa = true
    """

    with conn.cursor() as cur:
        cur.execute(sql, (list(wonums_que_salieron),))
        cantidad = cur.rowcount

    return cantidad


# ════════════════════════════════════════════════════════════════════
# 5. WORKLOGS - REEMPLAZO BULK
# ════════════════════════════════════════════════════════════════════

def reemplazar_worklogs(wonum, lista_worklogs, conn):
    """
    Reemplaza todos los worklogs de una OT.

    Estrategia: DELETE all + INSERT all en la misma transaccion.
    Es mas simple que diff worklog-por-worklog, y los volumenes son chicos
    (5-7 worklogs por OT en promedio).

    Argumentos:
        wonum: identificador de la OT
        lista_worklogs: lista de dicts con campos de worklog (ver CAMPOS_WORKLOG)
        conn: conexion abierta

    Retorna: cantidad de worklogs insertados.
    """
    with conn.cursor() as cur:
        # 1. DELETE existentes
        cur.execute(
            f"DELETE FROM {SCHEMA}.worklogs WHERE wonum = %s",
            (wonum,)
        )

        if not lista_worklogs:
            return 0

        # 2. INSERT nuevos
        columnas_sql = ", ".join(CAMPOS_WORKLOG)
        placeholders = ", ".join(["%s"] * len(CAMPOS_WORKLOG))

        sql = f"""
            INSERT INTO {SCHEMA}.worklogs ({columnas_sql})
            VALUES ({placeholders})
            ON CONFLICT (worklog_id) DO NOTHING
        """

        valores = []
        for w in lista_worklogs:
            fila = tuple(w.get(c) for c in CAMPOS_WORKLOG)
            valores.append(fila)

        cur.executemany(sql, valores)
        return len(valores)


# ════════════════════════════════════════════════════════════════════
# 6. MANTENIMIENTO - LIMPIEZA DE VIEJAS
# ════════════════════════════════════════════════════════════════════

def limpiar_viejas_salidas(dias, conn):
    """
    Elimina fisicamente las OTs que llevan mas de N dias fuera de la bandeja.

    Criterio:
        activa = false
        AND salio_bandeja_at < NOW() - INTERVAL 'N days'

    Por CASCADE, sus worklogs y bot_states tambien se eliminan automaticamente.

    Retorna: cantidad de OTs eliminadas.
    """
    sql = f"""
        DELETE FROM {SCHEMA}.work_orders
        WHERE activa = false
          AND salio_bandeja_at < NOW() - INTERVAL '%s days'
    """

    with conn.cursor() as cur:
        cur.execute(sql, (dias,))
        return cur.rowcount


# ════════════════════════════════════════════════════════════════════
# 7. ESTADISTICAS
# ════════════════════════════════════════════════════════════════════

def contar_filas(conn):
    """
    Retorna un dict con el conteo actual de cada tabla del schema onms.
    Util para logs y validacion post-ETL.
    """
    resultado = {}
    with conn.cursor() as cur:
        for tabla in ["work_orders", "worklogs", "bot_states"]:
            cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{tabla}")
            resultado[tabla] = cur.fetchone()[0]

        # Tambien contar las activas vs inactivas en work_orders
        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.work_orders WHERE activa = true")
        resultado["work_orders_activas"] = cur.fetchone()[0]

        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.work_orders WHERE activa = false")
        resultado["work_orders_inactivas"] = cur.fetchone()[0]

    return resultado
