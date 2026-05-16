"""
client.py (Postgres del dashboard ONMS)
---------------------------------------
Funciones para operar sobre el schema 'onms' en Postgres.

PROPOSITO:
    Punto unico de acceso a la BD del dashboard. Encapsula UPSERT,
    gestion de worklogs y limpieza por estado.

NO HACE:
    - No habla con Maximo (eso es de integrations/maximo/rest_api.py)
    - No transforma datos (eso es de domain/transformers/)
    - No orquesta nada (eso es de etl/bandeja_o_gesfo.py)

PATRON:
    Funciones sueltas que reciben la conexion como parametro. La conexion
    la maneja el orquestador (etl/bandeja_o_gesfo.py).

MODELO (rediseno mayo 2026):
    - Un solo ETL que sincroniza work_orders con Maximo.
    - Tablero muestra: INPRG (<DIAS_INPRG_RECIENTES) + COMP/CLOSE/CAN (<DIAS_RETENCION_CERRADAS).
    - Las OTs que ya no estan en el modelo se eliminan fisicamente (CASCADE de worklogs).
    - 'primera_aparicion' se llena solo en INSERT (no se sobrescribe en UPDATE).
"""

import logging
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values  

from core.config import (
    PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DATABASE,
)


# ═══════════════════════════════════════════════════════════════════
# CONFIGURACION
# ═══════════════════════════════════════════════════════════════════

SCHEMA = "onms"

# Campos de work_orders que el ETL escribe (sin metadata).
# Estos son los que se comparan en el UPSERT para detectar cambios.
CAMPOS_WORK_ORDER = [
    # Identificadores y enriquecimiento
    "cinum", "ci_description", "description",
    # Estado
    "status", "etom_phase", "woclass", "worktype", "classstructureid",
    # Personas
    "reported_by", "assigned_to", "owner_group", "persongroup",
    # Ubicacion
    "location", "nom_ubicacion", "direccion",
    # Severidad
    "severity",
    # Fechas
    "creation_date", "actual_start", "actual_finish",
    "changedate", "statusdate",
    # SPEC_CAMPOS (en minusculas, igual que en BD)
    "eecc_cuadrilla_fo", "tipo_cuadrilla_fo", "operador_fo",
    "numero_caso_fo", "coordinador_red_fo", "lider_de_zona_fo",
    "responsable_zona_nivel3_fo", "persona_que_reporta", "area_que_reporta_fo",
    "numero_ot_gestot", "provisional", "iru_afectado", "outage_asociado",
    "tipo_tramo", "tipo_operacion_fo", "dist_optica", "origen_medida",
    "tipo_causa", "observ_cierre", "coordenada_corte_long", "coordenada_corte_lat",
    "parada_reloj", "tiempo_efect",
    # Enriquecimiento via Oracle
    "ciudad", "departamento",
    # Metadata derivada
    "cant_worklogs",
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


# ═══════════════════════════════════════════════════════════════════
# 1. CONEXION
# ═══════════════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════════════
# 2. WORK ORDERS - UPSERT
# ═══════════════════════════════════════════════════════════════════

def upsert_work_order(registro, conn):
    """
    Inserta o actualiza una OT en work_orders.

    Logica:
        - Si la OT no existe -> INSERT (incluye primera_aparicion = NOW())
        - Si existe y algo cambio -> UPDATE (NO toca primera_aparicion)
        - Si existe y nada cambio -> nada

    El campo primera_aparicion se llena solo al insertar y nunca se
    sobrescribe. Asi capturamos aproximadamente cuando la OT entro en
    O_GESFO (con margen de error de minutos por la frecuencia del ETL).

    Retorna:
        'INSERTED' | 'UPDATED' | 'UNCHANGED'
    """
    wonum = registro["wonum"]
    ahora = datetime.now()

    # Lista de columnas a insertar (incluye primera_aparicion solo para INSERT)
    columnas = ["wonum"] + CAMPOS_WORK_ORDER + ["primera_aparicion", "ultima_actualizacion"]
    valores = [wonum] + [registro.get(c) for c in CAMPOS_WORK_ORDER] + [ahora, ahora]

    placeholders = ", ".join(["%s"] * len(columnas))
    columnas_sql = ", ".join(columnas)

    # Para el UPDATE en caso de conflicto: actualizar todos los campos
    # EXCEPTO primera_aparicion (que solo debe llenarse al insertar).
    columnas_update = CAMPOS_WORK_ORDER + ["ultima_actualizacion"]
    set_clauses = ", ".join([f"{c} = EXCLUDED.{c}" for c in columnas_update])

    # WHERE para detectar cambios reales: solo actualiza si algun campo difiere.
    # (Excluimos ultima_actualizacion del check porque siempre cambia.)
    diff_check = " OR ".join([
        f"{SCHEMA}.work_orders.{c} IS DISTINCT FROM EXCLUDED.{c}"
        for c in CAMPOS_WORK_ORDER
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
            # El WHERE bloqueo el UPDATE: no hubo cambios reales
            return "UNCHANGED"
        elif result[0]:
            # xmax = 0 significa INSERT
            return "INSERTED"
        else:
            # xmax != 0 significa UPDATE
            return "UPDATED"


# ═══════════════════════════════════════════════════════════════════
# 3. WORKLOGS - REEMPLAZO BULK
# ═══════════════════════════════════════════════════════════════════

# def reemplazar_worklogs(wonum, lista_worklogs, conn):
#     """
#     Reemplaza todos los worklogs de una OT.

#     Estrategia: DELETE all + INSERT all en la misma transaccion.
#     Es mas simple que diff worklog-por-worklog, y los volumenes son
#     chicos (5-7 worklogs por OT en promedio).

#     Argumentos:
#         wonum: identificador de la OT
#         lista_worklogs: lista de dicts con campos de worklog
#         conn: conexion abierta

#     Retorna: cantidad de worklogs insertados.
#     """
#     with conn.cursor() as cur:
#         # 1. DELETE existentes
#         cur.execute(
#             f"DELETE FROM {SCHEMA}.worklogs WHERE wonum = %s",
#             (wonum,)
#         )

#         if not lista_worklogs:
#             return 0

#         # 2. INSERT nuevos
#         columnas_sql = ", ".join(CAMPOS_WORKLOG)
#         placeholders = ", ".join(["%s"] * len(CAMPOS_WORKLOG))

#         sql = f"""
#             INSERT INTO {SCHEMA}.worklogs ({columnas_sql})
#             VALUES ({placeholders})
#             ON CONFLICT (worklog_id) DO NOTHING
#         """

#         valores = []
#         for w in lista_worklogs:
#             fila = tuple(w.get(c) for c in CAMPOS_WORKLOG)
#             valores.append(fila)

#         cur.executemany(sql, valores)
#         return len(valores)
    
    
# ═══════════════════════════════════════════════════════════════════
# IMPORTANTE: agrega este import al inicio de postgres/client.py,
# junto a los demas imports de psycopg2:
#
#     from psycopg2.extras import execute_values
#
# Luego reemplaza la funcion reemplazar_worklogs existente por esta:
# ═══════════════════════════════════════════════════════════════════

def reemplazar_worklogs(wonum, lista_worklogs, conn):
    """
    Reemplaza todos los worklogs de una OT.

    Estrategia: DELETE all + INSERT all en la misma transaccion.
    Es mas simple que diff worklog-por-worklog, y los volumenes son
    chicos (5-7 worklogs por OT en promedio).

    [OPTIMIZACION 2026-05]
    Usa execute_values en lugar de executemany. Diferencia clave:
        - executemany: emite N INSERTs separados (uno por fila).
          Con 3284 worklogs/ciclo = 3284 roundtrips a Postgres.
        - execute_values: emite UN INSERT con todas las filas como
          tuplas adicionales en el VALUES. Un solo roundtrip por OT.

    Argumentos:
        wonum: identificador de la OT
        lista_worklogs: lista de dicts con campos de worklog
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

        # 2. INSERT nuevos en un solo roundtrip
        columnas_sql = ", ".join(CAMPOS_WORKLOG)

        # Nota: con execute_values el placeholder es UN SOLO `%s`
        # despues de VALUES. La libreria expande internamente a
        # VALUES (col1, col2, ...), (col1, col2, ...), ...
        sql = f"""
            INSERT INTO {SCHEMA}.worklogs ({columnas_sql})
            VALUES %s
            ON CONFLICT (worklog_id) DO NOTHING
        """

        valores = [
            tuple(w.get(c) for c in CAMPOS_WORKLOG)
            for w in lista_worklogs
        ]

        execute_values(cur, sql, valores, page_size=200)
        return len(valores)    


# ═══════════════════════════════════════════════════════════════════
# 4. LIMPIEZA
# ═══════════════════════════════════════════════════════════════════

def limpiar_fuera_de_modelo(conn, dias_cerradas=14):
    """
    Elimina fisicamente OTs que ya no deben estar en el tablero:
        - status NOT IN ('INPRG')
        - AND statusdate < hace dias_cerradas dias

    Las INPRG no se borran aqui (esa limpieza la hace borrar_wonums()
    cuando una INPRG sale del filtro server-side de Maximo, ej. por
    superar DIAS_INPRG_RECIENTES).

    Por CASCADE de la FK, sus worklogs tambien se eliminan.

    Argumentos:
        conn: conexion abierta
        dias_cerradas: dias de retencion para COMP/CLOSE/CAN (default 14)

    Retorna: cantidad de OTs eliminadas.
    """
    sql = f"""
        DELETE FROM {SCHEMA}.work_orders
        WHERE status != 'INPRG'
          AND statusdate < NOW() - (INTERVAL '1 day' * %s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (dias_cerradas,))
        return cur.rowcount


def borrar_wonums(conn, wonums_a_borrar):
    """
    Borra OTs especificas por wonum. Se usa cuando una OT desaparecio
    de Maximo o ya no entra al modelo (ej. INPRG con reportdate
    fuera de DIAS_INPRG_RECIENTES).

    Argumentos:
        conn: conexion abierta
        wonums_a_borrar: iterable de wonums (set o lista)

    Retorna: cantidad de OTs eliminadas.
    """
    if not wonums_a_borrar:
        return 0
    sql = f"DELETE FROM {SCHEMA}.work_orders WHERE wonum = ANY(%s)"
    with conn.cursor() as cur:
        cur.execute(sql, (list(wonums_a_borrar),))
        return cur.rowcount


# ═══════════════════════════════════════════════════════════════════
# 5. ESTADISTICAS
# ═══════════════════════════════════════════════════════════════════

def contar_filas(conn):
    """
    Retorna un dict con el conteo actual de cada tabla del schema onms.
    Util para logs y validacion post-ETL.
    """
    resultado = {}
    with conn.cursor() as cur:
        for tabla in ["work_orders", "worklogs"]:
            cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{tabla}")
            resultado[tabla] = cur.fetchone()[0]

        # Conteo por status (informativo)
        cur.execute(f"""
            SELECT status, COUNT(*)
            FROM {SCHEMA}.work_orders
            GROUP BY status
            ORDER BY COUNT(*) DESC
        """)
        resultado["por_status"] = dict(cur.fetchall())

    return resultado