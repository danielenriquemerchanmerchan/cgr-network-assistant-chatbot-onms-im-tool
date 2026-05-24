# scripts/sync_personas_maximo.py
import cx_Oracle     # o cx_Oracle si usás esa lib
from integrations.postgres.client import obtener_conexion
from core.config import (
    ORACLE_USER, ORACLE_PSW, ORACLE_DSN,
)

# 1. Conectar a Oracle Maximo
ora = cx_Oracle.connect(user=ORACLE_USER, password=ORACLE_PSW, dsn=ORACLE_DSN)
ora_cur = ora.cursor()
ora_cur.execute("""
    SELECT personid, displayname, status, department, location
    FROM   maximo.person
    WHERE  status = 'ACTIVE'
""")
filas = ora_cur.fetchall()
print(f"Personas obtenidas de Maximo: {len(filas):,}")

# 2. Conectar a Postgres y cargar
pg = obtener_conexion()
with pg.cursor() as cur:
    # Limpiar antes (estrategia "full refresh")
    cur.execute("TRUNCATE TABLE onms.cat_persona_maximo;")

    # Insertar en bulk con execute_values (mucho más rápido que loop)
    from psycopg2.extras import execute_values
    sql = """
        INSERT INTO onms.cat_persona_maximo
            (personid, displayname, status, department, location)
        VALUES %s
    """
    execute_values(cur, sql, filas, page_size=500)

pg.commit()
print("OK")

ora_cur.close()
ora.close()
pg.close()