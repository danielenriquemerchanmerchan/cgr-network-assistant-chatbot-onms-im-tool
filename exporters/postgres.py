"""
postgres_exporter.py
--------------------
Exporta registros OT a una tabla PostgreSQL usando psycopg2.execute_values
para bulk insert eficiente.

Comparte el estilo de conexion con database.py (MySQL) pero usa
credenciales PG_* de config.py.
"""

import psycopg2
from psycopg2.extras import execute_values
import logging

from config import PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DATABASE
from campos_gesfo import CAMPOS_OT, SPEC_CAMPOS
from exporters.base import Exporter


class PostgresExporter(Exporter):
    """Upsert de registros a una tabla PostgreSQL."""

    def __init__(
        self,
        tabla="ots_gesfo_4213",
        schema="public",
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        truncate_before_insert=False,
    ):
        self.tabla  = tabla
        self.schema = schema
        self.conn_params = dict(
            host=host, port=port, dbname=database,
            user=user, password=password,
        )
        self.truncate_before_insert = truncate_before_insert
        self.columnas = CAMPOS_OT + SPEC_CAMPOS

    # ── Conexion ───────────────────────────────────────────────
    def _conectar(self):
        return psycopg2.connect(**self.conn_params)

    # ── Export ─────────────────────────────────────────────────
    def export(self, registros):
        if not registros:
            logging.warning("PostgresExporter: sin registros para insertar")
            return f"PostgreSQL {self.schema}.{self.tabla}: 0 filas"

        conn = None
        try:
            conn = self._conectar()
            with conn.cursor() as cur:
                tabla_full = f'"{self.schema}"."{self.tabla}"'

                if self.truncate_before_insert:
                    cur.execute(f"TRUNCATE TABLE {tabla_full}")
                    logging.info(f"Tabla {tabla_full} truncada")

                cols_sql = ", ".join(f'"{c}"' for c in self.columnas)
                updates  = ", ".join(
                    f'"{c}"=EXCLUDED."{c}"'
                    for c in self.columnas if c != "wonum"
                )

                sql = (
                    f"INSERT INTO {tabla_full} ({cols_sql}) VALUES %s "
                    f'ON CONFLICT ("wonum") DO UPDATE SET {updates}'
                )

                valores = [
                    tuple(reg.get(c, "") for c in self.columnas)
                    for reg in registros
                ]

                execute_values(cur, sql, valores, page_size=500)
                n = cur.rowcount
            conn.commit()
            logging.info(f"PostgreSQL {self.schema}.{self.tabla}: {n} filas afectadas")
            return f"PostgreSQL {self.schema}.{self.tabla}: {n} filas"

        except Exception as e:
            logging.error(f"Error en PostgresExporter ({self.tabla}): {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()