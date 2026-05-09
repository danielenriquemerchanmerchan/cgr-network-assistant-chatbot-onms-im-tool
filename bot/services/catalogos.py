"""
bot/services/catalogos.py
-------------------------
Servicios de lectura de catálogos onms.cat_*

PROPOSITO:
    Centralizar las consultas a las tablas catálogo del schema onms.
    Estos servicios SIEMPRE leen de BD (no hay caché). Asi cualquier
    cambio en los catálogos se refleja inmediatamente en el bot
    sin reiniciar.

CATALOGOS EXPUESTOS:
    - obtener_grupos_causal_activos()        → cat_grupo_causal_parada
    - obtener_causales_de_grupo(tipo_grupo)  → cat_causal_parada filtrado
"""

import logging

from integrations.postgres.client import obtener_conexion, cerrar_conexion

logger = logging.getLogger(__name__)


def obtener_grupos_causal_activos():
    """
    Lee los grupos de causales activos del catalogo cat_grupo_causal_parada.

    Retorna:
        list de dicts con keys: codigo, descripcion, descripcion_corta, 
                                emoji, orden_visualizacion
        Lista vacia si no hay grupos o si fallo la consulta.
    """
    conn = None
    try:
        conn = obtener_conexion()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT codigo, descripcion, descripcion_corta, 
                       emoji, orden_visualizacion
                FROM onms.cat_grupo_causal_parada
                WHERE activo = true
                ORDER BY orden_visualizacion, codigo
                """
            )
            rows = cur.fetchall()
            return [
                {
                    "codigo":              row[0],
                    "descripcion":         row[1],
                    "descripcion_corta":   row[2],
                    "emoji":               row[3],
                    "orden_visualizacion": row[4],
                }
                for row in rows
            ]
    except Exception as e:
        logger.error(f"Error obteniendo grupos de causal: {e}")
        return []
    finally:
        if conn:
            cerrar_conexion(conn)


def obtener_causales_de_grupo(tipo_grupo):
    """
    Lee las causales activas de un grupo especifico.

    Argumentos:
        tipo_grupo: str (ej. 'espera', 'bloqueo', 'riesgo', 'otro')

    Retorna:
        list de dicts con keys: codigo, descripcion, emoji, orden_visualizacion
        Lista vacia si no hay causales o si fallo la consulta.
    """
    conn = None
    try:
        conn = obtener_conexion()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT codigo, descripcion, emoji, orden_visualizacion
                FROM onms.cat_causal_parada
                WHERE activo = true AND tipo_grupo = %s
                ORDER BY orden_visualizacion, descripcion
                """,
                (tipo_grupo,),
            )
            rows = cur.fetchall()
            return [
                {
                    "codigo":              row[0],
                    "descripcion":         row[1],
                    "emoji":               row[2],
                    "orden_visualizacion": row[3],
                }
                for row in rows
            ]
    except Exception as e:
        logger.error(f"Error obteniendo causales de grupo {tipo_grupo}: {e}")
        return []
    finally:
        if conn:
            cerrar_conexion(conn)
            
def obtener_estimados_duracion_activos():
    """
    Lee de la BD los estimados de duracion activos de cat_estimado_duracion,
    ordenados por minutos_minimo (mas corto primero).

    Retorna lista de dicts:
        [
            {
                'codigo': 'hasta_30min',
                'descripcion': 'Hasta 30 minutos',
                'requiere_atencion': False
            },
            ...
        ]

    Si no hay estimados activos, retorna lista vacia.
    """
    conn = None
    try:
        conn = obtener_conexion()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT codigo, descripcion, requiere_atencion
                FROM onms.cat_estimado_duracion
                WHERE activo = true
                ORDER BY 
                    CASE WHEN minutos_minimo IS NULL THEN 999999 
                         ELSE minutos_minimo 
                    END,
                    codigo
                """
            )
            rows = cur.fetchall()
            return [
                {
                    "codigo": row[0],
                    "descripcion": row[1],
                    "requiere_atencion": row[2],
                }
                for row in rows
            ]
    except Exception as e:
        logger.error(f"Error obteniendo estimados de duracion: {e}")
        return []
    finally:
        if conn:
            cerrar_conexion(conn)