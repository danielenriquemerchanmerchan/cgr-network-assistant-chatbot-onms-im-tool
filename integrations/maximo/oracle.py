"""
oracle.py
---------
Acceso a Oracle de Maximo. Dos casos de uso:

1. CONSULTA PUNTUAL (1 sitio):
   - obtener_info_sitio(location) -> dict
   - enriquecer_ot(ot_data) -> dict
   Usadas por consumidores que necesitan info de un sitio especifico.
   Cada llamada abre/cierra conexion Oracle.

2. CONSULTA MASIVA (todos los sitios):
   - cargar_cache_sitios() -> dict[location -> info_sitio]
   - aplicar_sitio_a_registro(registro, cache) -> registro
   Usadas por el ETL bandeja_o_gesfo.py que procesa cientos de OTs por
   corrida. Una sola query trae todos los sitios al inicio, luego los
   lookups son en memoria. Evita 250+ conexiones Oracle por corrida.
"""

import cx_Oracle
import logging
from core.config import ORACLE_USER, ORACLE_PSW, ORACLE_DSN


# ═══════════════════════════════════════════════════════════════════
# CONEXION
# ═══════════════════════════════════════════════════════════════════

def _conectar():
    """Retorna una conexion activa a Oracle Maximo."""
    return cx_Oracle.connect(
        user=ORACLE_USER,
        password=ORACLE_PSW,
        dsn=ORACLE_DSN,
        encoding="UTF-8"
    )


# ═══════════════════════════════════════════════════════════════════
# 1. CONSULTA PUNTUAL (1 sitio)
# ═══════════════════════════════════════════════════════════════════

def obtener_info_sitio(location):
    """
    Consulta BD_V_FLM_SITIOS para obtener informacion geografica
    de un sitio a partir de su codigo de location (ej: 'S7821').

    Parametros:
        location (str): codigo del sitio. Ej: 'S7821'

    Retorna:
        dict con los campos del sitio, o None si no se encontro.

        Campos del dict:
            cilocation   -> codigo del sitio. Ej: 'S7821'
            nom_sitio    -> nombre del sitio
            ciudad       -> municipio. Ej: 'IBAGUE'
            depto        -> departamento. Ej: 'TOLIMA'
            direccion    -> direccion fisica del sitio
            aliado       -> empresa aliada (EECC)
    """
    if not location:
        return None

    try:
        conn = _conectar()
        cur  = conn.cursor()
        cur.execute("""
            SELECT cilocation, nom_sitio, ciudad, depto, direccion, aliado
            FROM maximo.bd_v_flm_sitios
            WHERE cilocation = :location
            AND rownum = 1
        """, {"location": location})

        row = cur.fetchone()
        if not row:
            logging.warning(f"Sitio '{location}' no encontrado en BD_V_FLM_SITIOS")
            return None

        return {
            "cilocation": row[0],
            "nom_sitio":  row[1],
            "ciudad":     row[2],
            "depto":      row[3],
            "direccion":  row[4],
            "aliado":     row[5],
        }

    except Exception as e:
        logging.error(f"Error consultando sitio '{location}' en Oracle: {e}")
        return None
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def enriquecer_ot(ot_data):
    """
    Enriquece una OT con informacion geografica del sitio.

    Toma el dict retornado por maximo_wo.consultar_ot() y agrega
    los campos ciudad, departamento y direccion consultando Oracle.

    NOTA: Para uso intensivo (muchas OTs en un loop) usar mejor el
    cache via cargar_cache_sitios() + aplicar_sitio_a_registro().

    Parametros:
        ot_data (dict): dict retornado por consultar_ot()

    Retorna:
        El mismo dict con los campos adicionales:
            ciudad       -> municipio del sitio
            departamento -> departamento del sitio
            direccion    -> direccion fisica del sitio
            aliado       -> empresa aliada
            nom_sitio    -> nombre del sitio
    """
    # Valores por defecto en caso de no encontrar el sitio
    ot_data["ciudad"]       = None
    ot_data["departamento"] = None
    ot_data["direccion"]    = None
    ot_data["aliado"]       = None
    ot_data["nom_sitio"]    = None

    location = ot_data.get("location") or ot_data.get("raw", {}).get("location")

    if not location:
        logging.warning(f"OT {ot_data.get('wonum')} no tiene campo location")
        return ot_data

    info = obtener_info_sitio(location)

    if info:
        ot_data["ciudad"]       = info.get("ciudad")
        ot_data["departamento"] = info.get("depto")
        ot_data["direccion"]    = info.get("direccion")
        ot_data["aliado"]       = info.get("aliado")
        ot_data["nom_sitio"]    = info.get("nom_sitio")
        logging.info(
            f"OT {ot_data.get('wonum')} enriquecida: "
            f"{info.get('ciudad')} - {info.get('depto')}"
        )
    else:
        logging.warning(
            f"OT {ot_data.get('wonum')}: no se encontro info para location '{location}'"
        )

    return ot_data


# ═══════════════════════════════════════════════════════════════════
# 2. CONSULTA MASIVA (cache para el ETL)
# ═══════════════════════════════════════════════════════════════════

def cargar_cache_sitios():
    """
    Carga TODOS los sitios desde maximo.lochierarchy + maximo.locations
    en un dict en memoria. Una sola query Oracle al inicio del ETL evita
    cientos de conexiones repetidas durante el procesamiento.

    USA LA JERARQUIA GEOGRAFICA DE MAXIMO (systemid='GEO'):
        S{xxx} -> L{xxx} -> M{xxx} -> D{xx} -> P{xx}
        sitio   localidad  municipio  depto   pais

    Esta jerarquia cubre ~25,000 sitios (vs ~10,000 de bd_v_flm_sitios)
    y es la fuente raiz que la UI de Maximo usa para mostrar municipio
    y departamento.

    Retorna:
        dict {location: info_sitio}
        donde info_sitio es un dict con: cilocation, nom_sitio, ciudad, depto.

        Si la query falla, retorna {} (dict vacio). El ETL continua
        sin enriquecimiento — las OTs quedan con ciudad/departamento
        en None.
    """
    cache = {}
    try:
        conn = _conectar()
        cur = conn.cursor()
        cur.execute("""
            WITH jerarquia AS (
                SELECT 
                    sitio.location AS cilocation,
                    loc.parent AS cod_municipio,
                    mun.parent AS cod_departamento
                FROM maximo.lochierarchy sitio
                LEFT JOIN maximo.lochierarchy loc 
                    ON loc.location = sitio.parent
                   AND loc.systemid = 'GEO'
                LEFT JOIN maximo.lochierarchy mun
                    ON mun.location = loc.parent
                   AND mun.systemid = 'GEO'
                WHERE sitio.systemid = 'GEO'
                  AND (sitio.location LIKE 'S%' OR sitio.location LIKE 'C%')
            )
            SELECT 
                j.cilocation,
                locs_sitio.description AS nom_sitio,
                locs_mun.description AS ciudad,
                locs_dep.description AS departamento
            FROM jerarquia j
            LEFT JOIN maximo.locations locs_sitio ON locs_sitio.location = j.cilocation
            LEFT JOIN maximo.locations locs_mun ON locs_mun.location = j.cod_municipio
            LEFT JOIN maximo.locations locs_dep ON locs_dep.location = j.cod_departamento
        """)

        for row in cur:
            cilocation = row[0]
            cache[cilocation] = {
                "cilocation": row[0],
                "nom_sitio":  row[1],
                "ciudad":     row[2],
                "depto":      row[3],
            }

        logging.info(f"[Oracle] Cache de sitios cargado: {len(cache)} sitios")
        return cache

    except Exception as e:
        logging.error(f"[Oracle] Error cargando cache de sitios: {e}")
        return {}
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def aplicar_sitio_a_registro(registro, cache_sitios):
    """
    Agrega los campos ciudad y departamento a un registro de OT
    haciendo lookup en el cache de sitios.

    No agrega direccion porque el campo direccion ya existe en
    work_orders y viene de Maximo (campo distinto).

    Parametros:
        registro (dict): registro de OT con al menos el campo 'location'.
        cache_sitios (dict): el dict retornado por cargar_cache_sitios().

    Retorna:
        El mismo dict con los campos ciudad y departamento agregados.
        Si no se encuentra el sitio, los deja en None.
    """
    registro["ciudad"]       = None
    registro["departamento"] = None

    location = registro.get("location")
    if not location:
        return registro

    info = cache_sitios.get(location)
    if info:
        registro["ciudad"]       = info.get("ciudad")
        registro["departamento"] = info.get("depto")

    return registro