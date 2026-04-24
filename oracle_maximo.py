import cx_Oracle
import logging
from config import ORACLE_USER, ORACLE_PSW, ORACLE_DSN


def _conectar():
    """Retorna una conexion activa a Oracle Maximo."""
    return cx_Oracle.connect(
        user=ORACLE_USER,
        password=ORACLE_PSW,
        dsn=ORACLE_DSN,
        encoding="UTF-8"
    )


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