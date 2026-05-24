"""
05_actualizar_specs_desde_catalogo.py
-------------------------------------
PRUEBA DE INTEGRACION: actualizar las especificaciones de una OT en
Maximo DESARROLLO con el payload de specs armado dinamicamente desde
las tablas catalogo en Postgres.

A diferencia de 02_actualizar_specs.py (que usa JSON hardcodeado), este
script simula el flujo que ejecutara el frontend NOC:

  1. Toma el wonum de la OT creada por el script 04.
  2. Resuelve el href de esa OT (Maximo lo necesita para el PATCH).
  3. Consulta cat_coord_fibra con (depto, municipio) -> EECC + coord red.
  4. Consulta cat_coord_pi con depto             -> lider de zona.
  5. Combina catalogos + constantes + inputs simulados.
  6. Arma el payload de specs y llama a actualizar_ot(href, payload).

NOTA: en el script 06 (end-to-end), el href ya viene en el resultado de
crear_ot(), asi que no hay que resolverlo. Aqui lo resolvemos solo
porque arrancamos con el wonum hardcoded.

ENTRADAS:
    - WONUM hardcodeado (el que devolvio el script 04).
    - PUNTA_DESPACHO + DEVICE_DESPACHO simulados (para resolver depto/muni).
    - .env apuntando a DEV (10.80.123.13).
    - PG_* apuntando a 192.168.44.114 schema onms.

EJECUCION:
    py -m tests.integracion_maximo.05_actualizar_specs_desde_catalogo
"""

import core.logging_setup  # noqa: F401 — inicializa logging del proyecto

import logging
import requests
from requests.auth import HTTPBasicAuth

from core.config import (
    MAXIMO_URL as URL_BASE,
    MAXIMO_USER as USERNAME,
    MAXIMO_PASSWORD as PASSWORD,
    MAXIMO_TIMEOUT as TIMEOUT,
)
from integrations.postgres.client import obtener_conexion, cerrar_conexion
from integrations.maximo.rest_api import actualizar_ot
from tests.integracion_maximo._comun import (
    guard_ambiente_dev,
    imprimir_resultado,
)


# ═══════════════════════════════════════════════════════════════════
# INPUT: WONUM de la OT creada por el script 04
# ═══════════════════════════════════════════════════════════════════
# Reemplazar este valor con el wonum que devuelva la corrida del 04.

WONUM = "7941578"   # <-- AJUSTAR con el wonum nuevo despues de correr 04


# ═══════════════════════════════════════════════════════════════════
# INPUTS SIMULADOS DEL FRONTEND NOC
# ═══════════════════════════════════════════════════════════════════
# En el script 06 (end-to-end) estos datos llegan directo desde el script 04.
# Aqui los hardcodeamos porque estamos probando el 05 aislado.

# Identificacion del lado de despacho (para resolver depto/municipio)
PUNTA_A          = "ANT_BEL_BELL_H401"
PUNTA_B          = "ANT_MED_PRAD_H501_D063"
PUNTA_DESPACHO   = "destino"        # "origen" o "destino"

# Inputs capturados del operador en el frontend
PERSONA_QUE_REPORTA  = "SANDRO.GUAYARA"   # username Maximo de quien reporta
AREA_QUE_REPORTA_FO  = "O_DXINT"      # area del que reporta
RESPONSABLE_NIVEL3   = "LGMELENDEZHE"  # turno (LCRAMOSCA o LGMELENDEZHE)


# ═══════════════════════════════════════════════════════════════════
# CONSTANTES DE SPECS (siempre iguales para OTs RBHFO)
# ═══════════════════════════════════════════════════════════════════

CLASSSTRUCTUREID    = "4213"
TIPO_TRAMO          = "RBHFO"
TIPO_OPERACION_FO   = "Red Movil"
TIPO_CUADRILLA_FO   = "Cuadrilla de Disponibilidad"
OPERADOR_FO         = "Colombia Telecomunicaciones"
OBSERV_CIERRE       = "Pendiente Informe"
COORDENADA_NA       = "NA"
PARADA_RELOJ_INI    = "0,0000"
TIEMPO_EFECT_INI    = "0,0000"


# ═══════════════════════════════════════════════════════════════════
# RESOLUCION DEL HREF DESDE EL WONUM
# ═══════════════════════════════════════════════════════════════════

def obtener_href_de_wonum(wonum):
    """
    Resuelve el href de una OT a partir de su wonum.
    Solo necesario en este script aislado: en el flujo real (script 06),
    el href ya viene en el resultado de crear_ot().
    """
    try:
        r = requests.get(
            f'{URL_BASE}?lean=1&oslc.where=wonum="{wonum}"'
            f'&oslc.select=wonum,href',
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            timeout=TIMEOUT,
        )
        if r.status_code != 200:
            logging.error(f"Error HTTP {r.status_code} al obtener href de OT {wonum}")
            return None
        members = r.json().get("rdfs:member") or r.json().get("member")
        if not members:
            logging.warning(f"OT {wonum} no encontrada")
            return None
        return members[0].get("href") or None
    except Exception as e:
        logging.error(f"Error obteniendo href de OT {wonum}: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════
# RESOLUCION DESDE POSTGRES
# ═══════════════════════════════════════════════════════════════════

def resolver_depto_y_muni(conn, punta_a, punta_b, lado):
    """
    Resuelve el departamento y municipio del lado de despacho, consultando
    inventario_anillo_bh_fusion.
    """
    sql = """
        SELECT mun_origen,  depto_origen,
               mun_destino, depto_destino
        FROM   onms.inventario_anillo_bh_fusion
        WHERE  (device_origen = %s AND device_destino = %s)
            OR (device_origen = %s AND device_destino = %s)
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (punta_a, punta_b, punta_b, punta_a))
        row = cur.fetchone()
        if not row:
            return None
        mun_o, dep_o, mun_d, dep_d = row
        if lado == "origen":
            return {"municipio": mun_o, "departamento": dep_o}
        elif lado == "destino":
            return {"municipio": mun_d, "departamento": dep_d}
        else:
            raise ValueError(f"lado debe ser 'origen' o 'destino', recibido: {lado}")


def resolver_coord_fibra(conn, departamento, municipio):
    """
    Busca en cat_coord_fibra la EECC y coordinador de red para el sitio.
    Aplica la regla de override: si hay fila para el municipio especifico,
    la usa; si no, cae al default del departamento.
    """
    sql = """
        SELECT eecc_cuadrilla_fo, coordinador_red_fo
        FROM   onms.cat_coord_fibra
        WHERE  departamento = %s
          AND  (municipio = %s OR municipio IS NULL)
        ORDER  BY municipio NULLS LAST
        LIMIT  1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (departamento, municipio))
        row = cur.fetchone()
        if not row:
            return None
        return {"eecc_cuadrilla_fo": row[0], "coordinador_red_fo": row[1]}


def resolver_coord_pi(conn, departamento):
    """
    Busca en cat_coord_pi el lider de zona para el departamento.
    """
    sql = """
        SELECT lider_de_zona_fo
        FROM   onms.cat_coord_pi
        WHERE  departamento = %s
        LIMIT  1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (departamento,))
        row = cur.fetchone()
        if not row:
            return None
        return {"lider_de_zona_fo": row[0]}


# ═══════════════════════════════════════════════════════════════════
# CONSTRUCCION DEL PAYLOAD DE SPECS
# ═══════════════════════════════════════════════════════════════════

def spec_aln(attr_id, value):
    """Helper para construir una spec con valor alfanumerico."""
    return {
        "spi:assetattrid":      attr_id,
        "spi:alnvalue":         value,
        "spi:classstructureid": CLASSSTRUCTUREID,
    }


def spec_table(attr_id, value):
    """Helper para construir una spec con valor referenciado a tabla."""
    return {
        "spi:assetattrid":      attr_id,
        "spi:tablevalue":       value,
        "spi:classstructureid": CLASSSTRUCTUREID,
    }


def armar_payload_specs(eecc, coord_red, lider_zona):
    """
    Combina catalogos + constantes + inputs simulados en el payload final.
    """
    specs = [
    spec_aln("EECC_CUADRILLA_FO",            eecc),
    spec_aln("TIPO_CUADRILLA_FO",            TIPO_CUADRILLA_FO),
    spec_aln("OPERADOR_FO",                  OPERADOR_FO),
    spec_aln("COORDINADOR_RED_FO",           coord_red),
    spec_table("LIDER_DE_ZONA_FO",           lider_zona),         # ← spec_table
    spec_table("RESPONSABLE_ZONA_NIVEL3_FO", RESPONSABLE_NIVEL3), # ← spec_table
    spec_table("PERSONA_QUE_REPORTA",        PERSONA_QUE_REPORTA),# ← spec_table
    spec_table("AREA_QUE_REPORTA_FO",        AREA_QUE_REPORTA_FO),# ← spec_table
    spec_aln("TIPO_TRAMO",                   TIPO_TRAMO),
    spec_aln("TIPO_OPERACION_FO",            TIPO_OPERACION_FO),
    spec_aln("OBSERV_CIERRE",                OBSERV_CIERRE),
    spec_aln("COORDENADA_CORTE_LONG",        COORDENADA_NA),
    spec_aln("COORDENADA_CORTE_LAT",         COORDENADA_NA),
    spec_aln("PARADA_RELOJ",                 PARADA_RELOJ_INI),
    spec_aln("TIEMPO_EFECT",                 TIEMPO_EFECT_INI),
]
    return {"spi:workorderspec": specs}


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    print("=" * 70)
    print("PRUEBA 05: ACTUALIZAR SPECS DESDE CATALOGO (payload dinamico)")
    print("=" * 70)

    guard_ambiente_dev()

    # 0. Resolver href de la OT
    print(f"WONUM a actualizar: {WONUM}")
    href = obtener_href_de_wonum(WONUM)
    if not href:
        print(f"ERROR: no se pudo resolver el href para wonum={WONUM}. Abortando.")
        return
    print(f"href resuelto:      {href}")
    print()

    # 1. Abrir conexion Postgres
    conn = obtener_conexion()
    if not conn:
        print("ERROR: no se pudo abrir conexion Postgres. Abortando.")
        return

    try:
        print("Inputs simulados del frontend:")
        print(f"  Punta A             = {PUNTA_A}")
        print(f"  Punta B             = {PUNTA_B}")
        print(f"  Despacho a          = {PUNTA_DESPACHO}")
        print(f"  Persona que reporta = {PERSONA_QUE_REPORTA}")
        print(f"  Area que reporta    = {AREA_QUE_REPORTA_FO}")
        print(f"  Responsable Nivel3  = {RESPONSABLE_NIVEL3}")
        print()

        # 2. Resolver depto + municipio del lado de despacho
        ubic = resolver_depto_y_muni(conn, PUNTA_A, PUNTA_B, PUNTA_DESPACHO)
        if not ubic:
            print(f"ERROR: enlace {PUNTA_A} <-> {PUNTA_B} no encontrado. Abortando.")
            return
        print(f"Ubicacion de despacho:")
        print(f"  departamento = {ubic['departamento']}")
        print(f"  municipio    = {ubic['municipio']}")
        print()

        # 3. Resolver EECC + coordinador red desde cat_coord_fibra
        cf = resolver_coord_fibra(conn, ubic["departamento"], ubic["municipio"])
        if not cf or not cf["eecc_cuadrilla_fo"] or not cf["coordinador_red_fo"]:
            print(f"ERROR: cat_coord_fibra no tiene EECC/coord asignado para "
                  f"depto={ubic['departamento']}, muni={ubic['municipio']}. Abortando.")
            return
        print(f"Resuelto desde cat_coord_fibra:")
        print(f"  eecc_cuadrilla_fo  = {cf['eecc_cuadrilla_fo']}")
        print(f"  coordinador_red_fo = {cf['coordinador_red_fo']}")
        print()

        # 4. Resolver lider de zona desde cat_coord_pi
        cp = resolver_coord_pi(conn, ubic["departamento"])
        if not cp or not cp["lider_de_zona_fo"]:
            print(f"ERROR: cat_coord_pi no tiene lider asignado para "
                  f"depto={ubic['departamento']}. Abortando.")
            return
        print(f"Resuelto desde cat_coord_pi:")
        print(f"  lider_de_zona_fo   = {cp['lider_de_zona_fo']}")
        print()

        # 5. Armar payload de specs
        payload = armar_payload_specs(
            eecc       = cf["eecc_cuadrilla_fo"],
            coord_red  = cf["coordinador_red_fo"],
            lider_zona = cp["lider_de_zona_fo"],
        )

        print(f"Payload armado: {len(payload['spi:workorderspec'])} specs")
        print()
        for s in payload["spi:workorderspec"]:
            attr = s["spi:assetattrid"]
            val  = s.get("spi:alnvalue") or s.get("spi:tablevalue")
            tipo = "aln" if "spi:alnvalue" in s else "tbl"
            print(f"  [{tipo}] {attr:<28} = {val}")
        print()

    finally:
        cerrar_conexion(conn)

    # 6. Llamar a actualizar_ot (PATCH sobre el href)
    resultado = actualizar_ot(href, payload)
    imprimir_resultado("RESULTADO actualizar_ot()", resultado)

    if resultado.get("success"):
        print("=" * 70)
        print(f"SPECS ACTUALIZADAS EXITOSAMENTE en wonum {WONUM}")
        print("=" * 70)
        print("Validar en Maximo UI (pestana Especificaciones) que los")
        print("15 atributos quedaron con sus valores correctos.")
        print("=" * 70)
    else:
        print("=" * 70)
        print("LA ACTUALIZACION FALLO. Revisar log para detalles.")
        print("=" * 70)


if __name__ == "__main__":
    main()