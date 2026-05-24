"""
04_crear_ot_desde_catalogo.py
-----------------------------
PRUEBA DE INTEGRACION: crear una OT en Maximo DESARROLLO con el payload
top-level armado dinamicamente desde las tablas catalogo en Postgres.

A diferencia de 01_crear_ot_minima.py (que usa JSON hardcodeado),
este script simula el flujo que ejecutara el frontend NOC:

  1. El usuario elige las dos puntas del enlace y a cual despachar.
  2. Se consulta inventario_anillo_bh_fusion para resolver
     mun_origen, mun_destino, sit_location, departamento.
  3. Se arma el payload top-level con esos datos + constantes.
  4. Se llama a crear_ot() con ese payload dinamico.

OBJETIVO:
    Validar end-to-end que las tablas catalogo + la funcion crear_ot()
    permiten generar una OT correcta sin payloads hardcodeados.

ENTRADAS:
    - Constantes hardcodeadas mas abajo (PUNTA_A, PUNTA_B, PUNTA_DESPACHO)
      que simulan lo que el operador NOC ingresaria en el frontend.
    - .env apuntando a DEV (10.80.123.13) para Maximo.
    - PG_* apuntando a 192.168.44.114 schema onms.

SALIDAS:
    - Imprime el payload armado y el wonum creado.
    - Log a logs/log_DD-MM-YYYY_HHMM.txt.

EJECUCION:
    Desde la raiz del proyecto:
        py -m tests.integracion_maximo.04_crear_ot_desde_catalogo
"""

import core.logging_setup  # noqa: F401 — inicializa logging del proyecto

from datetime import datetime

from integrations.postgres.client import obtener_conexion, cerrar_conexion
from integrations.maximo.rest_api import crear_ot
from tests.integracion_maximo._comun import (
    guard_ambiente_dev,
    imprimir_resultado,
)


# ═══════════════════════════════════════════════════════════════════
# INPUTS SIMULADOS DEL FRONTEND NOC
# ═══════════════════════════════════════════════════════════════════
# En el sistema real, estos valores los ingresa el operador en el
# frontend NOC. Aqui los hardcodeamos para la prueba.

PUNTA_A          = "ANT_BEL_BELL_H401"
PUNTA_B          = "ANT_MED_PRAD_H501_D063"
PUNTA_DESPACHO   = "destino"        # "origen" o "destino"

# Datos del operador / area que reporta (capturados en el frontend)
REPORTED_BY      = "EJROZOCA"
LEAD             = "LGMELENDEZHE"
IMPACTO          = "1"


# ═══════════════════════════════════════════════════════════════════
# CONSTANTES DEL PAYLOAD (siempre iguales para OTs O_GESFO)
# ═══════════════════════════════════════════════════════════════════

WOCLASS           = "WORKORDER"
WORKTYPE          = "MC"
STATUS_INICIAL    = "WAPPR"
OWNERGROUP        = "O_GESFO"
CLASSSTRUCTUREID  = "4213"


# ═══════════════════════════════════════════════════════════════════
# FUNCIONES DE RESOLUCION DESDE POSTGRES
# ═══════════════════════════════════════════════════════════════════

def resolver_enlace(conn, punta_a, punta_b):
    """
    Busca el enlace (Punta A, Punta B) en inventario_anillo_bh_fusion.
    Acepta el enlace en cualquier orden (A->B o B->A) para no depender
    de como el operador lo haya seleccionado en el frontend.

    Retorna un dict con las columnas relevantes, o None si no existe.
    """
    sql = """
        SELECT id, anillo,
               device_origen,  mun_origen,  depto_origen,
               sit_location_origen,  sit_description_origen,
               device_destino, mun_destino, depto_destino,
               sit_location_destino, sit_description_destino
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
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))


def extraer_datos_despacho(enlace, lado_despacho):
    """
    Dado el enlace resuelto y el lado al que se despacha la cuadrilla,
    retorna los datos del lado de despacho (cinum, location, municipio,
    departamento) y los municipios para armar la description.

    La description se arma SIEMPRE con el orden origen -> destino del
    anillo, independiente de a que lado se despache.

    Retorna dict con:
        cinum_despacho, location_despacho, municipio_despacho, depto_despacho,
        mun_origen_anillo, mun_destino_anillo
    """
    if lado_despacho == "origen":
        cinum    = enlace["device_origen"]
        location = enlace["sit_location_origen"]
        muni     = enlace["mun_origen"]
        depto    = enlace["depto_origen"]
    elif lado_despacho == "destino":
        cinum    = enlace["device_destino"]
        location = enlace["sit_location_destino"]
        muni     = enlace["mun_destino"]
        depto    = enlace["depto_destino"]
    else:
        raise ValueError(f"lado_despacho debe ser 'origen' o 'destino', recibido: {lado_despacho}")

    return {
        "cinum_despacho":      cinum,
        "location_despacho":   location,
        "municipio_despacho":  muni,
        "depto_despacho":      depto,
        "mun_origen_anillo":   enlace["mun_origen"],
        "mun_destino_anillo":  enlace["mun_destino"],
    }


# ═══════════════════════════════════════════════════════════════════
# CONSTRUCCION DEL PAYLOAD
# ═══════════════════════════════════════════════════════════════════

def armar_description(mun_origen, mun_destino):
    """
    Arma la description en el formato exacto que usa Maximo:
        'Falla FiOp, Mov. {mun_origen} - Mov. {mun_destino}'

    Capitaliza los municipios (que en la BD vienen en mayusculas).
    """
    def cap(s):
        return s.title() if s else ""
    return f"Falla FiOp, Mov. {cap(mun_origen)} - Mov. {cap(mun_destino)}"


def armar_payload_toplevel(datos):
    """
    Arma el dict que se va a enviar a crear_ot().
    Combina constantes + datos resueltos desde Postgres + inputs del operador.
    """
    return {
        "description":       armar_description(
                                 datos["mun_origen_anillo"],
                                 datos["mun_destino_anillo"]
                             ),
        "woclass":           WOCLASS,
        "worktype":          WORKTYPE,
        "status":            STATUS_INICIAL,
        "ownergroup":        OWNERGROUP,
        "classstructureid":  CLASSSTRUCTUREID,
        "cinum":             datos["cinum_despacho"],
        "location":          datos["location_despacho"],
        "reportedby":        REPORTED_BY,
        "lead":              LEAD,
        "impacto":           IMPACTO,
    }


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    print("=" * 70)
    print("PRUEBA 04: CREAR OT DESDE CATALOGO (payload dinamico)")
    print("=" * 70)

    # 1. Verificar que estamos contra Maximo dev
    guard_ambiente_dev()

    # 2. Abrir conexion Postgres
    conn = obtener_conexion()
    if not conn:
        print("ERROR: no se pudo abrir conexion Postgres. Abortando.")
        return

    try:
        # 3. Resolver enlace desde inventario_anillo_bh_fusion
        print(f"Inputs simulados del frontend:")
        print(f"  Punta A         = {PUNTA_A}")
        print(f"  Punta B         = {PUNTA_B}")
        print(f"  Despacho a      = {PUNTA_DESPACHO}")
        print()

        enlace = resolver_enlace(conn, PUNTA_A, PUNTA_B)
        if not enlace:
            print(f"ERROR: enlace {PUNTA_A} <-> {PUNTA_B} no encontrado en "
                  f"inventario_anillo_bh_fusion. Abortando.")
            return

        print("Enlace resuelto:")
        print(f"  id              = {enlace['id']}")
        print(f"  anillo          = {enlace['anillo']}")
        print(f"  device_origen   = {enlace['device_origen']:<35}  "
              f"mun={enlace['mun_origen']}, depto={enlace['depto_origen']}, "
              f"loc={enlace['sit_location_origen']}")
        print(f"  device_destino  = {enlace['device_destino']:<35}  "
              f"mun={enlace['mun_destino']}, depto={enlace['depto_destino']}, "
              f"loc={enlace['sit_location_destino']}")
        print()

        # 4. Extraer datos del lado de despacho
        datos = extraer_datos_despacho(enlace, PUNTA_DESPACHO)
        print(f"Datos del lado de despacho ({PUNTA_DESPACHO}):")
        for k, v in datos.items():
            print(f"  {k:<22} = {v}")
        print()

        # 5. Armar payload
        payload = armar_payload_toplevel(datos)
        print("Payload top-level armado:")
        for k, v in payload.items():
            print(f"  {k:<20} = {v}")
        print()

    finally:
        cerrar_conexion(conn)

    # 6. Llamar a crear_ot
    resultado = crear_ot(payload)
    imprimir_resultado("RESULTADO crear_ot()", resultado)

    if resultado.get("success"):
        wonum = resultado.get("ot")
        href  = resultado.get("href") or ""
        print("=" * 70)
        print("OT CREADA EXITOSAMENTE")
        print("=" * 70)
        print(f"  wonum: {wonum}")
        print(f"  href : {href if href else '(vacio - revisar header Location)'}")
        print()
        print("Siguiente paso: ejecutar 05_actualizar_specs_desde_catalogo.py")
        print(f"con wonum={wonum}.")
        print("=" * 70)
    else:
        print("=" * 70)
        print("LA CREACION FALLO. Revisar log para detalles.")
        print("=" * 70)


if __name__ == "__main__":
    main()