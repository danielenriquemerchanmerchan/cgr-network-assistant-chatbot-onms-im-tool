"""
01_crear_ot_minima.py
---------------------
PRUEBA DE INTEGRACION: crear una OT en Maximo DESARROLLO con solo
los campos top-level minimos (sin specs).

OBJETIVO:
    Validar que el endpoint POST /maximo/oslc/os/RESTWO acepta el payload
    minimo y retorna un wonum + href utilizables.

ENTRADAS:
    - payloads/crear_ot_minima.json (campos top-level hardcoded)
    - .env apuntando a DEV (10.80.123.13)

SALIDAS:
    - Imprime en consola: wonum creado y href para usar en el script 02.
    - Log a logs/log_DD-MM-YYYY_HHMM.txt (formato del proyecto).

EJECUCION:
    Desde la raiz del proyecto:
        py -m tests.integracion_maximo.01_crear_ot_minima

ADVERTENCIA:
    Si el .env apunta a 10.80.123.11 (PROD), el script aborta.

PLACEHOLDERS QUE SE REEMPLAZAN EN RUNTIME:
    [TIMESTAMP] -> hora legible (para la description). Ej: "2026-05-28 09:27:31"
    [NOW_ISO]   -> hora ISO 8601 con zona (-05:00) para campos de fecha
                   como schedstart / actstart. Ej: "2026-05-28T09:27:31-05:00"
"""

import core.logging_setup  # noqa: F401 — inicializa logging del proyecto

from datetime import datetime

from integrations.maximo.rest_api import crear_ot
from tests.integracion_maximo._comun import (
    guard_ambiente_dev,
    imprimir_resultado,
    cargar_payload,
)


def _reemplazar_placeholders(payload):
    """
    Reemplaza los placeholders de fecha/hora en TODOS los valores
    string del payload:
        [TIMESTAMP] -> hora legible "YYYY-MM-DD HH:MM:SS"
        [NOW_ISO]   -> hora ISO 8601 con zona "YYYY-MM-DDTHH:MM:SS-05:00"

    Ambos usan el MISMO instante (datetime.now()) para que la OT quede
    coherente: la description, schedstart y actstart reflejan la misma
    hora de creacion.
    """
    ahora      = datetime.now()
    ts_legible = ahora.strftime("%Y-%m-%d %H:%M:%S")
    ts_iso     = ahora.strftime("%Y-%m-%dT%H:%M:%S-05:00")

    resultado = {}
    for k, v in payload.items():
        if isinstance(v, str):
            v = v.replace("[TIMESTAMP]", ts_legible)
            v = v.replace("[NOW_ISO]", ts_iso)
        resultado[k] = v
    return resultado


def main():
    print("=" * 70)
    print("PRUEBA 01: CREAR OT MINIMA (solo top-level)")
    print("=" * 70)

    # 1. Verificar que estamos en dev
    guard_ambiente_dev()

    # 2. Cargar payload base
    payload = cargar_payload("crear_ot_minima.json")

    # 3. Reemplazar placeholders de fecha/hora (description, schedstart, actstart)
    #    Todos usan el mismo instante de creacion.
    payload = _reemplazar_placeholders(payload)

    print("Payload a enviar:")
    for k, v in payload.items():
        print(f"  {k:20s} = {v}")
    print()

    # 4. Llamar a crear_ot
    resultado = crear_ot(payload)
    imprimir_resultado("RESULTADO crear_ot()", resultado)

    # 5. Si tuvo exito, dejar instrucciones claras para el siguiente paso
    if resultado.get("success"):
        wonum = resultado.get("ot")
        href  = resultado.get("href") or ""
        print("=" * 70)
        print("OT CREADA EXITOSAMENTE")
        print("=" * 70)
        print(f"  wonum: {wonum}")
        print(f"  href : {href if href else '(vacio - revisar header Location del response)'}")
        print()
        print("Siguiente paso: ejecutar 02_actualizar_specs.py con este wonum.")
        print("=" * 70)
    else:
        print("=" * 70)
        print("LA CREACION FALLO. Revisar log para detalles.")
        print("=" * 70)


if __name__ == "__main__":
    main()