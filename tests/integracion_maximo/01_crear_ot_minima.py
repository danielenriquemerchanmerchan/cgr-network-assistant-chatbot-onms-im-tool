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
"""

import core.logging_setup  # noqa: F401 — inicializa logging del proyecto

from datetime import datetime

from integrations.maximo.rest_api import crear_ot
from tests.integracion_maximo._comun import (
    guard_ambiente_dev,
    imprimir_resultado,
    cargar_payload,
)


def main():
    print("=" * 70)
    print("PRUEBA 01: CREAR OT MINIMA (solo top-level)")
    print("=" * 70)

    # 1. Verificar que estamos en dev
    guard_ambiente_dev()

    # 2. Cargar payload base
    payload = cargar_payload("crear_ot_minima.json")

    # 3. Reemplazar [TIMESTAMP] por la hora actual para que cada corrida
    #    produzca una description unica y sea facil ubicarla en Maximo UI
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload["description"] = payload["description"].replace("[TIMESTAMP]", timestamp)

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