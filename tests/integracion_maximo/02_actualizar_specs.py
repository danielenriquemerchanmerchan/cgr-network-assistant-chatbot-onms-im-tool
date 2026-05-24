"""
02_actualizar_specs.py
----------------------
PRUEBA DE INTEGRACION: actualizar las especificaciones (workorderspec)
de una OT existente en Maximo DESARROLLO via PATCH.

OBJETIVO:
    Validar que actualizar_ot() puede llenar los specs de una OT con el
    array workorderspec embebido (prefijo spi:), tal como esta documentado
    en el docstring de la funcion.

ENTRADAS:
    - wonum por argumento de linea de comandos (sys.argv[1])
    - payloads/actualizar_specs.json (specs a aplicar, prefijo spi: ya incluido)
    - .env apuntando a DEV (10.80.123.13)

SALIDAS:
    - Imprime el resultado del PATCH.
    - Log a logs/log_DD-MM-YYYY_HHMM.txt.

EJECUCION:
    Desde la raiz del proyecto, pasando el wonum creado por el script 01:
        py -m tests.integracion_maximo.02_actualizar_specs 10563200

NOTA:
    El script obtiene el href consultando la OT por wonum (no requiere
    que pases el href). Esto lo hace usable incluso para OTs creadas en
    sesiones anteriores.
"""

import sys
import core.logging_setup  # noqa: F401

from integrations.maximo.rest_api import actualizar_ot, consultar_ot
from tests.integracion_maximo._comun import (
    guard_ambiente_dev,
    imprimir_resultado,
    cargar_payload,
)


def resolver_href(wonum):
    """
    Consulta la OT para obtener su href. consultar_ot() retorna el dict
    completo incluyendo data["raw"]["href"] que es el que necesita PATCH.
    """
    info = consultar_ot(wonum)
    if not info:
        return None
    return info.get("raw", {}).get("href")


def main():
    print("=" * 70)
    print("PRUEBA 02: ACTUALIZAR SPECS DE OT EXISTENTE")
    print("=" * 70)

    # 1. Verificar ambiente
    guard_ambiente_dev()

    # 2. Leer wonum de argumentos
    if len(sys.argv) < 2:
        print("ERROR: falta el wonum como argumento.")
        print("Uso: py -m tests.integracion_maximo.02_actualizar_specs <wonum>")
        sys.exit(1)

    wonum = sys.argv[1].strip()
    print(f"OT objetivo: {wonum}")
    print()

    # 3. Resolver href de la OT
    print("Resolviendo href de la OT...")
    href = resolver_href(wonum)
    if not href:
        print(f"ERROR: no se pudo obtener el href de la OT {wonum}.")
        print("Posibles causas:")
        print("  - El wonum no existe en este ambiente.")
        print("  - Falla de conexion a Maximo.")
        sys.exit(1)
    print(f"href: {href}")
    print()

    # 4. Cargar payload de specs
    payload = cargar_payload("actualizar_specs.json")

    print(f"Specs a aplicar: {len(payload['spi:workorderspec'])}")
    for spec in payload["spi:workorderspec"]:
        attr  = spec.get("spi:assetattrid", "")
        valor = spec.get("spi:alnvalue") or spec.get("spi:numvalue") or "(sin valor)"
        print(f"  {attr:25s} = {valor}")
    print()

    # 5. Llamar a actualizar_ot
    resultado = actualizar_ot(href, payload)
    imprimir_resultado("RESULTADO actualizar_ot()", resultado)

    if resultado.get("success"):
        print("=" * 70)
        print("SPECS ACTUALIZADOS EXITOSAMENTE")
        print("=" * 70)
        print(f"Verificar en Maximo UI: la OT {wonum} ahora debe mostrar")
        print(f"esos {len(payload['spi:workorderspec'])} specs en la pestana Especificaciones.")
        print("=" * 70)
    else:
        print("=" * 70)
        print("LA ACTUALIZACION FALLO. Revisar log para detalles.")
        print("=" * 70)


if __name__ == "__main__":
    main()