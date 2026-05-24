"""
03_crear_y_actualizar_completo.py
---------------------------------
PRUEBA DE INTEGRACION end-to-end: crear OT minima + actualizar specs en
una sola ejecucion. Es el flujo que aproximara el futuro endpoint del
backend que generara OTs con pocos clicks desde la UI.

OBJETIVO:
    Validar el encadenamiento crear_ot() -> actualizar_ot() sin intervencion
    manual. Si este script pasa, el camino para el modulo de creacion
    automatizada esta despejado.

ENTRADAS:
    - payloads/crear_ot_minima.json
    - payloads/actualizar_specs.json
    - .env apuntando a DEV (10.80.123.13)

SALIDAS:
    - Imprime wonum + href creados, luego resultado del PATCH.
    - Log unificado en logs/log_DD-MM-YYYY_HHMM.txt.

EJECUCION:
    Desde la raiz del proyecto:
        py -m tests.integracion_maximo.03_crear_y_actualizar_completo

DIFERENCIA CON 01 + 02:
    Aqui no se obtiene href reconsultando la OT. Se usa el href que
    devuelve crear_ot() directamente, ahorrando un round-trip. Si el
    header Location viene vacio (puede pasar en ciertas configuraciones
    de Maximo), se cae al metodo de reconsulta como fallback.
"""

import sys
import core.logging_setup  # noqa: F401

from datetime import datetime

from integrations.maximo.rest_api import crear_ot, actualizar_ot, consultar_ot
from tests.integracion_maximo._comun import (
    guard_ambiente_dev,
    imprimir_resultado,
    cargar_payload,
)


def main():
    print("=" * 70)
    print("PRUEBA 03: FLUJO COMPLETO (crear + actualizar specs)")
    print("=" * 70)

    # 1. Guard de ambiente
    guard_ambiente_dev()

    # ─────────────────────────────────────────────────────────────
    # PASO 1: Crear OT minima
    # ─────────────────────────────────────────────────────────────
    print()
    print(">>> PASO 1: crear OT minima")
    print("-" * 70)

    payload_crear = cargar_payload("crear_ot_minima.json")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload_crear["description"] = payload_crear["description"].replace(
        "[TIMESTAMP]", timestamp
    )

    resultado_crear = crear_ot(payload_crear)
    imprimir_resultado("RESULTADO crear_ot()", resultado_crear)

    if not resultado_crear.get("success"):
        print("ABORT: la creacion fallo. No se intentara actualizar specs.")
        sys.exit(1)

    wonum = resultado_crear.get("ot")
    href  = resultado_crear.get("href")

    # ─────────────────────────────────────────────────────────────
    # Fallback: si Maximo no devuelve el header Location, reconsultamos
    # ─────────────────────────────────────────────────────────────
    if not href:
        print("Aviso: crear_ot no devolvio href. Reconsultando la OT...")
        info = consultar_ot(wonum)
        href = info.get("raw", {}).get("href") if info else None

    if not href:
        print(f"ABORT: no se pudo obtener el href de la OT {wonum}.")
        sys.exit(1)

    print(f"wonum creado: {wonum}")
    print(f"href        : {href}")

    # ─────────────────────────────────────────────────────────────
    # PASO 2: Actualizar specs sobre la OT recien creada
    # ─────────────────────────────────────────────────────────────
    print()
    print(">>> PASO 2: actualizar specs")
    print("-" * 70)

    payload_specs = cargar_payload("actualizar_specs.json")
    print(f"Specs a aplicar: {len(payload_specs['spi:workorderspec'])}")

    resultado_actualizar = actualizar_ot(href, payload_specs)
    imprimir_resultado("RESULTADO actualizar_ot()", resultado_actualizar)

    # ─────────────────────────────────────────────────────────────
    # Resumen final
    # ─────────────────────────────────────────────────────────────
    print()
    print("=" * 70)
    print("RESUMEN")
    print("=" * 70)
    print(f"  wonum creado            : {wonum}")
    print(f"  href                    : {href}")
    print(f"  Creacion exitosa        : {resultado_crear.get('success')}")
    print(f"  Actualizacion exitosa   : {resultado_actualizar.get('success')}")
    if resultado_crear.get("success") and resultado_actualizar.get("success"):
        print()
        print("OK: flujo end-to-end completo. Verificar en Maximo UI.")
    else:
        print()
        print("HUBO ERRORES. Revisar log para detalles.")
    print("=" * 70)


if __name__ == "__main__":
    main()