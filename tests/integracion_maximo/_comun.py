"""
_comun.py
---------
Helpers compartidos por los scripts de integracion con Maximo.

CONTENIDO:
    - guard_ambiente_dev(): aborta si el .env apunta a produccion.
    - imprimir_resultado(): imprime el dict que retornan las funciones
      de rest_api.py en un formato legible.
    - HOST_DEV / HOST_PROD: hosts conocidos para identificar ambiente.

POR QUE EXISTE:
    Los 3 scripts (01_crear, 02_actualizar, 03_completo) repetian las
    mismas verificaciones de ambiente y el mismo bloque de impresion.
    Centralizar evita inconsistencias y olvidos peligrosos.
"""

import logging
import sys
import json

from core.config import MAXIMO_BASE_URL


# Hosts conocidos. Se identifican por sustring en la URL.
HOST_DEV  = "10.80.123.13"
HOST_PROD = "10.80.123.11"


def guard_ambiente_dev():
    """
    Verifica que MAXIMO_BASE_URL apunte al ambiente de desarrollo.

    Aborta el script con sys.exit(1) si detecta que apunta a produccion
    o si el host no es reconocido. Esto evita ejecutar accidentalmente
    operaciones de escritura (crear OT, actualizar OT) contra el ambiente
    productivo.

    Llamar al inicio de TODOS los scripts de escritura.
    """
    url = (MAXIMO_BASE_URL or "").lower()

    if HOST_PROD in url:
        print("=" * 70)
        print("ABORT: el .env apunta a PRODUCCION (host {})".format(HOST_PROD))
        print("Estos scripts solo deben correrse contra DESARROLLO ({}).".format(HOST_DEV))
        print("Verifica tu archivo .env y vuelve a intentarlo.")
        print("=" * 70)
        sys.exit(1)

    if HOST_DEV not in url:
        print("=" * 70)
        print("ABORT: MAXIMO_BASE_URL no apunta a un host conocido.")
        print("Valor actual: {}".format(MAXIMO_BASE_URL))
        print("Hosts esperados:")
        print("  DEV : {}".format(HOST_DEV))
        print("  PROD: {} (BLOQUEADO para estos scripts)".format(HOST_PROD))
        print("=" * 70)
        sys.exit(1)

    logging.info("Guard ambiente OK: apuntando a DEV ({})".format(MAXIMO_BASE_URL))


def imprimir_resultado(titulo, resultado):
    """
    Imprime el dict de resultado de las funciones de rest_api.py en
    formato legible. Las funciones retornan dict con claves:
    success, message, status, ot, href (opcional).

    Parametros:
        titulo (str): cabecera descriptiva del paso
        resultado (dict): el dict retornado por crear_ot / actualizar_ot
    """
    print()
    print("-" * 70)
    print(titulo)
    print("-" * 70)
    print(json.dumps(resultado, indent=2, ensure_ascii=False))
    print()


def cargar_payload(nombre_archivo):
    """
    Carga un JSON desde la carpeta payloads/ relativa a este modulo.

    Parametros:
        nombre_archivo (str): nombre del archivo (ej: 'crear_ot_minima.json')

    Retorna:
        dict con el contenido del JSON.
    """
    import os
    ruta = os.path.join(os.path.dirname(__file__), "payloads", nombre_archivo)
    with open(ruta, "r", encoding="utf-8") as f:
        return json.load(f)