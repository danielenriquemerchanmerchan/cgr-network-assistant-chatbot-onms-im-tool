"""
listar_historico_ots_o_gesfo.py
-------------------------------
Genera tabla maestra de OTs de O_GESFO (classstructureid=4213)
con todos los campos, specifications, descripcion del CI y el historial
completo de worklogs (avances) de cada OT.

Pipeline:
    1. Extract    -> maximo_wo.listar_ots + obtener_detalle_ot + obtener_ci_description
    2. Transform  -> domain.transformers (OT plana + worklogs planos)
    3. Load       -> exporters.* (Excel, MySQL, PostgreSQL, ...)

El pipeline devuelve DOS listas:
    - registros_ots:      1 fila por OT
    - registros_worklogs: 1 fila por avance (relacion por wonum)
"""

import logging
from logger_config import logger  # inicializa logging a archivo

# Agregar salida por consola
_console = logging.StreamHandler()
_console.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
))
logging.getLogger().addHandler(_console)

from config import MAXIMO_PAGE_SIZE

# API Maximo: todo centralizado en maximo_wo
from maximo_wo import (
    listar_ots,
    obtener_detalle_ot,
    obtener_ci_description,
    extraer_worklogs_inline,
)

# Transformers puros
from domain.transformers.ot import construir_registro
from domain.transformers.worklog import construir_registros_worklog

# Exporters
from exporters.excel import ExcelExporter
# from exporters.mysql    import MySQLExporter
# from exporters.postgres import PostgresExporter


# ══════════════════════════════════════════════════════════════
# 1. EXTRAER Y TRANSFORMAR
# ══════════════════════════════════════════════════════════════

def extraer_registros(ownergroup, classstructureid, page_size=MAXIMO_PAGE_SIZE):
    """
    Ejecuta el pipeline extract + transform.

    Parametros:
        ownergroup       (str): grupo propietario. Ej: 'O_GESFO'
        classstructureid (str): ID de clasificacion. Ej: '4213'
        page_size        (int): tamanyo de pagina para paginacion

    Retorna:
        tupla (registros_ots, registros_worklogs)
    """
    logging.info(f"Consultando OTs {ownergroup} / classstructureid={classstructureid}")

    members = listar_ots(ownergroup, classstructureid, page_size=page_size)
    total   = len(members)
    logging.info(f"Total OTs obtenidas del listado: {total}")

    registros_ots       = []
    registros_worklogs  = []
    ci_cache            = {}

    for i, m in enumerate(members, 1):
        href  = m.get("href")
        cinum = m.get("cinum") or ""

        detalle = obtener_detalle_ot(href)
        if detalle is None:
            continue

        # CI description (cached)
        ci_desc = obtener_ci_description(cinum, cache=ci_cache) if cinum else ""

        # Worklogs inline (sin requests extra)
        worklogs_crudos = extraer_worklogs_inline(detalle)
        wonum           = detalle.get("wonum") or ""
        worklogs_planos = construir_registros_worklog(wonum, worklogs_crudos)

        # Registro OT con cantidad de avances precomputada
        registro_ot = construir_registro(
            m, detalle,
            ci_description=ci_desc,
            cant_worklogs=len(worklogs_planos),
        )

        registros_ots.append(registro_ot)
        registros_worklogs.extend(worklogs_planos)

        if i % 50 == 0 or i == total:
            pct = 100 * i / total if total else 0
            logging.info(
                f"Procesadas {i}/{total} OTs ({pct:.1f}%) — "
                f"{len(registros_worklogs)} worklogs acumulados"
            )

    logging.info(
        f"Total registros: {len(registros_ots)} OTs, "
        f"{len(registros_worklogs)} worklogs"
    )
    return registros_ots, registros_worklogs


# ══════════════════════════════════════════════════════════════
# 2. CARGAR A DESTINOS
# ══════════════════════════════════════════════════════════════

def cargar_a_destinos(registros_ots, registros_worklogs, destinos):
    """
    Envia los registros a cada destino configurado.
    Captura errores por destino para que uno roto no tumbe los otros.
    """
    for dest in destinos:
        nombre = dest.__class__.__name__
        try:
            resultado = dest.export(registros_ots, registros_worklogs)
            logging.info(f"[{nombre}] OK -> {resultado}")
        except Exception as e:
            logging.error(f"[{nombre}] ERROR: {e}")


# ══════════════════════════════════════════════════════════════
# 3. MAIN
# ══════════════════════════════════════════════════════════════

def main():
    # Destinos activos (descomentar los que quieras usar)
    destinos = [
        ExcelExporter(output_file="output/tabla_maestra_cinum_gesfo.xlsx"),
        # MySQLExporter(tabla_ots="ots_gesfo_4213", tabla_worklogs="ots_worklogs"),
        # PostgresExporter(tabla_ots="ots_gesfo_4213", tabla_worklogs="ots_worklogs"),
    ]

    # Pipeline
    registros_ots, registros_worklogs = extraer_registros(
        ownergroup="O_GESFO",
        classstructureid="4213",
    )

    cargar_a_destinos(registros_ots, registros_worklogs, destinos)

    logging.info("Proceso listar_historico_ots_o_gesfo finalizado")


if __name__ == "__main__":
    main()