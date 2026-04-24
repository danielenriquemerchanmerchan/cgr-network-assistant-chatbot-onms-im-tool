"""
tabla_maestra_4213.py
---------------------
Genera tabla maestra de OTs de O_GESFO (classstructureid=4213)
con todos los campos principales, specifications y descripcion del CI.

Pipeline:
    1. Extract    -> maximo_wo (paginacion completa + detalle + CI cache)
    2. Transform  -> ot_transformer (registro plano)
    3. (Enrich)   -> oracle_maximo.enriquecer_ot (opcional: ciudad, depto)
    4. Load       -> exporters.* (Excel, MySQL, PostgreSQL, ...)

Para agregar un destino nuevo: crear clase que herede de Exporter
y agregarla a la lista `destinos` en main().
"""

import logging
from logger_config import logger  # inicializa logging a archivo

from config import MAXIMO_PAGE_SIZE

from maximo_wo import (
    listar_ots, obtener_detalle_ot, obtener_ci_description,
    extraer_worklogs_inline,
)
from ot_transformer import construir_registro
from base import Exporter
from excel_exporter import ExcelExporter
# from mysql_exporter    import MySQLExporter
# from postgres_exporter import PostgresExporter
# Enriquecimiento opcional con Oracle (ciudad, depto, direccion)
# from oracle_maximo import enriquecer_ot


# ══════════════════════════════════════════════════════════════
# 1. EXTRAER Y TRANSFORMAR
# ══════════════════════════════════════════════════════════════

def extraer_registros(ownergroup, classstructureid, page_size=MAXIMO_PAGE_SIZE,
                      enriquecer_con_oracle=False):
    """
    Ejecuta el pipeline extract + transform.

    Parametros:
        ownergroup            (str): grupo propietario. Ej: 'O_GESFO'
        classstructureid      (str): ID de clasificacion. Ej: '4213'
        page_size             (int): tamanyo de pagina para paginacion
        enriquecer_con_oracle (bool): si True, agrega ciudad/depto/direccion

    Retorna:
        list[dict] con los registros planos.
    """
    logging.info(f"Consultando OTs {ownergroup} / classstructureid={classstructureid}")

    members = listar_ots(ownergroup, classstructureid, page_size=page_size)
    total   = len(members)
    logging.info(f"Total OTs obtenidas del listado: {total}")

    registros = []
    ci_cache  = {}

    for i, m in enumerate(members, 1):
        href  = m.get("href")
        cinum = m.get("cinum") or ""

        detalle = obtener_detalle_ot(href)
        if detalle is None:
            continue

        ci_desc = obtener_ci_description(cinum, cache=ci_cache) if cinum else ""
        registro = construir_registro(m, detalle, ci_description=ci_desc)

        # Enriquecimiento opcional con datos geograficos de Oracle
        if enriquecer_con_oracle:
            # enriquecer_ot agrega: ciudad, departamento, direccion, aliado, nom_sitio
            # Ojo: modifica registro['raw']? No. Aqui trabajamos con registro plano.
            # Se llama directamente con el registro que ya tiene 'location'.
            from oracle_maximo import enriquecer_ot
            enriquecer_ot(registro)

        registros.append(registro)

        if i % 100 == 0:
            logging.info(f"Procesadas {i}/{total} OTs...")

    logging.info(f"Total registros construidos: {len(registros)}")
    return registros


# ══════════════════════════════════════════════════════════════
# 2. CARGAR A DESTINOS
# ══════════════════════════════════════════════════════════════

def cargar_a_destinos(registros, destinos):
    """
    Envia los registros a cada destino configurado.
    Captura errores por destino para que uno roto no tumbe los otros.
    """
    for dest in destinos:
        nombre = dest.__class__.__name__
        try:
            resultado = dest.export(registros)
            logging.info(f"[{nombre}] OK -> {resultado}")
        except Exception as e:
            logging.error(f"[{nombre}] ERROR: {e}")


# ══════════════════════════════════════════════════════════════
# 3. MAIN
# ══════════════════════════════════════════════════════════════

def main():
    # Destinos activos (descomentar los que quieras usar)
    destinos = [
        ExcelExporter(output_file="tabla_maestra_cinum_gesfo.xlsx"),
        # MySQLExporter(tabla="ots_gesfo_4213"),
        # PostgresExporter(tabla="ots_gesfo_4213", schema="public"),
    ]

    # Pipeline
    registros = extraer_registros(
        ownergroup="O_GESFO",
        classstructureid="4213",
        enriquecer_con_oracle=False,
    )

    cargar_a_destinos(registros, destinos)

    logging.info("Proceso tabla_maestra_4213 finalizado")


if __name__ == "__main__":
    main()