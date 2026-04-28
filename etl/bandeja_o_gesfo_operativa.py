"""
bandeja_operativa_o_gesfo.py
----------------------------
ETL RAPIDO de bandeja operativa: sincroniza solo las OTs OPERATIVAS de O_GESFO.

DIFERENCIA CON bandeja_o_gesfo.py (completo):
    - Solo procesa OTs creadas en los ultimos 7 dias (operativas reales)
    - Volumen tipico: ~100 OTs (vs ~1500 del completo)
    - Tiempo: ~2-3 minutos (vs ~28 min del completo)
    - Frecuencia recomendada: cada 5 minutos
    - NO toca las OTs zombies en BD (no se entera si cambiaron)

ZOMBIES:
    Las OTs zombies (>7 dias en INPRG/COMP) se sincronizan con
    bandeja_o_gesfo.py (completo) que se ejecuta una vez al dia.
    Este script no las trae de Maximo, pero al final reclasifica las
    OTs que ya cumplieron 7 dias de OPERATIVA -> ZOMBIE.

FILTRO DE OTs:
    worktype = 'MC' AND classstructureid = '4213' AND ownergroup = 'O_GESFO'
    AND status IN ('INPRG', 'COMP', 'CLOSE')
    AND creation_date >= NOW() - 7 dias

DETECCION DE SALIDAS:
    Compara las OTs OPERATIVAS de BD con las de Maximo.
    Si una OPERATIVA estaba en BD y ya no aparece -> marcar como salida.
    Las zombies NO se tocan (las maneja el script completo).

EJECUCION:
    py -m etl.bandeja_operativa_o_gesfo
"""

import sys
import time
import logging
from datetime import datetime, timedelta

from core.logging_setup import logger
from integrations.maximo.rest_api import (
    listar_ots,
    obtener_detalle_ot,
    obtener_ci_description,
    extraer_worklogs_inline,
)
from domain.transformers.ot import construir_registro
from domain.transformers.worklog import construir_registros_worklog
from integrations.postgres.client import (
    obtener_conexion,
    cerrar_conexion,
    obtener_wonums_operativas,
    upsert_work_order,
    reemplazar_worklogs,
    marcar_salidas_bandeja,
    reclasificar_envejecidas,
    contar_filas,
)

# Reusamos las funciones del script completo
from etl.bandeja_o_gesfo_completo import (
    parsear_fecha,
    clasificar_ot,
    calcular_etom_phase,
    procesar_ot,
)


# ════════════════════════════════════════════════════════════════════
# CONFIGURACION
# ════════════════════════════════════════════════════════════════════

OWNERGROUP = "O_GESFO"
CLASSSTRUCTUREID = "4213"
WORKTYPES_VALIDOS = {"MC"}
STATUSES_VALIDOS = {"INPRG", "COMP", "CLOSE"}

DIAS_OPERATIVA = 7  # Solo OTs creadas en ultimos N dias


# ════════════════════════════════════════════════════════════════════
# FILTRO ESPECIFICO PARA OPERATIVAS
# ════════════════════════════════════════════════════════════════════

def es_operativa(ot, ahora):
    """
    Filtra OTs OPERATIVAS segun el campo de fecha apropiado por status:

        - INPRG: creadas en los ultimos 7 dias (creation_date)
        - COMP:  cambiaron a COMP en los ultimos 7 dias (changedate)
        - CLOSE: cerradas en los ultimos 7 dias (actfinish)

    Esto es consistente con es_relevante() del script completo, asegurando
    que el operativo no marque como "salida" a una OT que el completo si
    considera operativa (ej: una CLOSE recien cerrada con creation antigua).
    """
    if ot.get("worktype") not in WORKTYPES_VALIDOS:
        return False

    status = ot.get("status")
    if status not in STATUSES_VALIDOS:
        return False

    umbral = ahora - timedelta(days=DIAS_OPERATIVA)

    if status == "INPRG":
        creation = parsear_fecha(ot.get("reportdate"))
        return creation is not None and creation >= umbral

    if status == "COMP":
        changedate = parsear_fecha(ot.get("changedate"))
        return changedate is not None and changedate >= umbral

    if status == "CLOSE":
        actfinish = parsear_fecha(ot.get("actfinish"))
        return actfinish is not None and actfinish >= umbral

    return False


# ════════════════════════════════════════════════════════════════════
# ORQUESTADOR
# ════════════════════════════════════════════════════════════════════

def sincronizar_bandeja_operativa():
    """
    Flujo del ETL operativo (rapido).

    Pasos:
        1. Conectar a Postgres
        2. Listar OTs de Maximo
        3. Filtrar solo operativas (frescas)
        4. Procesar cada una
        5. Detectar salidas DENTRO DEL RANGO OPERATIVO
        6. Reclasificar OTs que envejecieron (de OPERATIVA a ZOMBIE)
        7. Estadisticas finales

    NO HACE limpieza de zombies viejas (eso lo hace el ETL completo).
    """
    inicio = time.time()
    ahora = datetime.now()

    conn = obtener_conexion()
    if conn is None:
        logging.error("[Operativa] No se pudo conectar a Postgres. Abortando.")
        return False

    try:
        # 1. Wonums OPERATIVAS actualmente en BD (para diff)
        wonums_operativas_bd = obtener_wonums_operativas(conn)
        logging.info(f"[Operativa] Wonums OPERATIVAS en BD al inicio: "
                     f"{len(wonums_operativas_bd)}")

        # 2. Listar OTs de Maximo
        logging.info(f"[Operativa] Consultando Maximo...")
        todas_ots = listar_ots(
            ownergroup=OWNERGROUP,
            classstructureid=CLASSSTRUCTUREID,
        )
        logging.info(f"[Operativa] OTs traidas de Maximo: {len(todas_ots)}")

        # 3. Filtrar solo operativas
        ots_operativas = [o for o in todas_ots if es_operativa(o, ahora)]
        logging.info(f"[Operativa] OTs operativas (filtradas): {len(ots_operativas)}")

        # 4. Procesar cada una
        ci_cache = {}
        wonums_procesados = set()
        contadores = {"INSERTED": 0, "UPDATED": 0, "UNCHANGED": 0, "ERROR": 0}
        worklogs_total = 0

        for i, ot in enumerate(ots_operativas, 1):
            resultado = procesar_ot(ot, ahora, ci_cache, conn)
            wonums_procesados.add(resultado["wonum"])

            if resultado["error"]:
                contadores["ERROR"] += 1
            else:
                contadores[resultado["estado_upsert"]] += 1
                worklogs_total += resultado["worklogs_count"]

            if i % 25 == 0:
                logging.info(f"[Operativa] Procesadas {i}/{len(ots_operativas)} OTs...")

        # 5. Detectar salidas DENTRO DEL RANGO OPERATIVO
        # Las que estaban OPERATIVAS en BD pero ya no aparecen en el listado
        wonums_que_salieron = wonums_operativas_bd - wonums_procesados
        cant_salidas = marcar_salidas_bandeja(wonums_que_salieron, conn)
        conn.commit()
        logging.info(f"[Operativa] OTs marcadas como salidas: {cant_salidas}")

        # 6. Reclasificar las OPERATIVAS que ya envejecieron a ZOMBIE
        cant_reclasificadas = reclasificar_envejecidas(conn)
        conn.commit()
        if cant_reclasificadas > 0:
            logging.info(f"[Operativa] OTs reclasificadas OPERATIVA -> ZOMBIE: "
                         f"{cant_reclasificadas}")

        # 7. Estadisticas finales
        duracion = time.time() - inicio
        stats = contar_filas(conn)

        logging.info("="*60)
        logging.info("RESUMEN DE LA EJECUCION OPERATIVA")
        logging.info("="*60)
        logging.info(f"  Duracion total:                {duracion:.1f}s")
        logging.info(f"  OTs Maximo (universo):         {len(todas_ots)}")
        logging.info(f"  OTs operativas (filtradas):    {len(ots_operativas)}")
        logging.info(f"  Insertadas (nuevas):           {contadores['INSERTED']}")
        logging.info(f"  Actualizadas (con cambios):    {contadores['UPDATED']}")
        logging.info(f"  Sin cambios:                   {contadores['UNCHANGED']}")
        logging.info(f"  Con error:                     {contadores['ERROR']}")
        logging.info(f"  Worklogs cargados:             {worklogs_total}")
        logging.info(f"  Marcadas como salidas:         {cant_salidas}")
        logging.info(f"  Reclasificadas a ZOMBIE:       {cant_reclasificadas}")
        logging.info(f"  --- Estado final de tablas ---")
        logging.info(f"  work_orders activas:           {stats['work_orders_activas']}")
        logging.info(f"  work_orders inactivas:         {stats['work_orders_inactivas']}")
        logging.info(f"  worklogs:                      {stats['worklogs']}")
        logging.info(f"  bot_states:                    {stats['bot_states']}")
        logging.info("="*60)

        return True

    except Exception as e:
        logging.exception(f"[Operativa] Error inesperado: {e}")
        conn.rollback()
        return False

    finally:
        cerrar_conexion(conn)


# ════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════

def main():
    logging.info("="*60)
    logging.info("ETL BANDEJA OPERATIVA O_GESFO - INICIO")
    logging.info("="*60)

    exito = sincronizar_bandeja_operativa()

    if exito:
        logging.info("ETL operativo completado exitosamente")
        sys.exit(0)
    else:
        logging.error("ETL operativo finalizo con errores")
        sys.exit(1)


if __name__ == "__main__":
    main()