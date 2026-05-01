"""
bandeja_o_gesfo_operativa.py
----------------------------
ETL RAPIDO de bandeja operativa: sincroniza las OTs dentro de la ventana
operativa (ultimos DIAS_VENTANA_OPERATIVA dias) desde Maximo a Postgres.

DIFERENCIA CON bandeja_o_gesfo_completo.py:
    - Filtra desde Maximo (server-side): solo MC + recientes + INPRG/COMP/CLOSE
    - Volumen tipico: ~150-200 OTs (con ventana de 14 dias)
    - Tiempo: ~2-3 minutos (vs ~28 min del completo)
    - Frecuencia recomendada: cada 10 minutos (programado en scheduler.py)
    - NO toca las OTs MUY_ANTIGUA en BD (las maneja el completo a las 3am)

VENTANA OPERATIVA (en core/config.py: DIAS_VENTANA_OPERATIVA = 14):
    Esto cubre:
    - INPRG creadas en ultimos 14 dias (FRESCA, TIBIA, ANTIGUA)
    - COMP recientes (SOLUCIONADO)
    - CLOSE recientes (DOCUMENTADO)
    
    Las INPRG que envejecen mas de 14 dias pasan a MUY_ANTIGUA y siguen en BD,
    pero el operativo no las trae mas. El frontend filtra segun necesite.

CLASIFICACION OPERATIVA (umbrales en core/config.py):
    INPRG con < UMBRAL_FRESCA dias    → FRESCA
    INPRG con < UMBRAL_TIBIA dias     → TIBIA
    INPRG con < UMBRAL_ANTIGUA dias   → ANTIGUA
    INPRG con >= UMBRAL_ANTIGUA dias  → MUY_ANTIGUA
    COMP                               → SOLUCIONADO
    CLOSE                              → DOCUMENTADO

FILTRO DE OTs (server-side en Maximo):
    worktype = 'MC'
    AND classstructureid = '4213'
    AND ownergroup = 'O_GESFO'
    AND status IN ('INPRG', 'COMP', 'CLOSE')
    AND reportdate >= NOW() - DIAS_VENTANA_OPERATIVA dias

FILTRO DEFENSIVO (en Python, post-fetch):
    es_operativa() valida cada OT segun el campo de fecha apropiado por status:
        - INPRG: creation_date < DIAS_VENTANA_OPERATIVA dias
        - COMP:  changedate    < DIAS_VENTANA_OPERATIVA dias
        - CLOSE: actfinish     < DIAS_VENTANA_OPERATIVA dias

ORDEN DEL FLUJO (importante para evitar marcar envejecidas como salidas):
    1. Reclasificar PRIMERO segun edad actual (FRESCA/TIBIA/ANTIGUA/MUY_ANTIGUA)
    2. Obtener wonums dentro de la ventana operativa de BD (despues de reclasificar)
    3. Listar OTs de Maximo (con filtros server-side)
    4. Procesar cada una
    5. Marcar como salidas las que estaban en BD pero no aparecieron
       (ahora son SALIDAS REALES, no envejecimiento)

EJECUCION:
    py -m etl.bandeja_o_gesfo_operativa
    
    O via scheduler:
    py -m etl.scheduler   (lo lanza automaticamente cada 10 min)
"""

import sys
import time
import logging
from datetime import datetime, timedelta
from core.config import DIAS_VENTANA_OPERATIVA
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



# ════════════════════════════════════════════════════════════════════
# FILTRO DEFENSIVO (post-fetch)
# ════════════════════════════════════════════════════════════════════

def es_operativa(ot, ahora):
    """
    Filtro defensivo en Python. Como el filtro principal ya se aplica en
    Maximo (server-side) en listar_ots(), esta funcion confirma que cada
    OT es realmente operativa segun el campo de fecha apropiado por status:

        - INPRG: creadas en los ultimos DIAS_VENTANA_OPERATIVA dias (creation_date)
        - COMP:  cambiaron a COMP en los ultimos DIAS_VENTANA_OPERATIVA dias (changedate)
        - CLOSE: cerradas en los ultimos DIAS_VENTANA_OPERATIVA dias (actfinish)

    Esto asegura que el operativo tiene una vision consistente:
    OTs INPRG con menos de DIAS_VENTANA_OPERATIVA dias (frescas o demoradas).
    OTs COMP/CLOSE recientemente cerradas (visible para entregas de turno).
    """
    if ot.get("worktype") not in WORKTYPES_VALIDOS:
        return False

    status = ot.get("status")
    if status not in STATUSES_VALIDOS:
        return False

    umbral = ahora - timedelta(days=DIAS_VENTANA_OPERATIVA)

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

    ORDEN IMPORTANTE:
        1. Recalcular clasificacion de TODAS las OTs activas (segun edad actual)
        2. Obtener wonums dentro de la ventana operativa de BD
        3. Listar OTs de Maximo CON FILTROS SERVER-SIDE
        4. Filtro defensivo en Python (es_operativa)
        5. Procesar cada una
        6. Detectar SALIDAS REALES (las que estaban en BD y ya no aparecen)
        7. Estadisticas finales

    NO HACE limpieza de OTs antiguas (eso lo hace el ETL completo).
    """
    inicio = time.time()
    ahora = datetime.now()

    conn = obtener_conexion()
    if conn is None:
        logging.error("[Operativa] No se pudo conectar a Postgres. Abortando.")
        return False

    try:
        # 1. RECLASIFICAR PRIMERO segun edad actual
        # Recalcula clasificacion_operativa de todas las OTs activas:
        # FRESCA / TIBIA / ANTIGUA / MUY_ANTIGUA / SOLUCIONADO / DOCUMENTADO
        # Esto saca del cubo "ventana operativa" a las que cumplieron
        # DIAS_VENTANA_OPERATIVA dias antes de calcular salidas.
        cant_reclasificadas = reclasificar_envejecidas(conn)
        conn.commit()
        if cant_reclasificadas > 0:
            logging.info(f"[Operativa] OTs reclasificadas (cambio de categoria): "
                         f"{cant_reclasificadas}")

        # 2. Wonums dentro de la ventana operativa en BD (DESPUES de reclasificar)
        wonums_operativas_bd = obtener_wonums_operativas(conn)
        logging.info(f"[Operativa] Wonums en ventana operativa al inicio: "
                     f"{len(wonums_operativas_bd)}")

        # 3. Listar OTs de Maximo con filtros server-side
        # Le pedimos a Maximo solo las relevantes:
        # MC + creadas en ultimos DIAS_VENTANA_OPERATIVA dias + INPRG/COMP/CLOSE
        logging.info(f"[Operativa] Consultando Maximo con filtros server-side...")
        fecha_desde = ahora - timedelta(days=DIAS_VENTANA_OPERATIVA)
        todas_ots = listar_ots(
            ownergroup=OWNERGROUP,
            classstructureid=CLASSSTRUCTUREID,
            worktype="MC",
            fecha_desde=fecha_desde,
            status_in=list(STATUSES_VALIDOS),
        )
        logging.info(f"[Operativa] OTs traidas de Maximo: {len(todas_ots)}")

        # 4. Filtro defensivo en Python (validacion adicional por status+fecha)
        ots_operativas = [o for o in todas_ots if es_operativa(o, ahora)]
        logging.info(f"[Operativa] OTs operativas (validadas): {len(ots_operativas)}")

        # 5. Procesar cada una
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

        # 6. Detectar SALIDAS REALES
        # Las que estaban dentro de la ventana operativa en BD pero no aparecen
        # en Maximo. Como ya reclasificamos las envejecidas en el paso 1, las
        # que quedan aqui son salidas legitimas (cambiaron de owner, fueron
        # canceladas, etc.)
        wonums_que_salieron = wonums_operativas_bd - wonums_procesados
        cant_salidas = marcar_salidas_bandeja(wonums_que_salieron, conn)
        conn.commit()
        logging.info(f"[Operativa] OTs marcadas como salidas: {cant_salidas}")

        # 7. Estadisticas finales
        duracion = time.time() - inicio
        stats = contar_filas(conn)

        logging.info("="*60)
        logging.info("RESUMEN DE LA EJECUCION OPERATIVA")
        logging.info("="*60)
        logging.info(f"  Duracion total:                {duracion:.1f}s")
        logging.info(f"  Reclasificadas:                {cant_reclasificadas}")
        logging.info(f"  OTs Maximo (filtradas):        {len(todas_ots)}")
        logging.info(f"  OTs operativas (validadas):    {len(ots_operativas)}")
        logging.info(f"  Insertadas (nuevas):           {contadores['INSERTED']}")
        logging.info(f"  Actualizadas (con cambios):    {contadores['UPDATED']}")
        logging.info(f"  Sin cambios:                   {contadores['UNCHANGED']}")
        logging.info(f"  Con error:                     {contadores['ERROR']}")
        logging.info(f"  Worklogs cargados:             {worklogs_total}")
        logging.info(f"  Marcadas como salidas:         {cant_salidas}")
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