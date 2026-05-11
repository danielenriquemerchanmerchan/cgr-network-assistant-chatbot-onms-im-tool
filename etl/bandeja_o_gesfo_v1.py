"""
bandeja_o_gesfo.py
------------------
ETL UNICO de sincronizacion de la bandeja O_GESFO desde Maximo a Postgres.

MODELO DE NEGOCIO:
    El tablero muestra:
      - INPRG con reportdate dentro de DIAS_INPRG_RECIENTES dias.
        Las INPRG mas viejas son consideradas "huerfanas" y NO entran
        al tablero. Se ven solo desde control_inprg.py.
      - COMP/CLOSE/CAN con statusdate dentro de DIAS_RETENCION_CERRADAS
        dias. Sirve como traza historica corta del cierre.

ESTRATEGIA:
    Procesa TODAS las OTs del modelo en cada corrida. No hay diff
    incremental porque agregar un worklog NO actualiza changedate de
    la OT, asi que un diff por changedate perderia avances.

    El filtro DIAS_INPRG_RECIENTES mantiene el universo en ~350 OTs,
    lo cual es manejable cada 10 minutos.

EJECUCION:
    py -m etl.bandeja_o_gesfo
"""

import sys
import time
import logging
from datetime import datetime, timedelta

from core.config import DIAS_INPRG_RECIENTES, DIAS_RETENCION_CERRADAS
from core.logging_setup import logger

from integrations.maximo.rest_api import (
    listar_ots,
    obtener_detalle_ot,
    obtener_ci_description,
    extraer_worklogs_inline,
)
from integrations.maximo.oracle import (
    cargar_cache_sitios,
    aplicar_sitio_a_registro,
)
from integrations.postgres.client import (
    obtener_conexion,
    cerrar_conexion,
    upsert_work_order,
    reemplazar_worklogs,
    limpiar_fuera_de_modelo,
    borrar_wonums,
    contar_filas,
)
from domain.transformers.ot import construir_registro
from domain.transformers.worklog import construir_registros_worklog


# ═══════════════════════════════════════════════════════════════════
# CONFIGURACION
# ═══════════════════════════════════════════════════════════════════

OWNERGROUP = "O_GESFO"
CLASSSTRUCTUREID = "4213"
WORKTYPE = "MC"


# ═══════════════════════════════════════════════════════════════════
# UTILIDADES
# ═══════════════════════════════════════════════════════════════════

def calcular_etom_phase(status):
    mapping = {
        "WAPPR": "Pending",
        "INPRG": "Survey & Analyze",
        "WMATL": "Isolate",
        "COMP":  "Correct",
        "CLOSE": "Close",
        "CAN":   "Cancelled",
    }
    return mapping.get(status, "Unknown")


def mapear_a_postgres(registro):
    mapeo = {
        "reportdate":    "creation_date",
        "actstart":      "actual_start",
        "actfinish":     "actual_finish",
        "targstartdate": "target_start",
        "targcompdate":  "target_finish",
        "lead":          "assigned_to",
        "reportedby":    "reported_by",
        "ownergroup":    "owner_group",
        "impacto":       "severity",
    }
    nuevo = {}
    for k, v in registro.items():
        if k in mapeo:
            nuevo[mapeo[k]] = v
        elif k.isupper() or ("_" in k and any(c.isupper() for c in k)):
            nuevo[k.lower()] = v
        else:
            nuevo[k] = v
    return nuevo


def mapear_worklog_a_postgres(worklog):
    return worklog


# ═══════════════════════════════════════════════════════════════════
# LISTAR OTs DEL MODELO
# ═══════════════════════════════════════════════════════════════════

def listar_ots_del_modelo():
    """
    Lista todas las wonums+metadata del modelo:
        - INPRG con reportdate >= hace DIAS_INPRG_RECIENTES dias
        - COMP/CLOSE/CAN con statusdate >= hace DIAS_RETENCION_CERRADAS dias

    Las INPRG ultraviejas (>DIAS_INPRG_RECIENTES) NO se traen.
    Esas se consultan aparte desde control_inprg.py.
    """
    ahora = datetime.now()
    fecha_inprg = ahora - timedelta(days=DIAS_INPRG_RECIENTES)
    fecha_cerradas = ahora - timedelta(days=DIAS_RETENCION_CERRADAS)

    inprg = listar_ots(
        ownergroup=OWNERGROUP,
        classstructureid=CLASSSTRUCTUREID,
        worktype=WORKTYPE,
        status_in=["INPRG"],
        fecha_desde=fecha_inprg,
    )
    logging.info(
        f"[Listado] INPRG (ultimos {DIAS_INPRG_RECIENTES}d): {len(inprg)}"
    )

    cerradas = listar_ots(
        ownergroup=OWNERGROUP,
        classstructureid=CLASSSTRUCTUREID,
        worktype=WORKTYPE,
        status_in=["COMP", "CLOSE", "CAN"],
        statusdate_desde=fecha_cerradas,
    )
    logging.info(
        f"[Listado] COMP/CLOSE/CAN (ultimos {DIAS_RETENCION_CERRADAS}d): {len(cerradas)}"
    )

    return inprg + cerradas


# ═══════════════════════════════════════════════════════════════════
# PROCESAR UNA OT
# ═══════════════════════════════════════════════════════════════════

def procesar_ot(member, cache_sitios, ci_cache, conn):
    """Trae detalle, transforma, enriquece, UPSERT, reemplaza worklogs."""
    wonum = member.get("wonum")
    href = member.get("href")
    cinum = member.get("cinum") or ""

    resultado = {
        "wonum": wonum,
        "estado_upsert": None,
        "worklogs_count": 0,
        "error": None,
    }

    try:
        detalle = obtener_detalle_ot(href)
        if not detalle:
            resultado["error"] = "obtener_detalle_ot devolvio None"
            return resultado

        ci_desc = obtener_ci_description(cinum, cache=ci_cache) if cinum else ""

        worklogs_crudos = extraer_worklogs_inline(detalle)
        cant_worklogs = len(worklogs_crudos)

        registro = construir_registro(
            member=member,
            detalle=detalle,
            ci_description=ci_desc,
            cant_worklogs=cant_worklogs,
        )
        registro["etom_phase"] = calcular_etom_phase(registro.get("status"))
        registro = mapear_a_postgres(registro)
        registro = aplicar_sitio_a_registro(registro, cache_sitios)

        estado = upsert_work_order(registro, conn)
        resultado["estado_upsert"] = estado

        registros_worklog = construir_registros_worklog(wonum, worklogs_crudos)
        registros_worklog = [mapear_worklog_a_postgres(w) for w in registros_worklog]
        cant_insertados = reemplazar_worklogs(wonum, registros_worklog, conn)
        resultado["worklogs_count"] = cant_insertados

        conn.commit()

    except Exception as e:
        conn.rollback()
        resultado["error"] = f"{type(e).__name__}: {e}"
        logging.error(f"Error procesando OT {wonum}: {resultado['error']}")

    return resultado


# ═══════════════════════════════════════════════════════════════════
# ORQUESTADOR
# ═══════════════════════════════════════════════════════════════════

def sincronizar_bandeja():
    inicio = time.time()

    conn = obtener_conexion()
    if conn is None:
        logging.error("[Bandeja] No se pudo conectar a Postgres. Abortando.")
        return False

    try:
        # 1. Cache de sitios
        logging.info("[Bandeja] Cargando cache de sitios...")
        cache_sitios = cargar_cache_sitios()
        logging.info(f"[Bandeja] Cache sitios: {len(cache_sitios)} entradas")

        # 2. Listar OTs del modelo desde Maximo
        logging.info("[Bandeja] Listando OTs en Maximo...")
        t0 = time.time()
        ots_maximo = listar_ots_del_modelo()
        logging.info(
            f"[Bandeja] Total Maximo: {len(ots_maximo)} OTs ({time.time()-t0:.1f}s)"
        )

        # 3. Procesar TODAS las OTs (no hay diff incremental)
        # Razon: agregar un worklog NO actualiza changedate de la OT.
        # Para que los avances esten siempre frescos, refrescamos todo
        # cada corrida. El filtro DIAS_INPRG_RECIENTES mantiene el
        # universo en ~350 OTs, manejable cada 10 min.
        a_procesar = ots_maximo

        ci_cache = {}
        contadores = {"INSERTED": 0, "UPDATED": 0, "UNCHANGED": 0, "ERROR": 0}
        worklogs_total = 0

        for i, ot in enumerate(a_procesar, 1):
            resultado = procesar_ot(ot, cache_sitios, ci_cache, conn)
            if resultado["error"]:
                contadores["ERROR"] += 1
            else:
                contadores[resultado["estado_upsert"]] += 1
                worklogs_total += resultado["worklogs_count"]
            if i % 25 == 0:
                logging.info(f"[Bandeja] {i}/{len(a_procesar)}...")

        # 4. Limpieza A: huerfanas (en BD pero ya no estan en Maximo)
        wonums_maximo = {ot["wonum"] for ot in ots_maximo}
        with conn.cursor() as cur:
            cur.execute("SELECT wonum FROM onms.work_orders")
            wonums_bd = {row[0] for row in cur.fetchall()}
        huerfanas = wonums_bd - wonums_maximo
        cant_huerfanas = borrar_wonums(conn, huerfanas)
        if cant_huerfanas > 0:
            logging.info(f"[Bandeja] Borradas huerfanas: {cant_huerfanas}")

        # 5. Limpieza B: caducadas (>14d en COMP/CLOSE/CAN)
        cant_caducadas = limpiar_fuera_de_modelo(conn, DIAS_RETENCION_CERRADAS)
        if cant_caducadas > 0:
            logging.info(f"[Bandeja] Borradas caducadas: {cant_caducadas}")

        conn.commit()

        # 6. Stats
        duracion = time.time() - inicio
        stats = contar_filas(conn)

        logging.info("=" * 60)
        logging.info("RESUMEN")
        logging.info("=" * 60)
        logging.info(f"  Duracion:                  {duracion:.1f}s")
        logging.info(f"  OTs Maximo (modelo):       {len(ots_maximo)}")
        logging.info(f"  Procesadas con detalle:    {len(a_procesar)}")
        logging.info(f"  Insertadas:                {contadores['INSERTED']}")
        logging.info(f"  Actualizadas:              {contadores['UPDATED']}")
        logging.info(f"  Sin cambios reales:        {contadores['UNCHANGED']}")
        logging.info(f"  Errores:                   {contadores['ERROR']}")
        logging.info(f"  Worklogs cargados:         {worklogs_total}")
        logging.info(f"  Borradas huerfanas:        {cant_huerfanas}")
        logging.info(f"  Borradas caducadas:        {cant_caducadas}")
        logging.info(f"  --- BD final ---")
        logging.info(f"  work_orders:               {stats['work_orders']}")
        logging.info(f"  worklogs:                  {stats['worklogs']}")
        logging.info(f"  Por status:                {stats['por_status']}")
        logging.info("=" * 60)

        return True

    except Exception as e:
        logging.exception(f"[Bandeja] Error inesperado: {e}")
        conn.rollback()
        return False
    finally:
        cerrar_conexion(conn)


def main():
    logging.info("Iniciando ETL bandeja_o_gesfo")
    exito = sincronizar_bandeja()
    sys.exit(0 if exito else 1)


if __name__ == "__main__":
    main()