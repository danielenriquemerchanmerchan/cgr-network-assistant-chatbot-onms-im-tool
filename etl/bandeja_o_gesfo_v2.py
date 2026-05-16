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

OPTIMIZACIONES DE ESCRITURA (2026-05):
    - El commit se hace por batch de N OTs (no por OT). Esto reduce
      drasticamente la cantidad de fsync forzados.
    - Si una OT falla, su excepcion se captura y se hace rollback al
      batch parcial. Como el ETL es idempotente (UPSERT + reemplazo
      total de worklogs) y corre cada 10 min, perder un batch no es
      problema: el proximo ciclo lo reprocesa.

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

from bot.services.asignador_ot import asignar_ots_pendientes
from bot.services.limpiador_ot import limpiar_huerfanas


# ═══════════════════════════════════════════════════════════════════
# CONFIGURACION
# ═══════════════════════════════════════════════════════════════════

OWNERGROUP = "O_GESFO"
CLASSSTRUCTUREID = "4213"
WORKTYPE = "MC"

# Cada cuantas OTs procesadas hacer commit a Postgres.
# - Muy bajo (1): muchos fsync, lento (era el problema original).
# - Muy alto (>200): si una OT falla, se pierde un batch grande y
#   hay que reprocesarlo todo en el proximo ciclo.
# 50 es un buen balance.
COMMIT_CADA = 50


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
    """
    Trae detalle, transforma, enriquece, UPSERT, reemplaza worklogs.

    NOTA: NO hace commit. El commit es responsabilidad del orquestador
    (cada COMMIT_CADA OTs). Esto reduce drasticamente la cantidad de
    fsync forzados en Postgres.

    [INSTRUMENTACION] Devuelve tambien tiempos por fase para diagnostico:
        - t_detalle:   GET de detalle a Maximo
        - t_ci:        GET de description del CI (puede venir de cache)
        - t_transform: transformacion + enriquecimiento en memoria
        - t_pg:        upsert + reemplazo de worklogs en Postgres
                       (sin contar commit)
    """
    wonum = member.get("wonum")
    href = member.get("href")
    cinum = member.get("cinum") or ""

    resultado = {
        "wonum": wonum,
        "estado_upsert": None,
        "worklogs_count": 0,
        "error": None,
        "t_detalle": 0.0,
        "t_ci": 0.0,
        "t_transform": 0.0,
        "t_pg": 0.0,
    }

    try:
        # --- Fase 1: GET detalle a Maximo ---
        t0 = time.time()
        detalle = obtener_detalle_ot(href)
        resultado["t_detalle"] = time.time() - t0

        if not detalle:
            resultado["error"] = "obtener_detalle_ot devolvio None"
            return resultado

        # --- Fase 2: GET description del CI (con cache) ---
        t0 = time.time()
        ci_desc = obtener_ci_description(cinum, cache=ci_cache) if cinum else ""
        resultado["t_ci"] = time.time() - t0

        # --- Fase 3: Transformacion en memoria ---
        t0 = time.time()
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

        registros_worklog = construir_registros_worklog(wonum, worklogs_crudos)
        registros_worklog = [mapear_worklog_a_postgres(w) for w in registros_worklog]
        resultado["t_transform"] = time.time() - t0

        # --- Fase 4: Postgres (upsert + worklogs, SIN commit) ---
        t0 = time.time()
        estado = upsert_work_order(registro, conn)
        resultado["estado_upsert"] = estado

        cant_insertados = reemplazar_worklogs(wonum, registros_worklog, conn)
        resultado["worklogs_count"] = cant_insertados
        resultado["t_pg"] = time.time() - t0

    except Exception as e:
        # Importante: el rollback del batch lo hace el orquestador.
        # Aqui solo capturamos para no romper el loop.
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
        batches_fallidos = 0

        # [INSTRUMENTACION] Acumuladores de tiempos por fase
        t_detalle_total = 0.0
        t_ci_total = 0.0
        t_transform_total = 0.0
        t_pg_total = 0.0
        t_commit_total = 0.0
        t_detalle_max = 0.0
        t_pg_max = 0.0
        wonum_detalle_max = None
        wonum_pg_max = None

        t_loop_inicio = time.time()
        for i, ot in enumerate(a_procesar, 1):
            resultado = procesar_ot(ot, cache_sitios, ci_cache, conn)

            # Acumular tiempos para diagnostico
            t_detalle_total += resultado["t_detalle"]
            t_ci_total += resultado["t_ci"]
            t_transform_total += resultado["t_transform"]
            t_pg_total += resultado["t_pg"]

            if resultado["t_detalle"] > t_detalle_max:
                t_detalle_max = resultado["t_detalle"]
                wonum_detalle_max = resultado["wonum"]
            if resultado["t_pg"] > t_pg_max:
                t_pg_max = resultado["t_pg"]
                wonum_pg_max = resultado["wonum"]

            if resultado["error"]:
                contadores["ERROR"] += 1
                # Si una OT del batch fallo, rollback al batch parcial.
                # Las OTs que pasaron antes en este batch se reprocesaran
                # en el proximo ciclo del ETL (es idempotente).
                try:
                    conn.rollback()
                    batches_fallidos += 1
                except Exception:
                    pass
            else:
                contadores[resultado["estado_upsert"]] += 1
                worklogs_total += resultado["worklogs_count"]

            # Commit por batch
            if i % COMMIT_CADA == 0:
                t_c0 = time.time()
                try:
                    conn.commit()
                except Exception as e:
                    logging.error(f"[Bandeja] Error en commit del batch {i}: {e}")
                    conn.rollback()
                    batches_fallidos += 1
                t_commit_total += time.time() - t_c0
                logging.info(f"[Bandeja] {i}/{len(a_procesar)}...")

        # Commit final del ultimo batch (las OTs restantes)
        t_c0 = time.time()
        try:
            conn.commit()
        except Exception as e:
            logging.error(f"[Bandeja] Error en commit final: {e}")
            conn.rollback()
            batches_fallidos += 1
        t_commit_total += time.time() - t_c0

        t_loop_total = time.time() - t_loop_inicio

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

        # 6. Asignar OTs nuevas a ot_bandeja con su coordinador
        # Toma las INPRG en work_orders que aun no estan en ot_bandeja
        # y las inserta consultando onms.coordinador_zona. No envia
        # Telegram (eso es trabajo del notificador del bot, que corre
        # como job aparte cada N segundos dentro del bot).
        # Si falla, NO se rompe el ETL: lo que ya quedo en work_orders
        # esta persistido y el proximo ciclo reintentara la asignacion.
        try:
            stats_asig = asignar_ots_pendientes(conn)
            logging.info(
                f"[Asignador] Evaluadas={stats_asig['evaluadas']} | "
                f"asignadas_ok={stats_asig['asignadas_ok']} | "
                f"sin_coord={stats_asig['sin_coordinador']} | "
                f"errores={stats_asig['errores']}"
            )
        except Exception as e:
            stats_asig = None
            logging.exception(
                f"[Asignador] Falla inesperada del asignador (el ETL "
                f"continua con sus stats): {e}"
            )

        # 6.b. Limpiar OTs huerfanas (asignaciones cuya OT ya no esta en
        # work_orders porque cambio de grupo, se cerro, etc.).
        # Solo MARCA asignacion_activa=false. Los avisos a las cuadrillas
        # quedan pendientes de implementar (futuro: integrar con notificador
        # del bot).
        try:
            huerfanas = limpiar_huerfanas(conn)
            if huerfanas:
                logging.info(
                    f"[Limpiador] {len(huerfanas)} OT(s) huerfana(s) "
                    f"desactivada(s):"
                )
                for h in huerfanas:
                    logging.info(
                        f"[Limpiador] - asig={h['asignacion_id']} "
                        f"wonum={h['wonum']} cuadrilla={h['cuadrilla_id']}"
                    )
            else:
                logging.info("[Limpiador] No hay huerfanas para limpiar.")
        except Exception as e:
            logging.exception(
                f"[Limpiador] Falla en limpieza de huerfanas: {e}"
            )

        # 7. Stats
        duracion = time.time() - inicio
        stats = contar_filas(conn)

        # Promedios (evita division por cero)
        n = max(len(a_procesar), 1)
        t_detalle_avg = t_detalle_total / n
        t_ci_avg = t_ci_total / n
        t_transform_avg = t_transform_total / n
        t_pg_avg = t_pg_total / n

        logging.info("=" * 60)
        logging.info("RESUMEN")
        logging.info("=" * 60)
        logging.info(f"  Duracion total:            {duracion:.1f}s")
        logging.info(f"  OTs Maximo (modelo):       {len(ots_maximo)}")
        logging.info(f"  Procesadas con detalle:    {len(a_procesar)}")
        logging.info(f"  Insertadas:                {contadores['INSERTED']}")
        logging.info(f"  Actualizadas:              {contadores['UPDATED']}")
        logging.info(f"  Sin cambios reales:        {contadores['UNCHANGED']}")
        logging.info(f"  Errores:                   {contadores['ERROR']}")
        logging.info(f"  Batches con rollback:      {batches_fallidos}")
        logging.info(f"  Worklogs cargados:         {worklogs_total}")
        logging.info(f"  Borradas huerfanas:        {cant_huerfanas}")
        logging.info(f"  Borradas caducadas:        {cant_caducadas}")
        logging.info(f"  --- BD final ---")
        logging.info(f"  work_orders:               {stats['work_orders']}")
        logging.info(f"  worklogs:                  {stats['worklogs']}")
        logging.info(f"  Por status:                {stats['por_status']}")
        logging.info(f"  --- DIAGNOSTICO DE TIEMPOS ---")
        logging.info(f"  Loop completo:             {t_loop_total:.1f}s ({n} OTs)")
        logging.info(f"  Maximo (detalle) total:    {t_detalle_total:.1f}s "
                     f"({100*t_detalle_total/t_loop_total:.0f}%) | "
                     f"avg {t_detalle_avg*1000:.0f}ms | "
                     f"max {t_detalle_max*1000:.0f}ms ({wonum_detalle_max})")
        logging.info(f"  Maximo (CI desc) total:    {t_ci_total:.1f}s "
                     f"({100*t_ci_total/t_loop_total:.0f}%) | "
                     f"avg {t_ci_avg*1000:.0f}ms")
        logging.info(f"  Transformacion total:      {t_transform_total:.1f}s "
                     f"({100*t_transform_total/t_loop_total:.0f}%) | "
                     f"avg {t_transform_avg*1000:.0f}ms")
        logging.info(f"  Postgres (sin commit):     {t_pg_total:.1f}s "
                     f"({100*t_pg_total/t_loop_total:.0f}%) | "
                     f"avg {t_pg_avg*1000:.0f}ms | "
                     f"max {t_pg_max*1000:.0f}ms ({wonum_pg_max})")
        logging.info(f"  Postgres (commits batch):  {t_commit_total:.1f}s "
                     f"({100*t_commit_total/t_loop_total:.0f}%)")
        suma_fases = (t_detalle_total + t_ci_total + t_transform_total
                      + t_pg_total + t_commit_total)
        overhead = t_loop_total - suma_fases
        logging.info(f"  Overhead (no medido):      {overhead:.1f}s "
                     f"({100*overhead/t_loop_total:.0f}%)")
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