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

ARQUITECTURA DE 2 FASES (2026-05):
    El procesamiento se divide en dos fases para optimizar I/O:

    Fase 1 (PARALELA, MAX_WORKERS hilos):
        Descarga de Maximo: detalle de cada OT + description del CI.
        Es I/O puro, libera el GIL durante la espera HTTP, asi que
        N hilos descargan en paralelo. Postgres no se toca aqui.

    Fase 2 (SECUENCIAL, 1 hilo):
        Transformacion + UPSERT + reemplazo de worklogs + commit por
        batch. Una sola conexion a Postgres, sin contencion.

    Esto convierte ~166s de I/O secuencial en ~21s de I/O paralela,
    sin tocar la logica de Postgres (que ya es rapida con execute_values).

OPTIMIZACIONES DE ESCRITURA (2026-05):
    - reemplazar_worklogs usa execute_values (1 roundtrip por OT en
      lugar de N).
    - commit cada COMMIT_CADA OTs (no por OT).
    - Si una OT falla, rollback del batch parcial. Como el ETL es
      idempotente (UPSERT + reemplazo total de worklogs) y corre cada
      10 min, perder un batch no es problema.

EJECUCION:
    py -m etl.bandeja_o_gesfo
"""

import sys
import time
import logging
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# Workers para la fase de descarga paralela contra Maximo.
# - 1: comportamiento secuencial original.
# - 4-8: balance entre velocidad y carga sobre el servidor Maximo.
# - >12: rendimientos marginales decrecientes y mayor riesgo de
#   saturar al servidor on-prem.
MAX_WORKERS_MAXIMO = 8

# Cada cuantas OTs procesadas hacer commit a Postgres.
# - Muy bajo (1): muchos fsync, lento (era el problema original).
# - Muy alto (>200): si una OT falla, se pierde un batch grande.
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
# FASE 1: DESCARGA PARALELA DESDE MAXIMO
# ═══════════════════════════════════════════════════════════════════

def descargar_ot_de_maximo(member, ci_cache, ci_cache_lock):
    """
    Trabajo de un worker en la fase paralela.
    Solo hace I/O contra Maximo (detalle + CI desc). NO toca Postgres.

    El ci_cache es compartido entre threads y se protege con un lock
    minimal solo para el get/set del dict. En el peor caso, dos threads
    pidiendo el mismo CI simultaneamente lo descargan dos veces; no
    rompe correctitud, solo es ineficiente. El lock evita corrupcion
    del dict, no la doble descarga.

    Retorna un dict con todo lo necesario para que la fase 2 (secuencial)
    pueda hacer la transformacion y escritura sin tocar Maximo.
    """
    wonum = member.get("wonum")
    href = member.get("href")
    cinum = member.get("cinum") or ""

    paquete = {
        "wonum": wonum,
        "member": member,
        "detalle": None,
        "ci_desc": "",
        "error": None,
        "t_detalle": 0.0,
        "t_ci": 0.0,
    }

    try:
        # GET detalle
        t0 = time.time()
        detalle = obtener_detalle_ot(href)
        paquete["t_detalle"] = time.time() - t0

        if not detalle:
            paquete["error"] = "obtener_detalle_ot devolvio None"
            return paquete

        paquete["detalle"] = detalle

        # GET CI desc (con cache compartido thread-safe)
        if cinum:
            t0 = time.time()
            # Check cache (con lock corto)
            with ci_cache_lock:
                ci_desc = ci_cache.get(cinum)
            if ci_desc is None:
                # No estaba: descargar (sin lock, es la parte lenta)
                ci_desc = obtener_ci_description(cinum, cache=None) or ""
                # Guardar (con lock corto)
                with ci_cache_lock:
                    ci_cache[cinum] = ci_desc
            paquete["ci_desc"] = ci_desc
            paquete["t_ci"] = time.time() - t0

    except Exception as e:
        paquete["error"] = f"{type(e).__name__}: {e}"
        logging.error(f"Error descargando OT {wonum}: {paquete['error']}")

    return paquete


def descargar_todos_paralelo(ots_maximo, max_workers):
    """
    Fase 1: descarga en paralelo los detalles + CI de todas las OTs.
    Retorna lista de paquetes en el orden original (no es estricto,
    pero ayuda al log progresivo).
    """
    ci_cache = {}
    ci_cache_lock = threading.Lock()
    paquetes = [None] * len(ots_maximo)
    completadas = 0
    log_every = max(50, len(ots_maximo) // 8)

    with ThreadPoolExecutor(max_workers=max_workers,
                            thread_name_prefix="maximo-dl") as ex:
        # Mapear cada future al indice original
        future_to_idx = {
            ex.submit(descargar_ot_de_maximo, ot, ci_cache, ci_cache_lock): i
            for i, ot in enumerate(ots_maximo)
        }

        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                paquetes[idx] = future.result()
            except Exception as e:
                # No deberia entrar aqui porque descargar_ot_de_maximo
                # captura sus propios errores, pero por defensividad:
                paquetes[idx] = {
                    "wonum": ots_maximo[idx].get("wonum"),
                    "member": ots_maximo[idx],
                    "detalle": None,
                    "ci_desc": "",
                    "error": f"future_exception: {type(e).__name__}: {e}",
                    "t_detalle": 0.0,
                    "t_ci": 0.0,
                }
            completadas += 1
            if completadas % log_every == 0:
                logging.info(
                    f"[Descarga] {completadas}/{len(ots_maximo)} OTs bajadas..."
                )

    return paquetes


# ═══════════════════════════════════════════════════════════════════
# FASE 2: ESCRITURA SECUENCIAL A POSTGRES
# ═══════════════════════════════════════════════════════════════════

def escribir_ot_en_postgres(paquete, cache_sitios, conn):
    """
    Fase 2: con el paquete ya bajado, transforma y escribe a Postgres.
    NO hace commit (lo hace el orquestador por batch).
    """
    wonum = paquete["wonum"]
    resultado = {
        "wonum": wonum,
        "estado_upsert": None,
        "worklogs_count": 0,
        "error": paquete["error"],
        "t_transform": 0.0,
        "t_pg": 0.0,
    }

    # Si la descarga ya fallo, no hay nada que escribir
    if paquete["error"] or paquete["detalle"] is None:
        return resultado

    try:
        member = paquete["member"]
        detalle = paquete["detalle"]
        ci_desc = paquete["ci_desc"]

        # --- Transformacion en memoria ---
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

        # --- Postgres (upsert + worklogs, SIN commit) ---
        t0 = time.time()
        estado = upsert_work_order(registro, conn)
        resultado["estado_upsert"] = estado

        cant_insertados = reemplazar_worklogs(wonum, registros_worklog, conn)
        resultado["worklogs_count"] = cant_insertados
        resultado["t_pg"] = time.time() - t0

    except Exception as e:
        resultado["error"] = f"{type(e).__name__}: {e}"
        logging.error(f"Error escribiendo OT {wonum}: {resultado['error']}")

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

        a_procesar = ots_maximo

        # ───────────────────────────────────────────────────────────
        # FASE 1: Descarga paralela desde Maximo
        # ───────────────────────────────────────────────────────────
        logging.info(
            f"[Bandeja] Fase 1: descargando detalles ({MAX_WORKERS_MAXIMO} workers)..."
        )
        t_fase1_inicio = time.time()
        paquetes = descargar_todos_paralelo(a_procesar, MAX_WORKERS_MAXIMO)
        t_fase1 = time.time() - t_fase1_inicio

        # Sumas de tiempos individuales (suma > t_fase1 porque hubo paralelismo)
        t_detalle_total = sum(p["t_detalle"] for p in paquetes)
        t_ci_total = sum(p["t_ci"] for p in paquetes)
        errores_descarga = sum(1 for p in paquetes if p["error"])
        logging.info(
            f"[Bandeja] Fase 1 completa: {t_fase1:.1f}s reales "
            f"(suma serial habria sido {t_detalle_total + t_ci_total:.1f}s) "
            f"| errores_descarga={errores_descarga}"
        )

        # ───────────────────────────────────────────────────────────
        # FASE 2: Escritura secuencial a Postgres
        # ───────────────────────────────────────────────────────────
        logging.info("[Bandeja] Fase 2: escribiendo a Postgres...")
        t_fase2_inicio = time.time()

        contadores = {"INSERTED": 0, "UPDATED": 0, "UNCHANGED": 0, "ERROR": 0}
        worklogs_total = 0
        batches_fallidos = 0
        t_transform_total = 0.0
        t_pg_total = 0.0
        t_commit_total = 0.0
        t_pg_max = 0.0
        wonum_pg_max = None

        for i, paquete in enumerate(paquetes, 1):
            resultado = escribir_ot_en_postgres(paquete, cache_sitios, conn)

            t_transform_total += resultado["t_transform"]
            t_pg_total += resultado["t_pg"]
            if resultado["t_pg"] > t_pg_max:
                t_pg_max = resultado["t_pg"]
                wonum_pg_max = resultado["wonum"]

            if resultado["error"]:
                contadores["ERROR"] += 1
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
                logging.info(f"[Escritura] {i}/{len(paquetes)}...")

        # Commit final
        t_c0 = time.time()
        try:
            conn.commit()
        except Exception as e:
            logging.error(f"[Bandeja] Error en commit final: {e}")
            conn.rollback()
            batches_fallidos += 1
        t_commit_total += time.time() - t_c0

        t_fase2 = time.time() - t_fase2_inicio

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

        # 6.b. Limpiar OTs huerfanas
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
        n = max(len(a_procesar), 1)

        # Speedup de la paralelizacion
        suma_serial = t_detalle_total + t_ci_total
        speedup = suma_serial / t_fase1 if t_fase1 > 0 else 0

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
        logging.info(f"  Errores descarga Maximo:   {errores_descarga}")
        logging.info(f"  Batches con rollback:      {batches_fallidos}")
        logging.info(f"  Worklogs cargados:         {worklogs_total}")
        logging.info(f"  Borradas huerfanas:        {cant_huerfanas}")
        logging.info(f"  Borradas caducadas:        {cant_caducadas}")
        logging.info(f"  --- BD final ---")
        logging.info(f"  work_orders:               {stats['work_orders']}")
        logging.info(f"  worklogs:                  {stats['worklogs']}")
        logging.info(f"  Por status:                {stats['por_status']}")
        logging.info(f"  --- DIAGNOSTICO DE TIEMPOS ---")
        logging.info(f"  Fase 1 - Descarga paralela: {t_fase1:.1f}s "
                     f"({MAX_WORKERS_MAXIMO} workers)")
        logging.info(f"    suma serial habria sido:  {suma_serial:.1f}s")
        logging.info(f"    speedup:                  {speedup:.1f}x")
        logging.info(f"    Maximo detalle (suma):    {t_detalle_total:.1f}s "
                     f"| avg {t_detalle_total/n*1000:.0f}ms/OT")
        logging.info(f"    Maximo CI desc (suma):    {t_ci_total:.1f}s "
                     f"| avg {t_ci_total/n*1000:.0f}ms/OT")
        logging.info(f"  Fase 2 - Escritura PG:      {t_fase2:.1f}s")
        logging.info(f"    Transformacion:           {t_transform_total:.1f}s")
        logging.info(f"    Postgres (sin commit):    {t_pg_total:.1f}s "
                     f"| avg {t_pg_total/n*1000:.0f}ms/OT "
                     f"| max {t_pg_max*1000:.0f}ms ({wonum_pg_max})")
        logging.info(f"    Postgres (commits):       {t_commit_total:.1f}s")
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