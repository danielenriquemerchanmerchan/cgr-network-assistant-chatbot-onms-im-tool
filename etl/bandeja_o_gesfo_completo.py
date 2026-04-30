"""
bandeja_o_gesfo.py
------------------
ETL de sincronizacion de la bandeja activa de O_GESFO desde Maximo a Postgres.

PROPOSITO:
    Mantener la tabla onms.work_orders sincronizada con Maximo cada 5 minutos.
    Cada ejecucion:
        1. Trae OTs vigentes de Maximo
        2. UPSERT en work_orders (solo cambios reales)
        3. Reemplaza worklogs de cada OT
        4. Marca como salidas las OTs que ya no aparecen
        5. Limpia OTs viejas (mas de 5 dias inactivas)

FILTRO DE OTs RELEVANTES:
    worktype = 'MC' AND classstructureid = '4213' AND ownergroup = 'O_GESFO'
    Y ademas:
        status = 'INPRG'                                    (TODAS)
        OR (status = 'COMP'  AND changedate >= NOW() - 7d)  (recientes)
        OR (status = 'CLOSE' AND actfinish  >= NOW() - 7d)  (recientes)

CLASIFICACION OPERATIVA:
    OPERATIVA       -> creacion < 7 dias O cualquier CLOSE recien cerrada
    INPRG_ZOMBIE    -> INPRG con creacion > 7 dias (sin actividad reciente)
    COMP_ZOMBIE     -> COMP con creacion > 7 dias

EJECUCION:
    py -m etl.bandeja_o_gesfo

PROXIMA FASE:
    Programar con Task Scheduler de Windows para ejecucion cada 5 min.
"""

import sys
import time
import logging
from datetime import datetime, timedelta

from core.config import UMBRAL_FRESCA, UMBRAL_TIBIA, UMBRAL_ANTIGUA, DIAS_VENTANA_OPERATIVA
from core.logging_setup import logger  # inicializa logging
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
    obtener_wonums_activos,
    upsert_work_order,
    reemplazar_worklogs,
    marcar_salidas_bandeja,
    limpiar_viejas_salidas,
    contar_filas,
)


# ════════════════════════════════════════════════════════════════════
# CONFIGURACION
# ════════════════════════════════════════════════════════════════════

OWNERGROUP = "O_GESFO"
CLASSSTRUCTUREID = "4213"
WORKTYPES_VALIDOS = {"MC"}
STATUSES_VALIDOS = {"INPRG", "COMP", "CLOSE"}

DIAS_OPERATIVA = 7              # Threshold para clasificar OPERATIVA vs ZOMBIE
DIAS_RETENCION_SALIDAS = 5      # Eliminar OTs que llevan mas de N dias inactivas


# ════════════════════════════════════════════════════════════════════
# FUNCIONES AUXILIARES
# ════════════════════════════════════════════════════════════════════

def parsear_fecha(fecha_str):
    """Convierte fecha ISO de Maximo a datetime. Devuelve None si falla."""
    if not fecha_str:
        return None
    try:
        return datetime.strptime(fecha_str[:19], "%Y-%m-%dT%H:%M:%S")
    except Exception:
        return None


def es_relevante(ot, ahora):
    """
    Determina si una OT pasa el filtro para entrar a la bandeja.

    Reglas (umbral en core/config.py: DIAS_VENTANA_OPERATIVA):
        - worktype debe ser MC
        - status INPRG: siempre (sin importar antiguedad)
        - status COMP:  solo si changedate >= hace DIAS_VENTANA_OPERATIVA dias
        - status CLOSE: solo si actfinish  >= hace DIAS_VENTANA_OPERATIVA dias
    """
    # Filtro 1: worktype
    if ot.get("worktype") not in WORKTYPES_VALIDOS:
        return False

    # Filtro 2: status
    status = ot.get("status")
    if status not in STATUSES_VALIDOS:
        return False

    # Filtro 3: por status, fecha de relevancia
    if status == "INPRG":
        return True

    umbral = ahora - timedelta(days=DIAS_VENTANA_OPERATIVA)

    if status == "COMP":
        changedate = parsear_fecha(ot.get("changedate"))
        return changedate is not None and changedate >= umbral

    if status == "CLOSE":
        actfinish = parsear_fecha(ot.get("actfinish"))
        return actfinish is not None and actfinish >= umbral

    return False


def clasificar_ot(ot, ahora):
    """
    Determina la clasificacion operativa de una OT segun status + antiguedad.

    Reglas (umbrales en core/config.py):
        INPRG:
            < UMBRAL_FRESCA dias        → FRESCA
            < UMBRAL_TIBIA dias         → TIBIA
            < UMBRAL_ANTIGUA dias       → ANTIGUA
            >= UMBRAL_ANTIGUA dias      → MUY_ANTIGUA
        COMP   → SOLUCIONADO
        CLOSE  → DOCUMENTADO

    La clasificacion es DESCRIPTIVA. No controla logica de salidas ni
    visibilidad. El frontend decide como visualizar segun la categoria.

    Returns:
        'FRESCA' | 'TIBIA' | 'ANTIGUA' | 'MUY_ANTIGUA' | 'SOLUCIONADO' | 'DOCUMENTADO'
    """
    status = ot.get("status")

    if status == "COMP":
        return "SOLUCIONADO"

    if status == "CLOSE":
        return "DOCUMENTADO"

    if status == "INPRG":
        creation = parsear_fecha(ot.get("reportdate"))
        if creation is None:
            return "FRESCA"

        dias_creada = (ahora - creation).total_seconds() / 86400

        if dias_creada < UMBRAL_FRESCA:
            return "FRESCA"
        if dias_creada < UMBRAL_TIBIA:
            return "TIBIA"
        if dias_creada < UMBRAL_ANTIGUA:
            return "ANTIGUA"
        return "MUY_ANTIGUA"

    # Status desconocido (no deberia llegar aqui por los filtros previos)
    return "FRESCA"

def calcular_etom_phase(status):
    """Mapea status de Maximo a fase eTOM (TMForum)."""
    mapping = {
        "WAPPR":  "Pending",
        "INPRG":  "Survey & Analyze",
        "WMATL":  "Isolate",
        "COMP":   "Correct",
        "CLOSE":  "Close",
    }
    return mapping.get(status, "Unknown")


# ════════════════════════════════════════════════════════════════════
# PROCESAR UNA OT
# ════════════════════════════════════════════════════════════════════

def procesar_ot(member, ahora, ci_cache, conn):
    """
    Procesa una OT individual:
        1. Trae detalle completo de Maximo
        2. Trae descripcion del CI (con cache)
        3. Aplana via transformer
        4. Calcula campos derivados (etom_phase, clasificacion)
        5. UPSERT en work_orders
        6. Reemplaza sus worklogs

    Retorna dict con resultado:
        {
            'wonum': str,
            'estado_upsert': 'INSERTED'|'UPDATED'|'UNCHANGED',
            'worklogs_count': int,
            'error': str | None
        }
    """
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
        # 1. Detalle completo
        detalle = obtener_detalle_ot(href)
        if not detalle:
            resultado["error"] = "obtener_detalle_ot devolvio None"
            return resultado

        # 2. CI description
        ci_desc = obtener_ci_description(cinum, cache=ci_cache) if cinum else ""

        # 3. Worklogs crudos
        worklogs_crudos = extraer_worklogs_inline(detalle)
        cant_worklogs = len(worklogs_crudos)

        # 4. Aplanar OT
        registro = construir_registro(
            member=member,
            detalle=detalle,
            ci_description=ci_desc,
            cant_worklogs=cant_worklogs,
        )

        # 5. Campos derivados
        registro["etom_phase"] = calcular_etom_phase(registro.get("status"))
        registro["clasificacion_operativa"] = clasificar_ot(member, ahora)

        # 6. Renombrar campos para que coincidan con el modelo Postgres
        # ot_transformer entrega 'reportdate', BD espera 'creation_date'
        # ot_transformer entrega 'actstart', BD espera 'actual_start'
        # etc.
        registro = mapear_a_postgres(registro)

        # 7. UPSERT
        estado = upsert_work_order(registro, conn)
        resultado["estado_upsert"] = estado

        # 8. Worklogs aplanados
        registros_worklog = construir_registros_worklog(wonum, worklogs_crudos)
        registros_worklog = [mapear_worklog_a_postgres(w) for w in registros_worklog]

        cant_insertados = reemplazar_worklogs(wonum, registros_worklog, conn)
        resultado["worklogs_count"] = cant_insertados

        # 9. Commit por OT (estrategia de "fail safe")
        conn.commit()

    except Exception as e:
        conn.rollback()
        resultado["error"] = f"{type(e).__name__}: {e}"
        logging.error(f"Error procesando OT {wonum}: {resultado['error']}")

    return resultado


def mapear_a_postgres(registro):
    """
    Renombra campos del transformer al esquema de Postgres.

    Hace dos cosas:
    1. Renombra los campos top-level (Maximo usa 'reportdate', Postgres 'creation_date').
    2. Convierte a minusculas las claves de los SPEC_CAMPOS, que el transformer
       devuelve en mayusculas (porque asi vienen de Maximo) pero Postgres las
       tiene definidas en minusculas (convencion SQL profesional).

    El transformer y el Excel historico no se ven afectados. Esta conversion
    solo aplica para el camino de Postgres.
    """
    mapeo = {
        "reportdate": "creation_date",
        "actstart": "actual_start",
        "targstartdate": "target_start",
        "targcompdate": "target_finish",
        "actfinish": "actual_finish",
        "lead": "assigned_to",
        "reportedby": "reported_by",
        "ownergroup": "owner_group",
        "impacto": "severity",
    }

    nuevo = {}
    for k, v in registro.items():
        # Si el campo esta en el mapeo explicito, usar el nuevo nombre
        if k in mapeo:
            nuevo[mapeo[k]] = v
        # Si esta en mayusculas (probable spec), convertir a minusculas
        elif k.isupper() or "_" in k and any(c.isupper() for c in k):
            nuevo[k.lower()] = v
        # Sino, dejar tal cual
        else:
            nuevo[k] = v

    return nuevo


def mapear_worklog_a_postgres(worklog):
    """Renombra campos de worklog al esquema de Postgres."""
    # En este caso los nombres ya son los mismos
    return worklog


# ════════════════════════════════════════════════════════════════════
# ORQUESTADOR
# ════════════════════════════════════════════════════════════════════

def sincronizar_bandeja():
    """
    Flujo principal del ETL.

    Pasos:
        1. Conectar a Postgres
        2. Listar OTs de Maximo
        3. Filtrar relevantes
        4. Procesar cada una
        5. Detectar salidas y marcarlas
        6. Limpiar viejas
        7. Estadisticas finales
    """
    inicio = time.time()
    ahora = datetime.now()

    # 1. Conectar
    conn = obtener_conexion()
    if conn is None:
        logging.error("[Bandeja] No se pudo conectar a Postgres. Abortando.")
        return False

    try:
        # 2. Wonums actualmente activos en BD (para diff)
        wonums_en_bd = obtener_wonums_activos(conn)
        logging.info(f"[Bandeja] Wonums activos en BD al inicio: {len(wonums_en_bd)}")

        # 3. Listar OTs de Maximo (todas las de O_GESFO/4213, sin filtrar status)
        logging.info(f"[Bandeja] Consultando Maximo...")
        todas_ots = listar_ots(
            ownergroup=OWNERGROUP,
            classstructureid=CLASSSTRUCTUREID,
        )
        logging.info(f"[Bandeja] OTs traidas de Maximo: {len(todas_ots)}")

        # 4. Filtrar relevantes
        ots_relevantes = [o for o in todas_ots if es_relevante(o, ahora)]
        logging.info(f"[Bandeja] OTs relevantes despues del filtro: {len(ots_relevantes)}")

        # 5. Procesar cada una
        ci_cache = {}
        wonums_procesados = set()
        contadores = {"INSERTED": 0, "UPDATED": 0, "UNCHANGED": 0, "ERROR": 0}
        worklogs_total = 0

        for i, ot in enumerate(ots_relevantes, 1):
            resultado = procesar_ot(ot, ahora, ci_cache, conn)
            wonums_procesados.add(resultado["wonum"])

            if resultado["error"]:
                contadores["ERROR"] += 1
            else:
                contadores[resultado["estado_upsert"]] += 1
                worklogs_total += resultado["worklogs_count"]

            if i % 25 == 0:
                logging.info(f"[Bandeja] Procesadas {i}/{len(ots_relevantes)} OTs...")

        # 6. Detectar salidas: las que estaban en BD pero ya no aparecen
        wonums_que_salieron = wonums_en_bd - wonums_procesados
        cant_salidas = marcar_salidas_bandeja(wonums_que_salieron, conn)
        conn.commit()
        logging.info(f"[Bandeja] OTs marcadas como salidas: {cant_salidas}")

        # 7. Limpiar viejas
        cant_eliminadas = limpiar_viejas_salidas(DIAS_RETENCION_SALIDAS, conn)
        conn.commit()
        if cant_eliminadas > 0:
            logging.info(f"[Bandeja] OTs eliminadas (mas de {DIAS_RETENCION_SALIDAS}d "
                         f"inactivas): {cant_eliminadas}")

        # 8. Estadisticas finales
        duracion = time.time() - inicio
        stats = contar_filas(conn)

        logging.info("="*60)
        logging.info("RESUMEN DE LA EJECUCION")
        logging.info("="*60)
        logging.info(f"  Duracion total:               {duracion:.1f}s")
        logging.info(f"  OTs Maximo (universo):        {len(todas_ots)}")
        logging.info(f"  OTs relevantes (filtradas):   {len(ots_relevantes)}")
        logging.info(f"  Insertadas (nuevas):          {contadores['INSERTED']}")
        logging.info(f"  Actualizadas (con cambios):   {contadores['UPDATED']}")
        logging.info(f"  Sin cambios:                  {contadores['UNCHANGED']}")
        logging.info(f"  Con error:                    {contadores['ERROR']}")
        logging.info(f"  Worklogs cargados:            {worklogs_total}")
        logging.info(f"  Marcadas como salidas:        {cant_salidas}")
        logging.info(f"  Eliminadas (purga):           {cant_eliminadas}")
        logging.info(f"  --- Estado final de tablas ---")
        logging.info(f"  work_orders activas:          {stats['work_orders_activas']}")
        logging.info(f"  work_orders inactivas:        {stats['work_orders_inactivas']}")
        logging.info(f"  worklogs:                     {stats['worklogs']}")
        logging.info(f"  bot_states:                   {stats['bot_states']}")
        logging.info("="*60)

        return True

    except Exception as e:
        logging.exception(f"[Bandeja] Error inesperado: {e}")
        conn.rollback()
        return False

    finally:
        cerrar_conexion(conn)


# ════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════

def main():
    logging.info("="*60)
    logging.info("ETL BANDEJA O_GESFO - INICIO")
    logging.info("="*60)

    exito = sincronizar_bandeja()

    if exito:
        logging.info("ETL completado exitosamente")
        sys.exit(0)
    else:
        logging.error("ETL finalizo con errores")
        sys.exit(1)


if __name__ == "__main__":
    main()
