"""
scheduler.py
------------
Orquestador de ejecucion programada de los ETLs de O_GESFO.

PROGRAMACION:
    - Operativo: cada 10 minutos (excluyendo ventana del completo)
    - Completo:  cada dia a las 3:00 am

VENTANA DE EXCLUSION:
    El operativo NO corre entre 2:50am y 4:00am para no chocar con el completo.

EJECUCION:
    py -m etl.scheduler

DETENER:
    Ctrl+C (parada limpia, espera a que termine la tarea actual)
"""

import logging
import time
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from core.logging_setup import logger
from etl.bandeja_o_gesfo_operativa import sincronizar_bandeja_operativa
from etl.bandeja_o_gesfo_completo import sincronizar_bandeja


# ════════════════════════════════════════════════════════════════════
# WRAPPERS CON MANEJO DE EXCEPCIONES
# ════════════════════════════════════════════════════════════════════

def job_operativa():
    """Wrapper del ETL operativo. Errores los maneja APScheduler."""
    logging.info(">>> [Scheduler] Lanzando ETL OPERATIVO")
    sincronizar_bandeja_operativa()
    logging.info(">>> [Scheduler] ETL OPERATIVO finalizado")


def job_completo():
    """Wrapper del ETL completo. Errores los maneja APScheduler."""
    logging.info(">>> [Scheduler] Lanzando ETL COMPLETO")
    sincronizar_bandeja()
    logging.info(">>> [Scheduler] ETL COMPLETO finalizado")


# ════════════════════════════════════════════════════════════════════
# LISTENERS DE EVENTOS (para logging)
# ════════════════════════════════════════════════════════════════════

def listener(event):
    """Registra eventos de jobs ejecutados o con error."""
    if event.exception:
        logging.error(f"[Scheduler] Job {event.job_id} fallo: {event.exception}")
    else:
        logging.info(f"[Scheduler] Job {event.job_id} OK")


# ════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════

def main():
    logging.info("="*60)
    logging.info("SCHEDULER ETL O_GESFO - INICIO")
    logging.info("="*60)
    logging.info("Tareas programadas:")
    logging.info("  - Operativo: cada 10 min (excepto entre 02:50am y 04:00am)")
    logging.info("  - Completo:  cada dia a las 03:00am")
    logging.info("="*60)

    scheduler = BlockingScheduler()
    scheduler.add_listener(listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    # Tarea 1: Operativo cada 10 minutos, excluyendo hora 3 (3:00 a 3:59)
    scheduler.add_job(
        job_operativa,
        trigger=CronTrigger(minute='*/10', hour='0-2,4-23'),
        id='etl_operativa',
        name='ETL Operativo (cada 10 min)',
        max_instances=1,           # No solapa ejecuciones
        coalesce=True,             # Si pierde un ciclo, no acumula
    )

    # Tarea 2: Completo a las 3:00 am
    scheduler.add_job(
        job_completo,
        trigger=CronTrigger(hour=3, minute=0),
        id='etl_completo',
        name='ETL Completo (3:00 am)',
        max_instances=1,
        coalesce=True,
    )

    # Ejecucion inmediata del operativo al iniciar
    logging.info("[Scheduler] Ejecutando operativo inmediatamente al iniciar...")
    job_operativa()

    # Iniciar el scheduler (bloquea el proceso hasta Ctrl+C)
    try:
        logging.info("[Scheduler] Scheduler iniciado. Presiona Ctrl+C para detener.")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("="*60)
        logging.info("[Scheduler] Detenido por el usuario")
        logging.info("="*60)


if __name__ == "__main__":
    main()