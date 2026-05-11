"""
scheduler.py
------------
Orquestador de ejecucion programada del ETL bandeja_o_gesfo.

PROGRAMACION:
    - bandeja_o_gesfo: cada 10 minutos

DISEÑO:
    Bajo el nuevo modelo (mayo 2026), el ETL es uno solo. La logica
    incremental (diff por changedate) hace que la mayoria de corridas
    procesen pocas OTs (~20-50), tardando 1-2 minutos. La primera
    corrida tras un truncate tarda mas (~30 min) por procesar todo
    el universo, pero solo es esa.

EJECUCION:
    py -m etl.scheduler

DETENER:
    Ctrl+C (parada limpia, espera a que termine la tarea actual)
"""

import logging
import time

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from core.logging_setup import logger
from etl.bandeja_o_gesfo_v1 import sincronizar_bandeja


# ════════════════════════════════════════════════════════════════════
# WRAPPER CON MANEJO DE EXCEPCIONES
# ════════════════════════════════════════════════════════════════════

def job_bandeja():
    """Wrapper del ETL principal. Errores los maneja APScheduler."""
    logging.info(">>> [Scheduler] Lanzando ETL bandeja_o_gesfo")
    sincronizar_bandeja()
    logging.info(">>> [Scheduler] ETL bandeja_o_gesfo finalizado")


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
    logging.info("=" * 60)
    logging.info("SCHEDULER ETL O_GESFO - INICIO")
    logging.info("=" * 60)
    logging.info("Tareas programadas:")
    logging.info("  - bandeja_o_gesfo: cada 10 minutos")
    logging.info("=" * 60)

    scheduler = BlockingScheduler()
    scheduler.add_listener(listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    # Tarea unica: ETL incremental cada 10 minutos
    scheduler.add_job(
        job_bandeja,
        trigger=CronTrigger(minute='*/10'),
        id='etl_bandeja',
        name='ETL bandeja_o_gesfo (cada 10 min)',
        max_instances=1,           # No solapar ejecuciones
        coalesce=True,             # Si pierde un ciclo, no acumula
    )

    # Ejecucion inmediata al iniciar (no esperar al primer cron)
    logging.info("[Scheduler] Ejecutando bandeja_o_gesfo al iniciar...")
    job_bandeja()

    # Iniciar el scheduler (bloquea el proceso hasta Ctrl+C)
    try:
        logging.info("[Scheduler] Scheduler iniciado. Presiona Ctrl+C para detener.")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("=" * 60)
        logging.info("[Scheduler] Detenido por el usuario")
        logging.info("=" * 60)


if __name__ == "__main__":
    main()