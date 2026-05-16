"""
scheduler.py
------------
Orquestador de ejecucion programada del ETL bandeja_o_gesfo.

PROGRAMACION:
    - bandeja_o_gesfo: cada 5 minutos

DISEÑO:
    Bajo el nuevo modelo (mayo 2026), el ETL es uno solo. Tras las
    optimizaciones (paralelizacion de Maximo + execute_values en
    worklogs + commit por batch + cache de sitios con TTL), una
    corrida tipica tarda ~1.8 min para ~550 OTs, lo cual deja margen
    suficiente para correr cada 5 minutos.

    Defensas configuradas:
    - max_instances=1: si una corrida se alarga, la siguiente NO se
      lanza en paralelo (se descarta).
    - coalesce=True: si el scheduler pierde varios slots (p.ej. tras
      pausa del sistema), ejecuta UNO solo, no se acumulan corridas.
    - misfire_grace_time=60: tolera hasta 60s de retraso al arrancar
      un slot antes de descartarlo. Util tras hibernacion/lentitud.

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
from etl.bandeja_o_gesfo import sincronizar_bandeja


# ════════════════════════════════════════════════════════════════════
# CONFIGURACION
# ════════════════════════════════════════════════════════════════════

# Frecuencia del ETL en minutos. Cambiar aqui si en el futuro hay que
# subir o bajar.
INTERVALO_MINUTOS = 5


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
    logging.info(f"  - bandeja_o_gesfo: cada {INTERVALO_MINUTOS} minutos")
    logging.info("=" * 60)

    scheduler = BlockingScheduler()
    scheduler.add_listener(listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    scheduler.add_job(
        job_bandeja,
        trigger=CronTrigger(minute=f'*/{INTERVALO_MINUTOS}'),
        id='etl_bandeja',
        name=f'ETL bandeja_o_gesfo (cada {INTERVALO_MINUTOS} min)',
        max_instances=1,           # No solapar ejecuciones
        coalesce=True,             # Si pierde slots, no acumula
        misfire_grace_time=60,     # Tolera 60s de retraso al arrancar
    )

    # Ejecucion inmediata al iniciar (no esperar al primer cron).
    # Nota: esto bloquea ~1.8 min antes de que el scheduler arranque.
    # Es deseable para que la primera corrida cargue el cache de sitios
    # antes de que el scheduler tome control.
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