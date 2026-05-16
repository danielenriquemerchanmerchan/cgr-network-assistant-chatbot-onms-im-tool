"""
config.py
---------
Configuracion centralizada del proyecto.

Secretos (credenciales, URLs con IPs) vienen de .env
Constantes de negocio (clasificaciones, estados, timeouts) estan aqui como codigo.

Regla: un solo punto de entrada para leer .env. Los demas modulos
       consultan las constantes definidas aca, no leen .env directamente.
"""

import os
from dotenv import load_dotenv

# Cargar variables de .env al arrancar
load_dotenv()


# ══════════════════════════════════════════════════════════════
# MAXIMO API
# ══════════════════════════════════════════════════════════════

# URL base: todo Maximo cuelga de aqui
MAXIMO_BASE_URL = os.getenv("MAXIMO_BASE_URL")

# URLs derivadas (construidas a partir de base)
MAXIMO_URL      = f"{MAXIMO_BASE_URL}/RESTWO"
LOGOUT_URL      = MAXIMO_BASE_URL.replace("/oslc/os", "/oslc/logout")

# URL para Configuration Items (viene del .env por si apunta a otro host)
MAXIMO_CI_URL   = os.getenv("MAXIMO_CI_URL")

# Credenciales
MAXIMO_USER     = os.getenv("MAXIMO_USER")
MAXIMO_PASSWORD = os.getenv("MAXIMO_PASSWORD")

# Timeout en segundos
MAXIMO_TIMEOUT  = int(os.getenv("MAXIMO_TIMEOUT", "30"))

# Paginacion para extracciones masivas
MAXIMO_PAGE_SIZE = 200


# ══════════════════════════════════════════════════════════════
# MAXIMO ORACLE DB
# ══════════════════════════════════════════════════════════════


ORACLE_USER = os.getenv("ORACLE_USER")
ORACLE_PSW  = os.getenv("ORACLE_PSW")
ORACLE_DSN  = os.getenv("ORACLE_DSN")


# ══════════════════════════════════════════════════════════════
# MYSQL (bot_gesfo)
# ══════════════════════════════════════════════════════════════

DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "3306"))
DB_NAME     = os.getenv("DB_NAME", "bot_gesfo")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


# ══════════════════════════════════════════════════════════════
# POSTGRESQL (BD del dashboard ONMS, schema 'onms')
# ══════════════════════════════════════════════════════════════

PG_HOST     = os.getenv("PG_HOST", "localhost")
PG_PORT     = int(os.getenv("PG_PORT", "5432"))
PG_USER     = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")
PG_DATABASE = os.getenv("PG_DATABASE", "disponibilidad")


# ══════════════════════════════════════════════════════════════
# TELEGRAM BOT
# ══════════════════════════════════════════════════════════════

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_CHAT_ID  = int(os.getenv("ADMIN_CHAT_ID", "0"))


# ══════════════════════════════════════════════════════════════
# TELEGRAM — CONSTANTES DE NEGOCIO
# ══════════════════════════════════════════════════════════════

# Monitor de OTs
MONITOR_INTERVALO_MINUTOS  = 5   # cada cuanto revisa OTs nuevas
ACUSE_TIMEOUT_MINUTOS      = 15  # tiempo maximo para confirmar recibo
ALERTA_COORDINADOR_MENSAJE = (
    "OT {wonum} sin confirmar en {departamento} después de {minutos} minutos"
)


# ══════════════════════════════════════════════════════════════
# PTM
# ══════════════════════════════════════════════════════════════

PTM_USER     = os.getenv("PTM_USER")
PTM_PASSWORD = os.getenv("PTM_PASSWORD")


# ══════════════════════════════════════════════════════════════
# ROSE
# ══════════════════════════════════════════════════════════════

ROSE_USER     = os.getenv("ROSE_USER")
ROSE_PASSWORD = os.getenv("ROSE_PASSWORD")

# ═══════════════════════════════════════════════════════════════════
# ETL DE BANDEJA O_GESFO
# ═══════════════════════════════════════════════════════════════════
# Configuracion del ETL unico que sincroniza work_orders con Maximo.
# Logica: trae todas las OTs (cualquier status) cuyo changedate este
# dentro de la ventana definida abajo. Las que quedan fuera de ventana
# se eliminan fisicamente de la BD.

DIAS_INPRG_RECIENTES   = 90  # Solo INPRG creadas en los ultimos N dias
DIAS_RETENCION_CERRADAS = 14  # COMP/CLOSE/CAN se borran luego de N dias
PROCESAR_CERRADAS = False


# ═══════════════════════════════════════════════════════════════════
# ASIGNADOR AUTOMATICO DE OTs A ot_bandeja
# ═══════════════════════════════════════════════════════════════════
# Cada vez que el ETL termina de sincronizar work_orders, llama al
# asignador para que las INPRG nuevas (sin presencia en ot_bandeja)
# entren a la bandeja con un coordinador asignado segun coordinador_zona.

# Filtro opcional por patron de cinum. None = no filtra (todas las INPRG
# entran). Para limitar a un subset (ej. solo RBHFO en el futuro), poner
# 'RBHFO%' u otro patron LIKE valido en Postgres.
ASIGNADOR_FILTRO_CINUM_LIKE = None


# ═══════════════════════════════════════════════════════════════════
# NOTIFICADOR DE OTs NUEVAS A COORDINADOR (job del bot)
# ═══════════════════════════════════════════════════════════════════
# Job que vive en el bot (no en el ETL). Cada N segundos lee ot_bandeja
# y notifica via Telegram al coord asignado de cada OT que aun no se
# ha notificado (notificacion_coordinador_enviada_at IS NULL).

# Cada cuantos segundos corre el job del notificador.
NOTIFICADOR_INTERVALO_SEG = 60

# Cuantas OTs procesa por ciclo. En piloto, valor BAJO para no inundar
# el chat del coord. Subir cuando este validado el flujo end-to-end.
NOTIFICADOR_MAX_POR_CICLO = 5

# Maximo de reintentos si Telegram falla. Despues de N intentos la OT
# se da por "no notificable" y deja de aparecer en el SELECT del job.
# Esto evita que una OT con problema permanente quede en loop eterno.
NOTIFICADOR_MAX_INTENTOS = 3

# Pausa (segundos) entre mensajes consecutivos para no saturar Telegram.
NOTIFICADOR_PAUSA_ENTRE_MSG_SEG = 0.5

# ═══════════════════════════════════════════════════════════════════
# ETL DE CONTROL DE INPRG
# ═══════════════════════════════════════════════════════════════════
# Umbrales para clasificacion de OTs INPRG en el reporte de control
# (etl/control_inprg.py). Estos umbrales aplican solo al reporte de
# consola, NO controlan visibilidad ni logica del ETL principal.

UMBRAL_INPRG_RECIENTE   = 14  # < 14 dias  → recientes (lo deseable)
UMBRAL_INPRG_VIEJA      = 60  # 14-60 dias → viejas (rango aceptable)
                              # > 60 dias  → ultraviejas (alarma)

# ══════════════════════════════════════════════════════════════
# CONSTANTES HISTORICAS — revisar si todavia aplican
# ══════════════════════════════════════════════════════════════
# Estas variables estaban en config.py pero al 2026-04-23 no se
# usan en ningun archivo del proyecto. Se dejan comentadas por
# si son referencia para futuro. Eliminar si en 6 meses siguen
# sin usarse.

# ══════════════════════════════════════════════════════════════
# MAXIMO — CONSTANTES DE NEGOCIO (no son secretos)
# ══════════════════════════════════════════════════════════════

MAXIMO_GRUPO           = "O_GESFO"
MAXIMO_ESTADOS_EXCLUIR = ["COMP", "CLOSE", "CAN", "HIST"]
MAXIMO_WOCLASS         = ["WORKORDER", "CHANGE"]

# Diccionario de clasificaciones de OT
MAXIMO_CLASIFICACIONES = {
    "CORRECTIVO": {
        "classstructureid": "4213",
        "description_class": "RED DE ACCESO FO OYM \\ MTTO. CORRECTIVO \\ INCIDENCIA",
        "worktype": "MC"
    },
    "PREVENTIVO": {
        "classstructureid": "4215",
        "description_class": "RED DE ACCESO FO OYM \\ MTTO. PREVENTIVO \\ ASEGURAMIENTO",
        "worktype": "MP"
    },
    "GENERICA": {
        "classstructureid": "1885",
        "description_class": "FALLAS \\ GENERICA",
        "worktype": "EM"
    },
    "PERFORMANCE": {
        "classstructureid": "1886",
        "description_class": "FALLAS \\ PERFORMANCE",
        "worktype": "EM"
    },
    "OUTAGE": {
        "classstructureid": "1887",
        "description_class": "FALLAS \\ OUTAGE",
        "worktype": "EM"
    },
}

# ────────────────────────────────────────────────────────────────────
# MECANISMO A+B: OT activa por cuadrilla + pin en Telegram
# ────────────────────────────────────────────────────────────────────
#
# Si True, el bot pinea un mensaje en el grupo de la cuadrilla con los
# datos de la OT activa cada vez que cambia. Despinea el anterior si lo
# habia. Si False, el bot solo gestiona ot_activa_id en BD sin tocar pins
# (util si en algun grupo el bot pierde permisos de pin o si se quiere
# desactivar temporalmente sin redeploy).
#
# Requisito si esta en True: el bot debe ser admin con permiso
# "Pin messages" en cada grupo de cuadrilla.
PINEAR_OT_ACTIVA = True