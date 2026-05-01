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

# ════════════════════════════════════════════════════════════════════
# UMBRALES DE CLASIFICACION DE OTs (en dias)
# ════════════════════════════════════════════════════════════════════
# Modificar aqui para cambiar las reglas de clasificacion en TODO el proyecto.
# Estos umbrales aplican solo a OTs en estado INPRG.
# COMP siempre es SOLUCIONADO. CLOSE siempre es DOCUMENTADO.

UMBRAL_FRESCA  = 1   # < 1 dia       → FRESCA
UMBRAL_TIBIA   = 7   # 1 a 7 dias    → TIBIA
UMBRAL_ANTIGUA = 14  # 7 a 14 dias   → ANTIGUA
                     # > 14 dias     → MUY_ANTIGUA

# Ventana operativa: hasta cuantos dias atras trae el ETL operativo
DIAS_VENTANA_OPERATIVA = 14

# Retencion de OTs inactivas: cuantos dias se mantienen en BD despues de
# salir de Maximo, antes de eliminarlas fisicamente. Aplicado por el ETL
# completo en limpiar_viejas_salidas().
DIAS_RETENCION_SALIDAS = 5

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
