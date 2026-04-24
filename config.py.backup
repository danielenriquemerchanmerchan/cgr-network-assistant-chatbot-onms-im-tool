# config.py
# ─── Configuración general del sistema ────────────────────────

# Máximo API

# MAXIMO_URL      = "http://10.80.123.13:8001/maximo/oslc/os/RESTWO"
# LOGOUT_URL      = "http://10.80.123.13:8001/maximo/oslc/logout"
# MAXIMO_USER     = "restusr"
# MAXIMO_PASSWORD = "restusr2023"

# URL base para consultar Configuration Items (CI) en Maximo
MAXIMO_CI_URL = "http://10.80.123.11:8001/maximo/oslc/os/MXCI"
MAXIMO_URL      = "http://10.80.123.11:8001/maximo/oslc/os/RESTWO"
LOGOUT_URL      = "http://10.80.123.11:8001/maximo/oslc/logout"
MAXIMO_USER     = 'CENTROGESTION'
MAXIMO_PASSWORD = 'Centrogestion2025'

MAXIMO_TIMEOUT  = 30
MAXIMO_GRUPO    = "O_GESFO"
MAXIMO_ESTADOS_EXCLUIR = ["COMP", "CLOSE", "CAN", "HIST"]
MAXIMO_WOCLASS = ["WORKORDER", "CHANGE"]

# Telegram
TELEGRAM_TOKEN      = "7359250833:AAFTwh35Tp9-1qIIiGY9AMpq2m2tcai01-k"
ADMIN_CHAT_ID       = 5403703132  # OnCall_Fx

# Monitor de OTs
MONITOR_INTERVALO_MINUTOS  = 5   # cada cuánto revisa OTs nuevas
ACUSE_TIMEOUT_MINUTOS      = 15  # tiempo máximo para confirmar recibo
ALERTA_COORDINADOR_MENSAJE = "OT {wonum} sin confirmar en {departamento} después de {minutos} minutos"

# Base de datos MySQL
DB_HOST     = "192.168.44.114"
DB_PORT     = 3306
DB_NAME     = "bot_gesfo"
DB_USER     = "cgestion"
DB_PASSWORD = "T3l3f0n1c4"

# Base de datos Oracle Maximom
ORACLE_USER = 'CGDASHBOARD'
ORACLE_PSW  = 'CgMovistar19'
ORACLE_DSN  = 'racscanmaximo.nh.inet:1521/MAXIMO'

# Paginacion para extracciones masivas de OTs
MAXIMO_PAGE_SIZE = 200
 
# PostgreSQL (opcional, usado por PostgresExporter)
PG_HOST     = "localhost"
PG_PORT     = 5432
PG_USER     = "postgres"
PG_PASSWORD = ""
PG_DATABASE = "disponibilidad"

# Diccionario Clasificaciones
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