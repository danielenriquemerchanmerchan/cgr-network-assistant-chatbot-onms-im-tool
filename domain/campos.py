"""
campos.py
---------
Diccionario de definición de campos para OTs de O_GESFO (classstructureid=4213).

QUE ES:
    Define cuales campos componen el OUTPUT del proyecto (Excel, MySQL, Postgres).
    No define que se le pide a Maximo --- eso vive en integrations/maximo/client.py.
    De hecho a maximo se le esta consultando todo. 
    Ejemplo:
    Imagina que recibes un camión con 181 cajas(consulta maximo), pero solo necesitas 32 para tu inventario.
    Las 181 cajas son lo que Maximo te envía en el detalle (el JSON gigante)
    La lista de 32 cajas que necesitas está aquí en este archivo campos.py
    El trabajador (transformer) del muelle solo descarga las 32 cajas que necesitas y deja las otras 149 en el camión 
    

POR QUE EXISTE COMO ARCHIVO APARTE:
    Si los nombres de campos estuvieran dispersos en transformers, exporters y otros
    modulos, agregar/quitar un campo requeriria editar varios archivos. Aqui es UN
    SOLO LUGAR para todas las definiciones del modelo de datos.

QUE CONTIENE:
    - CAMPOS_OT          : campos top-level de la OT (wonum, status, location, etc.)
    - SPEC_CAMPOS        : campos del array workorderspec (TIPO_TRAMO, OPERADOR_FO, etc.)
    - HARDCODEADOS       : specs cuyos valores son predefinidos (se colocan por defecto)
    - WORKLOG_CAMPOS     : campos de cada worklog (avance) plano
    - CAMPOS_FECHA       : campos que se truncan a 19 chars (fechas ISO)
    - CAMPOS_STR_FORZADO : campos que se castean a str aunque vengan numericos
    - ANCHOS_COLUMNAS    : ancho de columna en Excel para cada campo
    - ANCHOS_WORKLOG     : ancho de columna en Excel para cada campo de worklog

COMO INTERACTUA:
    Este archivo campos.py
    - Lo lee transformers/ot.py para saber que campos extraer del JSON de Maximo
    - Lo lee transformers/worklog.py para saber que campos extraer de cada worklog
    - Lo leen los exporters (excel.py, mysql.py postgres.py) para saber que columnas crear

REGLA DE ORO:
    Los nombres en CAMPOS_OT y SPEC_CAMPOS deben coincidir EXACTAMENTE con los
    nombres que Maximo devuelve en el JSON. Si difieren, el transformer no los
    encuentra y quedan vacios.

COMO AGREGAR UN CAMPO NUEVO:
    1. Agregarlo a la lista correspondiente (CAMPOS_OT o SPEC_CAMPOS)
    2. Si es fecha, agregarlo a CAMPOS_FECHA
    3. Si es numerico pero queremos tratarlo como str, agregarlo a CAMPOS_STR_FORZADO
    4. Definir su ancho en ANCHOS_COLUMNAS
    5. (Opcional) Si necesita logica especial de mapeo, ajustar el transformer
"""

# ══════════════════════════════════════════════════════════════
# SPECIFICATIONS (workorderspec)
# ══════════════════════════════════════════════════════════════

SPEC_CAMPOS = [
    "EECC_CUADRILLA_FO", "TIPO_CUADRILLA_FO", "OPERADOR_FO",
    "NUMERO_CASO_FO", "COORDINADOR_RED_FO", "LIDER_DE_ZONA_FO",
    "RESPONSABLE_ZONA_NIVEL3_FO", "PERSONA_QUE_REPORTA", "AREA_QUE_REPORTA_FO",
    "NUMERO_OT_GESTOT", "PROVISIONAL", "IRU_AFECTADO", "OUTAGE_ASOCIADO",
    "TIPO_TRAMO", "TIPO_OPERACION_FO", "DIST_OPTICA", "ORIGEN_MEDIDA",
    "TIPO_CAUSA", "OBSERV_CIERRE", "COORDENADA_CORTE_LONG", "COORDENADA_CORTE_LAT",
    "PARADA_RELOJ", "TIEMPO_EFECT",
]

# Specs cuyos valores son calculados o libres (no se validan contra catalogo)
HARDCODEADOS = {
    "OBSERV_CIERRE", "COORDENADA_CORTE_LONG", "COORDENADA_CORTE_LAT",
    "PARADA_RELOJ", "TIEMPO_EFECT", "IRU_AFECTADO", "NUMERO_OT_GESTOT",
    "NUMERO_CASO_FO",
}


# ══════════════════════════════════════════════════════════════
# CAMPOS PRINCIPALES DE LA OT
# ══════════════════════════════════════════════════════════════

CAMPOS_OT = [
    "wonum", "woclass", "worktype", "classstructureid", "status",
    "description", "description_class",
    "cinum", "ci_description",
    "location", "nom_ubicacion",
    "siteid", "orgid", "ownergroup", "assignedownergroup", "persongroup",
    "reportdate", "schedstart", "actstart", "noweekmonth",
    "lead", "gerencia", "nom_gerencia", "nom_grupo",
    "direccion", "impacto", "cod_pep", "reportedby",
    "app_origen", "failurecode", "phone",
    "cant_worklogs",    # agregada: cantidad de avances (precomputada)
]

# Campos que Maximo devuelve como fecha ISO (se trunca a 19 chars)
CAMPOS_FECHA = {"reportdate", "schedstart", "actstart"}

# Campos que se castean a str aunque vengan numericos
CAMPOS_STR_FORZADO = {"noweekmonth", "impacto", "phone", "cant_worklogs"}


# ══════════════════════════════════════════════════════════════
# ANCHOS DE COLUMNA PARA EXCEL (hoja principal)
# ══════════════════════════════════════════════════════════════

ANCHOS_COLUMNAS = {
    "wonum": 12, "woclass": 12, "worktype": 10, "classstructureid": 16,
    "status": 10, "description": 45, "description_class": 40,
    "cinum": 30, "ci_description": 55,
    "location": 10, "nom_ubicacion": 22,
    "siteid": 8, "orgid": 10, "ownergroup": 12,
    "assignedownergroup": 18, "persongroup": 14,
    "reportdate": 18, "schedstart": 18, "actstart": 18, "noweekmonth": 12,
    "lead": 18, "gerencia": 12, "nom_gerencia": 35, "nom_grupo": 28,
    "direccion": 15, "impacto": 10, "cod_pep": 20,
    "reportedby": 15, "app_origen": 12, "failurecode": 12, "phone": 10,
    "cant_worklogs": 14,
}


# ══════════════════════════════════════════════════════════════
# WORKLOGS (avances de la OT)
# ══════════════════════════════════════════════════════════════

WORKLOG_CAMPOS = [
    "worklog_id",            # PK (viene de worklogid en Maximo)
    "wonum",                 # FK -> CAMPOS_OT.wonum (viene de recordkey)
    "createdate",            # fecha de creacion del avance
    "createby",              # autor que creo el avance
    "modifydate",            # fecha de ultima modificacion
    "modifyby",              # quien modifico por ultima vez
    "logtype",               # codigo: WORK, CLIENTNOTE, etc.
    "logtype_description",   # texto legible: "Client Note", etc.
    "description",           # titulo corto del avance
    "description_long",      # texto completo (description_longdescription)
    "clientviewable",        # 0/1 si el cliente puede verlo
]

# Campos fecha de worklog (truncado a 19 chars)
WORKLOG_CAMPOS_FECHA = {"createdate", "modifydate"}

# Anchos de columna Excel para hoja worklogs
ANCHOS_WORKLOG = {
    "worklog_id":           14,
    "wonum":                12,
    "createdate":           20,
    "createby":             16,
    "modifydate":           20,
    "modifyby":             16,
    "logtype":              14,
    "logtype_description":  18,
    "description":          45,
    "description_long":     80,
    "clientviewable":       12,
}

# ══════════════════════════════════════════════════════════════
# CLASIFICACION OPERATIVA (referencia)
# ══════════════════════════════════════════════════════════════
# La columna `clasificacion_operativa` en onms.work_orders puede tener
# 6 valores. Los umbrales de las categorias INPRG estan en core/config.py.
#
# La logica de clasificacion vive en:
#   - etl/bandeja_o_gesfo_completo.py:clasificar_ot()  (al hacer upsert)
#   - integrations/postgres/client.py:reclasificar_envejecidas()
#       (recalcula al inicio de cada corrida del ETL)
#
# Tabla de equivalencias:
#
#   STATUS  EDAD (dias)        CLASIFICACION
#   ──────────────────────────────────────────
#   INPRG   0 a UMBRAL_FRESCA  → FRESCA
#   INPRG   a UMBRAL_TIBIA     → TIBIA
#   INPRG   a UMBRAL_ANTIGUA   → ANTIGUA
#   INPRG   > UMBRAL_ANTIGUA   → MUY_ANTIGUA
#   COMP    cualquier          → SOLUCIONADO
#   CLOSE   cualquier          → DOCUMENTADO
#
# La clasificacion es DESCRIPTIVA. No controla logica de negocio
# (filtros, salidas, visibilidad). El frontend decide como visualizar
# las OTs segun esta categoria.

CLASIFICACIONES_OPERATIVAS = {
    "INPRG": ["FRESCA", "TIBIA", "ANTIGUA", "MUY_ANTIGUA"],
    "COMP":  ["SOLUCIONADO"],
    "CLOSE": ["DOCUMENTADO"],
}