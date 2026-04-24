"""
campos_gesfo.py
---------------
Definiciones de campos, specifications y metadatos del dominio
OT GESFO (ownergroup=O_GESFO, classstructureid=4213).

Separado de config.py porque son constantes de negocio, no configuracion.
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