"""
worklog_transformer.py
----------------------
Logica pura de transformacion para worklogs.
Convierte worklogs crudos de Maximo (clave 'worklog' del detalle)
a registros planos listos para exportar.

Sin dependencias de red, BD ni de formato de salida.
"""

from campos_gesfo import WORKLOG_CAMPOS, WORKLOG_CAMPOS_FECHA


def construir_registros_worklog(wonum, worklogs_crudos):
    """
    Convierte la lista de worklogs de una OT a registros planos.

    Parametros:
        wonum           (str): wonum de la OT padre
        worklogs_crudos (list[dict]): lista de worklogs crudos de Maximo

    Retorna:
        list[dict] con exactamente WORKLOG_CAMPOS como claves cada uno.
    """
    registros = []

    for wl in worklogs_crudos:
        registro = {
            "worklog_id":          str(wl.get("worklogid") or ""),
            "wonum":               wl.get("recordkey") or wonum,
            "createdate":          (wl.get("createdate") or "")[:19],
            "createby":            wl.get("createby") or "",
            "modifydate":          (wl.get("modifydate") or "")[:19],
            "modifyby":            wl.get("modifyby") or "",
            "logtype":             wl.get("logtype") or "",
            "logtype_description": wl.get("logtype_description") or "",
            "description":         wl.get("description") or "",
            "description_long":    wl.get("description_longdescription") or "",
            "clientviewable":      "1" if wl.get("clientviewable") else "0",
        }
        registros.append(registro)

    return registros