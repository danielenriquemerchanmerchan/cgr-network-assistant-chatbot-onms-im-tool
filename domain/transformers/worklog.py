def construir_registros_worklog(wonum, worklogs_crudos):
    """
    Convierte la lista de worklogs de una OT a registros planos.

    Parametros:
        wonum           (str): wonum de la OT padre
        worklogs_crudos (list[dict]): lista de worklogs crudos de Maximo

    Retorna:
        list[dict] con exactamente WORKLOG_CAMPOS como claves cada uno.

    Notas:
        - Las fechas vacias se devuelven como None (no como string vacio),
          para evitar errores en BDs que tipan TIMESTAMP estrictamente.
        - clientviewable se devuelve como bool nativo de Python.
    """
    registros = []

    for wl in worklogs_crudos:
        registro = {
            "worklog_id":          str(wl.get("worklogid") or ""),
            "wonum":               wl.get("recordkey") or wonum,
            "createdate":          _fecha_o_none(wl.get("createdate")),
            "createby":            wl.get("createby") or "",
            "modifydate":          _fecha_o_none(wl.get("modifydate")),
            "modifyby":            wl.get("modifyby") or "",
            "logtype":             wl.get("logtype") or "",
            "logtype_description": wl.get("logtype_description") or "",
            "description":         wl.get("description") or "",
            "description_long":    wl.get("description_longdescription") or "",
            "clientviewable":      bool(wl.get("clientviewable")),
        }
        registros.append(registro)

    return registros


def _fecha_o_none(valor):
    """
    Convierte una fecha de Maximo a string truncado a 19 chars,
    o None si esta vacia o ausente.
    """
    if not valor:
        return None
    return valor[:19]