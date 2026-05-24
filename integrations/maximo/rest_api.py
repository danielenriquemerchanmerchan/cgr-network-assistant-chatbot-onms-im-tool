"""
maximo_wo.py
------------
Modulo de operaciones con la API REST de Maximo.
Soporta OTs tipo WORKORDER, ACTIVITY y CHANGE.

Funciones:
    consultar_ot(wonum)                          -> dict | None
    crear_ot(datos)                              -> dict
    insertar_avance(wonum, texto, autor)          -> dict
    cambiar_estado(wonum, nuevo_estado)           -> dict
    adjuntar_archivo(wonum, nombre, contenido_b64) -> dict
    adjuntar_url(wonum, url, descripcion)         -> dict

Cierre de sesion:
    Cada llamada REST abre una sesion. La funcion _cerrar_sesion()
    la cierra automaticamente usando las cookies del response
    (LtpaToken2 + JSESSIONID) segun el manual Rest_Cierre_Sesion_v1.
"""

import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
import logging

from core.config import MAXIMO_CI_URL as URL_CI
from core.config import MAXIMO_PAGE_SIZE as PAGE_SIZE_DEFAULT
from core.config import MAXIMO_URL as URL_BASE
from core.config import MAXIMO_USER as USERNAME
from core.config import MAXIMO_PASSWORD as PASSWORD
from core.config import MAXIMO_TIMEOUT as TIMEOUT
from core.config import LOGOUT_URL

from core.config import MAXIMO_INCIDENT_URL
from core.config import MAXIMO_INCIDENT_USER
from core.config import MAXIMO_INCIDENT_PASSWORD
from core.config import MAXIMO_INCIDENT_SITEID

# URL base para adjuntos (objeto restwoadj)
ADJ_BASE = URL_BASE.replace("/RESTWO", "/restwoadj")


# ══════════════════════════════════════════════════════════════
# FUNCION INTERNA: cierre de sesion
# ══════════════════════════════════════════════════════════════

def _cerrar_sesion(response):
    """
    Cierra la sesion REST de Maximo usando las cookies del response.
    Segun manual Rest_Cierre_Sesion_v1: GET /maximo/oslc/logout
    con header Cookie: LtpaToken2=...;JSESSIONID=...
    """
    try:
        cookies = response.cookies.get_dict()
        if not cookies:
            return
        cookie_header = "; ".join([f"{k}={v}" for k, v in cookies.items()])
        requests.get(
            LOGOUT_URL,
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            headers={"Cookie": cookie_header},
            timeout=TIMEOUT
        )
        logging.debug("Sesion Maximo cerrada")
    except Exception as e:
        logging.warning(f"Error cerrando sesion Maximo: {e}")


def _obtener_href(wonum):
    """
    Obtiene el href de una OT a partir de su wonum.
    Funcion auxiliar usada por varias operaciones.
    Retorna (href, response) o (None, response) si no se encontro.
    """
    r = requests.get(
        f"{URL_BASE}?lean=1&oslc.where=wonum=\"{wonum}\""
        f"&oslc.select=wonum,href",
        auth=HTTPBasicAuth(USERNAME, PASSWORD),
        timeout=TIMEOUT
    )
    if r.status_code != 200:
        logging.error(f"Error HTTP {r.status_code} al obtener href de OT {wonum}")
        return None, r
    members = r.json().get("rdfs:member") or r.json().get("member")
    if not members:
        logging.warning(f"OT {wonum} no encontrada")
        return None, r
    href = members[0].get("href", "")
    return href or None, r

# ══════════════════════════════════════════════════════════════
# 1. CONSULTAR OT ESPECIFICA
# ══════════════════════════════════════════════════════════════

def consultar_ot(wonum):
    """
    Consulta una OT y retorna TODOS sus campos disponibles.

    Flujo:
        Paso 1: wonum -> href   (GET coleccion RESTWO)
        Paso 2: href  -> datos  (GET directo oslc.select=*)

    Retorna dict con campos normalizados + raw, o None si no existe.
    """
    r1 = None
    r2 = None
    try:
        r1 = requests.get(
            f"{URL_BASE}?lean=1&oslc.where=wonum=\"{wonum}\""
            f"&oslc.select=wonum,href,worklog_collectionref,status,status_description",
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            timeout=TIMEOUT
        )
        if r1.status_code != 200:
            logging.error(f"Error HTTP {r1.status_code} al consultar OT {wonum}")
            return None

        members = r1.json().get("rdfs:member") or r1.json().get("member")
        if not members:
            logging.warning(f"OT {wonum} no encontrada")
            return None

        href = members[0].get("href", "")
        if not href:
            logging.error(f"Sin href para OT {wonum}")
            return None

        r2 = requests.get(
            href,
            params={"lean": "1", "oslc.select": "*"},
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            timeout=TIMEOUT
        )
        if r2.status_code != 200:
            logging.error(f"Error HTTP {r2.status_code} al consultar href de OT {wonum}")
            return None

        data = r2.json()

        resumen = data.get("description_parent") or data.get("description") or ""
        if resumen and len(resumen) > 9:
            resumen = resumen[:-9]

        elementos_red = [
            {"cinum": i.get("cinum", ""), "location": i.get("location", "")}
            for i in data.get("multiassetlocci", [])
        ]

        return {
            "wonum":                 data.get("wonum"),
            "woclass":               data.get("woclass"),
            "worktype":              data.get("worktype"),
            "status":                data.get("status"),
            "status_description":    data.get("status_description"),
            "resumen":               resumen,
            "descripcion":           data.get("description", ""),
            "schedstart":            data.get("schedstart"),
            "schedfinish":           data.get("schedfinish"),
            "actstart":              data.get("actstart"),
            "actfinish":             data.get("actfinish"),
            "estdur":                data.get("estdur", 0),
            "ownergroup":            data.get("ownergroup"),
            "cinum":                 data.get("cinum"),
            "worklog_collectionref": data.get("worklog_collectionref", ""),
            "elementos_red":         elementos_red,
            "raw":                   data,
        }

    except Exception as e:
        logging.error(f"Error consultando OT {wonum}: {e}")
        return None

    finally:
        if r1 is not None:
            _cerrar_sesion(r1)
        if r2 is not None:
            _cerrar_sesion(r2)

# ══════════════════════════════════════════════════════════════
# 2. CREAR OT
# ══════════════════════════════════════════════════════════════

def crear_ot(datos):
    """
    Crea una nueva Orden de Trabajo en Maximo.

    Parametros:
        datos (dict): campos de la OT a crear. Campos minimos requeridos:
            woclass         -> tipo de OT: 'WORKORDER', 'CHANGE', etc.
            description     -> descripcion/titulo de la OT
            cinum           -> articulo de configuracion
            worktype        -> tipo de trabajo: 'EM', 'MC', 'POM', etc.
            status          -> estado inicial: 'WAPPR', 'INPRG', etc.
            ownergroup      -> grupo responsable: 'O_GESFO', etc.

        Campos opcionales:
            worklog         -> lista de notas iniciales
            multiassetlocci -> lista de elementos de red afectados
            classstructureid -> ID de clasificacion
            jpnum           -> plan de trabajo

    Retorna:
        dict con: success, message, status, ot (wonum creado), href
    """
    r = None
    try:
        payload = {
            "orgid":  "MOVISTAR",
            "siteid": "REDES",
            **datos
        }

        r = requests.post(
            f"{URL_BASE}?lean=1",
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            headers={
                "Content-Type": "application/json",
                "properties":   "wonum,description,status"
            },
            json=payload,
            timeout=TIMEOUT
        )

        if r.status_code in (200, 201):
            data      = r.json()
            wonum     = data.get("wonum", "")
            href      = r.headers.get("Location", "")
            logging.info(f"OT creada: {wonum}")
            return {
                "success": True,
                "message": f"OT {wonum} creada correctamente",
                "status":  "success",
                "ot":      wonum,
                "href":    href,
            }
        else:
            logging.error(f"Error creando OT HTTP {r.status_code}: {r.text}")
            return {"success": False, "message": f"Error HTTP {r.status_code}", "status": "http_error", "ot": None}

    except Exception as e:
        logging.error(f"Error creando OT: {e}")
        return {"success": False, "message": str(e), "status": "exception", "ot": None}

    finally:
        if r is not None:
            _cerrar_sesion(r)

# ══════════════════════════════════════════════════════════════
# 3. ACTUALIZAR OT
# ══════════════════════════════════════════════════════════════

def actualizar_ot(href, datos):
    """
    Actualiza campos de una OT existente via PATCH al href.
    Usado principalmente para agregar workorderspec despues de crear la OT.

    Parametros:
        href  (str): href completo de la OT retornado por crear_ot()
        datos (dict): campos a actualizar con prefijo spi:

    Retorna dict: {success, message, status, ot}

    Ejemplo de uso para agregar workorderspec:
        mx.actualizar_ot(href, {
            "spi:workorderspec": [
                {"spi:assetattrid": "EECC_CUADRILLA_FO", "spi:alnvalue": "Optecom",
                 "spi:classstructureid": "4213"},
                {"spi:assetattrid": "TIPO_CAUSA", "spi:alnvalue": "Falla en la Red FiOp",
                 "spi:classstructureid": "4213"},
            ]
        })
    """
    r = None
    try:
        r = requests.post(
            href,
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            headers={
                "x-method-override": "PATCH",
                "patchtype":         "MERGE",
                "properties":        "*",
                "Content-Type":      "application/json"
            },
            json=datos,
            timeout=TIMEOUT
        )

        if r.status_code == 200:
            logging.info(f"OT actualizada correctamente: {href}")
            return {"success": True, "message": "OT actualizada correctamente", "status": "success", "ot": href}
        else:
            logging.error(f"Error actualizando OT HTTP {r.status_code}: {r.text}")
            return {"success": False, "message": f"Error HTTP {r.status_code}", "status": "patch_error", "ot": href}

    except Exception as e:
        logging.error(f"Error actualizando OT {href}: {e}")
        return {"success": False, "message": str(e), "status": "exception", "ot": href}

    finally:
        if r is not None:
            _cerrar_sesion(r)

# ══════════════════════════════════════════════════════════════
# 4. INSERTAR AVANCE
# ══════════════════════════════════════════════════════════════

def insertar_avance(wonum, texto, autor="BOT"):
    """
    Inserta un avance en el worklog de la OT via PATCH con worklog embebido.

    Parametros:
        wonum  (str): numero de OT
        texto  (str): texto del avance
        autor  (str): nombre del autor (default 'BOT')

    Retorna dict: {success, message, status, ot}
    """
    r1 = None
    r2 = None
    try:
        r1 = requests.get(
            f"{URL_BASE}?lean=1&oslc.where=wonum=\"{wonum}\""
            f"&oslc.select=wonum,description,woclass,worklog,status,status_description",
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            timeout=TIMEOUT
        )
        if r1.status_code != 200:
            return {"success": False, "message": f"Error HTTP {r1.status_code}", "status": "http_error", "ot": wonum}

        members = r1.json().get("rdfs:member") or r1.json().get("member")
        if not members:
            return {"success": False, "message": "OT no encontrada", "status": "not_found", "ot": wonum}

        href_completo = members[0].get("worklog_collectionref")
        if not href_completo:
            return {"success": False, "message": "Sin worklog_collectionref", "status": "no_worklog_ref", "ot": wonum}

        href_base = href_completo.replace("/worklog1", "").rstrip("/")

        r2 = requests.post(
            href_base,
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            headers={
                "x-method-override": "PATCH",
                "patchtype":         "MERGE",
                "properties":        "wonum,status",
                "Content-Type":      "application/json"
            },
            json={
                "spi:worklog": [{
                    "spi:description":                 f"Avance_{autor}",
                    "spi:modifyby":                    autor,
                    "spi:description_longdescription": texto,
                }]
            },
            timeout=TIMEOUT
        )

        if r2.status_code == 200:
            logging.info(f"Avance insertado en OT {wonum}")
            return {"success": True, "message": "Avance insertado correctamente", "status": "success", "ot": wonum}
        else:
            logging.error(f"Error PATCH {r2.status_code}: {r2.text[:200]}")
            return {"success": False, "message": f"Error HTTP {r2.status_code}", "status": "patch_error", "ot": wonum}

    except Exception as e:
        logging.error(f"Error insertando avance en OT {wonum}: {e}")
        return {"success": False, "message": str(e), "status": "exception", "ot": wonum}

    finally:
        if r1 is not None:
            _cerrar_sesion(r1)
        if r2 is not None:
            _cerrar_sesion(r2)

# ══════════════════════════════════════════════════════════════
# 5. CAMBIAR ESTADO
# ══════════════════════════════════════════════════════════════

def cambiar_estado(wonum, nuevo_estado):
    """
    Cambia el estado de una OT en Maximo.

    Estados comunes:
        WAPPR -> En espera de aprobacion
        APPR  -> Aprobada
        INPRG -> En progreso
        COMP  -> Completada
        CLOSE -> Cerrada
        CAN   -> Cancelada

    Retorna dict: {success, message, status, ot}
    """
    r1 = None
    r2 = None
    try:
        href, r1 = _obtener_href(wonum)
        if not href:
            return {"success": False, "message": "OT no encontrada o sin href", "status": "not_found", "ot": wonum}

        fecha = datetime.now().strftime("%Y-%m-%dT%H:%M:%S-05:00")
        r2 = requests.post(
            href,
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            headers={
                "x-method-override": "PATCH",
                "patchtype":         "MERGE",
                "properties":        "wonum,status",
                "Content-Type":      "application/json"
            },
            json={
                "spi:status":     nuevo_estado,
                "spi:statusdate": fecha,
            },
            timeout=TIMEOUT
        )

        if r2.status_code == 200:
            logging.info(f"Estado de OT {wonum} cambiado a {nuevo_estado}")
            return {"success": True, "message": f"Estado cambiado a {nuevo_estado}", "status": "success", "ot": wonum}
        else:
            logging.error(f"Error PATCH estado {r2.status_code}: {r2.text[:200]}")
            return {"success": False, "message": f"Error HTTP {r2.status_code}", "status": "patch_error", "ot": wonum}

    except Exception as e:
        logging.error(f"Error cambiando estado de OT {wonum}: {e}")
        return {"success": False, "message": str(e), "status": "exception", "ot": wonum}

    finally:
        if r1 is not None:
            _cerrar_sesion(r1)
        if r2 is not None:
            _cerrar_sesion(r2)

# ══════════════════════════════════════════════════════════════
# 5. ADJUNTAR ARCHIVO
# ══════════════════════════════════════════════════════════════

def adjuntar_archivo(wonum, nombre_archivo, contenido_b64):
    """
    Adjunta un archivo a una OT en Maximo.
    El archivo debe enviarse codificado en Base64.

    Segun manual: POST a /maximo/oslc/os/restwoadj/{href_id}
    con PATCH MERGE y el archivo en spi:documentdata (Base64).

    Parametros:
        wonum          (str): numero de OT
        nombre_archivo (str): nombre del archivo con extension. Ej: 'foto.jpg'
        contenido_b64  (str): contenido del archivo codificado en Base64

    Retorna dict: {success, message, status, ot}

    Ejemplo de uso:
        import base64
        with open('foto.jpg', 'rb') as f:
            b64 = base64.b64encode(f.read()).decode('utf-8')
        resultado = adjuntar_archivo('10563196', 'foto.jpg', b64)
    """
    r1 = None
    r2 = None
    try:
        # Obtener href del objeto restwoadj para esta OT
        href_ot, r1 = _obtener_href(wonum)
        if not href_ot:
            return {"success": False, "message": "OT no encontrada", "status": "not_found", "ot": wonum}

        # Construir href del objeto restwoadj reemplazando restwo por restwoadj
        href_adj = href_ot.replace("/os/restwo/", "/os/restwoadj/")

        r2 = requests.post(
            href_adj,
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            headers={
                "x-method-override": "PATCH",
                "patchtype":         "MERGE",
                "properties":        "*",
                "Content-Type":      "application/json"
            },
            json={
                "spi:doclinks": [{
                    "spi:urltype":      "FILE",
                    "spi:doctype":      "Attachments",
                    "spi:urlname":      nombre_archivo,
                    "spi:documentdata": contenido_b64,
                    "spi:document":     nombre_archivo,
                }]
            },
            timeout=TIMEOUT
        )

        if r2.status_code in (200, 201):
            logging.info(f"Archivo '{nombre_archivo}' adjuntado a OT {wonum}")
            return {"success": True, "message": f"Archivo adjuntado correctamente", "status": "success", "ot": wonum}
        else:
            logging.error(f"Error adjuntando archivo HTTP {r2.status_code}: {r2.text[:200]}")
            return {"success": False, "message": f"Error HTTP {r2.status_code}", "status": "patch_error", "ot": wonum}

    except Exception as e:
        logging.error(f"Error adjuntando archivo a OT {wonum}: {e}")
        return {"success": False, "message": str(e), "status": "exception", "ot": wonum}

    finally:
        if r1 is not None:
            _cerrar_sesion(r1)
        if r2 is not None:
            _cerrar_sesion(r2)

# ══════════════════════════════════════════════════════════════
# 6. ADJUNTAR URL
# ══════════════════════════════════════════════════════════════

def adjuntar_url(wonum, url, descripcion=""):
    """
    Adjunta un enlace URL a una OT en Maximo.

    Segun manual: mismo endpoint que adjuntar_archivo pero con
    spi:urltype='URL' y sin spi:documentdata.

    Parametros:
        wonum       (str): numero de OT
        url         (str): URL a adjuntar. Ej: 'https://maps.google.com/...'
        descripcion (str): descripcion del enlace (opcional)

    Retorna dict: {success, message, status, ot}

    Ejemplo de uso:
        resultado = adjuntar_url('10563196',
                                 'https://maps.google.com/?q=4.44,75.24',
                                 'Ubicacion de la falla')
    """
    r1 = None
    r2 = None
    try:
        href_ot, r1 = _obtener_href(wonum)
        if not href_ot:
            return {"success": False, "message": "OT no encontrada", "status": "not_found", "ot": wonum}

        href_adj = href_ot.replace("/os/restwo/", "/os/restwoadj/")

        r2 = requests.post(
            href_adj,
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            headers={
                "x-method-override": "PATCH",
                "patchtype":         "MERGE",
                "properties":        "*",
                "Content-Type":      "application/json"
            },
            json={
                "spi:doclinks": [{
                    "spi:urltype":  "URL",
                    "spi:doctype":  "Attachments",
                    "spi:urlname":  url,
                    "spi:document": descripcion or url,
                }]
            },
            timeout=TIMEOUT
        )

        if r2.status_code in (200, 201):
            logging.info(f"URL adjuntada a OT {wonum}: {url}")
            return {"success": True, "message": "URL adjuntada correctamente", "status": "success", "ot": wonum}
        else:
            logging.error(f"Error adjuntando URL HTTP {r2.status_code}: {r2.text[:200]}")
            return {"success": False, "message": f"Error HTTP {r2.status_code}", "status": "patch_error", "ot": wonum}

    except Exception as e:
        logging.error(f"Error adjuntando URL a OT {wonum}: {e}")
        return {"success": False, "message": str(e), "status": "exception", "ot": wonum}

    finally:
        if r1 is not None:
            _cerrar_sesion(r1)
        if r2 is not None:
            _cerrar_sesion(r2)
            
            

            
            
# ══════════════════════════════════════════════════════════════
# 7. EXTRACCION EN LOTE: LISTAR OTs (PAGINACION COMPLETA)
# ══════════════════════════════════════════════════════════════
#
# PROPOSITO:
#     Trae el LISTADO de OTs que cumplen los filtros (ownergroup +
#     classstructureid). NO trae los detalles completos --- solo los
#     campos basicos (wonum, href, cinum, status, ...) que vienen en
#     la coleccion paginada.
#
# PAGINACION:
#     Maximo limita la respuesta a N OTs por pagina (page_size).
#     Esta funcion ITERA TODAS LAS PAGINAS hasta agotar el listado
#     (o hasta alcanzar max_members si se le pasa).
#
# CIERRE DE SESION:
#     Cada pagina hace UN request HTTP y cierra inmediatamente la
#     sesion (importante: no acumular sesiones huerfanas en Maximo).
#
# QUE TRAE:
#     Lista de dicts. Cada dict es una "member" con campos basicos:
#     {wonum, href, cinum, worktype, classstructureid, status,
#      description, location, nom_ubicacion, reportdate}
#
# COMO SE USA:
#     members = listar_ots("O_GESFO", "4213")
#     # despues, para cada member, llamar a obtener_detalle_ot(member["href"])
#
# PARAMETRO max_members:
#     Util para pruebas rapidas. Si se pasa max_members=20, corta
#     la paginacion al alcanzar 20 OTs en lugar de traer todas.

def listar_ots(
    ownergroup,
    classstructureid,
    page_size=PAGE_SIZE_DEFAULT,
    select=None,
    max_members=None,
    fecha_desde=None,
    fecha_hasta=None,
    changedate_desde=None,
    worktype=None,
    status_in=None,
    statusdate_desde=None,
):
    """
    Extrae TODAS las OTs que matchean los filtros, iterando todas las paginas.
    Usada por tabla_maestra_4213.py y otros ETL masivos.

    Cada pagina hace un request HTTP y cierra la sesion inmediatamente
    (importante para no dejar sesiones huerfanas en Maximo).

    Parametros:
        ownergroup       (str): grupo propietario. Ej: 'O_GESFO'
        classstructureid (str): ID de clasificacion. Ej: '4213'
        page_size        (int): tamanyo de pagina (default config)
        select           (str): campos a traer en la coleccion (default minimo)
        max_members      (int, opcional): limite de OTs a retornar. Si se alcanza,
                         detiene la paginacion. Util para pruebas rapidas.
                         None (default) = traer todas las paginas.
        fecha_desde      (datetime, opcional): filtra reportdate >= fecha_desde.
                         Util para historico de un periodo o para operativo (frescas).
        fecha_hasta      (datetime, opcional): filtra reportdate < fecha_hasta.
                         Combina con fecha_desde para obtener un rango cerrado.
        changedate_desde (datetime, opcional): filtra changedate >= fecha.
                         Util para detectar OTs con movimiento reciente.
        worktype         (str, opcional): filtra por worktype. Ej: 'MC', 'MP'.
        status_in        (list, opcional): filtra por status. Ej: ['INPRG','COMP'].

    Retorna:
        list[dict] con los members de todas las paginas, o [] si hay error.
    """
    if select is None:
        select = (
            "wonum,href,cinum,worktype,classstructureid,status,"
            "description,location,nom_ubicacion,reportdate,"
            "changedate,actfinish"
        )
    all_members = []
    pagina = 1

    while True:
        r = None
        try:
            # Construir el oslc.where dinamicamente segun los filtros recibidos
            where_partes = [
                f'ownergroup="{ownergroup}"',
                f'classstructureid="{classstructureid}"',
            ]
            if worktype:
                where_partes.append(f'worktype="{worktype}"')
            if status_in:
                valores = ",".join(f'"{s}"' for s in status_in)
                where_partes.append(f'status in [{valores}]')
            if fecha_desde:
                fecha_iso = fecha_desde.strftime("%Y-%m-%dT%H:%M:%S-05:00")
                where_partes.append(f'reportdate>="{fecha_iso}"')
            if fecha_hasta:
                fecha_iso = fecha_hasta.strftime("%Y-%m-%dT%H:%M:%S-05:00")
                where_partes.append(f'reportdate<"{fecha_iso}"')
            if changedate_desde:
                fecha_iso = changedate_desde.strftime("%Y-%m-%dT%H:%M:%S-05:00")
                where_partes.append(f'changedate>="{fecha_iso}"')
            if statusdate_desde:
                fecha_iso = statusdate_desde.strftime("%Y-%m-%dT%H:%M:%S-05:00")
                where_partes.append(f'statusdate>="{fecha_iso}"')

            where_str = " and ".join(where_partes)

            url = (
                f"{URL_BASE}?lean=1"
                f"&oslc.where={where_str}"
                f"&oslc.select={select}"
                f"&oslc.orderBy=-reportdate"
                f"&oslc.pageSize={page_size}"
                f"&pageno={pagina}"
            )
            r = requests.get(
                url,
                auth=HTTPBasicAuth(USERNAME, PASSWORD),
                timeout=TIMEOUT * 2,
            )
            if r.status_code != 200:
                logging.error(f"Error HTTP {r.status_code} listando OTs pagina {pagina}")
                break

            data        = r.json()
            members     = data.get("member", [])
            total       = data.get("responseInfo", {}).get("totalCount", 0)
            total_pages = data.get("responseInfo", {}).get("totalPages", 1)

            if not members:
                break

            all_members.extend(members)
            logging.info(
                f"Maximo listar_ots pagina {pagina}/{total_pages}: "
                f"{len(members)} OTs (acumulado: {len(all_members)}/{total})"
            )

            # Cortar si se alcanzo el limite solicitado
            if max_members is not None and len(all_members) >= max_members:
                all_members = all_members[:max_members]
                logging.info(f"Limite max_members={max_members} alcanzado, deteniendo paginacion")
                break

            if pagina >= total_pages:
                break
            pagina += 1

        except Exception as e:
            logging.error(f"Error listando OTs pagina {pagina}: {e}")
            break

        finally:
            if r is not None:
                _cerrar_sesion(r)

    return all_members


# ══════════════════════════════════════════════════════════════
# 8. EXTRACCION EN LOTE: DETALLE DE OT (incluye workorderspec + worklog)
# ══════════════════════════════════════════════════════════════
#
# PROPOSITO:
#     Dado el href de una OT especifica, trae TODOS sus campos:
#     campos top-level + array workorderspec (specs) + array worklog
#     (avances) + sub-arrays varios.
#
# DIFERENCIA CON listar_ots:
#     listar_ots trae 10 campos basicos por OT pero MUCHAS OTs.
#     obtener_detalle_ot trae ~180 campos pero UNA SOLA OT.
#
# ES LA FUNCION "PESADA":
#     Cuando se procesan 26000 OTs, esta funcion se llama 26000 veces.
#     Es lo que hace que el ETL completo tarde varias horas.
#
# CIERRE DE SESION:
#     Hace UN request y cierra la sesion en el finally.
#
# RETORNO:
#     Dict con todos los campos de la OT, incluyendo los arrays
#     anidados workorderspec y worklog. Si hay error, retorna None.

def obtener_detalle_ot(href):
    """
    Descarga el detalle completo de una OT desde su href.
    Incluye workorderspec, worklog, wostatus y todos los campos.

    Cierra la sesion inmediatamente despues de obtener la respuesta
    (importante para no dejar sesiones huerfanas en Maximo).

    Parametros:
        href (str): URL completa de la OT (campo href del listado)

    Retorna:
        dict con todos los campos y sub-colecciones, o None si hay error.
    """
    r = None
    try:
        r = requests.get(
            href,
            params={"lean": "1", "oslc.select": "*"},
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            timeout=TIMEOUT,
        )
        if r.status_code != 200:
            logging.error(f"Error HTTP {r.status_code} obteniendo detalle {href}")
            return None
        return r.json()
    except Exception as e:
        logging.error(f"Error obteniendo detalle de OT en {href}: {e}")
        return None
    finally:
        if r is not None:
            _cerrar_sesion(r)

# ══════════════════════════════════════════════════════════════
# 9. EXTRACCION EN LOTE: DESCRIPCION DE CI (con cache)
# ══════════════════════════════════════════════════════════════
#
# PROPOSITO:
#     Dado un codigo de CI (ej: "ENL_FOG"), trae su descripcion
#     legible (ej: "Enlace troncal Bogota-Medellin"). Esta info
#     vive en OTRO endpoint de Maximo: /MXCI (no /RESTWO).
#
# POR QUE EXISTE EL CACHE:
#     Muchas OTs comparten el mismo CI. Ej: si 500 OTs son sobre
#     "ENL_FOG", todas piden la misma descripcion. Sin cache,
#     serian 500 requests a Maximo. Con cache, solo 1 request real
#     y 499 lecturas de memoria.
#
# COMO FUNCIONA EL CACHE:
#     Es un dict que se pasa como parametro. La funcion verifica
#     si el cinum ya esta en el cache:
#       - Si SI: devuelve el valor cacheado, sin HTTP.
#       - Si NO: hace HTTP, guarda en el cache, devuelve.
#
# CIERRE DE SESION:
#     Cada consulta real hace UN request y cierra sesion.
#     Las consultas resueltas por cache no abren sesion.
#
# COMO SE USA:
#     ci_cache = {}
#     for ot in ots:
#         desc = obtener_ci_description(ot["cinum"], cache=ci_cache)

def obtener_ci_description(cinum, cache=None):
    """
    Consulta la descripcion de un CI. Usa cache dict compartido
    entre llamadas para evitar requests redundantes.

    Cierra la sesion inmediatamente despues del request
    (importante para no dejar sesiones huerfanas en Maximo).

    Parametros:
        cinum (str): numero del CI
        cache (dict, opcional): dict {cinum: description} reutilizable

    Retorna:
        str con la descripcion, o "" si no se encontro / hubo error.
    """
    if not cinum:
        return ""

    if cache is not None and cinum in cache:
        return cache[cinum]

    r = None
    desc = ""
    try:
        r = requests.get(
            f'{URL_CI}?lean=1'
            f'&oslc.where=cinum="{cinum}"'
            f'&oslc.select=cinum,description'
            f'&oslc.pageSize=1',
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            timeout=TIMEOUT,
        )
        members = r.json().get("member", [])
        desc = members[0].get("description", "") if members else ""
    except Exception as e:
        logging.warning(f"Error consultando CI {cinum}: {e}")
    finally:
        if r is not None:
            _cerrar_sesion(r)

    if cache is not None:
        cache[cinum] = desc
    return desc

# ══════════════════════════════════════════════════════════════
# 10. EXTRACCION EN LOTE: WORKLOGS DESDE DETALLE INLINE
# ══════════════════════════════════════════════════════════════
#
# PROPOSITO:
#     Extrae los worklogs (avances) que vienen ANIDADOS dentro del
#     detalle de la OT. Maximo los devuelve en detalle["worklog"]
#     como un array.
#
# NO HACE HTTP:
#     A diferencia de las otras 3 funciones, esta NO consulta nada.
#     Solo lee del dict que ya esta en memoria. Es practicamente
#     gratis (microsegundos).
#
# POR QUE EXISTE COMO FUNCION:
#     Para que el script ETL no tenga que conocer el nombre exacto
#     de la clave ("worklog") ni manejar el caso None. Si manana
#     Maximo cambia la clave a "worklogs", solo se modifica esta
#     funcion.
#
# RETORNO:
#     Lista de dicts crudos (worklogs tal como vienen de Maximo).
#     Despues los procesa transformers/worklog.py para aplanarlos.

def extraer_worklogs_inline(detalle):
    """
    Extrae los worklogs que vienen inline en el detalle de la OT
    (clave 'worklog' como list). No hace requests adicionales.

    Parametros:
        detalle (dict): response de obtener_detalle_ot()

    Retorna:
        list[dict] con los worklogs crudos tal como vienen de Maximo,
        o [] si la OT no tiene avances.
    """
    return detalle.get("worklog", []) or []


# ══════════════════════════════════════════════════════════════
# 11. RELACIONES ENTRE TICKETS (RESTINCIDENT / restincrel)
# ══════════════════════════════════════════════════════════════
#
# Estas funciones operan sobre INCIDENTES (no sobre work orders).
# El patron documentado por el manual MPR es:
#   1) GET RESTINCIDENT?oslc.where=ticketid="..." -> obtener href
#   2) Cambiar "restincident" por "restincrel" en el href
#   3) POST con x-method-override: PATCH al href modificado
#
# Para vincular se envia spi:relatedrecord con los datos del ticket.
# Para desvincular se agrega "_action": "Delete" al payload.
#
# Imports requeridos (ya presentes en el archivo):
#   from core.config import (
#       MAXIMO_INCIDENT_URL, MAXIMO_INCREL_URL,
#       MAXIMO_INCIDENT_USER, MAXIMO_INCIDENT_PASSWORD,
#       MAXIMO_INCIDENT_SITEID,
#   )
#
# Si reutilizas MAXIMO_USER / MAXIMO_PASSWORD para los incidentes,
# basta con sustituir las dos constantes en las llamadas de abajo.

# Auth dedicada para el objeto incidente (puede coincidir con la
# auth de OTs, pero el manual la documenta por separado).
_AUTH_INC = HTTPBasicAuth(MAXIMO_INCIDENT_USER, MAXIMO_INCIDENT_PASSWORD)


def _obtener_href_incidente(ticketid):
    """
    Obtiene el href de un incidente a partir de su ticketid.
    Funcion auxiliar interna, analoga a _obtener_href() pero contra
    el objeto RESTINCIDENT.

    Parametros:
        ticketid (str): identificador del ticket. Ej: "6054120"

    Retorna:
        (href, response) si se encontro el incidente, o (None, response)
        si no existe o la consulta fallo.

    El href devuelto apunta al objeto restincident; quien lo use
    debe transformarlo a restincrel para operar relaciones.
    """
    r = requests.get(
        f"{MAXIMO_INCIDENT_URL}/?lean=1"
        f"&oslc.where=ticketid=\"{ticketid}\""
        f"&oslc.select=ticketid,href",
        auth=_AUTH_INC,
        timeout=TIMEOUT
    )
    if r.status_code != 200:
        logging.error(f"Error HTTP {r.status_code} al obtener href de ticket {ticketid}")
        return None, r
    members = r.json().get("rdfs:member") or r.json().get("member")
    if not members:
        logging.warning(f"Ticket {ticketid} no encontrado")
        return None, r
    href = members[0].get("href", "")
    return href or None, r


# ══════════════════════════════════════════════════════════════
# REEMPLAZAR la funcion vincular_ticket() existente en rest_api.py
# con esta version.
#
# Cambio (descubierto via diagnostico):
#   Para vincular un incidente con una OT (relatedrecclass=
#   WORKORDER), Maximo exige el campo `relatedrecorgid` en el
#   payload. El manual MPR solo cubria el caso INCIDENT->INCIDENT
#   donde este campo no es necesario.
#
#   Solucion: agregar parametro opcional `relatedrecorgid` que se
#   incluye en el payload solo cuando se proporciona (None = no enviar).
#   Default razonable para nuestro entorno: "MOVISTAR".
# ══════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════
# REEMPLAZAR vincular_ticket() y desvincular_ticket() en
# rest_api.py con estas versiones.
#
# HALLAZGOS ACUMULADOS:
#   1. El endpoint correcto es el subrecurso del incidente:
#      {href_incident}/relatedrecord
#   2. Funciona para ambos casos: INCIDENT y WORKORDER.
#   3. Para WORKORDER, agregar spi:relatedrecorgid.
#   4. (NUEVO) Para CREAR (vincular), usar POST limpio sin los
#      headers x-method-override/patchtype. Esos headers son
#      para UPDATE sobre un recurso ya existente, no para crear
#      uno nuevo en una coleccion.
#   5. Para ELIMINAR (desvincular), si se necesitan los headers
#      de PATCH porque es una modificacion sobre la coleccion.
# ══════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════
# REEMPLAZAR vincular_ticket() y desvincular_ticket() en
# rest_api.py por estas versiones.
#
# REVERSION + CONSOLIDACION:
#   - Volver al endpoint correcto: restincrel (el del manual MPR).
#   - Mantener el parametro relatedrecorgid (necesario para OT).
#   - El "fix" del subrecurso /relatedrecord fue un error: ese
#     subrecurso NO crea relaciones, crea SUB-INCIDENTES (descubierto
#     por POST que devolvio HTTP 201 con un spi:ticketid nuevo).
#
#   El caso INCIDENT->WORKORDER sigue fallando con HTTP 400
#   "No es un ticket valido" por una razon que aun NO sabemos. Hay
#   que seguir diagnosticando. Posibles hipotesis pendientes:
#     - Maximo en este entorno bloquea la creacion de relaciones
#       INCIDENT->WORKORDER via API y solo permite hacerlo desde
#       la UI (la relacion existente 6054120->7708638 puede
#       haberse creado manualmente por la UI).
#     - Hay un campo adicional especifico para WORKORDER que aun
#       no hemos identificado.
#     - El endpoint correcto para WORKORDER es distinto al
#       restincrel; posiblemente no este expuesto via OSLC en este
#       entorno y solo se acceda via UI.
# ══════════════════════════════════════════════════════════════


def vincular_ticket(ticketid_origen, ticketid_relacionado,
                    relatetype="RELATED",
                    relatedrecclass="INCIDENT",
                    siteid=None,
                    relatedrecorgid=None,
                    isglobal=True,
                    notificar=False,
                    notifica_interesados=False,
                    aprobcomite=False,
                    presentacomite=False):
    """
    Vincula un incidente con otro registro en Maximo.

    Endpoint (del manual MPR):
        POST {href_incident_modificado}  donde el href se obtiene
        cambiando "restincident" por "restincrel".

    Headers: x-method-override=PATCH, patchtype=MERGE

    NOTA IMPORTANTE: El caso INCIDENT->WORKORDER aun no esta
    100% validado contra esta API. Maximo rechaza con HTTP 400
    "No es un ticket valido" pese a que la UI permite crear esa
    misma relacion. Se sospecha que la API restincrel solo soporta
    INCIDENT<->INCIDENT en este entorno.

    Parametros: (ver firma)
    Retorna dict: {success, message, status, ticket, ticket_relacionado}
    """
    site = siteid or MAXIMO_INCIDENT_SITEID

    if relatedrecclass.upper() == "WORKORDER" and relatedrecorgid is None:
        relatedrecorgid = "MOVISTAR"

    r1 = None
    r2 = None
    try:
        # Paso 1+2: href incidente y transformar restincident -> restincrel
        href_inc, r1 = _obtener_href_incidente(ticketid_origen)
        if not href_inc:
            return {
                "success": False,
                "message": f"Ticket origen {ticketid_origen} no encontrado",
                "status": "not_found",
                "ticket": ticketid_origen,
                "ticket_relacionado": ticketid_relacionado,
            }

        href_rel = href_inc.replace("/restincident/", "/restincrel/")

        item_rel = {
            "spi:relatedreckey":        ticketid_relacionado,
            "spi:relatetype":           relatetype,
            "spi:relatedrecclass":      relatedrecclass,
            "spi:notificar":            notificar,
            "spi:recordkey":            ticketid_origen,
            "spi:notifica_interesados": notifica_interesados,
            "spi:aprobcomite":          aprobcomite,
            "spi:presentacomite":       presentacomite,
            "spi:siteid":               site,
            "spi:relatedrecsiteid":     site,
        }
        if relatedrecorgid:
            item_rel["spi:relatedrecorgid"] = relatedrecorgid

        # Paso 3: POST con override PATCH (como en el manual MPR)
        r2 = requests.post(
            href_rel,
            auth=_AUTH_INC,
            headers={
                "x-method-override": "PATCH",
                "patchtype":         "MERGE",
                "properties":        "*",
                "Content-Type":      "application/json"
            },
            json={
                "spi:isglobal":      isglobal,
                "spi:relatedrecord": [item_rel],
            },
            timeout=TIMEOUT
        )

        if r2.status_code in (200, 201, 204):
            logging.info(
                f"Ticket {ticketid_origen} vinculado con {ticketid_relacionado} "
                f"({relatetype} / {relatedrecclass})"
            )
            return {
                "success": True,
                "message": "Tickets vinculados correctamente",
                "status": "success",
                "ticket": ticketid_origen,
                "ticket_relacionado": ticketid_relacionado,
            }
        else:
            logging.error(
                f"Error vinculando tickets HTTP {r2.status_code}: {r2.text[:500]}"
            )
            return {
                "success": False,
                "message": f"Error HTTP {r2.status_code}",
                "status": "patch_error",
                "ticket": ticketid_origen,
                "ticket_relacionado": ticketid_relacionado,
            }

    except Exception as e:
        logging.error(
            f"Error vinculando ticket {ticketid_origen} con {ticketid_relacionado}: {e}"
        )
        return {
            "success": False,
            "message": str(e),
            "status": "exception",
            "ticket": ticketid_origen,
            "ticket_relacionado": ticketid_relacionado,
        }

    finally:
        if r1 is not None:
            _cerrar_sesion(r1)
        if r2 is not None:
            _cerrar_sesion(r2)


def desvincular_ticket(ticketid_origen, ticketid_relacionado,
                       relatetype="RELATED",
                       relatedrecclass="INCIDENT",
                       siteid=None,
                       relatedrecorgid=None):
    """
    Elimina una relacion existente entre un incidente y otro registro.

    Endpoint: POST {href_modificado_restincrel} con "_action": "Delete"

    NOTA: Sobre incidentes en estado CLOSED, Maximo acepta el POST
    con HTTP 200 pero NO aplica el cambio (silent fail). El estado
    del incidente debe verificarse externamente antes de llamar.

    Parametros: (ver firma)
    Retorna dict: {success, message, status, ticket, ticket_relacionado}
    """
    site = siteid or MAXIMO_INCIDENT_SITEID

    if relatedrecclass.upper() == "WORKORDER" and relatedrecorgid is None:
        relatedrecorgid = "MOVISTAR"

    r1 = None
    r2 = None
    try:
        href_inc, r1 = _obtener_href_incidente(ticketid_origen)
        if not href_inc:
            return {
                "success": False,
                "message": f"Ticket origen {ticketid_origen} no encontrado",
                "status": "not_found",
                "ticket": ticketid_origen,
                "ticket_relacionado": ticketid_relacionado,
            }

        href_rel = href_inc.replace("/restincident/", "/restincrel/")

        item_rel = {
            "spi:recordkey":        ticketid_origen,
            "spi:class":            "INCIDENT",
            "spi:relatedreckey":    ticketid_relacionado,
            "spi:relatetype":       relatetype,
            "spi:relatedrecclass":  relatedrecclass,
            "spi:siteid":           site,
            "spi:relatedrecsiteid": site,
            "_action":              "Delete",
        }
        if relatedrecorgid:
            item_rel["spi:relatedrecorgid"] = relatedrecorgid

        r2 = requests.post(
            href_rel,
            auth=_AUTH_INC,
            headers={
                "x-method-override": "PATCH",
                "patchtype":         "MERGE",
                "properties":        "*",
                "Content-Type":      "application/json"
            },
            json={"spi:relatedrecord": [item_rel]},
            timeout=TIMEOUT
        )

        if r2.status_code in (200, 201, 204):
            logging.info(
                f"Relacion entre {ticketid_origen} y {ticketid_relacionado} eliminada "
                f"({relatetype} / {relatedrecclass})"
            )
            return {
                "success": True,
                "message": "Relacion eliminada correctamente",
                "status": "success",
                "ticket": ticketid_origen,
                "ticket_relacionado": ticketid_relacionado,
            }
        else:
            logging.error(
                f"Error desvinculando tickets HTTP {r2.status_code}: {r2.text[:500]}"
            )
            return {
                "success": False,
                "message": f"Error HTTP {r2.status_code}",
                "status": "patch_error",
                "ticket": ticketid_origen,
                "ticket_relacionado": ticketid_relacionado,
            }

    except Exception as e:
        logging.error(
            f"Error desvinculando ticket {ticketid_origen} de {ticketid_relacionado}: {e}"
        )
        return {
            "success": False,
            "message": str(e),
            "status": "exception",
            "ticket": ticketid_origen,
            "ticket_relacionado": ticketid_relacionado,
        }

    finally:
        if r1 is not None:
            _cerrar_sesion(r1)
        if r2 is not None:
            _cerrar_sesion(r2)


def listar_tickets_relacionados(ticketid, solo_clase=None):
    """
    Lista los registros relacionados con un ticket (incidente) en Maximo.

    Maximo expone las relaciones como un subrecurso del incidente:
        GET {href_incident}/relatedrecord

    En la misma coleccion vienen mezcladas:
        - Relaciones a otros incidentes (relatedrecclass="INCIDENT")
        - Relaciones a ordenes de trabajo (relatedrecclass="WORKORDER")

    Por defecto se devuelven TODAS. Para filtrar pasar `solo_clase`.

    Parametros:
        ticketid    (str): identificador del ticket origen
        solo_clase  (str|None): si se pasa, solo retorna los miembros
                                cuyo relatedrecclass coincida (case insensitive).
                                Valores comunes: "INCIDENT", "WORKORDER".
                                None (default) = sin filtro.

    Retorna dict:
        {
            success: bool,
            message: str,
            status:  str,
            ticket:  str,
            relacionados: [
                {
                    "relatedreckey":   "6054118",   # ticket o wonum relacionado
                    "relatetype":      "RELATED",    # o "FOLLOWUP", etc.
                    "relatedrecclass": "INCIDENT",   # o "WORKORDER"
                    "recordkey":       "6054120",   # ticket origen
                    "siteid":          "REDES",
                    "orgid":           "MOVISTAR",
                    "relatedrecordid": 2451621,     # id interno de la relacion
                    "notificar":       false,
                    "presentacomite":  false,
                    "aprobcomite":     false,
                    "notifica_interesados": false,
                    "href":            "...",       # href del registro de relacion
                    ...
                },
                ...
            ]
        }

    Ejemplo de uso:
        # Todas las relaciones
        info = listar_tickets_relacionados("6054120")

        # Solo tickets relacionados (excluye OTs)
        info = listar_tickets_relacionados("6054120", solo_clase="INCIDENT")

        # Solo OTs relacionadas
        info = listar_tickets_relacionados("6054120", solo_clase="WORKORDER")
    """
    r1 = None
    r2 = None
    try:
        # Paso 1: obtener href del incidente
        r1 = requests.get(
            f"{MAXIMO_INCIDENT_URL}/?lean=1"
            f"&oslc.where=ticketid=\"{ticketid}\""
            f"&oslc.select=ticketid,href",
            auth=_AUTH_INC,
            timeout=TIMEOUT
        )
        if r1.status_code != 200:
            return {
                "success": False,
                "message": f"Error HTTP {r1.status_code}",
                "status": "http_error",
                "ticket": ticketid,
                "relacionados": [],
            }

        members = r1.json().get("rdfs:member") or r1.json().get("member")
        if not members:
            return {
                "success": False,
                "message": "Ticket no encontrado",
                "status": "not_found",
                "ticket": ticketid,
                "relacionados": [],
            }

        href = members[0].get("href", "")
        if not href:
            return {
                "success": False,
                "message": "Sin href",
                "status": "no_href",
                "ticket": ticketid,
                "relacionados": [],
            }

        # Paso 2: GET al subrecurso /relatedrecord
        r2 = requests.get(
            f"{href}/relatedrecord",
            params={"lean": "1", "oslc.select": "*"},
            auth=_AUTH_INC,
            timeout=TIMEOUT
        )
        if r2.status_code != 200:
            return {
                "success": False,
                "message": f"Error HTTP {r2.status_code} en /relatedrecord",
                "status": "http_error",
                "ticket": ticketid,
                "relacionados": [],
            }

        data = r2.json()
        miembros = data.get("rdfs:member") or data.get("member") or []

        # Filtro opcional por clase
        if solo_clase:
            clase_norm = solo_clase.upper()
            miembros = [
                m for m in miembros
                if (m.get("relatedrecclass") or "").upper() == clase_norm
            ]

        # Las claves ya vienen sin prefijo "spi:" en este endpoint
        relacionados = list(miembros)

        return {
            "success": True,
            "message": f"{len(relacionados)} relacion(es) encontrada(s)",
            "status": "success",
            "ticket": ticketid,
            "relacionados": relacionados,
        }

    except Exception as e:
        logging.error(f"Error listando relaciones de ticket {ticketid}: {e}")
        return {
            "success": False,
            "message": str(e),
            "status": "exception",
            "ticket": ticketid,
            "relacionados": [],
        }

    finally:
        if r1 is not None:
            _cerrar_sesion(r1)
        if r2 is not None:
            _cerrar_sesion(r2)