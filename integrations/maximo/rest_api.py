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

def listar_ots(ownergroup, classstructureid, page_size=PAGE_SIZE_DEFAULT, select=None, max_members=None): # colocar max_member=20 si queremos traer solo 20 Ots y mas_member=None si queremos traer todo
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
            url = (
                f"{URL_BASE}?lean=1"
                f'&oslc.where=ownergroup="{ownergroup}" '
                f'and classstructureid="{classstructureid}"'
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