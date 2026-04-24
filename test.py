"""
test_listar.py
--------------
Prueba la consulta exacta que hace listar_ots para ver si cuelga.
"""
import requests
from requests.auth import HTTPBasicAuth
from config import MAXIMO_URL, MAXIMO_USER, MAXIMO_PASSWORD
import time

print("Construyendo URL...")

ownergroup       = "O_GESFO"
classstructureid = "4213"
select           = "wonum,href,cinum,worktype,classstructureid,status,description,location,nom_ubicacion,reportdate"
page_size        = 10

url = (
    f"{MAXIMO_URL}?lean=1"
    f'&oslc.where=ownergroup="{ownergroup}" '
    f'and classstructureid="{classstructureid}"'
    f"&oslc.select={select}"
    f"&oslc.orderBy=-reportdate"
    f"&oslc.pageSize={page_size}"
    f"&pageno=1"
)

print(f"URL: {url[:150]}...")
print()
print("Enviando request (timeout 60s)...")

inicio = time.time()
try:
    r = requests.get(
        url,
        auth=HTTPBasicAuth(MAXIMO_USER, MAXIMO_PASSWORD),
        timeout=60,
    )
    elapsed = time.time() - inicio
    print(f"Status: {r.status_code}")
    print(f"Tiempo: {elapsed:.1f}s")
    data = r.json()
    total       = data.get("responseInfo", {}).get("totalCount", 0)
    total_pages = data.get("responseInfo", {}).get("totalPages", 1)
    members     = data.get("member", [])
    print(f"Total OTs en Maximo (filtrado): {total}")
    print(f"Total paginas con page_size={page_size}: {total_pages}")
    print(f"Members en esta pagina: {len(members)}")
except Exception as e:
    elapsed = time.time() - inicio
    print(f"ERROR despues de {elapsed:.1f}s: {type(e).__name__}: {e}")