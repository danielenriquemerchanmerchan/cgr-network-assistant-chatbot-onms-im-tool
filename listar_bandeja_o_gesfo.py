import requests
from requests.auth import HTTPBasicAuth
from config import MAXIMO_URL, MAXIMO_USER, MAXIMO_PASSWORD

url = (
    f"{MAXIMO_URL}?lean=1"
    f'&oslc.where=ownergroup="O_GESFO"'
    f"&oslc.select=wonum,cinum,worktype,classstructureid,status"
    f"&oslc.orderBy=-reportdate"
    f"&oslc.pageSize=200"
)
r = requests.get(url, auth=HTTPBasicAuth(MAXIMO_USER, MAXIMO_PASSWORD), timeout=30)
members = r.json().get("member", [])

# Agrupar cinum únicos con su worktype y classstructureid
unicos = {}
for m in members:
    cinum = m.get("cinum") or "NULL"
    worktype = m.get("worktype") or "?"
    classi = m.get("classstructureid") or "?"
    clave = f"{cinum}|{worktype}|{classi}"
    if clave not in unicos:
        unicos[clave] = 0
    unicos[clave] += 1

print(f"\nTotal OTs consultadas: {len(members)}")
print(f"Combinaciones únicas cinum|worktype|classstructureid:\n")
for clave, count in sorted(unicos.items(), key=lambda x: -x[1]):
    print(f"  {count:4d} veces  →  {clave}")