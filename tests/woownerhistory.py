"""
test_inprg_90d.py
-----------------
Mide cuantas INPRG hay con reportdate en los ultimos 90 dias.
"""

import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta

from core.config import (
    MAXIMO_URL,
    MAXIMO_USER,
    MAXIMO_PASSWORD,
    MAXIMO_TIMEOUT,
)


def get_auth():
    return HTTPBasicAuth(MAXIMO_USER, MAXIMO_PASSWORD)


def query_count(where, descripcion):
    print(f"\n{'='*70}")
    print(descripcion)
    print(f"{'='*70}")
    print(f"where: {where}")
    url = (
        f"{MAXIMO_URL}?lean=1"
        f"&oslc.where={where}"
        f"&oslc.select=wonum"
        f"&oslc.pageSize=1"
    )
    r = requests.get(url, auth=get_auth(), timeout=MAXIMO_TIMEOUT)
    if r.status_code == 200:
        total = r.json().get("responseInfo", {}).get("totalCount", 0)
        print(f"  Total: {total}")
        return total
    else:
        print(f"  HTTP {r.status_code}: {r.text[:200]}")
        return None


def main():
    base = (
        'ownergroup="O_GESFO" '
        'and classstructureid="4213" '
        'and worktype="MC" '
        'and status="INPRG"'
    )

    # Probar varias ventanas para entender la distribucion
    for dias in [14, 30, 60, 90, 120, 180]:
        fecha = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%dT%H:%M:%S-05:00")
        where = f'{base} and reportdate>="{fecha}"'
        query_count(where, f"INPRG con reportdate >= ultimos {dias} dias")

    # Total sin filtro de fecha (referencia)
    query_count(base, "INPRG total (sin filtro fecha)")


if __name__ == "__main__":
    main()