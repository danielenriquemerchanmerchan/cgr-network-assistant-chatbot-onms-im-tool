"""
test_listado_simple_tiempo.py
-----------------------------
Mide cuanto tarda Maximo en devolver el listado completo de wonums+changedate
para todas las OTs del modelo (INPRG + cerradas recientes).

Si tarda menos de 30 segundos, el diseno propuesto funciona:
  - Listar rapido todas las OTs
  - Solo traer detalle de las que cambiaron desde la ultima corrida.
"""

import time
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


def listar_solo_metadata(where, descripcion):
    """Lista wonums + changedate sin traer detalle. Mide tiempo."""
    print(f"\n{'='*70}")
    print(descripcion)
    print(f"{'='*70}")

    inicio = time.time()
    todos = []
    pagina = 1

    while pagina <= 20:
        url = (
            f"{MAXIMO_URL}?lean=1"
            f"&oslc.where={where}"
            f"&oslc.select=wonum,status,changedate,statusdate"
            f"&oslc.pageSize=200"
            f"&pageno={pagina}"
        )
        r = requests.get(url, auth=get_auth(), timeout=MAXIMO_TIMEOUT)
        if r.status_code != 200:
            print(f"  HTTP {r.status_code}")
            return None
        data = r.json()
        members = data.get("member", [])
        todos.extend(members)
        if len(members) < 200:
            break
        pagina += 1

    duracion = time.time() - inicio
    print(f"  Recolectados: {len(todos)} OTs en {duracion:.1f} segundos")
    print(f"  Paginas: {pagina}")
    if todos:
        print(f"  Atributos por OT: {sorted(todos[0].keys())}")
        print(f"  Ejemplo: {todos[0]}")
    return todos, duracion


def main():
    fecha_14d = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%S-05:00")

    base = (
        'ownergroup="O_GESFO" '
        'and classstructureid="4213" '
        'and worktype="MC"'
    )

    # Listado 1: INPRG
    res1 = listar_solo_metadata(
        f'{base} and status="INPRG"',
        "LISTADO 1: INPRG (sin detalle)",
    )
    if res1 is None:
        return
    inprg, t1 = res1

    # Listado 2: COMP/CLOSE/CAN recientes
    res2 = listar_solo_metadata(
        f'{base} and status in ["COMP","CLOSE","CAN"] and statusdate>="{fecha_14d}"',
        "LISTADO 2: COMP/CLOSE/CAN recientes",
    )
    if res2 is None:
        return
    cerradas, t2 = res2

    total = len(inprg) + len(cerradas)
    tiempo_total = t1 + t2

    print(f"\n{'='*70}")
    print("RESUMEN")
    print(f"{'='*70}")
    print(f"  Total OTs listadas:  {total}")
    print(f"  Tiempo total:        {tiempo_total:.1f} segundos")
    print(f"\n  Esto es lo que tardaria SOLO listar (sin detalle).")
    print(f"  Si <30s, el modelo de 'listar primero, detalle solo si cambio' funciona.")


if __name__ == "__main__":
    main()