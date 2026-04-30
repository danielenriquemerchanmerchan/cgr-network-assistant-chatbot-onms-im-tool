"""
test_filtro_fecha_real.py
-------------------------
Test del filtro de fecha en la API de Maximo de tu proyecto.

PROPOSITO:
    Validar si la API que usas (URL_BASE = MAXIMO_BASE_URL/RESTWO) acepta
    filtros de fecha en el oslc.where. Usa exactamente el mismo patron de
    URL que listar_ots() para que el test sea representativo.

EJECUCION:
    py -m tests.test_filtro_fecha_real

QUE PRUEBA:
    Hace una sola consulta paginada (pagina 1, 200 OTs) con filtro de fecha
    de los ultimos 7 dias. Imprime cuantas trae y muestra las primeras 5.

QUE ESPERAMOS:
    Cantidad: pequena (~70-150 OTs operativas)
    NO: 26.593 (eso seria filtro ignorado o error)
"""

import requests
from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth

from core.logging_setup import logger
from core.config import (
    MAXIMO_URL,        # MAXIMO_BASE_URL/RESTWO
    MAXIMO_USER,
    MAXIMO_PASSWORD,
    MAXIMO_TIMEOUT,
    MAXIMO_PAGE_SIZE,
)


def main():
    print("="*70)
    print("TEST DE FILTRO DE FECHA EN LA API DE TU PROYECTO")
    print("="*70)

    # Calcular fecha de hace 7 dias en formato ISO 8601 con timezone Bogota
    hace_7_dias = datetime.now() - timedelta(days=7)
    fecha_iso = hace_7_dias.strftime("%Y-%m-%dT%H:%M:%S-05:00")

    print(f"\nURL base:                  {MAXIMO_URL}")
    print(f"Fecha umbral (hace 7 dias): {fecha_iso}")

    # Construir URL exactamente igual que listar_ots(), solo agregando el filtro
    select = "wonum,worktype,status,reportdate,description"
    ownergroup = "O_GESFO"
    classstructureid = "4213"

    url = (
        f"{MAXIMO_URL}?lean=1"
        f'&oslc.where=ownergroup="{ownergroup}" '
        f'and classstructureid="{classstructureid}" '
        f'and reportdate>="{fecha_iso}"'
        f"&oslc.select={select}"
        f"&oslc.orderBy=-reportdate"
        f"&oslc.pageSize={MAXIMO_PAGE_SIZE}"
        f"&pageno=1"
    )

    print(f"\nURL completa:")
    print(f"  {url}")
    print(f"\nLanzando consulta...")

    try:
        r = requests.get(
            url,
            auth=HTTPBasicAuth(MAXIMO_USER, MAXIMO_PASSWORD),
            timeout=MAXIMO_TIMEOUT,
            verify=False,
        )

        print(f"Status code: {r.status_code}")

        if r.status_code != 200:
            print(f"\n[ERROR] La consulta fallo:")
            print(r.text[:1000])
            return

        data = r.json()
        members = data.get("member", [])
        response_info = data.get("responseInfo", {})

        print(f"\n{'='*70}")
        print("RESULTADO")
        print(f"{'='*70}")
        print(f"OTs en esta pagina:       {len(members)}")

        # responseInfo puede tener totalCount, totalPages, pagenum
        total_count = response_info.get("totalCount")
        total_pages = response_info.get("totalPages")

        if total_count is not None:
            print(f"Total OTs (todas paginas): {total_count}")
        if total_pages is not None:
            print(f"Total paginas:            {total_pages}")

        if members:
            print(f"\nPrimeras 5 OTs como muestra:")
            print(f"  {'wonum':<12} {'worktype':<10} {'status':<8} {'fecha':<20} descripcion")
            print(f"  {'-'*12} {'-'*10} {'-'*8} {'-'*20} {'-'*40}")
            for ot in members[:5]:
                wonum = ot.get("wonum", "?")
                wt = ot.get("worktype", "?") or "?"
                status = ot.get("status", "?")
                fecha = (ot.get("reportdate") or "?")[:19]
                desc = ((ot.get("description") or "")[:40])
                print(f"  {wonum:<12} {wt:<10} {status:<8} {fecha:<20} {desc}")

        print(f"\n{'='*70}")
        if total_count is not None:
            if total_count < 500:
                print(f"[OK] El filtro FUNCIONA. Cantidad coherente ({total_count} OTs)")
                print("     Procede con la modificacion de listar_ots()")
            elif total_count < 5000:
                print(f"[ADVERTENCIA] Trajo {total_count} OTs. Verifica visualmente las fechas.")
            else:
                print(f"[ALERTA] Trajo {total_count} OTs. El filtro PROBABLEMENTE fue ignorado.")
        else:
            print("[INFO] Verifica visualmente las fechas de las primeras OTs.")
            print("       Todas deberian ser posteriores a la fecha umbral.")

    except requests.exceptions.Timeout:
        print(f"\n[ERROR] Timeout - la consulta tardo mas de {MAXIMO_TIMEOUT}s")
    except requests.exceptions.RequestException as e:
        print(f"\n[ERROR] Error de red: {e}")
    except Exception as e:
        print(f"\n[ERROR] Inesperado: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()