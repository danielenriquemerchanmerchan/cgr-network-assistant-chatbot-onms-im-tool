"""
test_maximo_or_v3.py
--------------------
Tercera ronda. Las pruebas anteriores confirmaron:
    - OSLC NO acepta OR ni parentesis
    - status="CAN" existe (273 OTs historicas)
    - Filtro por changedate >= 14d funciona (261 registros)

Esta ronda valida el matiz importante:
    - changedate refleja CUALQUIER modificacion, no solo cambio de status
    - statusdate refleja especificamente cuando cambio el status
    - Necesitamos saber cual usar en el plan B

Y de paso medimos el solapamiento real entre las dos queries del plan B
para estimar el tamaño del set deduplicado.

Ejecucion:
    py -m tests.etl_reportdate_or_changedate_v3

Borrar despues de validar.
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


def query_completo(where_clause, descripcion, page_size=200):
    """
    Ejecuta un query OSLC y retorna TODOS los wonums (paginando si hace falta).
    Devuelve set de wonums.
    """
    print(f"\n{'='*70}")
    print(f"PRUEBA: {descripcion}")
    print(f"{'='*70}")
    print(f"where: {where_clause}")

    wonums = set()
    pagina = 1

    while True:
        url = (
            f"{MAXIMO_URL}?lean=1"
            f"&oslc.where={where_clause}"
            f"&oslc.select=wonum,status,reportdate,changedate,statusdate"
            f"&oslc.pageSize={page_size}"
            f"&pageno={pagina}"
        )
        try:
            r = requests.get(
                url,
                auth=HTTPBasicAuth(MAXIMO_USER, MAXIMO_PASSWORD),
                timeout=MAXIMO_TIMEOUT,
            )
            if r.status_code != 200:
                print(f"  HTTP {r.status_code} en pagina {pagina}")
                print(f"  Body (primeros 500 chars): {r.text[:500]}")
                return None  # senal de error

            data = r.json()
            members = data.get("member", [])
            total = data.get("responseInfo", {}).get("totalCount", 0)

            if pagina == 1:
                print(f"  HTTP 200. Total segun Maximo: {total}")

            for m in members:
                if m.get("wonum"):
                    wonums.add(m["wonum"])

            if len(members) < page_size:
                # ultima pagina
                break
            pagina += 1
            if pagina > 20:
                print(f"  STOP: mas de 20 paginas, abortando paginacion")
                break

        except Exception as e:
            print(f"  EXCEPTION: {e}")
            return None

    print(f"  Wonums unicos recolectados: {len(wonums)}")
    return wonums


def main():
    ahora = datetime.now()
    fecha_14d = (ahora - timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%S-05:00")

    print(f"Fecha de corte (14 dias atras): {fecha_14d}")
    print(f"Maximo URL: {MAXIMO_URL}")

    # ---------------------------------------------------------------
    # PRUEBA 8: Validar que se puede filtrar por statusdate.
    # ---------------------------------------------------------------
    where_statusdate = (
        f'ownergroup="O_GESFO" '
        f'and classstructureid="4213" '
        f'and worktype="MC" '
        f'and status in ["INPRG","COMP","CLOSE","CAN"] '
        f'and statusdate>="{fecha_14d}"'
    )
    set_statusdate = query_completo(
        where_statusdate,
        "8. Filtro por statusdate (cambios de status reales)",
    )

    if set_statusdate is None:
        print("\n*** statusdate NO se puede usar como filtro en OSLC ***")
        print("Plan B tendra que usar changedate (con el matiz de las ediciones).")
        statusdate_usable = False
    else:
        print(f"\n*** statusdate SI se puede usar como filtro en OSLC ***")
        statusdate_usable = True

    # ---------------------------------------------------------------
    # PRUEBA 9: Set A (reportdate) - misma logica del operativo actual
    # ---------------------------------------------------------------
    where_a = (
        f'ownergroup="O_GESFO" '
        f'and classstructureid="4213" '
        f'and worktype="MC" '
        f'and status in ["INPRG","COMP","CLOSE","CAN"] '
        f'and reportdate>="{fecha_14d}"'
    )
    set_a = query_completo(where_a, "9. Set A - reportdate >= 14d (las nuevas)")

    # ---------------------------------------------------------------
    # PRUEBA 10: Set B (changedate) - las que se movieron
    # ---------------------------------------------------------------
    where_b = (
        f'ownergroup="O_GESFO" '
        f'and classstructureid="4213" '
        f'and worktype="MC" '
        f'and status in ["INPRG","COMP","CLOSE","CAN"] '
        f'and changedate>="{fecha_14d}"'
    )
    set_b = query_completo(where_b, "10. Set B - changedate >= 14d (con movimiento)")

    # ---------------------------------------------------------------
    # ANALISIS DE SOLAPAMIENTO
    # ---------------------------------------------------------------
    print(f"\n{'='*70}")
    print("ANALISIS DE SOLAPAMIENTO")
    print(f"{'='*70}")

    if set_a is not None and set_b is not None:
        union = set_a | set_b
        interseccion = set_a & set_b
        solo_a = set_a - set_b
        solo_b = set_b - set_a

        print(f"  |Set A (reportdate)|:           {len(set_a)}")
        print(f"  |Set B (changedate)|:           {len(set_b)}")
        print(f"  |Union (unicos para procesar)|: {len(union)}")
        print(f"  |Interseccion (en ambos)|:      {len(interseccion)}")
        print(f"  Solo en A (nuevas sin movimiento posterior): {len(solo_a)}")
        print(f"  Solo en B (viejas con movimiento reciente):  {len(solo_b)}")

        if solo_b:
            print(f"\n  Ejemplos de OTs viejas con movimiento reciente")
            print(f"  (las que el ETL actual ESTA PERDIENDO):")
            for wonum in list(solo_b)[:5]:
                print(f"    - {wonum}")

    if statusdate_usable:
        if set_statusdate is not None and set_b is not None:
            print(f"\n  |Set B (changedate)|:    {len(set_b)}")
            print(f"  |Set por statusdate|:    {len(set_statusdate)}")
            print(f"  Diferencia (movimientos NO de status): {len(set_b) - len(set_statusdate)}")

    print(f"\n{'='*70}")
    print("FIN DE PRUEBAS V3")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()