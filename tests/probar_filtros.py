"""
probar_filtros.py
-----------------
Test de los filtros nuevos agregados a listar_ots().
"""

from datetime import datetime, timedelta
from integrations.maximo.rest_api import listar_ots


def main():
    print("="*60)
    print("TEST DE FILTROS NUEVOS EN listar_ots()")
    print("="*60)

    # TEST 1: Filtro por worktype solamente
    print("\n--- Test 1: filtro worktype=MC ---")
    ots = listar_ots("O_GESFO", "4213", worktype="MC", max_members=200)
    print(f"OTs traidas: {len(ots)}")
    if ots:
        worktypes_unicos = set(o.get("worktype") for o in ots)
        print(f"Worktypes en resultado: {worktypes_unicos}")
        print("(deberia ser solo {'MC'})")

    # TEST 2: Filtro por fecha
    print("\n--- Test 2: filtro fecha_desde=hace 7 dias ---")
    hace_7d = datetime.now() - timedelta(days=7)
    ots = listar_ots("O_GESFO", "4213", fecha_desde=hace_7d)
    print(f"OTs traidas: {len(ots)}")
    print("(deberia ser ~150-200 OTs)")
    if ots:
        fechas = sorted(o.get("reportdate", "") for o in ots if o.get("reportdate"))
        print(f"Fecha mas antigua: {fechas[0][:19]}")
        print(f"Fecha mas reciente: {fechas[-1][:19]}")

    # TEST 3: Filtros combinados (caso del operativo)
    print("\n--- Test 3: filtros combinados (MC + fecha + status) ---")
    ots = listar_ots(
        "O_GESFO",
        "4213",
        worktype="MC",
        fecha_desde=hace_7d,
        status_in=["INPRG", "COMP", "CLOSE"],
    )
    print(f"OTs traidas: {len(ots)}")
    print("(deberia ser ~98 OTs - como el test directo de ayer)")
    if ots:
        statuses = {o.get("status") for o in ots}
        print(f"Statuses encontrados: {statuses}")
        print(f"Worktypes: {set(o.get('worktype') for o in ots)}")

    print("\n" + "="*60)
    print("Tests completados")
    print("="*60)


if __name__ == "__main__":
    main()