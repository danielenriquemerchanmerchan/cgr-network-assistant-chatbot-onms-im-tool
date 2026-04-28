"""
verificar_tipos_bandeja.py
--------------------------
Script de verificacion: muestra la distribucion de worktype y woclass
de las OTs INPRG/COMP en O_GESFO classstructureid=4213.

PROPOSITO:
    Antes de escalar estadisticas operativas, confirmar que el filtro
    actual (ownergroup=O_GESFO + classstructureid=4213) realmente trae
    solo OTs de fallas operativas y no actividades, preventivos u otros
    tipos de trabajo.

EJECUCION:
    py -m tests.verificar_tipos_bandeja
"""

from collections import Counter
from datetime import datetime, timedelta

from core.logging_setup import logger
from integrations.maximo.rest_api import listar_ots


def parsear_fecha(fecha_str):
    if not fecha_str:
        return None
    try:
        return datetime.strptime(fecha_str[:19], "%Y-%m-%dT%H:%M:%S")
    except Exception:
        return None


def main():
    print("\n" + "="*70)
    print("VERIFICACION DE TIPOS DE OT EN BANDEJA O_GESFO/4213")
    print("="*70)

    print("\nConsultando OTs de O_GESFO classstructureid=4213...")
    todas = listar_ots(ownergroup="O_GESFO", classstructureid="4213")
    print(f"Total OTs traidas: {len(todas)}")

    # Filtrar INPRG y COMP
    activas = [o for o in todas if o.get("status") in ("INPRG", "COMP")]
    print(f"De ellas, INPRG/COMP: {len(activas)}")

    if not activas:
        print("No hay OTs activas para analizar.")
        return

    # Distribucion por worktype
    worktypes = Counter(o.get("worktype") or "(sin worktype)" for o in activas)
    print(f"\n{'-'*70}")
    print("DISTRIBUCION POR worktype:")
    print(f"{'-'*70}")
    for wt, cnt in sorted(worktypes.items(), key=lambda x: -x[1]):
        pct = (cnt / len(activas)) * 100
        print(f"  {wt:<30}  {cnt:>6}  ({pct:>5.1f}%)")

    # Distribucion por status (INPRG vs COMP)
    statuses = Counter(o.get("status") for o in activas)
    print(f"\n{'-'*70}")
    print("DISTRIBUCION POR status:")
    print(f"{'-'*70}")
    for s, cnt in sorted(statuses.items(), key=lambda x: -x[1]):
        pct = (cnt / len(activas)) * 100
        print(f"  {s:<30}  {cnt:>6}  ({pct:>5.1f}%)")

    # Cruzar worktype x antiguedad
    ahora = datetime.now()
    print(f"\n{'-'*70}")
    print("CRUCE worktype x ANTIGUEDAD (en INPRG/COMP):")
    print(f"{'-'*70}")
    print(f"  {'worktype':<30}  {'<7d':>6} {'7-30d':>6} {'30-90d':>6} {'>90d':>6} {'Total':>6}")

    cruce = {}
    for ot in activas:
        wt = ot.get("worktype") or "(sin)"
        fecha = parsear_fecha(ot.get("reportdate"))
        if fecha is None:
            categoria = "sin_fecha"
        else:
            dias = (ahora - fecha).days
            if dias < 7:
                categoria = "fresca"
            elif dias < 30:
                categoria = "tibia"
            elif dias < 90:
                categoria = "antigua"
            else:
                categoria = "muy_antigua"

        if wt not in cruce:
            cruce[wt] = Counter()
        cruce[wt][categoria] += 1

    for wt in sorted(cruce.keys(), key=lambda k: -sum(cruce[k].values())):
        c = cruce[wt]
        total = sum(c.values())
        print(f"  {wt:<30}  {c['fresca']:>6} {c['tibia']:>6} "
              f"{c['antigua']:>6} {c['muy_antigua']:>6} {total:>6}")

    # Mostrar ejemplos de cada worktype para inspección visual
    print(f"\n{'-'*70}")
    print("EJEMPLOS POR worktype (3 OTs por tipo, las mas antiguas):")
    print(f"{'-'*70}")
    por_worktype = {}
    for ot in activas:
        wt = ot.get("worktype") or "(sin)"
        if wt not in por_worktype:
            por_worktype[wt] = []
        por_worktype[wt].append(ot)

    for wt in sorted(por_worktype.keys()):
        # Ordenar por reportdate ascendente (mas antiguas primero)
        ejemplos = sorted(
            por_worktype[wt],
            key=lambda o: o.get("reportdate") or ""
        )[:3]
        print(f"\n  worktype={wt}:")
        for ej in ejemplos:
            wonum = ej.get("wonum", "?")
            status = ej.get("status", "?")
            fecha = (ej.get("reportdate") or "")[:10]
            desc = (ej.get("description") or "")[:60]
            print(f"    {wonum} | {status} | {fecha} | {desc}")

    print(f"\n{'='*70}")
    print("Verificacion completada.")


if __name__ == "__main__":
    main()