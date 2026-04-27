"""
validar.py
----------
Script de validacion manual del pipeline ETL.

ESTADO ACTUAL: REFERENCIA / PRUEBA AD-HOC
    Sirvio durante el desarrollo para verificar manualmente que el
    transformer producia el output esperado a partir de un JSON real.

USO ACTUAL:
    Ejecutar como modulo desde la raiz del proyecto:
        py -m tests.validar

    Puede servir para validaciones puntuales: probar un campo nuevo
    agregado a campos.py, verificar el comportamiento con una OT
    especifica, debuggear sin esperar al pipeline completo.

USO FUTURO:
    Cuando el proyecto requiera tests automaticos (con pytest u otro
    framework), este archivo se reemplazara por tests propiamente
    dichos en tests/test_*.py que usen los fixtures de tests/fixtures/.
"""

import sys
import json
import requests
from requests.auth import HTTPBasicAuth

from core.config import (
    MAXIMO_URL as URL_WO,
    MAXIMO_USER as USERNAME,
    MAXIMO_PASSWORD as PASSWORD,
    MAXIMO_TIMEOUT as TIMEOUT,
)


def obtener_primera_ot_gesfo():
    """Toma el wonum mas reciente de O_GESFO para usarlo de prueba."""
    url = (
        f"{URL_WO}?lean=1"
        f'&oslc.where=ownergroup="O_GESFO"'
        f"&oslc.select=wonum,href"
        f"&oslc.orderBy=-reportdate"
        f"&oslc.pageSize=1"
    )
    r = requests.get(url, auth=HTTPBasicAuth(USERNAME, PASSWORD), timeout=TIMEOUT)
    members = r.json().get("member", [])
    if not members:
        return None, None
    return members[0].get("wonum"), members[0].get("href")


def obtener_ot_por_wonum(wonum):
    """Obtiene el href de una OT especifica."""
    url = (
        f"{URL_WO}?lean=1"
        f'&oslc.where=wonum="{wonum}"'
        f"&oslc.select=wonum,href"
    )
    r = requests.get(url, auth=HTTPBasicAuth(USERNAME, PASSWORD), timeout=TIMEOUT)
    members = r.json().get("member", [])
    if not members:
        return None
    return members[0].get("href")


def inspeccionar_detalle(href):
    """Descarga el detalle completo e inspecciona campos relacionados con worklog."""
    print(f"\n{'='*70}")
    print(f"Descargando detalle desde: {href}")
    print(f"{'='*70}\n")

    r = requests.get(
        href,
        params={"lean": "1", "oslc.select": "*"},
        auth=HTTPBasicAuth(USERNAME, PASSWORD),
        timeout=TIMEOUT,
    )
    data = r.json()

    # ── 1. Ver TODAS las claves del detalle ───────────────────
    print("CAMPOS DISPONIBLES EN EL DETALLE (top-level):")
    print("-" * 70)
    claves = sorted(data.keys())
    for k in claves:
        v = data[k]
        if isinstance(v, list):
            tipo = f"list[{len(v)}]"
        elif isinstance(v, dict):
            tipo = "dict"
        else:
            s = str(v)[:40]
            tipo = f"{type(v).__name__} = {s}"
        print(f"  {k:40s} {tipo}")

    # ── 2. Buscar cualquier clave que contenga 'worklog' ──────
    print("\n")
    print("CLAVES RELACIONADAS CON 'worklog':")
    print("-" * 70)
    worklog_keys = [k for k in data.keys() if "worklog" in k.lower()]
    if not worklog_keys:
        print("  (ninguna)")
    else:
        for k in worklog_keys:
            v = data[k]
            if isinstance(v, list):
                print(f"  {k:40s} list con {len(v)} items")
            else:
                print(f"  {k:40s} {type(v).__name__}")

    # ── 3. Si hay array de worklogs, mostrar estructura de 1 ──
    print("\n")
    print("ESTRUCTURA DE UN WORKLOG INDIVIDUAL:")
    print("-" * 70)

    # Los nombres posibles segun documentacion de Maximo OSLC
    posibles_nombres = ["worklog", "worklogs", "WORKLOG"]
    worklog_array = None
    nombre_usado = None
    for nombre in posibles_nombres:
        if nombre in data and isinstance(data[nombre], list):
            worklog_array = data[nombre]
            nombre_usado = nombre
            break

    if worklog_array is None:
        # Buscar cualquier lista cuyos items tengan claves sugerentes
        for k, v in data.items():
            if isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
                primer_item = v[0]
                claves_item = set(primer_item.keys())
                if any(x in claves_item for x in ["createdate", "createby", "worklogid", "logtype"]):
                    worklog_array = v
                    nombre_usado = k
                    print(f"  (no habia clave 'worklog' estandar, pero '{k}' parece serlo)")
                    break

    if worklog_array is None:
        print("  No se encontro array de worklogs en esta OT.")
        print("  Puede ser que esta OT no tenga avances, o que Maximo los exponga")
        print("  a traves de worklog_collectionref (URL separada).")

        # Intentar con worklog_collectionref
        wl_ref = data.get("worklog_collectionref")
        if wl_ref:
            print(f"\n  Detectado worklog_collectionref: {wl_ref}")
            print("  Consultandolo...")
            try:
                r2 = requests.get(
                    wl_ref,
                    params={"lean": "1", "oslc.select": "*"},
                    auth=HTTPBasicAuth(USERNAME, PASSWORD),
                    timeout=TIMEOUT,
                )
                data2 = r2.json()
                worklog_array = data2.get("member", [])
                nombre_usado  = "worklog_collectionref → member"
                print(f"  Devolvio {len(worklog_array)} worklogs desde endpoint separado.")
            except Exception as e:
                print(f"  Error consultando: {e}")

    if worklog_array and len(worklog_array) > 0:
        print(f"\n  Array usado: '{nombre_usado}'")
        print(f"  Cantidad de worklogs: {len(worklog_array)}")
        print(f"\n  Estructura del PRIMER worklog:")
        primer = worklog_array[0]
        for k in sorted(primer.keys()):
            v = str(primer[k])[:80]
            print(f"    {k:35s} {v}")

        if len(worklog_array) > 1:
            print(f"\n  Estructura del SEGUNDO worklog:")
            segundo = worklog_array[1]
            for k in sorted(segundo.keys()):
                v = str(segundo[k])[:80]
                print(f"    {k:35s} {v}")

    elif worklog_array is not None and len(worklog_array) == 0:
        print(f"  Array '{nombre_usado}' existe pero esta vacio (OT sin avances).")

    # ── 4. Guardar el JSON completo para inspeccion manual ────
    print("\n")
    print("JSON COMPLETO GUARDADO:")
    print("-" * 70)
    with open("ot_detalle_muestra.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print("  → ot_detalle_muestra.json")
    print("  Abrir ese archivo para inspeccionar el response completo.")


def main():
    # Permitir pasar un wonum especifico como argumento
    if len(sys.argv) > 1:
        wonum = sys.argv[1]
        print(f"Consultando OT especifica: {wonum}")
        href = obtener_ot_por_wonum(wonum)
        if not href:
            print(f"OT {wonum} no encontrada.")
            return
    else:
        print("Buscando la OT mas reciente de O_GESFO...")
        wonum, href = obtener_primera_ot_gesfo()
        if not href:
            print("No se encontraron OTs de O_GESFO.")
            return
        print(f"OT de prueba: {wonum}")

    inspeccionar_detalle(href)


if __name__ == "__main__":
    main()