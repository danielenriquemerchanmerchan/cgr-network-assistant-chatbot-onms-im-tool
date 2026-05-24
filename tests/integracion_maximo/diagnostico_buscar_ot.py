"""
diagnostico_buscar_ot.py
------------------------
Verifica si una OT existe en Maximo dev y muestra sus campos
identificatorios (siteid, orgid, wonum, woclass, etc.) para
diagnosticar errores de vinculacion.

Tambien inspecciona la relacion existente de 6054120->7708638
en la coleccion /relatedrecord para ver QUE site Maximo guardo
realmente para esa relacion (puede no ser REDES).

Uso:
    python -m tests.integracion_maximo.diagnostico_buscar_ot 7708638
"""

import sys
import json
import requests
from requests.auth import HTTPBasicAuth

from tests.integracion_maximo._comun import guard_ambiente_dev
from core.config import (
    MAXIMO_URL,
    MAXIMO_INCIDENT_URL,
    MAXIMO_INCIDENT_USER,
    MAXIMO_INCIDENT_PASSWORD,
    MAXIMO_TIMEOUT,
)


def main():
    guard_ambiente_dev()

    if len(sys.argv) < 2:
        print("Uso: python -m tests.integracion_maximo.diagnostico_buscar_ot <wonum>")
        sys.exit(1)
    wonum = sys.argv[1]

    auth = HTTPBasicAuth(MAXIMO_INCIDENT_USER, MAXIMO_INCIDENT_PASSWORD)

    # ─────────────────────────────────────────────────────────
    # Parte A: buscar la OT en el objeto RESTWO (work orders)
    # ─────────────────────────────────────────────────────────
    print(f"\n{'=' * 70}")
    print(f"  PARTE A: buscar OT {wonum} en RESTWO")
    print(f"{'=' * 70}\n")

    r1 = requests.get(
        f"{MAXIMO_URL}/?lean=1"
        f"&oslc.where=wonum=\"{wonum}\""
        f"&oslc.select=wonum,siteid,orgid,woclass,worktype,status,description",
        auth=auth, timeout=MAXIMO_TIMEOUT,
    )
    print(f"HTTP {r1.status_code}")
    members = (r1.json().get("rdfs:member") or r1.json().get("member")) or []
    if not members:
        print(f"✗ OT {wonum} NO encontrada en RESTWO\n")
    else:
        print(f"✓ OT encontrada ({len(members)} resultado/s):")
        for i, m in enumerate(members, 1):
            print(f"\n  Resultado {i}:")
            for k in ("wonum", "siteid", "orgid", "woclass", "worktype",
                      "status", "description"):
                if k in m:
                    print(f"      {k:<15} = {m[k]}")

    # ─────────────────────────────────────────────────────────
    # Parte B: ir a 6054120/relatedrecord y leer el item de WORKORDER
    #          para ver con que site/orgid lo guardo Maximo
    # ─────────────────────────────────────────────────────────
    print(f"\n{'=' * 70}")
    print(f"  PARTE B: inspeccionar relacion 6054120 -> {wonum}")
    print(f"          (ver con que site y orgid quedo guardada)")
    print(f"{'=' * 70}\n")

    # Obtener href del incidente 6054120
    r2 = requests.get(
        f"{MAXIMO_INCIDENT_URL}/?lean=1"
        f"&oslc.where=ticketid=\"6054120\"&oslc.select=ticketid,href",
        auth=auth, timeout=MAXIMO_TIMEOUT,
    )
    members2 = (r2.json().get("rdfs:member") or r2.json().get("member")) or []
    if not members2:
        print("✗ Incidente 6054120 no existe (no puedo inspeccionar la relacion)")
        return

    href = members2[0]["href"]
    r3 = requests.get(
        f"{href}/relatedrecord",
        params={"lean": "1", "oslc.select": "*"},
        auth=auth, timeout=MAXIMO_TIMEOUT,
    )
    data = r3.json()
    miembros = data.get("rdfs:member") or data.get("member") or []

    # Buscar el item cuya relatedreckey coincida con el wonum solicitado
    encontrado = None
    for m in miembros:
        if str(m.get("relatedreckey", "")) == str(wonum):
            encontrado = m
            break

    if not encontrado:
        print(f"✗ Relacion 6054120 -> {wonum} no encontrada en /relatedrecord")
        print(f"\nMiembros disponibles:")
        for m in miembros:
            print(f"   relatedreckey={m.get('relatedreckey')} "
                  f"relatedrecclass={m.get('relatedrecclass')} "
                  f"siteid={m.get('siteid')} "
                  f"relatedrecsiteid={m.get('relatedrecsiteid')}")
        return

    print(f"✓ Relacion existente encontrada. Campos completos:\n")
    print(json.dumps(encontrado, indent=4, ensure_ascii=False))

    print(f"\n>>> Campos clave para vincular:")
    for k in ("relatedreckey", "relatedrecclass", "relatetype",
              "siteid", "relatedrecsiteid",
              "orgid", "relatedrecorgid"):
        if k in encontrado:
            print(f"      {k:<22} = {encontrado[k]}")


if __name__ == "__main__":
    main()