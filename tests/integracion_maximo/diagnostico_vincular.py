"""
diagnostico_vincular_v3.py
--------------------------
Repite el POST limpio que devolvio 200 OK pero NO creo la relacion,
mostrando TODOS los headers y el body de respuesta de Maximo
sin filtrar.

El objetivo es ver si Maximo esta devolviendo algun warning,
location header, o respuesta vacia que indique por que aceptamos
el 200 sin crear nada.

Tambien prueba 4 variantes del POST para ver cual realmente crea:
    V1) POST limpio sin headers especiales (lo que falla silente)
    V2) POST con x-method-override=PATCH y patchtype=MERGE (lo que daba update_on_createuri)
    V3) POST con x-method-override=PATCH y patchtype=MERGE_LIST
    V4) POST con properties=* y sin nada mas

Uso:
    python -m tests.integracion_maximo.diagnostico_vincular_v3
"""

import json
import requests
from requests.auth import HTTPBasicAuth

from tests.integracion_maximo._comun import guard_ambiente_dev
from core.config import (
    MAXIMO_INCIDENT_URL,
    MAXIMO_INCIDENT_USER,
    MAXIMO_INCIDENT_PASSWORD,
    MAXIMO_TIMEOUT,
)


def _probar(etiqueta, url, headers, payload, auth):
    print(f"\n{'─' * 70}")
    print(f"  {etiqueta}")
    print(f"{'─' * 70}")
    print(f"  Headers enviados:")
    for k, v in headers.items():
        print(f"      {k}: {v}")

    r = requests.post(url, auth=auth, headers=headers, json=payload, timeout=MAXIMO_TIMEOUT)
    print(f"\n  HTTP {r.status_code}")
    print(f"\n  Response headers:")
    for k, v in r.headers.items():
        print(f"      {k}: {v}")
    print(f"\n  Response body:")
    if not r.text.strip():
        print("      (vacio)")
    else:
        try:
            print(json.dumps(r.json(), indent=6, ensure_ascii=False))
        except Exception:
            print(f"      {r.text[:1000]}")


def _listar(href_inc, auth):
    r = requests.get(
        f"{href_inc}/relatedrecord",
        params={"lean": "1", "oslc.select": "*"},
        auth=auth, timeout=MAXIMO_TIMEOUT,
    )
    return r.json().get("rdfs:member") or r.json().get("member") or []


def main():
    guard_ambiente_dev()

    auth = HTTPBasicAuth(MAXIMO_INCIDENT_USER, MAXIMO_INCIDENT_PASSWORD)

    # Obtener href del incidente origen
    r0 = requests.get(
        f"{MAXIMO_INCIDENT_URL}/?lean=1"
        f"&oslc.where=ticketid=\"6521466\"&oslc.select=ticketid,href",
        auth=auth, timeout=MAXIMO_TIMEOUT,
    )
    href_inc = (r0.json().get("rdfs:member") or r0.json().get("member"))[0]["href"]
    url = f"{href_inc}/relatedrecord"

    payload = {
        "spi:isglobal": True,
        "spi:relatedrecord": [
            {
                "spi:relatedreckey":        "7708638",
                "spi:relatetype":           "FOLLOWUP",
                "spi:relatedrecclass":      "WORKORDER",
                "spi:notificar":            False,
                "spi:recordkey":            "6521466",
                "spi:notifica_interesados": False,
                "spi:aprobcomite":          False,
                "spi:presentacomite":       False,
                "spi:siteid":               "REDES",
                "spi:relatedrecsiteid":     "REDES",
                "spi:class":                "INCIDENT",
                "spi:relatedrecorgid":      "MOVISTAR",
            }
        ]
    }

    print(f"\n{'=' * 70}")
    print(f"  DIAGNOSTICO v3: POST a {url}")
    print(f"  4 variantes de headers para identificar la correcta")
    print(f"{'=' * 70}")

    # Estado inicial
    rels = _listar(href_inc, auth)
    print(f"\nEstado inicial: {len(rels)} relacion(es)")

    # ---------- V1: POST limpio (el actual, falla silente)
    _probar(
        "V1) POST limpio (Content-Type + properties)",
        url,
        {"Content-Type": "application/json", "properties": "*"},
        payload, auth,
    )
    rels = _listar(href_inc, auth)
    print(f"\n  Tras V1: {len(rels)} relacion(es)  "
          f"{'✓ CREO' if any(m.get('relatedreckey')=='7708638' for m in rels) else '✗ NO creo'}")

    # ---------- V2: POST con override PATCH (el original que daba update_on_createuri)
    _probar(
        "V2) POST con x-method-override=PATCH y patchtype=MERGE",
        url,
        {
            "Content-Type":      "application/json",
            "properties":        "*",
            "x-method-override": "PATCH",
            "patchtype":         "MERGE",
        },
        payload, auth,
    )
    rels = _listar(href_inc, auth)
    print(f"\n  Tras V2: {len(rels)} relacion(es)  "
          f"{'✓ CREO' if any(m.get('relatedreckey')=='7708638' for m in rels) else '✗ NO creo'}")

    # ---------- V3: con patchtype=MERGE_LIST (variante OSLC para colecciones)
    _probar(
        "V3) POST con x-method-override=PATCH y patchtype=MERGE_LIST",
        url,
        {
            "Content-Type":      "application/json",
            "properties":        "*",
            "x-method-override": "PATCH",
            "patchtype":         "MERGE_LIST",
        },
        payload, auth,
    )
    rels = _listar(href_inc, auth)
    print(f"\n  Tras V3: {len(rels)} relacion(es)  "
          f"{'✓ CREO' if any(m.get('relatedreckey')=='7708638' for m in rels) else '✗ NO creo'}")

    # ---------- V4: payload PLANO (sin envoltorio spi:relatedrecord)
    payload_plano = payload["spi:relatedrecord"][0]
    _probar(
        "V4) POST con payload PLANO (single item, sin envoltorio)",
        url,
        {"Content-Type": "application/json", "properties": "*"},
        payload_plano, auth,
    )
    rels = _listar(href_inc, auth)
    print(f"\n  Tras V4: {len(rels)} relacion(es)  "
          f"{'✓ CREO' if any(m.get('relatedreckey')=='7708638' for m in rels) else '✗ NO creo'}")

    # Estado final
    print(f"\n{'=' * 70}")
    print(f"  RESUMEN FINAL")
    print(f"{'=' * 70}")
    print(f"Estado final: {len(rels)} relacion(es)")
    for r in rels:
        print(f"  - {r.get('relatedreckey')} ({r.get('relatedrecclass')}/{r.get('relatetype')})")


if __name__ == "__main__":
    main()