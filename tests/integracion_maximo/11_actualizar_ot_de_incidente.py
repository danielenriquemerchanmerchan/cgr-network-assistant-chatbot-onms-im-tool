"""
11_actualizar_ot_de_incidente.py
--------------------------------
PRUEBA DE INTEGRACION: completar la OT generada por
crear_incidente_con_ot, dejandola igual a las OTs de fibra
(Lectura B: forzar TODOS los campos).

ESTRATEGIA EN 3 FASES (una sola ejecucion):

    La OT nace heredando del incidente: classstructureid=1887 y
    ownergroup=O_GESRED. Varios campos dependen unos de otros y
    Maximo NO los procesa en el orden que uno espera dentro de un
    mismo PATCH. Por eso se separan:

    FASE A - ESTRUCTURALES (sin dependencias de persona):
        ownergroup, classstructureid, woclass, worktype, status.
        Cambia el grupo a O_GESFO y la clase a 4213.

    FASE B - DEPENDIENTES (requieren que A ya este aplicado):
        lead, cinum, location, reportedby, description, impacto,
        schedstart, actstart.
        El 'lead' se valida contra el ownergroup; por eso va DESPUES.
        schedstart/actstart se inyectan con la hora de ejecucion
        (datetime.now() en formato ISO 8601 con zona -05:00).

    FASE C - SPECS:
        Los 18 specs de fibra. Validos solo si classstructureid=4213.

ENTRADAS:
    - wonum por argumento (sys.argv[1])
    - payloads/actualizar_ot_completa.json (top_level + specs)
    - .env apuntando a DEV

PLACEHOLDERS QUE SE REEMPLAZAN EN RUNTIME:
    [TIMESTAMP] -> hora legible (para la description). "2026-05-28 09:27:31"
    [NOW_ISO]   -> hora ISO 8601 con zona (-05:00) para schedstart/actstart.
                   "2026-05-28T09:27:31-05:00"

EJECUCION:
        py -m tests.integracion_maximo.11_actualizar_ot_de_incidente 7941678
"""

import sys
from datetime import datetime
import core.logging_setup  # noqa: F401

from integrations.maximo.rest_api import actualizar_ot, consultar_ot
from tests.integracion_maximo._comun import (
    guard_ambiente_dev,
    imprimir_resultado,
    cargar_payload,
)

# Campos que van en la FASE A (estructurales, sin dependencias de persona).
CAMPOS_FASE_A = (
    "spi:ownergroup",
    "spi:classstructureid",
    "spi:woclass",
    "spi:worktype",
    "spi:status",
)


def resolver_href_y_clase(wonum):
    """
    Consulta la OT y retorna (href, classstructureid, ownergroup).
    Retorna (None, None, None) si no se encuentra.
    """
    info = consultar_ot(wonum)
    if not info:
        return None, None, None
    raw = info.get("raw", {})
    return raw.get("href"), raw.get("classstructureid"), raw.get("ownergroup")


def _reemplazar_placeholders(campos):
    """
    Reemplaza placeholders de fecha/hora en los valores string:
        [TIMESTAMP] -> "YYYY-MM-DD HH:MM:SS"
        [NOW_ISO]   -> "YYYY-MM-DDTHH:MM:SS-05:00"

    Ambos usan el MISMO instante (datetime.now()) para que la OT quede
    coherente: description, schedstart y actstart con la misma hora.
    """
    ahora      = datetime.now()
    ts_legible = ahora.strftime("%Y-%m-%d %H:%M:%S")
    ts_iso     = ahora.strftime("%Y-%m-%dT%H:%M:%S-05:00")

    resultado = {}
    for k, v in campos.items():
        if isinstance(v, str):
            v = v.replace("[TIMESTAMP]", ts_legible)
            v = v.replace("[NOW_ISO]", ts_iso)
        resultado[k] = v
    return resultado


def _imprimir_campos(titulo, campos):
    print(f"  {titulo} ({len(campos)} campos):")
    for k, v in campos.items():
        print(f"    {k:25s} = {v}")
    print()


def main():
    print("=" * 70)
    print("PRUEBA 11: ACTUALIZAR OT COMPLETA (3 fases: estructural / dependiente / specs)")
    print("=" * 70)

    guard_ambiente_dev()

    if len(sys.argv) < 2:
        print("ERROR: falta el wonum como argumento.")
        print("Uso: py -m tests.integracion_maximo.11_actualizar_ot_de_incidente <wonum>")
        sys.exit(1)

    wonum = sys.argv[1].strip()
    print(f"OT objetivo: {wonum}")
    print()

    # ── Estado inicial ────────────────────────────────────────────
    print("Resolviendo href, clasificacion y grupo actuales...")
    href, clase_ini, grupo_ini = resolver_href_y_clase(wonum)
    if not href:
        print(f"ERROR: no se pudo obtener el href de la OT {wonum}.")
        sys.exit(1)
    print(f"href:             {href}")
    print(f"classstructureid: {clase_ini}")
    print(f"ownergroup:       {grupo_ini}")
    print()

    # ── Cargar payload y separar en fases ─────────────────────────
    payload = cargar_payload("actualizar_ot_completa.json")
    payload = {k: v for k, v in payload.items() if not k.startswith("_")}

    top_level = dict(payload.get("top_level", {}))
    specs     = payload.get("specs", [])

    # Reemplazar placeholders de fecha/hora en todo el top_level
    # (cubre [TIMESTAMP] en description y [NOW_ISO] en schedstart/actstart)
    top_level = _reemplazar_placeholders(top_level)

    # Particionar top_level en fase A (estructurales) y fase B (resto)
    fase_a = {k: v for k, v in top_level.items() if k in CAMPOS_FASE_A}
    fase_b = {k: v for k, v in top_level.items() if k not in CAMPOS_FASE_A}

    clase_objetivo = fase_a.get("spi:classstructureid", "(no especificada)")
    grupo_objetivo = fase_a.get("spi:ownergroup", "(no especificado)")

    # ══════════════════════════════════════════════════════════════
    # FASE A: ESTRUCTURALES
    # ══════════════════════════════════════════════════════════════
    print("=" * 70)
    print("FASE A: campos ESTRUCTURALES")
    print(f"        classstructureid: {clase_ini} -> {clase_objetivo}")
    print(f"        ownergroup:       {grupo_ini} -> {grupo_objetivo}")
    print("=" * 70)
    _imprimir_campos("Enviando", fase_a)

    rA = actualizar_ot(href, fase_a)
    imprimir_resultado("RESULTADO Fase A", rA)
    if not rA.get("success"):
        print("✗ Fase A fallo. No se continua.")
        sys.exit(1)

    # Verificacion intermedia
    print("\nVerificando cambios estructurales...")
    _, clase_post, grupo_post = resolver_href_y_clase(wonum)
    print(f"  classstructureid ahora: {clase_post}")
    print(f"  ownergroup ahora:       {grupo_post}")
    ok_clase = str(clase_post) == str(clase_objetivo)
    ok_grupo = str(grupo_post) == str(grupo_objetivo)
    if not ok_clase:
        print(f"  ⚠ La clasificacion NO cambio a {clase_objetivo}.")
    if not ok_grupo:
        print(f"  ⚠ El ownergroup NO cambio a {grupo_objetivo}.")
    if ok_clase and ok_grupo:
        print(f"  ✓ Estructura confirmada.")
    print()

    # ══════════════════════════════════════════════════════════════
    # FASE B: DEPENDIENTES
    # ══════════════════════════════════════════════════════════════
    print("=" * 70)
    print("FASE B: campos DEPENDIENTES (lead, cinum, location, schedstart, actstart, etc.)")
    print("=" * 70)
    _imprimir_campos("Enviando", fase_b)

    rB = actualizar_ot(href, fase_b)
    imprimir_resultado("RESULTADO Fase B", rB)
    if not rB.get("success"):
        print("✗ Fase B fallo. No se intenta Fase C (specs).")
        sys.exit(1)

    # ══════════════════════════════════════════════════════════════
    # FASE C: SPECS
    # ══════════════════════════════════════════════════════════════
    print("=" * 70)
    print(f"FASE C: especificaciones ({len(specs)} specs)")
    print("=" * 70)
    for spec in specs:
        attr  = spec.get("spi:assetattrid", "")
        valor = (spec.get("spi:alnvalue")
                 or spec.get("spi:tablevalue")
                 or spec.get("spi:numvalue")
                 or "(sin valor)")
        print(f"  {attr:30s} = {valor}")
    print()

    rC = actualizar_ot(href, {"spi:workorderspec": specs})
    imprimir_resultado("RESULTADO Fase C (specs)", rC)

    # ── Cierre ────────────────────────────────────────────────────
    print("=" * 70)
    if rA.get("success") and rB.get("success") and rC.get("success"):
        print("LAS 3 FASES EJECUTADAS (HTTP OK)")
        print("=" * 70)
        print(f"VERIFICAR EN LA UI la OT {wonum}:")
        print(f"  1. classstructureid = {clase_objetivo}, ownergroup = {grupo_objetivo}")
        print(f"  2. lead, cinum, location, schedstart, actstart con los valores forzados")
        print(f"  3. Los {len(specs)} specs de fibra en Especificaciones")
        print(f"     (NO los viejos de 1887 como ASIGNACION_CORRECTA)")
        print(f"\n  RECORDAR: success=true = HTTP 200. La UI es la verdad,")
        print(f"  Maximo puede aceptar el PATCH e ignorar campos en silencio.")
    else:
        print("ALGUNA FASE FALLO. Revisar resultados arriba.")
    print("=" * 70)


if __name__ == "__main__":
    main()