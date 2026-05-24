"""
09_ciclo_completo_relaciones.py
-------------------------------
End-to-end de la familia restincrel:

    Paso 1: Listar relaciones actuales (snapshot inicial)
    Paso 2: Vincular ticket origen con relacionado
    Paso 3: Listar relaciones (debe aparecer la nueva)
    Paso 4: Desvincular el ticket vinculado en el paso 2
    Paso 5: Listar relaciones (debe NO aparecer ya)

Util como smoke test rapido tras cambios en rest_api.py o config.

Uso:
    python -m tests.integracion_maximo.09_ciclo_completo_relaciones
    python -m tests.integracion_maximo.09_ciclo_completo_relaciones 6054120 6054118
"""

import sys

from tests.integracion_maximo._comun import (
    guard_ambiente_dev,
    imprimir_resultado,
    cargar_payload,
)
from integrations.maximo.rest_api import (
    vincular_ticket,
    desvincular_ticket,
    listar_tickets_relacionados,
)


def _imprimir_relaciones(etiqueta, resultado):
    print(f"\n--- {etiqueta} ---")
    if not resultado["success"]:
        print(f"  ERROR: {resultado['message']}")
        return
    rels = resultado["relacionados"]
    if not rels:
        print("  (sin relaciones)")
        return
    for i, rel in enumerate(rels, 1):
        relkey  = rel.get("relatedreckey", "?")
        reltype = rel.get("relatetype", "?")
        relcls  = rel.get("relatedrecclass", "?")
        print(f"  {i}. {relkey:>10}  {reltype:<12}  {relcls}")


def main():
    guard_ambiente_dev()

    cfg = cargar_payload("vincular_ticket.json")

    if len(sys.argv) >= 3:
        cfg["ticketid_origen"]      = sys.argv[1]
        cfg["ticketid_relacionado"] = sys.argv[2]

    origen      = cfg["ticketid_origen"]
    relacionado = cfg["ticketid_relacionado"]

    print(f"════════════════════════════════════════════════════════════")
    print(f"  Ciclo completo de relaciones:  {origen}  ↔  {relacionado}")
    print(f"════════════════════════════════════════════════════════════")

    # Paso 1
    print("\n[1/5] Snapshot inicial...")
    snap_inicial = listar_tickets_relacionados(origen)
    _imprimir_relaciones("ANTES", snap_inicial)

    # Paso 2
    print("\n[2/5] Vinculando...")
    r_vincular = vincular_ticket(
        ticketid_origen=origen,
        ticketid_relacionado=relacionado,
        relatetype=cfg["relatetype"],
        relatedrecclass=cfg["relatedrecclass"],
        siteid=cfg["siteid"],
        isglobal=cfg["isglobal"],
        notificar=cfg["notificar"],
        notifica_interesados=cfg["notifica_interesados"],
        aprobcomite=cfg["aprobcomite"],
        presentacomite=cfg["presentacomite"],
    )
    imprimir_resultado("vincular", r_vincular)
    if not r_vincular["success"]:
        print("\n✗ Fallo el vinculo. Abortando ciclo.")
        sys.exit(1)

    # Paso 3
    print("\n[3/5] Verificando que aparezca la nueva relacion...")
    snap_post_vincular = listar_tickets_relacionados(origen)
    _imprimir_relaciones("DESPUES DE VINCULAR", snap_post_vincular)

    aparece = any(
        rel.get("relatedreckey") == relacionado
        for rel in snap_post_vincular.get("relacionados", [])
    )
    if not aparece:
        print(f"\n⚠ La relacion no aparece en el listado tras vincular. "
              f"Puede ser un retardo de indexacion de Maximo. Revisar UI.")

    # Paso 4
    print("\n[4/5] Desvinculando...")
    r_desvincular = desvincular_ticket(
        ticketid_origen=origen,
        ticketid_relacionado=relacionado,
        relatetype=cfg["relatetype"],
        relatedrecclass=cfg["relatedrecclass"],
        siteid=cfg["siteid"],
    )
    imprimir_resultado("desvincular", r_desvincular)
    if not r_desvincular["success"]:
        print("\n✗ Fallo el desvinculo. La relacion quedo creada en Maximo, "
              "limpiar manualmente si es necesario.")
        sys.exit(1)

    # Paso 5
    print("\n[5/5] Verificando que la relacion ya no este...")
    snap_final = listar_tickets_relacionados(origen)
    _imprimir_relaciones("DESPUES DE DESVINCULAR", snap_final)

    sigue = any(
        rel.get("relatedreckey") == relacionado
        for rel in snap_final.get("relacionados", [])
    )
    if sigue:
        print(f"\n⚠ La relacion AUN aparece. Verificar Maximo.")
        sys.exit(2)

    print("\n✓ Ciclo completo exitoso.")
    sys.exit(0)


if __name__ == "__main__":
    main()