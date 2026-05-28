"""
10_crear_incidente.py
---------------------
Crea un incidente en Maximo dev a partir del JSON del instructivo
de Centro Gestion. Este POST genera ademas la OT asociada
automaticamente (por configuracion del lado de Maximo).

Lee los datos desde payloads/crear_incidente.json.

ATENCION: este script CREA registros reales en Maximo dev
(un incidente + una OT). El guard de _comun.py impide que corra
contra produccion, pero aun en dev deja registros que conviene
anotar para limpieza posterior.

Uso:
    python -m tests.integracion_maximo.10_crear_incidente

Tras crear, el script intenta listar las relaciones del incidente
recien creado para mostrar la OT generada (si Maximo ya la vinculo).
"""

import sys

from tests.integracion_maximo._comun import (
    guard_ambiente_dev,
    imprimir_resultado,
    cargar_payload,
)
from integrations.maximo.rest_api import (
    crear_incidente_con_ot,
    listar_tickets_relacionados,
)


def main():
    guard_ambiente_dev()

    datos = cargar_payload("crear_incidente_con_ot.json")

    # Quitar las claves de comentario (empiezan con "_") antes de enviar
    datos = {k: v for k, v in datos.items() if not k.startswith("_")}

    print("→ Creando incidente (+ OT automatica) en Maximo dev")
    print(f"  description: {datos.get('description')}")
    print(f"  cinum:       {datos.get('cinum')}")
    print(f"  ownergroup:  {datos.get('ownergroup')}")

    resultado = crear_incidente_con_ot(datos)

    imprimir_resultado("crear_incidente", resultado)

    if not resultado["success"]:
        sys.exit(1)

    ticketid = resultado.get("ticket")
    wonum    = resultado.get("wonum")

    print(f"\n✓ Incidente creado: {ticketid}")
    if wonum:
        print(f"  OT devuelta en el response: {wonum}")
    else:
        print(f"  (El response no incluyo el wonum directamente; "
              f"Maximo pudo generar la OT por separado.)")

    # Intentar ver si ya hay una OT relacionada al incidente nuevo
    if ticketid:
        print(f"\n→ Consultando relaciones del incidente {ticketid}...")
        rels = listar_tickets_relacionados(ticketid)
        if rels["success"]:
            if rels["relacionados"]:
                print(f"  Relaciones encontradas ({len(rels['relacionados'])}):")
                for i, rel in enumerate(rels["relacionados"], 1):
                    print(f"    {i}. {rel.get('relatedreckey'):>10}  "
                          f"{rel.get('relatetype'):<12}  "
                          f"{rel.get('relatedrecclass')}")
            else:
                print(f"  (Sin relaciones visibles aun. La OT pudo crearse "
                      f"sin vinculo de tipo relatedrecord, o con retardo de "
                      f"indexacion. Verificar en la UI.)")
        else:
            print(f"  No se pudieron consultar relaciones: {rels['message']}")

    print(f"\n⚠ RECORDAR: anotar ticketid={ticketid}"
          + (f" y wonum={wonum}" if wonum else "")
          + " para limpieza posterior en Maximo dev.")


if __name__ == "__main__":
    main()