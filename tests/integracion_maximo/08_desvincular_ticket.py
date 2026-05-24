"""
08_desvincular_ticket.py
------------------------
Elimina una relacion existente entre un ticket y otro registro
(otro ticket O una orden de trabajo) en Maximo dev.

IMPORTANTE: en Maximo las relaciones de un incidente pueden ser a:
  - Otros INCIDENT (tickets relacionados) - relatetype tipico: RELATED
  - WORKORDER (ordenes de trabajo)        - relatetype tipico: FOLLOWUP

Por defecto el script asume INCIDENT/RELATED (lo mas comun). Si la
relacion es a una OT, pasar 3er y 4to argumento:
    relatedrecclass = WORKORDER
    relatetype      = FOLLOWUP

Usos:
    # Defaults del payload + relacion INCIDENT/RELATED
    python -m tests.integracion_maximo.08_desvincular_ticket

    # Solo ticketids, mantiene INCIDENT/RELATED
    python -m tests.integracion_maximo.08_desvincular_ticket 6054120 6054118

    # Para desvincular una OT relacionada (WORKORDER/FOLLOWUP)
    python -m tests.integracion_maximo.08_desvincular_ticket 6054120 7708638 WORKORDER FOLLOWUP

    # Cualquier combinacion explicita
    python -m tests.integracion_maximo.08_desvincular_ticket <origen> <relacionado> <clase> <tipo>

Pre-requisito:
    La relacion debe existir. Usa primero
    07_listar_tickets_relacionados para confirmar que clase/tipo aplica.
"""

import sys

from tests.integracion_maximo._comun import (
    guard_ambiente_dev,
    imprimir_resultado,
    cargar_payload,
)
from integrations.maximo.rest_api import desvincular_ticket


def main():
    guard_ambiente_dev()

    cfg = cargar_payload("vincular_ticket.json")

    # Args opcionales: 1=origen, 2=relacionado, 3=relatedrecclass, 4=relatetype
    if len(sys.argv) >= 3:
        cfg["ticketid_origen"]      = sys.argv[1]
        cfg["ticketid_relacionado"] = sys.argv[2]
    if len(sys.argv) >= 4:
        cfg["relatedrecclass"] = sys.argv[3].upper()
    if len(sys.argv) >= 5:
        cfg["relatetype"] = sys.argv[4].upper()

    print(f"→ Desvinculando {cfg['ticketid_origen']} de "
          f"{cfg['ticketid_relacionado']} "
          f"(clase={cfg['relatedrecclass']}, tipo={cfg['relatetype']})")

    resultado = desvincular_ticket(
        ticketid_origen      = cfg["ticketid_origen"],
        ticketid_relacionado = cfg["ticketid_relacionado"],
        relatetype           = cfg["relatetype"],
        relatedrecclass      = cfg["relatedrecclass"],
        siteid               = cfg["siteid"],
    )

    imprimir_resultado("desvincular_ticket", resultado)
    sys.exit(0 if resultado["success"] else 1)


if __name__ == "__main__":
    main()