"""
06_vincular_ticket.py
---------------------
Vincula dos registros en Maximo dev. Por defecto vincula dos INCIDENT
con relatetype=RELATED. Acepta argumentos opcionales para vincular
incidentes con OTs (WORKORDER/FOLLOWUP).

Usos:
    # Defaults del payload (INCIDENT/RELATED)
    python -m tests.integracion_maximo.06_vincular_ticket

    # Solo ticketids, mantiene clase/tipo del payload
    python -m tests.integracion_maximo.06_vincular_ticket 6054120 6054118

    # Vincular una OT (WORKORDER/FOLLOWUP)
    python -m tests.integracion_maximo.06_vincular_ticket 6054120 7708638 WORKORDER FOLLOWUP

    # Cualquier combinacion explicita
    python -m tests.integracion_maximo.06_vincular_ticket <origen> <relacionado> <clase> <tipo>
"""

import sys

from tests.integracion_maximo._comun import (
    guard_ambiente_dev,
    imprimir_resultado,
    cargar_payload,
)
from integrations.maximo.rest_api import vincular_ticket


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

    print(f"→ Vinculando {cfg['ticketid_origen']} con "
          f"{cfg['ticketid_relacionado']} "
          f"(clase={cfg['relatedrecclass']}, tipo={cfg['relatetype']})")

    resultado = vincular_ticket(
        ticketid_origen      = cfg["ticketid_origen"],
        ticketid_relacionado = cfg["ticketid_relacionado"],
        relatetype           = cfg["relatetype"],
        relatedrecclass      = cfg["relatedrecclass"],
        siteid               = cfg["siteid"],
        isglobal             = cfg["isglobal"],
        notificar            = cfg["notificar"],
        notifica_interesados = cfg["notifica_interesados"],
        aprobcomite          = cfg["aprobcomite"],
        presentacomite       = cfg["presentacomite"],
    )

    imprimir_resultado("vincular_ticket", resultado)
    sys.exit(0 if resultado["success"] else 1)


if __name__ == "__main__":
    main()