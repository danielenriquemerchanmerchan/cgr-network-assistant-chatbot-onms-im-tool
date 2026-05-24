"""
07_listar_tickets_relacionados.py
---------------------------------
Lista los tickets vinculados a un ticket dado.

Util como verificacion despues de 06_vincular_ticket.py y antes/
despues de 08_desvincular_ticket.py.

Uso:
    python -m tests.integracion_maximo.07_listar_tickets_relacionados
    python -m tests.integracion_maximo.07_listar_tickets_relacionados 6054120
"""

import sys

from tests.integracion_maximo._comun import (
    guard_ambiente_dev,
    imprimir_resultado,
    cargar_payload,
)
from integrations.maximo.rest_api import listar_tickets_relacionados


def main():
    guard_ambiente_dev()

    # Por defecto usa el ticket origen del payload de vincular
    cfg = cargar_payload("vincular_ticket.json")

    ticketid = sys.argv[1] if len(sys.argv) >= 2 else cfg["ticketid_origen"]

    print(f"→ Listando relaciones de ticket {ticketid}")

    resultado = listar_tickets_relacionados(ticketid)

    imprimir_resultado("listar_tickets_relacionados", resultado)

    if resultado["success"]:
        print(f"\nDetalle de relaciones ({len(resultado['relacionados'])}):")
        for i, rel in enumerate(resultado["relacionados"], 1):
            relkey  = rel.get("relatedreckey", "?")
            reltype = rel.get("relatetype", "?")
            relcls  = rel.get("relatedrecclass", "?")
            print(f"  {i}. {relkey:>10}  {reltype:<12}  {relcls}")

    sys.exit(0 if resultado["success"] else 1)


if __name__ == "__main__":
    main()