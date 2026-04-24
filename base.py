"""
base.py
-------
Interfaz comun para todos los exporters. Permite intercambiar destinos
(Excel, MySQL, PostgreSQL, JSON, ...) sin tocar el main.

Contrato minimo: export(registros_ots, registros_worklogs) que retorna
una descripcion del resultado (path, tabla, filas afectadas).
"""

from abc import ABC, abstractmethod


class Exporter(ABC):
    """Contrato minimo para un destino de exportacion."""

    @abstractmethod
    def export(self, registros_ots, registros_worklogs=None):
        """
        Exporta los registros al destino.

        Parametros:
            registros_ots      (list[dict]): lista de OTs planas
                    (claves: CAMPOS_OT + SPEC_CAMPOS)
            registros_worklogs (list[dict], opcional): lista de worklogs planos
                    (claves: WORKLOG_CAMPOS). Relacion con ots por 'wonum'.

        Retorna:
            str describiendo el resultado.
        """
        ... 