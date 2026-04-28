"""
ot_transformer.py
-----------------
Logica pura de transformacion: convierte la respuesta de Maximo
(detalle OT + workorderspec) a un registro plano listo para exportar.

Sin dependencias de red, BD ni de formato de salida.
"""

from domain.campos import (
    CAMPOS_OT, SPEC_CAMPOS, CAMPOS_FECHA, CAMPOS_STR_FORZADO,
)


# ══════════════════════════════════════════════════════════════
# 1. EXTRAER SPECS (workorderspec -> dict plano)
# ══════════════════════════════════════════════════════════════

def extraer_specs(detalle):
    """
    Aplana workorderspec a {assetattrid: valor}.

    Prioriza alnvalue > tablevalue > numvalue.
    """
    return {
        s.get("assetattrid"): (
            s.get("alnvalue") or s.get("tablevalue") or s.get("numvalue") or ""
        )
        for s in detalle.get("workorderspec", [])
    }


# ══════════════════════════════════════════════════════════════
# 2. CONSTRUIR REGISTRO PLANO
# ══════════════════════════════════════════════════════════════

def construir_registro(member, detalle, ci_description="", cant_worklogs=0):
    """
    Construye un registro plano combinando el detalle de la OT,
    sus specs y la descripcion del CI.

    Parametros:
        member         (dict): item del listado (tiene cinum, href, wonum)
        detalle        (dict): respuesta completa del href
        ci_description (str):  descripcion del CI (consultada aparte)
        cant_worklogs  (int):  cantidad de avances de la OT (precomputada)

    Retorna:
        dict con exactamente CAMPOS_OT + SPEC_CAMPOS como claves.
    """
    specs = extraer_specs(detalle)
    cinum = member.get("cinum") or detalle.get("cinum") or ""

    registro = {}

    # Campos principales de CAMPOS_OT (excepto los enriquecidos/calculados aparte)
    campos_directos = [
        c for c in CAMPOS_OT
        if c not in ("cinum", "ci_description", "cant_worklogs")
    ]
    for campo in campos_directos:
        valor_raw = detalle.get(campo)

        if campo in CAMPOS_FECHA:
            # Fechas: None si vacio o no existe, sino truncar a 19 chars
            if not valor_raw:
                valor = None
            else:
                valor = valor_raw[:19]
        elif campo in CAMPOS_STR_FORZADO:
            valor = str(valor_raw or "")
        else:
            valor = valor_raw or ""

        registro[campo] = valor

    # CI enriquecido
    registro["cinum"]          = cinum
    registro["ci_description"] = ci_description

    # Worklog count (precomputado)
    registro["cant_worklogs"] = str(cant_worklogs)

    # Specs
    for campo in SPEC_CAMPOS:
        registro[campo] = str(specs.get(campo, ""))

    return registro