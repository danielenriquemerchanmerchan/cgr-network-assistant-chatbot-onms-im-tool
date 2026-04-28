"""
diagnosticar_ot.py
------------------
Script de diagnostico: dado un wonum especifico, trae el detalle de Maximo,
lo aplana con el transformer, y reporta el largo de cada campo comparado
contra el tipo definido en Postgres.

Detecta campos que excederian VARCHAR(N) y muestra el contenido real.

USO:
    py -m tests.diagnosticar_ot 10612566

(reemplaza 10612566 por el wonum que quieres diagnosticar)
"""

import sys
import psycopg2

from core.logging_setup import logger
from core.config import PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DATABASE
from integrations.maximo.rest_api import (
    listar_ots,
    obtener_detalle_ot,
    obtener_ci_description,
    extraer_worklogs_inline,
)
from domain.transformers.ot import construir_registro
from domain.transformers.worklog import construir_registros_worklog


def obtener_tipos_columnas():
    """
    Consulta information_schema y devuelve dict
    {column_name: (data_type, max_length)} para work_orders y worklogs.
    """
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER,
        password=PG_PASSWORD, dbname=PG_DATABASE,
    )

    tipos = {}

    with conn.cursor() as cur:
        for tabla in ["work_orders", "worklogs"]:
            cur.execute("""
                SELECT column_name, data_type, character_maximum_length
                FROM information_schema.columns
                WHERE table_schema = 'onms' AND table_name = %s
            """, (tabla,))
            for column_name, data_type, max_length in cur.fetchall():
                tipos[(tabla, column_name)] = (data_type, max_length)

    conn.close()
    return tipos


def diagnosticar_registro(registro, tipos, tabla):
    """
    Compara cada campo del registro con su tipo en Postgres.
    Reporta los que exceden.
    """
    print(f"\n{'='*80}")
    print(f"DIAGNOSTICO DE {tabla}")
    print(f"{'='*80}")
    print(f"{'campo':<30} {'tipo':<20} {'limite':>8} {'real':>8}  {'estado':<10}")
    print("-"*80)

    excedidos = []

    for campo, valor in registro.items():
        tipo_info = tipos.get((tabla, campo))
        if tipo_info is None:
            print(f"{campo:<30} {'(no esta en BD)':<20}")
            continue

        data_type, max_length = tipo_info
        valor_str = str(valor) if valor is not None else ""
        real_len = len(valor_str)

        if max_length is None:
            # TEXT u otros sin limite
            estado = "OK (sin limite)"
            print(f"{campo:<30} {data_type:<20} {'inf':>8} {real_len:>8}  {estado}")
        elif real_len > max_length:
            estado = ">>> EXCEDE <<<"
            excedidos.append((campo, max_length, real_len, valor_str))
            print(f"{campo:<30} {data_type:<20} {max_length:>8} {real_len:>8}  {estado}")
        else:
            estado = "OK"
            print(f"{campo:<30} {data_type:<20} {max_length:>8} {real_len:>8}  {estado}")

    return excedidos


def main():
    if len(sys.argv) < 2:
        print("Uso: py -m tests.diagnosticar_ot <wonum>")
        sys.exit(1)

    wonum_buscado = sys.argv[1]

    print(f"\nBuscando OT {wonum_buscado} en Maximo...")

    # Listar todas las OTs y buscar la que queremos
    todas = listar_ots(ownergroup="O_GESFO", classstructureid="4213")
    matches = [o for o in todas if o.get("wonum") == wonum_buscado]

    if not matches:
        print(f"No se encontro la OT {wonum_buscado} en O_GESFO/4213")
        sys.exit(1)

    member = matches[0]
    print(f"OT encontrada. Trayendo detalle...")

    # Detalle
    detalle = obtener_detalle_ot(member["href"])
    if not detalle:
        print("No se pudo obtener detalle")
        sys.exit(1)

    # CI description
    cinum = member.get("cinum") or ""
    ci_desc = obtener_ci_description(cinum, cache={}) if cinum else ""

    # Worklogs
    worklogs_crudos = extraer_worklogs_inline(detalle)

    # Aplanar OT
    registro_ot = construir_registro(
        member=member,
        detalle=detalle,
        ci_description=ci_desc,
        cant_worklogs=len(worklogs_crudos),
    )

    # Aplicar el mismo mapeo que hace el ETL
    mapeo = {
        "reportdate": "creation_date",
        "actstart": "actual_start",
        "targstartdate": "target_start",
        "targcompdate": "target_finish",
        "actfinish": "actual_finish",
        "lead": "assigned_to",
        "reportedby": "reported_by",
        "ownergroup": "owner_group",
        "impacto": "severity",
    }
    registro_ot_mapeado = {mapeo.get(k, k): v for k, v in registro_ot.items()}

    # Aplanar worklogs
    registros_wl = construir_registros_worklog(wonum_buscado, worklogs_crudos)

    # Tipos de columnas
    print("\nConsultando tipos de columnas en Postgres...")
    tipos = obtener_tipos_columnas()

    # Diagnosticar work_order
    excedidos_wo = diagnosticar_registro(registro_ot_mapeado, tipos, "work_orders")

    # Diagnosticar worklogs (solo el primero como muestra)
    if registros_wl:
        excedidos_wl = diagnosticar_registro(registros_wl[0], tipos, "worklogs")
    else:
        excedidos_wl = []

    # Resumen final
    print(f"\n{'='*80}")
    print("RESUMEN DE CAMPOS QUE EXCEDEN")
    print(f"{'='*80}")

    if not excedidos_wo and not excedidos_wl:
        print("Ningun campo excede su limite.")
    else:
        for campo, limite, real, valor in excedidos_wo:
            print(f"\nCampo: work_orders.{campo}")
            print(f"  Limite: VARCHAR({limite})")
            print(f"  Real:   {real} chars")
            print(f"  Valor:  '{valor}'")
        for campo, limite, real, valor in excedidos_wl:
            print(f"\nCampo: worklogs.{campo}")
            print(f"  Limite: VARCHAR({limite})")
            print(f"  Real:   {real} chars")
            print(f"  Valor:  '{valor[:200]}...'" if len(valor) > 200 else f"  Valor:  '{valor}'")


if __name__ == "__main__":
    main()