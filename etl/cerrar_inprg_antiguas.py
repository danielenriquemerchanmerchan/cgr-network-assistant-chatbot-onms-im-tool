"""
cerrar_inprg_antiguas.py
------------------------
Cierra OTs en estado INPRG con mas de N dias de antiguedad, sin importar
el tecnico asignado (WORKORDER.LEAD).

CONTEXTO:
    Limpieza de OTs antiguas en O_GESFO que han quedado abiertas en INPRG
    pero que en la practica deberian estar cerradas. Como el universo es
    grande y heterogeneo (multiples tecnicos asignados), las cerramos
    directamente via API en lugar de coordinar caso por caso.

FLUJO DE ESTADOS EN MAXIMO:
    INPRG → COMP → CLOSE

    No se puede saltar directo de INPRG a CLOSE. Por eso el script aplica
    los DOS PATCH en secuencia (con pausa entre ellos para que Maximo
    refresque el statusdate).

UNIVERSO QUE PROCESA (default):
    - status = INPRG
    - dias_abierta > DIAS_MINIMOS              ← default 90, configurable
    - sin filtro de tecnico                    ← TODAS las OTs candidatas
    - ownergroup = O_GESFO (implicito por la fuente de datos)

OPCIONALMENTE:
    --tecnicos T1 T2 ...    restringe el universo a esos tecnicos
    --excluir-tecnicos T1 T2 ...   excluye esos tecnicos del universo

FUENTE DE LA LISTA:
    Postgres (schema onms.work_orders). Se asume ETL razonablemente fresco.
    Si quieres datos al minuto, corre antes el ETL operativo.

MODOS DE EJECUCION:
    1. Por defecto: DRY-RUN. Lista las OTs candidatas y las exporta a Excel,
       NO toca Maximo. Sirve para revisar antes de ejecutar.
    2. Con --ejecutar: realmente hace los PATCH a Maximo (INPRG→COMP→CLOSE).
    3. Con --solo-comp: solo lleva a COMP y se detiene (mas conservador).

USO:
    # 1. Dry-run (SIEMPRE primero) - solo lista a Excel
    py -m etl.cerrar_inprg_antiguas

    # 2. Probar con un puñado primero antes del lote completo
    py -m etl.cerrar_inprg_antiguas --ejecutar --limit 3

    # 3. Ejecutar el cierre real (todas las candidatas)
    py -m etl.cerrar_inprg_antiguas --ejecutar

    # 4. Solo dejar en COMP (sin CLOSE) - mas reversible
    py -m etl.cerrar_inprg_antiguas --ejecutar --solo-comp

    # 5. Cambiar el umbral de dias
    py -m etl.cerrar_inprg_antiguas --dias 60

    # 6. Restringir a tecnicos especificos
    py -m etl.cerrar_inprg_antiguas --tecnicos LUIS.RODRIGUEZG NESTOR.RAMIREZ

    # 7. Excluir tecnicos (ej: dejar tranquilas las de LGMELENDEZHE)
    py -m etl.cerrar_inprg_antiguas --excluir-tecnicos LGMELENDEZHE

SALIDA:
    output/Cierre_INPRG_Antiguas_{MODO}_{YYYYMMDD_HHMMSS}.xlsx
        Hoja 1 (Candidatas):  lista detallada de OTs a procesar
        Hoja 2 (Resultado):   resultado del cierre por OT (solo si --ejecutar)
"""

import argparse
import logging
import time
from datetime import datetime
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from psycopg2.extras import RealDictCursor

from core.logging_setup import logger
from integrations.postgres.client import obtener_conexion, cerrar_conexion
from integrations.maximo.rest_api import cambiar_estado, _obtener_href, _cerrar_sesion


# ════════════════════════════════════════════════════════════════════
# CONFIGURACION POR DEFECTO
# ════════════════════════════════════════════════════════════════════

DIAS_MINIMOS_DEFAULT = 90

# Por defecto NO se filtra por tecnico: el universo es TODAS las OTs INPRG
# antiguas. Se puede restringir con --tecnicos o excluir con --excluir-tecnicos.

# Pausa entre PATCH INPRG→COMP y COMP→CLOSE. Maximo a veces necesita
# unos segundos para refrescar el statusdate antes del siguiente cambio.
PAUSA_ENTRE_PATCH_SEG = 2

# Pausa entre OTs (para no saturar Maximo)
PAUSA_ENTRE_OTS_SEG = 1


# ════════════════════════════════════════════════════════════════════
# QUERY: OTs candidatas
# ════════════════════════════════════════════════════════════════════
# El filtro por tecnico es opcional. Si no se pasan tecnicos, se procesan
# TODAS las OTs INPRG con mas de N dias. Si se pasan via --tecnicos o
# --excluir-tecnicos, se aplica el filtro correspondiente.

QUERY_BASE = """
SELECT
    wo.wonum,
    wo.cinum,
    wo.ci_description,
    wo.description AS resumen,
    wo.assigned_to AS tecnico,
    wo.coordinador_red_fo AS coordinador,
    wo.lider_de_zona_fo AS lider_zona,
    wo.location AS codigo_sitio,
    wo.nom_ubicacion AS nombre_sitio,
    wo.creation_date AS fecha_creacion,
    EXTRACT(EPOCH FROM (NOW() - wo.creation_date)) / 86400 AS dias_abierta,
    wo.status,
    wo.worktype,
    wo.clasificacion_operativa,
    wo.numero_caso_fo AS numero_caso,
    wo.eecc_cuadrilla_fo AS eecc,
    wo.cant_worklogs
FROM onms.work_orders wo
WHERE wo.activa = true
  AND wo.status = 'INPRG'
  AND EXTRACT(EPOCH FROM (NOW() - wo.creation_date)) / 86400 > %(dias_minimos)s
  {filtro_tecnicos}
ORDER BY wo.creation_date ASC
"""


def construir_query(tecnicos_incluir, tecnicos_excluir):
    """
    Construye el query final inyectando el filtro de tecnicos segun corresponda.
    Devuelve (sql, params).
    """
    params = {"dias_minimos": None}  # se llena despues
    filtro = ""

    if tecnicos_incluir:
        filtro = "AND wo.assigned_to = ANY(%(tecnicos_incluir)s)"
        params["tecnicos_incluir"] = tecnicos_incluir
    elif tecnicos_excluir:
        # IMPORTANTE: usar COALESCE para que las OTs sin tecnico (NULL) no
        # se filtren accidentalmente, ya que NULL != ANY siempre da NULL.
        filtro = "AND COALESCE(wo.assigned_to, '') <> ALL(%(tecnicos_excluir)s)"
        params["tecnicos_excluir"] = tecnicos_excluir

    return QUERY_BASE.format(filtro_tecnicos=filtro), params


# Definicion de columnas de la hoja Candidatas
COLUMNAS_CANDIDATAS = [
    ("wonum",                    "WONUM",                    14),
    ("dias_abierta",             "Dias Abierta",             12),
    ("status",                   "Status",                   10),
    ("worktype",                 "Worktype",                 10),
    ("clasificacion_operativa",  "Clasificacion",            18),
    ("tecnico",                  "Tecnico Asignado (lead)",  22),
    ("fecha_creacion",           "Fecha Creacion",           18),
    ("resumen",                  "Resumen",                  50),
    ("coordinador",              "Coordinador",              22),
    ("lider_zona",               "Lider Zona",               18),
    ("codigo_sitio",             "Cod. Sitio",               12),
    ("nombre_sitio",             "Nombre Sitio",             30),
    ("eecc",                     "EECC",                     18),
    ("numero_caso",              "Numero Caso",              16),
    ("cinum",                    "CI",                       30),
    ("ci_description",           "CI Descripcion",           40),
    ("cant_worklogs",            "# Worklogs",               10),
]

# Definicion de columnas de la hoja Resultado
COLUMNAS_RESULTADO = [
    ("wonum",            "WONUM",                14),
    ("tecnico",          "Tecnico (lead)",       22),
    ("dias_abierta",     "Dias Abierta",         12),
    ("paso_comp_ok",     "INPRG→COMP OK",        14),
    ("paso_comp_msg",    "INPRG→COMP Mensaje",   40),
    ("paso_close_ok",    "COMP→CLOSE OK",        14),
    ("paso_close_msg",   "COMP→CLOSE Mensaje",   40),
    ("estado_final",     "Estado Final",         14),
    ("error",            "Error",                40),
]


# ════════════════════════════════════════════════════════════════════
# ESTILOS DE EXCEL
# ════════════════════════════════════════════════════════════════════

HEADER_FILL = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
HEADER_FONT = Font(name="Calibri", size=11, bold=True, color="FFFFFF")
HEADER_ALIGN = Alignment(horizontal="center", vertical="center", wrap_text=True)
CELL_ALIGN = Alignment(horizontal="left", vertical="top", wrap_text=True)

THIN_BORDER = Border(
    left=Side(style="thin", color="CCCCCC"),
    right=Side(style="thin", color="CCCCCC"),
    top=Side(style="thin", color="CCCCCC"),
    bottom=Side(style="thin", color="CCCCCC"),
)

FILL_OK    = PatternFill(start_color="E2EFDA", end_color="E2EFDA", fill_type="solid")  # verde
FILL_ERROR = PatternFill(start_color="F8CBAD", end_color="F8CBAD", fill_type="solid")  # rosa
FILL_PARCIAL = PatternFill(start_color="FFE699", end_color="FFE699", fill_type="solid")  # amarillo


# ════════════════════════════════════════════════════════════════════
# CIERRE DE UNA OT (INPRG → COMP → CLOSE)
# ════════════════════════════════════════════════════════════════════

def cerrar_ot_dos_pasos(wonum, solo_comp=False):
    """
    Lleva una OT de INPRG a CLOSE en dos PATCH:
        Paso 1: INPRG → COMP
        Paso 2: COMP → CLOSE  (omitido si solo_comp=True)

    Retorna dict con resultado de ambos pasos.
    """
    resultado = {
        "wonum":          wonum,
        "paso_comp_ok":   False,
        "paso_comp_msg":  "",
        "paso_close_ok":  False,
        "paso_close_msg": "",
        "estado_final":   "INPRG",
        "error":          "",
    }

    # PASO 1: INPRG → COMP
    try:
        r1 = cambiar_estado(wonum, "COMP")
        resultado["paso_comp_ok"]  = r1.get("success", False)
        resultado["paso_comp_msg"] = r1.get("message", "")
        if not resultado["paso_comp_ok"]:
            resultado["error"] = f"Fallo INPRG→COMP: {r1.get('message')}"
            return resultado
        resultado["estado_final"] = "COMP"
    except Exception as e:
        resultado["error"] = f"Excepcion en INPRG→COMP: {e}"
        return resultado

    if solo_comp:
        return resultado

    # PASO 2: COMP → CLOSE (con pausa)
    time.sleep(PAUSA_ENTRE_PATCH_SEG)

    try:
        r2 = cambiar_estado(wonum, "CLOSE")
        resultado["paso_close_ok"]  = r2.get("success", False)
        resultado["paso_close_msg"] = r2.get("message", "")
        if resultado["paso_close_ok"]:
            resultado["estado_final"] = "CLOSE"
        else:
            resultado["error"] = f"Fallo COMP→CLOSE: {r2.get('message')}"
    except Exception as e:
        resultado["error"] = f"Excepcion en COMP→CLOSE: {e}"

    return resultado


# ════════════════════════════════════════════════════════════════════
# CONSTRUCCION DE EXCEL
# ════════════════════════════════════════════════════════════════════

def aplicar_header(ws, columnas):
    for col_idx, (_, header, ancho) in enumerate(columnas, start=1):
        cell = ws.cell(row=1, column=col_idx, value=header)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = HEADER_ALIGN
        cell.border = THIN_BORDER
        ws.column_dimensions[get_column_letter(col_idx)].width = ancho
    ws.row_dimensions[1].height = 35
    ws.freeze_panes = "B2"


def construir_hoja_candidatas(ws, filas):
    aplicar_header(ws, COLUMNAS_CANDIDATAS)
    for row_idx, fila in enumerate(filas, start=2):
        for col_idx, (key, _, _) in enumerate(COLUMNAS_CANDIDATAS, start=1):
            valor = fila.get(key)
            if key == "dias_abierta" and valor is not None:
                valor = round(float(valor), 1)
            elif key == "fecha_creacion" and valor is not None:
                valor = valor.strftime("%Y-%m-%d %H:%M") if hasattr(valor, "strftime") else str(valor)
            cell = ws.cell(row=row_idx, column=col_idx, value=valor)
            cell.alignment = CELL_ALIGN
            cell.border = THIN_BORDER
    if filas:
        last_col = get_column_letter(len(COLUMNAS_CANDIDATAS))
        ws.auto_filter.ref = f"A1:{last_col}{len(filas) + 1}"


def construir_hoja_resultado(ws, resultados, solo_comp):
    aplicar_header(ws, COLUMNAS_RESULTADO)
    for row_idx, fila in enumerate(resultados, start=2):
        # Fill segun resultado
        if solo_comp:
            ok = fila.get("paso_comp_ok")
        else:
            ok = fila.get("paso_comp_ok") and fila.get("paso_close_ok")
        parcial = fila.get("paso_comp_ok") and not fila.get("paso_close_ok") and not solo_comp

        if ok:
            fill = FILL_OK
        elif parcial:
            fill = FILL_PARCIAL
        else:
            fill = FILL_ERROR

        for col_idx, (key, _, _) in enumerate(COLUMNAS_RESULTADO, start=1):
            valor = fila.get(key)
            if isinstance(valor, bool):
                valor = "SI" if valor else "NO"
            elif key == "dias_abierta" and valor is not None:
                valor = round(float(valor), 1)
            cell = ws.cell(row=row_idx, column=col_idx, value=valor)
            cell.alignment = CELL_ALIGN
            cell.border = THIN_BORDER
            cell.fill = fill
    if resultados:
        last_col = get_column_letter(len(COLUMNAS_RESULTADO))
        ws.auto_filter.ref = f"A1:{last_col}{len(resultados) + 1}"


# ════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Cierra OTs INPRG antiguas en Maximo (INPRG → COMP → CLOSE)."
    )
    parser.add_argument(
        "--ejecutar",
        action="store_true",
        help="Realmente ejecuta el cierre en Maximo. Sin esta flag, solo dry-run.",
    )
    parser.add_argument(
        "--solo-comp",
        action="store_true",
        help="Solo lleva las OTs a COMP, no a CLOSE. Mas conservador.",
    )
    parser.add_argument(
        "--dias",
        type=int,
        default=DIAS_MINIMOS_DEFAULT,
        help=f"Dias minimos de antiguedad (default {DIAS_MINIMOS_DEFAULT}).",
    )
    parser.add_argument(
        "--tecnicos",
        nargs="+",
        default=None,
        help="Restringir el universo a estos tecnicos. Si se omite, procesa TODOS.",
    )
    parser.add_argument(
        "--excluir-tecnicos",
        nargs="+",
        default=None,
        dest="excluir_tecnicos",
        help="Excluir estos tecnicos del universo. No combinable con --tecnicos.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limite de OTs a procesar (util para pruebas controladas).",
    )
    args = parser.parse_args()

    # Validacion: las dos flags de tecnicos son mutuamente excluyentes
    if args.tecnicos and args.excluir_tecnicos:
        parser.error("--tecnicos y --excluir-tecnicos son mutuamente excluyentes.")

    # Resumen del filtro de tecnicos para imprimir
    if args.tecnicos:
        filtro_tec = f"SOLO {args.tecnicos}"
    elif args.excluir_tecnicos:
        filtro_tec = f"TODOS excepto {args.excluir_tecnicos}"
    else:
        filtro_tec = "TODOS los tecnicos (sin filtro)"

    print("=" * 75)
    print("CIERRE DE OTs INPRG ANTIGUAS")
    print("=" * 75)
    print(f"  Modo:        {'EJECUCION REAL' if args.ejecutar else 'DRY-RUN (sin tocar Maximo)'}")
    print(f"  Estado meta: {'COMP' if args.solo_comp else 'CLOSE'}")
    print(f"  Dias min:    {args.dias}")
    print(f"  Tecnicos:    {filtro_tec}")
    print(f"  Limite:      {args.limit if args.limit else 'sin limite'}")
    print("=" * 75)

    # 1. Conectar a Postgres y obtener candidatas
    conn = obtener_conexion()
    if conn is None:
        print("[ERROR] No se pudo conectar a Postgres")
        return

    try:
        print("\n[1/3] Consultando candidatas en Postgres...")

        sql, params = construir_query(args.tecnicos, args.excluir_tecnicos)
        params["dias_minimos"] = args.dias

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            candidatas = [dict(r) for r in cur.fetchall()]

        if args.limit:
            candidatas = candidatas[: args.limit]

        print(f"      Candidatas encontradas: {len(candidatas)}")

        if not candidatas:
            print("\n[INFO] No hay OTs candidatas. Nada que hacer.")
            return

        # Resumen rapido por tecnico
        from collections import Counter
        por_tecnico = Counter((c["tecnico"] or "(sin tecnico)") for c in candidatas)
        print(f"      Distribucion por tecnico (top 15):")
        for tec, cant in por_tecnico.most_common(15):
            print(f"        {tec:<25} {cant:>5}")
        if len(por_tecnico) > 15:
            print(f"        ... y {len(por_tecnico) - 15} tecnicos mas")

    except Exception as e:
        print(f"[ERROR] Consulta Postgres fallo: {e}")
        import traceback; traceback.print_exc()
        cerrar_conexion(conn)
        return

    finally:
        cerrar_conexion(conn)

    # 2. Procesar (si --ejecutar)
    resultados = []
    if args.ejecutar:
        print(f"\n[2/3] Procesando {len(candidatas)} OTs en Maximo...")
        print(f"      (pausa de {PAUSA_ENTRE_PATCH_SEG}s entre PATCH y {PAUSA_ENTRE_OTS_SEG}s entre OTs)")

        for i, c in enumerate(candidatas, 1):
            wonum   = c["wonum"]
            tecnico = c["tecnico"]
            dias    = c["dias_abierta"]

            print(f"  [{i}/{len(candidatas)}] OT {wonum} (tec={tecnico}, dias={float(dias):.1f}) ... ", end="", flush=True)

            res = cerrar_ot_dos_pasos(wonum, solo_comp=args.solo_comp)
            res["tecnico"]      = tecnico
            res["dias_abierta"] = dias
            resultados.append(res)

            # Estado de la linea
            if args.solo_comp:
                if res["paso_comp_ok"]:
                    print(f"OK → {res['estado_final']}")
                else:
                    print(f"FAIL ({res['paso_comp_msg']})")
            else:
                if res["paso_comp_ok"] and res["paso_close_ok"]:
                    print(f"OK → CLOSE")
                elif res["paso_comp_ok"]:
                    print(f"PARCIAL → COMP (CLOSE fallo: {res['paso_close_msg']})")
                else:
                    print(f"FAIL ({res['paso_comp_msg']})")

            # Pausa entre OTs
            if i < len(candidatas):
                time.sleep(PAUSA_ENTRE_OTS_SEG)
    else:
        print("\n[2/3] DRY-RUN: omitiendo ejecucion en Maximo.")

    # 3. Generar Excel
    print(f"\n[3/3] Generando Excel...")
    wb = Workbook()

    ws_cand = wb.active
    ws_cand.title = "Candidatas"
    construir_hoja_candidatas(ws_cand, candidatas)

    if resultados:
        ws_res = wb.create_sheet(title="Resultado")
        construir_hoja_resultado(ws_res, resultados, args.solo_comp)

    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    sufijo = "EJECUCION" if args.ejecutar else "DRYRUN"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = output_dir / f"Cierre_INPRG_Antiguas_{sufijo}_{timestamp}.xlsx"
    wb.save(filename)

    # Resumen final
    print(f"\n{'=' * 75}")
    print(f"RESUMEN")
    print(f"{'=' * 75}")
    print(f"  Candidatas encontradas: {len(candidatas)}")
    if resultados:
        ok_total = sum(
            1 for r in resultados
            if (r["paso_comp_ok"] and r["paso_close_ok"]) or
               (args.solo_comp and r["paso_comp_ok"])
        )
        parciales = sum(
            1 for r in resultados
            if r["paso_comp_ok"] and not r["paso_close_ok"] and not args.solo_comp
        )
        fallos = len(resultados) - ok_total - parciales
        print(f"  OK total:               {ok_total}")
        if not args.solo_comp:
            print(f"  Parciales (COMP solo):  {parciales}")
        print(f"  Fallos:                 {fallos}")
    print(f"  Excel:                  {filename}")
    print(f"{'=' * 75}")


if __name__ == "__main__":
    main()