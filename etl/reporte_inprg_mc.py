"""
control_inprg.py
----------------
Genera un reporte Excel con TODAS las OTs MC en estado INPRG de O_GESFO,
trayendo los datos directamente desde Maximo (no desde Postgres).

DIFERENCIA CON reporte_inprg_mc.py:
    - reporte_inprg_mc.py: lee de Postgres (solo las INPRG <90d del modelo)
    - control_inprg.py:    lee de Maximo (TODAS las INPRG, incluyendo huerfanas)

PROPOSITO:
    Visibilidad total sobre las INPRG, incluyendo las huerfanas que
    estan abandonadas hace mas de 90 dias y no entran al tablero
    operativo. Util para limpieza administrativa.

TIEMPO:
    ~25-30 minutos. Cada OT requiere una llamada HTTP a Maximo para
    traer detalle + worklogs. No hay atajos.

EJECUCION:
    py -m etl.control_inprg

SALIDA:
    output/Reporte_INPRG_TODOS_{YYYYMMDD_HHMMSS}.xlsx
"""

import time
import logging
from datetime import datetime
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

from core.logging_setup import logger

from integrations.maximo.rest_api import (
    listar_ots,
    obtener_detalle_ot,
    obtener_ci_description,
    extraer_worklogs_inline,
)
from integrations.maximo.oracle import (
    cargar_cache_sitios,
    aplicar_sitio_a_registro,
)
from domain.transformers.ot import construir_registro


# ════════════════════════════════════════════════════════════════════
# CONFIGURACION
# ════════════════════════════════════════════════════════════════════

OWNERGROUP = "O_GESFO"
CLASSSTRUCTUREID = "4213"
WORKTYPE = "MC"


# Definicion de columnas de la hoja principal
# (igual a reporte_inprg_mc.py)
COLUMNAS_DETALLE = [
    ("wonum",                    "WONUM",                    14),
    ("dias_abierta",             "Dias Abierta",             12),
    ("dias_sin_avance",          "Dias Sin Avance",          12),
    ("fecha_creacion",           "Fecha Creacion",           18),
    ("resumen",                  "Resumen",                  50),
    ("tecnico",                  "Tecnico Asignado",         20),
    ("coordinador",              "Coordinador Red FO",       22),
    ("lider_zona",               "Lider Zona",               18),
    ("codigo_sitio",             "Cod. Sitio",               12),
    ("nombre_sitio",             "Nombre Sitio",             30),
    ("direccion",                "Direccion",                30),
    ("ciudad",                   "Ciudad",                   18),
    ("departamento",             "Departamento",             18),
    ("tipo_tramo",               "Tipo Tramo",               12),
    ("tipo_operacion_fo",        "Tipo Operacion",           14),
    ("operador",                 "Operador",                 22),
    ("eecc",                     "EECC Cuadrilla",           18),
    ("tipo_cuadrilla_fo",        "Tipo Cuadrilla",           20),
    ("numero_caso",              "Numero Caso",              16),
    ("outage_asociado",          "Outage",                   12),
    ("area_reporta",             "Area Reporta",             14),
    ("persona_que_reporta",      "Persona Reporta",          18),
    ("cinum",                    "CI",                       30),
    ("ci_description",           "CI Descripcion",           40),
    ("cant_worklogs",            "# Worklogs",               10),
    ("ultimo_avance_fecha",      "Ultimo Avance Fecha",      18),
    ("ultimo_avance_quien",      "Ultimo Avance Quien",      18),
    ("ultimo_avance_resumen",    "Ultimo Avance Resumen",    50),
    ("ultimo_avance_completo",   "Ultimo Avance Completo",   80),
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

FILL_CRITICO = PatternFill(start_color="F8CBAD", end_color="F8CBAD", fill_type="solid")
FILL_ALTO    = PatternFill(start_color="FFE699", end_color="FFE699", fill_type="solid")
FILL_MEDIO   = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")
FILL_BAJO    = PatternFill(start_color="E2EFDA", end_color="E2EFDA", fill_type="solid")


def color_por_antiguedad(dias):
    if dias is None:
        return None
    if dias > 90:
        return FILL_CRITICO
    if dias > 30:
        return FILL_ALTO
    if dias > 7:
        return FILL_MEDIO
    return FILL_BAJO


# ════════════════════════════════════════════════════════════════════
# EXTRAER DATOS DESDE MAXIMO
# ════════════════════════════════════════════════════════════════════

def parsear_fecha(fecha_str):
    """Parsea fecha ISO de Maximo a datetime, o None."""
    if not fecha_str:
        return None
    try:
        return datetime.fromisoformat(fecha_str.replace("Z", "+00:00")).replace(tzinfo=None)
    except (ValueError, AttributeError):
        return None


def construir_fila_desde_maximo(member, cache_sitios, ci_cache):
    """
    Trae detalle + worklogs de una OT desde Maximo y arma el dict
    con la misma forma que el query de reporte_inprg_mc.py devuelve.
    """
    wonum = member.get("wonum")
    href = member.get("href")
    cinum = member.get("cinum") or ""
    ahora = datetime.now()

    # 1. Detalle completo
    detalle = obtener_detalle_ot(href)
    if not detalle:
        return None

    # 2. CI description
    ci_desc = obtener_ci_description(cinum, cache=ci_cache) if cinum else ""

    # 3. Worklogs inline (los necesitamos para "ultimo avance")
    worklogs_crudos = extraer_worklogs_inline(detalle)
    cant_worklogs = len(worklogs_crudos)

    # 4. Aplanar OT (mismo transformer que el ETL)
    registro = construir_registro(
        member=member,
        detalle=detalle,
        ci_description=ci_desc,
        cant_worklogs=cant_worklogs,
    )

    # 5. Renombrar campos al formato de Postgres (para que el reporte
    # tenga los mismos nombres que el reporte_inprg_mc.py)
    mapeo = {
        "reportdate":    "creation_date",
        "actstart":      "actual_start",
        "actfinish":     "actual_finish",
        "lead":          "assigned_to",
    }
    nuevo = {}
    for k, v in registro.items():
        if k in mapeo:
            nuevo[mapeo[k]] = v
        elif k.isupper() or ("_" in k and any(c.isupper() for c in k)):
            nuevo[k.lower()] = v
        else:
            nuevo[k] = v
    registro = nuevo

    # 6. Enriquecer con sitio (ciudad, departamento)
    registro = aplicar_sitio_a_registro(registro, cache_sitios)

    # 7. Calcular dias abierta
    fecha_creacion = parsear_fecha(registro.get("creation_date"))
    dias_abierta = (ahora - fecha_creacion).total_seconds() / 86400 if fecha_creacion else None

    # 8. Encontrar ultimo worklog
    ultimo_avance_fecha = None
    ultimo_avance_quien = None
    ultimo_avance_resumen = None
    ultimo_avance_completo = None
    if worklogs_crudos:
        worklogs_ordenados = sorted(
            worklogs_crudos,
            key=lambda w: w.get("createdate", ""),
            reverse=True,
        )
        ultimo = worklogs_ordenados[0]
        ultimo_avance_fecha = parsear_fecha(ultimo.get("createdate"))
        ultimo_avance_quien = ultimo.get("createby")
        ultimo_avance_resumen = ultimo.get("description")
        ultimo_avance_completo = ultimo.get("description_long")

    dias_sin_avance = None
    if ultimo_avance_fecha:
        dias_sin_avance = (ahora - ultimo_avance_fecha).total_seconds() / 86400

    # 9. Armar fila con la misma forma que el query de Postgres devuelve
    return {
        "wonum":                    wonum,
        "cinum":                    registro.get("cinum"),
        "ci_description":           registro.get("ci_description"),
        "resumen":                  registro.get("description"),
        "dias_abierta":             dias_abierta,
        "fecha_creacion":           fecha_creacion,
        "fecha_inicio":             parsear_fecha(registro.get("actual_start")),
        "tecnico":                  registro.get("assigned_to"),
        "coordinador":              registro.get("coordinador_red_fo"),
        "lider_zona":               registro.get("lider_de_zona_fo"),
        "codigo_sitio":             registro.get("location"),
        "nombre_sitio":             registro.get("nom_ubicacion"),
        "direccion":                registro.get("direccion"),
        "ciudad":                   registro.get("ciudad"),
        "departamento":             registro.get("departamento"),
        "tipo_tramo":               registro.get("tipo_tramo"),
        "tipo_operacion_fo":        registro.get("tipo_operacion_fo"),
        "operador":                 registro.get("operador_fo"),
        "eecc":                     registro.get("eecc_cuadrilla_fo"),
        "tipo_cuadrilla_fo":        registro.get("tipo_cuadrilla_fo"),
        "numero_caso":              registro.get("numero_caso_fo"),
        "outage_asociado":          registro.get("outage_asociado"),
        "area_reporta":             registro.get("area_que_reporta_fo"),
        "persona_que_reporta":      registro.get("persona_que_reporta"),
        "cant_worklogs":            cant_worklogs,
        "ultimo_avance_fecha":      ultimo_avance_fecha,
        "ultimo_avance_quien":      ultimo_avance_quien,
        "ultimo_avance_resumen":    ultimo_avance_resumen,
        "ultimo_avance_completo":   ultimo_avance_completo,
        "dias_sin_avance":          dias_sin_avance,
    }


# ════════════════════════════════════════════════════════════════════
# HOJA 1: LISTA DETALLADA
# ════════════════════════════════════════════════════════════════════

def construir_hoja_detalle(ws, filas):
    for col_idx, (_, header, ancho) in enumerate(COLUMNAS_DETALLE, start=1):
        cell = ws.cell(row=1, column=col_idx, value=header)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = HEADER_ALIGN
        cell.border = THIN_BORDER
        ws.column_dimensions[get_column_letter(col_idx)].width = ancho

    ws.row_dimensions[1].height = 35
    ws.freeze_panes = "B2"

    for row_idx, fila in enumerate(filas, start=2):
        dias_abierta = fila.get("dias_abierta")
        fill_fila = color_por_antiguedad(dias_abierta)

        for col_idx, (key, _, _) in enumerate(COLUMNAS_DETALLE, start=1):
            valor = fila.get(key)

            if key == "dias_abierta" or key == "dias_sin_avance":
                if valor is not None:
                    valor = round(float(valor), 1)
            elif key in ("fecha_creacion", "fecha_inicio", "ultimo_avance_fecha"):
                if valor is not None:
                    valor = valor.strftime("%Y-%m-%d %H:%M") if hasattr(valor, "strftime") else str(valor)

            cell = ws.cell(row=row_idx, column=col_idx, value=valor)
            cell.alignment = CELL_ALIGN
            cell.border = THIN_BORDER
            if fill_fila:
                cell.fill = fill_fila

    if filas:
        last_col = get_column_letter(len(COLUMNAS_DETALLE))
        ws.auto_filter.ref = f"A1:{last_col}{len(filas) + 1}"


# ════════════════════════════════════════════════════════════════════
# HOJA 2: RESUMEN EJECUTIVO
# ════════════════════════════════════════════════════════════════════

def construir_hoja_resumen(ws, filas):
    from collections import Counter

    total = len(filas)

    rangos = {"0-7 dias": 0, "8-30 dias": 0, "31-90 dias": 0, ">90 dias": 0, "Sin fecha": 0}
    for f in filas:
        dias = f.get("dias_abierta")
        if dias is None:
            rangos["Sin fecha"] += 1
        elif dias <= 7:
            rangos["0-7 dias"] += 1
        elif dias <= 30:
            rangos["8-30 dias"] += 1
        elif dias <= 90:
            rangos["31-90 dias"] += 1
        else:
            rangos[">90 dias"] += 1

    por_depto = Counter((f.get("departamento") or "(sin departamento)") for f in filas)
    por_operador = Counter((f.get("operador") or "(sin operador)") for f in filas)
    por_eecc = Counter((f.get("eecc") or "(sin EECC)") for f in filas)
    por_tecnico = Counter((f.get("tecnico") or "(sin tecnico)") for f in filas)
    por_coordinador = Counter((f.get("coordinador") or "(sin coordinador)") for f in filas)

    ws.column_dimensions["A"].width = 35
    ws.column_dimensions["B"].width = 15
    ws.column_dimensions["C"].width = 15

    fila = 1

    cell = ws.cell(row=fila, column=1, value=f"REPORTE TODAS LAS INPRG MC - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    cell.font = Font(name="Calibri", size=14, bold=True, color="1F4E78")
    fila += 2

    cell = ws.cell(row=fila, column=1, value="Total OTs INPRG MC en O_GESFO:")
    cell.font = Font(bold=True)
    cell = ws.cell(row=fila, column=2, value=total)
    cell.font = Font(bold=True, size=12)
    fila += 2

    def seccion(titulo, items, top_n=None):
        nonlocal fila
        cell = ws.cell(row=fila, column=1, value=titulo)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell = ws.cell(row=fila, column=2, value="Cantidad")
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell = ws.cell(row=fila, column=3, value="%")
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        fila += 1

        if isinstance(items, dict):
            items_ordenados = list(items.items())
        else:
            items_ordenados = sorted(items.items(), key=lambda x: -x[1])

        if top_n:
            items_ordenados = items_ordenados[:top_n]

        for nombre, cant in items_ordenados:
            ws.cell(row=fila, column=1, value=nombre)
            ws.cell(row=fila, column=2, value=cant)
            ws.cell(row=fila, column=3, value=f"{cant/total*100:.1f}%" if total else "0%")
            fila += 1
        fila += 1

    seccion("Por antiguedad (dias abierta)", rangos)
    seccion("Por departamento", por_depto)
    seccion("Por operador", por_operador)
    seccion("Por coordinador (top 15)", por_coordinador, top_n=15)
    seccion("Por EECC / Cuadrilla (top 15)", por_eecc, top_n=15)
    seccion("Por tecnico asignado (top 15)", por_tecnico, top_n=15)


# ════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════

def main():
    print("=" * 70)
    print("CONTROL INPRG - REPORTE DE TODAS LAS INPRG MC EN O_GESFO")
    print("=" * 70)

    inicio = time.time()

    # 1. Cache de sitios
    print("\nCargando cache de sitios desde Oracle...")
    cache_sitios = cargar_cache_sitios()
    print(f"Cache: {len(cache_sitios)} sitios")

    # 2. Listar TODAS las INPRG (sin filtro de fecha)
    print("\nListando todas las INPRG en Maximo...")
    inprg_lista = listar_ots(
        ownergroup=OWNERGROUP,
        classstructureid=CLASSSTRUCTUREID,
        worktype=WORKTYPE,
        status_in=["INPRG"],
    )
    total = len(inprg_lista)
    print(f"INPRG encontradas: {total}")

    if not inprg_lista:
        print("\n[INFO] No hay OTs en estado INPRG. Nada que reportar.")
        return

    # 3. Traer detalle de cada una
    print(f"\nTrayendo detalle de cada OT desde Maximo (~{total} segundos estimados)...")
    ci_cache = {}
    filas = []
    errores = 0

    for i, member in enumerate(inprg_lista, 1):
        try:
            fila = construir_fila_desde_maximo(member, cache_sitios, ci_cache)
            if fila:
                filas.append(fila)
            else:
                errores += 1
        except Exception as e:
            errores += 1
            logging.error(f"Error procesando OT {member.get('wonum')}: {e}")

        if i % 50 == 0:
            elapsed = time.time() - inicio
            tasa = i / elapsed
            remaining = (total - i) / tasa
            print(f"  {i}/{total} OTs... (estimado restante: {remaining/60:.1f} min)")

    duracion = time.time() - inicio
    print(f"\nDetalle completado en {duracion/60:.1f} minutos")
    print(f"OTs procesadas: {len(filas)}")
    if errores > 0:
        print(f"Errores: {errores}")

    if not filas:
        print("\n[ERROR] No se pudo procesar ninguna OT.")
        return

    # Ordenar por antiguedad ascendente (mas viejas primero, igual que el original)
    filas.sort(key=lambda f: f.get("fecha_creacion") or datetime.min)

    # 4. Crear Excel
    print("\nGenerando Excel...")
    wb = Workbook()

    ws_detalle = wb.active
    ws_detalle.title = "OTs INPRG"
    construir_hoja_detalle(ws_detalle, filas)

    ws_resumen = wb.create_sheet(title="Resumen")
    construir_hoja_resumen(ws_resumen, filas)

    # 5. Guardar
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = output_dir / f"Reporte_INPRG_TODOS_{timestamp}.xlsx"

    wb.save(filename)

    print(f"\n[OK] Reporte generado: {filename}")
    print(f"     Total OTs:          {len(filas)}")
    print(f"     Duracion total:     {duracion/60:.1f} minutos")


if __name__ == "__main__":
    main()