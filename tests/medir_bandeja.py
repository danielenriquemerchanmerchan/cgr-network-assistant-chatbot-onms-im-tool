"""
medir_bandeja.py
----------------
Script de medicion de la bandeja activa de O_GESFO en Maximo.

PROPOSITO:
    Diagnosticar cuantas OTs estan actualmente en estado INPRG y COMP, y
    clasificarlas por antigüedad para decidir el filtro optimo del ETL de
    bandeja. Detecta OTs "zombies" (viejas sin actividad) vs OTs viejas
    legitimas (con worklogs recientes).

EJECUCION:
    py -m tests.medir_bandeja

SALIDA:
    Reporte por consola con conteos por antigüedad y actividad reciente.
    No modifica nada en Maximo ni en BD.

ESTRATEGIA EN 2 PASOS:
    Paso A (rapido): conteo total + clasificacion por creation_date.
    Paso B (profundo): solo se ejecuta si hay mas de 30 OTs antiguas.
                       Trae detalle + worklogs para detectar actividad real.
"""

import time
from datetime import datetime, timedelta
from collections import defaultdict

from core.logging_setup import logger  # inicializa logging
from core.config import MAXIMO_PAGE_SIZE
from integrations.maximo.rest_api import (
    listar_ots,
    obtener_detalle_ot,
    extraer_worklogs_inline,
)


# Umbral para activar el Paso B (analisis profundo)
UMBRAL_PASO_B = 30


def parsear_fecha_maximo(fecha_str):
    """Convierte fecha ISO de Maximo a datetime. Devuelve None si falla."""
    if not fecha_str:
        return None
    try:
        # Maximo entrega fechas como "2026-04-23T10:40:03-05:00" o similar
        # Truncamos a los primeros 19 chars (sin timezone) para parsear
        return datetime.strptime(fecha_str[:19], "%Y-%m-%dT%H:%M:%S")
    except Exception:
        return None


def clasificar_por_antiguedad(ots, ahora):
    """
    Clasifica una lista de OTs por antigüedad de creation_date.
    Devuelve un dict con listas de OTs por categoria.
    """
    clasif = {
        "fresca": [],          # < 7 dias
        "tibia": [],           # 7-30 dias
        "antigua": [],         # 30-90 dias
        "muy_antigua": [],     # > 90 dias
        "sin_fecha": [],       # creation_date NULL o invalido
    }

    for ot in ots:
        # En el listado paginado, el campo es 'reportdate'
        fecha = parsear_fecha_maximo(ot.get("reportdate"))
        if fecha is None:
            clasif["sin_fecha"].append(ot)
            continue

        dias = (ahora - fecha).days
        if dias < 7:
            clasif["fresca"].append(ot)
        elif dias < 30:
            clasif["tibia"].append(ot)
        elif dias < 90:
            clasif["antigua"].append(ot)
        else:
            clasif["muy_antigua"].append(ot)

    return clasif


def imprimir_paso_a(status, total, clasif):
    """Imprime el reporte del Paso A."""
    print(f"\n{'='*60}")
    print(f"PASO A: OTs en status {status}")
    print(f"{'='*60}")
    print(f"  Total:                              {total:>5}")
    print(f"  Frescas (< 7 dias):                 {len(clasif['fresca']):>5}")
    print(f"  Tibias (7-30 dias):                 {len(clasif['tibia']):>5}")
    print(f"  Antiguas (30-90 dias):              {len(clasif['antigua']):>5}")
    print(f"  Muy antiguas (> 90 dias):           {len(clasif['muy_antigua']):>5}")
    print(f"  Sin fecha de creacion:              {len(clasif['sin_fecha']):>5}")


def analizar_actividad_profunda(ots_viejas, ahora):
    """
    Para cada OT vieja, trae el detalle con worklogs y clasifica por
    actividad reciente.

    Retorna dict con:
        - 'con_actividad_reciente': OTs con al menos 1 worklog en ultimos 7 dias
        - 'sin_actividad_reciente': OTs sin worklogs recientes (zombies probables)
        - 'sin_worklogs': OTs sin ningun worklog
    """
    resultado = {
        "con_actividad_reciente": [],
        "sin_actividad_reciente": [],
        "sin_worklogs": [],
    }

    umbral = ahora - timedelta(days=7)
    total = len(ots_viejas)

    print(f"\nAnalizando {total} OTs viejas (esto tarda ~1s por OT)...")

    for i, ot in enumerate(ots_viejas, 1):
        wonum = ot.get("wonum", "?")
        href = ot.get("href")

        if not href:
            continue

        # Traer detalle completo
        detalle = obtener_detalle_ot(href)
        if not detalle:
            continue

        worklogs = extraer_worklogs_inline(detalle)

        if not worklogs:
            resultado["sin_worklogs"].append(ot)
        else:
            # Buscar el worklog mas reciente
            fechas_worklogs = []
            for w in worklogs:
                fecha_w = parsear_fecha_maximo(w.get("createdate"))
                if fecha_w:
                    fechas_worklogs.append(fecha_w)

            if not fechas_worklogs:
                resultado["sin_worklogs"].append(ot)
            else:
                ultimo = max(fechas_worklogs)
                if ultimo >= umbral:
                    resultado["con_actividad_reciente"].append({
                        "wonum": wonum,
                        "creation_date": ot.get("reportdate"),
                        "ultimo_worklog": ultimo.isoformat(),
                    })
                else:
                    resultado["sin_actividad_reciente"].append({
                        "wonum": wonum,
                        "creation_date": ot.get("reportdate"),
                        "ultimo_worklog": ultimo.isoformat(),
                    })

        if i % 10 == 0:
            print(f"  Procesadas {i}/{total}...")

    return resultado


def imprimir_paso_b(resultado, status):
    """Imprime el reporte del Paso B."""
    print(f"\n{'='*60}")
    print(f"PASO B: Analisis profundo de OTs viejas en {status}")
    print(f"{'='*60}")
    print(f"  Con actividad reciente (worklog < 7 dias): "
          f"{len(resultado['con_actividad_reciente']):>5}  <- legitimas viejas")
    print(f"  Sin actividad reciente:                    "
          f"{len(resultado['sin_actividad_reciente']):>5}  <- MUY_ANTIGUA probables")
    print(f"  Sin ningun worklog:                        "
          f"{len(resultado['sin_worklogs']):>5}  <- huerfanas sin avances")

    # Mostrar algunos ejemplos de zombies
    zombies = resultado['sin_actividad_reciente']
    if zombies:
        print(f"\n  Ejemplos de zombies (primeros 5):")
        for z in zombies[:5]:
            print(f"    {z['wonum']}: creada {z['creation_date'][:10]}, "
                  f"ultimo worklog {z['ultimo_worklog'][:10]}")


def medir_status(status, ahora):
    """Mide un status especifico (INPRG o COMP). Hace Paso A siempre, Paso B si vale."""
    print(f"\nConsultando OTs con status={status}...")
    inicio = time.time()

    # Paso A: traer listado
    select = "wonum,href,cinum,worktype,classstructureid,status,description,location,nom_ubicacion,reportdate"
    where = f'status="{status}" and ownergroup="O_GESFO" and classstructureid="4213"'

    # Reutilizamos listar_ots pero filtrando por status manualmente
    # (la funcion actual no acepta filtro custom, asi que pasamos el filtro como parte del select)
    # Truco: usamos el listar_ots existente que ya filtra por ownergroup + classstructureid
    # y luego filtramos por status en el script.

    todas_ots = listar_ots(
        ownergroup="O_GESFO",
        classstructureid="4213",
        page_size=MAXIMO_PAGE_SIZE,
    )

    # Filtrar por status localmente
    ots = [o for o in todas_ots if o.get("status") == status]
    total = len(ots)

    duracion_a = time.time() - inicio
    print(f"  Listado completo en {duracion_a:.1f}s. {total} OTs encontradas.")

    # Clasificar por antigüedad
    clasif = clasificar_por_antiguedad(ots, ahora)
    imprimir_paso_a(status, total, clasif)

    # Paso B: solo si hay suficientes OTs viejas
    ots_viejas = clasif["antigua"] + clasif["muy_antigua"]
    cantidad_viejas = len(ots_viejas)

    if cantidad_viejas >= UMBRAL_PASO_B:
        print(f"\n>>> Hay {cantidad_viejas} OTs viejas (>= {UMBRAL_PASO_B}). "
              f"Ejecutando Paso B...")
        inicio_b = time.time()
        resultado_b = analizar_actividad_profunda(ots_viejas, ahora)
        duracion_b = time.time() - inicio_b
        imprimir_paso_b(resultado_b, status)
        print(f"\n  Paso B completado en {duracion_b:.1f}s.")
    else:
        print(f"\n>>> Solo hay {cantidad_viejas} OTs viejas (< {UMBRAL_PASO_B}). "
              f"Paso B no se ejecuta (no vale la pena).")

    return total, clasif


def main():
    print("\n" + "="*60)
    print("MEDICION DE BANDEJA O_GESFO")
    print("="*60)

    ahora = datetime.now()
    print(f"Fecha/hora de la medicion: {ahora.isoformat()}")
    print(f"Umbral para Paso B: {UMBRAL_PASO_B} OTs viejas")

    # Medir INPRG
    total_inprg, clasif_inprg = medir_status("INPRG", ahora)

    # Medir COMP
    total_comp, clasif_comp = medir_status("COMP", ahora)

    # Resumen ejecutivo final
    print(f"\n\n{'='*60}")
    print("RESUMEN EJECUTIVO")
    print(f"{'='*60}")
    print(f"  Total OTs activas (INPRG + COMP):   {total_inprg + total_comp:>5}")
    print(f"  De las cuales, frescas (< 7 dias):  "
          f"{len(clasif_inprg['fresca']) + len(clasif_comp['fresca']):>5}")
    print(f"  Tibias (7-30 dias):                 "
          f"{len(clasif_inprg['tibia']) + len(clasif_comp['tibia']):>5}")
    print(f"  Antiguas (30-90 dias):              "
          f"{len(clasif_inprg['antigua']) + len(clasif_comp['antigua']):>5}")
    print(f"  Muy antiguas (> 90 dias):           "
          f"{len(clasif_inprg['muy_antigua']) + len(clasif_comp['muy_antigua']):>5}")
    print(f"\nMedicion completada.")


if __name__ == "__main__":
    main()