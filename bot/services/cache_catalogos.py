"""
bot/services/cache_catalogos.py
-------------------------------
Cache en memoria de los catalogos del schema onms.

PROPOSITO:
    Servir catalogos al bot desde RAM en lugar de consultar Postgres en
    cada interaccion. Los catalogos son tablas pequenas que cambian poco,
    asi que tenerlos en memoria es mas rapido y permite editarlos en BD
    sin redesplegar el bot (con /admin_recargar).

ALCANCE EN ESTA FASE (Bloque 1 / Fase 1 del Entregable 2):
    Solo el catalogo cat_mensaje. El resto de catalogos sigue siendo
    consultado a BD desde bot/services/catalogos.py, paradas.py, etc.
    En la Fase 3 (big bang) se agregaran aqui:
        cat_causal_parada, cat_grupo_causal_parada, cat_fase_operativa,
        cat_tipo_interaccion, cat_estimado_duracion, cat_nivel_urgencia,
        cat_estado_parada, cat_actor_tipo

ARQUITECTURA EN TRES MODOS:
    1. Arranque        : main.py llama cargar_caches() ANTES de inicializar
                         el bot. Si falla, el bot termina con sys.exit(1).
                         No hay arranque parcial con catalogos vacios.
    2. Interaccion     : los handlers llaman obtener_mensaje(codigo). La
                         funcion lee del dict en RAM, formatea con kwargs,
                         y devuelve. No toca BD.
    3. Recarga         : el comando admin /admin_recargar invoca
                         recargar_caches(). Si falla, los dicts viejos
                         siguen activos. Si pasa, los dicts se reemplazan
                         atomicamente bajo lock.

DECISIONES DE DISENO:
    - Diccionarios privados (prefijo _). Se acceden solo via funciones
      publicas. Asi nadie los muta por accidente desde fuera.
    - threading.Lock para la recarga. Garantiza que un handler no lea
      a mitad de un reemplazo de diccionario.
    - obtener_mensaje() nunca lanza excepcion: si falta la entrada o
      una variable, devuelve un fallback visible en chat. Asi un error
      de configuracion en BD no tumba el bot.
"""

import logging
from threading import Lock

from integrations.postgres.client import obtener_conexion, cerrar_conexion

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
# ESTADO INTERNO
# ══════════════════════════════════════════════════════════════

# Diccionario en memoria: { codigo: texto }
# Texto puede tener placeholders estilo {variable} para .format()
_cache_mensajes = {}

# Lock para reemplazo atomico durante la recarga.
# La carga inicial tambien lo toma para uniformidad, aunque en arranque
# no hay otro hilo compitiendo.
_cache_lock = Lock()


# ══════════════════════════════════════════════════════════════
# CARGA Y RECARGA
# ══════════════════════════════════════════════════════════════

def cargar_caches():
    """
    Carga todos los catalogos desde Postgres a memoria.

    Se llama UNA VEZ al arrancar el bot, desde main.py, ANTES de
    inicializar la Application de Telegram.

    Si falla (BD caida, schema mal, etc.) la excepcion se propaga al
    llamador. main.py la captura y termina con sys.exit(1) — el bot
    no debe arrancar con catalogos vacios.

    En la Fase 3 esta funcion crecera para cargar mas catalogos. La
    convencion es: cargar todo dentro del mismo lock, en variables
    locales primero, y solo al final reemplazar los globales. Asi si
    falla la carga del 5to catalogo, los 4 anteriores no quedan a
    medias en memoria.
    """
    logger.info("[cache] Cargando catalogos desde Postgres...")

    # Construir versiones nuevas en variables LOCALES.
    # No tocamos los globals hasta haber cargado todo con exito.
    nuevos_mensajes = _cargar_cat_mensaje()

    # Reemplazo atomico bajo lock.
    global _cache_mensajes
    with _cache_lock:
        _cache_mensajes = nuevos_mensajes

    logger.info(
        f"[cache] Catalogos cargados: cat_mensaje={len(_cache_mensajes)} entradas"
    )


def recargar_caches():
    """
    Recarga todos los catalogos sin reiniciar el bot.

    Se invoca desde el handler /admin_recargar.

    Si la recarga falla, los catalogos viejos en memoria siguen activos
    y la excepcion se propaga al llamador (que la convierte en mensaje
    de error al admin). Esto garantiza que un error de BD durante una
    recarga no deje al bot sin catalogos.
    """
    logger.info("[cache] Recarga solicitada por admin")
    cargar_caches()  # mismo metodo: ya hace reemplazo atomico
    logger.info("[cache] Recarga completada")


# ══════════════════════════════════════════════════════════════
# CARGADORES POR CATALOGO (privados)
# ══════════════════════════════════════════════════════════════

def _cargar_cat_mensaje():
    """
    Lee onms.cat_mensaje y devuelve un dict { codigo: texto }
    con solo los registros activos.

    No modifica el cache global. Eso es responsabilidad del llamador
    (cargar_caches), que orquesta el reemplazo atomico.

    Lanza excepcion si la conexion o la consulta falla.
    """
    conn = None
    try:
        conn = obtener_conexion()
        if conn is None:
            raise RuntimeError(
                "obtener_conexion() devolvio None — revisar credenciales PG_*"
            )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT codigo, texto
                FROM onms.cat_mensaje
                WHERE activo = true
                """
            )
            rows = cur.fetchall()
            return {row[0]: row[1] for row in rows}
    finally:
        if conn:
            cerrar_conexion(conn)


# ══════════════════════════════════════════════════════════════
# ACCESORES PUBLICOS
# ══════════════════════════════════════════════════════════════

def obtener_mensaje(codigo: str, **kwargs) -> str:
    """
    Devuelve el texto del mensaje identificado por 'codigo', formateado
    con los kwargs.

    Argumentos:
        codigo : str — identificador jerarquico, ej. 'midiendo.exito'
        kwargs : variables a sustituir con .format()

    Retorna:
        str — texto formateado, listo para enviar por Telegram.

    Comportamiento ante errores (NUNCA lanza excepcion):
        - codigo no existe en cache       -> "[mensaje:CODIGO no encontrado]"
        - codigo existe pero falta una variable en kwargs -> texto sin formatear
                                            (mejor mostrar el placeholder
                                             que romper la respuesta)

    Esta funcion NO consulta BD. Si el codigo no esta es porque el cache
    no lo tiene cargado: o no existe en BD, o esta como activo=false, o
    se inserto despues de la ultima carga (en cuyo caso /admin_recargar
    lo trae).
    """
    # Lectura sin lock: los reemplazos del dict son atomicos en CPython
    # gracias al GIL, y solo apuntamos a la referencia global.
    texto = _cache_mensajes.get(codigo)

    if texto is None:
        logger.error(f"[cache] Mensaje no encontrado: '{codigo}'")
        return f"[mensaje:{codigo} no encontrado]"

    if not kwargs:
        return texto

    try:
        return texto.format(**kwargs)
    except KeyError as e:
        # El texto tiene un placeholder {x} pero kwargs no lo trae.
        logger.error(
            f"[cache] Variable faltante en mensaje '{codigo}': {e}. "
            f"Devolviendo texto sin formatear."
        )
        return texto
    except (IndexError, ValueError) as e:
        # Placeholders posicionales o sintaxis rara en el texto.
        logger.error(
            f"[cache] Error de formato en mensaje '{codigo}': {e}. "
            f"Devolviendo texto sin formatear."
        )
        return texto


def existe_mensaje(codigo: str) -> bool:
    """
    True si el codigo existe en el cache. Util para chequeos defensivos
    o para tests. No es de uso comun en handlers.
    """
    return codigo in _cache_mensajes


def estado_cache():
    """
    Devuelve un dict con el estado actual del cache.
    Util para logs, debugging y para el mensaje de respuesta de
    /admin_recargar.

    Retorna:
        { 'cat_mensaje': N }
    """
    return {
        "cat_mensaje": len(_cache_mensajes),
    }