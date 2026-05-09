"""
bot/services/cuadrillas.py
--------------------------
Servicios relacionados con identificacion de cuadrillas.

PROPOSITO:
    Dado un chat_id de Telegram (que llega cuando alguien escribe en un grupo),
    saber a que cuadrilla pertenece ese grupo. Es la primera operacion que el
    bot hace en CADA mensaje recibido para saber con quien esta hablando.

NO HACE:
    - No registra nada (eso es de services/interacciones.py)
    - No procesa mensajes (eso es de los handlers)
"""

import logging

from integrations.postgres.client import obtener_conexion, cerrar_conexion

logger = logging.getLogger(__name__)


def identificar_cuadrilla_por_chat_id(chat_id):
    """
    Dado un chat_id de Telegram, busca a que cuadrilla pertenece.

    Argumentos:
        chat_id: int, el chat_id de Telegram (negativo para grupos).

    Retorna:
        dict con datos de la cuadrilla si se encontro y esta activa.
        None si no existe o esta inactiva.

    Ejemplo de retorno:
        {
            "cuadrilla_id":  "CUNDINAMARCA_2",
            "nombre":        "Cuadrilla Cundinamarca 2 - Opegin",
            "contratista":   "Opegin",
            "departamento":  "Cundinamarca"
        }
    """
    conn = None
    try:
        conn = obtener_conexion()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT cuadrilla_id, nombre, contratista, departamento
                FROM onms.cuadrillas
                WHERE telegram_chat_id = %s AND activa = true
                """,
                (chat_id,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return {
                "cuadrilla_id": row[0],
                "nombre":       row[1],
                "contratista":  row[2],
                "departamento": row[3],
            }
    except Exception as e:
        logger.error(f"Error identificando cuadrilla por chat_id {chat_id}: {e}")
        return None
    finally:
        if conn:
            cerrar_conexion(conn)