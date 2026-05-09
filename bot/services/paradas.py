"""
bot/services/paradas.py
-----------------------
Logica de creacion y cierre de paradas de reloj.

Centraliza los SELECT/INSERT/UPDATE sobre onms.paradas_reloj
para que los handlers no toquen SQL directo.
"""

import logging

from integrations.postgres.client import obtener_conexion, cerrar_conexion

logger = logging.getLogger(__name__)


def crear_parada(
    asignacion_id,
    wonum,
    cuadrilla_id,
    causal,
    estimado_duracion,
    requiere_atencion_exhaustiva,
    declarada_por_chat_id,
    declarada_por_telegram_user_id,
    causal_otro_detalle=None,
    comentario_apertura=None,
):
    """
    Crea una parada de reloj nueva en estado 'activa'.

    Tambien actualiza ot_bandeja.fase_operativa = 'pausada'.

    Argumentos:
        asignacion_id (int): FK a ot_bandeja
        wonum (str): denormalizado
        cuadrilla_id (str): FK a cuadrillas
        causal (str): codigo del catalogo cat_causal_parada
        estimado_duracion (str): codigo del catalogo cat_estimado_duracion
        requiere_atencion_exhaustiva (bool): si CGR debe monitorear
        declarada_por_chat_id (int): chat_id Telegram
        declarada_por_telegram_user_id (int): user_id Telegram
        causal_otro_detalle (str | None): obligatorio si causal == 'otro'
        comentario_apertura (str | None): texto libre opcional

    Retorna:
        parada_id (int) si OK, None si fallo.
    """
    conn = None
    try:
        conn = obtener_conexion()
        with conn.cursor() as cur:
            # Insertar la parada
            cur.execute(
                """
                INSERT INTO onms.paradas_reloj (
                    asignacion_id, wonum, cuadrilla_id,
                    causal, causal_otro_detalle,
                    estimado_duracion,
                    requiere_atencion_exhaustiva,
                    comentario_apertura,
                    declarada_por_chat_id,
                    declarada_por_telegram_user_id
                ) VALUES (
                    %s, %s, %s,
                    %s, %s,
                    %s,
                    %s,
                    %s,
                    %s, %s
                )
                RETURNING parada_id
                """,
                (
                    asignacion_id, wonum, cuadrilla_id,
                    causal, causal_otro_detalle,
                    estimado_duracion,
                    requiere_atencion_exhaustiva,
                    comentario_apertura,
                    declarada_por_chat_id,
                    declarada_por_telegram_user_id,
                ),
            )
            parada_id = cur.fetchone()[0]

            # Cambiar fase operativa de la asignacion a 'pausada'
            cur.execute(
                """
                UPDATE onms.ot_bandeja
                SET fase_operativa = 'pausada'
                WHERE asignacion_id = %s
                """,
                (asignacion_id,),
            )

            conn.commit()
            logger.info(
                f"Parada {parada_id} creada para asignacion {asignacion_id} "
                f"(causal={causal}, estimado={estimado_duracion}, "
                f"atencion={requiere_atencion_exhaustiva})"
            )
            return parada_id

    except Exception as e:
        logger.error(f"Error creando parada para asignacion {asignacion_id}: {e}")
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            cerrar_conexion(conn)


def obtener_parada_activa(asignacion_id):
    """
    Busca la parada activa (estado='activa') de una asignacion.

    Una asignacion solo puede tener una parada activa a la vez (las cerradas
    quedan en estado='cerrada' y no se cuentan).

    Retorna dict con datos de la parada, o None si no hay activa.
    """
    conn = None
    try:
        conn = obtener_conexion()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 
                    parada_id,
                    asignacion_id,
                    wonum,
                    cuadrilla_id,
                    causal,
                    estimado_duracion,
                    fecha_inicio,
                    requiere_atencion_exhaustiva
                FROM onms.paradas_reloj
                WHERE asignacion_id = %s
                  AND estado = 'activa'
                LIMIT 1
                """,
                (asignacion_id,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return {
                "parada_id": row[0],
                "asignacion_id": row[1],
                "wonum": row[2],
                "cuadrilla_id": row[3],
                "causal": row[4],
                "estimado_duracion": row[5],
                "fecha_inicio": row[6],
                "requiere_atencion_exhaustiva": row[7],
            }
    except Exception as e:
        logger.error(f"Error buscando parada activa de asignacion {asignacion_id}: {e}")
        return None
    finally:
        if conn:
            cerrar_conexion(conn)


def cerrar_parada(
    parada_id,
    cerrada_por_chat_id,
    cerrada_por_telegram_user_id,
    comentario_cierre=None,
    fase_operativa_al_reanudar="midiendo",
):
    """
    Cierra una parada activa: estado='cerrada', fecha_fin=NOW().

    Tambien restaura ot_bandeja.fase_operativa al valor que pasemos
    (default 'midiendo' porque la mayoria de paradas ocurren mientras miden,
    pero el handler puede pasar otra fase si tiene contexto).

    Retorna:
        dict con {parada_id, asignacion_id, fecha_fin, duracion_real}
        o None si fallo.
    """
    conn = None
    try:
        conn = obtener_conexion()
        with conn.cursor() as cur:
            # Cerrar la parada
            cur.execute(
                """
                UPDATE onms.paradas_reloj
                SET estado = 'cerrada',
                    fecha_fin = NOW(),
                    comentario_cierre = %s,
                    cerrada_por_chat_id = %s,
                    cerrada_por_telegram_user_id = %s
                WHERE parada_id = %s
                  AND estado = 'activa'
                RETURNING parada_id, asignacion_id, fecha_fin, duracion_real
                """,
                (
                    comentario_cierre,
                    cerrada_por_chat_id,
                    cerrada_por_telegram_user_id,
                    parada_id,
                ),
            )
            row = cur.fetchone()
            if row is None:
                logger.warning(f"Parada {parada_id} no se cerro (no estaba activa?)")
                conn.rollback()
                return None

            asignacion_id = row[1]

            # Restaurar fase operativa
            cur.execute(
                """
                UPDATE onms.ot_bandeja
                SET fase_operativa = %s
                WHERE asignacion_id = %s
                """,
                (fase_operativa_al_reanudar, asignacion_id),
            )

            conn.commit()
            logger.info(
                f"Parada {parada_id} cerrada (duracion={row[3]}). "
                f"Fase asignacion {asignacion_id} restaurada a '{fase_operativa_al_reanudar}'"
            )
            return {
                "parada_id": row[0],
                "asignacion_id": row[1],
                "fecha_fin": row[2],
                "duracion_real": row[3],
            }
    except Exception as e:
        logger.error(f"Error cerrando parada {parada_id}: {e}")
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            cerrar_conexion(conn)