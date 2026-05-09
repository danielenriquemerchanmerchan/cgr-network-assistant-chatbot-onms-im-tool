"""
bot/services/interacciones.py
-----------------------------
Servicio de registro de interacciones en onms.bot_interacciones.

PROPOSITO:
    API central que TODOS los handlers usan para escribir interacciones
    en la base. Garantiza:
    - Aplicar reglas del catalogo cat_tipo_interaccion (urgencia, atencion CGR)
    - Insertar con jsonb correcto en metadata
    - Loggear cada registro

PATRON:
    Una sola funcion publica: registrar_interaccion(...)
    Recibe ~15 parametros, la mayoria opcionales.
    Retorna interaccion_id si OK, None si fallo.
"""

import json
import logging

from integrations.postgres.client import obtener_conexion, cerrar_conexion

logger = logging.getLogger(__name__)


def registrar_interaccion(
    # ──── Obligatorios ────
    tipo_interaccion,        # str: codigo del catalogo cat_tipo_interaccion
    direccion,               # str: 'entrante' / 'saliente' / 'sistema'
    actor_tipo,              # str: 'cuadrilla' / 'coordinador_contratista' / 'cgr' / 'sistema' ...

    # ──── Opcionales: identificacion del actor ────
    actor_id=None,           # str: cuadrilla_id, coordinador_id, usuario CGR
    cuadrilla_id=None,       # str: si la interaccion es de/para una cuadrilla

    # ──── Opcionales: contexto operativo ────
    wonum=None,              # str: numero de OT (denormalizado)
    asignacion_id=None,      # int: FK a ot_bandeja
    parada_id=None,          # int: FK a paradas_reloj

    # ──── Opcionales: contenido ────
    contenido_texto=None,    # str: texto del mensaje
    contenido_media_path=None,
    contenido_media_tipo=None,  # str: foto/audio/sticker/etc (FK a cat_tipo_media)
    metadata=None,           # dict: info estructurada (se serializa a jsonb)

    # ──── Opcionales: contexto Telegram ────
    telegram_chat_id=None,
    telegram_chat_title=None,
    telegram_message_id=None,
    telegram_user_id=None,
    telegram_username=None,

    # ──── Opcionales: overrides ────
    nivel_urgencia=None,         # str: si None, se toma del catalogo
    atencion_requerida=None,     # bool: si None, se toma del catalogo
):
    """
    Registra una interaccion en onms.bot_interacciones.

    Si nivel_urgencia o atencion_requerida son None, los toma del catalogo
    cat_tipo_interaccion (segun el tipo_interaccion dado).

    Retorna:
        interaccion_id (int) si se registro OK.
        None si fallo.
    """
    conn = None
    try:
        conn = obtener_conexion()

        # Si no se especificaron urgencia/atencion, los tomamos del catalogo
        if nivel_urgencia is None or atencion_requerida is None:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT nivel_urgencia_default, requiere_atencion_cgr
                    FROM onms.cat_tipo_interaccion
                    WHERE codigo = %s
                    """,
                    (tipo_interaccion,),
                )
                row = cur.fetchone()
                if row is None:
                    logger.error(
                        f"Tipo de interaccion no existe en catalogo: {tipo_interaccion}"
                    )
                    return None
                if nivel_urgencia is None:
                    nivel_urgencia = row[0] or "normal"
                if atencion_requerida is None:
                    atencion_requerida = row[1]

        # Convertir metadata a JSON (si vino dict)
        metadata_json = json.dumps(metadata) if metadata else None

        # Insertar
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO onms.bot_interacciones (
                    fecha_hora,
                    asignacion_id,
                    parada_id,
                    wonum,
                    cuadrilla_id,
                    tipo_interaccion,
                    direccion,
                    nivel_urgencia,
                    actor_tipo,
                    actor_id,
                    telegram_chat_id,
                    telegram_chat_title_snapshot,
                    telegram_message_id,
                    telegram_user_id,
                    telegram_username,
                    contenido_texto,
                    contenido_media_path,
                    contenido_media_tipo,
                    metadata,
                    estado_procesamiento,
                    atencion_requerida
                ) VALUES (
                    NOW(),
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s::jsonb,
                    'procesado',
                    %s
                )
                RETURNING interaccion_id
                """,
                (
                    asignacion_id, parada_id, wonum, cuadrilla_id,
                    tipo_interaccion, direccion, nivel_urgencia,
                    actor_tipo, actor_id,
                    telegram_chat_id, telegram_chat_title,
                    telegram_message_id, telegram_user_id, telegram_username,
                    contenido_texto, contenido_media_path, contenido_media_tipo,
                    metadata_json,
                    atencion_requerida,
                ),
            )
            interaccion_id = cur.fetchone()[0]
            conn.commit()

            logger.info(
                f"Interaccion #{interaccion_id} registrada: "
                f"{tipo_interaccion} / actor={actor_tipo}/{actor_id} / wonum={wonum}"
            )
            return interaccion_id

    except Exception as e:
        logger.error(f"Error registrando interaccion: {e}")
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            cerrar_conexion(conn)