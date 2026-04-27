"""
client.py (MySQL bot_gesfo)
---------------------------
Cliente de la base MySQL del bot de Telegram.

ESTADO ACTUAL: PAUSADO
    El bot de Telegram no esta activo en este momento. Este modulo
    permanece para preservar el codigo de las operaciones del bot
    (registro de tecnicos, acuses de recibo, avances reportados desde chat).

PLAN A FUTURO:
    Cuando se reactive el bot, esta funcionalidad probablemente migrara
    a Postgres para unificar la BD del proyecto. Las tablas se rediseñaran
    para relacionarse con work_orders mediante FK por wonum, soportando
    avances/estados desde Telegram (En sitio, Iniciando labores,
    Parada de reloj, Solicito llamada, etc.) que se reflejaran tanto
    en el dashboard web como en Maximo via insertar_avance().

NO SE IMPORTA EN NINGUN SCRIPT DEL FLUJO ACTUAL.
    No estorba el desarrollo del dashboard. Permanece como referencia.

Funciones (originales del bot):
    Tecnicos:
        registrar_tecnico, aprobar_tecnico, rechazar_tecnico,
        obtener_tecnicos_activos, obtener_tecnico, tecnico_existe

    OTs notificadas:
        registrar_ot, ot_ya_notificada, actualizar_status_ot,
        cerrar_ot, obtener_ots_activas, actualizar_ultimo_chequeo

    Historial de estados:
        registrar_cambio_estado

    Acuses de recibo:
        registrar_acuse_pendiente, confirmar_acuse,
        obtener_acuses_pendientes

    Avances:
        registrar_avance
"""

import mysql.connector
import logging
from datetime import datetime
from core.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD


def _conectar():
    """Retorna una conexión activa a MySQL."""
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        charset="utf8mb4"
    )


# ══════════════════════════════════════════════════════════════
# TÉCNICOS
# ══════════════════════════════════════════════════════════════

def registrar_tecnico(chat_id, nombre, empleado, username, departamento):
    """
    Registra un nuevo técnico con estado PENDIENTE.
    Retorna True si se registró, False si ya existe.
    """
    try:
        conn = _conectar()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO tecnicos (chat_id, nombre, numero_empleado, username, departamento)
            VALUES (%s, %s, %s, %s, %s)
        """, (chat_id, nombre, empleado, username, departamento))
        conn.commit()
        logging.info(f"Técnico registrado: {nombre} ({chat_id}) - {departamento}")
        return True
    except mysql.connector.IntegrityError:
        logging.warning(f"Técnico {chat_id} ya existe")
        return False
    except Exception as e:
        logging.error(f"Error registrando técnico {chat_id}: {e}")
        return False
    finally:
        cur.close()
        conn.close()


def aprobar_tecnico(chat_id):
    """Cambia el estado del técnico a ACTIVO."""
    try:
        conn = _conectar()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE tecnicos SET estado = 'ACTIVO' WHERE chat_id = %s
        """, (chat_id,))
        conn.commit()
        logging.info(f"Técnico {chat_id} aprobado")
        return cur.rowcount > 0
    except Exception as e:
        logging.error(f"Error aprobando técnico {chat_id}: {e}")
        return False
    finally:
        cur.close()
        conn.close()


def rechazar_tecnico(chat_id):
    """Cambia el estado del técnico a RECHAZADO."""
    try:
        conn = _conectar()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE tecnicos SET estado = 'RECHAZADO' WHERE chat_id = %s
        """, (chat_id,))
        conn.commit()
        logging.info(f"Técnico {chat_id} rechazado")
        return cur.rowcount > 0
    except Exception as e:
        logging.error(f"Error rechazando técnico {chat_id}: {e}")
        return False
    finally:
        cur.close()
        conn.close()


def obtener_tecnicos_activos(departamento):
    """
    Retorna lista de chat_ids de técnicos ACTIVOS de un departamento.
    """
    try:
        conn = _conectar()
        cur  = conn.cursor()
        cur.execute("""
            SELECT chat_id FROM tecnicos
            WHERE departamento = %s AND estado = 'ACTIVO'
        """, (departamento,))
        return [row[0] for row in cur.fetchall()]
    except Exception as e:
        logging.error(f"Error obteniendo técnicos de {departamento}: {e}")
        return []
    finally:
        cur.close()
        conn.close()


def obtener_tecnico(chat_id):
    """
    Retorna dict con los datos del técnico, o None si no existe.
    """
    try:
        conn = _conectar()
        cur  = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT * FROM tecnicos WHERE chat_id = %s
        """, (chat_id,))
        return cur.fetchone()
    except Exception as e:
        logging.error(f"Error obteniendo técnico {chat_id}: {e}")
        return None
    finally:
        cur.close()
        conn.close()


def tecnico_existe(chat_id):
    """Retorna True si el técnico ya está registrado (cualquier estado)."""
    return obtener_tecnico(chat_id) is not None


# ══════════════════════════════════════════════════════════════
# OTs NOTIFICADAS
# ══════════════════════════════════════════════════════════════

def registrar_ot(ot_data):
    """
    Registra una OT nueva en la base de datos.

    ot_data es un dict con los campos de la OT enriquecida:
        wonum, woclass, worktype, ownergroup, description, resumen,
        location, nom_ubicacion, ciudad, departamento, latitud, longitud,
        schedstart, schedfinish, reportdate, status_actual,
        status_descripcion, chat_ids_notificados, notificacion_exitosa
    """
    try:
        conn = _conectar()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO ots_notificadas (
                wonum, woclass, worktype, ownergroup,
                description, resumen, location, nom_ubicacion,
                ciudad, departamento, latitud, longitud,
                schedstart, schedfinish, reportdate,
                status_actual, status_descripcion,
                chat_ids_notificados, notificacion_exitosa
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s
            )
        """, (
            ot_data.get("wonum"),
            ot_data.get("woclass"),
            ot_data.get("worktype"),
            ot_data.get("ownergroup"),
            ot_data.get("description", "")[:255],
            ot_data.get("resumen", "")[:255],
            ot_data.get("location"),
            ot_data.get("nom_ubicacion", "")[:255],
            ot_data.get("ciudad"),
            ot_data.get("departamento"),
            ot_data.get("latitud"),
            ot_data.get("longitud"),
            ot_data.get("schedstart"),
            ot_data.get("schedfinish"),
            ot_data.get("reportdate"),
            ot_data.get("status_actual"),
            ot_data.get("status_descripcion"),
            ot_data.get("chat_ids_notificados"),
            ot_data.get("notificacion_exitosa", False),
        ))
        conn.commit()
        logging.info(f"OT {ot_data.get('wonum')} registrada en BD")
        return True
    except mysql.connector.IntegrityError:
        logging.warning(f"OT {ot_data.get('wonum')} ya existe en BD")
        return False
    except Exception as e:
        logging.error(f"Error registrando OT {ot_data.get('wonum')}: {e}")
        return False
    finally:
        cur.close()
        conn.close()


def ot_ya_notificada(wonum):
    """Retorna True si la OT ya fue registrada en la BD."""
    try:
        conn = _conectar()
        cur  = conn.cursor()
        cur.execute("SELECT wonum FROM ots_notificadas WHERE wonum = %s", (wonum,))
        return cur.fetchone() is not None
    except Exception as e:
        logging.error(f"Error verificando OT {wonum}: {e}")
        return False
    finally:
        cur.close()
        conn.close()


def actualizar_status_ot(wonum, status_nuevo, status_descripcion):
    """Actualiza el status actual de una OT y registra la fecha del chequeo."""
    try:
        conn = _conectar()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE ots_notificadas
            SET status_actual      = %s,
                status_descripcion = %s,
                fecha_ultimo_chequeo = NOW()
            WHERE wonum = %s
        """, (status_nuevo, status_descripcion, wonum))
        conn.commit()
        return cur.rowcount > 0
    except Exception as e:
        logging.error(f"Error actualizando status de OT {wonum}: {e}")
        return False
    finally:
        cur.close()
        conn.close()


def cerrar_ot(wonum):
    """Marca una OT como inactiva (cerrada) y registra la fecha de cierre."""
    try:
        conn = _conectar()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE ots_notificadas
            SET activa       = FALSE,
                fecha_cierre = NOW(),
                fecha_ultimo_chequeo = NOW()
            WHERE wonum = %s
        """, (wonum,))
        conn.commit()
        logging.info(f"OT {wonum} cerrada en BD")
        return cur.rowcount > 0
    except Exception as e:
        logging.error(f"Error cerrando OT {wonum}: {e}")
        return False
    finally:
        cur.close()
        conn.close()


def obtener_ots_activas():
    """
    Retorna lista de dicts con las OTs activas para monitorear sus cambios.
    """
    try:
        conn = _conectar()
        cur  = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT wonum, status_actual, departamento
            FROM ots_notificadas
            WHERE activa = TRUE
        """)
        return cur.fetchall()
    except Exception as e:
        logging.error(f"Error obteniendo OTs activas: {e}")
        return []
    finally:
        cur.close()
        conn.close()


def actualizar_ultimo_chequeo(wonum):
    """Actualiza la fecha del último chequeo de una OT."""
    try:
        conn = _conectar()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE ots_notificadas SET fecha_ultimo_chequeo = NOW()
            WHERE wonum = %s
        """, (wonum,))
        conn.commit()
    except Exception as e:
        logging.error(f"Error actualizando chequeo de OT {wonum}: {e}")
    finally:
        cur.close()
        conn.close()


# ══════════════════════════════════════════════════════════════
# HISTORIAL DE ESTADOS
# ══════════════════════════════════════════════════════════════

def registrar_cambio_estado(wonum, status_anterior, status_nuevo,
                             status_descripcion, fecha_cambio=None):
    """
    Registra un cambio de estado detectado en una OT.
    """
    try:
        conn = _conectar()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO ot_historial_estados
                (wonum, status_anterior, status_nuevo, status_nuevo_descripcion, fecha_cambio)
            VALUES (%s, %s, %s, %s, %s)
        """, (wonum, status_anterior, status_nuevo, status_descripcion, fecha_cambio))
        conn.commit()
        logging.info(f"OT {wonum}: cambio de estado {status_anterior} → {status_nuevo}")
        return True
    except Exception as e:
        logging.error(f"Error registrando cambio de estado de OT {wonum}: {e}")
        return False
    finally:
        cur.close()
        conn.close()


# ══════════════════════════════════════════════════════════════
# ACUSES DE RECIBO
# ══════════════════════════════════════════════════════════════

def registrar_acuse_pendiente(wonum, chat_id, nombre_tecnico, departamento):
    """
    Registra un acuse pendiente cuando se notifica una OT a un técnico.
    """
    try:
        conn = _conectar()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO acuses_recibido
                (wonum, chat_id, nombre_tecnico, departamento, fecha_notificacion)
            VALUES (%s, %s, %s, %s, NOW())
        """, (wonum, chat_id, nombre_tecnico, departamento))
        conn.commit()
        return True
    except Exception as e:
        logging.error(f"Error registrando acuse pendiente OT {wonum} técnico {chat_id}: {e}")
        return False
    finally:
        cur.close()
        conn.close()


def confirmar_acuse(wonum, chat_id):
    """
    Marca como confirmado el acuse de un técnico para una OT.
    """
    try:
        conn = _conectar()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE acuses_recibido
            SET confirmado  = TRUE,
                fecha_acuse = NOW()
            WHERE wonum = %s AND chat_id = %s AND confirmado = FALSE
        """, (wonum, chat_id))
        conn.commit()
        logging.info(f"Acuse confirmado: OT {wonum} por técnico {chat_id}")
        return cur.rowcount > 0
    except Exception as e:
        logging.error(f"Error confirmando acuse OT {wonum} técnico {chat_id}: {e}")
        return False
    finally:
        cur.close()
        conn.close()


def obtener_acuses_pendientes(timeout_minutos):
    """
    Retorna acuses sin confirmar que superaron el timeout.
    Usado por el monitor para alertar al admin.
    """
    try:
        conn = _conectar()
        cur  = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT a.wonum, a.chat_id, a.nombre_tecnico,
                   a.departamento, a.fecha_notificacion,
                   o.description
            FROM acuses_recibido a
            JOIN ots_notificadas o ON a.wonum = o.wonum
            WHERE a.confirmado = FALSE
              AND TIMESTAMPDIFF(MINUTE, a.fecha_notificacion, NOW()) >= %s
        """, (timeout_minutos,))
        return cur.fetchall()
    except Exception as e:
        logging.error(f"Error obteniendo acuses pendientes: {e}")
        return []
    finally:
        cur.close()
        conn.close()


# ══════════════════════════════════════════════════════════════
# AVANCES
# ══════════════════════════════════════════════════════════════

def registrar_avance(wonum, chat_id, nombre_tecnico, texto, exitoso):
    """
    Registra un avance insertado desde el bot.
    """
    try:
        conn = _conectar()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO ot_avances
                (wonum, chat_id, nombre_tecnico, texto_avance, exitoso)
            VALUES (%s, %s, %s, %s, %s)
        """, (wonum, chat_id, nombre_tecnico, texto, exitoso))
        conn.commit()
        logging.info(f"Avance registrado: OT {wonum} por {nombre_tecnico}")
        return True
    except Exception as e:
        logging.error(f"Error registrando avance OT {wonum}: {e}")
        return False
    finally:
        cur.close()
        conn.close()