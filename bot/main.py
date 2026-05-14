"""
bot/main.py
-----------
Punto de entrada del bot ONMS.

ARRANQUE:
    1. Configurar logging.
    2. Verificar TELEGRAM_TOKEN.
    3. Cargar caches de catalogos desde Postgres (FALLA -> sys.exit(1)).
       Es deliberado: no queremos un bot vivo con catalogos vacios o a
       medias. Si BD no esta, el bot no debe estar.
    4. Inicializar Application de Telegram.
    5. Registrar handlers (admin + comandos + botones inline).
    6. run_polling.

CAMBIOS RECIENTES:
    - Bloque 1 Entregable 2: arranque carga cat_mensaje en memoria.
      /admin_recargar permite refrescar el cache sin reiniciar el bot.
"""

import logging
import sys

from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters


from core.config import TELEGRAM_TOKEN, NOTIFICADOR_INTERVALO_SEG
from bot.services.cache_catalogos import cargar_caches
from bot.services.notificador_ot import notificar_pendientes_job
from bot.handlers import (
    start,
    reportar,
    bandeja,
    llegada,
    midiendo,
    hallazgo,
    empalmando,
    validando,
    normalizado,
    retiro,
    avance,
    cierre,
    bloqueo,
    reanudar,
    callback_botones,
    admin,
    visita_fallida
)


# ──────────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='"%(asctime)s ; %(name)s ; %(levelname)s ; %(message)s"',
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("BOT ONMS - INICIO")
    logger.info("=" * 60)

    # 1) Validar token
    if not TELEGRAM_TOKEN:
        logger.error("TELEGRAM_TOKEN no esta configurado en .env")
        sys.exit(1)

    # 2) Cargar caches de catalogos ANTES de inicializar el bot.
    #    Si Postgres no responde, el bot NO arranca. No es aceptable
    #    quedar en linea con catalogos vacios.
    try:
        cargar_caches()
    except Exception as e:
        logger.error(
            f"Falla al cargar catalogos desde Postgres: {e}",
            exc_info=True,
        )
        logger.error(
            "El bot NO arrancara. Revisar que Postgres este activo y "
            "que las credenciales PG_* en .env sean correctas."
        )
        sys.exit(1)

    # 3) Inicializar Application
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # 4) Registrar handlers admin (privados, restringidos por ADMIN_CHAT_ID)
    app.add_handler(CommandHandler("admin_recargar", admin.recargar))

    # 5) Registrar handlers de comandos operativos
    app.add_handler(CommandHandler("start",       start.handle))
    app.add_handler(CommandHandler("reportar",    reportar.handle))
    app.add_handler(CommandHandler("bandeja",     bandeja.handle))
    app.add_handler(CommandHandler("llegada",     llegada.handle))
    app.add_handler(CommandHandler("midiendo",    midiendo.handle))
    app.add_handler(CommandHandler("hallazgo",    hallazgo.handle))
    app.add_handler(CommandHandler("empalmando",  empalmando.handle))
    app.add_handler(CommandHandler("validando",   validando.handle))
    app.add_handler(CommandHandler("normalizado", normalizado.handle))
    app.add_handler(CommandHandler("retiro",      retiro.handle))
    app.add_handler(CommandHandler("avance",      avance.handle))
    app.add_handler(CommandHandler("cierre",      cierre.handle))
    app.add_handler(CommandHandler("bloqueo",     bloqueo.handle))
    app.add_handler(CommandHandler("visita_fallida", visita_fallida.handle))
    app.add_handler(CommandHandler("reanudar",    reanudar.handle))
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND,reportar.recibir_texto_avance_libre,))
   

    # 6) Handler general para botones inline
    app.add_handler(CallbackQueryHandler(callback_botones.handle))

    # 7) Job: notificador de OTs nuevas al coordinador
    #    Corre cada NOTIFICADOR_INTERVALO_SEG segundos. Lee ot_bandeja
    #    con notificacion_coordinador_enviada_at IS NULL y manda los
    #    mensajes via Telegram. La primera ejecucion arranca 10s
    #    despues de levantarse el bot, para dar tiempo a que el polling
    #    este estable.
    app.job_queue.run_repeating(
        callback=notificar_pendientes_job,
        interval=NOTIFICADOR_INTERVALO_SEG,
        first=10,
        name="notificador_ot",
    )

    logger.info(
        "Handlers registrados: /admin_recargar, /start, /reportar, /bandeja, /llegada, "
        "/midiendo, /hallazgo, /empalmando, /validando, /normalizado, "
        "/retiro, /avance, /cierre, /bloqueo, /reanudar, /visita_fallida + botones inline"
    )
    logger.info(
        f"Job notificador_ot registrado (cada {NOTIFICADOR_INTERVALO_SEG}s)"
    )
    logger.info("Iniciando polling... (Ctrl+C para detener)")

    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()