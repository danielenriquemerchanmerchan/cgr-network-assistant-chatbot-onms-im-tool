"""
bot/main.py
-----------
Bot ONMS - Etapa 7: comando /bloqueo con botones inline + /reanudar.
"""

import logging

from telegram.ext import Application, CommandHandler, CallbackQueryHandler

from core.config import TELEGRAM_TOKEN
from bot.handlers import (
    start, 
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
    if not TELEGRAM_TOKEN:
        logger.error("TELEGRAM_TOKEN no esta configurado en .env")
        return

    logger.info("=" * 60)
    logger.info("BOT ONMS - INICIO (Etapa 7: /bloqueo + /reanudar)")     # NUEVO (texto)
    logger.info("=" * 60)

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # Registrar handlers de comandos
    app.add_handler(CommandHandler("start",    start.handle))
    app.add_handler(CommandHandler("bandeja",  bandeja.handle))
    app.add_handler(CommandHandler("llegada",  llegada.handle))
    app.add_handler(CommandHandler("midiendo", midiendo.handle))
    app.add_handler(CommandHandler("hallazgo", hallazgo.handle))
    app.add_handler(CommandHandler("hallazgo",   hallazgo.handle))
    app.add_handler(CommandHandler("empalmando", empalmando.handle))
    app.add_handler(CommandHandler("validando",  validando.handle))
    app.add_handler(CommandHandler("normalizado", normalizado.handle))
    app.add_handler(CommandHandler("retiro",     retiro.handle))
    app.add_handler(CommandHandler("avance",   avance.handle))
    app.add_handler(CommandHandler("cierre",   cierre.handle))
    app.add_handler(CommandHandler("bloqueo",  bloqueo.handle))
    app.add_handler(CommandHandler("reanudar", reanudar.handle))         # NUEVO

    # Handler general para botones inline (CUALQUIER boton)
    app.add_handler(CallbackQueryHandler(callback_botones.handle))

    logger.info(
    "Handlers registrados: /start, /bandeja, /llegada, /midiendo, "
    "/hallazgo, /empalmando, /validando, /normalizado, /retiro, "
    "/avance, /cierre, /bloqueo, /reanudar + botones inline"
    )                                                                    # NUEVO (texto)
    logger.info("Iniciando polling... (Ctrl+C para detener)")

    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()