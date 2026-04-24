import logging
import os
from datetime import datetime

# Registro de Log
log_folder = 'logs'
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

now = datetime.now()
archivo = now.strftime('%d-%m-%Y_%H%M')

# Formato comun para ambos handlers
formato = '"%(asctime)s ; %(name)s ; %(levelname)s ; %(message)s"'
formatter = logging.Formatter(formato)

# Handler 1: archivo (mismo comportamiento que antes)
file_handler = logging.FileHandler(
    os.path.join(log_folder, f'log_{archivo}.txt'),
    mode='a',
    encoding='utf-8'
)
file_handler.setFormatter(formatter)

# Handler 2: consola (nuevo)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Configurar el root logger con ambos handlers
logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, console_handler]
)

logger = logging.getLogger("main_logger")