"""
bot/handlers/visita_fallida.py
------------------------------
Handler del comando /visita_fallida.

Flujo:
    1. La cuadrilla escribe en su grupo: /visita_fallida [razon]
    2. Si no hay razon -> bot muestra plantilla sugerida.
    3. Si hay razon -> bot identifica sobre que OT actua:
       - Si la cuadrilla tiene UNA sola OT activa, se aplica a ella.
       - Si tiene varias, se muestran botones para escoger.
       - Si no tiene ninguna, se rechaza.
    4. Pantalla de confirmacion con botones [Si declarar] / [Cancelar].
    5. Al confirmar:
       - UPDATE visita_fallida=true en ot_bandeja.
       - INSERT worklog tipo 'visita_fallida' con la razon.
       - Mensaje al grupo indicando el resultado.

Bloqueos posteriores: cuando la OT esta marcada visita_fallida=true, los
demas handlers de avance (/midiendo, /retiro, etc.) deben verificar con
ot_esta_bloqueada_por_visita_fallida() y responder con el mensaje
'visita_fallida.ot_bloqueada' si aplica.
"""

import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from bot.services.cuadrillas import identificar_cuadrilla_por_chat_id
from bot.services.bandeja import obtener_ots_activas_cuadrilla
from bot.services.cache_catalogos import obtener_mensaje

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# HELPER: escapar caracteres especiales de Telegram Markdown V1
# ═══════════════════════════════════════════════════════════════════

def _escapar_md(texto):
    """
    Escapa caracteres reservados de Markdown V1 de Telegram para que
    texto del usuario no rompa el formato. Caracteres reservados:
        * _ ` [ ]
    El | no es oficialmente reservado pero Telegram lo trata raro
    cuando hay otros caracteres de formato cerca, asi que tambien lo
    escapamos.
    """
    if not texto:
        return ""
    s = str(texto)
    for ch in ("\\", "*", "_", "`", "[", "]"):
        s = s.replace(ch, "\\" + ch)
    return s


# ═══════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════

async def handle(update, context):
    """
    Maneja /visita_fallida [razon].

    Reglas:
    - Solo desde grupos de cuadrilla.
    - Si no hay razon -> mostrar plantilla.
    - Si la cuadrilla tiene mas de 1 OT activa -> mostrar menu de seleccion.
    """
    chat = update.effective_chat
    chat_id = chat.id
    chat_type = chat.type

    # 1) Solo desde grupos
    if chat_type not in ("group", "supergroup"):
        await update.message.reply_text(
            "ℹ️ El comando /visita_fallida solo funciona desde el grupo "
            "de la cuadrilla."
        )
        return

    # 2) Identificar cuadrilla
    cuadrilla = identificar_cuadrilla_por_chat_id(chat_id)
    if cuadrilla is None:
        await update.message.reply_text(
            f"⚠️ Este grupo no esta registrado.\n"
            f"`chat_id = {chat_id}`",
            parse_mode="Markdown",
        )
        return

    # 3) Extraer la razon del comando (todo lo que viene despues de "/visita_fallida")
    texto_completo = update.message.text or ""
    # Quitar el comando inicial y espacios
    partes = texto_completo.split(None, 1)
    razon = partes[1].strip() if len(partes) > 1 else ""

    # 4) Si no hay razon -> mostrar plantilla
    if not razon:
        plantilla = obtener_mensaje("visita_fallida.plantilla")
        await update.message.reply_text(plantilla, parse_mode="Markdown")
        return

    # 5) Buscar las OTs activas de la cuadrilla
    ots = obtener_ots_activas_cuadrilla(cuadrilla["cuadrilla_id"])
    if not ots:
        await update.message.reply_text(
            f"ℹ️ La bandeja de *{cuadrilla['nombre']}* esta vacia. "
            f"No hay OTs sobre las cuales declarar visita fallida.",
            parse_mode="Markdown",
        )
        return

    # 6) Una sola OT -> mostrar confirmacion directa
    if len(ots) == 1:
        await _mostrar_confirmacion(
            update.message,
            ots[0]["asignacion_id"],
            ots[0]["wonum"],
            razon,
        )
        return

    # 7) Varias OTs -> menu de seleccion
    keyboard = []
    for ot in ots:
        # Truncar el wonum y razon para el callback_data si fuera muy largo
        # (Telegram limita callback_data a 64 bytes).
        # Como la razon ya esta en el texto del mensaje, en el callback solo
        # pasamos asignacion_id + un marcador. La razon la rescatamos del
        # texto del mensaje original via context.user_data.
        keyboard.append([
            InlineKeyboardButton(
                text=f"🚩 OT {ot['wonum']}",
                callback_data=f"visfall_elegir|{ot['asignacion_id']}",
            )
        ])
    keyboard.append([
        InlineKeyboardButton(
            text="⬅️ Cancelar",
            callback_data="cancelar_operacion",
        )
    ])

    # Guardamos la razon en user_data para recuperarla cuando se haga click.
    # Notese que user_data es por user_id en python-telegram-bot, y los
    # botones inline preservan ese contexto si el mismo usuario los presiona.
    if context.user_data is not None:
        context.user_data["visita_fallida_razon"] = razon

    await update.message.reply_text(
        text=(
            f"🚩 *Visita fallida - selecciona la OT*\n\n"
            f"Razón que registrarás:\n_\"{_escapar_md(razon)}\"_\n\n"
            f"Tienes {len(ots)} OTs activas. ¿Sobre cuál aplicas la "
            f"declaración?"
        ),
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


# ═══════════════════════════════════════════════════════════════════
# HELPER: pantalla de confirmacion con botones
# ═══════════════════════════════════════════════════════════════════

async def _mostrar_confirmacion(message, asignacion_id, wonum, razon):
    """
    Envia el mensaje de confirmacion con botones [Si declarar] / [Cancelar].
    Se usa tanto cuando hay UNA OT (directo) como cuando se eligio una desde
    el menu de seleccion.
    """
    texto = obtener_mensaje(
        "visita_fallida.confirmar",
        wonum=wonum,
        razon=razon,
    )

    # Empacamos asignacion_id + razon en el callback_data del boton.
    # Pero callback_data tiene limite 64 bytes. La razon puede ser larga,
    # asi que NO la pasamos en callback_data. La rescatamos del texto del
    # mensaje al ejecutar (el bot reanaliza el texto entre " y ").
    keyboard = [
        [InlineKeyboardButton(
            text="✅ Sí, declarar fallida",
            callback_data=f"visfall_confirmar|{asignacion_id}",
        )],
        [InlineKeyboardButton(
            text="⬅️ Cancelar",
            callback_data="cancelar_operacion",
        )],
    ]

    await message.reply_text(
        text=texto,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )