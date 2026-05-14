"""
bot/handlers/_markdown.py
-------------------------
Utilidades para renderizar texto seguro en mensajes Markdown V1 de
Telegram.

REGLA DEL SISTEMA:
    Cualquier campo que venga de BD o de input de usuario y se vaya
    a incluir en un mensaje con parse_mode="Markdown" debe pasar por
    `md()` para escapar caracteres especiales.

Caracteres reservados de Markdown V1:
    \\ * _ ` [ ]

Sin escape, estos caracteres rompen el render del mensaje completo
y Telegram responde con BadRequest "Can't parse entities".

USO:
    from bot.handlers._markdown import md, md_or_dash, md_trunc, md_dept_abbr

    f"📍 {md(ot['ciudad'])}"
    f"  {md_trunc(ot['descripcion'])}"
    f"  📍 {md_or_dash(ot['departamento'])}"
"""

_RESERVADOS = ("\\", "*", "_", "`", "[", "]")


def md(texto):
    """
    Escapa caracteres reservados de Markdown V1.

    Acepta cualquier valor (None, str, int, etc.) y siempre retorna
    string. Si el valor es None o cadena vacia, retorna cadena vacia.

    Para los casos donde se quiere mostrar guion "—" cuando el valor
    es vacio, usar md_or_dash() en su lugar.
    """
    if texto is None:
        return ""
    s = str(texto)
    for ch in _RESERVADOS:
        s = s.replace(ch, "\\" + ch)
    return s


def md_or_dash(valor):
    """
    Como md(), pero retorna "—" si el valor es None o cadena vacia.
    Util para mostrar campos opcionales en bandejas y reportes.
    """
    if valor is None:
        return "—"
    if isinstance(valor, str) and valor.strip() == "":
        return "—"
    return md(valor)


def md_trunc(texto, maximo=100):
    """
    Trunca un texto a `maximo` caracteres (agregando "…" al final si
    fue truncado) y luego le aplica md() para escape de Markdown.
    Si el valor es None o cadena vacia, retorna "—".
    """
    if texto is None or (isinstance(texto, str) and texto.strip() == ""):
        return "—"
    t = str(texto).strip()
    if len(t) > maximo:
        t = t[:maximo].rstrip() + "…"
    return md(t)


def md_dept_abbr(depto):
    """
    Toma un departamento (ej. "VALLE DEL CAUCA"), extrae la palabra
    significativa (ej. "VALLE"), y le aplica md() por si tiene
    caracteres reservados.

    Sigue la misma logica que el helper que ya existia: salta palabras
    cortas como "LA", "EL", "LOS", "LAS", "DE", "DEL".
    """
    if depto is None or not isinstance(depto, str) or not depto.strip():
        return "—"
    palabras = depto.strip().upper().split()
    saltar = {"LA", "EL", "LOS", "LAS", "DE", "DEL"}
    for palabra in palabras:
        if palabra not in saltar:
            return md(palabra)
    return md(palabras[0])