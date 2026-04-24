# Modularizacion `tabla_maestra_4213.py`

Pipeline ETL para extraer OTs del ownergroup `O_GESFO` (classstructureid `4213`)
desde Maximo y cargarlas a uno o varios destinos (Excel, MySQL, PostgreSQL, ...).

## Arquitectura

```
proyecto/
├── config.py                      ← YA EXISTE (agregar patch al final)
├── logger_config.py               ← YA EXISTE (reutilizado)
├── maximo_wo.py                   ← YA EXISTE (tus operaciones 1-a-1)
├── oracle_maximo.py               ← YA EXISTE (enriquecimiento opcional)
├── database.py                    ← YA EXISTE (bot_gesfo)
│
├── campos_gesfo.py                ← NUEVO: constantes de dominio
├── maximo_extract.py              ← NUEVO: extraccion masiva + paginacion
├── tabla_maestra_4213.py          ← NUEVO: orquestador principal
│
├── transformers/
│   └── ot_transformer.py          ← NUEVO: logica pura
│
└── exporters/
    ├── base.py                    ← NUEVO: interfaz Exporter
    ├── excel_exporter.py          ← NUEVO: Excel multi-hoja
    ├── mysql_exporter.py          ← NUEVO: reutiliza credenciales DB_*
    └── postgres_exporter.py       ← NUEVO: credenciales PG_*
```

## Pasos para integrar

### 1. Agregar constantes al `config.py` existente

Copiar el contenido de `config_patch.py` al final de tu `config.py` actual:

```python
# URL base para consultar Configuration Items (CI) en Maximo
MAXIMO_CI_URL = "http://10.80.123.11:8001/maximo/oslc/os/MXCI"

# Paginacion para extracciones masivas de OTs
MAXIMO_PAGE_SIZE = 200

# PostgreSQL (opcional)
PG_HOST     = "localhost"
PG_PORT     = 5432
PG_USER     = "postgres"
PG_PASSWORD = ""
PG_DATABASE = "disponibilidad"
```

### 2. Copiar los archivos nuevos a la raiz del proyecto

No hace falta tocar nada del codigo existente (`maximo_wo.py`,
`oracle_maximo.py`, `database.py`, `logger_config.py`). El nuevo modulo
`maximo_extract.py` solo agrega funciones de extraccion masiva
(complementa, no reemplaza, a `maximo_wo.py` que opera OT por OT).

### 3. Ejecutar

```bash
python tabla_maestra_4213.py
```

## Reutilizacion de codigo existente

| Ya existente          | Como se reutiliza                                          |
|-----------------------|------------------------------------------------------------|
| `config.py`           | Credenciales Maximo, MySQL. Solo se agregan 3 constantes.  |
| `logger_config.py`    | `from logger_config import logger` al inicio del main.     |
| `maximo_wo.py`        | No se modifica. Comparte credenciales via config.          |
| `oracle_maximo.py`    | `enriquecer_ot()` llamable opcionalmente desde el pipeline.|
| `database.py`         | `MySQLExporter` reutiliza el patron `_conectar()` y DB_*.  |

## Agregar un nuevo destino

1. Crear `exporters/mi_destino.py`:

```python
from exporters.base import Exporter
import logging

class MiDestinoExporter(Exporter):
    def export(self, registros):
        # tu logica...
        logging.info(f"MiDestino: {len(registros)} registros enviados")
        return f"MiDestino OK: {len(registros)} registros"
```

2. Activarlo en `tabla_maestra_4213.py`:

```python
from exporters.mi_destino import MiDestinoExporter

destinos = [
    ExcelExporter(),
    MiDestinoExporter(),
]
```

Sin tocar la extraccion ni la transformacion.

## Enriquecimiento con Oracle

Si quieres agregar ciudad, departamento, direccion y aliado a cada registro
(consultando `maximo.bd_v_flm_sitios` via `oracle_maximo.enriquecer_ot`),
actualiza la llamada en `tabla_maestra_4213.py`:

```python
registros = extraer_registros(
    ownergroup="O_GESFO",
    classstructureid="4213",
    enriquecer_con_oracle=True,   # <-- True
)
```

Cada registro tendra campos adicionales: `ciudad`, `departamento`,
`direccion` (de Oracle), `aliado`, `nom_sitio`. Si los quieres en el
Excel/MySQL/PG, agregalos a `CAMPOS_OT` en `campos_gesfo.py`.

## Tipado de columnas para MySQL/PostgreSQL

El upsert usa `wonum` como clave unica. DDL minimo sugerido (MySQL):

```sql
CREATE TABLE ots_gesfo_4213 (
    wonum VARCHAR(20) PRIMARY KEY,
    woclass VARCHAR(20),
    worktype VARCHAR(10),
    -- ... resto de CAMPOS_OT como VARCHAR
    reportdate DATETIME,
    schedstart DATETIME,
    actstart   DATETIME,
    -- ... resto de SPEC_CAMPOS como VARCHAR/TEXT
    description TEXT,
    ci_description TEXT,
    INDEX idx_cinum (cinum),
    INDEX idx_location (location),
    INDEX idx_reportdate (reportdate)
);
```

Si quieres, te genero el DDL completo derivado automaticamente de
`CAMPOS_OT + SPEC_CAMPOS` con tipos razonables — solo pidelo.