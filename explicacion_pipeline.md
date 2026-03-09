# Explicación Completa del Pipeline — Forest eBikes London Data Pipeline

---

## 1. Pipeline Completo

### Vista General

```
Cloud Scheduler (cron daily)
    ↓ HTTP POST (OIDC auth)
Cloud Functions Gen 2 x7 (Python extractors)
    ↓ MERGE statement
BigQuery `raw` dataset (7 tablas)
    ↓ dbt Cloud Run Job
BigQuery `mart` dataset (3 mart tables + 7 staging views)
    ↓ (Looker Studio — futuro)
Dashboard
```

### Disparador: Cloud Scheduler

Cada día a las 06:xx UTC, Cloud Scheduler dispara 7 jobs HTTP escalonados (cada 5 minutos para evitar colisiones):

| Hora UTC | Función |
|---|---|
| 06:00 | weather |
| 06:05 | air quality |
| 06:10 | news |
| 06:15 | countries |
| 06:20 | TfL bikepoints |
| 06:25 | bank holidays |
| 06:30 | crime |

El scheduler usa una **Service Account** con el rol `cloudfunctions.invoker` y genera un **OIDC token** para autenticar el request. Sin ese token, la Cloud Function rechaza el request (HTTP 403).

### Cloud Functions Gen 2 — El Corazón del Pipeline

Cada función sigue **exactamente el mismo patrón de 4 pasos**:

```
[Extract] → [Validate] → [Load (MERGE)] → [Return JSON]
```

**Paso 1 — Extract:**
La función llama a la API externa con `httpx` (cliente HTTP moderno, async-capable) con **3 reintentos automáticos** usando `tenacity` (backoff exponencial: 1s, 2s, 4s). Si la API falla 3 veces, la función lanza excepción.

**Paso 2 — Validate (quality.py):**
Antes de escribir a BigQuery, corre validaciones de calidad:
- Checks de nulos en columnas críticas
- Checks de rango (e.g., temperatura entre -50°C y 60°C)
- Detección de duplicados dentro del batch
Si falla → HTTP 500, no escribe nada a BigQuery.

**Paso 3 — Load con MERGE:**
Usa `google-cloud-bigquery` para ejecutar un `MERGE` statement en BigQuery. Esto garantiza **idempotencia**: si la función corre dos veces el mismo día, no inserta duplicados — simplemente el segundo run no hace nada porque las claves ya existen.

**Paso 4 — Return JSON:**
```json
{
  "status": "ok",
  "rows_inserted": 168,
  "quality": {"passed": true, "null_violations": [], "range_violations": [], "duplicate_keys": 0}
}
```

### BigQuery — Raw Layer

7 tablas en el dataset `raw`:

| Tabla | API | Filas típicas/día |
|---|---|---|
| `raw_weather` | Open-Meteo forecast | 168 (7 días × 24h) |
| `raw_air_quality` | Open-Meteo AQ | 96 (4 pollutants × 24h) |
| `raw_news` | NewsAPI | ~20 artículos |
| `raw_countries` | RestCountries | 1 (UK, idempotente) |
| `raw_tfl_bikepoints` | TfL API | ~800 estaciones |
| `raw_bank_holidays` | GOV.UK | ~280 (todos los años) |
| `raw_crime` | UK Police API | ~5000 incidentes/mes |

Cada tabla tiene:
- `ingested_at` — timestamp de cuándo se escribió
- `_source` — identificador del API para trazabilidad
- **MERGE key** — columna(s) únicas que previenen duplicados

### dbt — Transform Layer

dbt es la herramienta que transforma datos en BigQuery usando SQL versionado. Corre como **Cloud Run Job** (container one-shot que ejecuta y termina).

**Staging layer** (7 views — no materializan datos, son SQL on-the-fly):
- Castean tipos, agregan flags calculados
- Ejemplo: `stg_tfl_bikepoints` agrega `occupancy_rate = nb_bikes / nb_docks`
- Ejemplo: `stg_crime` agrega `is_bicycle_theft = (category = 'bicycle-theft')`

**Mart layer** (3 tablas materializadas):
- `mart_city_conditions` — weather + air quality agregado por día
- `mart_news_enriched` — noticias + metadata de UK (población, región)
- `mart_safety_overview` — crimen + TfL bikes + bank holidays por mes

### Monitoreo

Cloud Monitoring tiene configurado:
- **Log-based metric**: cuenta logs con severity=ERROR de las Cloud Functions
- **Alert policy**: si hay >0 errores en 5 minutos → dispara alerta
- **Notification channel**: email a juan.luis.aracena@gmail.com

---

## 2. Decisiones Técnicas

### Cloud Functions Gen 2 vs. alternativas

**¿Por qué Cloud Functions?**

- **Cloud Composer**: ~$300/mes solo por tener el ambiente activo. Overkill para 7 funciones simples que corren 1 vez al día.
- **Cloud Run service**: Siempre activo, paga por requests — válido, pero requiere más configuración de servidor.
- **Cloud Functions Gen 2**: Serverless puro. Pagas solo cuando corre (segundos de ejecución). Para este caso son centavos al mes.

Gen 2 específicamente (sobre Gen 1) porque usa **Cloud Run internamente**, lo que da:
- Timeout hasta 60 minutos (Gen 1 máximo 9 min)
- Más memoria (hasta 16 GB)
- Instancias concurrentes

### Cloud Scheduler vs. Cloud Composer

| | Cloud Composer | Cloud Scheduler |
|---|---|---|
| Costo | ~$300/mes | ~$0.10/mes |
| Features | DAGs, dependencias, retry UI, backfill | Cron simple, HTTP |
| Complejidad | Alta (Kubernetes under the hood) | Mínima |
| Para este caso | Overkill | Suficiente |

**Decisión:** Cloud Scheduler. Las 7 funciones son independientes entre sí — no hay dependencias DAG reales. El escalonamiento de 5 minutos evita cualquier problema de concurrencia en BigQuery.

> Si el proyecto creciera a 50+ fuentes con dependencias complejas, migraría a Composer o Prefect.

### BigQuery vs. PostgreSQL

- **Escala**: BigQuery maneja petabytes sin configuración. PostgreSQL requiere tuning, índices, vacuums.
- **Serverless**: No hay servidor que mantener, parchear, escalar.
- **Integración nativa**: Cloud Functions → BigQuery con la librería oficial sin drivers adicionales.
- **dbt + BigQuery**: Combinación estándar en la industria para data engineering.
- **Costo**: El tier gratuito cubre 10 GB storage + 1 TB queries/mes — este proyecto usa <1 GB.

### MERGE en vez de INSERT

```sql
MERGE `raw.raw_weather` T
USING staging S ON T.forecast_date = S.forecast_date AND T.location_city = S.location_city
WHEN NOT MATCHED THEN INSERT (...)
```

**¿Por qué?** Idempotencia. Si el scheduler dispara dos veces (retry automático, falla de red, re-deploy), la segunda ejecución no duplica datos.

### httpx + tenacity para HTTP

- **httpx** sobre `requests`: API moderna, soporte async nativo, mejor manejo de timeouts.
- **tenacity**: 3 intentos, backoff exponencial (1s → 2s → 4s), solo reintenta en errores de red/5xx — no en 4xx.

### Terraform modular

```
modules/
  bigquery/        ← datasets + tablas
  cloud_functions/ ← zip + deploy de las 7 funciones
  cloud_scheduler/ ← 7 jobs + service account
  secret_manager/  ← news-api-key
  monitoring/      ← metric + alert + email
```

Cada módulo es desplegable por separado: `terraform apply -target=module.cloud_functions`.

### Secret Manager para NewsAPI key

La única API que requiere key es NewsAPI. Se guarda en Secret Manager y la Cloud Function la lee en runtime. El acceso está controlado por IAM — solo la service account de las funciones puede leerlo.

### Open-Meteo AQ en vez de OpenAQ v2

OpenAQ v2 fue deprecada completamente (HTTP 410 Gone). Open-Meteo ofrece los mismos pollutants (PM10, PM2.5, NO₂, O₃) sin API key, sin límites de rate.

---

## 3. Los Datos — Qué hay en BigQuery y de dónde viene

### `raw_weather` — Open-Meteo Forecast
- Pronóstico horario para Londres (51.51, -0.13)
- 168 filas por run (7 días × 24 horas)
- Útil para correlacionar clima con demanda de bicicletas

### `raw_air_quality` — Open-Meteo Air Quality
- 4 contaminantes: PM10, PM2.5, NO₂, O₃
- 96 filas por run (4 pollutants × 24 horas)
- Útil para medir calidad del aire y argumentar transporte verde

### `raw_news` — NewsAPI
- Query: `"London electric bike OR micro-mobility OR forest ebikes"`
- Hasta 20 artículos por run
- Dedup key: MD5 del URL
- Útil para monitoreo de marca y tendencias del sector

### `raw_countries` — RestCountries
- Metadata estática del país GB (nombre, capital, población, área, región, idiomas, monedas)
- 1 fila, idempotente
- Tabla de dimensión para enriquecer otros datasets

### `raw_tfl_bikepoints` — TfL BikePoint API
- Estado actual de ~800 estaciones Santander Cycles
- nb_bikes, nb_empty_docks, nb_docks por estación
- Competencia directa de Forest eBikes

### `raw_bank_holidays` — GOV.UK
- Todos los feriados UK para 3 divisiones
- ~280 filas, idempotente
- Correlaciona feriados con movilidad y crimen

### `raw_crime` — UK Police API
- Crimen callejero del último mes, radio 1km del centro de Londres
- ~5000 filas por run
- `bicycle-theft` es la métrica clave para Forest eBikes

### Flujo en BigQuery

```
raw_weather + raw_air_quality  ──→ mart_city_conditions
raw_news + raw_countries       ──→ mart_news_enriched
raw_crime + raw_tfl + raw_bank ──→ mart_safety_overview
```

---

## 4. dbt — Qué es y Cómo lo Implementamos

### ¿Qué es dbt?

**dbt (data build tool)** es la capa `T` del ELT — transforma datos que ya están en el warehouse usando SQL versionado.

**Lo que dbt NO hace:** extraer ni cargar datos.

**Lo que dbt SÍ hace:**
- Compila SQL con Jinja templating (`{{ ref('stg_weather') }}`)
- Resuelve dependencias entre modelos automáticamente
- Materializa resultados como views o tables en BigQuery
- Corre tests de calidad (`not_null`, `unique`)
- Genera documentación del lineage automáticamente

### El Concepto de Modelo

```sql
-- models/staging/stg_weather.sql
SELECT
    CAST(forecast_date AS TIMESTAMP) AS forecast_date,
    CAST(temperature_2m AS FLOAT64)  AS temperature_2m,
    CASE weathercode
        WHEN 0  THEN 'Clear sky'
        WHEN 1  THEN 'Mainly clear'
        WHEN 61 THEN 'Rain: Slight'
        ELSE         'Other'
    END AS weather_description
FROM {{ source('raw', 'raw_weather') }}
```

### Staging Layer (7 views)

| Modelo | Columna agregada |
|---|---|
| `stg_weather` | `weather_description` (CASE WHEN sobre weathercode) |
| `stg_air_quality` | filtra `value < 0` (errores de sensor) |
| `stg_tfl_bikepoints` | `occupancy_rate = nb_bikes / nb_docks` |
| `stg_bank_holidays` | `is_england_wales` flag |
| `stg_crime` | `is_resolved`, `is_bicycle_theft` flags |
| `stg_news` | `is_recent` (publicado en últimos 7 días) |
| `stg_countries` | solo casteos |

### Mart Layer (3 tablas)

**`mart_city_conditions`** — weather + air quality por día
**`mart_news_enriched`** — noticias recientes + metadata UK (CROSS JOIN con countries)
**`mart_safety_overview`** — crimen + TfL bikes + bank holidays por mes

### Por Qué dbt vs. SQL directo

| | SQL directo | dbt |
|---|---|---|
| Versionado | Manual | Git nativo |
| Dependencias | Manual | Automático (`{{ ref() }}`) |
| Tests | Manuales | Declarativos |
| Documentación | Manual | Auto-generada |

### Estado actual en GCP

Cloud Run module comentado en Terraform — Cloud Run no soporta imágenes de `ghcr.io` directamente, requiere copiarla a Artifact Registry primero. Los modelos SQL están escritos y funcionales; es un paso de infraestructura pendiente.

---

## 5. Cloud Functions — Código Detallado

### Patrón Universal (fn_extract_weather como ejemplo)

#### Imports y Constantes

```python
import functions_framework
from quality import validate
import httpx
from google.cloud import bigquery
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

_OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
_SOURCE = "open-meteo"
_TABLE = "raw_weather"
```

Las constantes se inicializan una sola vez al cargar el container — no en cada request.

#### Lógica de Retry

```python
def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500  # solo 5xx
    return isinstance(exc, (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError))

@retry(
    retry=retry_if_exception(_is_retryable),
    wait=wait_exponential(multiplier=1, min=2, max=10),  # 2s → 4s → 8s
    stop=stop_after_attempt(3),
    reraise=True,
)
```

- HTTP 5xx → reintenta (el servidor puede recuperarse)
- HTTP 4xx → NO reintenta (error de configuración, reintentar no ayuda)
- Errores de red → reintenta

#### Extractor `_extract()`

Open-Meteo devuelve arrays paralelos que la función transpone en lista de dicts:

```python
return [
    {
        "ingested_at": ingested_at,  # capturado al inicio del batch
        "forecast_date": hourly["time"][i],
        "temperature_2m": hourly["temperature_2m"][i],
        ...
    }
    for i in range(len(hourly["time"]))
]
```

#### Loader `_load_to_bq()` — Patrón Staging + MERGE

```python
# 1. Carga a staging con WRITE_TRUNCATE (schema explícito)
client.load_table_from_json(records, staging_table, job_config=job_config).result()

# 2. MERGE a tabla destino (idempotente)
merge_sql = """
    MERGE `raw_weather` T
    USING `_staging_raw_weather` S
    ON T.forecast_date = S.forecast_date AND T.location_city = S.location_city
    WHEN NOT MATCHED THEN INSERT (...)
"""
result = client.query(merge_sql).result()
```

`.result()` bloquea hasta confirmación de BigQuery — sin esto la función retornaría antes de que los datos estén escritos.

#### Handler HTTP

```python
@functions_framework.http
def handler(request):
    project = os.environ.get("BQ_PROJECT", "")   # inyectado por Terraform
    dataset = os.environ.get("BQ_DATASET", "raw")
    try:
        records = _extract()
        report = validate(_TABLE, records)         # quality check
        if not report.passed:
            return {"status": "error", "quality": report.to_dict()}, 500  # fail-fast
        inserted = _load_to_bq(records, project, dataset)
        return {"status": "ok", "rows_inserted": inserted, "quality": report.to_dict()}, 200
    except Exception as exc:
        logger.exception("...")                    # stack trace completo → Cloud Monitoring
        return {"status": "error", "message": str(exc)}, 500
```

### Diferencias por Función

| Función | Diferencia notable en `_extract()` |
|---|---|
| `fn_extract_airquality` | Transpone 4 pollutants en filas separadas |
| `fn_extract_news` | Lee API key de Secret Manager antes del request |
| `fn_extract_countries` | Un JSON → 1 fila, parsea arrays de idiomas/monedas |
| `fn_extract_tfl` | Itera ~800 estaciones, parsea `additionalProperties` anidados |
| `fn_extract_bankholidays` | Itera 3 divisiones del JSON |
| `fn_extract_crime` | Parsea objeto `location` anidado, maneja `outcome_status` nullable |
