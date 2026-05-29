# 🌫️ Monitor de Calidad del Aire — Valle de Aburrá

### Pipeline de datos en tiempo real con ML en AWS | Sistemas Intensivos 2026

---

## Equipo

| Integrante       | Rol                                                | Estado        |
| ---------------- | -------------------------------------------------- | ------------- |
| **Nico**   | Infraestructura AWS + Ingesta                      | ✅ Completado |
| **Juanes** | Pipeline Glue + Features + ML                      | ✅ Completado |
| **Mateo**  | Dashboard QuickSight + Alertas SNS + Orquestación | ✅ Completado  |

---

## Descripción del Proyecto

Sistema de monitoreo de calidad del aire para el Valle de Aburrá que ingesta datos en tiempo real desde la API de OpenWeather, los procesa mediante un pipeline ETL en AWS Glue, entrena un modelo de clasificación binaria con SparkML (MLlib) para predecir episodios de contaminación alta, y expone los resultados en un dashboard de QuickSight con alertas automáticas vía SNS.

---

## Arquitectura

```
OpenWeather API
      │  (cada 15 min)
      ▼
Lambda siata-air-quality-ingestor
      │
      ▼
Kinesis Data Streams
      │
      ▼
Kinesis Firehose ──────────────────► S3: raw/year=YYYY/month=MM/day=DD/
                                              │
                              ┌───────────────┘
                              ▼
                    Glue Job 1: raw-to-curated
                              │
                              ▼
                    S3: curated/clean/   ◄── Athena / QuickSight (Mateo)
                              │
                              ▼
                    Glue Job 2: build-features
                              │
                              ▼
                    S3: curated/features/
                              │
                              ▼
                    Glue Job 3: ml-training
                         (SparkML MLlib)
                              │
                    ┌─────────┴──────────┐
                    ▼                    ▼
           curated/model/      curated/predictions/
                                         │
                              ┌──────────┘
                              ▼
                    Lambda alertas (Mateo)
                              │
                              ▼
                         SNS Topic
                         (email/SMS)
                            
EventBridge (cada hora) ──► Step Functions ──► Jobs 1→2→3→Lambda
```

---

## Stack Tecnológico

| Capa            | Servicio                      | Detalle                                  |
| --------------- | ----------------------------- | ---------------------------------------- |
| Fuente de datos | OpenWeather Air Pollution API | AQI + CO, NO2, O3, PM2.5, PM10           |
| Ingesta         | Lambda + EventBridge          | Cada 15 minutos                          |
| Streaming       | Kinesis Data Streams          | 1 shard                                  |
| Entrega         | Kinesis Firehose              | JSON → S3 raw/                          |
| Almacenamiento  | S3                            | Bucket `air-quality-medellin-2026`     |
| ETL             | AWS Glue (PySpark)            | 3 jobs, 2 workers G.1X c/u               |
| Catálogo       | Glue Data Catalog             | Base de datos `air_quality_db`         |
| Consultas       | Amazon Athena                 | Tabla `clean`, tabla `features`      |
| ML              | Glue SparkML (MLlib)          | Logistic Regression binaria              |
| Orquestación   | Step Functions                | State Machine `pipeline-aire-medellin` |
| Scheduling      | EventBridge                   | Trigger cada hora                        |
| Alertas         | Lambda + SNS                  | Email automático si AQI ≥ 3            |
| Dashboard       | Amazon QuickSight             | 4 paneles + mapa                         |
| IAM             | LabRole                       | Cuenta AWS Academy compartida            |

---


---

# Implementación por Integrante

## 👨‍💻 Nico — Infraestructura AWS e Ingesta

### Objetivo
Construir el pipeline de streaming compatible con AWS Academy / Vocareum sin crear usuarios IAM ni roles personalizados.

### Recursos creados

| Servicio | Nombre |
|----------|---------|
| S3 Bucket | `air-quality-medellin-2026` |
| Kinesis Data Stream | `air-quality-stream` |
| Lambda | `air-quality-ingestor` |
| EventBridge Scheduler | `air-quality-15min` |
| CloudWatch Logs | `/aws/lambda/air-quality-ingestor` |

### Flujo implementado

```text
OpenWeather Air Pollution API
        ↓
Lambda air-quality-ingestor
        ↓
Kinesis Data Streams
        ↓
Kinesis Firehose
        ↓
S3 raw/year=YYYY/month=MM/day=DD/
```

### Restricciones AWS Academy

- No fue posible crear usuarios IAM.
- No fue posible administrar roles personalizados.
- Se utilizó exclusivamente `LabRole` y roles generados automáticamente por AWS.
- La solución fue diseñada para ejecutarse completamente dentro de Vocareum Learner Lab.

### Lambda de ingesta

La Lambda consulta cada 15 minutos la API de contaminación de OpenWeather para múltiples ubicaciones del Valle de Aburrá:

| Estación | Latitud | Longitud |
|----------|----------|----------|
| Medellín | 6.2442 | -75.5812 |
| Bello | 6.3373 | -75.5579 |
| Envigado | 6.1700 | -75.5870 |

Variables de entorno:

```env
OPENWEATHER_API_KEY=<api_key>
KINESIS_STREAM_NAME=air-quality-stream
```

### EventBridge

Configuración:

```text
rate(15 minutes)
```

Target:

```text
air-quality-ingestor
```

### Validación realizada

- Lambda ejecutándose correctamente.
- Datos enviados a Kinesis.
- Firehose escribiendo JSON en S3.
- CloudWatch sin errores críticos.
- Datos llegando automáticamente cada 15 minutos.

---

## 👨‍🔬 Juanes — ETL, Features y Machine Learning

Juanes implementó el pipeline de transformación de datos utilizando AWS Glue, construyendo las capas:

```text
raw/
   ↓
clean/
   ↓
features/
   ↓
model/
   ↓
predictions/
```

Responsabilidades principales:

- Limpieza y normalización de datos JSON.
- Conversión JSON → Parquet.
- Construcción de variables predictivas.
- Entrenamiento del modelo Spark MLlib.
- Exportación de predicciones hacia Athena y alertas.

---

## 👨‍💼 Mateo — Alertas, Dashboard y Orquestación

Mateo implementó:

- SNS Topic para alertas.
- Lambda de alertas.
- Step Functions.
- EventBridge de ejecución programada.
- Dashboard Streamlit.
- Exportaciones para presentación.

El dashboard consume datos provenientes de Athena y presenta indicadores operacionales y predicciones de contaminación en tiempo real.

---

## Estructura del Bucket S3

```
air-quality-medellin-2026/
├── raw/
│   └── year=2026/
│       └── month=05/
│           ├── day=24/     ← datos reales Firehose
│           └── day=25/     ← datos reales Firehose
├── curated/
│   ├── clean/              ← Parquet limpio (Job 1)
│   ├── features/           ← Features ML (Job 2)
│   ├── model/              ← Modelo serializado SparkML (Job 3)
│   └── predictions/        ← Predicciones PELIGROSO/NORMAL (Job 3)
└── athena-results/         ← Output queries Athena
```

---

## Estaciones Activas

| ID  | Nombre    | Latitud | Longitud |
| --- | --------- | ------- | -------- |
| MED | Medellín | 6.2442  | -75.5812 |
| BEL | Bello     | 6.3373  | -75.5579 |
| ENV | Envigado  | 6.1700  | -75.5870 |

---

## Estructura del Repositorio

air-quality-medellin/
├── README.md
├── ingesta/
│   └── lambda_ingestor.py          ← Lambda que llama OpenWeather → Kinesis 
├── glue_jobs/
│   ├── job1_raw_to_curated.py      ← ETL: JSON raw → Parquet limpio (Juanes)
│   ├── job2_build_features.py      ← Feature engineering con ventanas (Juanes)
│   └── job3_ml_training.py         ← SparkML Logistic Regression (Juanes)
├── ml/
│   └── generar_datos_sinteticos.py ← Script local para backfill de datos (Juanes)
├── step_functions/
│   └── pipeline_state_machine.json ← Definición Step Functions (Mateo)
└── mateo/
    ├── lambda/
    │   └── lambda_alertas.py       ← Lee predictions/ y publica en SNS (Mateo)
    ├── eventbridge/
    │   └── regla_eventbridge.json  ← Trigger horario para Step Functions (Mateo)
    └── quicksight/
        └── README_quicksight.md    ← Instrucciones conexión Athena → QuickSight (Mateo)

---

## Resultados del Modelo ML

**Algoritmo:** Logistic Regression binaria (Apache Spark MLlib)
**Objetivo:** Predecir si habrá contaminación alta (`aqi ≥ 3`) → `contaminacion_alta = 1`

### Features utilizadas

| Feature           | Descripción                       |
| ----------------- | ---------------------------------- |
| `pm25_prom_6h`  | Promedio PM2.5 últimas 6 horas    |
| `aqi_prom_24h`  | Promedio AQI últimas 24 horas     |
| `tendencia_aqi` | Diferencia AQI vs lectura anterior |
| `hora_dia`      | Hora del día (0–23)              |
| `dia_semana`    | Día de la semana (0=lunes)        |

### Dataset

|                                | Valor                   |
| ------------------------------ | ----------------------- |
| Total registros                | 2.245                   |
| Clase 0 — Normal              | 1.357 (60.4%)           |
| Clase 1 — Contaminación alta | 888 (39.6%)             |
| Split entrenamiento / test     | 80% / 20% (1.811 / 434) |

### Métricas obtenidas

| Métrica           | Valor            |
| ------------------ | ---------------- |
| **AUC-ROC**  | **0.8571** |
| **Accuracy** | **0.7604** |
| **F1 Score** | **0.7576** |

### Matriz de confusión (test set — 434 registros)

|                           | Predicho: Normal | Predicho: Peligroso |
| ------------------------- | ---------------- | ------------------- |
| **Real: Normal**    | 208 ✅ (VP)      | 40 ❌ (FP)          |
| **Real: Peligroso** | 64 ❌ (FN)       | 122 ✅ (VN)         |

**Interpretación:** El modelo tiene un AUC-ROC de 0.857, lo que indica excelente capacidad discriminativa. El recall sobre la clase peligrosa es del 65.6% (122/186), lo que significa que detecta 2 de cada 3 episodios de contaminación alta. Los 64 falsos negativos se reducirían con mayor volumen de datos históricos reales.

### Output de predicciones

Cada registro en `curated/predictions/` contiene:

```
station_name | timestamp_parsed | aqi | contaminacion_alta_pred 
| probabilidad_alerta | estado_predicho (PELIGROSO/NORMAL)
```

---

## Cómo correr el pipeline manualmente

### Prerrequisitos

- Credenciales AWS Academy activas (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`)
- Python 3.x con boto3 instalado (solo para script de datos sintéticos)

### 1. Generar datos históricos sintéticos (si se necesita backfill)

```bash
cd ml/
python generar_datos_sinteticos.py

# Subir a S3
aws s3 cp synthetic_raw_data/ \
  s3://air-quality-medellin-2026/raw/ \
  --recursive
```

### 2. Correr los Glue Jobs en orden

En la consola AWS Glue → Jobs, correr en secuencia:

```
1. air-quality-raw-to-curated
2. air-quality-build-features
3. air-quality-ml-training
```

O una vez configurado Step Functions:

```bash
# Disparar manualmente la State Machine
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:XXXX:stateMachine:pipeline-aire-medellin
```

### 3. Verificar en Athena

```sql
-- Verificar datos limpios
SELECT COUNT(*) FROM air_quality_db.clean;

-- Verificar features
SELECT station_name, aqi, pm25_prom_24h, aqi_prom_24h,
       tendencia_aqi, hora_dia, contaminacion_alta
FROM air_quality_db.features
ORDER BY timestamp_parsed DESC
LIMIT 10;
```

---

## Parámetros de los Glue Jobs

### Job 1 — `air-quality-raw-to-curated`

| Parámetro        | Valor                                             |
| ----------------- | ------------------------------------------------- |
| `--SOURCE_PATH` | `s3://air-quality-medellin-2026/raw/`           |
| `--TARGET_PATH` | `s3://air-quality-medellin-2026/curated/clean/` |
| IAM Role          | LabRole                                           |
| Workers           | 2 x G.1X                                          |

### Job 2 — `air-quality-build-features`

| Parámetro          | Valor                                                |
| ------------------- | ---------------------------------------------------- |
| `--CURATED_PATH`  | `s3://air-quality-medellin-2026/curated/clean/`    |
| `--FEATURES_PATH` | `s3://air-quality-medellin-2026/curated/features/` |
| IAM Role            | LabRole                                              |
| Workers             | 2 x G.1X                                             |

### Job 3 — `air-quality-ml-training`

| Parámetro             | Valor                                                   |
| ---------------------- | ------------------------------------------------------- |
| `--FEATURES_PATH`    | `s3://air-quality-medellin-2026/curated/features/`    |
| `--MODEL_PATH`       | `s3://air-quality-medellin-2026/curated/model/`       |
| `--PREDICTIONS_PATH` | `s3://air-quality-medellin-2026/curated/predictions/` |
| IAM Role               | LabRole                                                 |
| Workers                | 2 x G.1X                                                |

---

## Notas importantes

- **AWS Academy** tiene sesiones de máximo 4 horas. Si expiran las credenciales, renovarlas desde el portal y re-exportar `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
- **Concurrencia Glue:** Maximum concurrency = 1 en todos los jobs
- **Workers:** No usar más de 2 workers G.1X por job para conservar créditos
- Firehose escribe **JSON** (no Parquet) — el Job 1 hace la conversión a Parquet
- La tabla en Athena se llama `clean` (no `aire` ni `mediciones`)
- El AQI de OpenWeather va de 1 a 5 (1=Buena, 2=Moderada, 3=Insalubre sensibles, 4=Insalubre, 5=Peligrosa)

---

---
 
## SNS Topic + Lambda de alertas
 
**SNS Topic creado:**
```
Nombre:        air-quality-alerts
Display name:  Calidad del Aire Medellín
Tipo:          Standard
Región:        us-east-1
Suscripciones: 3 emails del equipo (Nicolás, Juan Esteban, Mateo) — confirmadas
```
 
**Lambda `lambda_alertas` desplegada:**
- Trigger: S3 `ObjectCreated` en prefijo `curated/predictions/` con sufijo `.parquet`
- Lee Parquet con `pyarrow` (layer `AWSSDKPandas-Python311`)
- Filtros aplicados:
  1. `contaminacion_alta_pred = 1.0`
  2. `probabilidad_alerta ≥ 0.70` (configurable vía env var `PROB_THRESHOLD`)
  3. `timestamp_parsed` dentro de las últimas 24 horas (configurable vía env var `HOURS_WINDOW`)
- Publica al SNS topic con resumen formateado en español
- Runtime: Python 3.11 · Memoria: 256 MB · Timeout: 1 min
- IAM Role: `LabRole`
- Decodifica URL-encoded keys de S3 (`station_id%3DMED` → `station_id=MED`)
**Variables de entorno:**
 
| Clave | Valor |
|---|---|
| `SNS_TOPIC_ARN` | `arn:aws:sns:us-east-1:501105294149:air-quality-alerts` |
| `PROB_THRESHOLD` | `0.70` |
| `HOURS_WINDOW` | `24` |
 
Código fuente: `lambda/lambda_alertas.py`
 
---
 
## Step Functions
 
**State Machine `air-quality-pipeline` creada:**
 
```
Start
  → Glue: air-quality-raw-to-curated  (.sync)
  → Glue: air-quality-build-features  (.sync)
  → Glue: air-quality-ml-training     (.sync)
  → End
```
 
- Tipo: Estándar
- IAM Role: `LabRole`
- Logging: deshabilitado (ahorro de costos)
- Probada end-to-end: los 3 jobs corren en secuencia, dispara la Lambda automáticamente vía S3 trigger al generar nuevas predicciones
Definición JSON: `step_functions/pipeline_state_machine.json`
 
---
 
## EventBridge (trigger horario)
 
**Regla `air-quality-pipeline-trigger` creada:**
 
```
Nombre:    air-quality-pipeline-trigger
Schedule:  rate(2 hours)
Target:    Step Functions → air-quality-pipeline
IAM Role:  LabRole
Estado:    Habilitada
```
 
> Se eligió ventana de 2 horas en lugar de 1 hora para reducir costos de Glue ~50% sin afectar la utilidad de las alertas (la calidad del aire no cambia drásticamente cada hora).
 
Configuración: `eventbridge/regla_eventbridge.json`
 
---
 
## Dashboard (Streamlit)
 
> **Nota:** QuickSight no estaba disponible en la cuenta AWS Academy (`voclabs/LabRole` no tiene permisos `quicksight:*`). Se optó por **Streamlit + Plotly** desplegado localmente, manteniendo el flujo de datos vía Athena → CSV.
 
**Dashboard `dashboard.py` construido con:**
- Streamlit (framework web)
- Plotly Express (gráficos interactivos)
- Pandas (procesamiento)
**Paneles incluidos:**
 
| Panel | Tipo | Fuente |
|---|---|---|
| 4 KPIs (AQI máx, AQI promedio, PM2.5 máx, % lecturas peligrosas) | Métricas | `air_quality_db.clean` |
| Serie de tiempo del AQI con umbral peligroso | Line chart | `air_quality_db.clean` |
| Mapa interactivo de estaciones | Scatter mapbox | `air_quality_db.clean` |
| Distribución por categoría AQI | Bar chart horizontal | `air_quality_db.clean` |
| Top 10 peores lecturas | Tabla | `air_quality_db.clean` |
 
**Filtros interactivos:** estaciones (multi-select) y rango de fechas.
 
**Cómo correrlo:**
```bash
pip install streamlit pandas plotly
streamlit run dashboard.py
```
Se abre automáticamente en `http://localhost:8501`.
 
**Datos:** se exportan vía Athena con la query `dashboard/query_export.sql` a CSV y se cargan al dashboard.
 
Código fuente: `dashboard/dashboard.py`
 
---
 
## ⏳ Pendiente 5 — Diagrama de arquitectura
 
<img width="1600" height="789" alt="image" src="https://github.com/user-attachments/assets/1ca350a7-73d3-42f7-8a89-aacc368921b4" />

 
---
 
## ⏳ Pendiente 6 — Presentación
 
