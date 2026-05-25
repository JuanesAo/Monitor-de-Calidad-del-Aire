# 🌫️ Monitor de Calidad del Aire — Valle de Aburrá

### Pipeline de datos en tiempo real con ML en AWS | Sistemas Intensivos 2026

---

## Equipo

| Integrante       | Rol                                                | Estado        |
| ---------------- | -------------------------------------------------- | ------------- |
| **Nico**   | Infraestructura AWS + Ingesta                      | ✅ Completado |
| **Juanes** | Pipeline Glue + Features + ML                      | ✅ Completado |
| **Mateo**  | Dashboard QuickSight + Alertas SNS + Orquestación | 🔲 Pendiente  |

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

# 📋 Pendientes para Mateo

> Todo lo siguiente está listo para ser conectado. Las predicciones ya están en `curated/predictions/` particionadas por `station_id`.

## Pendiente 1 — SNS Topic + Lambda de alertas

**Crear SNS topic:**

```
Nombre: air-quality-alerts
Suscripciones: email del equipo
```

**Lambda `lambda-air-quality-alertas`:**

- Lee `s3://air-quality-medellin-2026/curated/predictions/`
- Filtra registros donde `estado_predicho = 'PELIGROSO'`
- Publica mensaje en SNS con: estación, AQI, probabilidad, timestamp
- IAM Role: LabRole
- El script base está en `mateo/lambda/lambda_alertas.py`

## Pendiente 2 — Step Functions

Crear State Machine `pipeline-aire-medellin` en Step Functions con esta secuencia:

```
Start
  → Wait 20s
  → Glue: air-quality-raw-to-curated  (.sync)
  → Wait 20s
  → Glue: air-quality-build-features  (.sync)
  → Wait 20s
  → Glue: air-quality-ml-training     (.sync)
  → Wait 20s
  → Lambda: lambda-air-quality-alertas
  → End
```

La definición JSON está en `step_functions/pipeline_state_machine.json`.

## Pendiente 3 — EventBridge (trigger horario)

Crear regla en EventBridge:

```
Nombre: trigger-pipeline-aire-horario
Schedule: rate(1 hour)
Target: Step Functions → pipeline-aire-medellin
IAM Role: LabRole
```

La configuración JSON está en `mateo/eventbridge/regla_eventbridge.json`.

## Pendiente 4 — Dashboard QuickSight

Conectar QuickSight a Athena y construir 4 paneles:

| Panel                                | Fuente                               | Tipo             |
| ------------------------------------ | ------------------------------------ | ---------------- |
| Serie de tiempo AQI por hora         | `air_quality_db.clean`             | Line chart       |
| Mapa de estaciones por nivel AQI     | `air_quality_db.clean`             | Mapa con colores |
| KPI gauge AQI máximo actual         | `air_quality_db.clean`             | KPI / Gauge      |
| Tabla predicciones / alertas activas | `curated/predictions/` vía Athena | Table            |

Ver instrucciones detalladas en `mateo/quicksight/README_quicksight.md`.

## Pendiente 5 — Diagrama de arquitectura

Crear en draw.io el diagrama completo del pipeline (ver sección Arquitectura de este README como referencia).

## Pendiente 6 — Presentación

10 slides + guion de demo en vivo:

1. Contexto problema calidad del aire Valle de Aburrá
2. Arquitectura AWS completa
3. Ingesta: Lambda + Kinesis + Firehose
4. ETL: Glue Job 1 raw→curated
5. Feature Engineering: Glue Job 2
6. Modelo ML: resultados AUC-ROC 0.857, Accuracy 76%, F1 0.757
7. Orquestación: Step Functions + EventBridge
8. Dashboard QuickSight (demo en vivo)
9. Alertas SNS (demo disparo de email)
10. Conclusiones y trabajo futuro
