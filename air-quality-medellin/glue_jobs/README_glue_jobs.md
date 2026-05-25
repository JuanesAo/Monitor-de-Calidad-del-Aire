# Documentación de los Glue Jobs

Este directorio contiene los scripts de PySpark utilizados en los **Glue Jobs** para procesar los datos de calidad del aire del Valle de Aburrá. Estos trabajos se ejecutan en orden secuencial como parte del pipeline ETL.

## 1. `air-quality-raw-to-curated.py`

### Propósito
Este es el primer paso del ETL (Extraer, Transformar y Cargar). Se encarga de tomar los datos "crudos" o *raw* (en formato JSON, provenientes de la ingesta de la API de OpenWeather vía Firehose) y convertirlos a un formato columnar óptimo para consultas y analítica (Parquet), guardándolos en la capa *curated*.

### Funcionamiento principal
- **Lectura:** Lee los archivos JSON desde `s3://air-quality-medellin-2026/raw/` imponiendo un esquema (`schema`) específico para evitar errores de tipado.
- **Limpieza de datos:**
  - Elimina registros nulos (en el `station_id`, `timestamp` o `pm25`).
  - Elimina duplicados si ocurren múltiples ingestas de la misma estación al mismo tiempo.
- **Transformaciones:**
  - Castea la fecha en formato string (`timestamp`) a un tipo de dato Timestamp de Spark (`timestamp_parsed`).
  - Extrae el año (`year`), mes (`month`), día (`day`) y la fecha sola (`curated_date`) para facilitar el particionado.
  - Calcula la categoría descriptiva del AQI (Índice de Calidad del Aire) basándose en las escalas de OpenWeather.
- **Escritura:** Escribe los datos limpios en S3 (`s3://air-quality-medellin-2026/curated/clean/`) en formato Parquet, particionados por `year`, `month` y `day`.

---

## 2. `air-quality-build-features.py`

### Propósito
Este trabajo corresponde al **Feature Engineering** (Ingeniería de Características). Transforma los datos limpios en un conjunto de datos enriquecido, ideal para que un modelo de Machine Learning capture patrones temporales y tendencias de contaminación.

### Funcionamiento principal
- **Lectura:** Lee los datos desde `s3://air-quality-medellin-2026/curated/clean/`.
- **Ventanas Móviles (Window Functions):**
  Aplica lógicas de ventana temporal por estación (`station_id`), ordenando los registros por tiempo, para mirar retrospectivamente y crear promedios de los últimos periodos:
  - Promedio de PM2.5 en las últimas 24 horas y 6 horas.
  - Promedio y máximo del AQI en las últimas 24 horas.
  - Promedio del AQI en la última hora.
- **Cálculo de Variables Alternas:**
  - Tendencia: Compara el AQI actual con la lectura inmediatamente anterior de la misma estación.
  - Variables de tiempo reales: Hora del día, día de la semana y una bandera (1 o 0) de si es fin de semana.
  - Generación del **Label (Variable objetivo)**: Crea una columna `contaminacion_alta` (1 si el AQI >= 3, 0 en caso contrario).
- **Escritura:** Guarda estas nuevas columnas ("Features") en `s3://air-quality-medellin-2026/curated/features/`, particionado por fecha corta (`curated_date`).

---

## 3. `air-quality-ml-training.py`

### Propósito
Es el trabajo encargado de **entrenar un modelo de Machine Learning** para predecir si habrá alta contaminación, y luego aplicar el modelo sobre los datos, guardando las predicciones resultantes. Está construido sobre **Apache Spark MLlib**.

### Funcionamiento principal
- **Pre-procesamiento:**
  - Lee los datos creados en el Job 2.
  - Asegura que todas las variables predictoras (ej. `pm25_prom_6h`, `tendencia_aqi`) y la etiqueta (`contaminacion_alta`) sean de tipo `Double`.
  - Agrupa todas las variables relevantes en un solo vector (`VectorAssembler`).
  - Escala el vector (`StandardScaler`) para estandarizar el peso estadístico de cada variable.
- **Entrenamiento y partición:**
  - Separa los datos aleatoriamente usando un *Split* 80% entrenamiento (train) y 20% pruebas (test).
  - Define un Pipeline y entrena un modelo de **Regresión Logística (Logistic Regression)**.
- **Evaluación:** Realiza predicciones sobre los datos de prueba e imprime métricas clave: AUC-ROC, Exactitud (Accuracy), y el F1 Score. Además muestra una matriz de confusión en los registros de Spark.
- **Resultados y Guardado:**
  - Serializa (guarda) el modelo de Machine Learning entrenado en `s3://air-quality-medellin-2026/curated/model/`.
  - Corre el modelo entrenado sobre todo el histórico completo y genera probabilidades y el estado alertado (`PELIGROSO` vs `NORMAL`).
  - Finalmente, escribe las predicciones finales con formato Parquet particionado en `s3://air-quality-medellin-2026/curated/predictions/`.
