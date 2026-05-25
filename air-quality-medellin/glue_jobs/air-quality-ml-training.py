"""
Glue Job: air-quality-ml-training
==================================
Entrena un modelo de clasificación binaria con SparkML (MLlib):
  - Lee features desde curated/features/
  - Entrena Logistic Regression
  - Guarda modelo en curated/model/
  - Guarda predicciones en curated/predictions/

Parámetros del job (Job parameters en la consola Glue):
  --FEATURES_PATH    s3://air-quality-medellin-2026/curated/features/
  --MODEL_PATH       s3://air-quality-medellin-2026/curated/model/
  --PREDICTIONS_PATH s3://air-quality-medellin-2026/curated/predictions/

IAM Role: LabRole
Workers: 2 x G.1X
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import datetime

# ─────────────────────────────────────────────
# INICIALIZACIÓN GLUE
# ─────────────────────────────────────────────

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'FEATURES_PATH',
    'MODEL_PATH',
    'PREDICTIONS_PATH'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

FEATURES_PATH    = args['FEATURES_PATH']
MODEL_PATH       = args['MODEL_PATH']
PREDICTIONS_PATH = args['PREDICTIONS_PATH']

print(f"[INFO] FEATURES_PATH    : {FEATURES_PATH}")
print(f"[INFO] MODEL_PATH       : {MODEL_PATH}")
print(f"[INFO] PREDICTIONS_PATH : {PREDICTIONS_PATH}")

# ─────────────────────────────────────────────
# 1. LEER FEATURES
# ─────────────────────────────────────────────

print("[INFO] Leyendo features...")
df = spark.read.parquet(FEATURES_PATH)
print(f"[INFO] Registros leídos: {df.count()}")
df.printSchema()

# ─────────────────────────────────────────────
# 2. PREPARAR DATOS
# ─────────────────────────────────────────────

# Features que definimos en el contexto del proyecto
FEATURE_COLS = [
    "pm25_prom_6h",
    "aqi_prom_24h",
    "tendencia_aqi",
    "hora_dia",
    "dia_semana"
]

LABEL_COL = "contaminacion_alta"

# Castear todas las features a Double (MLlib lo requiere)
for col in FEATURE_COLS:
    df = df.withColumn(col, F.col(col).cast(DoubleType()))

df = df.withColumn(LABEL_COL, F.col(LABEL_COL).cast(DoubleType()))

# Eliminar filas con nulls en features o label
df_clean = df.dropna(subset=FEATURE_COLS + [LABEL_COL])
print(f"[INFO] Registros después de dropna: {df_clean.count()}")

# Verificar distribución de clases
print("[INFO] Distribución de clases (contaminacion_alta):")
df_clean.groupBy(LABEL_COL).count().show()

# ─────────────────────────────────────────────
# 3. SPLIT TRAIN / TEST (80/20)
# ─────────────────────────────────────────────

train_df, test_df = df_clean.randomSplit([0.8, 0.2], seed=42)
print(f"[INFO] Train: {train_df.count()} | Test: {test_df.count()}")

# ─────────────────────────────────────────────
# 4. PIPELINE ML
# ─────────────────────────────────────────────

# Ensamblar features en un vector
assembler = VectorAssembler(
    inputCols=FEATURE_COLS,
    outputCol="features_vec"
)

# Escalar features (importante para Logistic Regression)
scaler = StandardScaler(
    inputCol="features_vec",
    outputCol="features_scaled",
    withMean=True,
    withStd=True
)

# Modelo: Logistic Regression binaria
lr = LogisticRegression(
    featuresCol="features_scaled",
    labelCol=LABEL_COL,
    predictionCol="prediction",
    probabilityCol="probability",
    rawPredictionCol="rawPrediction",
    maxIter=100,
    regParam=0.01,      # regularización L2 leve
    elasticNetParam=0.0  # 0 = L2, 1 = L1
)

pipeline = Pipeline(stages=[assembler, scaler, lr])

# ─────────────────────────────────────────────
# 5. ENTRENAR
# ─────────────────────────────────────────────

print("[INFO] Entrenando modelo...")
model = pipeline.fit(train_df)
print("[INFO] Entrenamiento completado.")

# ─────────────────────────────────────────────
# 6. EVALUAR
# ─────────────────────────────────────────────

predictions_test = model.transform(test_df)

# AUC-ROC
evaluator_roc = BinaryClassificationEvaluator(
    labelCol=LABEL_COL,
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)
auc = evaluator_roc.evaluate(predictions_test)

# Accuracy
evaluator_acc = MulticlassClassificationEvaluator(
    labelCol=LABEL_COL,
    predictionCol="prediction",
    metricName="accuracy"
)
accuracy = evaluator_acc.evaluate(predictions_test)

# F1
evaluator_f1 = MulticlassClassificationEvaluator(
    labelCol=LABEL_COL,
    predictionCol="prediction",
    metricName="f1"
)
f1 = evaluator_f1.evaluate(predictions_test)

print("=" * 50)
print(f"[MÉTRICAS] AUC-ROC  : {auc:.4f}")
print(f"[MÉTRICAS] Accuracy : {accuracy:.4f}")
print(f"[MÉTRICAS] F1 Score : {f1:.4f}")
print("=" * 50)

# Matriz de confusión simple
print("[INFO] Matriz de confusión:")
predictions_test.groupBy(LABEL_COL, "prediction").count().orderBy(LABEL_COL, "prediction").show()

# ─────────────────────────────────────────────
# 7. GUARDAR MODELO
# ─────────────────────────────────────────────

print(f"[INFO] Guardando modelo en {MODEL_PATH}...")
model.write().overwrite().save(MODEL_PATH)
print("[INFO] Modelo guardado.")

# ─────────────────────────────────────────────
# 8. GENERAR Y GUARDAR PREDICCIONES
# ─────────────────────────────────────────────

# Predicciones sobre TODOS los datos (para que Mateo tenga histórico en QuickSight)
print("[INFO] Generando predicciones sobre todos los registros...")
predictions_all = model.transform(df_clean)

# Extraer probabilidad de clase positiva (índice 1 del vector)
extract_prob = F.udf(lambda v: float(v[1]), DoubleType())

predictions_output = predictions_all.select(
    F.col("station_id"),
    F.col("station_name"),
    F.col("timestamp_parsed"),
    F.col("aqi"),
    F.col("pm25"),
    F.col("aqi_prom_24h"),
    F.col("pm25_prom_6h"),
    F.col("tendencia_aqi"),
    F.col("hora_dia"),
    F.col("dia_semana"),
    F.col(LABEL_COL).alias("contaminacion_alta_real"),
    F.col("prediction").alias("contaminacion_alta_pred"),
    extract_prob(F.col("probability")).alias("probabilidad_alerta"),
    F.when(F.col("prediction") == 1.0, "PELIGROSO")
     .otherwise("NORMAL").alias("estado_predicho"),
    F.lit(datetime.datetime.utcnow().isoformat()).alias("prediction_timestamp")
)

print(f"[INFO] Guardando predicciones en {PREDICTIONS_PATH}...")
predictions_output \
    .write \
    .mode("overwrite") \
    .partitionBy("station_id") \
    .parquet(PREDICTIONS_PATH)

print(f"[INFO] Predicciones guardadas.")
print(f"[INFO] Total predicciones: {predictions_output.count()}")

# Preview
print("[INFO] Muestra de predicciones:")
predictions_output.select(
    "station_name", "timestamp_parsed", "aqi",
    "contaminacion_alta_pred", "probabilidad_alerta", "estado_predicho"
).orderBy(F.col("timestamp_parsed").desc()).show(10, truncate=False)

# ─────────────────────────────────────────────
# FIN
# ─────────────────────────────────────────────

job.commit()
print("[INFO] Job completado exitosamente.")