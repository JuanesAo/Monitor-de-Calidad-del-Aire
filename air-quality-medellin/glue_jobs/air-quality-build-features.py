import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql import Window

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'CURATED_PATH', 'FEATURES_PATH'])

sc      = SparkContext()
glueCtx = GlueContext(sc)
spark   = glueCtx.spark_session
job     = Job(glueCtx)
job.init(args['JOB_NAME'], args)

CURATED  = args['CURATED_PATH']
FEATURES = args['FEATURES_PATH']

print(f"Leyendo curated desde: {CURATED}")
df = spark.read.parquet(CURATED)

# ── Ventana por estacion ordenada por tiempo ─────────────────
w_station = Window.partitionBy("station_id").orderBy("timestamp_parsed")
w_24h     = Window.partitionBy("station_id").orderBy(F.col("timestamp_parsed").cast("long")).rangeBetween(-86400, 0)
w_6h      = Window.partitionBy("station_id").orderBy(F.col("timestamp_parsed").cast("long")).rangeBetween(-21600, 0)
w_1h      = Window.partitionBy("station_id").orderBy(F.col("timestamp_parsed").cast("long")).rangeBetween(-3600,  0)

# ── Calcular features ─────────────────────────────────────────
df = df.withColumn("pm25_prom_24h",    F.avg("pm25").over(w_24h))
df = df.withColumn("pm25_prom_6h",     F.avg("pm25").over(w_6h))
df = df.withColumn("aqi_prom_24h",     F.avg("aqi").over(w_24h))
df = df.withColumn("aqi_max_24h",      F.max("aqi").over(w_24h))
df = df.withColumn("aqi_prom_1h",      F.avg("aqi").over(w_1h))

# ── Tendencia: diferencia AQI actual vs lectura anterior ──────
df = df.withColumn("aqi_anterior",     F.lag("aqi", 1).over(w_station))
df = df.withColumn("tendencia_aqi",    F.col("aqi") - F.col("aqi_anterior"))

# ── Variables temporales ──────────────────────────────────────
df = df.withColumn("hora_dia",         F.hour("timestamp_parsed"))
df = df.withColumn("dia_semana",       F.dayofweek("timestamp_parsed"))
df = df.withColumn("es_fin_semana",    (F.col("dia_semana").isin([1, 7])).cast("int"))

# ── Variable objetivo para ML ─────────────────────────────────
# aqi >= 3 = Insalubre para grupos sensibles o peor
df = df.withColumn("contaminacion_alta", (F.col("aqi") >= 3).cast("int"))

# ── Columnas finales ──────────────────────────────────────────
df_features = df.select(
    "station_id", "station_name",
    "latitude", "longitude",
    "timestamp_parsed", "curated_date",
    "aqi", "aqi_category",
    "pm25", "pm10", "co", "no2", "o3",
    "pm25_prom_24h", "pm25_prom_6h",
    "aqi_prom_24h", "aqi_max_24h", "aqi_prom_1h",
    "tendencia_aqi",
    "hora_dia", "dia_semana", "es_fin_semana",
    "contaminacion_alta"
).dropna(subset=["pm25_prom_24h", "aqi_prom_24h"])

print(f"Registros con features: {df_features.count()}")
df_features.show(5, truncate=False)

# ── Escribir en S3 ────────────────────────────────────────────
print(f"Escribiendo features en: {FEATURES}")
(
    df_features.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("curated_date")
    .save(FEATURES)
)

print("Features generadas exitosamente")
job.commit()