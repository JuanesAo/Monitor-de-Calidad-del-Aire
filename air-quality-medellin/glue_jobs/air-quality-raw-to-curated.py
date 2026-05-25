import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, TimestampType
)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_PATH', 'TARGET_PATH'])

sc        = SparkContext()
glueCtx   = GlueContext(sc)
spark     = glueCtx.spark_session
job       = Job(glueCtx)
job.init(args['JOB_NAME'], args)

SOURCE = args['SOURCE_PATH']
TARGET = args['TARGET_PATH']

# ── Schema explícito basado en los JSON reales de Nico ──────
schema = StructType([
    StructField("station_id",   StringType(),    True),
    StructField("station_name", StringType(),    True),
    StructField("latitude",     DoubleType(),    True),
    StructField("longitude",    DoubleType(),    True),
    StructField("aqi",          IntegerType(),   True),
    StructField("co",           DoubleType(),    True),
    StructField("no2",          DoubleType(),    True),
    StructField("o3",           DoubleType(),    True),
    StructField("pm25",         DoubleType(),    True),
    StructField("pm10",         DoubleType(),    True),
    StructField("timestamp",    StringType(),    True),
])

print(f"Leyendo JSON desde: {SOURCE}")
df = spark.read.schema(schema).json(SOURCE)

print(f"Registros leidos: {df.count()}")

# ── Limpieza ────────────────────────────────────────────────
df = df.dropna(subset=["station_id", "timestamp", "pm25"])
df = df.dropDuplicates(["station_id", "timestamp"])

# ── Castear timestamp ────────────────────────────────────────
df = df.withColumn(
    "timestamp_parsed",
    F.to_timestamp(F.col("timestamp"))
)

# ── Columnas de particionado ─────────────────────────────────
df = df.withColumn("year",  F.year("timestamp_parsed").cast("string"))
df = df.withColumn("month", F.month("timestamp_parsed").cast("string"))
df = df.withColumn("day",   F.dayofmonth("timestamp_parsed").cast("string"))
df = df.withColumn("curated_date", F.to_date("timestamp_parsed"))

# ── Calcular categoria AQI ───────────────────────────────────
df = df.withColumn("aqi_category",
    F.when(F.col("aqi") == 1, "Buena")
     .when(F.col("aqi") == 2, "Moderada")
     .when(F.col("aqi") == 3, "Insalubre sensibles")
     .when(F.col("aqi") == 4, "Insalubre")
     .when(F.col("aqi") == 5, "Peligrosa")
     .otherwise("Desconocida")
)

# ── Columnas finales ─────────────────────────────────────────
df_final = df.select(
    "station_id", "station_name",
    "latitude", "longitude",
    "aqi", "aqi_category",
    "co", "no2", "o3", "pm25", "pm10",
    "timestamp_parsed",
    "curated_date", "year", "month", "day"
)

print(f"Registros limpios: {df_final.count()}")
df_final.show(5, truncate=False)

# ── Escribir Parquet particionado ────────────────────────────
print(f"Escribiendo Parquet en: {TARGET}")
(
    df_final.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("year", "month", "day")
    .save(TARGET)
)

print("ETL completado exitosamente")
job.commit()