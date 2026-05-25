"""
Lambda: lambda_alertas
=======================
Se dispara cuando llega un nuevo archivo Parquet a:
    s3://air-quality-medellin-2026/curated/predictions/

Lee las predicciones, filtra las que tienen probabilidad alta de
contaminacion peligrosa y publica una alerta al topico SNS.

Trigger: S3 ObjectCreated en prefijo curated/predictions/
Runtime: Python 3.11
Layer:   AWSSDKPandas-Python311 (para leer Parquet con pyarrow)

Variables de entorno requeridas:
    SNS_TOPIC_ARN   ARN del topico air-quality-alerts
    PROB_THRESHOLD  Probabilidad minima para alertar (ej. 0.70)
    HOURS_WINDOW    Ventana en la que se revisan las alertas
"""

import json
import logging
import os
from datetime import datetime, timedelta
from io import BytesIO
from urllib.parse import unquote_plus

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SNS_TOPIC_ARN  = os.environ["SNS_TOPIC_ARN"]
PROB_THRESHOLD = float(os.environ.get("PROB_THRESHOLD", "0.70"))
HOURS_WINDOW   = int(os.environ.get("HOURS_WINDOW", "24"))

s3  = boto3.client("s3")
sns = boto3.client("sns")


def lambda_handler(event, context):
    """Punto de entrada. Procesa cada archivo S3 del evento."""
    logger.info("Evento recibido: %s", json.dumps(event))

    total_alerts = 0
    total_files  = 0

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        # S3 envia la key URL-encoded (ej. station_id%3DMED). Hay que decodificar.
        key = unquote_plus(record["s3"]["object"]["key"])
        logger.info("Procesando: s3://%s/%s", bucket, key)

        # Solo procesamos archivos Parquet (Glue tambien escribe _SUCCESS, metadata, etc.)
        if not key.endswith(".parquet"):
            logger.info("Ignorando archivo no-parquet: %s", key)
            continue

        predictions = read_parquet_predictions(bucket, key)
        alerts      = filter_dangerous(predictions, PROB_THRESHOLD)

        if alerts:
            message = format_message(alerts)
            publish_alert(message, alert_count=len(alerts))
            total_alerts += len(alerts)
        else:
            logger.info("Sin predicciones peligrosas en %s", key)

        total_files += 1

    return {
        "statusCode": 200,
        "body": json.dumps({
            "files_processed": total_files,
            "alerts_sent":     total_alerts,
        }),
    }


def read_parquet_predictions(bucket, key):
    """Lee un archivo Parquet de S3 y devuelve lista de dicts."""
    import pyarrow.parquet as pq

    obj   = s3.get_object(Bucket=bucket, Key=key)
    body  = obj["Body"].read()
    table = pq.read_table(BytesIO(body))
    return table.to_pylist()


def filter_dangerous(predictions, threshold):
    """
    Filtra registros con:
      - contaminacion_alta_pred = 1.0
      - probabilidad_alerta >= threshold
      - timestamp_parsed dentro de las ultimas HOURS_WINDOW horas
    """
    cutoff = datetime.utcnow() - timedelta(hours=HOURS_WINDOW)
    dangerous = []

    for pred in predictions:
        # Filtro 1: prediccion peligrosa
        if pred.get("contaminacion_alta_pred") != 1.0:
            continue

        # Filtro 2: probabilidad suficiente
        prob = pred.get("probabilidad_alerta", 0.0)
        if prob is None or float(prob) < threshold:
            continue

        # Filtro 3: timestamp reciente
        ts_raw = pred.get("timestamp_parsed")
        ts_dt  = parse_timestamp(ts_raw)
        if ts_dt is None or ts_dt < cutoff:
            continue

        dangerous.append(pred)

    logger.info(
        "Peligrosas: %d de %d (umbral %.2f, ventana %dh)",
        len(dangerous), len(predictions), threshold, HOURS_WINDOW,
    )
    return dangerous


def parse_timestamp(ts_raw):
    """Convierte el timestamp del Parquet a datetime UTC. Acepta str o datetime."""
    if ts_raw is None:
        return None

    # Spark guarda timestamps como objetos datetime
    if isinstance(ts_raw, datetime):
        return ts_raw

    # Por si llega como string
    if isinstance(ts_raw, str):
        try:
            return datetime.fromisoformat(ts_raw.replace("Z", ""))
        except ValueError:
            return None

    return None


def format_message(alerts):
    """Construye el mensaje de alerta en espanol."""
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    header = (
        "ALERTA - CALIDAD DEL AIRE VALLE DE ABURRA\n"
        f"Fecha de deteccion: {now}\n"
        f"Estaciones afectadas: {len(alerts)}\n"
        "------------------------------------------\n\n"
    )

    lines = []
    for alert in alerts:
        station  = alert.get("station_name", "desconocida")
        aqi      = alert.get("aqi", "n/a")
        pm25     = alert.get("pm25", "n/a")
        prob_pct = round(float(alert["probabilidad_alerta"]) * 100, 1)
        ts       = alert.get("timestamp_parsed", "n/a")

        line = (
            f"Estacion: {station}\n"
            f"  Estado:        PELIGROSO\n"
            f"  Probabilidad:  {prob_pct}%\n"
            f"  AQI actual:    {aqi}\n"
            f"  PM2.5:         {pm25}\n"
            f"  Timestamp:     {ts}\n"
        )
        lines.append(line)

    footer = (
        "\n------------------------------------------\n"
        "Recomendaciones:\n"
        "  - Evitar actividad fisica al aire libre.\n"
        "  - Personas con asma: usar mascarilla.\n"
        "  - Cerrar ventanas y limitar exposicion.\n\n"
        "Alerta generada automaticamente por el pipeline.\n"
        "Equipo Calidad del Aire - Universidad EAFIT"
    )

    return header + "\n".join(lines) + footer


def publish_alert(message, alert_count):
    """Publica el mensaje al topico SNS."""
    plural  = "es" if alert_count > 1 else ""
    subject = f"Alerta Calidad del Aire ({alert_count} estacion{plural})"

    response = sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject[:100],   # SNS limita subject a 100 chars
        Message=message,
    )
    logger.info("SNS publicado. MessageId: %s", response["MessageId"])
