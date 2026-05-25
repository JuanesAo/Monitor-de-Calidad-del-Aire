import pandas as pd
import numpy as np
import random

def generate_synthetic_data(num_records):
    """Generate synthetic air quality data."""
    data = {
        'timestamp': [],
        'temperature': [],
        'humidity': [],
        'pm2_5': [],
        'pm10': [],
        'co2': []
    }

    for _ in range(num_records):
        timestamp = pd.Timestamp.now() - pd.Timedelta(days=random.randint(0, 30), hours=random.randint(0, 23))
        temperature = round(random.uniform(15.0, 30.0), 2)  # Temperature in Celsius
        humidity = round(random.uniform(30.0, 90.0), 2)      # Humidity in percentage
        pm2_5 = round(random.uniform(0.0, 150.0), 2)         # PM2.5 in µg/m³
        pm10 = round(random.uniform(0.0, 150.0), 2)          # PM10 in µg/m³
        co2 = round(random.uniform(300.0, 1000.0), 2)        # CO2 in ppm

        data['timestamp'].append(timestamp)
        data['temperature'].append(temperature)
        data['humidity'].append(humidity)
        data['pm2_5'].append(pm2_5)
        data['pm10'].append(pm10)
        data['co2'].append(co2)

    return pd.DataFrame(data)

if __name__ == "__main__":
    num_records = 1000  # Specify the number of synthetic records to generate
    synthetic_data = generate_synthetic_data(num_records)
    synthetic_data.to_csv('synthetic_air_quality_data.csv', index=False)  # Save to CSV for further processing