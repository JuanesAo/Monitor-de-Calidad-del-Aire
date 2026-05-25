import json
import boto3
import os

def lambda_handler(event, context):
    # Initialize the OpenWeather API parameters
    api_key = os.environ['OPENWEATHER_API_KEY']
    city = 'Medellin'
    url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}'

    # Call the OpenWeather API
    response = requests.get(url)
    
    if response.status_code == 200:
        weather_data = response.json()
        
        # Send the data to Kinesis
        kinesis_client = boto3.client('kinesis')
        kinesis_stream_name = os.environ['KINESIS_STREAM_NAME']
        
        # Put the weather data into the Kinesis stream
        kinesis_client.put_record(
            StreamName=kinesis_stream_name,
            Data=json.dumps(weather_data),
            PartitionKey='partitionkey'
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps('Data ingested successfully')
        }
    else:
        return {
            'statusCode': response.status_code,
            'body': json.dumps('Failed to retrieve data from OpenWeather')
        }