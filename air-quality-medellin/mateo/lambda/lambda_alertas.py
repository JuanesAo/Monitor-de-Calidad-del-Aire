import json
import boto3
import os

def lambda_handler(event, context):
    # Initialize SNS client
    sns_client = boto3.client('sns')
    
    # Read predictions from the event
    predictions = event.get('predictions', [])
    
    # Check if there are predictions to process
    if not predictions:
        return {
            'statusCode': 400,
            'body': json.dumps('No predictions found.')
        }
    
    # Process each prediction
    for prediction in predictions:
        # Construct the alert message
        alert_message = f"Alert: Significant event detected with prediction: {prediction}"
        
        # Publish the alert to SNS
        response = sns_client.publish(
            TopicArn=os.environ['SNS_TOPIC_ARN'],
            Message=alert_message,
            Subject='Weather Prediction Alert'
        )
        
        print(f"Published alert: {alert_message} with response: {response}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Alerts published successfully.')
    }