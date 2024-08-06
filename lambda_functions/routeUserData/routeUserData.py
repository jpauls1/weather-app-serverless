import json
import boto3
import logging

SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/767397836288/weather-requests'

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Initialize SQS
sqs_client = boto3.client('sqs', region_name='us-east-1')

def lambda_handler(event, context):
    try:
        logger.debug(f"Received event: {json.dumps(event)}")

        # Handle both query string parameters and body parameters
        if event.get('queryStringParameters'):
            params = event['queryStringParameters']
        else:
            params = json.loads(event.get('body', '{}'))

        logger.debug(f"Parameters: {json.dumps(params)}")

        city = params.get('city')
        state = params.get('state', '')
        country = params.get('country', '')

        if not city:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps({"success": False, "error": "City is required."})
            }

        message = {
            "city": city,
            "state": state,
            "country": country
        }

        # Send message to SQS
        response = sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(message)
        )

        logger.debug(f"Sent message to SQS: {response}")

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({"success": True, "messageId": response['MessageId']})
        }

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({'error': 'Internal Server Error'})
        }

