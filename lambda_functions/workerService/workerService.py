import json
import boto3
import urllib3
from decimal import Decimal

# Establish connection for SQS client
sqs_client = boto3.client('sqs', region_name='us-east-1')
http = urllib3.PoolManager()

# Replace with your actual SQS queue URL and API key
SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/767397836288/weather-requests'
API_KEY = '5d5ba8a07e102ee516b77665df09c066'

def lambda_handler(event, context):
    print("Worker is processing SQS messages")
    
    for record in event['Records']:
        receipt_handle = record['receiptHandle']
        body = record['body']
        message_id = record['messageId']
        print(f"Received message: {body}")

        city, state, country = process_message(body)
        if city:
            locations = get_location(city, state, country)
            if locations:
                weather_data = get_weather(locations[0]['lat'], locations[0]['lon'], state, country)
                if weather_data['success']:
                    print(f"Weather data: {weather_data}")
                    save_to_dynamodb(message_id, city, state, country, weather_data)  # Save to DynamoDB

                # Delete message after processing
                sqs_client.delete_message(
                    QueueUrl=SQS_QUEUE_URL,
                    ReceiptHandle=receipt_handle
                )
                print(f"Deleted message with receipt handle: {receipt_handle}")
            else:
                print("Location not found, deleting message")
                sqs_client.delete_message(
                    QueueUrl=SQS_QUEUE_URL,
                    ReceiptHandle=receipt_handle
                )
                print(f"Deleted message with receipt handle: {receipt_handle} due to location not found")
        else:
            print("Invalid message, deleting from queue")
            sqs_client.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=receipt_handle
            )
            print(f"Deleted invalid message with receipt handle: {receipt_handle}")

def get_location(city, state=None, country=None):
    print(f"Fetching location for city: {city}, state: {state}, country: {country}")
    location_url = "http://api.openweathermap.org/geo/1.0/direct"
    
    if state and not country:
        country = "US"  # Default country to US if state is selected
    
    params = {
        'q': f"{city},{state},{country}" if state and country else f"{city},{state}" if state else f"{city},{country}" if country else city,
        'limit': 1,
        'appid': API_KEY
    }
    
    try:
        response = http.request('GET', location_url, fields=params)
        if response.status == 200:
            locations = json.loads(response.data.decode('utf-8'))
            print(f"Location data fetched successfully: {locations}")
            return locations
        else:
            print(f"Error fetching location data: {response.status}")
            return []
    except Exception as e:
        print(f"Error fetching location data: {e}")
        return []

def get_weather(lat, lon, state, country):
    print(f"Fetching weather for latitude: {lat}, longitude: {lon}")
    weather_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        'lat': lat,
        'lon': lon,
        'appid': API_KEY,
        'units': 'metric'
    }
    try:
        response = http.request('GET', weather_url, fields=params)
        if response.status == 200:
            weather_data = json.loads(response.data.decode('utf-8'))
            print(f"Weather data fetched successfully: {weather_data}")
            return {
                'success': True,
                'city': weather_data['name'],
                'main': weather_data['weather'][0]['main'],
                'description': weather_data['weather'][0]['description'],
                'temp': Decimal(str(weather_data['main']['temp'])),
                'feels_like': Decimal(str(weather_data['main']['feels_like'])),
                'temp_min': Decimal(str(weather_data['main']['temp_min'])),
                'temp_max': Decimal(str(weather_data['main']['temp_max'])),
                'pressure': weather_data['main']['pressure'],
                'humidity': weather_data['main']['humidity'],
                'visibility': weather_data.get('visibility', None),
                'wind_speed': Decimal(str(weather_data['wind']['speed'])),
                'wind_deg': weather_data['wind']['deg'],
                'clouds_all': weather_data['clouds']['all'],
                'coord_lat': Decimal(str(weather_data['coord']['lat'])),
                'coord_lon': Decimal(str(weather_data['coord']['lon'])),
                'state': state,
                'country': country
            }
        else:
            print(f"Error fetching weather data: {response.status}")
            return {
                'success': False,
                'error': f"HTTP status: {response.status}"
            }
    except Exception as e:
        print(f"Error fetching weather data: {e}")
        return {
            'success': False,
            'error': str(e)
        }

def process_message(message_body):
    try:
        message = json.loads(message_body)
        city = message.get('city')
        state = message.get('state', None)
        country = message.get('country', None)

        if not city:
            raise ValueError("City must be provided")

        print(f"Parsed message - City: {city}, State: {state}, Country: {country}")
        return city, state, country
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON message: {e}")
        return None, None, None
    except ValueError as e:
        print(f"Invalid message content: {e}")
        return None, None, None

def save_to_dynamodb(message_id, city, state, country, weather_data):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('WeatherData')  # Updated with correct table name
    
    item = {
        'MessageId': message_id,
        'city': city,
        'state': state,
        'country': country,
        'weather_data': weather_data
    }
    
    table.put_item(Item=item)
    print(f"Saved data to DynamoDB for message ID: {message_id}, city: {city}, state: {state}, country: {country}")

