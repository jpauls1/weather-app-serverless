import json
import boto3
 
s3 = boto3.client('s3')

def lambda_handler(event, context):
    print("Received event: ", json.dumps(event, indent=2))  # Log the entire event

    for record in event['Records']:
        if record['eventName'] == 'INSERT':
            new_image = record['dynamodb']['NewImage']
            
            print("New Image: ", json.dumps(new_image, indent=2))  # Debugging line to print the new image
            
            try:
                weather_data = {
                    'MessageId': new_image['MessageId']['S'],
                    'city': new_image['city']['S'],
                    'state': new_image['state']['S'],
                    'country': new_image['country']['S'],
                    'weather_data': {
                        'main': new_image['weather_data']['M']['main']['S'],
                        'temp': float(new_image['weather_data']['M']['temp']['N']),
                        'feels_like': float(new_image['weather_data']['M']['feels_like']['N']),
                        'temp_min': float(new_image['weather_data']['M']['temp_min']['N']),
                        'temp_max': float(new_image['weather_data']['M']['temp_max']['N']),
                        'pressure': int(new_image['weather_data']['M']['pressure']['N']),
                        'humidity': int(new_image['weather_data']['M']['humidity']['N']),
                        'visibility': int(new_image['weather_data']['M']['visibility']['N']),
                        'wind_speed': float(new_image['weather_data']['M']['wind_speed']['N']),
                        'wind_deg': int(new_image['weather_data']['M']['wind_deg']['N']),
                        'clouds_all': int(new_image['weather_data']['M']['clouds_all']['N']),
                        'coord_lat': float(new_image['weather_data']['M']['coord_lat']['N']),
                        'coord_lon': float(new_image['weather_data']['M']['coord_lon']['N'])
                    }
                }
                
                print(f"New weather data: {json.dumps(weather_data, indent=2)}")
                
                # Create a unique key for the S3 file
                key = f"{weather_data['city']}-{weather_data['state']}-{weather_data['country']}-weather_data.txt"
                bucket_name = 'jeffs-weather-app'
                
                # Upload the weather data to S3
                s3.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=json.dumps(weather_data).encode('utf-8'),
                    ContentType='application/json'
                )
                
                print(f"Weather data uploaded to S3: {key}")
            
            except KeyError as e:
                print(f"KeyError: {e} - Check the structure of the new_image")
            except Exception as e:
                print(f"Error processing new image: {e}")

    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete')
    }

