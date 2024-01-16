import os
import json
import boto3
import requests

from datetime import datetime

class ApiDataSaver():

    """
    ApiDataSaver Class

    A class for fetching weather data from the OpenWeather API, saving it as JSON, and storing it in an AWS S3 bucket.

    Methods:
        - token_api(): Retrieves the OpenWeather API token from AWS Secrets Manager.
        - requisicao(): Makes a request to the OpenWeather API and returns the weather data as a JSON string.
        - saving_at_s3(): Saves the weather data to a JSON file and uploads it to an S3 bucket.

    Attributes:
        - profile_name (str): AWS profile name.
        - region_name (str): AWS region name.
    """

    profile_name = 'CRISTIAN_AWS'
    region_name = 'us-east-1'

    def token_api(self):

        """
        token_api Method

        Retrieves the OpenWeather API token from AWS Secrets Manager.

        Returns:
            - str: OpenWeather API URL with the token.
        """

        client = boto3.session.Session(profile_name=self.profile_name).client('secretsmanager', region_name=self.region_name)
        response = client.get_secret_value(SecretId='env/openweather')
        secret_data = response['SecretString']
        secrets_dict = json.loads(secret_data)
        open_weather_token = secrets_dict.get('OPEN_WEATHER_SECRET', '')
        API_LINK = f"https://api.openweathermap.org/data/2.5/weather?q=novo hamburgo,br&APPID={open_weather_token}"
        return API_LINK

    def requisicao(self):

        """
        requisicao Method

        Makes a request to the OpenWeather API and returns the weather data as a JSON string.

        Returns:
            - str: Weather data in JSON format.
        """

        API_LINK = self.token_api()
        requisicao = requests.get(API_LINK)
        wheater_json = requisicao.json()
        weather_json_string = json.dumps(wheater_json)
        return weather_json_string

    def saving_at_s3(self):

        """
        saving_at_s3 Method

        Saves the weather data to a JSON file and uploads it to an S3 bucket.

        Returns:
            - None
        """
         
        weather_json_string = self.requisicao()
        s3 = boto3.session.Session(profile_name=self.profile_name).client('s3')
        bucket_name = 'data-integration-projects'
        directory_path = 'openweather-data-lake/raw-data/'
        current_date = datetime.now()
        dia_da_execucao = current_date.strftime('%Y-%m-%d-%H-%M-%S')
        year = str(current_date.year)
        month = str(current_date.month).zfill(2)
        day = str(current_date.day).zfill(2)
        directory_path = f'{directory_path}{year}/{month}/{day}/'
        file_name = f'weather_data_{dia_da_execucao}.json'
        s3.put_object(Bucket=bucket_name, Key=directory_path + file_name, Body=weather_json_string)
        print(f'Arquivo salvo em: {bucket_name}/{directory_path}{file_name}')
        
# Exemplo de uso
api_data_saver = ApiDataSaver()
api_data_saver.saving_at_s3()
