import os
import json
import boto3
import requests
import pandas as pd
import pyarrow as pa
import awswrangler as wr
import pyarrow.parquet as pq
from datetime import datetime

class ApiDataSaver():

    def token_api(self):
        client = boto3.session.Session(profile_name='CRISTIAN_AWS').client('secretsmanager', region_name='us-east-1')
        response = client.get_secret_value(SecretId='env/openweather')
        secret_data = response['SecretString']
        secrets_dict = json.loads(secret_data)
        open_weather_token = secrets_dict.get('OPEN_WEATHER_SECRET', '')  # Correção aqui
        API_LINK = f"https://api.openweathermap.org/data/2.5/weather?q=novo hamburgo,br&APPID={open_weather_token}"
        return API_LINK

    def requisicao(self, API_LINK):
        requisicao = requests.get(API_LINK)
        wheater_json = requisicao.json()
        weather_json_string = json.dumps(wheater_json)
        return weather_json_string
