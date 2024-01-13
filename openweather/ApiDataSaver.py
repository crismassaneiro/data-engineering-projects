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

    def __init__(self, profile_name='CRISTIAN_AWS',
                       region_name='us-east-1'):
        self.profile_name = profile_name
        self.region_name = region_name

    def token_api(profile_name, region_name ):
        client = boto3.session.Session(profile_name).client('secretsmanager', region_name)
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
    
    def saving_at_s3(self, profile_name, weather_json_string):
        s3 = boto3.session.Session(profile_name).client('s3')
        bucket_name = 'data-integration-projects'
        directory_path = 'openweather-data-lake/raw-data/'
        current_date = datetime.now()
        dia_da_execucao = current_date.strftime('%Y-%m-%d-%H-%M-%S')
        year = str(current_date.year)
        month = str(current_date.month).zfill(2)  # Adicionar zero à esquerda se necessário
        day = str(current_date.day).zfill(2)  # Adicionar zero à esquerda se necessário
        directory_path = f'{directory_path}{year}/{month}/{day}/'
        file_name = f'weather_data_{dia_da_execucao}.json'
        s3.put_object(Bucket=bucket_name, Key=directory_path + file_name, Body=weather_json_string)
        return print(f'Arquivo salvo em: {bucket_name}/{directory_path}{file_name}')
