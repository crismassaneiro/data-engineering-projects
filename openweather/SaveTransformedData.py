import boto3
import pandas as pd
from datetime import datetime

from TransformData import TransformData

class SaveTransformedData():
        
    def catuabinha(df):

        profile_name = 'CRISTIAN_AWS'

        s3 = boto3.session.Session(profile_name=profile_name).client('s3')
        bucket_name = 'data-integration-projects'
        directory_path = 'openweather-data-lake/processed-data/'

        current_date = datetime.now()
        dia_da_execucao = current_date.strftime('%Y-%m-%d-%H-%M-%S')

        year = str(current_date.year)
        month = str(current_date.month).zfill(2)  # Adicionar zero à esquerda se necessário
        day = str(current_date.day).zfill(2)  # Adicionar zero à esquerda se necessário

        directory_path = f'{directory_path}{year}/{month}/{day}/'

        file_name = f'df_{dia_da_execucao}.parquet'

        df = pd.DataFrame({'example_key': ['example_value']})

        df.to_parquet(file_name)

        s3.upload_file(file_name, bucket_name, directory_path + file_name)
        print("Deu Bom")
        print(f'Arquivo salvo em: {bucket_name}/{directory_path}{file_name}')

if __name__ == "__main__":
    df_transformed = TransformData()
    SaveTransformedData.catuabinha(df_transformed)
