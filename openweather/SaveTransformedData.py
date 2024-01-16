import boto3
import pandas as pd
from datetime import datetime

from TransformData import TransformData

"""
SaveTransformedData Module

This module defines a class, SaveTransformedData, which is responsible for saving transformed data to an AWS S3 bucket.
It uses the Boto3 library for AWS interaction and Pandas for handling data.

Usage:
    - Instantiate the TransformData class to get the transformed data.
    - Call the 'catuabinha' method of SaveTransformedData to save the transformed data to the specified S3 bucket.

Note:
    - Before running this code, ensure that the necessary AWS credentials are properly configured.
"""

class SaveTransformedData():

    """
    SaveTransformedData Module
    
    This module defines a class, SaveTransformedData, which is responsible for saving transformed data to an AWS S3 bucket.
    It uses the Boto3 library for AWS interaction and Pandas for handling data.

    Usage:
        - Instantiate the TransformData class to get the transformed data.
        - Call the 'catuabinha' method of SaveTransformedData to save the transformed data to the specified S3 bucket.

    Note:
        - Before running this code, ensure that the necessary AWS credentials are properly configured.
    """

    def catuabinha(df):

        """
        catuabinha Method

        Saves the provided DataFrame to a Parquet file and uploads it to an S3 bucket.

        Parameters:
            - df (pd.DataFrame): Transformed data to be saved.

        Returns:
            - None
        """

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
