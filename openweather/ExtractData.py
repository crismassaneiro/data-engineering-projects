import boto3
import pandas as pd
import awswrangler as wr

class ExtractData:

    def extract_data(self, profile_name):
        try:
            s3 = boto3.session.Session(profile_name=profile_name)
            bucket_name = 'data-integration-projects/openweather-data-lake/raw-data'
            year = "2024"
            month = "01"
            day = "05" 
            df = wr.s3.read_json(f's3://{bucket_name + "/" + year + "/" + month + "/" + day}/*', lines=True, boto3_session=s3)
            print("Deu tudo certo")
            return df

        except Exception as e:
            print(f"Erro durante a extração de dados: {e}")
            return None

if __name__ == "__main__":
    # Crie uma instância da sua classe e forneça o nome do perfil como uma string
    extract = ExtractData()
    profile_name = 'CRISTIAN_AWS'
    df = extract.extract_data(profile_name=profile_name)
    # Agora você pode usar o DataFrame df conforme necessário
