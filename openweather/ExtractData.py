import boto3
import pandas as pd
import awswrangler as wr

class ExtractData():
    
    """
    ExtractData Class

    A class for extracting data from an AWS S3 bucket using AWS Glue DataBrew and Wrangler.

    Methods:
        - extract_data(profile_name): Retrieves data from the specified S3 bucket.

    Attributes:
        - None
    """

    def extract_data(self, profile_name):

        """
        extract_data Method

        Retrieves data from the specified S3 bucket using AWS Glue DataBrew and Wrangler.

        Parameters:
            - profile_name (str): AWS profile name.

        Returns:
            - pd.DataFrame: Extracted data in DataFrame format.
        """
         
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
    extract = ExtractData()
    profile_name = 'CRISTIAN_AWS'
    df = extract.extract_data(profile_name=profile_name)

