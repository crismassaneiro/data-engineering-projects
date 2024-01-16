import pandas as pd
import awswrangler as wr

from ExtractData import ExtractData

class TransformData():

    """
    TransformData Class

    A class for transforming raw weather data obtained from the OpenWeather API.

    Methods:
        - transform_data(df): Applies transformations to the raw weather data.

    Attributes:
        - None
    """

    def transform_data(df):

        """
        transform_data Method

        Applies transformations to the raw weather data.

        Parameters:
            - df (pd.DataFrame): Raw weather data.

        Returns:
            - pd.DataFrame: Transformed weather data.
        """
        
        df['lon'] = df['coord'].apply(lambda x: x['lon'])
        df['lat'] = df['coord'].apply(lambda x: x['lat'])
        df['clouds_description'] = df['weather'].apply(lambda x: x[0]['description'] if x and isinstance(x, list) and 'description' in x[0] else None)
        df['temp'] = df['main'].apply(lambda x: x['temp'])
        df['feels_like'] = df['main'].apply(lambda x: x['feels_like'])
        df['temp_min'] = df['main'].apply(lambda x: x['temp_min'])
        df['temp_max'] = df['main'].apply(lambda x: x['temp_max'])
        df['pressure'] = df['main'].apply(lambda x: x['pressure'])
        df['humidity'] = df['main'].apply(lambda x: x['humidity'])
        df['clouds_number'] = df['clouds'].apply(lambda x: x['all'])
        df['country'] = df['sys'].apply(lambda x: x['country'])
        df['dt'] = pd.to_numeric(df['dt'])
        df['dt_utc'] = pd.to_datetime(df['dt'], unit='s')
        df['dt_utc'] = pd.to_datetime(df['dt_utc'], utc=True)
        df.drop(columns= ["coord", "weather", "main", "wind", "clouds", "sys", "dt", "timezone"], inplace= True)
        df.reset_index(drop= True)
        print("Deu certo!")
        return df   

if __name__ == "__main__":

    extract_instance = ExtractData()
    profile_name = 'CRISTIAN_AWS'
    df_raw = extract_instance.extract_data(profile_name=profile_name)
    df_transformed = TransformData.transform_data(df_raw)