class ApiDataSaver():

# Criar um cliente para o AWS Secrets Manager com base no perfil
client = boto3.session.Session(profile_name='CRISTIAN_AWS').client('secretsmanager', region_name='us-east-1')

# Recuperar o segredo
response = client.get_secret_value(SecretId='env/openweather')
secret_data = response['SecretString']

# Analisar o conteúdo do segredo (assumindo que é um formato JSON)
secrets_dict = json.loads(secret_data)

# Obtenha o valor associado à chave 'OPEN_WEATHER_SECRECT'
open_weather_token = secrets_dict.get('OPEN_WEATHER_SECRECT', '')