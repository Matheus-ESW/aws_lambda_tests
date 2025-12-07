import urllib3
import json
import os
import boto3
from datetime import datetime

# Carregar variáveis de ambiente
url = os.getenv('API_URL')
api_key = os.getenv('CMC_API_KEY')
bucket_name = os.getenv('S3_BUCKET_NAME')

# Parâmetros da requisição para obter a cotação do Bitcoin
parameters = {
    'symbol': 'BTC',
    'convert': 'USD'
}

# Headers com a chave da API
headers = {
    'Accept': 'application/json',
    'X-CMC_PRO_API_KEY': api_key
}

# Criar um PoolManager para gerenciar conexões
http = urllib3.PoolManager()

# Cliente S3
s3 = boto3.client('s3')

# Função Lambda
def lambda_handler(event, context):
    try:
        # Converte os parâmetros para o formato de query string
        query_string = '&'.join([f'{key}={value}' for key, value in parameters.items()])
        full_url = f"{url}?{query_string}"

        # Fazendo o request GET para a API
        response = http.request('GET', full_url, headers=headers)
        data = json.loads(response.data.decode('utf-8'))

        # Gerar nome do arquivo com timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        file_name = f"bitcoin_quote_{timestamp}.json"

        # Salvar o JSON no S3
        s3.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json.dumps(data, indent=2),
            ContentType='application/json'
        )

        # Verificar se os dados do Bitcoin estão presentes na resposta
        if 'data' in data and 'BTC' in data['data']:
            bitcoin_data = data['data']['BTC']
            usd_quote = bitcoin_data['quote']['USD']

            print(f"Cotação do Bitcoin obtida e salva no S3: {usd_quote}")

            return {
                "statusCode": 200,
                "body": {
                    "mensagem": "Cotação salva com sucesso no S3",
                    "arquivo": file_name,
                    "cotacao": usd_quote
                }
            }
        else:
            erro = data.get('status', {}).get('error_message', 'Erro desconhecido')
            print(f"Erro ao obter a cotação do Bitcoin: {erro}")

            return {
                "statusCode": 500,
                "body": {
                    "erro": erro
                }
            }

    except urllib3.exceptions.HTTPError as e:
        print(f"Erro na requisição: {e}")
        return {
            "statusCode": 500,
            "body": {
                "erro": str(e)
            }
        }