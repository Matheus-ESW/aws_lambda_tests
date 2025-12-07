def lambda_handler():
    print("Função executada a cada 10 minutos.")
    return {
        'statusCode': 200,
        'body': 'Execução bem-sucedida.'
    }

lambda_handler()