import os

import boto3
import pandas as pd

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')


def lambda_handler(event, context):
    # recebe os parâmetros bucket_name e object_key da requisição HTTP
    bucket_name = event['bucket_name']
    object_key = event['object_key']

    # realiza a leitura do arquivo CSV no S3
    s3_object = s3.Object(bucket_name, object_key)
    file_content = s3_object.get()['Body'].read().decode('utf-8')

    # converte o conteúdo do arquivo CSV em um DataFrame utilizando a
    # biblioteca Pandas
    df = pd.read_csv(pd.compat.StringIO(file_content))

    # realiza o tratamento dos dados
    df['cpf'] = df['cpf'].str.replace('[^0-9]', '')
    df['cnpj'] = df['cnpj'].str.replace('[^0-9]', '')
    df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y').dt.strftime(
        '%Y-%m-%d'
        )

    ''' Salva os dados tratados em um banco de dados
    Aqui é necessário utilizar a biblioteca do banco de dados de sua
    preferência
    Segue abaixo um exemplo utilizando a biblioteca psycopg2 para o banco de
    dados Postgres
    '''
    import psycopg2

    db_host = os.environ['DB_HOST']
    db_port = os.environ['DB_PORT']
    db_name = os.environ['DB_NAME']
    db_user = os.environ['DB_USER']
    db_pass = os.environ['DB_PASS']

    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_pass
    )
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute(
            "INSERT INTO tabela1 (cpf, cnpj, data) VALUES (%s, %s, %s)",
            (row['cpf'], row['cnpj'], row['data'])
        )

    conn.commit()
    cur.close()
    conn.close()

    return {
        'statusCode': 200,
        'body': 'Dados processados com sucesso!'
    }
