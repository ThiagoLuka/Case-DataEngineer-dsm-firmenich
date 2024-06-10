"""
description:
    Tratamento
    executar processo de tratamento de dados no data lake da tabela historico_consumo
workflow:
    - Cast colunas:
        - id_curral : long
        - id_raca : long
        - id_lote : long
        - peso_entrada : double
        - peso_medio : double
        - quantidade_animais : Integer
        - dia_confinamento : Integer
        - consumo_real : double
        - consumo_meta : double  -- calculado consumo_meta = peso_medio * 1,81%
        - data : timestamp
        - cod_cliente : string  (coluna da tabela)
        - row_ts : timestamp (coluna da tabela)  -- data hora da execução
        - id_treated_data uuid (coluna da tabela)  -- chave gerada
    - Inserir novos registros na tabela Historico_Consumo na camada de tratamento
    - Para cada mensagem tratada pela função (Faas) gerar arquivo json num bucket do datalake

    Abaixo exemplo de código da cloud function
"""
import json
from datetime import datetime
import base64
import uuid

from google.cloud import bigquery
from google.cloud import storage

credentials_json = 'data-engineer-dsm-firmenich-abd3f60c4d27.json'
project_bucket = 'dsm-firmenich'
bigquery_dataset = 'trm'
bigquery_table = 'historico_consumo'


def process_msg(event, context):
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print('Recebido')

    # processing data
    data = json.loads(pubsub_message)
    data['id_treated_data'] = str(uuid.uuid4())
    data['consumo_meta'] = data['peso_medio'] * 1.0176
    data['row_ts'] = datetime.now().isoformat()

    id_lote = data['id_lote']
    print(f'id_lote: {id_lote} - processado')

    # writing to storage
    write_to_gcs(data)
    print(f'id_lote: {id_lote} - armazenado no storage')

    # writing to bigquery
    insert_to_bigquery(data)
    print(f'id_lote: {id_lote} - inserido no bigquery')


def write_to_gcs(data):
    storage_client = storage.Client.from_service_account_json(credentials_json)
    bucket = storage_client.get_bucket(project_bucket)

    cod_cliente = data['cod_cliente']
    data_datetime = datetime.fromisoformat(data['data'].replace(' UTC', ''))
    folder = f'transformed_data/{cod_cliente}/year={data_datetime.year}/month={data_datetime.month}/day={data_datetime.day}'
    execution_timestamp = data['row_ts']
    filename = f"historico_consumo_{execution_timestamp}.json"
    blob = bucket.blob(f"{folder}/{filename}")

    processed_data = json.dumps(data, indent=4)
    blob.upload_from_string(processed_data, content_type='application/json')


def insert_to_bigquery(data):
    bigquery_client = bigquery.Client.from_service_account_json(credentials_json)
    dataset_ref = bigquery_client.get_dataset(bigquery_dataset)
    table = dataset_ref.table(bigquery_table)

    bigquery_client.insert_rows_json(table, [data])
