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
import time
import base64
from google.cloud import pubsub_v1
from google.cloud import bigquery
from google.cloud import storage

def hello_pubsub(event, context):

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(pubsub_message)
    insert_to_bigquery(data)
    write_to_gcs(pubsub_message)

def insert_to_bigquery(data):
    # implementação do método para gravar dados no bigquery
    client = bigquery.Client()
    # ...

def write_to_gcs(data):
    # implementação do método para gerar arquivo no GCS
    storage_client = storage.Client.from_service_account_json('credentials.json')
    # ...