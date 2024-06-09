"""
Nesse case você irá criar o pipeline de processamento em tempo real (Ingestão e Tratamento), 
para processar as mensagens de entrada no datalake.

- O candidato precisa criar pipeline para processar dados de Historico de Consumo dos Lotes de Confinamento em tempo real.

Entregas:
- Criar job python para ler arquivo json e publicar dados na fila
    - para simular os dados enviados temos 8 arquivos, 4 por fazenda, cada um com dados do dia.
    - criar tópico no serviço de mensagens assíncronas para receber as mensagens  ex. Google Cloud Pub/Sub
    - no case tem um mockup do job para publicar mensagens no tópico: publicar_msgs.py

- Criar ingestão dos dados no pipeline com gatilho no tópico criado para processamento das mensagens em tempo real. Ex. Google Cloud Function
    - Função deve receber mensagem e tratar os dados
        - preencher coluna consumo_meta = peso_medio * 1,76%
    - inserir dados na tabela historico_consumo
    - criar um arquivo json para cada mensagem tratada Ex. nome do arquivo historico_consumo_{timestamp}.json 
        - salvar cada arquivo gerado num bucket do "datalake", como backup.
    - no case tem um mockup dos arquivos da Cloud Function: main.py e o requirements.txt

- Sugestão de banco de dados: BigQuery (mas pode ser qualquer banco, caso necessário) 
- Sugestão de utlizar GCP
- Na pasta dados temos um arquivo sql com a criação da tabela historico_consumo

"""
