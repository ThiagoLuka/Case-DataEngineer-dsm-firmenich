"""
Este job é só um pequeno exemplo de como pode ser feita a publicação das mensagens pro case
"""
from google.cloud import pubsub_v1
import time
import json
from google.auth import jwt

#dados do projeto google e topico
project_id = "teste-312617" #projeto gcp
topic_id = "teste"  # topico Cloud Function

# configurando credencial para publicar no PubSub 
# talvez vc precisa criar a variável de ambiente GOOGLE_APPLICATION_CREDENTIALS
service_account_info = json.load(open("credentials.json"))
audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
credentials = jwt.Credentials.from_service_account_info(
    service_account_info, audience=audience
)
credentials_pub = credentials.with_claims(audience=audience)
publisher = pubsub_v1.PublisherClient(credentials=credentials_pub)
topic_path = publisher.topic_path(project_id, topic_id)

#ler arquivos FazendaTeste_HistoricoConsumo...json
# lista = dados

for item in lista:
    data = json.dumps(item).encode("utf-8")
    future = publisher.publish(
        topic_path, data, origin="python", username="xxxxxxx"
    )
    print(future.result())

print("Finalizado")