import json
import functools

from google.cloud import pubsub_v1


project_id = 'data-engineer-dsm-firmenich'
topic_id = 'consumo-lotes-confinamento'
files = [
    'dados\\FazendaUm_Consumo_20230101.json',
    'dados\\FazendaUm_Consumo_20230102.json',
    'dados\\FazendaUm_Consumo_20230103.json',
    'dados\\FazendaUm_Consumo_20230104.json',
    'dados\\FazendaDois_Consumo_20230101.json',
    'dados\\FazendaDois_Consumo_20230102.json',
    'dados\\FazendaDois_Consumo_20230103.json',
    'dados\\FazendaDois_Consumo_20230104.json',
]


def publish_file_all_items(
    file_name: str,
    project_id: str,
    topic_id: str
) -> None:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(
        project=project_id,
        topic=topic_id,
    )

    file_data = []
    with open(file_name) as file:
        for item in file.readlines():
            file_data.append(item)

    for item in file_data:
        future = publisher.publish(
            topic=topic_path,
            data=item.encode('utf-8'),
            origin='thiago_local_python'
        )
        id_curral = json.loads(item)['id_curral']
        future.add_done_callback(functools.partial(print, f"Message sent - id_curral: {id_curral}"))


for file_name in files:
    publish_file_all_items(
        file_name=file_name,
        project_id=project_id,
        topic_id=topic_id,
    )
