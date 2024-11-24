from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

import json
import pandas as pd

class KafkaApi():
    
    def create_topic(self, topic_name:str, num_partitions:int, replication_factor:int):
        admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
        topics = []
        topics.append(NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor))
        admin_client.create_topics(topics)
        print(f'{topic_name} was created')


    def delete_topic(self, topic:str):
        admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
        admin_client.delete_topics([topic])
        print(f'{topic} was deleted')


    def insert_to_topic(self, topic:str, csv_path:str):
        counter = 0
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
        # Инициализация продьюсера
        for chunk in pd.read_csv(csv_path, delimiter=',', chunksize=100):

            dict_to_kafka = chunk.to_dict()
            data = json.dumps(dict_to_kafka, default=str).encode('utf-8')

            key = str(counter).encode()

            # заливка данных в кафку
            producer.send(topic=topic, key=key, value=data)

            counter += 1

        print('Data was added successfully!')


    def read_topic(self, topic:str):
        consumer = KafkaConsumer(topic,
                         group_id='test-consumer-group',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='earliest',
                         enable_auto_commit=False)

        out_list = []

        for message in consumer:
            
            out_list.append(message)

            print(out_list)
            break




    


