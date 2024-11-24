from kafka_api import KafkaApi

k = KafkaApi()

k.create_topic('test-topic', 3, 1)
k.insert_to_topic('test-topic', 'wall.csv')
k.read_topic('test-topic')
k.delete_topic('test-topic')