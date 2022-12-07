from kafka import KafkaProducer
from Decorators import Facilities


@Facilities.Singleton
class Publisher:

    def __init__(self):
        self._producer = None

    def publish_message(self, topic_name, key, value):
        try:
            key_bytes = bytes(key, encoding='utf-8')
            value_bytes = bytes(value, encoding='utf-8')
            self.send(topic_name, key=key_bytes, value=value_bytes)
            self.flush()
            print('Message published successfully.')
        except Exception as ex:
            print('Exception in publishing message')
            print(str(ex))

    def connect_kafka_producer(self) -> KafkaProducer:
        _producer = None
        try:
            self._producer = KafkaProducer(bootstrap_servers=['35.86.112.176:9092'], api_version=(0, 10))
        except Exception as ex:
            print('Exception while connecting Kafka')
            print(str(ex))
        finally:
            return self._producer
