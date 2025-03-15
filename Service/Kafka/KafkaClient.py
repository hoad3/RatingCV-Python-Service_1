from kafka import KafkaProducer
import json


class KafkaClient:
    def __init__(self, broker: str):
        self.producer = KafkaProducer(
            bootstrap_servers=broker,
            max_request_size=10485760,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def send(self, topic: str, data: dict, timeout: int = 20):
        future = self.producer.send(topic, data)
        return future.get(timeout=timeout)