import argparse

from kafka import KafkaConsumer
import json
import logging


logging.basicConfig(level=logging.INFO)


class ConsumeSchedule:
    def __init__(self, host: str, topic: str):
        self.host = host
        self.topic = topic
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.host,
            key_deserializer=lambda key: key.decode('utf-8'),
            value_deserializer=lambda value: json.loads(value.decode('utf-8'))
        )
        logging.info("Consumer initialized")

    def consume(self):
        logging.info("Consuming messages")
        for message in self.consumer:
            self.process_result(record=message)

    @staticmethod
    def process_result(record: dict):
        logging.info(record)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--host', type=str, default='localhost:9092', required=False)
    parser.add_argument('--topic', type=str, default='airline_schedule', required=False)

    args = parser.parse_args()

    consumer = ConsumeSchedule(host=args.host, topic=args.topic)
    consumer.consume()
