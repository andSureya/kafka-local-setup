# in-built
import csv
import time
import json
import logging
import argparse
from uuid import uuid4
from os.path import dirname, join
from typing import List, Optional, Dict, Any

# 3rd party
from kafka import KafkaProducer


logging.basicConfig(level=logging.INFO)


class ScheduleProducer:
    def __init__(self, filename: str, topic_name: str, kafka_host: str):
        self.filename = filename
        self.hostname = kafka_host
        self.topic_name = topic_name
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_host,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=str.encode
        )

        self.column_names: Optional[List[str]] = None

    def publish(self):
        try:
            logging.info("Reading CSV file")
            with open(self.filename) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                line_count = 0
                for row in csv_reader:
                    if line_count == 0:
                        self.column_names = row
                    else:
                        self.publish_record(row=row)
                        logging.info(f'Processed {line_count} lines.')

                    line_count += 1
        except Exception as e:
            logging.error("Failed to publish all messages to Kafka.")
            logging.error(e)

    def publish_record(self, row: List[str]):
        dict_record: Dict[str, Any] = dict(zip(self.column_names, row))
        time.sleep(2)
        self.producer.send(
            topic=self.topic_name,
            key=uuid4().hex,
            value=dict_record
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    schedule_file_path = join(dirname(__file__), 'schedule.csv')

    parser.add_argument('--kafka_host', type=str, default='localhost:9092', required=False)
    parser.add_argument('--file_name', type=str, default=schedule_file_path, required=False)
    parser.add_argument('--topic_name', type=str, default='airline_schedule', required=False)

    args = parser.parse_args()

    logging.info("Processing with Args: {}".format(args))

    producer: ScheduleProducer = ScheduleProducer(
        filename=args.file_name, topic_name=args.topic_name, kafka_host=args.kafka_host
    )

    logging.info("Beginning producing...")
    producer.publish()
