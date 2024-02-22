from rabbitmq_client import RMQConsumer, ConsumeParams, QueueParams
import pika


def callback(ch, method, properties, body):
    print(f" [x] Received {body}")

class RabbitMQ:
    def __init__(self):
        self.queue_name = 'TEST_QUEUE_1'
        self.connection_params = pika.ConnectionParameters('localhost')
        self.consumer = RMQConsumer(self.connection_params)
        self.queue_params = QueueParams(
            self.queue_name,
            durable=True
        )

    def consume(self):
        with pika.BlockingConnection(self.connection_params) as connection:
            channel = connection.channel()
            channel.queue_declare(queue=self.queue_name)
            channel.basic_consume(
                queue=self.queue_name,
                auto_ack=True,
                on_message_callback=callback
            )
            channel.start_consuming()

    def publish(self):
        with pika.BlockingConnection(self.connection_params) as connection:
            channel = connection.channel()
            channel.queue_declare(queue=self.queue_name)
            channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body='Hello, RabbitMQ!'
            )

        print("Message sent successfully.")
