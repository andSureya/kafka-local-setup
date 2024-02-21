from rabbitmq_client import RMQConsumer, ConsumeParams, QueueParams
import pika


def message_handler(channel, method, properties, body):
    print("Received message:", body.decode())


class RabbitMQ:
    def __init__(self):
        self.queue_name = 'TEST_QUEUE_1'
        self.connection_params = pika.ConnectionParameters('localhost')
        self.consumer = RMQConsumer(self.connection_params)
        self.queue_params = QueueParams(self.queue_name, durable=True)

    def consume(self):
        consume_params = ConsumeParams(self.queue_params, message_handler)
        self.consumer.consume(consume_params)

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


if __name__ == '__main__':
    mq = RabbitMQ()
    mq.publish()
