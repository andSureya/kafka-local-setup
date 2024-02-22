from sample_scripts.rabbit_client import RabbitMQ

if __name__ == '__main__':
    mq = RabbitMQ()
    mq.consume()
