import os
import json
import logging
import uuid

import aio_pika
import pika

logger = logging.getLogger(__name__)


class PikaClient:

    def __init__(self):
        """

        Notes:
            PUBLISH_QUEUE: a name for a queue, where we shall send our outgoing messages.
        """

        self.publish_queue_name = os.environ.get('PUBLISH_QUEUE', 'foo_publish_queue')
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=os.environ.get('RABBIT_HOST', '127.0.0.1'))
        )
        self.channel = self.connection.channel()
        self.publish_queue = self.channel.queue_declare(queue=self.publish_queue_name)
        self.callback_queue = self.publish_queue.method.queue
        self.response = None
        logger.info('Pika connection initialized...')

    def send_message(self, message: dict):
        """Method to publish message to RabbitMQ"""
        self.channel.basic_publish(
            exchange='',
            routing_key=self.publish_queue_name,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=str(str(uuid.uuid4()))
            ),
            body=json.dumps(message)
        )
