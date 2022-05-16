import os
import json
import logging
import uuid

import aio_pika
import pika

logger = logging.getLogger(__name__)


class PikaClient:

    def __init__(self, process_callable):
        """

        Args:
            process_callable: a callable callback which will handle the actual business logic
                to process the incoming message.

        Notes:
            PUBLISH_QUEUE: a name for a queue, where we shall send our outgoing messages.
        """

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=os.environ.get('RABBIT_HOST', '127.0.0.1'))
        )
        self.channel = self.connection.channel()
        self.response = None
        self.process_callable = process_callable
        logger.info('Pika connection initialized...')

    async def consume(self, loop):
        """Setup message listener with the current running loop

        Args:
            loop:

        Notes
            CONSUME_QUEUE: name of the queue, from which we shall get the incoming messages.

        """
        connection = await aio_pika.connect_robust(host=os.environ.get('RABBIT_HOST', '127.0.0.1'),
                                                   port=5672,
                                                   loop=loop)
        channel = await connection.channel()
        queue = await channel.declare_queue(os.environ.get('CONSUME_QUEUE', 'foo_consume_queue'))
        await queue.consume(self.process_incoming_message, no_ack=False)
        logger.info('Established pika async listener')
        return connection

    async def process_incoming_message(self, message):
        """Processing incoming message from RabbitMQ"""
        message.ack()
        body = message.body
        logger.info('Received message')
        if body:
            self.process_callable(json.loads(body))
