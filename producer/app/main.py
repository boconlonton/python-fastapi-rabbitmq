import logging
import asyncio

from fastapi import FastAPI

from pika_client import PikaClient


logger = logging.getLogger(__name__)


class FooApp(FastAPI):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pika_client = PikaClient(self.log_incoming_message)

    @classmethod
    def log_incoming_message(cls, message: dict):
        """Method to do something meaningful with the incoming message"""
        logger.info(f'Here we got incoming message {message}')


foo_app = FooApp()
foo_app.include_router(router)


@foo_app.on_event('startup')
async def startup():
    loop = asyncio.get_running_loop()
    task = loop.create_task(foo_app.pika_client.consume(loop))
    await task
