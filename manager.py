import asyncio
import json
import re
import uuid
import logging
from time import sleep
from typing import Dict, Coroutine, List

import aioamqp
from aioamqp.channel import Channel
from aioamqp.envelope import Envelope
from aioamqp.properties import Properties

logger = logging.getLogger(__name__)


class Configuration:

    def __init__(self, rabbit_host, sleep_period, rabbit_username, rabbit_password, virtualhost, exchange_name, queue_name):
        self.rabbit_host = rabbit_host
        self.sleep_period = sleep_period
        self.rabbit_username = rabbit_username
        self.rabbit_password = rabbit_password
        self.virtualhost = virtualhost
        self.exchange_name = exchange_name
        self.queue_name = queue_name


class CorrelationManager:

    async def find_request_by_correlation_id(self, correlation_id: str) -> str:
        raise NotImplementedError('You should implement this yourself')


class RouteManager:
    routes = {}

    def add_route(self, routing_key, view):
        self.routes[re.compile(routing_key)] = view

    def _test_reg(self, routing_key):
        for route, view in self.routes.items():
            if re.match(route, routing_key):
                return view

    def get_view(self, routing_key: str):
        return self._test_reg(routing_key)


class DatabusApp:
    def __init__(self, conf: Configuration, correlation_manager: CorrelationManager, max_workers=10):
        self.correlation_manager = correlation_manager()
        self.conf = conf
        self.router = RouteManager()
        self.task_queue = asyncio.Queue()
        self.max_workers = max_workers

    def add_route(self, routing_key, view, as_list=False):
        view.as_list = as_list
        self.router.add_route(routing_key, view)

    def add_routes(self, routes: Dict[str, Coroutine[Dict, None, None]]):
        for key, view in routes.items():
            self.router.add_route(key, view)

    async def _get_routing_key_for_rpc(self, correlation_id):
        route = await self.correlation_manager.find_request_by_correlation_id(correlation_id)
        if route:
            return route
        else:
            logger.warning(f"Got unknown correlation_id: {correlation_id}")

    async def list_to_view(self, func, data: List[Dict]):
        [await func(item) for item in data]
        return True

    async def _serve_message(self, channel: Channel, body, envelope: Envelope, properties: Properties):
        try:
            if envelope.routing_key == self.conf.queue_name:
                routing_key = await self._get_routing_key_for_rpc(properties.correlation_id)
            else:
                routing_key = envelope.routing_key
            work_func = self.router.get_view(routing_key)
            await self.task_queue.put((work_func, json.loads(body), channel, envelope.delivery_tag))
        except ConnectionResetError as ex:
            logger.exception(ex)
            raise ex

    async def _receive(self, reconnect: bool = True, reconnect_wait_period=2):
        async def on_error_callback(exception):
            logger.debug('on_error_callback: %s', str(exception))
            if isinstance(exception, aioamqp.AmqpClosedConnection):
                await asyncio.sleep(reconnect_wait_period)
                await self._receive(self.max_workers)
        try:
            transport, protocol = await aioamqp.connect(on_error=on_error_callback,
                                                        host=self.conf.rabbit_host,
                                                        login=self.conf.rabbit_username,
                                                        password=self.conf.rabbit_password,
                                                        virtualhost=self.conf.virtualhost)
            channel = await protocol.channel()
            await channel.basic_qos(prefetch_count=self.max_workers)
            await channel.basic_consume(self._serve_message, queue_name=self.conf.queue_name, no_ack=False)
        except aioamqp.AmqpClosedConnection:
            logging.debug("AmqpClosedConnection, will call on_error")
        except (OSError, ConnectionRefusedError) as e:
            if reconnect:
                logger.warning(str(e))
                await asyncio.sleep(reconnect_wait_period)
                await self._receive()
            else:
                raise e

    async def _consume(self, worker_id):
        while True:
            # wait for an item from the producer
            func, item, channel, delivery_tag = await self.task_queue.get()
            try:
                logger.debug("Worker %s started" % worker_id)
                if func.as_list is True:
                    result = await self.list_to_view(func, item)
                else:
                    result = await func(item)
                if result:
                    if delivery_tag:
                        await channel.basic_client_ack(delivery_tag=delivery_tag)
                    logger.debug("Worker %s completed the task" % worker_id)
                else:
                    logger.warning("Router handler %s doesn't returned anything for worker %s" % (func, worker_id))
                    if delivery_tag:
                        await channel.basic_client_nack(delivery_tag=delivery_tag)
                    await asyncio.sleep(60)
                self.task_queue.task_done()
            except Exception as ex:
                logger.exception(ex)
                logger.warning("Worker %s going to sleep" % worker_id)
                if delivery_tag:
                    await channel.basic_client_nack(delivery_tag=delivery_tag)
                await asyncio.sleep(60)
            await asyncio.sleep(0.01)

    async def _create_workers(self):
        return [asyncio.ensure_future(self._consume(str(uuid.uuid4()))) for worker in range(self.max_workers)]

    def start(self, loop):
        workers = []
        try:
            logger.info("Creating workers")
            workers = loop.run_until_complete(self._create_workers())
            logger.info("Connecting to RabbitMQ")
            loop.run_until_complete(self._receive())
            loop.run_forever()
        except ConnectionRefusedError as ex:
            logger.warning(ex)
            sleep(10)
            self.start(loop)
        finally:
            for worker in workers:
                logger.info("Canceling worker %s" % str(worker))
                worker.cancel()
            loop.close()
