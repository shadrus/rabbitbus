import asyncio
import json
import re
import uuid
import logging
from time import sleep
from typing import Dict, Coroutine, Tuple, Type, Awaitable, Callable

import aioamqp
from aioamqp.channel import Channel
from aioamqp.envelope import Envelope
from aioamqp.properties import Properties

logger = logging.getLogger(__name__)


class Configuration:

    def __init__(self, rabbit_host,
                 sleep_period,
                 rabbit_username,
                 rabbit_password,
                 virtualhost,
                 exchange_name,
                 queue_name):
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


class Request:
    
    def __init__(self, channel: Channel, body, envelope: Envelope, properties: Properties):
        self.body = body
        self.channel = channel
        self.envelope = envelope
        self.properties = properties
    
    def json(self):
        return json.loads(self.body)


class RouteManager:
    routes = {}

    def add_route(self, routing_key, view):
        self.routes[re.compile(routing_key)] = view

    def get_view(self, routing_key: str) -> Callable[[Request], Awaitable[None]]:
        for route, view in self.routes.items():
            if re.match(route, routing_key):
                return view


class DatabusApp:
    def __init__(self,
                 conf: Configuration,
                 correlation_manager: Type[CorrelationManager] = None,
                 max_workers: int = 10):
        if correlation_manager:
            self.correlation_manager = correlation_manager()
        else:
            self.correlation_manager = None
        self.conf: Configuration = conf
        self.router = RouteManager()
        self.task_queue: asyncio.Queue[Tuple[Callable[[Request], Awaitable[None]], Request]] = asyncio.Queue()
        self.max_workers = max_workers

    def add_route(self, routing_key, view):
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

    async def _serve_message(self, channel: Channel, body, envelope: Envelope, properties: Properties):
        try:
            if envelope.routing_key == self.conf.queue_name and self.correlation_manager:
                routing_key = await self._get_routing_key_for_rpc(properties.correlation_id)
            else:
                routing_key = envelope.routing_key
            work_func = self.router.get_view(routing_key)
            request = Request(channel, body, envelope, properties)
            await self.task_queue.put((work_func, request))
        except ConnectionResetError as ex:
            logger.exception(ex)
            raise ex

    async def _receive(self, reconnect: bool = True, reconnect_wait_period: int = 2):
        async def on_error_callback(exception):
            logger.debug('on_error_callback: %s', str(exception))
            if isinstance(exception, aioamqp.AmqpClosedConnection):
                await asyncio.sleep(reconnect_wait_period)
                await self._receive(reconnect, reconnect_wait_period)

        transport = None
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
            if transport and not transport.is_closing():
                await transport.close()
            if reconnect:
                logger.warning(str(e))
                await asyncio.sleep(reconnect_wait_period)
                await self._receive(reconnect, reconnect_wait_period)
            else:
                raise e

    async def __set_response(self, request: Request, success, worker_id):
        if success:
            if request.envelope.delivery_tag:
                await request.channel.basic_client_ack(delivery_tag=request.envelope.delivery_tag)
            logger.debug("Worker %s completed the task" % worker_id)
        else:
            logger.warning("Router handler %s doesn't returned anything for request %s" % (request.body, worker_id))
            if request.envelope.delivery_tag:
                await request.channel.basic_client_nack(delivery_tag=request.envelope.delivery_tag)
            await asyncio.sleep(10)

    async def _consume(self):
        worker_id = str(uuid.uuid4())
        while True:
            # wait for an item from the producer
            func, request = await self.task_queue.get()
            try:
                logger.debug("Worker %s started" % worker_id)
                await self.__set_response(request, await func(request))
                self.task_queue.task_done()
            except Exception as ex:
                logger.exception(ex)
                logger.warning("Worker %s going to sleep because of exception" % worker_id)
                if request.envelope.delivery_tag:
                    await request.channel.basic_client_nack(delivery_tag=request.envelope.delivery_tag)
                await asyncio.sleep(10)
            await asyncio.sleep(0.01)

    async def _create_workers(self):
        return [asyncio.create_task(self._consume() for _ in range(self.max_workers))]

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
