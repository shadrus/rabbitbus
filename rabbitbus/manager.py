import asyncio
import re
import logging
from time import sleep
from typing import Dict, Coroutine, Tuple, Type, Awaitable, Callable, Union, Optional

import aioamqp
from aioamqp.channel import Channel
from aioamqp.envelope import Envelope
from aioamqp.properties import Properties

from rabbitbus.acks.requests import AmqpRequest
from rabbitbus.acks.responses import AckResponse, NackResponse

logger = logging.getLogger(__name__)


class Configuration:

    def __init__(self,
                 rabbit_host: str,
                 rabbit_username: str,
                 rabbit_password: str,
                 virtualhost: str,
                 queue_name: str,
                 rabbit_port: int = 5672,
                 exchange_name: Optional[str] = None):
        """
        Configuration class to connect to RabbitMQ.
        Args:
            rabbit_host: the host to connect to
            rabbit_username: login
            rabbit_password: password
            virtualhost: virtualhost
            queue_name: queue to consume
            rabbit_port: the port to connect to, default 5672
            exchange_name: exchange for responses
        """
        self.rabbit_port = rabbit_port
        self.rabbit_host = rabbit_host
        self.rabbit_username = rabbit_username
        self.rabbit_password = rabbit_password
        self.virtualhost = virtualhost
        self.exchange_name = exchange_name
        self.queue_name = queue_name


class CorrelationManager:
    """
    CorrelationManager is interface, that you should to implement if your app sends RPC somewhere to RabbitMQ
    Read about RPC https://www.rabbitmq.com/tutorials/tutorial-six-python.html
    """

    async def find_request_by_correlation_id(self, correlation_id: str) -> str:

        """
        Args:
            correlation_id: correlation id from RPC
        Returns:
            str: real key
        """
        raise NotImplementedError('You should implement this yourself')


class RouteManager:
    routes = {}

    def add_route(self, routing_key, view):
        self.routes[re.compile(routing_key)] = view

    def get_view(self, routing_key: str) -> Callable[[AmqpRequest], Awaitable[None]]:
        for route, view in self.routes.items():
            if re.match(route, routing_key):
                return view


class DatabusApp:

    def __init__(self,
                 conf: Configuration,
                 correlation_manager: Type[CorrelationManager] = None,
                 max_workers: int = 10,
                 reconnect_interval: int = 10,
                 sleep_interval: Union[int, None] = 10):
        """
        Args:
            conf: Configuration instance
            correlation_manager: instance that implements CorrelationManager interface.
                                 Is needed to convert reply routing key to real one. Useful for RPC requests.
            max_workers: How many workers to create for consuming
            reconnect_interval: seconds to wait for reconnect on disconnect
            sleep_interval: seconds to sleep on exception

        Examples:
            >>> app = DatabusApp(conf=conf, max_workers=1, correlation_manager=ImplementedCorrelationManager)
        """
        self.sleep_interval = sleep_interval
        self.reconnect_interval = reconnect_interval
        if correlation_manager:
            self.correlation_manager = correlation_manager()
        else:
            self.correlation_manager = None
        self.__conf: Configuration = conf
        self.__router = RouteManager()
        self.__task_queue: asyncio.Queue[Tuple[Callable[[AmqpRequest], Awaitable[None]], AmqpRequest]] = asyncio.Queue()
        self.__max_workers = max_workers
        self.__consuming_semaphore = asyncio.Semaphore(1)

    async def pause_consuming(self):
        """
        Prevent workers to get new messages from queue
        """
        if not self.__consuming_semaphore.locked():
            await self.__consuming_semaphore.acquire()
            logger.debug("Consuming was paused")

    def continue_consuming(self):
        """
        Continue getting new messages from queue
        """
        if self.__consuming_semaphore.locked():
            self.__consuming_semaphore.release()
            logger.debug("Consuming was continued")

    def is_paused(self) -> bool:
        return self.__consuming_semaphore.locked()

    def add_route(self, routing_key: str, view):
        """
        Args:
            routing_key: message with this routing key will be send to the view.
            view: coroutine callable
        Examples:
            >>> async def my_view(request: AmqpRequest):
            >>>    # Write your code here
            >>>    return AckResponse()
            >>> app = DatabusApp(conf=conf, max_workers=1)
            >>> app.add_route(r'.*', my_view)
        """
        self.__router.add_route(routing_key, view)

    def add_routes(self, routes: Dict[str, Coroutine[Dict, None, None]]):
        """
        Args:
            routes: create routes from dictionary.
        Examples:
            >>> routes = {'route_one': view1, 'route_two': view2}
            >>> app.add_routes(routes)
        """
        for key, view in routes.items():
            self.__router.add_route(key, view)

    async def __get_routing_key_for_rpc(self, correlation_id):
        route = await self.correlation_manager.find_request_by_correlation_id(correlation_id)
        if route:
            return route
        else:
            logger.warning(f"Got unknown correlation_id: {correlation_id}")

    async def __serve_message(self, channel: Channel, body, envelope: Envelope, properties: Properties):
        try:
            if envelope.routing_key == self.__conf.queue_name and self.correlation_manager:
                routing_key = await self.__get_routing_key_for_rpc(properties.correlation_id)
            else:
                routing_key = envelope.routing_key
            work_func = self.__router.get_view(routing_key)
            if not work_func:
                logger.warning("Got message with routing key %s, but can't find right view", routing_key)
                return
            request = AmqpRequest(self, channel, body, envelope, properties)
            await self.__task_queue.put((work_func, request))
        except ConnectionResetError as ex:
            logger.exception(ex)
            raise ex

    async def __receive(self, reconnect: bool = True, reconnect_wait_period: int = 2):
        async def on_error_callback(exception):
            logger.debug('on_error_callback: %s', str(exception))
            if isinstance(exception, aioamqp.AmqpClosedConnection):
                await asyncio.sleep(reconnect_wait_period)
                await self.__receive(reconnect, reconnect_wait_period)

        transport = None
        try:
            transport, protocol = await aioamqp.connect(on_error=on_error_callback,
                                                        host=self.__conf.rabbit_host,
                                                        login=self.__conf.rabbit_username,
                                                        password=self.__conf.rabbit_password,
                                                        virtualhost=self.__conf.virtualhost)
            channel = await protocol.channel()
            await channel.basic_qos(prefetch_count=self.__max_workers)
            await channel.basic_consume(self.__serve_message, queue_name=self.__conf.queue_name, no_ack=False)
        except aioamqp.AmqpClosedConnection:
            logging.debug("AmqpClosedConnection, will call on_error")
        except (OSError, ConnectionRefusedError) as e:
            if transport and not transport.is_closing():
                await transport.close()
            if reconnect:
                logger.warning(str(e))
                await asyncio.sleep(reconnect_wait_period)
                await self.__receive(reconnect, reconnect_wait_period)
            else:
                raise e

    async def __set_response(self, request: AmqpRequest, response: Union[AckResponse, NackResponse, None], worker_id):
        if isinstance(response, AckResponse):
            if request.properties.reply_to:
                pass  # TODO send data to exchange
            if request.envelope.delivery_tag:
                await request.channel.basic_client_ack(delivery_tag=request.envelope.delivery_tag)
            logger.debug("Worker %s completed the task" % worker_id)
        else:
            logger.warning("Router handler %s doesn't returned valid response or returned NackResponse" % request.body)
            if request.envelope.delivery_tag:
                await request.channel.basic_client_nack(delivery_tag=request.envelope.delivery_tag)
            if self.sleep_interval:
                await asyncio.sleep(self.sleep_interval)

    async def __consume(self, worker_id):
        while True:
            # wait for an item from the producer
            if not self.__consuming_semaphore.locked():
                func, request = await self.__task_queue.get()
                try:
                    logger.debug("Worker %s started" % worker_id)
                    await self.__set_response(request, await func(request), worker_id)
                    self.__task_queue.task_done()
                except Exception as ex:
                    logger.exception(ex)
                    logger.warning("Worker %s going to sleep because of exception" % worker_id)
                    if request.envelope.delivery_tag:
                        await request.channel.basic_client_nack(delivery_tag=request.envelope.delivery_tag)
                    if self.sleep_interval:
                        await asyncio.sleep(self.sleep_interval)
            await asyncio.sleep(0.001)

    async def __create_workers(self):
        for worker_id in range(self.__max_workers):
            asyncio.create_task(self.__consume(worker_id+1))

    def start(self, loop):
        workers = []
        try:
            logger.info("Creating workers")
            workers = loop.run_until_complete(self.__create_workers())
            logger.info("Connecting to RabbitMQ")
            loop.run_until_complete(self.__receive())
            loop.run_forever()
        except ConnectionRefusedError as ex:
            logger.warning(ex)
            sleep(self.reconnect_interval)
            self.start(loop)
        finally:
            for worker in workers:
                logger.info("Canceling worker %s" % str(worker))
                worker.cancel()
            loop.close()
