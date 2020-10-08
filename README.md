![PyPI - Python Version](https://img.shields.io/pypi/pyversions/rabbitbus)

# RabbitBus

Feel RabbitMQ like HTTP

  - Custom CorrelationManagers
  - Regexp roures


### Installation

RabbitBus requires Python 3.6 >, aioamqp.

Install the dependencies and library.

```sh
$ pip install rabbitbus
```

Example:

```python
import asyncio
import logging
from rabbitbus.manager import DatabusApp, Configuration
from rabbitbus.acks.requests import AmqpRequest
from rabbitbus.acks.responses import AckResponse

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

async def my_view(request: AmqpRequest):
    # Write your code here
    return AckResponse()

def serve():
    loop = asyncio.get_event_loop()
    # Inherit from CorrelationManager for custom correlation storages
    app = DatabusApp(conf=Configuration())
    app.add_route(r'^CASH_REGISTER_EQUIPMENTS[a-zA-Z_]{4}$', my_view)
    app.start(loop)


if __name__ == '__main__':
    serve()
```
