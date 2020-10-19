![PyPI - Python Version](https://img.shields.io/pypi/pyversions/rabbitbus)
[![Build Status](https://travis-ci.org/shadrus/rabbitbus.svg?branch=master)](https://travis-ci.org/shadrus/rabbitbus)
# RabbitBus

Feel RabbitMQ like HTTP

  - Custom CorrelationManagers
  - Regexp routes


### Installation

RabbitBus requires Python 3.6 >, aioamqp.

Install the dependencies and library.

```sh
$ pip install rabbitbus
```

Example:

```python
import asyncio
from rabbitbus.manager import DatabusApp, Configuration
from rabbitbus.acks.requests import AmqpRequest
from rabbitbus.acks.responses import AckResponse

async def my_view(request: AmqpRequest):
    # Write your code here
    return AckResponse(request)

def serve():
    loop = asyncio.get_event_loop()
    # Inherit from CorrelationManager for custom correlation storages
    app = DatabusApp(conf=Configuration())
    app.add_route(r'^CASH_REGISTER_EQUIPMENTS[a-zA-Z_]{4}$', my_view)
    app.start(loop)


if __name__ == '__main__':
    serve()
```
