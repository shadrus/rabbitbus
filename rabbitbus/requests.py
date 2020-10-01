import json

from aioamqp.channel import Channel
from aioamqp.envelope import Envelope
from aioamqp.properties import Properties


class AmqpRequest:

    def __init__(self, channel: Channel, body, envelope: Envelope, properties: Properties):
        self.body = body
        self.channel = channel
        self.envelope = envelope
        self.properties = properties

    def json(self) -> dict:
        return json.loads(self.body)