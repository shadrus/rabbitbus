import json

from aioamqp.channel import Channel
from aioamqp.envelope import Envelope
from aioamqp.properties import Properties

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ..manager import DatabusApp


class AmqpRequest:

    def __init__(self, app: 'DatabusApp', channel: Channel, body, envelope: Envelope, properties: Properties):
        self.app = app
        self.body = body
        self.channel = channel
        self.envelope = envelope
        self.properties = properties

    def json(self) -> dict:
        """
        Returns:
            dict: converts json to dict
        """
        return json.loads(self.body)
