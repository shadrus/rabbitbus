from rabbitbus.acks.requests import AmqpRequest


class AckResponse:
    """
    Response to return ack answer to RabbitMQ
    """
    def __init__(self, request: AmqpRequest, data=None):
        """
        If data is not None and request is RPC, then we will send response to the app exchange.
        """
        self.data = data
        self.request = request


class NackResponse:
    """
    Response to return nack answer to RabbitMQ
    """

    def __init__(self, request: AmqpRequest):
        self.request = request
