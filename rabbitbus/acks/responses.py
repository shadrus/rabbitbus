class AckResponse:
    """
    Response to return ack answer to RabbitMQ
    """
    def __init__(self, data=None):
        """
        If data is not None and request is RPC, then we will send response to the app exchange.
        """
        self.data = data


class NackResponse:
    """
    Response to return nack answer to RabbitMQ
    """
    pass
