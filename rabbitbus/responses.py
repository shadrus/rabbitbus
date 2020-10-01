from requests import AmqpRequest


class AckResponse:
    def __init__(self, data=None):
        self.data = data


class NackResponse:
    pass
