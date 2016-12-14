class KiveAuthException(Exception):
    pass


class KiveClientException(Exception):
    pass


class KiveServerException(Exception):
    pass


class KiveMalformedDataException(Exception):
    pass


class KiveRunFailedException(Exception):
    pass


def is_client_error(code):
    return 400 <= code <= 499


def is_server_error(code):
    return 500 <= code <= 599
