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
    return code >= 400 and code <= 499


def is_server_error(code):
    return code >= 500 and code <= 599
