"""
This init module provides some simple exception classes
and an alias to the main KiveAPI object.
"""


from .errors import (
    KiveAuthException, KiveClientException, KiveMalformedDataException,
    KiveRunFailedException, KiveServerException
)
from .kiveapi import KiveAPI
kapi = KiveAPI
