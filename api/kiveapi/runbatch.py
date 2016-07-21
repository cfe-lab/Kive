"""
This module defines a wrapper for Kive's RunBatch
object, and some support methods.
"""
from . import KiveMalformedDataException


class RunBatch:
    """
    A wrapper class for Kive's RunBatch object.
    """

    def __init__(self, rb):
        """
        Parse a JSON representation of a RunBatch.

        :param rb: a JSON representation of a RunBatch as produced by RunBatchSerializer.
        """
        try:
            self.id = rb.get("id")
            self.name = rb.get("name")
            self.description = rb.get("description")
            self.users_allowed = rb.get("users_allowed")
            self.groups_allowed = rb.get("groups_allowed")
        except (ValueError, IndexError):
            raise KiveMalformedDataException(
                'Server gave malformed RunBatch object:\n%s' % rb
            )

    def __str__(self):
        return self.name

    def __unicode__(self):
        return self.name

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return self.id == other.id

    def __ne__(self, other):
        return not (self == other)
