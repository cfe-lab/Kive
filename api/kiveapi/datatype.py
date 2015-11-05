"""
This module defines a wrapper for Kive's CompoundDatatype
object, and some support methods.
"""
from . import KiveMalformedDataException


class CompoundDatatype:
    """
    A wrapper class for Kive's CompoundDatatype object
    """

    def __init__(self, cdt):
        try:
            if cdt is None:
                self.cdt_id = '__raw__'
                self.name = 'Raw CDT'
            elif type(cdt) == dict:
                self.cdt_id = cdt['id']
                self.name = cdt['representation']
            else:
                self.cdt_id = cdt
                self.name = 'Compound Datatype id {}'.format(self.cdt_id)
        except (ValueError, IndexError):
            raise KiveMalformedDataException(
                'Server gave malformed CDT object:\n%s' % cdt
            )

    def __str__(self):
        return self.name

    def __unicode__(self):
        return self.name

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return self.cdt_id == other.cdt_id or self.name == other.name

    def __ne__(self, other):
        return not (self == other)