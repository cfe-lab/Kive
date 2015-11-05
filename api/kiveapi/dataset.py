"""
This module defines a wrapper for Kive's Dataset
object, and some support methods.
"""
from .datatype import CompoundDatatype
from . import KiveMalformedDataException


class Dataset(object):
    """
    A wrapper class for Kive's Dataset object
    """

    def __init__(self, obj, api=None):
        try:
            if type(obj) == dict:
                self.dataset_id = obj['id']
                self.symbolicdataset_id = obj.get('symbolic_id', None)
                self.filename = obj['filename']
                self.name = obj['name'] if 'name' in obj else obj['output_name']
                self.cdt = CompoundDatatype(obj['compounddatatype']) if 'compounddatatype' in obj else None

        except (ValueError, IndexError, KeyError):
            raise KiveMalformedDataException(
                'Server gave malformed Dataset object:\n%s' % obj
            )
        self.api = api

    def __str__(self):
        return self.name

    def __unicode__(self):
        return self.name

    def __repr__(self):
        return '<Dataset (%s): "%s" (%s)>' % (self.dataset_id, str(self), str(self.cdt))

    def download(self, handle):
        """ Downloads this dataset and streams it into handle

        :param handle: A file handle
        """

        for block in self._request_download().iter_content(1024):
            handle.write(block)

    def _request_download(self):
        """ Send a download request for this dataset.

        :return: a response object
        """
        response = self.api.get("@api_dataset_dl",
                                context={'dataset-id': self.dataset_id},
                                is_json=False,
                                stream=True)

        return response

    def readlines(self):
        """ Returns an iterator to lines in the data set, including newlines.
        """

        for line in self._request_download().iter_lines():
            yield line + '\n'
