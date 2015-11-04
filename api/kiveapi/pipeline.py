"""
This module defines a wrapper for Kive's Pipeline
objects, and some support methods.
"""
from .datatype import CompoundDatatype
from . import KiveMalformedDataException


class PipelineInput(object):
    """
    A wrapper class for Kive's TransformInput object
    """

    def __init__(self, obj):
        try:
            self.dataset_idx = obj['dataset_idx']
            self.dataset_name = obj['dataset_name']
            self.compounddatatype = CompoundDatatype(None) if obj['structure'] is None else \
                CompoundDatatype(obj['structure']['compounddatatype'])

        except (ValueError, IndexError):
            raise KiveMalformedDataException(
                'Server gave malformed PipelineInput object:\n%s' % obj
            )

    def __str__(self):
        return self.dataset_name

    def __unicode__(self):
        return self.dataset_name

    def __repr__(self):
        return '<Input (%d): %s (%s)>' % (self.dataset_idx, self.dataset_name, str(self.compounddatatype))


class Pipeline(object):
    """
    A wrapper class for Kive's Pipeline object
    """

    def __init__(self, obj):
        try:
            if type(obj) == dict:
                self.pipeline_id = obj['id']
                self.family = obj['family']
                self.revision_name = obj['display_name']
                self.revision_number = obj['revision_number']
                self.published = obj["published"] if "published" in obj else False
                self.inputs = [PipelineInput(i) for i in obj['inputs']]
                self.inputs = sorted(self.inputs, key=lambda x: x.dataset_idx)
                self.details = obj

            else:
                self.pipeline_id = object
                self.family = None
                self.revision_number = None
                self.revision_name = None
                self.published = None
                self.inputs = None

        except (ValueError, IndexError):
            raise KiveMalformedDataException(
                'Server gave malformed Pipeline object:\n%s' % obj
            )

    def __str__(self):
        return '%s - rev %d' % (self.revision_name, self.revision_number) if self.revision_name is not None else 'N/A'

    def __unicode__(self):
        return str(self)

    def __repr__(self):
        return '<Pipeline (%d): %s>' % (self.pipeline_id, str(self))


class PipelineFamily(object):
    """
    A wrapper class for Kive's PipelineFamily object
    """

    def __init__(self, obj):
        try:
            self.family_id = obj['id']
            self.name = obj['name']
            self.pipelines = [Pipeline(p) for p in obj['members']]
            try:
                self.published_version = [p for p in self.pipelines if p.published][0]
            except IndexError:
                self.published_version = None


        except (ValueError, IndexError):
            raise KiveMalformedDataException(
                'Server gave malformed PipelineFamily object:\n%s' % obj
            )

    def __str__(self):
        return self.name

    def __unicode__(self):
        return self.name

    def __repr__(self):
        return '<Pipeline family (%d): %s>' % (self.family_id, str(self))

    def latest(self):
        try:
            return sorted(self.pipelines, key=lambda p: p.revision_number, reverse=True)[0]
        except IndexError:
            return None

    def published_or_latest(self):
        return self.published_version if self.published_version is not None else self.latest()