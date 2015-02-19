"""
Forms for creating Pipeline objects.
"""

from django import forms

from pipeline.models import PipelineStep


class PipelineStepForm (forms.ModelForm):
    """
    content_type - either 'method' or 'pipeline'
    object_id - ???
    transformation - ForeignKey to Method or Pipeline
    step_num - the step this transformation occupies in Pipeline
    outputs_to_delete - keys to TransformationOutput objects
    """
    class Meta:
        model = PipelineStep
        exclude = (pipeline, )


class PipelineStepInputCableForm (forms.ModelForm):
    pass