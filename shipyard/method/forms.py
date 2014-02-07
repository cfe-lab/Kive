"""
Generate an HTML form to create a new Datatype object
"""

from django import forms
from method.models import CodeResource, CodeResourceRevision, CodeResourceDependency, Method, MethodFamily
from metadata.models import CompoundDatatype
from transformation.models import TransformationInput, TransformationOutput, XputStructure

# code resource forms
class CodeResourceMinimalForm (forms.Form):
    """
    use for validating only two entries
    """
    revision_name = forms.CharField(max_length=255)
    revision_desc = forms.CharField(max_length=255)

class CodeResourcePrototypeForm (forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super(CodeResourcePrototypeForm, self).__init__(*args, **kwargs)
        self.fields['revision_name'].label = 'Name of prototype'
        self.fields['revision_name'].help_text = 'A short name for this prototype'
        self.fields['revision_desc'].label = 'Description'
        self.fields['revision_desc'].help_text = 'A detailed description of this prototype'
        self.fields['content_file'].help_text = 'File containing this new code resource'
    class Meta:
        model = CodeResourceRevision
        #fields = ('revision_name', 'revision_desc', 'content_file')
        exclude = ('revision_parent', 'coderesource', 'MD5_checksum',)

class CodeResourceRevisionForm (forms.ModelForm):
   class Meta:
        model = CodeResourceRevision
        fields = ('revision_name', 'revision_desc', 'content_file', )

class CodeResourceDependencyForm (forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super(CodeResourceDependencyForm, self).__init__(*args, **kwargs)
        self.fields['coderesource'].choices = self.get_code_resource_list()

    def get_code_resource_list(self):
        return [('', '--- CodeResource ---')] + [(x.id, x.name) for x in CodeResource.objects.all()]

    coderesource = forms.ChoiceField(choices = [('', '--- CodeResource ---')] + [(x.id, x.name) for x in CodeResource.objects.all()])
    revisions = forms.ChoiceField(choices=[('', '--- select a CodeResource first ---')])

    class Meta:
        model = CodeResourceDependency
        exclude = ('coderesourcerevision', 'requirement')


class MethodForm (forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super(MethodForm, self).__init__(*args, **kwargs)
        self.fields['coderesource'].choices = [('', '--- CodeResource ---')] + [(x.id, x.name) for x in CodeResource.objects.all()]

    coderesource = forms.ChoiceField(choices = [('', '--- CodeResource ---')] + [(x.id, x.name) for x in CodeResource.objects.all()])
    revisions = forms.ChoiceField(choices=[('', '--- select a CodeResource first ---')])
    class Meta:
        model = Method
        fields = ('revision_name', 'revision_desc', 'random')


class TransformationInputForm (forms.ModelForm):
    class Meta:
        model = TransformationInput # derived from abstract class TransformationXput
        fields = ('dataset_name', 'dataset_idx')

class TransformationOutputForm (forms.ModelForm):
    class Meta:
        model = TransformationOutput
        fields = ('dataset_name', 'dataset_idx')

class XputStructureForm (forms.ModelForm):
    compounddatatype = forms.ModelChoiceField(queryset = CompoundDatatype.objects.all())
    class Meta:
        model = XputStructure
        fields = ('compounddatatype', 'min_row', 'max_row')