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
        self.fields['coderesource'].label = 'Code resource'
        self.fields['coderesource'].help_text = 'The code resource for which this method is a set of instructions.'

        self.fields['revision_name'].label = 'Name'
        self.fields['revision_name'].help_text = 'A short name for this new method'

        self.fields['revision_desc'].label = 'Description'
        self.fields['revision_desc'].help_text = 'A detailed description for this new method'

        self.fields['family'].help_text = 'Assign this new method to an existing MethodFamily, or leave blank to create new family'

    coderesource = forms.ChoiceField(choices = [('', '--- CodeResource ---')] + [(x.id, x.name) for x in CodeResource.objects.all()])
    revisions = forms.ChoiceField(choices=[('', '--- select a CodeResource first ---')])

    class Meta:
        model = Method
        fields = ('revision_name', 'revision_desc', 'random', 'coderesource', 'revisions', 'family')


class TransformationXputForm (forms.ModelForm):
    input_output = forms.ChoiceField(choices=[('input', 'IN'), ('output', 'OUT')])
    class Meta:
        model = TransformationInput # derived from abstract class TransformationXput
        fields = ('dataset_name', )


class XputStructureForm (forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super(XputStructureForm, self).__init__(*args, **kwargs)
        self.fields['min_row'].widget.attrs['class'] = 'shortIntField'
        self.fields['max_row'].widget.attrs['class'] = 'shortIntField'
    compounddatatype = forms.ModelChoiceField(queryset = CompoundDatatype.objects.all())
    class Meta:
        model = XputStructure
        fields = ('compounddatatype', 'min_row', 'max_row')

class MethodFamilyForm (forms.ModelForm):
    def __init__ (self, *args, **kwargs):
        super(MethodFamilyForm, self).__init__(*args, **kwargs)
        self.fields['name'].label = 'Family name'
        self.fields['description'].label = 'Family description'
    class Meta:
        model = MethodFamily