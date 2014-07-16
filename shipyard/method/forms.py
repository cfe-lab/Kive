"""
Generate an HTML form to create a new Datatype object
"""

from django import forms
from method.models import CodeResource, CodeResourceRevision, CodeResourceDependency, Method, MethodFamily
from metadata.models import CompoundDatatype
from transformation.models import TransformationInput, TransformationOutput, XputStructure

import logging

logger = logging.getLogger(__name__)

# code resource forms
class CodeResourceMinimalForm (forms.Form):
    """
    use for validating only two entries
    """
    revision_name = forms.CharField(max_length=255)
    revision_desc = forms.CharField(max_length=255)

class CodeResourcePrototypeForm (forms.ModelForm):
    """
    A form for submitting the first version of a CodeResource, which
    we refer to as the "prototype".  We require two sets of names and
    descriptions.  The first set refer to the CodeResource itself,
    which is an abstraction of a file that is going to be revised many
    times.  The resource name should be something that refers to the actual
    function of the CodeResource (e.g., NucleotideTranslator) rather than
    revision names that are only meant to tell different version apart,
    (e.g., "Scarlet (1)", "Bicycle (2)", "Henry (3)").
    """
    # additional form fields for CodeResource object
    resource_name = forms.CharField(max_length=255,
                                    label='Resource name',
                                    help_text='A name that refers to the actual function of the CodeResource.')
    resource_desc = forms.CharField(widget = forms.Textarea(attrs={'rows':5}),
                                    label = 'Resource description',
                                    help_text='A brief description of what this CodeResource (this and all subsequent '
                                              'versions) is supposed to do')

    def __init__(self, *args, **kwargs):
        super(CodeResourcePrototypeForm, self).__init__(*args, **kwargs)
        self.fields['revision_name'].label = 'Prototype name'
        self.fields['revision_name'].help_text = 'A short name for this prototype, ' \
                                                 'used only to differentiate it from subsequent versions.'

        self.fields['revision_desc'].label = 'Prototype description'
        self.fields['revision_desc'].help_text = 'A brief description of this prototype'
        self.fields['revision_desc'].initial = 'Prototype version'
        self.fields['revision_desc'].widget = forms.Textarea(attrs={'rows': 2})

        self.fields['content_file'].help_text = 'File containing this new code resource'
    class Meta:
        model = CodeResourceRevision
        fields = ('resource_name', 'resource_desc', 'content_file', 'revision_name', 'revision_desc')
        #exclude = ('revision_parent', 'coderesource', 'MD5_checksum',)

class CodeResourceRevisionForm (forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super(CodeResourceRevisionForm, self).__init__(*args, **kwargs)
        self.fields['content_file'].label = 'File'
    class Meta:
        model = CodeResourceRevision
        fields = ('content_file', 'revision_name', 'revision_desc', )


class CodeResourceDependencyForm (forms.ModelForm):
    def __init__(self, parent=None, *args, **kwargs):
        super(CodeResourceDependencyForm, self).__init__(*args, **kwargs)
        self.fields['coderesource'].choices = self.get_code_resource_list(parent)

    def get_code_resource_list(self, parent):
        # required to refresh list on addition of a new CodeResource
        if parent is None:
            queryset = CodeResource.objects.all()
        else:
            queryset = CodeResource.objects.exclude(pk=parent)
        logger.debug(queryset.query)
        return [('', '--- CodeResource ---')] + [(x.id, x.name) for x in queryset]

    coderesource = forms.ChoiceField(choices=[('', '--- CodeResource ---')] +
                                             [(x.id, x.name) for x in CodeResource.objects.all()])
    revisions = forms.ChoiceField(choices=[('', '--- select a CodeResource first ---')])

    class Meta:
        model = CodeResourceDependency
        #exclude = ('coderesourcerevision', 'requirement')
        fields = ('coderesource', 'revisions', 'depPath', 'depFileName')


class MethodForm (forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super(MethodForm, self).__init__(*args, **kwargs)

        # this is required to re-populate the drop-down with CRs created since first load
        self.fields['coderesource'].choices = [('', '--- CodeResource ---')] + \
                                              [(x.id, x.name) for x in CodeResource.objects.all().order_by('name')]
        self.fields['coderesource'].label = 'Code resource'
        self.fields['coderesource'].help_text = 'The code resource for which this method is a set of instructions.'

        self.fields['revision_name'].label = 'Name'
        self.fields['revision_name'].help_text = 'A short name for this new method'

        self.fields['revision_desc'].label = 'Description'
        self.fields['revision_desc'].help_text = 'A detailed description for this new method'

    coderesource = forms.ChoiceField(choices = [('', '--- CodeResource ---')] +
                                               [(x.id, x.name) for x in CodeResource.objects.all().order_by('name')])
    revisions = forms.ChoiceField(choices=[('', '--- select a CodeResource first ---')])

    class Meta:
        model = Method
        fields = ('coderesource', 'revisions', 'revision_name', 'revision_desc', 'deterministic')
        widgets = {
            'revision_desc': forms.Textarea(attrs={'rows': 5,
                                                   'cols': 30,
                                                   'style': 'height: 3em;'}),
        }


class MethodReviseForm (forms.ModelForm):
    """
    Revise an existing method.  No need to specify MethodFamily.
    """
    def __init__(self, *args, **kwargs):
        super(MethodReviseForm, self).__init__(*args, **kwargs)

        self.fields['revision_name'].label = 'Name'
        self.fields['revision_name'].help_text = 'A short name for this new method'

        self.fields['revision_desc'].label = 'Description'
        self.fields['revision_desc'].help_text = 'A detailed description for this new method'

    #coderesource = forms.ChoiceField(choices = [('', '--- CodeResource ---')] + [(x.id, x.name) for x in CodeResource.objects.all()])
    revisions = forms.ChoiceField() # to be populated by view function

    class Meta:
        model = Method
        fields = ('revisions', 'revision_name', 'revision_desc', 'deterministic')


class TransformationXputForm (forms.ModelForm):
    input_output = forms.ChoiceField(choices=[('input', 'IN'), ('output', 'OUT')])
    class Meta:
        model = TransformationInput # derived from abstract class TransformationXput
        fields = ('dataset_name', )


class XputStructureForm (forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super(XputStructureForm, self).__init__(*args, **kwargs)
        self.fields['compounddatatype'].choices = [('', '--------'), ('__raw__', 'Unstructured')] + \
                                                  [(x.id, str(x)) for x in CompoundDatatype.objects.all()]
        self.fields['min_row'].widget.attrs['class'] = 'shortIntField'
        self.fields['max_row'].widget.attrs['class'] = 'shortIntField'

    choices = [('', '--------'), ('__raw__', 'Unstructured')]
    choices.extend([(x.id, str(x)) for x in CompoundDatatype.objects.all()])

    compounddatatype = forms.ChoiceField(choices=choices)
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
