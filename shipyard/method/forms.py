"""
Generate an HTML form to create a new Datatype object
"""

from django import forms
from method.models import CodeResource, CodeResourceRevision, CodeResourceDependency, Method, MethodFamily
from metadata.models import CompoundDatatype
from transformation.models import TransformationInput, XputStructure
from django.contrib.auth.models import User, Group

import logging

logger = logging.getLogger(__name__)

# code resource forms
class CodeResourceMinimalForm (forms.Form):
    """
    use for validating only two entries
    """
    revision_name = forms.CharField(max_length=255)
    revision_desc = forms.CharField(max_length=255)


class CodeResourcePrototypeForm(forms.Form):
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
    # Form fields for the parent CodeResource object.
    resource_name = forms.CharField(
        max_length=255,
        label='Resource name',
        help_text='A name that refers to the actual function of the CodeResource.'
    )

    resource_desc = forms.CharField(
        widget = forms.Textarea(attrs={'rows':5}),
        label = 'Resource description',
        help_text='A brief description of what this CodeResource (this and all subsequent versions) is supposed to do'
    )

    # Stuff that goes directly into the CodeResourceRevision.
    content_file = forms.FileField(
        label="File",
        help_text="File containing this new code resource"
    )

    users_allowed = forms.MultipleChoiceField(
        label="Users allowed",
        help_text="Which users are allowed access to this resource?",
        choices=[(u.id, u.username) for u in User.objects.all()],
        required=False
    )

    groups_allowed = forms.MultipleChoiceField(
        label="Groups allowed",
        help_text="Which groups are allowed access to this resource?",
        choices=[(g.id, g.name) for g in Group.objects.all()],
        required=False
    )

    def __init__(self, *args, **kwargs):
        super(CodeResourcePrototypeForm, self).__init__(*args, **kwargs)
        self.fields["users_allowed"].choices = [(u.id, u.username) for u in User.objects.all()]
        self.fields["groups_allowed"].choices = [(g.id, g.name) for g in Group.objects.all()]


class CodeResourceRevisionForm(forms.Form):

    # Stuff that goes directly into the CodeResourceRevision.
    content_file = forms.FileField(
        label="File",
        help_text="File contents of this code resource revision"
    )

    revision_name = forms.CharField(
        label="Revision name",
        help_text="A short name to differentiate this revision from previous versions."
    )

    revision_desc = forms.CharField(
        widget=forms.Textarea(attrs={'rows': 2}),
        label="Revision description",
        help_text="A brief description of this version of the resource",
        initial=""
    )

    users_allowed = forms.MultipleChoiceField(
        label="Users allowed",
        help_text="Which users are allowed access to this resource?",
        choices=[(u.id, u.username) for u in User.objects.all()],
        required=False
    )

    groups_allowed = forms.MultipleChoiceField(
        label="Groups allowed",
        help_text="Which groups are allowed access to this resource?",
        choices=[(g.id, g.name) for g in Group.objects.all()],
        required=False
    )

    def __init__(self, *args, **kwargs):
        super(CodeResourceRevisionForm, self).__init__(*args, **kwargs)
        self.fields["users_allowed"].choices = [(u.id, u.username) for u in User.objects.all()]
        self.fields["groups_allowed"].choices = [(g.id, g.name) for g in Group.objects.all()]


class CodeResourceDependencyForm (forms.Form):
    """
    Form for submitting a CodeResourceDependency.

    initial:  A dictionary to pass initial values from view function
    parent:  Primary key (ID) of CodeResource having this dependency.
    """
    # The attrs to the widget are to enhance the resulting HTML output.
    coderesource = forms.ChoiceField(
        widget=forms.Select(attrs={'class': 'coderesource'}),
        choices=[('', '--- CodeResource ---')] + [(x.id, x.name) for x in CodeResource.objects.all()]
    )

    revisions = forms.ChoiceField(choices=[('', '--- select a CodeResource first ---')])

    depPath = forms.CharField(
        label="Dependency path",
        help_text="Where a code resource dependency must exist in the sandbox relative to it's parent"
    )

    depFileName = forms.CharField(
        label="Dependency file name",
        help_text="The file name the dependency is given on the sandbox at execution"
    )

    def __init__(self, initial=None, parent=None, *args, **kwargs):
        super(CodeResourceDependencyForm, self).__init__(*args, **kwargs)
        self.fields['coderesource'].choices = self.get_code_resource_list(parent)
        if initial:
            # populate drop-downs before rendering template
            cr = CodeResource.objects.get(pk=initial['coderesource'])
            self.fields['coderesource'].initial = cr.pk

            rev = CodeResourceRevision.objects.get(coderesource=cr)
            if type(rev) is list:
                self.fields['revisions'].choices = [(x.pk, x.revision_name) for x in rev]
                self.fields['revisions'].initial = initial['revisions']
            else:
                self.fields['revisions'].choices = [(rev.pk, rev.revision_name)]

            self.fields['depPath'].initial = initial['depPath']
            self.fields['depFileName'].initial = initial['depFileName']


    def get_code_resource_list(self, parent):
        # required to refresh list on addition of a new CodeResource
        if parent is None:
            queryset = CodeResource.objects.all()
        else:
            queryset = CodeResource.objects.exclude(pk=parent)
        logger.debug(queryset.query)
        return [('', '--- CodeResource ---')] + [(x.id, x.name) for x in queryset]

    class Meta:
        model = CodeResourceDependency
        #exclude = ('coderesourcerevision', 'requirement')
        fields = ('coderesource', 'revisions', 'depPath', 'depFileName')


class MethodForm(forms.ModelForm):
    coderesource = forms.ChoiceField(choices = [('', '--- CodeResource ---')] +
                                               [(x.id, x.name) for x in CodeResource.objects.all().order_by('name')])
    revisions = forms.ChoiceField(choices=[('', '--- select a CodeResource first ---')])

    users_allowed = forms.MultipleChoiceField(
        label="Users allowed",
        help_text="Which users are allowed access to this resource?",
        choices=[(u.id, u.username) for u in User.objects.all()],
        required=False
    )

    groups_allowed = forms.MultipleChoiceField(
        label="Groups allowed",
        help_text="Which groups are allowed access to this resource?",
        choices=[(g.id, g.name) for g in Group.objects.all()],
        required=False
    )

    # We override the threads field.
    threads = forms.IntegerField(min_value=1, initial=1,
                                 help_text="Number of threads used during execution")

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

    class Meta:
        model = Method
        fields = ('coderesource', 'revisions', 'revision_name', 'revision_desc', 'reusable', "threads")
        widgets = {
            'revision_desc': forms.Textarea(attrs={'rows': 5,
                                                   'cols': 30,
                                                   'style': 'height: 5em;'}),
        }


class MethodReviseForm (forms.ModelForm):
    """Revise an existing method.  No need to specify MethodFamily."""
    users_allowed = forms.MultipleChoiceField(
        label="Users allowed",
        help_text="Which users are allowed access to this resource?",
        choices=[(u.id, u.username) for u in User.objects.all()],
        required=False
    )

    groups_allowed = forms.MultipleChoiceField(
        label="Groups allowed",
        help_text="Which groups are allowed access to this resource?",
        choices=[(g.id, g.name) for g in Group.objects.all()],
        required=False
    )

    # We override the threads field.
    threads = forms.IntegerField(min_value=1, initial=1,
                                 help_text="Number of threads used during execution")

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
        fields = ('revisions', 'revision_name', 'revision_desc', 'reusable', "threads")


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
    """Form to create a new MethodFamily"""
    def __init__ (self, *args, **kwargs):
        super(MethodFamilyForm, self).__init__(*args, **kwargs)
        self.fields['name'].label = 'Family name'
        self.fields['description'].label = 'Family description'
    class Meta:
        model = MethodFamily
        widgets = {
            'description': forms.Textarea(attrs={'rows': 5,
                                                   'cols': 30,
                                                   'style': 'height: 5em;'}),
        }
        exclude = ()
