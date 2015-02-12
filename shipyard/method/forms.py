"""
Generate an HTML form to create a new Datatype object
"""

from django import forms
from method.models import CodeResource, CodeResourceRevision, CodeResourceDependency, Method, MethodFamily
from metadata.models import CompoundDatatype
from transformation.models import TransformationInput, XputStructure
from django.contrib.auth.models import User, Group
from metadata.forms import AccessControlForm

import logging

logger = logging.getLogger(__name__)


# CodeResource forms.
class CodeResourceMinimalForm(AccessControlForm):
    """
    use for validating only two entries
    """
    revision_name = forms.CharField(max_length=255)
    revision_desc = forms.CharField(max_length=255)


class CodeResourcePrototypeForm(AccessControlForm):
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
        widget=forms.Textarea(attrs={'rows':5}),
        label='Resource description',
        help_text='A brief description of what this CodeResource (this and all subsequent versions) is supposed to do'
    )

    # Stuff that goes directly into the CodeResourceRevision.
    content_file = forms.FileField(
        label="File",
        help_text="File containing this new code resource"
    )


class CodeResourceRevisionForm(AccessControlForm):

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


def _get_code_resource_list(but_not_this_one=None):
    """
    Gets all CodeResources other than that of the specified one.
    """
    # required to refresh list on addition of a new CodeResource
    if but_not_this_one is None:
        queryset = CodeResource.objects.all()
    else:
        queryset = CodeResource.objects.exclude(pk=but_not_this_one)
    # logger.debug(queryset.query)
    return [('', '--- CodeResource ---')] + [(x.id, x.name) for x in queryset]


class CodeResourceDependencyForm(forms.Form):
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

    # We override this field so that it doesn't try to validate.
    revisions = forms.IntegerField(widget=forms.Select(choices=[('', '--- select a CodeResource first ---')]))

    depPath = forms.CharField(
        label="Dependency path",
        help_text="Where a code resource dependency must exist in the sandbox relative to it's parent",
        required=False
    )

    depFileName = forms.CharField(
        label="Dependency file name",
        help_text="The file name the dependency is given on the sandbox at execution",
        required=False
    )

    def __init__(self, data=None, initial=None, parent=None, *args, **kwargs):
        super(CodeResourceDependencyForm, self).__init__(data, *args, initial=initial, **kwargs)

        self.fields['coderesource'].choices = _get_code_resource_list(parent)
        if initial:
            # Re-populate drop-downs before rendering the template.
            cr = CodeResource.objects.get(pk=initial['coderesource'])

            rev = CodeResourceRevision.objects.filter(coderesource=cr)
            self.fields['revisions'].widget.choices = [(x.pk, x.revision_name) for x in rev]
            if initial.has_key("revisions"):
                assert initial.has_key("coderesource")
                assert initial["revisions"] in [x.pk for x in rev]


# Method forms.
class MethodReviseForm(AccessControlForm):
    """Revise an existing method.  No need to specify the CodeResource."""
    # This is populated by the calling view.
    revisions = forms.ChoiceField()

    revision_name = forms.CharField(
        label="Name",
        help_text="A short name for this new method"
    )

    revision_desc = forms.CharField(
        label="Description",
        help_text="A detailed description for this new method",
        widget=forms.Textarea(attrs={'rows': 5, 'cols': 30, 'style': 'height: 5em;'})
    )

    reusable = forms.ChoiceField(
        choices=Method.REUSABLE_CHOICES,
        help_text="""Is the output of this method the same if you run it again with the same inputs?

deterministic: always exactly the same

reusable: the same but with some insignificant differences (e.g., rows are shuffled)

non-reusable: no -- there may be meaningful differences each time (e.g., timestamp)
""")

    threads = forms.IntegerField(min_value=1, initial=1,
                                 help_text="Number of threads used during execution")


class MethodForm(MethodReviseForm):
    """
    Form used in creating a Method.
    """
    coderesource = forms.ChoiceField(
        choices = ([('', '--- CodeResource ---')] +
                   [(x.id, x.name) for x in CodeResource.objects.all().order_by('name')]),
        label="Code resource",
        help_text="The code resource for which this method is a set of instructions.",
        required=True)

    # We override this field.
    revisions = forms.IntegerField(
        widget=forms.Select(choices=[('', '--- select a CodeResource first ---')])
    )

    def __init__(self, *args, **kwargs):
        super(MethodForm, self).__init__(*args, **kwargs)

        # This is required to re-populate the drop-down with CRs created since first load.
        self.fields['coderesource'].choices = [('', '--- CodeResource ---')] + \
                                              [(x.id, x.name) for x in CodeResource.objects.all().order_by('name')]


class TransformationXputForm (forms.ModelForm):
    class Meta:
        model = TransformationInput  # derived from abstract class TransformationXput
        fields = ('dataset_name', )


class XputStructureForm (forms.Form):
    XS_CHOICES = [('', '--------'), ('__raw__', 'Unstructured')]
    XS_CHOICES.extend([(x.id, str(x)) for x in CompoundDatatype.objects.all()])

    compounddatatype = forms.ChoiceField(choices=XS_CHOICES)

    min_row = forms.IntegerField(
        min_value=0, initial=0,
        label="Minimum rows",
        help_text="Minimum number of rows this input/output returns",
        required=False,
        widget=forms.NumberInput(attrs={"class": "shortIntField"}))

    max_row = forms.IntegerField(
        min_value=1,
        label="Maximum rows",
        help_text="Maximum number of rows this input/output returns",
        required=False,
        widget=forms.NumberInput(attrs={"class": "shortIntField"}))

    def __init__(self, *args, **kwargs):
        super(XputStructureForm, self).__init__(*args, **kwargs)
        self.fields['compounddatatype'].choices = [('', '--------'), ('__raw__', 'Unstructured')] + \
                                                  [(x.id, str(x)) for x in CompoundDatatype.objects.all()]


class MethodFamilyForm (forms.Form):
    """Form used in creating a new MethodFamily."""
    name = forms.CharField(label="Family name")

    description = forms.CharField(
        label="Family description",
        widget=forms.Textarea(attrs={'rows': 5, 'cols': 30, 'style': 'height: 5em;'}),
    )
