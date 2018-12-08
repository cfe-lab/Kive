"""
Generate an HTML form to create a new Datatype object
"""
from django.forms import ModelForm
from django.http import Http404
from django import forms
from django.contrib.auth.models import Group
from django.contrib.auth import get_user_model

from container.models import ContainerFamily
from method.models import CodeResource, CodeResourceRevision, Method, MethodFamily, DockerImage
from metadata.models import CompoundDatatype
from metadata.forms import AccessControlForm, PermissionsField

import logging

logger = logging.getLogger(__name__)


# CodeResource forms.
class CodeResourceDetailsForm(forms.ModelForm):
    """
    Form used for updating a CodeResource (not a CodeResourceRevision).
    """
    permissions = PermissionsField(
        label="Users and groups allowed",
        help_text="Which users and groups are allowed access to this CodeResource?",
        required=False
    )

    class Meta:
        model = CodeResource
        fields = ("name", "description", "permissions")

    def __init__(self, data=None, addable_users=None, addable_groups=None, *args, **kwargs):
        addable_users = addable_users if addable_users is not None else get_user_model().objects.all()
        addable_groups = addable_groups if addable_groups is not None else Group.objects.all()
        super(CodeResourceDetailsForm, self).__init__(data, *args, **kwargs)
        self.fields["permissions"].set_users_groups_allowed(addable_users, addable_groups)


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
        widget=forms.Textarea(attrs={'rows': 5}),
        label='Resource description',
        help_text='A brief description of what this CodeResource (this and all subsequent versions) is supposed to do',
        required=False
    )

    # Stuff that goes directly into the CodeResourceRevision.
    content_file = forms.FileField(
        label="File",
        help_text="File containing this new code resource",
        required=False,
        allow_empty_file=True
    )


class CodeResourceRevisionDetailsForm(forms.ModelForm):
    """
    Form used for updating a CodeResourceRevision.
    """
    permissions = PermissionsField(
        label="Users and groups allowed",
        help_text="Which users and groups are allowed access to this CodeResourceRevision?",
        required=False
    )

    class Meta:
        model = CodeResourceRevision
        fields = ("revision_name", "revision_desc", "permissions")

    def __init__(self, data=None, addable_users=None, addable_groups=None, *args, **kwargs):
        addable_users = addable_users if addable_users is not None else get_user_model().objects.all()
        addable_groups = addable_groups if addable_groups is not None else Group.objects.all()
        super(CodeResourceRevisionDetailsForm, self).__init__(data, *args, **kwargs)
        self.fields["permissions"].set_users_groups_allowed(addable_users, addable_groups)


class CodeResourceRevisionForm(AccessControlForm):

    # Stuff that goes directly into the CodeResourceRevision.
    content_file = forms.FileField(
        label="File",
        help_text="File contents of this code resource revision",
        required=False,
        allow_empty_file=True
    )

    revision_name = forms.CharField(
        label="Revision name",
        help_text="A short name to differentiate this revision from previous versions.",
        required=False
    )

    revision_desc = forms.CharField(
        widget=forms.Textarea(attrs={'rows': 2}),
        label="Revision description",
        help_text="A brief description of this version of the resource",
        initial="",
        required=False
    )


def _get_code_resource_list(user, but_not_this_one=None):
    """
    Gets all CodeResources other than that of the specified one.

    This is required to refresh the list of eligible CodeResources during the
    addition of a new MethodDependency.
    """
    if user is None:
        queryset = CodeResource.objects.all()
    else:
        queryset = CodeResource.filter_by_user(user).distinct()
    if but_not_this_one is not None:
        queryset = queryset.exclude(pk=but_not_this_one)
    return [('', '--- CodeResource ---')] + [(x.id, x.name) for x in queryset]


class MethodDependencyForm(forms.Form):
    """
    Form for submitting a MethodDependency.

    initial:  A dictionary to pass initial values from view function
    parent:  Primary key (ID) of Method having this dependency.
    """
    # The attrs to the widget are to enhance the resulting HTML output.
    coderesource = forms.ChoiceField(
        widget=forms.Select(attrs={'class': 'coderesource'}),
        choices=[('', '--- CodeResource ---')],
        required=False
    )

    # We override this field so that it doesn't try to validate.
    revisions = forms.IntegerField(
        widget=forms.Select(choices=[('', '--- select a CodeResource first ---')]),
        required=False)

    path = forms.CharField(
        label="Dependency path",
        help_text="Where a code resource dependency must exist in the sandbox relative to it's parent",
        required=False
    )

    filename = forms.CharField(
        label="Dependency file name",
        help_text="The file name the dependency is given on the sandbox at execution",
        required=False
    )

    def __init__(self, data=None, user=None, initial=None, *args, **kwargs):
        super(MethodDependencyForm, self).__init__(data, initial=initial, *args, **kwargs)

        eligible_crs = _get_code_resource_list(user)
        self.fields['coderesource'].choices = eligible_crs

        # Re-populate drop-downs before rendering if possible.
        populator = None
        if data is not None:
            populator = data
        elif initial is not None:
            populator = initial

        if populator is not None:
            # Re-populate drop-downs before rendering the template.
            cr_pk = populator["coderesource"]

            # The first entry of eligible_crs is ("", "--- CodeResource ---") so we skip it.
            if int(cr_pk) not in [int(x[0]) for x in eligible_crs[1:]]:
                raise Http404(
                    "CodeResource with ID {} used in dependency definition is invalid".format(
                        populator["coderesource"]))

            rev = CodeResourceRevision.filter_by_user(user).filter(coderesource__pk=cr_pk)
            self.fields['revisions'].widget.choices = [
                (x.pk, x.revision_name) for x in rev]
            if "revisions" in populator:
                try:
                    assert "coderesource" in populator
                    assert int(populator["revisions"]) in (x.pk for x in rev)
                except AssertionError as e:
                    raise Http404(e)


# Method forms.
class MethodReviseForm(AccessControlForm):
    """Revise an existing method.  No need to specify the CodeResource."""
    # This is populated by the calling view.
    driver_revisions = forms.IntegerField(
        label='Revisions',
        widget=forms.Select(choices=[('', '--- select a CodeResource first ---')]),
        help_text='Select a revision of the driver script.',
        required=False)

    container = forms.IntegerField(
        label="Container",
        widget=forms.Select(choices=[('', '--- select a Container Family first ---')]),
        help_text="Method will run in this container")

    revision_name = forms.CharField(
        label="Name",
        help_text="A short name for this new method",
        required=False
    )

    revision_desc = forms.CharField(
        label="Description",
        help_text="A detailed description for this new method",
        widget=forms.Textarea(attrs={'rows': 5, 'cols': 30, 'style': 'height: 5em;'}),
        required=False
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

    memory = forms.IntegerField(min_value=0, initial=6000,
                                help_text="Memory (MB) required for execution (0 allocates all memory on the node)")

    confirm_shebang = forms.BooleanField(label="Override a missing shebang?",
                                         help_text="Click to override a missing shebang in the code resource",
                                         initial=False,
                                         required=False)


class MethodForm(MethodReviseForm):
    """
    Form used in creating a Method.
    """
    coderesource = forms.ChoiceField(
        choices=[('', '--- CodeResource ---')],
        label="Code resource",
        help_text="Driver script for the method",
        required=False)

    containerfamily = forms.ChoiceField(
        choices=[('', '--- Container Family ---')],
        label='Container Family',
        help_text="Container to run the method in",
        required=False)

    def __init__(self, data=None, user=None, *args, **kwargs):
        super(MethodForm, self).__init__(data, *args, **kwargs)

        # This is required to re-populate the drop-down with CRs created since first load.
        if user is not None:
            self.fields["coderesource"].choices = (
                [('', '--- CodeResource ---')] +
                [(x.id, x.name) for x in CodeResource.filter_by_user(user).order_by('name')]
            )
            self.fields["containerfamily"].choices = (
                [('', '--- Container Family ---')] +
                [(x.id, x.name) for x in ContainerFamily.filter_by_user(user)])


class MethodDetailsForm(forms.ModelForm):
    permissions = PermissionsField(
        label="Users and groups allowed",
        help_text="Which users and groups are allowed access to this Method?",
        required=False
    )

    class Meta:
        model = Method
        fields = ("revision_name", "revision_desc", "permissions")

    def __init__(self, data=None, addable_users=None, addable_groups=None, *args, **kwargs):
        addable_users = addable_users if addable_users is not None else get_user_model().objects.all()
        addable_groups = addable_groups if addable_groups is not None else Group.objects.all()
        super(MethodDetailsForm, self).__init__(data, *args, **kwargs)
        self.fields["permissions"].set_users_groups_allowed(addable_users, addable_groups)


class TransformationXputForm (forms.Form):
    dataset_name = forms.CharField()


class XputStructureForm (forms.Form):
    XS_CHOICES = [('', '--------'), ('__raw__', 'Unstructured')]

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

    def __init__(self, data=None, user=None, *args, **kwargs):
        super(XputStructureForm, self).__init__(data=data, *args, **kwargs)

        more_choices = CompoundDatatype.choices(user)
        self.fields['compounddatatype'].choices = [('', '--------'), ('__raw__', 'Unstructured')] + more_choices


class MethodFamilyForm(forms.ModelForm):
    """Form used in creating a new MethodFamily."""
    permissions = PermissionsField(
        label="Users and groups allowed",
        help_text="Which users and groups are allowed access to this MethodFamily?",
        required=False
    )

    name = forms.CharField(label="Family name")

    description = forms.CharField(
        label="Family description",
        widget=forms.Textarea(attrs={'rows': 5, 'cols': 30, 'style': 'height: 5em;'}),
        required=False
    )

    class Meta:
        model = MethodFamily
        fields = ("name", "description", "permissions")

    def __init__(self, data=None, addable_users=None, addable_groups=None, *args, **kwargs):
        addable_users = addable_users if addable_users is not None else get_user_model().objects.all()
        addable_groups = addable_groups if addable_groups is not None else Group.objects.all()
        super(MethodFamilyForm, self).__init__(data, *args, **kwargs)
        self.fields["permissions"].set_users_groups_allowed(addable_users, addable_groups)


class DockerImageForm(ModelForm):
    permissions = PermissionsField(
        label="Users and groups allowed",
        help_text="Which users and groups are allowed access to this image?",
        required=False)

    class Meta:
        model = DockerImage
        fields = ['name',
                  'git',
                  'tag',
                  'description',
                  'permissions']

    def __init__(self, data=None, addable_users=None, addable_groups=None, *args, **kwargs):
        addable_users = addable_users if addable_users is not None else get_user_model().objects.all()
        addable_groups = addable_groups if addable_groups is not None else Group.objects.all()
        super(DockerImageForm, self).__init__(data, *args, **kwargs)
        self.fields["permissions"].set_users_groups_allowed(addable_users, addable_groups)
