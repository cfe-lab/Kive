"""
metadata.forms
"""

from django import forms
from django.contrib.auth.models import User, Group

from metadata.models import Datatype, BasicConstraint, CompoundDatatypeMember, CompoundDatatype


def setup_form_users_allowed(form, users_allowed):
    """
    Helper that sets up the users_allowed field on a Form or ModelForm that has a users_allowed
    ModelMultipleChoiceField (e.g. anything that inherits from AccessControl).
    """
    form.fields["users_allowed"].queryset = users_allowed if users_allowed else User.objects.all()


def setup_form_groups_allowed(form, groups_allowed):
    form.fields["groups_allowed"].queryset = groups_allowed if groups_allowed else Group.objects.all()


class AccessControlForm(forms.Form):

    users_allowed = forms.ModelMultipleChoiceField(
        label="Users allowed",
        help_text="Which users are allowed access to this resource?",
        queryset=User.objects.all(),
        required=False
    )

    groups_allowed = forms.ModelMultipleChoiceField(
        label="Groups allowed",
        help_text="Which groups are allowed access to this resource?",
        queryset=Group.objects.all(),
        required=False
    )

    def __init__(self, data=None, files=None, possible_users_allowed=None, possible_groups_allowed=None,
                 *args, **kwargs):
        super(AccessControlForm, self).__init__(data, files, *args, **kwargs)
        setup_form_users_allowed(self, possible_users_allowed)
        setup_form_groups_allowed(self, possible_groups_allowed)


class DatatypeForm (forms.ModelForm):

    restricts = forms.ModelMultipleChoiceField(
        queryset = Datatype.objects.all(),
        required=True,
        help_text='The new Datatype is a special case of one or more existing Datatypes; e.g., DNA restricts string.',
        initial=Datatype.objects.filter(name='string'))

    class Meta:
        model = Datatype
        fields = ('name', 'description', 'restricts', "users_allowed", "groups_allowed")

    def __init__(self, data=None, users_allowed=None, groups_allowed=None, *args, **kwargs):
        super(DatatypeForm, self).__init__(data, *args, **kwargs)
        setup_form_users_allowed(self, users_allowed)
        setup_form_groups_allowed(self, groups_allowed)


class BasicConstraintForm (forms.ModelForm):
    #ruletype = forms.ChoiceField(BasicConstraint.CONSTRAINT_TYPES)
    class Meta:
        model = BasicConstraint
        exclude = ()
        #exclude = ('datatype', )


class IntegerConstraintForm (forms.Form):
    minval = forms.FloatField(required=False, help_text='Minimum numerical value')
    maxval = forms.FloatField(required=False, help_text='Maximum numerical value')


class StringConstraintForm (forms.Form):
    minlen = forms.IntegerField(required=False, help_text='Minimum string length (must be non-negative integer)')
    maxlen = forms.IntegerField(required=False, help_text='Maximum string length (must be non-negative integer)')
    regexp = forms.CharField(
        required=False,
        help_text=('A regular expression that can be recognized by the Python re module (Perl-like syntax). '
                   'To define multiple regexes, enclose each in double-quotes (") and separate with commas (,).'))


class CompoundDatatypeMemberForm(forms.ModelForm):
    datatype = forms.ModelChoiceField(
        queryset = Datatype.objects.all(), required=True, help_text="This column's expected datatype")

    blankable = forms.BooleanField(
        required=False,
        help_text="If a file has this CompoundDatatype, can its entries in this column be blank?"
    )

    class Meta:
        model = CompoundDatatypeMember
        exclude = ('compounddatatype', 'column_idx')

    def __init__(self, data=None, user=None, *args, **kwargs):
        super(CompoundDatatypeMemberForm, self).__init__(data, *args, **kwargs)

        if user is not None:
            self.fields["datatype"].queryset = Datatype.filter_by_user(user)


class CompoundDatatypeForm(forms.ModelForm):
    class Meta:
        model = CompoundDatatype
        exclude = ("user",)

    def __init__(self, data=None, users_allowed=None, groups_allowed=None, *args, **kwargs):
        super(CompoundDatatypeForm, self).__init__(data, *args, **kwargs)
        setup_form_users_allowed(self, users_allowed)
        setup_form_groups_allowed(self, groups_allowed)

