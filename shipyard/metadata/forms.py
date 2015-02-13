"""
metadata.forms
"""

from django import forms
from django.contrib.auth.models import User, Group

from datetime import datetime

from metadata.models import Datatype, BasicConstraint, CompoundDatatypeMember, CompoundDatatype


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

    def __init__(self, possible_users_allowed=None, possible_groups_allowed=None, *args, **kwargs):
        super(AccessControlForm, self).__init__(*args, **kwargs)
        if possible_users_allowed:
            self.fields["users_allowed"].queryset = possible_users_allowed
        else:
            self.fields["users_allowed"].queryset = User.objects.all()

        if possible_groups_allowed:
            self.fields["groups_allowed"].queryset = possible_groups_allowed
        else:
            self.fields["groups_allowed"].queryset = Group.objects.all()


class DatatypeForm (forms.ModelForm, AccessControlForm):

    restricts = forms.ModelMultipleChoiceField(
        queryset = Datatype.objects.all(),
        required=True,
        help_text='The new Datatype is a special case of one or more existing Datatypes; e.g., DNA restricts string.',
        initial=Datatype.objects.filter(name='string'))

    class Meta:
        model = Datatype
        fields = ('name', 'description', 'restricts', "users_allowed", "groups_allowed")


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

    class Meta:
        model = CompoundDatatypeMember
        exclude = ('compounddatatype', 'column_idx')


class CompoundDatatypeForm(forms.ModelForm, AccessControlForm):
    class Meta:
        model = CompoundDatatype
        exclude = ("user",)
