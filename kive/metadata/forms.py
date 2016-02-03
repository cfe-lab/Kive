"""
metadata.forms
"""
import json
import copy

from django import forms
from django.contrib.auth.models import Group
from django.contrib.auth import get_user_model
from django.template import loader, Context

from metadata.models import Datatype, BasicConstraint, CompoundDatatypeMember, CompoundDatatype


def setup_form_users_allowed(form, users_allowed):
    """
    Helper that sets up the users_allowed field on a Form or ModelForm that has a users_allowed
    ModelMultipleChoiceField (e.g. anything that inherits from AccessControl).
    """
    form.fields["users_allowed"].queryset = users_allowed if users_allowed else get_user_model().objects.all()


def setup_form_groups_allowed(form, groups_allowed):
    form.fields["groups_allowed"].queryset = groups_allowed if groups_allowed else Group.objects.all()


class UsersAllowedWidget(forms.SelectMultiple):
    """
    A sub-widget of the PermissionsWidget.  This should not be created on its own.
    """
    class Media:
        css = {
            "all": ("metadata/accumulator.css",)
        }
        js = ("metadata/accumulator.js",)


class GroupsAllowedWidget(forms.SelectMultiple):
    """
    A sub-widget of the PermissionsWidget.  This should not be created on its own.
    """
    class Media:
        css = {
            "all": ("metadata/accumulator.css",)
        }
        js = ("metadata/accumulator.js",)


class PermissionsWidget(forms.MultiWidget):

    def __init__(self, user_choices=None, group_choices=None, attrs=None):
        self.user_choices = user_choices or []
        self.group_choices = group_choices or []
        attrs = attrs or {}

        hidden_ms_class = "pw-hidden-multiselect"
        users_attrs = copy.copy(attrs)
        user_widget_classes = "pw-hidden-users " + hidden_ms_class
        if "class" in users_attrs:
            users_attrs["class"] += " " + user_widget_classes
        else:
            users_attrs["class"] = user_widget_classes

        groups_attrs = copy.copy(attrs)
        group_widget_classes = "pw-hidden-groups " + hidden_ms_class
        if "class" in groups_attrs:
            groups_attrs["class"] += " " + group_widget_classes
        else:
            groups_attrs["class"] = group_widget_classes

        sub_widgets = [
            UsersAllowedWidget(attrs=users_attrs, choices=self.user_choices),
            GroupsAllowedWidget(attrs=groups_attrs, choices=self.group_choices)
        ]
        super(PermissionsWidget, self).__init__(sub_widgets, attrs)

    def decompress(self, value):
        """
        Unpacks a "glommed together" value passed by MultiValueField.

        value will be a JSON-encoded pair of lists: one for
        Users allowed and one for Groups allowed.
        """
        users_allowed = []
        groups_allowed = []
        if value:
            parsed_value = json.loads(value)
            assert isinstance(parsed_value, list)
            assert len(parsed_value) == 2
            users_and_groups = json.loads(value)

            # The internals work when passing around PKs but not the actual objects.
            users_allowed = [x for x in users_and_groups[0]]
            groups_allowed = [x for x in users_and_groups[1]]

        return [users_allowed, groups_allowed]

    def format_output(self, rendered_widgets):
        pw_template = loader.get_template("metadata/permissions_widget.html")
        users = [{"id": x[0], "username": x[1]} for x in self.user_choices]
        groups = [{"id": x[0], "name": x[1]} for x in self.group_choices]
        c = Context(
            {
                "users": users,
                "groups": groups,
                "users_widget": rendered_widgets[0],
                "groups_widget": rendered_widgets[1]
            }
        )
        return pw_template.render(c)


def user_choices(user_queryset):
    return [(x.id, x.username) for x in user_queryset]


def group_choices(group_queryset):
    return [(x.id, x.name) for x in group_queryset]


class PermissionsField(forms.MultiValueField):
    # The default widget for this field.  Any widget must have the same
    # prototype as PermissionsWidget.
    widget = PermissionsWidget

    def __init__(self, widget=None, require_all_fields=False, *args, **kwargs):
        # In order to avoid problems with using the User model at import time, we
        # set the queryset to None.  This means we must set it later using
        # set_user_groups_allowed, at run time.
        # user_queryset = user_queryset or get_user_model().objects.all()
        # group_queryset = group_queryset or Group.objects.all()

        fields = (
            forms.ModelMultipleChoiceField(queryset=None, required=False),
            forms.ModelMultipleChoiceField(queryset=None, required=False)
        )

        widget = widget or self.widget()
        if isinstance(widget, type):
            widget = widget()
        # This will be set later, at run time.
        widget.widgets[0].choices = user_choices([])
        widget.widgets[1].choices = group_choices([])

        super(PermissionsField, self).__init__(
            widget=widget,
            fields=fields,
            require_all_fields=require_all_fields,
            *args, **kwargs
        )

    def compress(self, data_list):
        # data_list consists of two lists: the first is of Users, the second of Groups.
        if len(data_list) == 0:
            user_pks = []
            group_pks = []
        else:
            user_pks = [x.pk for x in data_list[0]]
            group_pks = [x.pk for x in data_list[1]]

        return json.dumps([user_pks, group_pks])

    def set_users_groups_allowed(self, users_allowed, groups_allowed):
        """
        Update the valid users and groups allowed, and update the widget.
        """
        self.widget.user_choices = user_choices(users_allowed)
        self.fields[0].queryset = users_allowed
        self.widget.widgets[0].choices = self.widget.user_choices

        self.widget.group_choices = group_choices(groups_allowed)
        self.fields[1].queryset = groups_allowed
        self.widget.widgets[1].choices = self.widget.group_choices


class AccessControlForm(forms.Form):

    permissions = PermissionsField(
        label="Users and groups allowed",
        help_text="Which users and groups are allowed access to this resource?",
        required=False
    )

    def __init__(self, data=None, files=None, possible_users_allowed=None, possible_groups_allowed=None,
                 *args, **kwargs):
        super(AccessControlForm, self).__init__(data, files, *args, **kwargs)

        for idx in (0, 1):
            if "permissions_{}".format(idx) in self.initial:
                self.fields["permissions"].fields[idx].initial = self.initial["permissions_{}".format(idx)]

        # This is now at run time, so we can set the permissions.
        possible_users_allowed = possible_users_allowed or get_user_model().objects.all()
        possible_groups_allowed = possible_groups_allowed or Group.objects.all()
        self.fields["permissions"].set_users_groups_allowed(possible_users_allowed, possible_groups_allowed)


class DatatypeForm (forms.ModelForm):

    permissions = PermissionsField(
        label="Users and groups allowed",
        help_text="Which users and groups are allowed access to this Datatype?",
        required=False
    )

    restricts = forms.ModelMultipleChoiceField(
        queryset = Datatype.objects.all(),
        required=True,
        help_text='The new Datatype is a special case of one or more existing Datatypes; e.g., DNA restricts string.',
        initial=Datatype.objects.filter(name='string'))

    class Meta:
        model = Datatype
        fields = ('name', 'description', 'restricts', "permissions")

    def __init__(self, data=None, users_allowed=None, groups_allowed=None, *args, **kwargs):
        super(DatatypeForm, self).__init__(data, *args, **kwargs)

        users_allowed = users_allowed or get_user_model().objects.all()
        groups_allowed = groups_allowed or Group.objects.all()
        self.fields["permissions"].set_users_groups_allowed(users_allowed, groups_allowed)


class DatatypeDetailsForm (forms.ModelForm):

    permissions = PermissionsField(
        label="Users and groups allowed",
        help_text="Which users and groups are allowed access to this Datatype?",
        required=False
    )

    class Meta:
        model = Datatype
        fields = ('name', 'description', "permissions")

    def __init__(self, data=None, addable_users=None, addable_groups=None, *args, **kwargs):
        super(DatatypeDetailsForm, self).__init__(data, *args, **kwargs)
        self.fields["permissions"].set_users_groups_allowed(addable_users, addable_groups)


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

    permissions = PermissionsField(
        label="Users and groups allowed",
        help_text="Which users and groups are allowed access to this CompoundDatatype?",
        required=False
    )

    class Meta:
        model = CompoundDatatype
        exclude = ("user", "users_allowed", "groups_allowed")

    def __init__(self, data=None, users_allowed=None, groups_allowed=None, *args, **kwargs):
        super(CompoundDatatypeForm, self).__init__(data, *args, **kwargs)
        users_allowed = users_allowed if users_allowed is not None else get_user_model().objects.all()
        groups_allowed = groups_allowed if groups_allowed is not None else Group.objects.all()
        self.fields["permissions"].set_users_groups_allowed(users_allowed, groups_allowed)

