"""
metadata.forms
"""
import json
import copy

from django import forms
from django.contrib.auth.models import Group
from django.contrib.auth import get_user_model
from django.forms.utils import ErrorList


class UsersAllowedWidget(forms.SelectMultiple):
    """
    A sub-widget of the PermissionsWidget.  This should not be created on its own.
    """
    class Media(object):
        css = {
            "all": ("metadata/accumulator.css",)
        }
        js = ("metadata/accumulator.js",)


class GroupsAllowedWidget(forms.SelectMultiple):
    """
    A sub-widget of the PermissionsWidget.  This should not be created on its own.
    """
    class Media(object):
        css = {
            "all": ("metadata/accumulator.css",)
        }
        js = ("metadata/accumulator.js",)


class PermissionsWidget(forms.MultiWidget):
    template_name = "metadata/permissions_widget.html"
    widgets = None  # Filled in later.

    def __init__(self, user_choices=None, group_choices=None, attrs=None):
        self.user_choices = user_choices or []
        self.group_choices = group_choices or []
        self.old_users = []
        self.old_groups = []
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
            GroupsAllowedWidget(attrs=groups_attrs, choices=self.group_choices)]
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

    def get_context(self, name, value, attrs):
        context = super(PermissionsWidget, self).get_context(name, value, attrs)
        context['users'] = [{"username": x[0]} for x in self.user_choices]
        context['groups'] = [{"name": x[0]} for x in self.group_choices]
        context['old_users'] = [{"username": x[0]} for x in self.old_users]
        context['old_groups'] = [{"name": x[0]} for x in self.old_groups]
        return context


def get_user_choices(user_queryset):
    return [(x.username, x.username) for x in user_queryset]


def get_group_choices(group_queryset):
    return [(x.name, x.name) for x in group_queryset]


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
            forms.ModelMultipleChoiceField(queryset=None, required=False, to_field_name="username"),
            forms.ModelMultipleChoiceField(queryset=None, required=False, to_field_name="name")
        )

        widget = widget or self.widget()
        if isinstance(widget, type):
            widget = widget()
        # This will be set later, at run time.
        widget.widgets[0].choices = get_user_choices([])
        widget.widgets[1].choices = get_group_choices([])

        super(PermissionsField, self).__init__(
            widget=widget,
            fields=fields,
            require_all_fields=require_all_fields,
            *args, **kwargs
        )

    def compress(self, data_list):
        # data_list consists of two lists: the first is of Users, the second of Groups.
        if len(data_list) == 0:
            user_names = []
            group_names = []
        else:
            user_names = [x.username for x in data_list[0]]
            group_names = [x.name for x in data_list[1]]

        return json.dumps([user_names, group_names])

    def set_users_groups_allowed(self, users_allowed, groups_allowed):
        """
        Update the valid users and groups allowed, and update the widget.
        """
        self.widget.user_choices = get_user_choices(users_allowed)
        self.fields[0].queryset = users_allowed
        self.widget.widgets[0].choices = self.widget.user_choices

        self.widget.group_choices = get_group_choices(groups_allowed)
        self.fields[1].queryset = groups_allowed
        self.widget.widgets[1].choices = self.widget.group_choices

    def set_old_users_groups(self, old_users, old_groups):
        self.widget.old_users = get_user_choices(old_users)
        self.widget.old_groups = get_group_choices(old_groups)
        if self.widget.old_users or self.widget.old_groups:
            placeholder = [('None', 'None')]
            if not self.widget.old_users:
                self.widget.old_users = placeholder
            if not self.widget.old_groups:
                self.widget.old_groups = placeholder


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


class PermissionsForm(forms.ModelForm):
    """ Improved version of AccessControlForm.

    Views that use subclasses of this form should override get_form_kwargs()
    and set kwargs['user'], plus an optional list in kwargs['access_limits'].
    """
    permissions = PermissionsField(
        label="Users and groups allowed",
        required=False)

    def __init__(self,
                 data=None,
                 files=None,
                 auto_id='id_%s',
                 prefix=None,
                 initial=None,
                 error_class=ErrorList,
                 label_suffix=None,
                 empty_permitted=False,
                 instance=None,
                 *args,
                 **kwargs):
        """ Initialize the permissions field of a form.

        Supports all the regular parameters for a ModelForm, plus:
        :param access_limits: list of AccessControl objects - a user or group
            must have access to all of these objects to be available in the
            permissions field
        """
        self.access_limits = kwargs.pop('access_limits', [])

        super(PermissionsForm, self).__init__(data,
                                              files,
                                              auto_id,
                                              prefix,
                                              initial,
                                              error_class,
                                              label_suffix,
                                              empty_permitted,
                                              instance,
                                              *args,
                                              **kwargs)

        # Find which users and groups haven't been added yet.
        if instance is None:
            addable_users = get_user_model().objects.all()
            addable_groups = Group.objects.all()
        else:
            addable_users, addable_groups = instance.other_users_groups()
            self.fields["permissions"].set_old_users_groups(
                instance.users_allowed.all(),
                instance.groups_allowed.all())

        # Limit to users and groups that can see dependencies.
        for access_limit in self.access_limits:
            addable_users, addable_groups = access_limit.intersect_permissions(
                addable_users,
                addable_groups)
        self.fields["permissions"].set_users_groups_allowed(addable_users,
                                                            addable_groups)
