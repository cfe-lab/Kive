from django.core.exceptions import ValidationError as DjangoValidationError, \
    NON_FIELD_ERRORS as DJANGO_NON_FIELD_ERRORS
from django.db import transaction

from rest_framework import permissions, mixins, serializers
from rest_framework.decorators import detail_route
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework.settings import api_settings
from rest_framework.viewsets import ReadOnlyModelViewSet
from rest_framework.exceptions import APIException

from archive.models import summarize_redaction_plan
from metadata.models import AccessControl, RTPNotFinished
from portal.views import developer_check, admin_check


def convert_validation(ex):
    """ Convert Django validation error to REST framework validation error """
    
    errors = {}
    for message in ex:
        if message is tuple:
            field, error = message
            if field != DJANGO_NON_FIELD_ERRORS:
                translated_field = field
            else:
                translated_field = api_settings.NON_FIELD_ERRORS_KEY
        else:
            translated_field = api_settings.NON_FIELD_ERRORS_KEY
            error = message
        errors[translated_field] = error
    
    return serializers.ValidationError(errors)


class StandardPagination(PageNumberPagination):
    page_size_query_param = 'page_size'


class IsGrantedReadOnly(permissions.BasePermission):
    """ Custom permission for historical resources like runs.
    
    All authenticated users can see instances they have been allowed access to
    either because they own them, they are in users_allowed, or they are in
    groups_allowed.
    Only administrators can modify records.
    """
    def has_permission(self, request, view):
        return (admin_check(request.user) or
                request.method in permissions.SAFE_METHODS)
    
    def has_object_permission(self, request, view, obj):
        if admin_check(request.user):
            return True
        return obj.can_be_accessed(request.user)


class IsGrantedReadCreate(permissions.BasePermission):
    """ Custom permission for Read/Write resources like datasets.

    All authenticated users can see instances they have been allowed access to
    either because they own them, they are in users_allowed, or they are in
    groups_allowed.
    """
    def has_permission(self, request, view):
        return (admin_check(request.user) or
                request.method in permissions.SAFE_METHODS or
                request.method == "POST")

    def has_object_permission(self, request, view, obj):
        if admin_check(request.user):
            return True
        return obj.can_be_accessed(request.user)


class IsDeveloperOrGrantedReadOnly(IsGrantedReadOnly):
    """ Custom permission for developer resources like code
    
    Developers can create new instances, and all authenticated users can see
    instances they have been allowed access to either because they own them,
    they are in users_allowed, or they are in groups_allowed.
    """
    def has_permission(self, request, view):
        if admin_check(request.user):
            return True
        if request.method in permissions.SAFE_METHODS:
            return True
        return developer_check(request.user) and request.method in ("POST", "PATCH")


class GrantedModelMixin(object):
    """ Filter instances that the user has been granted access to.
    
    Mix this in with a view set to add a query parameter:
    
    * is_granted - true For administrators, this limits the list to only include
        records that the user has been explicitly granted access to. For other
        users, this has no effect.
    
    The model must derive from AccessControl, or filter_granted() must be
    overridden.
    """

    def get_queryset(self):
        if self.request.QUERY_PARAMS.get('is_granted') == 'true':
            is_admin = False
        else:
            is_admin = admin_check(self.request.user)
        base_queryset = super(GrantedModelMixin, self).get_queryset()
        if is_admin:
            return base_queryset
        return self.filter_granted(base_queryset)

    def filter_granted(self, queryset):
        """ Filter a queryset to only include records explicitly granted.
        """
        return AccessControl.filter_by_user(self.request.user,
                                            queryset=queryset)


class RedactModelMixin(object):
    """ Redacts a model instance and build a redaction plan.

    Mix this in with a view set to provide default behaviour for data redaction.
    This overrides the `partial_update` method so that it automatically redacts an
    object if it sees the `is_redacted` flag when PATCH'd. After that, the patch_object
    method is called, which you should override if you want to do any proper PATCH object
    updates.

    * patch_object() - override this on the super class, it should
        return a response containing the JSON representation of the patched
        object.
    * partial_update() - redacts the given instance, if the request's POST data contains
        is_redacted=false.
    * build_redaction_plan() - returns all instances that will be redacted when you
        patch the object with is_redacted=true. Returns a dict: {model_name: set(instance)}

    """
    def patch_object(self, request, pk=None):
        pass

    def partial_update(self, request, pk=None):
        is_redacted = request.data.get("is_redacted", "false") == "true"
        if is_redacted:
            try:
                self.get_object().redact()
            except RTPNotFinished as e:
                raise APIException(e.msg)
            return Response({'message': 'Object redacted.'})
        return self.patch_object(request, pk)

    @detail_route(methods=['get'])
    def redaction_plan(self, request, pk=None):
        redaction_plan = self.get_object().build_redaction_plan()
        return Response(summarize_redaction_plan(redaction_plan))


class RemoveModelMixin(mixins.DestroyModelMixin):
    """ Remove a model instance and build a removal plan.
    
    Mix this in with a view set to call remove() instead of destroy() on a
    DELETE command. The model must define the following methods:
    
    * remove() - deletes the given instance, as well as any instances of this
        and other models that reference it. Intended as a drastic clean up
        measure when sensitive data has been inappropriately added to Kive and
        all traces must be removed.
    * build_removal_plan() - returns all instances that will be removed when you
        call remove(). Returns a dict: {model_name: set(instance)}
    """

    @detail_route(methods=['get'], suffix='Removal Plan')
    def removal_plan(self, request, pk=None):
        removal_plan = self.get_object().build_removal_plan()
        return Response(summarize_redaction_plan(removal_plan))
    
    def perform_destroy(self, instance):
        try:
            instance.remove()
        except RTPNotFinished as e:
            raise APIException(e.msg)


class RemovableModelViewSet(RemoveModelMixin,
                            GrantedModelMixin,
                            ReadOnlyModelViewSet):
    """ The most common view set for developer models.
    
    For now, we only support GET and DELETE through the REST API. The DELETE
    command actually triggers the remove() method instead of destroy().
    """
    pass


class CleanCreateModelMixin(mixins.CreateModelMixin):
    """
    A mixin that adds POST support through the REST API.
    """
    @transaction.atomic
    def perform_create(self, serializer):
        """
        Handle creation and cleaning of a new object.
        """
        try:
            new_obj = serializer.save()
            new_obj.full_clean()
        except DjangoValidationError as ex:
            raise convert_validation(ex.messages)

class SearchableModelMixin(object):
    """
    Implements some boilerplate code common to ViewSets that allow filtering.
    """
    def apply_filters(self, queryset):
        # Parse the request to get all the applied filters, and refine the queryset.
        idx = 0
        while True:
            key = self.request.GET.get('filters[{}][key]'.format(idx))
            if key is None:
                break
            value = self.request.GET.get('filters[{}][val]'.format(idx), '')
            queryset = self._add_filter(queryset, key, value)
            idx += 1

        return queryset

    @staticmethod
    def _add_filter(queryset, key, value):
        """
        Filter the specified queryset by the specified key and value.
        """
        raise NotImplementedError("This must be overridden by the subclass")
