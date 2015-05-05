from rest_framework import permissions
from portal.views import developer_check, admin_check

class IsDeveloperOrGrantedReadOnly(permissions.BasePermission):
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
        return developer_check(request.user) and request.method == 'POST'
    
    def has_object_permission(self, request, view, obj):
        if admin_check(request.user):
            return True
        return obj.can_be_accessed(request.user)