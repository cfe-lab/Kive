"""
portal.views
"""
from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader
from django.contrib.auth.decorators import login_required, user_passes_test
from django.views.generic.base import ContextMixin

from constants import groups


def developer_check(user):
    return user.groups.filter(pk=groups.DEVELOPERS_PK).exists()


def admin_check(user):
    return user.is_staff


class AdminViewMixin(ContextMixin):
    def get_context_data(self, **kwargs):
        context = super(AdminViewMixin, self).get_context_data(**kwargs)
        # noinspection PyUnresolvedReferences
        request_user = self.request.user
        context['is_user_admin'] = admin_check(request_user)
        object = context.get('object')
        owner = getattr(object, 'user', None)
        context['is_owner'] = owner == request_user
        return context


@login_required
def home(request):
    """
    Default homepage
    """
    user_is_developer = developer_check(request.user)
    if not user_is_developer and not request.user.is_staff and not request.user.is_superuser:
        return HttpResponseRedirect("/usr.html")

    t = loader.get_template('portal/index.html')
    c = {"is_developer": user_is_developer}
    return HttpResponse(t.render(c, request))


@login_required
@user_passes_test(developer_check)
def dev(request):
    """
    Developer portal
    """
    t = loader.get_template('portal/dev.html')
    return HttpResponse(t.render({}, request))


@login_required
def usr(request):
    """
    User portal
    """
    t = loader.get_template('portal/usr.html')
    return HttpResponse(t.render({}, request))
