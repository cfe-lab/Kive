"""
portal.views
"""
from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader, RequestContext
from django.contrib.auth.decorators import login_required, user_passes_test

from constants import groups


def developer_check(user):
    return user.groups.filter(pk=groups.DEVELOPERS_PK).exists()


def admin_check(user):
    return user.groups.filter(pk=groups.ADMIN_PK).exists()


@login_required
def home(request):
    """
    Default homepage
    """
    user_is_developer = developer_check(request.user)
    if not user_is_developer and not request.user.is_staff and not request.user.is_superuser:
        return HttpResponseRedirect("/usr.html")

    t = loader.get_template('portal/index.html')
    c = RequestContext(request, {"is_developer": user_is_developer})
    return HttpResponse(t.render(c))


@login_required
@user_passes_test(developer_check)
def dev(request):
    """
    Developer portal
    """
    t = loader.get_template('portal/dev.html')
    c = RequestContext(request)
    return HttpResponse(t.render(c))


@login_required
def usr(request):
    """
    User portal
    """
    t = loader.get_template('portal/usr.html')
    c = RequestContext(request)
    return HttpResponse(t.render(c))
