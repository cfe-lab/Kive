"""
portal.views
"""

from django.core.context_processors import csrf
from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader, Context
from django.contrib.auth.decorators import login_required, user_passes_test

from constants import groups


def developer_check(user):
    return user.groups.filter(pk=groups.DEVELOPERS_PK).exists()


@login_required
def home(request):
    """
    Default homepage
    """
    if not developer_check(request.user):
        return HttpResponseRedirect("/usr.html")

    t = loader.get_template('portal/index.html')
    c = Context({"user": request.user})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


@login_required
@user_passes_test(developer_check)
def dev(request):
    """
    Developer portal
    """
    t = loader.get_template('portal/dev.html')
    c = Context({"user": request.user})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


@login_required
def usr(request):
    """
    User portal
    """
    t = loader.get_template('portal/usr.html')
    c = Context({"user": request.user})
    c.update(csrf(request))
    return HttpResponse(t.render(c))
