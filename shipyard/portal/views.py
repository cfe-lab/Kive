"""
portal.views
"""

from django.core.context_processors import csrf
from django.http import HttpResponse
from django.template import loader, Context
from django.contrib.auth.decorators import login_required


@login_required
def home(request):
    """
    Default homepage
    """
    t = loader.get_template('portal/index.html')
    c = Context({"user": request.user})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


@login_required
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
