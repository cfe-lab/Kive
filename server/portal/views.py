"""
portal.views
"""

from django.http import HttpResponse
from django.template import loader, Context

def home(request):
    """
    Default homepage
    """
    t = loader.get_template('portal/index.html')
    c = Context()
    return HttpResponse(t.render(c))

def dev(request):
    """
    Developer portal
    """
    t = loader.get_template('portal/dev.html')
    c = Context()
    return HttpResponse(t.render(c))

def usr(request):
    """
    User portal
    """
    t = loader.get_template('portal/usr.html')
    c = Context()
    return HttpResponse(t.render(c))
