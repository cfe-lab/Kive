"""
portal.views
"""
from rest_framework.authentication import SessionAuthentication, BasicAuthentication, TokenAuthentication
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader, RequestContext
from django.contrib.auth.decorators import login_required, user_passes_test
from django.core.urlresolvers import reverse

from constants import groups


def developer_check(user):
    return user.groups.filter(pk=groups.DEVELOPERS_PK).exists()


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


@api_view(['GET'])
@authentication_classes((SessionAuthentication, BasicAuthentication, TokenAuthentication))
@permission_classes((IsAuthenticated,))
def api_home(request):
    """
    Presents a user with the list of actions they can perform with
    their permissions. At this point
    """
    home_dir = {
        'user': {
            'id': request.user.id,
            'username': request.user.username
        },
        'directory': {
            name: reverse(name) for name in ['api_dataset_home']
        }
    }
    return Response(home_dir)
