from collections import OrderedDict

from django.urls import NoReverseMatch
from rest_framework import routers, views
from rest_framework.response import Response
from rest_framework.reverse import reverse
from rest_framework.routers import Route
import itertools
from django.core.exceptions import ImproperlyConfigured


class KiveRouter(routers.DefaultRouter):
    def get_api_root_view(self, api_urls=None):
        """
        Return a view to use as the API root.

        api_urls is a dummy parameter that's included because the parent method
        includes it.
        """
        known_actions = [route.mapping.values()
                         for route in self.routes if isinstance(route, Route)]
        known_actions = set(itertools.chain(*known_actions))

        api_root_dict = OrderedDict()
        list_name = self.routes[0].name
        for prefix, viewset, basename in self.registry:
            api_root_dict[prefix] = list_name.format(basename=basename)
            for methodname in dir(viewset):
                attr = getattr(viewset, methodname)
                detail = getattr(attr, 'detail', True)
                if hasattr(attr, 'mapping'):
                    if methodname in known_actions:
                        raise ImproperlyConfigured(
                            'Cannot use @detail_route or @list_route '
                            'decorators on method "%s" '
                            'as it is an existing route' % methodname)
                    if not detail:
                        custom_list_name = '{}-{}'.format(prefix, methodname)
                        url_name = '{}-{}'.format(basename, methodname)
                        api_root_dict[custom_list_name] = url_name

        class APIRoot(views.APIView):
            _ignore_model_permissions = True

            def get(self, request, *args, **kwargs):
                ret = OrderedDict()
                namespace = request.resolver_match.namespace
                for key, url_name in api_root_dict.items():
                    if namespace:
                        url_name = namespace + ':' + url_name
                    try:
                        ret[key] = reverse(
                            url_name,
                            args=args,
                            kwargs=kwargs,
                            request=request,
                            format=kwargs.get('format', None)
                        ) + '?page_size=25'
                    except NoReverseMatch:
                        # Don't bail out if eg. no list routes exist, only detail routes.
                        continue

                return Response(ret)

        return APIRoot.as_view()
