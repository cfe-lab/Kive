from django.conf.urls import include
import django.contrib.auth.views
from django.urls import re_path

from container.ajax import ContainerFamilyViewSet, ContainerViewSet, ContainerAppViewSet, ContainerChoiceViewSet, \
    ContainerRunViewSet, BatchViewSet, ContainerArgumentViewSet, ContainerLogViewSet
from librarian.ajax import DatasetViewSet, ExternalFileDirectoryViewSet
from kive.kive_router import KiveRouter
from portal.ajax import UserViewSet
from portal.forms import LoginForm

import portal.views
import container.views
import librarian.views

# (Un)comment the next two lines to enable/disable the admin:
from django.contrib import admin
admin.autodiscover()

router = KiveRouter()
router.register(r'batches', BatchViewSet)
# ContainerChoice before ContainerFamily, so ContainerFamily gets used for all
# URL's.
router.register(r'containerchoices', ContainerChoiceViewSet)
router.register(r'containerfamilies', ContainerFamilyViewSet)
router.register(r'containers', ContainerViewSet)
router.register(r'containerapps', ContainerAppViewSet)
router.register(r'containerargs', ContainerArgumentViewSet)
router.register(r'containerruns', ContainerRunViewSet)
router.register(r'containerlogs', ContainerLogViewSet)
router.register(r'datasets', DatasetViewSet)
router.register(r'externalfiledirectories', ExternalFileDirectoryViewSet)
router.register(r'users', UserViewSet)

urlpatterns = [
    # '',
    # Examples:
    # re_path(r'^$', 'kive.views.home', name='home'),
    # re_path(r'^kive/', include('kive.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # re_path(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    # deprecated way : url(r'^admin/', include(admin.site.urls)),
    # new way for Django 2.0 going forward
    re_path(r'^admin/', admin.site.urls),
    re_path(r'^$', portal.views.home, name='home'),
    re_path(r'^login/$',
            django.contrib.auth.views.LoginView.as_view(
                template_name="portal/login.html",
                authentication_form=LoginForm),
            name='login'),
    re_path(r'^logout_then_login/$',
            django.contrib.auth.views.logout_then_login,
            name='logout'),

    re_path(r'^dev.html$', portal.views.dev, name='dev'),
    re_path(r'^usr.html$', portal.views.usr, name='usr'),

    re_path(r'^batch_update/(?P<pk>\d+)/$',
            container.views.BatchUpdate.as_view(),
            name='batch_update'),
    re_path(r'^container_families$',
            container.views.ContainerFamilyList.as_view(),
            name='container_families'),
    re_path(r'^container_family_add$',
            container.views.ContainerFamilyCreate.as_view(),
            name='container_family_add'),
    re_path(r'^container_family_update/(?P<pk>\d+)/$',
            container.views.ContainerFamilyUpdate.as_view(),
            name='container_family_update'),

    re_path(r'^container_family_update/(?P<family_id>\d+)/container_add$',
            container.views.ContainerCreate.as_view(),
            name='container_add'),
    re_path(r'^container_update/(?P<pk>\d+)/$',
            container.views.ContainerUpdate.as_view(),
            name='container_update'),
    re_path(r'^container_update/(?P<pk>\d+)/content',
            container.views.ContainerContentUpdate.as_view(),
            name='container_content_update'),

    re_path(r'^container_update/(?P<container_id>\d+)/app_add$',
            container.views.ContainerAppCreate.as_view(),
            name='container_app_add'),
    re_path(r'^container_app_update/(?P<pk>\d+)/$',
            container.views.ContainerAppUpdate.as_view(),
            name='container_app_update'),

    re_path(r'^container_choices$',
            container.views.ContainerChoiceList.as_view(),
            name='container_choices'),
    re_path(r'^container_inputs$',
            container.views.ContainerInputList.as_view(),
            name='container_inputs'),
    re_path(r'^container_runs$',
            container.views.ContainerRunList.as_view(),
            name='container_runs'),
    re_path(r'^container_runs/(?P<pk>\d+)/$',
            container.views.ContainerRunUpdate.as_view(),
            name='container_run_detail'),
    re_path(r'^container_logs/(?P<pk>\d+)/$',
            container.views.ContainerLogDetail.as_view(),
            name='container_log_detail'),

    re_path(r'^datasets$', librarian.views.datasets, name='datasets'),
    re_path(r'^dataset_download/(?P<dataset_id>\d+)$',
            librarian.views.dataset_download,
            name='dataset_download'),
    re_path(r'^dataset_view/(?P<dataset_id>\d+)$',
            librarian.views.dataset_view,
            name='dataset_view'),
    re_path(r'^datasets_add_bulk',
            librarian.views.datasets_add_bulk,
            name='datasets_add_bulk'),
    re_path(r'^datasets_bulk',
            librarian.views.datasets_bulk,
            name='datasets_bulk'),
    re_path(r'^datasets_add_archive$',
            librarian.views.datasets_add_archive,
            name='datasets_add_archive'),

    re_path(r'^datasets_lookup/$',
            librarian.views.dataset_lookup,
            name='dataset_lookup'),
    re_path(r'^datasets_lookup/(?P<filename>.{0,50})/(?P<filesize>\d+)/'
            r'(?P<md5_checksum>[0-9A-Fa-f]{32})$',
            librarian.views.dataset_lookup,
            name='dataset_lookup'),
    re_path(r'^lookup$', librarian.views.lookup, name='lookup'),

    # Urls for django-rest-framework
    re_path(r'^api/', include(router.urls), name='api_home'),
]
