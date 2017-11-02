from django.conf.urls import url, include
import django.contrib.auth.views

from archive.ajax import MethodOutputViewSet, RunViewSet, RunBatchViewSet
from librarian.ajax import DatasetViewSet, ExternalFileDirectoryViewSet
from kive.kive_router import KiveRouter
from metadata.ajax import DatatypeViewSet, CompoundDatatypeViewSet
from method.ajax import MethodViewSet, MethodFamilyViewSet, CodeResourceViewSet, CodeResourceRevisionViewSet, \
    DockerImageViewSet
from pipeline.ajax import PipelineFamilyViewSet, PipelineViewSet
from portal.ajax import UserViewSet
from portal.forms import LoginForm

import portal.views
import metadata.views
import method.views
import pipeline.views
import librarian.views
import archive.views
import sandbox.views

# (Un)comment the next two lines to enable/disable the admin:
from django.contrib import admin
admin.autodiscover()

router = KiveRouter()
router.register(r'coderesourcerevisions', CodeResourceRevisionViewSet)
router.register(r'coderesources', CodeResourceViewSet)
router.register(r'compounddatatypes', CompoundDatatypeViewSet)
router.register(r'dockerimages', DockerImageViewSet)
router.register(r'datasets', DatasetViewSet)
router.register(r'externalfiledirectories', ExternalFileDirectoryViewSet)
router.register(r'datatypes', DatatypeViewSet)
router.register(r'methodfamilies', MethodFamilyViewSet)
router.register(r'methodoutputs', MethodOutputViewSet)
router.register(r'methods', MethodViewSet)
router.register(r'pipelines', PipelineViewSet)
router.register(r'pipelinefamilies', PipelineFamilyViewSet)
router.register(r'runs', RunViewSet)
router.register(r'runbatches', RunBatchViewSet)
router.register(r'users', UserViewSet)

urlpatterns = [
    # '',
    # Examples:
    # url(r'^$', 'kive.views.home', name='home'),
    # url(r'^kive/', include('kive.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    url(r'^admin/', include(admin.site.urls)),
    url(r'^$', portal.views.home, name='home'),
    url(r'^login/$', django.contrib.auth.views.login,
        {"template_name": "portal/login.html",
         "authentication_form": LoginForm,
         "current_app": "portal"}, name='login'),
    url(r'^logout_then_login/$', django.contrib.auth.views.logout_then_login,
        {"current_app": "portal"}, name='logout'),

    url(r'^dev.html$', portal.views.dev, name='dev'),
    url(r'^usr.html$', portal.views.usr, name='usr'),

    url(r'^datatypes$', metadata.views.datatypes, name='datatypes'),
    url(r'^datatypes/(?P<id>\d+)/$', metadata.views.datatype_detail, name='datatype_detail'),
    url(r'^datatype_add$', metadata.views.datatype_add, name='datatype_add'),

    url(r'^compound_datatypes$', metadata.views.compound_datatypes, name='compound_datatypes'),
    url(r'^compound_datatypes/(?P<id>\d+)/$', metadata.views.compound_datatype_detail,
        name='compound_datatype_detail'),
    url(r'^compound_datatype_add$', metadata.views.compound_datatype_add, name='compound_datatype_add'),

    url(r'^resources$', method.views.resources, name='resources'),
    url(r'^resource_add$', method.views.resource_add, name='resource_add'),
    url(r'^resource_revisions/(?P<pk>\d+)/$', method.views.resource_revisions, name='resource_revisions'),
    url(r'^resource_revision_add/(?P<pk>\d+)/$', method.views.resource_revision_add, name='resource_revision_add'),
    url(r'^resource_revision_view/(?P<pk>\d+)/$', method.views.resource_revision_view, name='resource_revision_view'),

    url(r'^docker_images$', method.views.docker_images, name='docker_images'),
    url(r'^docker_image_add$', method.views.docker_image_add, name='docker_image_add'),
    url(r'^docker_image_view/(?P<image_id>\d+)/$', method.views.docker_image_view, name='docker_image_view'),

    url(r'^method_families$', method.views.method_families, name='method_families'),
    url(r'^method_new$', method.views.method_new, name='method_new'),
    url(r'^methods/(?P<pk>\d+)/$', method.views.methods, name='methods'),
    url(r'^method_add/(?P<pk>\d+)/$', method.views.method_add, name='method_add'),
    url(r'^method_view/(?P<pk>\d+)/$', method.views.method_view, name='method_view'),
    url(r'^method_revise/(?P<pk>\d+)/$', method.views.method_revise, name='method_revise'),

    url(r'^pipeline_families$', pipeline.views.pipeline_families, name='pipeline_families'),
    url(r'^pipeline_new$', pipeline.views.pipeline_new, name='pipeline_new'),
    url(r'^pipelines/(?P<id>\d+)/$', pipeline.views.pipelines, name='pipelines'),
    url(r'^pipeline_add/(?P<id>\d+)/$', pipeline.views.pipeline_add, name='pipeline_add'),
    url(r'^pipeline_view/(?P<id>\d+)/$', pipeline.views.pipeline_view, name='pipeline_view'),
    url(r'^pipeline_revise/(?P<id>\d+)$', pipeline.views.pipeline_revise, name='pipeline_revise'),

    url(r'^datasets$', librarian.views.datasets, name='datasets'),
    url(r'^dataset_download/(?P<dataset_id>\d+)$', librarian.views.dataset_download, name='dataset_download'),
    url(r'^dataset_view/(?P<dataset_id>\d+)$', librarian.views.dataset_view, name='dataset_view'),
    url(r'^datasets_add_bulk', librarian.views.datasets_add_bulk, name='datasets_add_bulk'),
    url(r'^datasets_bulk', librarian.views.datasets_bulk, name='datasets_bulk'),
    url(r'^datasets_add_archive$', librarian.views.datasets_add_archive, name='datasets_add_archive'),

    url(r'^datasets_lookup/$', librarian.views.dataset_lookup, name='dataset_lookup'),
    url(r'^datasets_lookup/(?P<filename>.{0,50})/(?P<filesize>\d+)/(?P<md5_checksum>[0-9A-Fa-f]{32})$',
        librarian.views.dataset_lookup,
        name='dataset_lookup'),
    url(r'^lookup$', librarian.views.lookup, name='lookup'),

    url(r'^stdout_download/(?P<methodoutput_id>\d+)$', archive.views.stdout_download, name='stdout_download'),
    url(r'^stdout_view/(?P<methodoutput_id>\d+)$', archive.views.stdout_view, name='stdout_view'),
    url(r'^stderr_download/(?P<methodoutput_id>\d+)$', archive.views.stderr_download, name='stderr_download'),
    url(r'^stderr_view/(?P<methodoutput_id>\d+)$', archive.views.stderr_view, name='stderr_view'),
    url(r'^choose_pipeline$', sandbox.views.choose_pipeline, name='choose_pipeline'),
    url(r'^choose_inputs/$', sandbox.views.choose_inputs, name='choose_inputs'),
    url(r'^runs$', sandbox.views.runs, name='runs'),
    url(r'^view_results/(?P<run_id>\d+)/$', sandbox.views.view_results, name='view_results'),
    url(r'^view_run/(?P<run_id>\d+)$', sandbox.views.view_run, name='view_run'),
    url(r'^view_run/(?P<run_id>\d+)/(?P<md5>[0-9a-fA-F]{32})$', sandbox.views.view_run, name='view_run'),
    url(r'^runbatch/(?P<runbatch_pk>\d+)$', sandbox.views.runbatch, name='runbatch'),

    # Urls for django-rest-framework
    url(r'^api/', include(router.urls), name='api_home'),
]
