from django.conf.urls import patterns, url, include

from archive.ajax import DatasetViewSet, MethodOutputViewSet
from fleet.ajax import RunToProcessViewSet
from kive.kive_router import KiveRouter
from metadata.ajax import DatatypeViewSet, CompoundDatatypeViewSet
from method.ajax import MethodViewSet, MethodFamilyViewSet, CodeResourceViewSet, CodeResourceRevisionViewSet
from pipeline.ajax import PipelineFamilyViewSet, PipelineViewSet
import portal.ajax
from portal.forms import LoginForm

# (Un)comment the next two lines to enable/disable the admin:
from django.contrib import admin
admin.autodiscover()

router = KiveRouter()
router.register(r'coderesourcerevisions', CodeResourceRevisionViewSet)
router.register(r'coderesources', CodeResourceViewSet)
router.register(r'compounddatatypes', CompoundDatatypeViewSet)
router.register(r'datasets', DatasetViewSet)
router.register(r'datatypes', DatatypeViewSet)
router.register(r'methodfamilies', MethodFamilyViewSet)
router.register(r'methodoutputs', MethodOutputViewSet)
router.register(r'methods', MethodViewSet)
router.register(r'pipelinefamilies', PipelineFamilyViewSet)
router.register(r'pipelines', PipelineViewSet)
router.register(r'runs', RunToProcessViewSet)
router.register(r'stagedfiles', portal.ajax.StagedFileViewSet)

urlpatterns = patterns(
    '',
    # Examples:
    # url(r'^$', 'kive.views.home', name='home'),
    # url(r'^kive/', include('kive.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    url(r'^admin/', include(admin.site.urls)),
    url(r'^$', 'portal.views.home', name='home'),
    url(r'^login/$', 'django.contrib.auth.views.login',
        {"template_name": "portal/login.html",
         "authentication_form": LoginForm,
         "current_app": "portal"}, name='login'),
    url(r'^logout_then_login/$', 'django.contrib.auth.views.logout_then_login',
        {"current_app": "portal"}, name='logout'),

    url(r'^dev.html$', 'portal.views.dev', name='dev'),

    url(r'^datatypes$', 'metadata.views.datatypes', name='datatypes'),
    url(r'^datatypes/(?P<id>\d+)/$', 'metadata.views.datatype_detail', name='datatype_detail'),
    url(r'^datatype_add$', 'metadata.views.datatype_add', name='datatype_add'),
    url(r'^get_python_type/$', 'metadata.ajax.get_python_type', name='get_python_type'),

    url(r'^compound_datatypes$', 'metadata.views.compound_datatypes', name='compound_datatypes'),
    url(r'^compound_datatype_add$', 'metadata.views.compound_datatype_add', name='compound_datatype_add'),

    url(r'^resources$', 'method.views.resources', name='resources'),
    url(r'^resource_add$', 'method.views.resource_add', name='resource_add'),
    url(r'^resource_revisions/(?P<id>\d+)/$', 'method.views.resource_revisions', name='resource_revisions'),
    url(r'^resource_revision_add/(?P<id>\d+)/$', 'method.views.resource_revision_add', name='resource_revision_add'),
    url(r'^resource_revision_view/(?P<id>\d+)/$', 'method.views.resource_revision_view', name='resource_revision_view'),

    url(r'^get_revisions/$', 'method.ajax.populate_revision_dropdown', name='populate_revision_dropdown'),

    url(r'^method_families$', 'method.views.method_families', name='method_families'),
    #url(r'^method_family_add$', 'method.views.method_add', name='method_family_add'),
    url(r'^method_new$', 'method.views.method_new', name='method_new'),
    url(r'^methods/(?P<id>\d+)/$', 'method.views.methods', name='methods'),
    url(r'^method_add/(?P<id>\d+)/$', 'method.views.method_add', name='method_add'),
    url(r'^method_revise/(?P<id>\d+)/$', 'method.views.method_revise', name='method_revise'),

    url(r'^get_method_revisions/$', 'pipeline.ajax.populate_method_revision_dropdown', name='populate_method_revision_dropdown'),

    url(r'^pipeline_families$', 'pipeline.views.pipeline_families', name='pipeline_families'),
    url(r'^pipeline_new$', 'pipeline.views.pipeline_new', name='pipeline_new'),
    url(r'^pipelines/(?P<id>\d+)/$', 'pipeline.views.pipelines', name='pipelines'),
    url(r'^pipeline_add/(?P<id>\d+)/$', 'pipeline.views.pipeline_add', name='pipeline_add'),
    url(r'^pipeline_revise/(?P<id>\d+)$', 'pipeline.views.pipeline_revise', name='pipeline_revise'),

    url(r'^usr.html$', 'portal.views.usr', name='usr'),

    url(r'^datasets$', 'archive.views.datasets', name='datasets'),
    url(r'^dataset_download/(?P<dataset_id>\d+)$', 'archive.views.dataset_download', name='dataset_download'),
    url(r'^dataset_view/(?P<dataset_id>\d+)$', 'archive.views.dataset_view', name='dataset_view'),
    url(r'^stdout_download/(?P<methodoutput_id>\d+)$', 'archive.views.stdout_download', name='stdout_download'),
    url(r'^stdout_view/(?P<methodoutput_id>\d+)$', 'archive.views.stdout_view', name='stdout_view'),
    url(r'^stderr_download/(?P<methodoutput_id>\d+)$', 'archive.views.stderr_download', name='stderr_download'),
    url(r'^stderr_view/(?P<methodoutput_id>\d+)$', 'archive.views.stderr_view', name='stderr_view'),
    url(r'^datasets_add$', 'archive.views.datasets_add', name='datasets_add'),
    url(r'^datasets_add_bulk', 'archive.views.datasets_add_bulk', name='datasets_add_bulk'),
    url(r'^datasets_bulk', 'archive.views.datasets_bulk', name='datasets_bulk'),

    url(r'^datasets_add_archive$', 'archive.views.datasets_add_archive', name='datasets_add_archive'),

    url(r'^datasets_lookup/$', 'archive.views.dataset_lookup', name='dataset_lookup'),
    url(r'^datasets_lookup/(?P<md5_checksum>[0-9A-Fa-f]{32})$', 'archive.views.dataset_lookup', name='dataset_lookup'),
    url(r'^lookup$', 'archive.views.lookup', name='lookup'),

    url(r'^choose_pipeline$', 'sandbox.views.choose_pipeline', name='choose_pipeline'),
    url(r'^choose_inputs$', 'sandbox.views.choose_inputs', name='choose_inputs'),
    url(r'^runs$', 'sandbox.views.runs', name='runs'),
    url(r'^view_results/(?P<rtp_id>\d+)/$', 'sandbox.views.view_results', name='view_results'),
    url(r'^run_pipeline$', 'sandbox.views.run_pipeline', name='run_pipeline'),
    url(r'^view_run/(?P<rtp_id>\d+)$', 'sandbox.views.view_run', name='view_run'),
    url(r'^view_run/(?P<rtp_id>\d+)/(?P<md5>[0-9a-fA-F]{32})$', 'sandbox.views.view_run', name='view_run'),
    url(r'^filter_datasets$', 'sandbox.ajax.filter_datasets', name='filter_datasets'),
    url(r'^filter_pipelines$', 'sandbox.ajax.filter_pipelines', name='filter_pipelines'),
    url(r'^get_failed_output$', 'sandbox.ajax.get_failed_output', name='get_failed_output'),

    # Urls for django-rest-framework
    url(r'^api/', include(router.urls), name='api_home'),
    url(r'^api/auth/$', 'portal.views.api_auth', name='api_auth'),
)
