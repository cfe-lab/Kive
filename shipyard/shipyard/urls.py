from django.conf.urls import patterns, include, url
from django import forms
from portal.forms import *
from metadata.forms import *
from method.forms import *

#from copperfish.preview import *

# Uncomment the next two lines to enable the admin:
#from django.contrib import admin
#admin.autodiscover()

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'shipyard.views.home', name='home'),
    # url(r'^shipyard/', include('shipyard.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    # url(r'^admin/', include(admin.site.urls)),
    url(r'^$', 'portal.views.home', name='home'),

    url(r'^dev.html$', 'portal.views.dev', name='dev'),

    url(r'^datatypes$', 'metadata.views.datatypes', name='datatypes'),
    url(r'^datatypes/(?P<id>\d+)/$', 'metadata.views.datatype_detail', name='datatype_detail'),
    url(r'^datatype_add$', 'metadata.views.datatype_add', name='datatype_add'),

    url(r'compound_datatypes$', 'metadata.views.compound_datatypes', name='compound_datatypes'),
    url(r'compound_datatype_add$', 'metadata.views.compound_datatype_add', name='compound_datatype_add'),

    url(r'resources$', 'method.views.resources', name='resources'),
    url(r'resource_add$', 'method.views.resource_add', name='resource_add'),
    url(r'^resource_revisions/(?P<id>\d+)/$', 'method.views.resource_revisions', name='resource_revisions'),
    url(r'^resource_revision_add/(?P<id>\d+)/$', 'method.views.resource_revision_add', name='resource_revision_add'),

    url(r'get_revisions/$', 'method.ajax.populate_revision_dropdown', name='populate_revision_dropdown'),

    url(r'methods$', 'method.views.methods', name='methods'),
    url(r'method_add$', 'method.views.method_add', name='method_add'),
    url(r'^methods/(?P<id>\d+)/$', 'method.views.method_revise', name='method_revise'),

    url(r'get_method_revisions/$', 'pipeline.ajax.populate_method_revision_dropdown', name='populate_method_revision_dropdown'),
    url(r'get_method_io/$', 'pipeline.ajax.get_method_io', name='get_method_io'),

    url(r'pipelines$', 'pipeline.views.pipelines', name='pipelines'),

    url(r'^usr.html$', 'portal.views.usr', name='usr'),

    url(r'^datasets', 'archive.views.datasets', name='datasets'),
)
