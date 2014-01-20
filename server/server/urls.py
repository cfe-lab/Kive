from django.conf.urls import patterns, include, url
from django import forms
from copperfish.forms import *
#from copperfish.preview import *

# Uncomment the next two lines to enable the admin:
#from django.contrib import admin
#admin.autodiscover()

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'server.views.home', name='home'),
    # url(r'^server/', include('server.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    # url(r'^admin/', include(admin.site.urls)),
    url(r'^$', 'copperfish.views.home', name='home'),
    url(r'^dev.html$', 'copperfish.views.dev', name='dev'),
    url(r'^datatypes$', 'copperfish.views.datatypes', name='datatypes'),
    url(r'^datatypes/(?P<id>\d+)/$', 'copperfish.views.datatype_detail', name='datatype_detail'),
    url(r'datatype_add$', 'copperfish.views.datatype_add', name='datatype_add'),
    #url(r'datatype_add$', DatatypeFormPreview(DatatypeForm)),
    #url(r'^datatype_add/(?P<id>\d+)_str$', StringBasicConstraintFormPreview(StringBasicConstraintForm)),
    #url(r'^datatype_add/(?P<id>\d+)_int$', IntegerBasicConstraintFormPreview(IntegerBasicConstraintForm)),
    #url(r'^datatype_finish/(?P<id>\d+)_(?P<Python_type>\w+)$', 'copperfish.views.datatype_add', name='datatype_add'),
    url(r'resources$', 'copperfish.views.resources', name='resources'),
    url(r'resource_add$', 'copperfish.views.resource_add', name='resource_add'),
    url(r'^resources/(?P<id>\d+)/$', 'copperfish.views.resource_add_revision', name='resource_add_revision'),

    url(r'get_revisions/$', 'copperfish.ajax.populate_revision_dropdown', name='populate_revision_dropdown'),

    url(r'^usr.html$', 'copperfish.views.usr', name='usr'),
    url(r'^datasets', 'copperfish.views.datasets', name='datasets'),
)
