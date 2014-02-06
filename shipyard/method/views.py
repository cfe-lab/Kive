"""
method.views
"""

from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader, Context
from method.models import CodeResource, CodeResourceRevision, Method
from method.forms import *
#from django.shortcuts import render, render_to_response
from django.core.context_processors import csrf
from django.core.exceptions import ValidationError
from django.forms.util import ErrorList
from datetime import datetime
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile


def resources(request):
    """
    Display a list of all code resources (parents) in database
    """
    resources = CodeResource.objects.filter()

    t = loader.get_template('method/resources.html')
    c = Context({'resources': resources})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


def resource_add(request):
    """
    Add a new code resource with a prototype (no revisions).  The FILENAME of the prototype will
    be used as the symbolic filename for all subsequent revisions of this code resource.
    The actual filename will be suffixed with date and time when saved to the filesystem.
    On execution, Shipyard will refer to a revision's CodeResource to get the original filename and
    copy the revision file over to the sandbox.
    NAME provides an opportunity to provide a more intuitive and user-accessible name.
    """
    t = loader.get_template('method/resource_add.html')

    if request.method == 'POST':
        query = request.POST.dict()

        # validate name and description entries (return error if blank)
        min_form = CodeResourceMinimalForm(request.POST)
        if not min_form.is_valid():
            # create unbound form
            form = CodeResourcePrototypeForm(request.POST)
            form._errors = min_form.errors
            dep_form = CodeResourceDependencyForm()
            c = Context({'resource_form': form, 'dependency_form': dep_form})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

        try:
            file_in_memory = request.FILES['content_file']
        except:
            # no file specified
            form = CodeResourcePrototypeForm(request.POST)
            form._errors = min_form.errors
            form._errors['content_file'] = ErrorList([u'You must specify a file upload.'])
            dep_form = CodeResourceDependencyForm()
            c = Context({'resource_form': form, 'dependency_form': dep_form})
            c.update(csrf(request))
            return HttpResponse(t.render(c))


        # create new CodeResource
        new_code_resource = CodeResource.objects.create(name=query['revision_name'],
                                                        description=query['revision_desc'],
                                                        filename=file_in_memory.name)

        # modify actual filename prior to saving revision object
        file_in_memory.name += '_' + datetime.now().strftime('%Y%m%d%H%M%S')

        prototype = CodeResourceRevision(revision_name=query['revision_name'],
                                         revision_desc=query['revision_desc'],
                                         coderesource=new_code_resource,
                                         content_file=file_in_memory)
        try:
            prototype.full_clean()
            prototype.save()

            # now parse dependencies if any exist
            to_save = []
            exceptions = []
            for k in query.iterkeys():
                if not k.startswith('revisions'):
                    continue

                # see if contents of this form result in a valid CodeResourceDependency
                rev_id = query[k]
                if rev_id == '':
                    # ignore incomplete or unused dependency forms
                    print 'ignoring incomplete CodeResourceDependencyForm'
                    continue

                suffix = ('_' + k.split('_')[-1]) if '_' in k else ''
                on_revision = CodeResourceRevision.objects.get(pk=rev_id)
                dependency = CodeResourceDependency(coderesourcerevision=prototype,
                                                    requirement=on_revision,
                                                    depPath=query['depPath'+suffix],
                                                    depFileName=query['depFileName'+suffix])
                try:
                    dependency.full_clean()
                except ValidationError as e:
                    exceptions.extend(e.messages)
                    pass

                to_save.append(dependency)

            # only save CR dependencies if they all check out
            if exceptions:
                prototype.delete()
                raise # delete code resource
            else:
                for dependency in to_save:
                    dependency.save()

            return HttpResponseRedirect('/resources')
        except:
            new_code_resource.delete()
            raise

        # return form with last (non-valid) entries
        form = CodeResourcePrototypeForm(request.POST, request.FILES)
        dep_form = CodeResourceDependencyForm(request.POST)

    else:
        form = CodeResourcePrototypeForm()
        dep_form = CodeResourceDependencyForm()

    t = loader.get_template('method/resource_add.html')
    c = Context({'resource_form': form, 'dependency_form': dep_form})
    c.update(csrf(request))
    
    return HttpResponse(t.render(c))



def resource_revise(request, id):
    """
    Revise a code resource.  The form will initially be populated with values of the last
    revision to this code resource.
    """
    t = loader.get_template('method/resource_revise.html')
    this_code_resource = CodeResource.objects.get(pk=id)
    all_revisions = CodeResourceRevision.objects.filter(coderesource=this_code_resource).order_by('-revision_DateTime')
    last_revision = all_revisions[0]

    if request.method == 'POST':
        query = request.POST.dict()

        # validate name and description entries (return error if blank)
        min_form = CodeResourceMinimalForm(request.POST)
        if not min_form.is_valid():
            # create unbound form
            form = CodeResourceRevisionForm(request.POST)
            form._errors = min_form.errors
            dep_form = CodeResourceDependencyForm()
            c = Context({'resource_form': form, 'dependency_form': dep_form})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

        try:
            file_in_memory = request.FILES['content_file']
        except:
            # no file specified
            form = CodeResourceRevisionForm(request.POST)
            form._errors = min_form.errors
            form._errors['content_file'] = ErrorList([u'You must specify a file upload.'])
            dep_form = CodeResourceDependencyForm()
            c = Context({'resource_form': form, 'dependency_form': dep_form})
            c.update(csrf(request))
            return HttpResponse(t.render(c))

        # modify actual filename prior to saving revision object
        file_in_memory.name += '_' + datetime.now().strftime('%Y%m%d%H%M%S')

        revision = CodeResourceRevision(revision_name=query['revision_name'],
                                        revision_desc=query['revision_desc'],
                                        coderesource=this_code_resource,
                                        content_file=file_in_memory)
        try:
            revision.full_clean()
            revision.save()
            return HttpResponseRedirect('/resources')
        except:
            raise
        # otherwise return form with user entries
        form = CodeResourceRevisionForm(request.POST, request.FILES)
    else:
        if last_revision:
            form = CodeResourceRevisionForm(initial={'revision_desc': last_revision.revision_desc,
                                                     'revision_name': last_revision.revision_name})
            dep_form = CodeResourceDependencyForm(request.POST)
        else:
            form = CodeResourceRevisionForm()
            dep_form = CodeResourceDependencyForm()

    c = Context({'resource_form': form, 'coderesource': this_code_resource, 'dependency_form': dep_form})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


def methods(request):
    """
    Display a list of all Methods in database.
    """
    methods = Method.objects.all()
    t = loader.get_template('method/methods.html')
    c = Context({'methods': methods})
    c.update(csrf(request))
    return HttpResponse(t.render(c))


def methods_add (request):
    if request.method == 'POST':
        pass
    else:
