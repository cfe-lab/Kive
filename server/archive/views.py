# Create your views here.
from django.http import HttpResponse, HttpResponseRedirect
from copperfish import models
from django.template import loader, Context
from copperfish.models import BasicConstraint, CodeResource
from copperfish.forms import *
#from django.shortcuts import render, render_to_response
from django.core.context_processors import csrf
from django.core.exceptions import ValidationError
from django.forms.util import ErrorList
from datetime import datetime
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile


def home(request):
    """
    Default homepage
    """
    t = loader.get_template('index.html')
    c = Context()
    return HttpResponse(t.render(c))

def dev(request):
    """
    Developer portal
    """
    t = loader.get_template('dev.html')
    c = Context()
    return HttpResponse(t.render(c))

def datatypes(request):
    """
    Render table and form on user request for datatypes.html
    """
    datatypes = models.Datatype.objects.all()
    t = loader.get_template('datatypes.html')
    c = Context({'datatypes': datatypes})
    c.update(csrf(request))
    return HttpResponse(t.render(c))
    #return render_to_response('datatypes.html', {'form': form})


def datatype_add(request):
    """
    Render form for creating a new Datatype
    """
    exceptions = []
    
    if request.method == 'POST':
        dform = DatatypeForm(request.POST) # create form bound to POST data
        query = request.POST.dict()
        
        if dform.is_valid():
            new_datatype = dform.save() # this has to be saved to database to be passed to BasicConstraint()
            minlen, maxlen, regexp, minval, maxval = None, None, None, None, None
            
            if new_datatype.Python_type == 'str':
                # manually create and validate BasicConstraint objects    
                if query['minlen']:
                    minlen = BasicConstraint(datatype=new_datatype, ruletype='minlen', rule=query['minlen'])
                    try:
                        minlen.full_clean()
                    except ValidationError as e:
                        exceptions.extend(e.messages)
                        pass
                
                if query['maxlen']:
                    maxlen = BasicConstraint(datatype=new_datatype, ruletype='maxlen', rule=query['maxlen'])
                    try:
                        maxlen.full_clean()
                    except ValidationError as e:
                        exceptions.extend(e.messages)
                        pass
                
                if query['regexp']:
                    regexp = BasicConstraint(datatype=new_datatype, ruletype='regexp', rule=query['regexp'])
                    try:
                        regexp.full_clean()
                    except ValidationError as e:
                        exceptions.extend(e.messages)
                        pass
            
            elif new_datatype.Python_type in ['int', 'float']:
                if query['minval']:
                    minval = BasicConstraint(datatype=new_datatype, ruletype='minval', rule=query['minval'])
                    try:
                        minval.full_clean()
                    except ValidationError as e:
                        exceptions.extend(e.messages)
                        pass
                
                if query['maxval']:
                    maxval = BasicConstraint(datatype=new_datatype, ruletype='maxval', rule=query['maxval'])
                    try:
                        maxval.full_clean()
                    except ValidationError as e:
                        exceptions.extend(e.messages)
                        pass

            if exceptions:
                new_datatype.delete() # delete object from database
            else:
                # save basic constraint objects if they are defined
                if minlen: minlen.save()
                if maxlen: maxlen.save()
                if regexp: regexp.save()
                if minval: minval.save()
                if maxval: maxval.save()
                
                # re-check Datatype object
                new_datatype.full_clean()
                new_datatype.save()    
                return HttpResponseRedirect('/datatypes')
                
        
        # populate forms for display (one or more invalid forms)
        icform = IntegerConstraintForm({'minval': query.get('minval', None), 
            'maxval': query.get('maxval', None)})
        if exceptions:
            icform.errors['Basic Constraint Errors'] = ErrorList(exceptions)
        
        scform = StringConstraintForm({'minlen': query.get('minlen', None), 
            'maxlen': query.get('maxlen', None),
            'regexp': query.get('regexp', None)})
            
    else:
        dform = DatatypeForm() # unbound
        #cform = BasicConstraintForm()
        icform = IntegerConstraintForm()
        scform = StringConstraintForm()
    
    t = loader.get_template('datatype_add.html')
    c = Context({'datatype_form': dform,
                'int_con_form': icform,
                'str_con_form': scform})
    c.update(csrf(request))

    return HttpResponse(t.render(c))




def datatype_detail(request, id):
    # retrieve the Datatype object from database by PK
    this_datatype = models.Datatype.objects.get(pk=id)
    t = loader.get_template('datatype_detail.html')
    c = Context({'datatype': this_datatype, 
                'constraints': this_datatype.basic_constraints.all()})
    c.update(csrf(request))
    return HttpResponse(t.render(c))



def resources(request):
    """
    Display a list of all code resources (parents) in database
    """
    resources = models.CodeResource.objects.filter()

    t = loader.get_template('resources.html')
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

    t = loader.get_template('resource_add.html')

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

    t = loader.get_template('resource_add.html')
    c = Context({'resource_form': form, 'dependency_form': dep_form})
    c.update(csrf(request))
    
    return HttpResponse(t.render(c))



def resource_revise(request, id):
    """
    Revise a code resource.  The form will initially be populated with values of the last
    revision to this code resource.
    """
    t = loader.get_template('resource_revise.html')
    this_code_resource = models.CodeResource.objects.get(pk=id)
    all_revisions = models.CodeResourceRevision.objects.filter(coderesource=this_code_resource).order_by('-revision_DateTime')
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







def usr(request):
    """
    User portal
    """
    t = loader.get_template('usr.html')
    c = Context()
    return HttpResponse(t.render(c))
