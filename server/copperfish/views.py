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

    if request.method == 'POST':
        query = request.POST.dict()
        #files = request.FILES.dict()
        file_in_memory = request.FILES['content_file']
        content_file = ContentFile(file_in_memory.read())

        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        path = default_storage.save('CodeResources/%s_%s' % (file_in_memory.name, timestamp), content_file)

        """
        with open('CodeResources/%s_%s' % (content_file.name, timestamp), 'wb+') as destination:
            for chunk in content_file.chunks:
                destination.write(chunk)
        """
        new_code_resource = CodeResource.objects.create(name=query['revision_name'],
                                                        description=query['revision_desc'],
                                                        filename=file_in_memory.name)
        #cr = CodeResource(name=query['revision_name'], description=query['revision_desc'], filename=content_file.name)
        #cr.full_clean()
        #cr.save()

        #form = CodeResourcePrototypeForm(request.POST, request.FILES) # create form bound to POST data

        prototype = CodeResourceRevision(revision_name=query['revision_name'],
                                         revision_desc=query['revision_desc'],
                                         coderesource=new_code_resource,
                                         content_file=file_in_memory)
        try:
            prototype.full_clean()
            prototype.save()
            return HttpResponseRedirect('/resources')
        except:
            new_code_resource.delete()
            raise

        form = CodeResourcePrototypeForm(request.POST, request.FILES)

    else:
        form = CodeResourcePrototypeForm()

    t = loader.get_template('resource_add.html')
    c = Context({'resource_form': form})
    c.update(csrf(request))
    
    return HttpResponse(t.render(c))


def resource_add_revision(request, id):
    if request.method == 'POST':
        pass
    else:
        form = CodeResourceRevisionForm()

    t = loader.get_template('resource_add_revision.html')
    c = Context({'resource_form': form})
    c.update(csrf(request))
    return HttpResponse(t.render(c))



def usr(request):
    """
    User portal
    """
    t = loader.get_template('usr.html')
    c = Context()
    return HttpResponse(t.render(c))
