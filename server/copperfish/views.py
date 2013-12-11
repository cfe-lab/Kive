# Create your views here.
from django.http import HttpResponse, HttpResponseRedirect
from copperfish import models
from django.template import loader, Context
from copperfish.models import BasicConstraint
from copperfish.forms import *
#from django.shortcuts import render, render_to_response
from django.core.context_processors import csrf

def home(request):
    """
    Default homepage
    """
    t = loader.get_template('index.html')
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
    if request.method == 'POST':
        dform = DatatypeForm(request.POST)
        query = request.POST.dict()
        
        if dform.is_valid():
            new_datatype = dform.save(commit=False)
            try:            
                if new_datatype.Python_type == 'str':
                    # manually create and validate BasicConstraint objects    
                    minlen = BasicConstraintForm(initial={'datatype': new_datatype,
                                                    'ruletype': 'minlen', 
                                                    'rule': query['minlen']})
                    if minlen.is_valid():
                        minlen.save(commit=False)
                    else:
                        raise ValidationError
                    
                    maxlen = BasicConstraintForm(initial={'datatype': new_datatype,
                                                    'ruletype': 'maxlen', 
                                                    'rule': query['maxlen']})
                    if maxlen.is_valid():
                        minlen.save(commit=False)
                    else:
                        raise ValidationError
                    
                    regexp = BasicConstraintForm(initial={'datatype': new_datatype,
                                                    'ruletype': 'regexp', 
                                                    'rule': query['regexp']})
                    if regexp.is_valid():
                        regexp.save(commit=False)
                    else:
                        raise ValidationError
                
                elif new_datatype.Python_type in ['int', 'float']:
                    minval = BasicConstraintForm(initial={'datatype': new_datatype,
                                                    'ruletype': 'minval', 
                                                    'rule': query['minval']})
                    print 'call is_valid'
                    if minval.is_valid():
                        minval.save(commit=False)
                    else:
                        print 'minval validation error', minval.errors
                        raise ValidationError
                    
                    maxval = BasicConstraintForm(initial={'datatype': new_datatype,
                                                    'ruletype': 'maxval', 
                                                    'rule': query['maxval']})
                    if maxval.is_valid():
                        maxval.save(commit=False)
                    else:
                        print 'maxval validation error', maxval.errors
                        raise ValidationError
                
                # re-check Datatype object
                new_datatype.full_clean()
                new_datatype.save()
                return HttpResponseRedirect('/datatypes')
                
            except:
                pass # through
                
        icform = IntegerConstraintForm({'minval': query.get('minval', None), 
            'maxval': query.get('maxval', None)})
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



