# Create your views here.
from django.http import HttpResponse
from copperfish import models
from django.template import loader, Context
from copperfish.models import BasicConstraint
from copperfish.forms import DatatypeForm, BasicConstraintForm
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
    if request.method == 'POST':
        dform = DatatypeForm(request.POST)
        if dform.is_valid():
            new_datatype = dform.save() # return instance of Model
            
            # add basic constraints ONLY if datatype is valid!
            query = request.POST.dict()
            numberOfFields = 0 # how many basic constraints were submitted
            for key in query.iterkeys():
                if key.startswith('ruletype'):
                    numberOfFields += 1

            for i in range(numberOfFields):
                ruletype = query['ruletype' + (str(i-1) if i > 0 else '')]
                rule = query['rule' + (str(i-1) if i > 0 else '')]
                new_constraint = BasicConstraint(datatype = new_datatype,
                                            ruletype = ruletype,
                                            rule = rule)
                
                new_constraint.full_clean()
                new_constraint.save()
                
            # re-check Datatype object
            new_datatype.full_clean()
            
            # additional fields will disappear on submit, but let's
            # at least set the original field to the submitted values
            cform = BasicConstraintForm({'ruletype': query['ruletype'],
                                        'rule': query['rule']})
        
    else:
        dform = DatatypeForm() # unbound
        cform = BasicConstraintForm()

    datatypes = models.Datatype.objects.all()
    t = loader.get_template('datatypes.html')
    c = Context({'datatypes': datatypes, 
                'datatype_form': dform,
                'constraint_form': cform})
    c.update(csrf(request))

    return HttpResponse(t.render(c))
    #return render_to_response('datatypes.html', {'form': form})



def datatype_detail(request, id):
    # retrieve the Datatype object from database by PK
    this_datatype = models.Datatype.objects.get(pk=id)
    
    if request.method == 'POST':
        query = request.POST.dict()
        # modify Datatype / spawn new version of immutable Datatype
        
            
        return HttpResponseRedirect('/datatypes/')
    
    form = DatatypeForm(instance = this_datatype)
    
    # how many basic constraints are associated with current Datatype?
    cforms = []
    for i, bc in enumerate(this_datatype.basic_constraints.all()):
        cforms.append(BasicConstraintForm(instance = bc))
    
    t = loader.get_template('datatype_detail.html')
    c = Context({'datatype': this_datatype, 
                'form': form,
                'constraint_forms': cforms})
    c.update(csrf(request))
    return HttpResponse(t.render(c))



