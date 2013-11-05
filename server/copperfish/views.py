# Create your views here.
from django.http import HttpResponse
from copperfish import models
from django.template import loader, Context
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
                cform = BasicConstraintForm({'datatype': new_datatype,
                                            'ruletype': ruletype,
                                            'rule': rule})
                
                
                #if cform.is_valid():
                #    print 'ok'
                new_constraint = cform.save(commit = False)
                """
                new_constraint.datatype = new_datatype
                if new_constraint.is_valid():
                    new_constraint.save()
                """
        
        #cform = BasicConstraintForm(request.POST)
        #if cform.is_valid():
        #    new_constraint = cform.save()
        #    pass
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
