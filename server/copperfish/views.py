# Create your views here.
from django.http import HttpResponse
from copperfish import models
from django.template import loader, Context
from copperfish.forms import DatatypeForm
#from django.shortcuts import render, render_to_response
from django.core.context_processors import csrf

def home(request):
	t = loader.get_template('index.html')
	c = Context()
	return HttpResponse(t.render(c))


def datatypes(request):
	#return HttpResponse(t.render(c))
	
	if request.method == 'POST':
		form = DatatypeForm(request.POST)
		if form.is_valid():
			new_datatype = form.save()
	else:
		form = DatatypeForm() # unbound
	
	datatypes = models.Datatype.objects.all()
	t = loader.get_template('datatypes.html')
	c = Context({'datatypes': datatypes, 'form': form})
	c.update(csrf(request))
	
	return HttpResponse(t.render(c))
	#return render_to_response('datatypes.html', {'form': form})
