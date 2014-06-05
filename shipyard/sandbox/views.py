from django.template import loader, Context
from django.core.context_processors import csrf
from django.http import HttpResponse, HttpResponseRedirect

import pipeline.models

def sandbox_setup(request):
    """Get a list of all Pipelines in Shipyard."""
    t = loader.get_template('sandbox/sandbox.html')
    families = pipeline.models.PipelineFamily.objects.all()
    families = [(f, f.members.all()) for f in families]
    c = Context({'families': families})
    c.update(csrf(request))
    return HttpResponse(t.render(c))
