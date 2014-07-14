from django.template import loader, Context
from django.core.context_processors import csrf
from django.http import HttpResponse, HttpResponseRedirect

from sandbox.forms import PipelineSelectionForm
from pipeline.models import PipelineFamily

import pipeline.models

def sandbox_setup(request):
    """Create forms for all Pipelines in Shipyard."""
    t = loader.get_template('sandbox/sandbox.html')
    families = pipeline.models.PipelineFamily.objects.all()
    forms = [PipelineSelectionForm(pipeline_family_pk=f.pk) for f in families]
    c = Context({'pipeline_forms': forms})
    c.update(csrf(request))
    return HttpResponse(t.render(c))
