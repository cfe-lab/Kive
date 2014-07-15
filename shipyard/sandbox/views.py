from django.template import loader, Context
from django.core.context_processors import csrf
from django.http import HttpResponse
from django.contrib.auth.models import User

from sandbox.forms import *

import pipeline.models
import librarian.models
import archive.models
from sandbox.execute import Sandbox

def choose_pipeline(request):
    """Create forms for all Pipelines in Shipyard."""
    template = loader.get_template("sandbox/choose_pipeline.html")
    families = pipeline.models.PipelineFamily.objects.all()
    forms = [PipelineSelectionForm(pipeline_family_pk=f.pk) for f in families]
    context = Context({"pipeline_forms": forms})
    context.update(csrf(request))
    return HttpResponse(template.render(context))

def choose_inputs(request):
    """Load the input selection page."""
    if request.method == "POST":
        template = loader.get_template("sandbox/choose_inputs.html")
        pipeline = request.POST.get("pipeline")
        form = InputSelectionForm(pipeline=pipeline)
        context = Context({"input_form": form})
        context.update(csrf(request))
        return HttpResponse(template.render(context))
    else:
        # Method not allowed
        return HttpResponse(status=405)

def view_results(request):
    """View outputs from a pipeline run."""
    if request.method == "POST":
        template = loader.get_template("sandbox/view_results.html")
        run_pk = request.POST.get("run")
        run = archive.models.Run.objects.get(pk=run_pk)
        context = Context({"run": run})
        context.update(csrf(request))
        return HttpResponse(template.render(context))
    else:
        # Method not allowed
        return HttpResponse(status=405)
