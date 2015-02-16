from django.template import loader, Context
from django.core.context_processors import csrf
from django.http import HttpResponse

import archive.models
import pipeline.models
from sandbox.forms import PipelineSelectionForm


def choose_pipeline(request):
    """Create forms for all Pipelines in Shipyard."""
    template = loader.get_template("sandbox/choose_pipeline.html")
    families = pipeline.models.PipelineFamily.objects.all()
    forms = []
    for family in families:
        if len(family.complete_members) > 0:
            forms.append(PipelineSelectionForm(pipeline_family_pk=family.pk))
    context = Context({"pipeline_forms": forms})
    context.update(csrf(request))
    return HttpResponse(template.render(context))

def choose_inputs(request):
    """Load the input selection page."""
    if request.method == "GET":
        template = loader.get_template("sandbox/choose_inputs.html")
        pipeline_pk = int(request.GET.get("pipeline"))

        response_data = []
        my_pipeline = pipeline.models.Pipeline.objects.get(pk=pipeline_pk)

        # Find all compatible datasets for each input.
        for my_input in my_pipeline.inputs.order_by("dataset_idx"):
            if my_input.is_raw():
                query = archive.models.Dataset.objects.filter(symbolicdataset__structure__isnull=True)
            else:
                compound_datatype = my_input.get_cdt()
                query = archive.models.Dataset.objects.filter(
                    symbolicdataset__structure__compounddatatype=compound_datatype)
            count = query.count()
            query = query.order_by("created_by", "date_created")[:10]
            response_data.append((my_input, query, count))

        context = Context({"input_data": response_data})
        context.update(csrf(request))
        return HttpResponse(template.render(context))
    else:
        # Method not allowed
        return HttpResponse(status=405)

def view_results(request, run_id):
    """View outputs from a pipeline run."""
    template = loader.get_template("sandbox/view_results.html")
    run = archive.models.Run.objects.get(pk=run_id)
    context = Context({"run": run})
    context.update(csrf(request))
    return HttpResponse(template.render(context))
