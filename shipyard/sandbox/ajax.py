from multiprocessing import Process
import time
import json
import re

from django.http import HttpResponse
from django.core import serializers
from django.contrib.auth.models import User
from django.contrib.contenttypes.models import ContentType
from django.db.models import Q

from pipeline.models import Pipeline, PipelineFamily
from archive.models import Dataset, Run
from librarian.models import SymbolicDataset
from transformation.models import TransformationInput
from metadata.models import CompoundDatatype
from execute import Sandbox
from forms import PipelineSelectionForm

class AJAXRequestHandler:
    """A simple class to handle AJAX requests."""

    def __init__(self, request, response_fun, *args, **kwargs):
        """Construct a new request handler.

        INPUTS
        request         the request (AJAX or not) to respond to
        response_fun    a function which returns data to pass back to the client
        *args           additional arguments to response_fun
        **kwargs        keyword arguments to response_fun
        """
        if request.is_ajax():
            self.response = HttpResponse()
            self.response.write(response_fun(request, *args, **kwargs))
        else:
            self.response = HttpResponse(status=405) # Method not allowed

def _run_pipeline(request):
    """Run a Pipeline as the global Shipyard user."""
    pipeline_pk = request.GET.get("pipeline")
    pipeline = Pipeline.objects.get(pk=pipeline_pk)

    symbolic_datasets = []
    for i in range(1, pipeline.inputs.count()+1):
        pk = int(request.GET.get("input_{}".format(i)))
        symbolic_datasets.append(SymbolicDataset.objects.get(pk=pk))

    # TODO: for now this is just using the global Shipyard user
    user = User.objects.get(username="shipyard")

    sandbox = Sandbox(user, pipeline, symbolic_datasets)
    Process(target=sandbox.execute_pipeline).start()
    return json.dumps({"run": sandbox.run.pk, "status": "Starting run", "finished": False})

def run_pipeline(request):
    return AJAXRequestHandler(request, _run_pipeline).response

def _filter_datasets(request):
    filters = json.loads(request.GET.get("filter_data"))
    try:
        cdt_pk = int(request.GET.get("compound_datatype"))
        query = Dataset.objects.filter(symbolicdataset__structure__compounddatatype=cdt_pk)
    except TypeError:
        query = Dataset.objects.filter(symbolicdataset__structure__isnull=True)

    for filter_instance in filters:
        key, value = filter_instance["key"], filter_instance["val"]
        if key == "Name":
            query = query.filter(name__iregex=value)
        elif key == "Uploaded" and value:
            query = query.filter(created_by__isnull=True)
        elif key == "Smart":
            query = query.filter(Q(name__iregex=value) | Q(description__iregex=value))

    response_data = []
    for dataset in query.all():
        response_data.append({"pk": dataset.pk, 
                              "Name": dataset.name, 
                              "Date": dataset.date_created.strftime("%b %e, %Y, %l:%M %P")})
    return json.dumps(response_data)

def filter_datasets(request):
    return AJAXRequestHandler(request, _filter_datasets).response

def _filter_pipelines(request):
    """
    Search for Pipelines matching a query; return serialized forms.
    """
    filters = json.loads(request.GET.get("filter_data"))
    query = PipelineFamily.objects.all()

    for filter_instance in filters:
        key, value = filter_instance["key"], filter_instance["val"]
        if key == "Smart":
            query = query.filter(Q(name__iregex=value) | Q(description__iregex=value))
    forms = [PipelineSelectionForm(pipeline_family_pk=f.pk) for f in query]
    response_data = []
    for family in query:
        form = PipelineSelectionForm(pipeline_family_pk=f.pk) 
        response_data.append({"Pipeline Family": form.family_name,
                              "Revision": form.fields["pipeline"].widget.render("id_pipeline", "")})
    return json.dumps(response_data)

def filter_pipelines(request):
    return AJAXRequestHandler(request, _filter_pipelines).response

def _get_run_progress(run):
    """
    Return a tuple (status, finished), where status is a string
    describing the Run's current state, and finished is True if
    the Run is finished or False if it's in progress.
    """
    # Run is finished?
    if run.is_complete():
        if run.successful_execution():
            return "Complete"
        return "Failed"

    # One of the steps is in progress?
    total_steps = run.pipeline.steps.count()
    for i, step in enumerate(run.runsteps.order_by("pipelinestep__step_num"), start=1):
        if not step.is_complete():
            return "Running step {} of {}".format(i, total_steps)

    # Just finished a step, but didn't start the next one?
    if run.runsteps.count() < total_steps:
        return "Starting step {} of {}".format(run.runsteps.count()+1, total_steps)

    # One of the outcables is in progress?
    total_cables = run.pipeline.outcables.count()
    for i, cable in enumerate(run.runoutputcables.order_by("pipelineoutputcable__output_idx"), start=1):
        if not cable.is_complete():
            return "Creating output {} of {}".format(i, total_cables)

    # Just finished a cable, but didn't start the next one?
    if run.runoutputcables.count() < total_cables:
        return "Starting output {} of {}"

    # Something is wrong.
    return "Unknown status"

def _poll_run_progress(request):
    run_pk = int(request.GET.get("run"))
    last_status = request.GET.get("status")
    run = Run.objects.get(pk=run_pk)
    finished = run.is_complete()
    status = _get_run_progress(run)

    # Arrrgh I hate sleeping. Find a better way.
    while status == last_status and not finished:
        time.sleep(1)
        finished = run.is_complete()
        status = _get_run_progress(run)
    return json.dumps({"status": status, "run": run_pk, "finished": finished})

def poll_run_progress(request):
    return AJAXRequestHandler(request, _poll_run_progress).response
