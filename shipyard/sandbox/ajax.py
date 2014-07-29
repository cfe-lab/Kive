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

def run_pipeline(request):
    """Run a Pipeline as the global Shipyard user."""
    if request.is_ajax():
        response = HttpResponse()
        pipeline_pk = request.GET.get("pipeline")
        pipeline = Pipeline.objects.get(pk=pipeline_pk)

        symbolic_datasets = []
        for i in range(1, pipeline.inputs.count()+1):
            pk = int(request.GET.get("input_{}".format(i)))
            symbolic_datasets.append(SymbolicDataset.objects.get(pk=pk))

        # TODO: for now this is just using the global Shipyard user
        user = User.objects.get(username="shipyard")

        sandbox = Sandbox(user, pipeline, symbolic_datasets)
        response.write(json.dumps({"run": sandbox.run.pk, "status": "Starting run", "finished": False}))
        Process(target=sandbox.execute_pipeline).start()

        return response
    else:
        return HttpResponse(status=405) # Method not allowed.

def filter_datasets(request):
    if request.is_ajax():
        filters = json.loads(request.GET.get("filter_data"))
        try:
            cdt_pk = int(request.GET.get("compound_datatype"))
            query = Dataset.objects.filter(symbolicdataset__structure__compounddatatype=cdt_pk)
        except TypeError:
            query = Dataset.objects.filter(symbolicdataset__structure__isnull=True)

        for filter_instance in filters:
            key = filter_instance["key"]
            value = filter_instance["val"]
            if key == "Name":
                query = query.filter(Q(name__iregex=value) | Q(description__iregex=value))
            elif key == "Uploaded" and value:
                query = query.filter(created_by__isnull=True)

        response_data = []
        for dataset in query.all():
            response_data.append({"pk": dataset.pk, 
                                  "Name": dataset.name, 
                                  "Date": str(dataset.date_created)})
        response = HttpResponse()
        response.write(json.dumps(response_data))
        return response
    else:
        return HttpResponse(status=405) # Method not allowed.

def _get_run_progress(run):
    """
    Return a tuple (status, finished), where status is a string
    describing the Run's current state, and finished is True if
    the Run is finished or False if it's in progress.
    """
    # Run is finished?
    if run.is_complete():
        if run.successful_execution():
            return ("Complete", True)
        return ("Failed", True)

    # One of the steps is in progress?
    total_steps = run.pipeline.steps.count()
    for i, step in enumerate(run.runsteps.order_by("pipelinestep__step_num"), start=1):
        if not step.is_complete():
            return ("Running step {} of {}".format(i, total_steps), False)

    # Just finished a step, but didn't start the next one?
    if run.runsteps.count() < total_steps:
        return ("Starting step {} of {}".format(run.runsteps.count()+1, total_steps), False)

    # One of the outcables is in progress?
    total_cables = run.pipeline.outcables.count()
    for i, cable in enumerate(run.runoutputcables.order_by("pipelineoutputcable__output_idx"), start=1):
        if not cable.is_complete():
            return ("Creating output {} of {}".format(i, total_cables), False)

    # Just finished a cable, but didn't start the next one?
    if run.runoutputcables.count() < total_cables:
        return ("Starting output {} of {}".format(run.runoutputcables.count()+1, total_cables), False)

    # Something is wrong.
    return ("Unknown status", False)

def poll_run_progress(request):
    if request.is_ajax():
        run_pk = int(request.GET.get("run"))
        last_status = request.GET.get("status")
        run = Run.objects.get(pk=run_pk)
        status, finished = _get_run_progress(run)
        # Arrrgh I hate sleeping. Find a better way.
        while status == last_status and not finished:
            time.sleep(1)
            status, finished = _get_run_progress(run)

        response = HttpResponse()
        response.write(json.dumps({"run": run_pk, "status": status, "finished": finished}))
        return response
    else:
        raise Http404
