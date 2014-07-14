from django.http import HttpResponse, Http404
from django.core import serializers
from django.contrib.auth.models import User
from django.contrib.contenttypes.models import ContentType

from pipeline.models import Pipeline, PipelineFamily
from archive.models import Dataset, Run, RunOutputCable, RunSIC
from execute import Sandbox

import json

def run_pipeline(request):
    """Run a Pipeline as the global Shipyard user."""
    if request.is_ajax():
        response = HttpResponse()
        pipeline_pk = request.POST.get("pipeline_pk")
        pipeline = Pipeline.objects.get(pk=pipeline_pk)
        dataset_pks = request.POST.getlist("dataset_pks[]")

        datasets = [Dataset.objects.get(pk=pk) for pk in dataset_pks]
        inputs = [d.symbolicdataset for d in datasets]

        user = User.objects.get(username="shipyard")

        sandbox = Sandbox(user, pipeline, inputs)
        sandbox.execute_pipeline()

        response.write(json.dumps(qs2dict(sandbox.run)))
        return response
    else:
        raise Http404

# TODO: should this go in Run (ie. run.get_progress())?
def _get_run_progress(run):
    # Run is finished?
    if run.is_complete():
        if run.succesful_execution():
            return "Complete"
        return "Failed"

    # One of the steps is in progress?
    total_steps = run.pipeline.steps.count()
    for i, step in enumerate(run.runsteps.order_by(pipelinestep__step_num), start=1):
        if not step.is_complete():
            return "Running step {} of {}".format(i, total_steps)

    # Just finished a step, but didn't start the next one?
    if run.runsteps.count() < total_steps:
        return "Starting step {} of {}".format(run.runsteps.count()+1, total_steps)

    # One of the outcables is in progress?
    total_cables = run.pipeline.outcables.count()
    for i, cable in enumerate(run.runoutputcables.order_by(pipelineoutputcable__output_idx), start=1):
        if not cable.is_complete():
            return "Creating output {} of {}".format(i, total_cables)

    # Just finished a cable, but didn't start the next one?
    if run.runoutputcables.count() < total_cables:
        return "Starting output {} of {}".format(run.runoutputcables.count()+1, total_cables)

    # Something is wrong.
    return "Unknown status"

def poll_run_progress(request):
    if request.is_ajax():
        response = HttpResponse()
        run_pk = int(request.POST.get("run_pk"))
        run = Run.objects.get(pk=run_pk)
        response.write(_get_run_progress(run))
        return response
    else:
        raise Http404
