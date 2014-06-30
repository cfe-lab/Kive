from django.http import HttpResponse, Http404
from django.core import serializers
from django.contrib.auth.models import User
from django.contrib.contenttypes.models import ContentType

from pipeline.models import Pipeline, PipelineFamily
from archive.models import Dataset, Run, RunOutputCable, RunSIC
from execute import Sandbox

import json

# TODO: this might be better suited as a helper in the backend
def find_runs_with_inputs(pipeline, datasets):
    """Find all Runs of a pipeline with the given set of inputs."""
    runs = set([run.pk for run in Run.objects.filter(pipeline=pipeline)])
    for i, dataset in enumerate(datasets, start=1):
        if dataset is None: continue
        curr_runs = set([]) # runs compatible with this dataset being at position i
        for runsic in RunSIC.objects.filter(runstep__run__pipeline=pipeline, PSIC__source_step=0,
                                            PSIC__source__transformationinput__dataset_idx=i):
            if runsic.execrecord.execrecordins.first().symbolicdataset == dataset.symbolicdataset:
                curr_runs.add(runsic.runstep.run.pk)
        runs = runs.intersection(curr_runs)
        if len(runs) == 0:
            return runs
    return runs

def qs2dict(qs):
    """Make a QuerySet or Django object into a dictionary."""
    if qs.__class__.__name__ == "QuerySet":
        ser = serializers.serialize("json", qs)
        return json.loads(ser)
    else:
        ser = serializers.serialize("json", [qs,])
        return json.loads(ser)[0]

def handle_xput_request(request):
    if request.is_ajax():
        pipeline_pk = request.POST.get("pipeline_pk")
        dataset_pks = request.POST.getlist("dataset_pks[]")
        pipeline = Pipeline.objects.get(pk=pipeline_pk)
        datasets = []
        if dataset_pks:
            datasets = [Dataset.objects.get(pk=pk) for pk in dataset_pks]
        return (pipeline, datasets)
    else:
        raise Http404

def get_pipeline_inputs(request):
    """Get inputs for a pipeline, and possible datasets for each."""
    pipeline = handle_xput_request(request)[0]
    response = HttpResponse()

    res = []
    for trans_input in pipeline.inputs.all():
        cdt = trans_input.compounddatatype
        if trans_input.is_raw():
            compat = Dataset.objects.filter(symbolicdataset__structure__isnull=True)
        else:
            compat = Dataset.objects.filter(symbolicdataset__structure__compounddatatype=cdt)
        input_dict = qs2dict(trans_input)
        compat_dict = qs2dict(compat)
        res.append([input_dict, compat_dict])
    response.write(json.dumps(res))
    return response

def get_pipeline_outputs(request):
    """Get outputs from a pipeline for a particular set of inputs."""
    pipeline, datasets = handle_xput_request(request)
    response = HttpResponse()

    outputs = [[] for i in range(pipeline.outputs.count())]
    for run_pk in find_runs_with_inputs(pipeline, datasets):
        run = Run.objects.get(pk=run_pk)
        for i, outcable in enumerate(run.runoutputcables.order_by("pipelineoutputcable__output_idx")):
            sd = outcable.execrecord.execrecordouts.first().symbolicdataset

            # If the output has real data, serialize it. Otherwise
            # serialize the symbolic version.
            if sd.has_data():
                dataset_dict = qs2dict(sd.dataset)
            else:
                dataset_dict = qs2dict(sd)
            if not dataset_dict in outputs[i]:
                outputs[i].append(dataset_dict)

    # We will return a list of tuples (output, [datasets]).
    res = []
    for i, to in enumerate(pipeline.outputs.order_by("dataset_idx")):
        res.append((qs2dict(to), outputs[i]))

    response.write(json.dumps(res))
    return response

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
