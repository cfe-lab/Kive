import time
import json
import re
import os
import itertools

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
from datachecking.models import ContentCheckLog, IntegrityCheckLog
from execute import Sandbox
from forms import PipelineSelectionForm
import fleet.models
from django.db import transaction


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

    # Inform the fleet that this is to be processed.
    with transaction.atomic():
        run_to_start = fleet.models.RunToProcess(user=user, pipeline=pipeline)
        run_to_start.save()

        for i, sd in enumerate(symbolic_datasets):
            run_to_start.inputs.create(symbolicdataset=sd, index=i)

    return json.dumps({"run": None, "status": "Waiting", "finished": False, "success": True,
                       "queue_placeholder": run_to_start.pk, "crashed": False})


def run_pipeline(request):
    return AJAXRequestHandler(request, _run_pipeline).response


def _filter_datasets(request):
    filters = json.loads(request.GET.get("filter_data"))
    try:
        cdt_pk = int(request.GET.get("compound_datatype"))
        query = Dataset.objects.filter(symbolicdataset__structure__compounddatatype=cdt_pk)
    except TypeError:
        query = Dataset.objects.filter(symbolicdataset__structure__isnull=True)
    query = query.order_by("-date_created")

    for filter_instance in filters:
        key, value = filter_instance["key"], filter_instance["val"]
        if key == "Name":
            query = query.filter(name__iregex=value)
        elif key == "Uploaded" and value:
            query = query.filter(created_by__isnull=True)
        elif key == "Smart":
            query = query.filter(Q(name__iregex=value) | Q(description__iregex=value))
    if len(filters) == 0:
        query = query[:10]

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


# def _in_progress(queue_pk):
#     rtp_qs = fleet.models.RunToProcess.objects.filter(pk=queue_pk)
#     if not rtp_qs.exists():
#         return False
#
#     rtp = rtp_qs.first()
#     if not rtp.started:
#         return False
#     return not rtp.run.is_complete()


def _describe_run_failure(run):
    """
    Return a tuple (error, reason) describing a Run failure.

    TODO: this is very rudimentary at the moment.
    - It does not take recovery into account - should report which step
      was actually executed and failed, not which step tried to recover
      and failed.
    - It does not take sub-pipelines into account.
    - Failure details for a cable are not reported.
    - Details of cell errors are not reported.
    """
    total_steps = run.pipeline.steps.count()
    error = ""
    reason = ""

    # Check each step for failure.
    for i, runstep in enumerate(run.runsteps.order_by("pipelinestep__step_num"), start=1):

        if runstep.is_complete() and not runstep.successful_execution():
            error = "Step {} of {} failed".format(i, total_steps)

            # Check each cable.
            total_cables = runstep.pipelinestep.cables_in.count()
            for j, runcable in enumerate(runstep.RSICs.order_by("PSIC__dest__dataset_idx"), start=1):
                if not runcable.successful_execution():
                    return (error, "Input cable {} of {} failed".format(j, total_cables))

            # Check the step execution.
            if not runstep.log:
                return (error, "Recovery failed")
            return_code = runstep.log.methodoutput.return_code 
            if return_code != 0:
                return (error, "Return code {}".format(return_code))

            # Check for bad output.
            for output in runstep.execrecord.execrecordouts.all():
                try:
                    check = runstep.log.content_checks.get(symbolicdataset=output.symbolicdataset)
                except ContentCheckLog.DoesNotExist:
                    try:
                        check = runstep.log.integrity_checks.get(symbolicdataset=output.symbolicdataset)
                    except IntegrityCheckLog.DoesNotExist:
                        continue

                if check.is_fail():
                    return (error, "Output {}: {}".format(output.generic_output.definite.dataset_idx, check))

            # Something else went wrong with the step?
            return (error, "Unknown error")
                
    # Check each output cable.
    total_cables = run.pipeline.outcables.count()
    for i, runcable in enumerate(run.runoutputcables.order_by("pipelineoutputcable__output_idx")):
        if not runcable.successful_execution():
            return ("Output {} of {} failed".format(i, total_cables), "could not copy file")

    # Shouldn't reach here.
    return ("Unknown error", "Unknown reason")




def _poll_run_progress(request):
    """
    Helper to produce a JSON description of the current state of a run.
    """
    rtp_pk = request.GET.get("queue_placeholder")
    rtp = fleet.models.RunToProcess.objects.get(pk=rtp_pk)

    last_status = request.GET.get("status")
    status = rtp.get_run_progress()

    # If the Run isn't done but the process is, we've crashed.
    # FIXME we are no longer monitoring threads directly here, so we need another way to know if
    # the pipeline has crashed.
    #crashed = rtp.started and not rtp.finished and not rtp.running
    # For now....
    crashed = False

    # Arrrgh I hate sleeping. Find a better way.
    while status == last_status and not rtp.finished:
        time.sleep(1)
        status = rtp.get_run_progress()

    success = rtp.started and rtp.run.successful_execution()

    return json.dumps({"status": status, "run": run_pk, "finished": rtp.finished, "success": success,
                       "queue_placeholder": queue_placeholder, "crashed": crashed})


def poll_run_progress(request):
    return AJAXRequestHandler(request, _poll_run_progress).response


def tail(handle, nbytes):
    """Get the last nbytes from a file."""
    orig_pos = handle.tell()
    handle.seek(0, 2)
    size = handle.tell()
    handle.seek(max(size-nbytes, 0), 0)
    result = handle.read()
    handle.seek(orig_pos)
    return result


def _get_failed_output(request):
    """Head the stdout and stderr of the failed step of a Run.
    
    TODO: Does not handle invoked steps.
    """
    run_pk = int(request.GET.get("run"))
    run = Run.objects.get(pk=run_pk)
    
    stdout, stderr = None, None
    for component in itertools.chain(run.runsteps.all(), run.runoutputcables.all()):
        if not component.successful_execution():
            if component.has_log and hasattr(component.log, "methodoutput"):
                stdout = component.log.methodoutput.output_log
                stderr = component.log.methodoutput.error_log
                break

    response_data = {"stdout": "", "stderr": ""}
    if stdout is not None:
        response_data["stdout"] = tail(stdout.file, 1024)
    if stderr is not None:
        response_data["stderr"] = tail(stderr.file, 1024)
    return json.dumps(response_data)


def get_failed_output(request):
    return AJAXRequestHandler(request, _get_failed_output).response
