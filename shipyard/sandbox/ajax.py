from datetime import timedelta
import itertools
import json
import logging
import time

from django.db import transaction
from django.db.models import Q
from django.http import HttpResponse
from django.utils import timezone
from django.contrib.auth.decorators import login_required

from archive.models import Dataset, Run
import fleet.models
from forms import PipelineSelectionForm, PipelineSubmissionForm, InputSubmissionForm
from librarian.models import SymbolicDataset
from pipeline.models import Pipeline, PipelineFamily
import metadata.models

ajax_logger = logging.getLogger("sandbox.ajax")


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
    """Run a Pipeline.
    
    Request parameters are:
    
    * pipeline - the pipeline id
    * input_1, input_2, etc. - the *symbolic* dataset ids to use as inputs
    """
    pipeline_submission = PipelineSubmissionForm(request.GET)
    pipeline_submission.is_valid()
    pipeline = Pipeline.objects.get(pk=pipeline_submission.cleaned_data["pipeline_pk"])

    symbolic_datasets = []
    for i in range(1, pipeline.inputs.count()+1):
        curr_input_form = InputSubmissionForm({"input_pk": request.GET.get("input_{}".format(i))})
        curr_input_form.is_valid()
        symbolic_datasets.append(SymbolicDataset.objects.get(pk=curr_input_form.cleaned_data["input_pk"]))

    # Inform the fleet that this is to be processed.
    with transaction.atomic():
        run_to_start = fleet.models.RunToProcess(user=request.user, pipeline=pipeline)
        run_to_start.save()
        run_to_start.users_allowed.add(pipeline_submission.cleaned_data["users_allowed"])
        run_to_start.groups_allowed.add(pipeline_submission.cleaned_data["groups_allowed"])

        for i, sd in enumerate(symbolic_datasets):
            run_to_start.inputs.create(symbolicdataset=sd, index=i)

    return json.dumps({"run": None, "status": "Waiting", "finished": False, "success": True,
                       "queue_placeholder": run_to_start.pk, "crashed": False})


@login_required
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
                              "symbolicdataset_id": dataset.symbolicdataset_id,
                              "Name": dataset.name, 
                              "Date": dataset.date_created.strftime("%b %e, %Y, %l:%M %P")})
    return json.dumps(response_data)


@login_required
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
    response_data = []
    for family in query:
        form = PipelineSelectionForm(pipeline_family_pk=family.pk) 
        response_data.append({"Pipeline Family": form.family_name,
                              "Revision": form.fields["pipeline"].widget.render("id_pipeline", "")})
    return json.dumps(response_data)


@login_required
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


def _load_status(user):
    user_plus = metadata.models.KiveUser.kiveify(user)
    try:
        recent_time = timezone.now() - timedelta(minutes=5)
        old_aborted_runs = fleet.models.ExceedsSystemCapabilities.objects.values(
            'runtoprocess_id').filter(runtoprocess__time_queued__lt=recent_time)
        runs = fleet.models.RunToProcess.objects.filter(
            user_plus.access_query(),
            Q(run_id__isnull=True)|
            Q(run__end_time__isnull=True)|
            Q(run__end_time__gte=recent_time)).distinct().exclude(
            pk__in=old_aborted_runs).order_by('time_queued')
        run_reports = [run.get_run_progress() for run in runs]
        
        status = '\n'.join(run_reports)
    except StandardError as e:
        status = str(e)
        ajax_logger.error('Status report failed.', exc_info=e)
    return status


def _poll_run_progress(request):
    """
    Helper to produce a JSON description of the current state of a run.
    """
    rtp_pk = request.GET.get("queue_placeholder")
    rtp = fleet.models.RunToProcess.objects.get(pk=rtp_pk)

    last_status = request.GET.get("status")

    # If the Run isn't done but the process is, we've crashed.
    # FIXME we are no longer monitoring threads directly here, so we need another way to know if
    # the pipeline has crashed.
    #crashed = rtp.started and not rtp.finished and not rtp.running
    # For now....
    crashed = False

    # Arrrgh I hate sleeping. Find a better way.
    while True:
        status = _load_status(request.user)
        if status != last_status or not status:
            break
        time.sleep(1)
        # ajax_logger.debug("status: {}".format(status))
        # ajax_logger.debug("rtp.finished: {}".format(rtp.finished))
        # ajax_logger.debug("run PK: {}".format(None if rtp.run is None else rtp.run.pk))

    success = rtp.started and rtp.run and rtp.run.successful_execution()

    run_pk = None if rtp.run is None else rtp.run.pk
    return_val = json.dumps({"status": status, "run": run_pk, "finished": len(status) == 0, "success": success,
                             "queue_placeholder": rtp_pk, "crashed": crashed})
    ajax_logger.debug("Returning: {}".format(return_val))
    return return_val


@login_required
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
        if not component.is_successful():
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


@login_required
def get_failed_output(request):
    return AJAXRequestHandler(request, _get_failed_output).response
