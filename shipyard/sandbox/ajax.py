from datetime import timedelta
import itertools
import json
import logging
import time

from django.db.models import Q
from django.http import HttpResponse
from django.utils import timezone
from django.contrib.auth.decorators import login_required

from archive.models import Dataset, Run
import fleet.models
from forms import PipelineSelectionForm
from pipeline.models import PipelineFamily

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


def _load_status(user):
    runs = fleet.models.RunToProcess.filter_by_user(user).order_by('time_queued')
    is_active_required = True
    if is_active_required:
        recent_time = timezone.now() - timedelta(minutes=5)
        old_aborted_runs = fleet.models.ExceedsSystemCapabilities.objects.values(
            'runtoprocess_id').filter(runtoprocess__time_queued__lt=recent_time)
        runs = runs.filter(
            Q(run_id__isnull=True)|
            Q(run__end_time__isnull=True)|
            Q(run__end_time__gte=recent_time)).distinct().exclude(
            pk__in=old_aborted_runs)
    return [run.get_run_progress() for run in runs]
    """ Find all active runs, and return a dict for each with the status.
    
    @return [{'id': run_id, 'status': status, 'name': name}]
    """

def _is_status_changed(runs, request):
    for i, run in enumerate(runs):
        prefix = 'previous[runs][{}]'.format(i)
        previous_run_id = request.GET.get(prefix + '[id]')
        previous_run_id = previous_run_id and int(previous_run_id)
        if (run.get('id') != previous_run_id or
            run['name'] != request.GET.get(prefix + '[name]') or 
            run['status'] != request.GET.get(prefix + '[status]')):
            return True
    
    return False

def _poll_run_progress(request):
    """
    Helper to produce a JSON description of the current state of a run.
    """
    result = {'runs': [], 'errors': []}
    try:
        # Arrrgh I hate sleeping. Find a better way.
        while True:
            runs = _load_status(request.user)
            if _is_status_changed(runs, request):
                break
            time.sleep(1)

        result['runs'] = runs
    except StandardError:
        ajax_logger.error('Status report failed.', exc_info=True)
        result['errors'].append('Status report failed.')

    ajax_logger.debug("Returning: %r", result)
    return json.dumps(result)

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
