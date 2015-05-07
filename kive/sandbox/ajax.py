from datetime import timedelta, datetime
import itertools
import json
import logging

from django.db.models import Q
from django.http import HttpResponse
from django.utils import timezone
from django.contrib.auth.decorators import login_required

from archive.models import Dataset, Run
import fleet.models
from forms import PipelineSelectionForm
from pipeline.models import PipelineFamily
from exceptions import KeyError
from fleet.models import RunToProcessInput
from rest_framework.reverse import reverse

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
            self.response = HttpResponse(status=405)  # Method not allowed


def _filter_datasets(request):
    filters = json.loads(request.GET.get("filter_data"))
    try:
        cdt_pk = int(request.GET.get("compound_datatype"))
        query = Dataset.objects.filter(symbolicdataset__structure__compounddatatype=cdt_pk)
    except ValueError:
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


def _add_run_filter(runs, key, value):
    if key == 'active':
        recent_time = timezone.now() - timedelta(minutes=5)
        old_aborted_runs = fleet.models.ExceedsSystemCapabilities.objects.values(
            'runtoprocess_id').filter(runtoprocess__time_queued__lt=recent_time)
        return runs.filter(
            Q(run_id__isnull=True)|
            Q(run__end_time__isnull=True)|
            Q(run__end_time__gte=recent_time)).distinct().exclude(
            pk__in=old_aborted_runs)
    if key == 'name':
        runs_with_matching_inputs = RunToProcessInput.objects.filter(
            symbolicdataset__dataset__name__icontains=value).values(
                'runtoprocess_id')
        return runs.filter(
            Q(pipeline__family__name__icontains=value)|
            Q(id__in=runs_with_matching_inputs))
    if key in ('startafter', 'startbefore', 'endafter', 'endbefore'):
        t = timezone.make_aware(datetime.strptime(value, '%d %b %Y %H:%M'),
                                timezone.get_current_timezone())
        if key == 'startafter':
            return runs.filter(run__start_time__gte=t)
        if key == 'startbefore':
            return runs.filter(run__start_time__lte=t)
        if key == 'endafter':
            return runs.filter(run__end_time__gte=t)
        if key == 'endbefore':
            return runs.filter(run__end_time__lte=t)
    raise KeyError(key)


def load_status(request, rtp_id=None):
    """ Find all matching runs, and return a dict for each with the status.
    
    @param request: A web request with search parameters in the query string.
    * is_granted=true - For administrators, this limits the list to only include
        records that the user has been explicitly granted access to. For other
        users, this has no effect.
    * filters[n][key]=x&filters[n][val]=y - Apply different filters to the
        search for runs. n starts at 0 and increases by 1 for each added filter.
        Some filters just have a key and ignore the val value. The possible
        filters are listed below.
    * filters[n][key]=active - runs that are still running or recently finished.
    * filters[n][key]=name&filters[n][val]=match - runs where an input dataset
        name or the pipeline family name match the value (case insensitive)
    * filters[n][key]=startafter&filters[n][val]=DD+Mon+YYYY+HH:MM - runs that
        started after the given date and time.
    * filters[n][key]=startbefore&filters[n][val]=DD+Mon+YYYY+HH:MM - runs that
        started before the given date and time.
    * filters[n][key]=endafter&filters[n][val]=DD+Mon+YYYY+HH:MM - runs that
        ended after the given date and time.
    * filters[n][key]=endbefore&filters[n][val]=DD+Mon+YYYY+HH:MM - runs that
        ended before the given date and time.
    @param rtp_id: id of a RunToProcess to find.
    @return ([{'id': run_id, 'status': s, 'name': n, 'start': t, 'end': t}],
        has_more) where has_more is true if more runs matched the search
        criteria than were returned.
    @return ({ as above }, has_more) if rtp_id is not None
    """
    is_admin = request.GET.get('is_granted', '').lower() != 'true'
    runs = fleet.models.RunToProcess.filter_by_user(
        request.user,
        is_admin=is_admin).order_by('-time_queued')
    
    i = 0
    while True:
        key = request.GET.get('filters[{}][key]'.format(i))
        if key is None:
            break
        value = request.GET.get('filters[{}][val]'.format(i))
        runs = _add_run_filter(runs, key, value)
        i += 1

    if rtp_id is not None:
        runs = runs.filter(id=rtp_id)

    runs = runs.prefetch_related('pipeline__steps',
                                 'run__runsteps__log',
                                 'run__runsteps__pipelinestep__cables_in',
                                 'run__runsteps__pipelinestep__transformation__method',
                                 'run__runsteps__pipelinestep__transformation__pipeline',
                                 'run__pipeline__outcables__poc_instances__run',
                                 'run__pipeline__outcables__poc_instances__log',
                                 'run__pipeline__steps')

    if rtp_id is not None:
        run = runs.first()
        if run is not None:
            return run.get_run_progress(True), False
        return None, False

    LIMIT = 30
    has_more = False
    report = []
    for i, run in enumerate(runs[:LIMIT + 1]):
        if i == LIMIT:
            has_more = True
            break
        progress = run.get_run_progress()
        progress['url'] = reverse('runtoprocess-detail',
                                  kwargs={'pk': run.pk},
                                  request=request)
        progress['removal_plan'] = reverse('runtoprocess-removal-plan',
                                           kwargs={'pk': run.pk},
                                           request=request)
        report.append(progress)
    return report, has_more


def _is_status_changed(runs, request):
    for i, run in enumerate(runs):
        prefix = 'previous[runs][{}]'.format(i)
        previous_run_id = request.GET.get(prefix + '[id]')
        previous_run_id = previous_run_id and int(previous_run_id)
        if (run.get('id') != previous_run_id or
            run.get('end') != request.GET.get(prefix + '[end]') or 
            run['status'] != request.GET.get(prefix + '[status]')):
            return True
    
    return False

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
