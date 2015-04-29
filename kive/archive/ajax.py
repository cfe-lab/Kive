import json

from django.contrib.auth.decorators import login_required, user_passes_test
from django.http.response import Http404, HttpResponse
from django.views.decorators.http import require_POST

from archive.models import Dataset, MethodOutput
from portal.views import admin_check
from archive.views import api_get_datasets

JSON_CONTENT_TYPE = 'application/json'

def _load_methodoutput(request, methodoutput_id):
    if not request.is_ajax():
        raise Http404

    try:
        return MethodOutput.objects.get(pk=methodoutput_id)
    except Dataset.DoesNotExist:
        raise Http404(
            "Method output {} cannot be accessed".format(methodoutput_id))

def _build_run_outputs_response(run):
    return HttpResponse(
        json.dumps([output.__dict__ for output in run.get_output_summary()]),
        content_type=JSON_CONTENT_TYPE)

def _is_dry_run(request):
    return request.POST.get('dry_run') == 'true'

@login_required
@user_passes_test(admin_check)
@require_POST
def dataset_redact(request, dataset_id):
    """
    Redact the file associated with the dataset.
    """
    if not request.is_ajax():
        raise Http404

    try:
        dataset = Dataset.objects.get(pk=dataset_id)
    except Dataset.DoesNotExist:
        raise Http404("ID {} cannot be accessed".format(dataset_id))
    
    is_dry_run = _is_dry_run(request)
    if is_dry_run:
        #TODO: do an actual dry run once it's fixed.
        summary = "This will redact ? data sets, ? logs, and ? runs."
        return HttpResponse(json.dumps(summary), content_type=JSON_CONTENT_TYPE)
    
    dataset.symbolicdataset.redact(dry_run=is_dry_run)

    # FIXME: This is how we reload all the datasets if we're not redacting
    # from the run result page. We should try to do this in a more clean way?
    if request.POST.get('datasets') == 'true':
        return api_get_datasets(request, -1)

    return _build_run_outputs_response(dataset.created_by.parent_run)

@login_required
@user_passes_test(admin_check)
@require_POST
def stdout_redact(request, methodoutput_id):
    methodoutput = _load_methodoutput(request, methodoutput_id)
    if _is_dry_run(request):
        summary = "This will redact 1 log."
        return HttpResponse(json.dumps(summary), content_type=JSON_CONTENT_TYPE)
    
    methodoutput.redact_output_log()
    return _build_run_outputs_response(methodoutput.execlog.record.parent_run)

@login_required
@user_passes_test(admin_check)
@require_POST
def stderr_redact(request, methodoutput_id):
    methodoutput = _load_methodoutput(request, methodoutput_id)
    if _is_dry_run(request):
        summary = "This will redact 1 log."
        return HttpResponse(json.dumps(summary), content_type=JSON_CONTENT_TYPE)
    
    methodoutput.redact_error_log()
    return _build_run_outputs_response(methodoutput.execlog.record.parent_run)
