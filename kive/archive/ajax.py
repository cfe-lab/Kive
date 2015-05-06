import json

from django.contrib.auth.decorators import login_required, user_passes_test
from django.http.response import Http404, HttpResponse
from django.views.decorators.http import require_POST

from archive.models import Dataset, MethodOutput, Run
from librarian.models import SymbolicDataset
from portal.views import admin_check
from archive.views import api_get_datasets
from metadata.models import deletion_order

from rest_framework import viewsets, permissions, mixins
from rest_framework.decorators import detail_route, list_route
from rest_framework.response import Response

from archive.serializers import DatasetSerializer
from archive.forms import DatasetForm
from archive.views import _build_download_response

from kive.ajax import IsDeveloperOrGrantedReadOnly, RemovableModelViewSet, RedactModelMixin

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


class DatasetViewSet(RemovableModelViewSet, RedactModelMixin):
    queryset = Dataset.objects.all()
    serializer_class = DatasetSerializer
    permission_classes = (permissions.IsAuthenticated, IsDeveloperOrGrantedReadOnly)

    def create(self, request):
        """
        Override the create function, this allows us to POST to
        this viewset, but also provides us with an incorrect form on
        the front end.
        """
        single_dataset_form = DatasetForm(request.POST, request.FILES, user=request.user, prefix="")
        symdataset = single_dataset_form.create_dataset(request.user) if single_dataset_form.is_valid() else None

        if symdataset is None:
            return Response({'errors': single_dataset_form.errors}, status=400)
        return Response(DatasetSerializer(symdataset.dataset).data, status=201)

    def patch_object(self, request, pk=None):
        return Response(DatasetSerializer(self.get_object(), context={'request': request}).data)

    @detail_route(methods=['get'])
    def download(self, request, pk=None):
        """
        """
        accessible_SDs = SymbolicDataset.filter_by_user(request.user)
        dataset = self.get_object()

        if dataset.symbolicdataset not in accessible_SDs:
            return Response(None, status=404)

        return _build_download_response(dataset.dataset_file)





@login_required
@user_passes_test(admin_check)
@require_POST
def remove_run(request, run_id):
    """
    Redact the file associated with the dataset.
    """
    if not request.is_ajax():
        raise Http404

    try:
        run = Run.objects.get(pk=run_id)
    except Run.DoesNotExist:
        raise Http404("Run id {} cannot be accessed".format(run_id))
    
    if _is_dry_run(request):
        plan = run.build_removal_plan()
        keys = deletion_order[:]
        keys.remove('ExecRecords')
        summary = ""
        for key in keys:
            count = len(plan[key])
            if count:
                if summary:
                    summary += ', '
                summary += '{} {}'.format(count, key)
        summary = "This will remove " + summary + "."
        return HttpResponse(json.dumps(summary), content_type=JSON_CONTENT_TYPE)
    
    run.remove()

    return HttpResponse()

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
    
    if _is_dry_run(request):
        plan = dataset.symbolicdataset.build_redaction_plan()
        runs = {exec_record.generator.record.top_level_run for exec_record in plan['ExecRecords']}

        summary = "This will redact {} data sets and {} logs from {} runs.".format(
            len(plan['SymbolicDatasets']),
            len(plan['OutputLogs']) + len(plan['ErrorLogs']),
            len(runs))
        return HttpResponse(json.dumps(summary), content_type=JSON_CONTENT_TYPE)
    
    dataset.symbolicdataset.redact()

    # FIXME: This is how we reload all the datasets if we're not redacting
    # from the run result page. We should try to do this in a more clean way?
    if request.POST.get('datasets') == 'true':
        return api_get_datasets(request, -1)

    return _build_run_outputs_response(dataset.created_by.parent_run)


@login_required
@user_passes_test(admin_check)
@require_POST
def dataset_remove(request, dataset_id):
    """
    Redact the file associated with the dataset.
    """
    if not request.is_ajax():
        raise Http404

    try:
        dataset = Dataset.objects.get(pk=dataset_id)
    except Dataset.DoesNotExist:
        raise Http404("ID {} cannot be accessed".format(dataset_id))

    if _is_dry_run(request):
        plan = dataset.symbolicdataset.build_removal_plan()
        runs = {exec_record.generator.record.top_level_run for exec_record in plan['ExecRecords']}

        # summary = "This will remove {} data sets and {} logs from {} runs.".format(
        summary = "This will remove {} data sets and {} runs.".format(
            len(plan['SymbolicDatasets']),
            # len(plan['OutputLogs']) + len(plan['ErrorLogs']),
            len(runs))
        return HttpResponse(json.dumps(summary), content_type=JSON_CONTENT_TYPE)

    dataset.symbolicdataset.remove()

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
