import json

from django.contrib.auth.decorators import login_required, user_passes_test
from django.http.response import Http404, HttpResponse
from django.views.decorators.http import require_POST
from rest_framework import permissions, status
from rest_framework.decorators import detail_route
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from archive.forms import DatasetForm
from archive.serializers import DatasetSerializer, MethodOutputSerializer
from archive.models import Dataset, MethodOutput, Run, summarize_redaction_plan
from archive.views import _build_download_response
from kive.ajax import IsDeveloperOrGrantedReadOnly, RemovableModelViewSet, RedactModelMixin,\
    IsGrantedReadOnly, IsGrantedReadCreate
from librarian.models import SymbolicDataset
from metadata.models import deletion_order
from portal.views import admin_check

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
    """ List and modify datasets.
    
    POST to the list to upload a new dataset, DELETE an instance to remove it
    along with all runs that produced or consumed it, or PATCH is_redacted=true
    on an instance to blank its contents along with any other instances or logs
    that used it as input.
    """
    queryset = Dataset.objects.all()
    serializer_class = DatasetSerializer
    permission_classes = (permissions.IsAuthenticated, IsGrantedReadCreate)

    def filter_granted(self, queryset):
        """ Filter a queryset to only include records explicitly granted.
        """
        return queryset.filter(
            symbolicdataset__in=SymbolicDataset.filter_by_user(self.request.user))

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
        return Response(DatasetSerializer(symdataset.dataset, context={'request': request}).data,  status=201)

    def patch_object(self, request, pk=None):
        return Response(DatasetSerializer(self.get_object(), context={'request': request}).data)

    @detail_route(methods=['get'])
    def download(self, request, pk=None):
        """
        """
        accessible_SDs = SymbolicDataset.filter_by_user(request.user)
        dataset = self.get_object()

        if dataset.symbolicdataset not in accessible_SDs:
            return Response(None, status=status.HTTP_404_NOT_FOUND)

        return _build_download_response(dataset.dataset_file)


class MethodOutputViewSet(ReadOnlyModelViewSet):
    """ List and redact method output records.
    
    PATCH output_redacted=true, error_redacted=true, or code_redacted=true on an
    instance to blank its output log, error log, or return code.
    """
    queryset = MethodOutput.objects.all()
    serializer_class = MethodOutputSerializer
    permission_classes = (permissions.IsAuthenticated, IsGrantedReadOnly)

    def patch_object(self, request, pk=None):
        return Response(MethodOutputSerializer(
            self.get_object(),
            context={'request': request}).data)

    def partial_update(self, request, pk=None):
        method_output = self.get_object()
        redactions = {'output_redacted': method_output.redact_output_log,
                      'error_redacted': method_output.redact_error_log,
                      'code_redacted': method_output.redact_return_code}
        
        unexpected_keys = set(request.DATA.keys()) - set(redactions.keys())
        if unexpected_keys:
            return Response(
                {'errors': ['Cannot update fields ' + ','.join(unexpected_keys)]},
                status=status.HTTP_400_BAD_REQUEST)
        for field, redact in redactions.iteritems():
            if request.DATA.get(field, False):
                redact()
        return self.patch_object(request, pk)

    @detail_route(methods=['get'])
    def output_redaction_plan(self, request, pk=None):
        execlog = self.get_object().execlog
        redaction_plan = execlog.build_redaction_plan(output_log=True,
                                                      error_log=False,
                                                      return_code=False)
        return Response(summarize_redaction_plan(redaction_plan))

    @detail_route(methods=['get'])
    def error_redaction_plan(self, request, pk=None):
        execlog = self.get_object().execlog
        redaction_plan = execlog.build_redaction_plan(output_log=False,
                                                      error_log=True,
                                                      return_code=False)
        return Response(summarize_redaction_plan(redaction_plan))

    @detail_route(methods=['get'])
    def code_redaction_plan(self, request, pk=None):
        execlog = self.get_object().execlog
        redaction_plan = execlog.build_redaction_plan(output_log=False,
                                                      error_log=False,
                                                      return_code=True)
        return Response(summarize_redaction_plan(redaction_plan))


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
