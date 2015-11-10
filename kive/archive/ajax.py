from datetime import datetime, timedelta

from django.db import transaction
from django.db.models import Q
from django.core.exceptions import ValidationError as DjangoValidationError, PermissionDenied
from django.utils import timezone

from rest_framework import permissions, status
from rest_framework.decorators import detail_route, list_route
from rest_framework.exceptions import APIException
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from archive.serializers import DatasetSerializer, MethodOutputSerializer, RunSerializer,\
    RunProgressSerializer, RunOutputsSerializer
from archive.models import Run, RunInput, ExceedsSystemCapabilities, Dataset, MethodOutput,\
    summarize_redaction_plan
from archive.views import _build_download_response
from portal.views import admin_check

from kive.ajax import RemovableModelViewSet, RedactModelMixin, IsGrantedReadOnly, IsGrantedReadCreate,\
    StandardPagination, RemovableModelViewSet, CleanCreateModelMixin, SearchableModelMixin,\
    convert_validation

from librarian.models import SymbolicDataset

JSON_CONTENT_TYPE = 'application/json'


class DatasetViewSet(RemovableModelViewSet,
                     CleanCreateModelMixin,
                     RedactModelMixin):
    """ List and modify datasets.
    
    POST to the list to upload a new dataset, DELETE an instance to remove it
    along with all runs that produced or consumed it, or PATCH is_redacted=true
    on an instance to blank its contents along with any other instances or logs
    that used it as input.
    
    Query parameters for the list view:
    * page_size=n - limit the results and page through them
    * is_granted=true - For administrators, this limits the list to only include
        records that the user has been explicitly granted access to. For other
        users, this has no effect.
    * filters[n][key]=x&filters[n][val]=y - Apply different filters to the
        search. n starts at 0 and increases by 1 for each added filter.
        Some filters just have a key and ignore the val value. The possible
        filters are listed below.
    * filters[n][key]=smart&filters[n][val]=match - name or description contain
        the value (case insensitive)
    * filters[n][key]=name&filters[n][val]=match - name contains the value (case
        insensitive)
    * filters[n][key]=description&filters[n][val]=match - description contains the value (case
        insensitive)
    * filters[n][key]=user&filters[n][val]=match - username of the creating user contains the value (case
        insensitive)
    * filters[n][key]=uploaded - only include datasets uploaded by users, not
        generated by pipeline runs.
    * filters[n][key]=user&filters[n][val]=match - username of the creating user contains the value (case
        insensitive)
    * filters[n][key]=createdafter&filters[n][val]=match - Dataset was created after this time/date
    * filters[n][key]=createdbefore&filters[n][val]=match - Dataset was created before this time/date
    * filters[n][key]=cdt&filters[n][val]=id - only include datasets with the
        compound datatype id, or raw type if id is missing.
    """
    queryset = Dataset.objects.all()
    serializer_class = DatasetSerializer
    permission_classes = (permissions.IsAuthenticated, IsGrantedReadCreate)
    pagination_class = StandardPagination

    def filter_granted(self, queryset):
        """ Filter a queryset to only include records explicitly granted.
        """
        return queryset.filter(
            symbolicdataset__in=SymbolicDataset.filter_by_user(self.request.user))

    def filter_queryset(self, queryset):
        queryset = super(DatasetViewSet, self).filter_queryset(queryset)
        i = 0
        while True:
            key = self.request.GET.get('filters[{}][key]'.format(i))
            if key is None:
                break
            value = self.request.GET.get('filters[{}][val]'.format(i), '')
            queryset = self._add_filter(queryset, key, value)
            i += 1
        return queryset
    
    def _add_filter(self, queryset, key, value):
        if key == 'smart':
            return queryset.filter(Q(name__icontains=value) |
                                   Q(description__icontains=value))
        if key == 'name':
            return queryset.filter(name__icontains=value)
        if key == 'description':
            return queryset.filter(description__icontains=value)
        if key == "user":
            return queryset.filter(symbolicdataset__user__username__icontains=value)
        if key == 'uploaded':
            return queryset.filter(created_by=None)
        if key == 'cdt':
            if value == '':
                return queryset.filter(symbolicdataset__structure__isnull=True)
            else:
                return queryset.filter(
                    symbolicdataset__structure__compounddatatype=value)

        if key in ('createdafter', 'createdbefore'):
            t = timezone.make_aware(datetime.strptime(value, '%d %b %Y %H:%M'),
                                    timezone.get_current_timezone())
            if key == 'createdafter':
                return queryset.filter(date_created__gte=t)
            if key == 'createdbefore':
                return queryset.filter(date_created__lte=t)

        raise APIException('Unknown filter key: {}'.format(key))

    @transaction.atomic
    def perform_create(self, serializer):
        try:
            new_dataset = serializer.save()
            new_dataset.clean()
        except DjangoValidationError as ex:
            raise convert_validation(ex)

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


class RunPermission(permissions.BasePermission):
    """
    Custom permission for Runs.

    All users should be allowed to create Runs.  Users should be allowed to
    rerun any Run visible to them.  However, Runs may only be stopped by
    administrators or their owner.
    """
    def has_permission(self, request, view):
        return (admin_check(request.user) or
                request.method in permissions.SAFE_METHODS or
                request.method == "POST" or
                request.method == "PATCH")

    def has_object_permission(self, request, view, obj):
        if admin_check(request.user):
            return True
        if not obj.can_be_accessed(request.user):
            return False
        if request.method == "PATCH":
            return obj.user == request.user
        return request.method in permissions.SAFE_METHODS


class RunViewSet(CleanCreateModelMixin, RemovableModelViewSet,
                 SearchableModelMixin):
    """ Runs, including those that haven't started yet

    Query parameters for the list view:

    * is_granted=true - For administrators, this limits the list to only include
        records that the user has been explicitly granted access to. For other
        users, this has no effect.

    Alternate list view: runs/status/
    This will return status summaries for all the requested runs, up to a limit.
    It also returns has_more, which is true if more runs matched the search
    criteria than the limit. Query parameters:

    * is_granted - same as above
    * filters[n][key]=x&filters[n][val]=y - Apply different filters to the
        search for runs. n starts at 0 and increases by 1 for each added filter.
        Some filters just have a key and ignore the val value. The possible
        filters are listed below.
    * filters[n][key]=active - runs that are still running or recently finished.
    * filters[n][key]=name&filters[n][val]=match - runs whose display name matches
        the value (case insensitive).  This either means: the Run's assigned name, if
        it has one; or the Pipeline name and/or the first input Dataset's name.
    * filters[n][key]=user&filters[n][val]=match - runs created by the specified user
    * filters[n][key]=startafter&filters[n][val]=DD+Mon+YYYY+HH:MM - runs that
        started after the given date and time.
    * filters[n][key]=startbefore&filters[n][val]=DD+Mon+YYYY+HH:MM - runs that
        started before the given date and time.
    * filters[n][key]=endafter&filters[n][val]=DD+Mon+YYYY+HH:MM - runs that
        ended after the given date and time.
    * filters[n][key]=endbefore&filters[n][val]=DD+Mon+YYYY+HH:MM - runs that
        ended before the given date and time.

    Parameter for a PATCH:
    * is_stop_requested(=true) - the Run is marked for stopping.
    """

    queryset = Run.objects.all()
    serializer_class = RunSerializer
    permission_classes = (permissions.IsAuthenticated, RunPermission)
    pagination_class = StandardPagination

    # Special pagination for the status list route.
    status_pagination_class = StandardPagination
    status_serializer_class = RunProgressSerializer

    @list_route(methods=['get'], suffix='Status List')
    def status(self, request):
        runs = self.get_queryset().order_by('-time_queued')
        runs = self.apply_filters(runs)
        runs = self._build_run_prefetch(runs)

        if not hasattr(self, "_status_paginator"):
            self._status_paginator = self.status_pagination_class()

        page = self._status_paginator.paginate_queryset(runs, request, view=self.status)
        if page is not None:
            status_serializer = self.status_serializer_class(page, many=True, context={"request": request})
            return self._status_paginator.get_paginated_response(status_serializer.data)

        # If we aren't using pagination, use the bare serializer.
        bare_serializer = self.status_serializer_class(runs, many=True, context={"request": request})
        return Response(bare_serializer.data)

    @detail_route(methods=['get'], suffix='Status')
    def run_status(self, request, pk=None):
        runs = Run.objects.filter(pk=pk)
        runs = self._build_run_prefetch(runs)
        run = runs.first()

        progress = None
        if run is not None:
            progress = run.get_run_progress(True)

        return Response(progress)

    @detail_route(methods=['get'], suffix='Outputs')
    def run_outputs(self, request, pk=None):
        run = self.get_object()
        return Response(RunOutputsSerializer(
            run,
            context={'request': request}).data)

    def patch_object(self, request, pk=None):
        return Response(RunSerializer(self.get_object(), context={'request': request}).data)

    def partial_update(self, request, pk=None):
        """
        Add PATCH functionality to this view.

        This is used for stopping runs.
        """
        is_stop_requested = request.data.get("is_stop_requested", False)
        if is_stop_requested:
            run = self.get_object()

            if request.user == run.user or admin_check(request.user):
                with transaction.atomic():
                    run.stopped_by = request.user
                    run.save()
            else:
                raise PermissionDenied

        return self.patch_object(request, pk)

    @staticmethod
    def _build_run_prefetch(runs):
        return runs.prefetch_related('pipeline__steps',
                                     'runsteps__log',
                                     'runsteps__pipelinestep__cables_in',
                                     'runsteps__pipelinestep__transformation__method',
                                     'runsteps__pipelinestep__transformation__pipeline',
                                     'pipeline__outcables__poc_instances__run',
                                     'pipeline__outcables__poc_instances__log',
                                     'pipeline__steps')

    @staticmethod
    def _add_filter(queryset, key, value):
        if key == 'active':
            recent_time = timezone.now() - timedelta(minutes=5)
            old_aborted_runs = ExceedsSystemCapabilities.objects.values(
                'run_id').filter(run__time_queued__lt=recent_time)
            return queryset.filter(
                Q(end_time__isnull=True)|
                Q(end_time__gte=recent_time)
            ).distinct().exclude(
                pk__in=old_aborted_runs
            )
        if key == 'name':
            runs_with_matching_inputs = RunInput.objects.filter(
                symbolicdataset__dataset__name__icontains=value).values(
                    'run_id')
            return queryset.filter(
                Q(name__icontains=value)|
                (Q(name="") & (Q(pipeline__family__name__icontains=value)|
                               Q(id__in=runs_with_matching_inputs))))
        if key == "user":
            return queryset.filter(user__username__icontains=value)
        if key in ('startafter', 'startbefore', 'endafter', 'endbefore'):
            t = timezone.make_aware(datetime.strptime(value, '%d %b %Y %H:%M'),
                                    timezone.get_current_timezone())
            if key == 'startafter':
                return queryset.filter(start_time__gte=t)
            if key == 'startbefore':
                return queryset.filter(start_time__lte=t)
            if key == 'endafter':
                return queryset.filter(end_time__gte=t)
            if key == 'endbefore':
                return queryset.filter(end_time__lte=t)
        raise APIException('Unknown filter key: {}'.format(key))
