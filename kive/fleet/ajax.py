from datetime import timedelta, datetime

from django.core.exceptions import ValidationError
from django.db import transaction
from django.db.models import Q
from django.utils import timezone
from rest_framework import permissions
from rest_framework.decorators import detail_route, list_route
from rest_framework.exceptions import APIException
from rest_framework.response import Response
from rest_framework.reverse import reverse

import fleet
from fleet.models import RunToProcess, RunToProcessInput
from fleet.serializers import RunToProcessSerializer,\
    RunToProcessOutputsSerializer
from kive.ajax import IsGrantedReadCreate, RemovableModelViewSet, StandardPagination
from librarian.models import SymbolicDataset
from sandbox.forms import InputSubmissionForm, RunSubmissionForm
from sandbox.views import RunSubmissionError


class RunToProcessViewSet(RemovableModelViewSet):
    """ Runs or requests to start runs
    
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
    """
    
    queryset = RunToProcess.objects.all()
    serializer_class = RunToProcessSerializer
    permission_classes = (permissions.IsAuthenticated, IsGrantedReadCreate)
    pagination_class = StandardPagination

    def create(self, request):
        """
        Creates a new run from a pipeline.

        Request parameters are:

        * pipeline - the pipeline id
        * input_1, input_2, etc. - the *symbolic* dataset ids to use as inputs
        """
        try:
            with transaction.atomic():
                dummy_rtp = RunToProcess(user=request.user)
                rsf = RunSubmissionForm(request.POST, instance=dummy_rtp)

                try:
                    rsf_good = rsf.is_valid()
                except ValidationError as e:
                    rsf.add_error(None, e)
                    rsf_good = False

                curr_pipeline = rsf.cleaned_data["pipeline"]
                if not rsf_good:
                    return Response({'errors': rsf.errors}, status=400)

                # All inputs are good, so save then create the inputs
                rtp = rsf.save()
                for i in range(1, curr_pipeline.inputs.count()+1):
                    curr_input_form = InputSubmissionForm({"input_pk": request.POST.get("input_{}".format(i))})
                    if not curr_input_form.is_valid():
                        return Response({'errors': curr_input_form.errors}, status=400)

                    # Check that the chosen SD is usable.
                    curr_SD = SymbolicDataset.objects.get(pk=curr_input_form.cleaned_data["input_pk"])
                    try:
                        rtp.validate_restrict_access([curr_SD])
                    except ValidationError as e:
                        return Response({'errors': [str(e)]}, status=400)
                    rtp.inputs.create(symbolicdataset=curr_SD, index=i)

                try:
                    rtp.clean()

                except ValidationError as e:
                    return Response({'errors': [str(e)]}, status=400)

        except RunSubmissionError as e:
            return Response({'errors': [str(e)]}, status=400)

        return Response(RunToProcessSerializer(rtp, context={'request': request}).data)
    
    @list_route(methods=['get'], suffix='Status List')
    def status(self, request):
        runs = self.get_queryset().order_by('-time_queued')

        i = 0
        while True:
            key = request.GET.get('filters[{}][key]'.format(i))
            if key is None:
                break
            value = request.GET.get('filters[{}][val]'.format(i), '')
            runs = self._add_run_filter(runs, key, value)
            i += 1

        runs = self._build_rtp_prefetch(runs)

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

        return Response({'runs': report, 'has_more': has_more})

    @detail_route(methods=['get'], suffix='Status')
    def run_status(self, request, pk=None):
        runs = fleet.models.RunToProcess.objects.filter(pk=pk)
        runs = self._build_rtp_prefetch(runs)
        run = runs.first()

        progress = None
        if run is not None:
            progress = run.get_run_progress(True)

        return Response(progress)

    @detail_route(methods=['get'], suffix='Outputs')
    def run_outputs(self, request, pk=None):
        rtp = self.get_object()
        return Response(RunToProcessOutputsSerializer(
            rtp,
            context={'request': request}).data)

    @staticmethod
    def _build_rtp_prefetch(runs):
        return runs.prefetch_related('pipeline__steps',
                                     'run__runsteps__log',
                                     'run__runsteps__pipelinestep__cables_in',
                                     'run__runsteps__pipelinestep__transformation__method',
                                     'run__runsteps__pipelinestep__transformation__pipeline',
                                     'run__pipeline__outcables__poc_instances__run',
                                     'run__pipeline__outcables__poc_instances__log',
                                     'run__pipeline__steps')

    @staticmethod
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
        if key == "user":
            return runs.filter(user__username__icontains=value)
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
        raise APIException('Unknown filter key: {}'.format(key))
