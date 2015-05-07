from django.db import transaction
from django.core.exceptions import ValidationError

from rest_framework import permissions
from rest_framework.decorators import detail_route, list_route
from rest_framework.response import Response

from archive.serializers import DatasetSerializer
from fleet.models import RunToProcess
from fleet.serializers import RunToProcessSerializer
from kive.ajax import IsDeveloperOrGrantedReadOnly, RemovableModelViewSet
from librarian.models import SymbolicDataset
from sandbox.ajax import load_status
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
    permission_classes = (permissions.IsAuthenticated, IsDeveloperOrGrantedReadOnly)

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
        runs, has_more = load_status(request)
        return Response({ 'runs': runs, 'has_more': has_more })

    @detail_route(methods=['get'], suffix='Status')
    def run_status(self, request, pk=None):
        run, _ = load_status(request, pk)
        return Response(run)

    @detail_route(methods=['get'], suffix='Results')
    def run_results(self, request, pk=None):
        rtp = self.get_object()

        if rtp.run is None:
            return Response({'errors': ['Run not found!']}, status=404)

        outputs = [oc.execrecord.execrecordouts.first().symbolicdataset.dataset
                   for oc in rtp.run.outcables_in_order if oc.execrecord is not None]

        return Response(DatasetSerializer(outputs, many=True, context={'request': request}).data)

