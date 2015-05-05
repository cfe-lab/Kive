from django.db import transaction
from django.core.exceptions import ValidationError

from rest_framework import permissions, mixins
from rest_framework.decorators import detail_route
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from kive.ajax import IsDeveloperOrGrantedReadOnly, RemovableModelViewSet

from sandbox.forms import PipelineSelectionForm, InputSubmissionForm, RunSubmissionForm

from sandbox.views import RunSubmissionError

from fleet.models import RunToProcess
from librarian.models import SymbolicDataset

from fleet.serializers import RunToProcessSerializer
from archive.serializers import DatasetSerializer


class RunToProcessViewSet(RemovableModelViewSet):
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

        resp = {
            'run': RunToProcessSerializer(rtp).data
        }
        return Response(resp)

    @detail_route(methods=['get'])
    def run_status(self, request, pk=None):
        from sandbox.ajax import _load_status
        run, _ = _load_status(request, pk)
        return Response(run)

    @detail_route(methods=['get'])
    def run_results(self, request, pk=None):
        rtp = self.get_object()

        if rtp.run is None:
            return Response({'errors': ['Run not found!']}, status=404)

        outputs = [oc.execrecord.execrecordouts.first().symbolicdataset.dataset
                   for oc in rtp.run.outcables_in_order if oc.execrecord is not None]

        return Response(DatasetSerializer(outputs, many=True, context={'request': request}).data)

