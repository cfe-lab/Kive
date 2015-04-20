from rest_framework.authentication import SessionAuthentication, BasicAuthentication, TokenAuthentication
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status as rf_status

from pipeline.serializers import PipelineFamilySerializer, PipelineSerializer
from fleet.serializers import RunToProcessSerializer
from archive.serializers import DatasetSerializer

from django.template import loader, RequestContext
from django.http import HttpResponse, Http404, HttpResponseRedirect
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.core.exceptions import ValidationError
from django.core.urlresolvers import reverse

import librarian.models
import archive.models
import pipeline.models
from sandbox.forms import PipelineSelectionForm, InputSubmissionForm, RunSubmissionForm
import fleet.models
from django.db.models import Count
from metadata.models import KiveUser


@api_view(['GET'])
@authentication_classes((SessionAuthentication, BasicAuthentication, TokenAuthentication))
@permission_classes((IsAuthenticated,))
def api_pipelines_home(request):
    pipeline_dir = {
        'directory': {
            name: reverse(name) for name in [
                'api_pipelines_get',
                'api_pipelines_startrun',
                'api_pipelines_get_the_runs']
        }
    }
    return Response(pipeline_dir)


def _prepare_pipeline_selection_forms(user):
    user = KiveUser.kiveify(user)
    families = pipeline.models.PipelineFamily.objects\
        .annotate(member_count=Count('members'))\
        .filter(user.access_query(), member_count__gt=0)
    return [PipelineSelectionForm(pipeline_family_pk=f.pk) for f in families]


@login_required
def choose_pipeline(request):
    """Create forms for all Pipelines in Shipyard."""
    template = loader.get_template("sandbox/choose_pipeline.html")
    context = RequestContext(request, {"pipeline_forms": _prepare_pipeline_selection_forms(request.user)})
    return HttpResponse(template.render(context))


@api_view(['GET'])
@authentication_classes((SessionAuthentication, BasicAuthentication, TokenAuthentication))
@permission_classes((IsAuthenticated,))
def api_get_pipelines(request, page=0):
    pagesize = 1
    page = int(page)

    user = KiveUser.kiveify(request.user)
    families = pipeline.models.PipelineFamily.objects\
        .annotate(member_count=Count('members'))\
        .filter(user.access_query(), member_count__gt=0)[page*pagesize:(page+1)*pagesize]

    next_page = None
    if len(families) == pagesize:
        next_page = reverse('api_pipelines_get_page', kwargs={'page': page+1})

    pipelines = {
        'next_page': next_page,
        'families': PipelineFamilySerializer(families, many=True).data
    }

    return Response(pipelines)


def _assemble_inputs(pipeline, user):
    """
    Find all compatible datasets for each pipeline input.
    """
    # FIXME make this return a FormSet here!
    input_data = []
    for curr_input in pipeline.inputs.order_by("dataset_idx"):
        viewable_SDs = librarian.models.SymbolicDataset.filter_by_user(user)
        query = archive.models.Dataset.objects.filter(symbolicdataset__in=viewable_SDs).order_by(
            "-date_created")
        if curr_input.is_raw():
            query = query.filter(symbolicdataset__structure__isnull=True)
        else:
            compound_datatype = curr_input.get_cdt()
            query = query.filter(
                symbolicdataset__structure__compounddatatype=compound_datatype)
        count = query.count()
        datasets = query[:10]
        input_data.append((curr_input, datasets, count))

    return input_data


@login_required
def choose_inputs(request):
    """Load the input selection page."""
    context = RequestContext(request)

    if request.method != "GET":
        # This isn't allowed!
        return HttpResponse(status=405)

    template = loader.get_template("sandbox/choose_inputs.html")
    pipeline_pk = int(request.GET.get("pipeline"))
    pipeline_qs = pipeline.models.Pipeline.filter_by_user(request.user).filter(pk=pipeline_pk)
    if not pipeline_qs.exists():
        raise Http404("ID {} is not accessible".format(pipeline_pk))

    rsf = RunSubmissionForm({"pipeline": pipeline_qs.first()}, pipeline_qs=pipeline_qs)

    context.update({"input_data": _assemble_inputs(pipeline_qs.first(), request.user),
                    "run_submission_form": rsf})
    return HttpResponse(template.render(context))


class RunSubmissionError(Exception):
    pass


@login_required
def run_pipeline(request):
    """Run a Pipeline.

    Request parameters are:

    * pipeline - the pipeline id
    * input_1, input_2, etc. - the *symbolic* dataset ids to use as inputs
    """
    context = RequestContext(request)
    if request.method != "POST":
        return HttpResponse(status=405)

    print request.POST
    try:
        # If we need to bail, in most cases we will use this template.
        template = loader.get_template("sandbox/choose_inputs.html")
        with transaction.atomic():
            dummy_rtp = fleet.models.RunToProcess(user=request.user)
            rsf = RunSubmissionForm(request.POST, instance=dummy_rtp)

            try:
                rsf_good = rsf.is_valid()
            except ValidationError as e:
                rsf.add_error(None, e)
                rsf_good = False

            curr_pipeline = rsf.cleaned_data["pipeline"]
            if not rsf_good:
                if "pipeline" in rsf.cleaned_data:
                    # We go back to the choose inputs screen, with this form now annotated with errors.
                    context.update({"input_data": _assemble_inputs(curr_pipeline, request.user),
                                    "run_submission_form": rsf})
                else:
                    # Go back to the choose pipeline screen -- change the template.
                    template = loader.get_template("sandbox/choose_pipeline.html")
                    context.update({"pipeline_forms": _prepare_pipeline_selection_forms(request.user),
                                    "error_msg": "Pipeline was invalid"})
                # Raise an exception to break the transaction.
                raise RunSubmissionError()

            rtp = rsf.save()

            # Now try and put together RunToProcessInputs from the specified inputs.
            for i in range(1, curr_pipeline.inputs.count()+1):
                curr_input_form = InputSubmissionForm({"input_pk": request.POST.get("input_{}".format(i))})
                if not curr_input_form.is_valid():
                    context.update({"input_data": _assemble_inputs(curr_pipeline, request.user),
                                    "run_submission_form": rsf,
                                    "input_error_msg": "Input {} is invalid".format(i)})
                    raise RunSubmissionError()

                # Check that the chosen SD is usable.
                curr_SD = librarian.models.SymbolicDataset.objects.get(pk=curr_input_form.cleaned_data["input_pk"])
                try:
                    rtp.validate_restrict_access([curr_SD])
                except ValidationError as e:
                    context.update({"input_data": _assemble_inputs(curr_pipeline, request.user),
                                    "run_submission_form": rsf,
                                    "input_error_msg": e.messages})
                    raise RunSubmissionError()

                rtp.inputs.create(symbolicdataset=curr_SD, index=i)

            try:
                rtp.clean()
            except ValidationError as e:
                rsf.add_error(None, e)
                context.update({"input_data": _assemble_inputs(curr_pipeline, request.user),
                                "run_submission_form": rsf})
                raise RunSubmissionError()

    except RunSubmissionError:
        return HttpResponse(template.render(context))

    # Success -- redirect to the active runs view.
    return HttpResponseRedirect("/view_run/%d" % rtp.id)


@api_view(['POST'])
@authentication_classes((SessionAuthentication, BasicAuthentication, TokenAuthentication))
@permission_classes((IsAuthenticated,))
def api_run_pipeline(request):
    """Run a Pipeline.

    Request parameters are:

    * pipeline - the pipeline id
    * input_1, input_2, etc. - the *symbolic* dataset ids to use as inputs
    """
    rtp = None
    try:
        with transaction.atomic():
            dummy_rtp = fleet.models.RunToProcess(user=request.user)
            rsf = RunSubmissionForm(request.POST, instance=dummy_rtp)

            try:
                rsf_good = rsf.is_valid()
            except ValidationError as e:
                rsf.add_error(None, e)
                rsf_good = False

            curr_pipeline = rsf.cleaned_data["pipeline"]
            if not rsf_good:
                return Response({'errors': rsf.errors}, status=rf_status.HTTP_400_BAD_REQUEST)

            # All inputs are good, so save then create the inputs
            rtp = rsf.save()
            for i in range(1, curr_pipeline.inputs.count()+1):
                curr_input_form = InputSubmissionForm({"input_pk": request.POST.get("input_{}".format(i))})
                if not curr_input_form.is_valid():
                    return Response({'errors': curr_input_form.errors}, status=rf_status.HTTP_400_BAD_REQUEST)

                # Check that the chosen SD is usable.
                curr_SD = librarian.models.SymbolicDataset.objects.get(pk=curr_input_form.cleaned_data["input_pk"])
                try:
                    rtp.validate_restrict_access([curr_SD])
                except ValidationError as e:
                    return Response({'errors': [str(e)]}, status=rf_status.HTTP_400_BAD_REQUEST)
                rtp.inputs.create(symbolicdataset=curr_SD, index=i)

            try:
                rtp.clean()

            except ValidationError as e:
                return Response({'errors': [str(e)]}, status=rf_status.HTTP_400_BAD_REQUEST)

    except RunSubmissionError as e:
        return Response({'errors': [str(e)]}, status=rf_status.HTTP_400_BAD_REQUEST)

    resp = {
        'run': RunToProcessSerializer(rtp).data
    }
    return Response(resp)


@login_required
def runs(request):
    """Display all active runs for this user."""
    context = RequestContext(request)
    template = loader.get_template("sandbox/runs.html")
    return HttpResponse(template.render(context))


@api_view(['GET'])
@authentication_classes((SessionAuthentication, BasicAuthentication, TokenAuthentication))
@permission_classes((IsAuthenticated,))
def api_get_runs(request):
    from sandbox.ajax import _load_status

    all_runs, has_more = _load_status(request)
    resp = {
        'runs': runs,
        'has_more': has_more
    }
    return Response(resp)


@api_view(['GET'])
@authentication_classes((SessionAuthentication, BasicAuthentication, TokenAuthentication))
@permission_classes((IsAuthenticated,))
def api_poll_run_progress(request, rtp_id):
    from sandbox.ajax import _load_status
    run, _ = _load_status(request, rtp_id)
    resp = {
        'run': run,
        'results': reverse('api_pipelines_runresults', kwargs={'rtp_id': rtp_id}),
    }
    return Response(resp)


@login_required
def view_results(request, id):
    """View outputs from a pipeline run."""
    template = loader.get_template("sandbox/view_results.html")
    context = RequestContext(request)

    four_oh_four = False
    try:
        run = archive.models.Run.objects.get(pk=id)
        if not run.can_be_accessed(request.user):
            four_oh_four = True
    except archive.models.Run.DoesNotExist:
        four_oh_four = True

    if four_oh_four:
        raise Http404("ID {} does not exist or is not accessible".format(id))
    
    outputs = [] # [(step_name, output_name, size, date, view_url, down_url)]
    for i, outcable in enumerate(run.outcables_in_order):
        dataset = outcable.execrecord.execrecordouts.first().symbolicdataset.dataset
        outputs.append(((i == 0 and 'Run outputs' or ''),
                        outcable.pipelineoutputcable.dest,
                        dataset.dataset_file.size,
                        dataset.date_created,
                        "../../dataset_view/{}".format(dataset.id),
                        "../../dataset_download/{}".format(dataset.id)))
        
    for runstep in run.runsteps_in_order:
        execlog = runstep.execrecord.generator
        methodoutput = execlog.methodoutput
        outputs.append((runstep.pipelinestep,
                        'Standard out',
                        methodoutput.output_log.size,
                        execlog.end_time,
                        "../../stdout_view/{}".format(methodoutput.id),
                        "../../stdout_download/{}".format(methodoutput.id)))
        outputs.append(('',
                        'Standard error',
                        methodoutput.error_log.size,
                        execlog.end_time,
                        "../../stderr_view/{}".format(methodoutput.id),
                        "../../stderr_download/{}".format(methodoutput.id)))
        for output in runstep.execrecord.execrecordouts_in_order:
            dataset = output.symbolicdataset.dataset
            outputs.append(('',
                            output.generic_output,
                            dataset.dataset_file.size,
                            dataset.date_created,
                            "../../dataset_view/{}".format(dataset.id),
                            "../../dataset_download/{}".format(dataset.id)))
    context.update({"outputs": outputs})
    return HttpResponse(template.render(context))


@api_view(['GET'])
@authentication_classes((SessionAuthentication, BasicAuthentication, TokenAuthentication))
@permission_classes((IsAuthenticated,))
def api_get_run_results(request, rtp_id):
    four_oh_four = False
    try:
        rtp = fleet.models.RunToProcess.objects.get(id=rtp_id)
        run = rtp.run
        if not run.can_be_accessed(request.user):
            four_oh_four = True
    except archive.models.Run.DoesNotExist:
        four_oh_four = True

    if four_oh_four:
        raise Http404("ID {} does not exist or is not accessible".format(id))

    outputs = [oc.execrecord.execrecordouts.first().symbolicdataset.dataset for oc in run.outcables_in_order]
    resp = {
        'results': DatasetSerializer(outputs, many=True).data,
    }
    return Response(resp)


@login_required
def view_run(request, rtp_id, md5=None):
    rtp = fleet.models.RunToProcess.objects.get(id=rtp_id)

    template = loader.get_template("sandbox/view_run.html")
    context = RequestContext(request, {'rtp_id': rtp_id, 'md5': md5, 'pipeline': rtp.pipeline})
    return HttpResponse(template.render(context))