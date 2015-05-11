import json

from django.contrib.auth.decorators import login_required
from django.core.exceptions import ValidationError
from django.db import transaction
from django.db.models import Count
from django.http import HttpResponse, Http404, HttpResponseRedirect
from django.template import loader, RequestContext

import archive.models
import fleet.models
import librarian.models
from metadata.models import KiveUser
import pipeline.models
from portal.views import admin_check
from sandbox.forms import PipelineSelectionForm, InputSubmissionForm, RunSubmissionForm
from fleet.serializers import RunToProcessOutputsSerializer
from fleet.models import RunToProcess

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
    context = RequestContext(request, {
        "pipeline_forms": _prepare_pipeline_selection_forms(request.user),
        "error_msg": ""})
    return HttpResponse(template.render(context))


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
                    "run_submission_form": rsf,
                    "input_error_msg": ""})
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


@login_required
def runs(request):
    """Display all active runs for this user."""
    context = RequestContext(request)
    context['is_user_admin'] = admin_check(request.user)
    template = loader.get_template("sandbox/runs.html")
    return HttpResponse(template.render(context))

@login_required
def view_results(request, rtp_id):
    """View outputs from a pipeline run."""
    template = loader.get_template("sandbox/view_results.html")
    context = RequestContext(request)
    context['is_user_admin'] = admin_check(request.user)
    context['rtp_id'] = rtp_id
    
    four_oh_four = False
    try:
        rtp = fleet.models.RunToProcess.objects.get(id=rtp_id)
        if not rtp.can_be_accessed(request.user):
            four_oh_four = True
    except RunToProcess.DoesNotExist:
        four_oh_four = True

    if four_oh_four:
        raise Http404("ID {} does not exist or is not accessible".format(id))

    context["outputs"] = json.dumps(RunToProcessOutputsSerializer(
        rtp,
        context={ 'request': request }).data)
    return HttpResponse(template.render(context))

@login_required
def view_run(request, rtp_id, md5=None):
    rtp = fleet.models.RunToProcess.objects.get(id=rtp_id)

    template = loader.get_template("sandbox/view_run.html")
    context = RequestContext(request, {'rtp_id': rtp_id, 'md5': md5, 'pipeline': rtp.pipeline})
    return HttpResponse(template.render(context))
