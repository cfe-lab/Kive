from django.template import loader, RequestContext
from django.http import HttpResponse, Http404, HttpResponseRedirect
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.core.exceptions import ValidationError

import librarian.models
import archive.models
import pipeline.models
from sandbox.forms import PipelineSelectionForm, InputSubmissionForm, RunSubmissionForm
import fleet.models


def _prepare_pipeline_selection_forms(user):
    families = pipeline.models.PipelineFamily.filter_by_user(user)
    forms = []
    for family in families:
        if len(family.complete_members) > 0:
            forms.append(PipelineSelectionForm(pipeline_family_pk=family.pk))
    return forms


@login_required
def choose_pipeline(request):
    """Create forms for all Pipelines in Shipyard."""
    template = loader.get_template("sandbox/choose_pipeline.html")
    context = RequestContext(request, {"pipeline_forms": _prepare_pipeline_selection_forms(request.user)})
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
    return HttpResponseRedirect("/runs")


@login_required
def runs(request):
    """Display all active runs for this user."""
    context = RequestContext(request)
    template = loader.get_template("sandbox/runs.html")
    return HttpResponse(template.render(context))


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
        methodoutput = runstep.log.methodoutput
        outputs.append((runstep.pipelinestep,
                        'Standard out',
                        methodoutput.output_log.size,
                        runstep.log.end_time,
                        "../../stdout_view/{}".format(methodoutput.id),
                        "../../stdout_download/{}".format(methodoutput.id)))
        outputs.append(('',
                        'Standard error',
                        methodoutput.error_log.size,
                        runstep.log.end_time,
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
