import logging

from django.contrib.auth.decorators import login_required
from django.core.exceptions import ValidationError
from django.db import transaction
from django.http import HttpResponse, Http404, HttpResponseRedirect
from django.template import loader
from django.views.decorators.http import require_GET, require_POST
from django.contrib.auth.models import User, Group

from rest_framework.renderers import JSONRenderer

from archive.models import Dataset, Run
from archive.serializers import RunOutputsSerializer
from pipeline.models import Pipeline
from portal.views import admin_check
from sandbox.forms import InputSubmissionForm, RunSubmissionForm, RunDetailsForm


LOGGER = logging.getLogger(__name__)


@login_required
def choose_pipeline(request, error_message=''):
    """Create forms for all Pipelines in Shipyard."""
    template = loader.get_template("sandbox/choose_pipeline.html")
    context = {"error_msg": error_message}
    return HttpResponse(template.render(context, request))


@login_required
@require_GET
def choose_inputs(request):
    pipeline_pk = int(request.GET.get("pipeline"))
    return _choose_inputs_for_pipeline(request, pipeline_pk)


def _choose_inputs_for_pipeline(request,
                                pipeline_pk,
                                rsf=None,
                                input_error_message=''):
    """Load the input selection page."""
    context = {}

    template = loader.get_template("sandbox/choose_inputs.html")
    pipeline_qs = Pipeline.filter_by_user(request.user).filter(pk=pipeline_pk)

    pipeline = pipeline_qs.first()
    if pipeline is None:
        raise Http404("ID {} is not accessible".format(pipeline_pk))

    if rsf is None:
        rsf = RunSubmissionForm({"pipeline": pipeline}, pipeline_qs=pipeline_qs)

    context.update({"inputs": pipeline.inputs.order_by("dataset_idx"),
                    "run_submission_form": rsf,
                    "input_error_msg": input_error_message,
                    "pipeline": pipeline})
    return HttpResponse(template.render(context, request))


class RunSubmissionError(Exception):
    """ Exception used to roll back a run submission.
    
    Includes the error response to display to the user.
    """
    
    def __init__(self, response):
        self.response = response


@login_required
@require_POST
def run_pipeline(request):
    """Run a Pipeline.

    Request parameters are:

    * pipeline - the pipeline id
    * input_1, input_2, etc. - the dataset ids to use as inputs
    """

    try:
        with transaction.atomic():
            dummy_rtp = Run(user=request.user)
            rsf = RunSubmissionForm(request.POST, instance=dummy_rtp)
    
            try:
                rsf_good = rsf.is_valid()
            except ValidationError as e:
                rsf.add_error(None, e)
                rsf_good = False
    
            curr_pipeline = rsf.cleaned_data["pipeline"]
            if not rsf_good:
                if "pipeline" in rsf.cleaned_data:
                    # back to choose inputs, with form now including errors.
                    raise RunSubmissionError(_choose_inputs_for_pipeline(
                        request,
                        curr_pipeline.pk,
                        rsf))
                raise RunSubmissionError(choose_pipeline(request,
                                                         "Pipeline was invalid."))
    
            rtp = rsf.save()
            rtp.grant_from_json(rsf.cleaned_data["permissions"])
    
            # Now try and put together RunInputs from the specified inputs.
            for i in range(1, curr_pipeline.inputs.count()+1):
                curr_input_form = InputSubmissionForm({"input_pk": request.POST.get("input_{}".format(i))})
                if not curr_input_form.is_valid():
                    raise RunSubmissionError(_choose_inputs_for_pipeline(
                        request,
                        curr_pipeline.pk,
                        rsf,
                        "Input {} is invalid".format(i)))
    
                # Check that the chosen dataset is usable.
                dataset = Dataset.objects.get(pk=curr_input_form.cleaned_data["input_pk"])
                try:
                    rtp.validate_restrict_access([dataset])
                except ValidationError as e:
                    raise RunSubmissionError(_choose_inputs_for_pipeline(
                        request,
                        curr_pipeline.pk,
                        rsf,
                        e.messages))
    
                rtp.inputs.create(dataset=dataset, index=i)
    
            try:
                rtp.clean()
            except ValidationError as e:
                raise RunSubmissionError(_choose_inputs_for_pipeline(
                    request,
                    curr_pipeline.pk,
                    rsf))
    except RunSubmissionError as e:
        return e.response

    # Success -- redirect to the active runs view.
    return HttpResponseRedirect("/view_run/%d" % rtp.id)


@login_required
def runs(request):
    """Display all active runs for this user."""
    context = {
        "user": request.user,
        "is_user_admin": admin_check(request.user)
    }
    template = loader.get_template("sandbox/runs.html")
    return HttpResponse(template.render(context, request))


@login_required
def view_results(request, run_id):
    """View outputs from a pipeline run."""
    go_back_to_view = request.GET.get('back_to_view', None) == 'true'
    
    four_oh_four = False
    try:
        # If we're going to change the permissions, this will make it faster.
        # FIXME punch this up so it gets the top-level runs of the ExecRecords it reuses
        run = Run.objects.prefetch_related(
            "runsteps__RSICs__outputs__integrity_checks__usurper",
            "runsteps__outputs__integrity_checks__usurper",
            "runoutputcables__outputs__integrity_checks__usurper",
            "runsteps__execrecord__generator__record__runstep__run",
            "runsteps__RSICs__execrecord__generator__record__runsic__runstep__run",
            "runoutputcables__execrecord__generator__record__runoutputcable__run"
        ).get(pk=run_id)

        if run.is_subrun():
            four_oh_four = True

        if not run.can_be_accessed(request.user) and not admin_check(request.user):
            four_oh_four = True
    except Run.DoesNotExist:
        four_oh_four = True
    if four_oh_four:
        raise Http404("ID {} does not exist or is not accessible".format(run_id))

    with transaction.atomic():
        run_complete = run.is_complete(use_cache=True)

    if request.method == "POST":
        # We don't allow changing anything until after the Run is finished.
        if not run_complete:
            # A RunSubmissionForm which we can use to make submission and editing easier.
            run_form = RunDetailsForm(
                users_allowed=User.objects.none(),
                groups_allowed=Group.objects.none(),
                initial={"name": run.name, "description": run.description}
            )
            run_form.add_error(None, "This run cannot be modified until it is complete.")

        else:
            # We are going to try and update this Run.  We don't bother restricting the
            # PermissionsField here; instead we will catch errors at the model validation
            # step.
            run_form = RunDetailsForm(
                request.POST,
                instance=run
            )
            try:
                if run_form.is_valid():
                    with transaction.atomic():
                        run.name = run_form.cleaned_data["name"]
                        run.description = run_form.cleaned_data["description"]
                        run.save()
                        run.increase_permissions_from_json(run_form.cleaned_data["permissions"])
                        run.clean()

                    if go_back_to_view:
                        return HttpResponseRedirect("/view_run/{}".format(run_id))
                    else:
                        return HttpResponseRedirect("/runs")

            except (AttributeError, ValidationError, ValueError) as e:
                LOGGER.exception(e.message)
                run_form.add_error(None, e)

    else:
        # A RunSubmissionForm which we can use to make submission and editing easier.
        run_form = RunSubmissionForm(
            users_allowed=User.objects.none(),
            groups_allowed=Group.objects.none(),
            initial={"name": run.name, "description": run.description}
        )

    template = loader.get_template("sandbox/view_results.html")

    context = {
        "run": run,
        "outputs": JSONRenderer().render(RunOutputsSerializer(run, context={'request': request}).data),
        "run_form": run_form,
        "is_complete": run_complete,
        "is_owner": run.user == request.user,
        "is_user_admin": admin_check(request.user),
        "back_to_view": go_back_to_view
    }

    return HttpResponse(template.render(context, request))


@login_required
def view_run(request, run_id, md5=None):
    run = Run.objects.get(pk=run_id)

    template = loader.get_template("sandbox/view_run.html")
    context = {
        'run': run,
        'md5': md5
    }
    return HttpResponse(template.render(context, request))
