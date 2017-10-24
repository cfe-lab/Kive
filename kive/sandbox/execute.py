"""Code that is responsible for the execution of Pipelines."""

from collections import defaultdict
import logging
import os
import stat
import random
import tempfile
import time
import itertools
import pwd

from django.utils import timezone
from django.db import transaction, OperationalError, InternalError
from django.contrib.auth.models import User
from django.conf import settings

from archive.models import RunStep, Run, ExecLog, RunSIC, RunCable, RunComponent, RunOutputCable
from constants import dirnames, extensions, runcomponentstates
import file_access_utils
from librarian.models import Dataset, ExecRecord
import pipeline.models
from method.models import Method
from datachecking.models import IntegrityCheckLog
from fleet.exceptions import StopExecution
from file_access_utils import copy_and_confirm, FileCreationError

logger = logging.getLogger("Sandbox")

sandbox_prefix = "user{}_run{}_"
# This is used by the fleet Manager when cleaning up.
sandbox_glob = sandbox_prefix.format("*", "*") + "*"


class Sandbox:
    """
    A Sandbox is the environment in which a Pipeline is run. It contains
    all the information necessary to run the Pipeline, including the code
    for all steps of the Pipeline, and the data to feed in. The Sandbox keeps
    track of a single Run of the Pipeline it was created with.

    Note that Sandboxes are single-use; that is, a Pipeline may only be run
    once in a Sandbox. To run the same Pipeline again, you must create a new
    Sandbox.
    """
    # dataset_fs_map: maps Dataset to a FS path: the path where a data
    # file would be if it were created (Whether or not it is there)
    # If the path is None, the Dataset is on the DB.

    # socket_map: maps (run, generator, socket) to Datasets.
    # A generator is a cable, or a pipeline step. A socket is a TI/TO.
    # If the generator is none, the socket is a pipeline input.
    # This will be used to look up inputs when running a pipeline.

    # ps_map: maps PS to (path, RunStep of PS): the path tells you
    # the directory that the PS would have been run in
    # (whether or not it was): the RunStep tells you what inputs are
    # needed (Which in turn will lead back to an dataset_fs_map lookup),
    # and allows you to fill it in on recovery.

    # queue_for_processing: list of tasks that are ready to be processed, i.e.
    # all of the required inputs are available and ready to go.

    # step_execute_info: table of RunStep "bundles" giving all the information
    # necessary to process a RunStep.

    # cable_map maps cables to ROC/RSIC.

    def __init__(self, run):
        """
        Sets up a sandbox environment to run a Pipeline: space on
        the file system, along with dataset_fs_map/socket_map/etc.

        INPUTS
        run           A Run object to fill in (e.g. if we're starting this using the fleet);

        PRECONDITIONS
        run.inputs must have real data
        """
        if settings.KIVE_SANDBOX_WORKER_ACCOUNT:
            pwd_info = pwd.getpwnam(settings.KIVE_SANDBOX_WORKER_ACCOUNT)
            self.uid = pwd_info.pw_uid
            self.gid = pwd_info.pw_gid
        else:
            # get our own current uid/hid
            self.uid = os.getuid()
            self.gid = os.getgid()

        self.run = run
        user = run.user
        my_pipeline = run.pipeline
        inputs = [x.dataset for x in run.inputs.order_by("index")]
        sandbox_path = run.sandbox_path

        self.logger = logger
        # logging.getLogger(self.__class__.__name__)
        self.user = user
        self.pipeline = my_pipeline
        self.inputs = inputs
        self.dataset_fs_map = {}
        self.socket_map = {}
        self.cable_map = {}
        self.ps_map = {}
        self.pipeline.check_inputs(self.inputs)

        # Determine a sandbox path, and input/output directories for
        # top-level Pipeline.
        self.sandbox_path = sandbox_path or tempfile.mkdtemp(
            prefix=sandbox_prefix.format(self.user, self.run.pk),
            dir=file_access_utils.create_sandbox_base_path())

        self.run.sandbox_path = self.sandbox_path
        self.run.save()

        in_dir = os.path.join(self.sandbox_path, dirnames.IN_DIR)
        self.out_dir = os.path.join(self.sandbox_path, dirnames.OUT_DIR)

        self.logger.debug("initializing maps")
        for i, pipeline_input in enumerate(inputs, start=1):
            corresp_pipeline_input = self.pipeline.inputs.get(dataset_idx=i)
            self.socket_map[(self.run, None, corresp_pipeline_input)] = pipeline_input
            self.dataset_fs_map[pipeline_input] = os.path.join(
                in_dir,
                "run{}_{}".format(self.run.pk, corresp_pipeline_input.pk))

        # Make the sandbox directory.
        self.logger.debug("file_access_utils.set_up_directory({})".format(self.sandbox_path))
        file_access_utils.configure_sandbox_permissions(self.sandbox_path)
        file_access_utils.set_up_directory(self.sandbox_path)
        file_access_utils.set_up_directory(in_dir)
        file_access_utils.set_up_directory(self.out_dir)
        file_access_utils.configure_sandbox_permissions(self.out_dir)

        # Queue of RunSteps/RunCables to process.
        self.queue_for_processing = []

        # PipelineSteps and PipelineCables "bundled" with necessary information for running them.
        # This will be used when it comes time to finish these cables, either as a first execution or as a recovery.
        # Keys are (run, generator) pairs.
        self.step_execute_info = {}
        self.cable_execute_info = {}

        # A table of RunSteps/RunCables completed.
        # self.tasks_completed = {}

        # A table keyed by Datasets, whose values are lists of the RunSteps/RunCables waiting on them.
        self.tasks_waiting = defaultdict(list)

        # The inverse table to the above: the keys are RunSteps/RunCables waiting on recovering Datasets,
        # and the values are all of the Datasets they're waiting for.
        self.waiting_for = {}

        # For each sub-pipeline, we track how many of their input cables have completed.
        self.sub_pipeline_cable_tracker = {}

    class RunInputEmptyException(Exception):
        """
        An exception raised when a cable encounters a Run input that has no data.
        """
        pass

    def step_xput_path(self, runstep, transformationxput, step_run_dir):
        """Path in Sandbox for PipelineStep TransformationXput."""
        file_suffix = extensions.RAW if transformationxput.is_raw() else extensions.CSV
        file_name = "step{}_{}.{}".format(runstep.step_num, transformationxput.dataset_name, file_suffix)

        if transformationxput.is_input:
            xput_dir = dirnames.IN_DIR
        else:
            xput_dir = dirnames.OUT_DIR
        return os.path.join(step_run_dir, xput_dir, file_name)

    def register_dataset(self, dataset, location):
        """Set the location of a Dataset on the file system.

        If the Dataset is already in the Sandbox (ie. it is in
        dataset_fs_map), do not update the existing location.

        INPUTS
        dataset     Dataset to register
        location            file path of dataset in the Sandbox
        """
        try:
            self.dataset_fs_map[dataset] = self.dataset_fs_map[dataset] or location
        except KeyError:
            self.dataset_fs_map[dataset] = location

    def find_dataset(self, dataset):
        """Find the location of a Dataset on the file system.

        INPUTS
        dataset     Dataset to locate

        OUTPUTS
        location            the path of dataset in the Sandbox,
                            or None if it's not there
        """
        try:
            location = self.dataset_fs_map[dataset]
        except KeyError:
            self.logger.debug("Dataset {} is not in the Sandbox".format(dataset))
            return None

        return (location if location and file_access_utils.file_exists(location) else None)

    def update_cable_maps(self, runcable, output_dataset, output_path):
        """Update maps after cable execution.

        INPUTS
        runcable        RunCable created for cable execution
        output_dataset       Dataset output by cable
        output_path     where the cable wrote its output
        """
        self.register_dataset(output_dataset, output_path)
        cable = runcable.component
        self.socket_map[(runcable.parent_run, cable, cable.dest)] = output_dataset
        self.cable_map[(runcable.parent, cable)] = runcable

    def update_step_maps(self, runstep, step_run_dir, output_paths):
        """Update maps after pipeline step execution.

        INPUTS
        runstep         RunStep responsible for execution
        step_run_dir    directory where execution was done
        output_paths    paths where RunStep outputs were written,
                        ordered by index
        """
        pipelinestep = runstep.component
        my_run = runstep.run
        self.ps_map[(my_run, pipelinestep)] = (step_run_dir, runstep)
        execrecordouts = runstep.execrecord.execrecordouts

        for i, step_output in enumerate(pipelinestep.transformation.outputs.order_by("dataset_idx")):
            corresp_ero = execrecordouts.get(generic_output=step_output)
            corresp_dataset = corresp_ero.dataset
            self.register_dataset(corresp_dataset, output_paths[i])

            # This pipeline step, with the downstream TI, maps to corresp_dataset
            self.socket_map[(runstep.parent_run, pipelinestep, step_output)] = corresp_dataset

    def _register_missing_output(self, output_dataset, execlog, start_time, end_time):
        """Create a failed ContentCheckLog for missing cable output

        INPUTS
        output_dataset       Dataset cable was supposed to output
        execlog         ExecLog for cable execution which didn't produce
                        output
        start_time      time when we started checking for missing output
        end_time        time when we finished checking for missing output
        """
        self.logger.error("File doesn't exist - creating CCL with BadData")
        ccl = output_dataset.content_checks.create(start_time=start_time, end_time=end_time, execlog=execlog,
                                                   user=self.user)
        ccl.add_missing_output()

    def first_generator_of_dataset(self, dataset_to_find, curr_run=None):
        """
        Find the (run, generator) pair which first produced a Dataset.
        If generator is None, it indicates the socket is a Pipeline input. If
        both generator and run are None, it means the dataset wasn't found in the
        Pipeline.
        """
        # Is this a sub-run or a top-level run?
        if curr_run is None:
            # This is a top-level run.  Set curr_run accordingly.
            curr_run = self.run
        pipeline = curr_run.pipeline

        # First check if the dataset we're looking for is a Pipeline input.
        if curr_run == self.run:
            for socket in pipeline.inputs.order_by("dataset_idx"):
                key = (self.run, None, socket)
                if key in self.socket_map and self.socket_map[key] == dataset_to_find:
                    return (self.run, None)

        # If it's not a pipeline input, check all the steps.
        steps = curr_run.runsteps.all()
        steps = sorted(steps, key=lambda step: step.pipelinestep.step_num)

        for step in steps:
            # First check if the dataset is an input to this step. In that case, it
            # had to come in from a nontrivial cable (since we're checking the
            # steps in order, and we already checked everything produced prior to
            # this step).
            pipelinestep = step.pipelinestep
            for socket in pipelinestep.transformation.inputs.order_by("dataset_idx"):
                generator = pipelinestep.cables_in.get(dest=socket)
                key = (curr_run, generator, socket)
                if key in self.socket_map and self.socket_map[key] == dataset_to_find:
                    return (curr_run, generator)

            # If it wasn't an input to this step, but this step is a sub-pipeline,
            # it might be somewhere within the sub-pipeline. Search recursively.
            if hasattr(step, "child_run") and step.child_run is not None:
                run, generator = self.first_generator_of_dataset(dataset_to_find, step.child_run)
                if run is not None:

                    # Did we find it somewhere inside the sub-Pipeline?
                    if generator is not None:
                        return (run, generator)

                    # If it was an input to the sub-Pipeline, we need the cable leading in.
                    # May 23, 2014: I think this is redundant as this will get caught when
                    # looking at the cables feeding into this step.
                    # else:
                    #     generator = pipelinestep.cables_in.get(dest=socket)
                    #     return (curr_run, generator)

            # Now check if it's an output from this step.
            generator = pipelinestep
            for socket in pipelinestep.transformation.outputs.order_by("dataset_idx"):
                key = (curr_run, generator, socket)
                if key in self.socket_map and self.socket_map[key] == dataset_to_find:
                    return (curr_run, generator)

        # Finally, check if it's at the end of a nontrivial Pipeline output cable.
        for outcable in pipeline.outcables.order_by("output_idx"):
            socket = outcable.dest
            key = (curr_run, outcable, socket)
            if key in self.socket_map and self.socket_map[key] == dataset_to_find:
                return (curr_run, outcable)

        # If we're here, we didn't find it.
        return (None, None)

    ####
    # Code to execute code in an MPI environment.

    def enqueue_runnable_tasks(self, data_newly_available):
        """
        Function that queues steps/outcables that are ready to run now that new data is available.
        """
        for dataset in data_newly_available:
            assert dataset.has_data() or self.find_dataset(dataset) is not None

        # First, get anything that was waiting on this data to proceed.
        taxiing_for_takeoff = []
        for dataset in data_newly_available:
            if dataset in self.tasks_waiting:
                # Notify all tasks waiting on this dataset that it's now available.
                # Trigger any tasks for which that was the last piece it was waiting on.
                for task in self.tasks_waiting[dataset]:
                    self.waiting_for[task].remove(dataset)
                    if len(self.waiting_for[task]) == 0:
                        # Add this to the list of things that are ready to go.
                        taxiing_for_takeoff.append(task)

                # Remove this entry from self.tasks_waiting.
                self.tasks_waiting.pop(dataset)

        self.queue_for_processing = self.queue_for_processing + taxiing_for_takeoff

    def advance_pipeline(self, task_completed=None, run_to_advance=None, incables_completed=None,
                         steps_completed=None, outcables_completed=None):
        """
        Proceed through a pipeline, seeing what can run now that a step or cable has just completed.

        Note that if a sub-pipeline of the pipeline finishes, we report that the parent runstep
        has finished, not the cables.

        If task_completed is specified, that indicates that a new RunComponent has just finished
        (i.e. by the fleet), so we attempt to advance the Pipeline.

        If run_to_advance is specified, it means this is a recursive call, attempting to advance
        a sub-Pipeline given the new stuff that has been finished so far (which is passed on
        through the parameters incables_completed, steps_completed, and outcables_completed).

        PRE:
        at most one of run_to_advance and task_completed may not be None.
        if task_completed is not None, it is finished and successful.
        """
        assert (type(task_completed) in (RunStep,
                                         RunSIC,
                                         RunOutputCable) or
                task_completed is None)
        assert not (run_to_advance is not None and task_completed is not None)

        incables_completed = incables_completed or []
        steps_completed = steps_completed or []
        outcables_completed = outcables_completed or []

        run_to_resume = self.run
        if run_to_advance:
            assert run_to_advance.top_level_run == self.run
            run_to_resume = run_to_advance

        if task_completed is None and run_to_advance is None:
            self.logger.debug('Starting run "%s"', self.run)
        elif task_completed is not None:
            self.logger.debug('Advancing run "%s" after completion of task %s (coordinates: %s)',
                              self.run,
                              task_completed,
                              task_completed.get_coordinates())
        else:  # run_to_advance is not None
            self.logger.debug('Advancing sub-run "%s" (coordinates %s) of pipeline %s',
                              run_to_resume, run_to_resume.get_coordinates(), self.run)

        if task_completed is None and not run_to_resume.has_started():
            run_to_resume.start(save=True)

        # Refresh the run plan, unless this is a recursive call that starts a new sub-Pipeline.
        if not run_to_advance:
            self.run_plan = RunPlan()
            self.run_plan.load(self.run, self.inputs)
            self.run_plan.find_consistent_execution()

        pipeline_to_resume = run_to_resume.pipeline

        if run_to_resume != self.run:
            assert run_to_resume.top_level_run == self.run

        sandbox_path = self.sandbox_path
        if run_to_resume != self.run:
            sandbox_path = (self.step_execute_info[(run_to_resume.parent_runstep.run,
                                                    run_to_resume.parent_runstep.pipelinestep)]
                            .step_run_dir)

        # Update our lists of components completed.
        step_nums_completed = []
        if type(task_completed) == RunSIC:
            assert task_completed.dest_runstep.pipelinestep.is_subpipeline()
            incables_completed.append(task_completed)
        elif type(task_completed) == RunStep:
            steps_completed.append(task_completed)
        elif type(task_completed) == RunOutputCable:
            outcables_completed.append(task_completed)
        elif task_completed is None and run_to_advance is None:
            # This indicates that the only things accessible are the inputs.
            step_nums_completed.append(0)

        step_nums_completed += [x.step_num for x in steps_completed if x.run == run_to_resume]

        # A tracker for whether everything is complete or not.
        all_complete = True

        # Go through steps in order, looking for input cables pointing at the task(s) that have completed.
        # If task_completed is None, then we are starting the pipeline and we look at the pipeline inputs.
        for step in pipeline_to_resume.steps.order_by("step_num"):
            curr_RS = run_to_resume.runsteps.filter(pipelinestep=step).first()
            assert curr_RS is not None

            # If this is already running, we skip it, unless it's a sub-Pipeline.
            if curr_RS.is_running():
                if not step.is_subpipeline():
                    # This is a non-sub-run already in progress, so we leave it.
                    all_complete = False
                    continue

                # At this point, we know this is a sub-Pipeline, and is possibly waiting
                # for one of its input cables to finish.
                if type(task_completed) == RunSIC:
                    feeder_RSICs = curr_RS.RSICs.filter(pk__in=[x.pk for x in incables_completed])
                    if not feeder_RSICs.exists():
                        # This isn't one of the RunSICs for this sub-Run.
                        all_complete = False
                        continue
                    else:
                        self.sub_pipeline_cable_tracker[curr_RS].difference_update(set(feeder_RSICs))
                        if len(self.sub_pipeline_cable_tracker[curr_RS]) != 0:
                            # Not all of the cables are done yet.
                            all_complete = False
                            continue

                else:
                    # Look in the lists of tasks completed.  Do any of them belong to this sub-run?
                    complete_subtask_exists = False
                    for task in itertools.chain(incables_completed, steps_completed, outcables_completed):
                        task_coords = task.get_coordinates()
                        curr_step_coords = curr_RS.get_coordinates()
                        if task_coords[0:len(curr_step_coords)] == curr_step_coords:
                            complete_subtask_exists = True
                            break

                    if not complete_subtask_exists:
                        continue

                # Having reached here, we know that task_completed was either:
                #  - the last RunSIC the sub-Pipeline was waiting on, or
                #  - a task belonging to the sub-Run,
                # so we can advance the sub-Run and update the lists of components
                # completed.
                incables_completed, steps_completed, outcables_completed = self.advance_pipeline(
                    run_to_advance=curr_RS.child_run,
                    incables_completed=incables_completed,
                    steps_completed=steps_completed,
                    outcables_completed=outcables_completed
                )

                curr_RS.refresh_from_db()
                if curr_RS.child_run.is_cancelled():
                    curr_RS.cancel_running(save=True)
                    run_to_resume.cancel(save=True)
                    return incables_completed, steps_completed, outcables_completed
                elif curr_RS.child_run.is_failed():
                    curr_RS.finish_failure(save=True)
                    run_to_resume.mark_failure(save=True)
                    return incables_completed, steps_completed, outcables_completed
                elif curr_RS.child_run.is_successful():
                    curr_RS.finish_successfully(save=True)
                else:
                    all_complete = False

                # We've done all we can with this sub-Pipeline, so we move on to the next step.
                continue

            # Now, check that this step is still pending.  If not, skip ahead.
            if not curr_RS.is_pending():
                continue

            # If this step is not fed at all by any of the tasks that just completed,
            # we skip it -- it can't have just become ready to go.
            # Special case: this step has no inputs (for example, it's a random number generator).
            # If so, we just go ahead.
            fed_by_newly_completed = not step.cables_in.exists()
            if step.cables_in.filter(source_step__in=step_nums_completed).exists():
                fed_by_newly_completed = True
            if not fed_by_newly_completed:
                for cable in outcables_completed:
                    parent_runstep = cable.parent_run.parent_runstep
                    if parent_runstep is None or parent_runstep.run != run_to_resume:
                        continue
                    output_fed = parent_runstep.transformation.outputs.get(
                        dataset_idx=cable.pipelineoutputcable.output_idx
                    )
                    if step.cables_in.filter(source_step=parent_runstep.step_num, source=output_fed).exists():
                        fed_by_newly_completed = True
                        break

            if not fed_by_newly_completed and run_to_resume.is_subrun:
                # Check if this is fed by a completed incable (i.e. if this is part of a sub-Pipeline that is
                # fed directly from the inputs).

                pipeline_inputs_fed = []
                for incable in incables_completed:
                    if run_to_resume.parent_runstep != incable.dest_runstep:
                        continue
                    pipeline_inputs_fed.append(incable.PSIC.dest)

                are_any_used = step.cables_in.filter(source_step=0, source__in=pipeline_inputs_fed).exists()
                if are_any_used:
                    fed_by_newly_completed = True

            if not fed_by_newly_completed:
                # This one certainly isn't getting completed now.
                all_complete = False
                continue

            # Examine this step and see if all of the inputs are (at least symbolically) available.
            step_inputs = []

            # For each PSIC leading to this step, check if its required dataset is in the maps.
            all_inputs_fed = True
            for psic in step.cables_in.order_by("dest__dataset_idx"):
                socket = psic.source.definite
                run_to_query = run_to_resume

                # If the PSIC comes from another step, the generator is the source pipeline step,
                # or the output cable if it's a sub-pipeline.
                if psic.source_step != 0:
                    generator = pipeline_to_resume.steps.get(step_num=psic.source_step)
                    if socket.transformation.is_pipeline():
                        run_to_query = run_to_resume.runsteps.get(pipelinestep=generator).child_run
                        generator = generator.transformation.pipeline.outcables.get(output_idx=socket.dataset_idx)

                # Otherwise, the psic comes from step 0.
                else:
                    # If this step is not a subpipeline, the dataset was uploaded.
                    generator = None
                    # If this step is a subpipeline, then the run we are interested in is the parent run.
                    # Get the run and cable that feeds this PSIC.
                    if run_to_resume.parent_runstep is not None:
                        run_to_query = run_to_resume.parent_runstep.run
                        cables_into_subpipeline = run_to_resume.parent_runstep.pipelinestep.cables_in
                        generator = cables_into_subpipeline.get(dest=psic.source)

                if (run_to_query, generator, socket) in self.socket_map:
                    step_inputs.append(self.socket_map[(run_to_query, generator, socket)])
                else:
                    all_inputs_fed = False
                    break

            if not all_inputs_fed:
                # This step cannot be run yet, so we move on.
                all_complete = False
                continue

            # Start execution of this step.
            curr_run_coords = run_to_resume.get_coordinates()
            curr_run_plan = self.run_plan
            for coord in curr_run_coords:
                curr_run_plan = curr_run_plan.step_plans[coord-1].subrun_plan
            assert curr_RS == curr_run_plan.step_plans[step.step_num-1].run_step
            run_dir = os.path.join(sandbox_path, "step{}".format(step.step_num))

            step_coords = curr_RS.get_coordinates()
            step_coord_render = step_coords if len(step_coords) > 1 else step_coords[0]
            self.logger.debug("Beginning execution of step %s (%s)", step_coord_render, step)

            # At this point we know that all inputs are available.
            if step.is_subpipeline():
                self.logger.debug("Step %s (coordinates %s) is a sub-Pipeline.  Marking it "
                                  "as started, and handling its input cables.",
                                  curr_RS, curr_RS.get_coordinates())
                curr_RS.start(save=True)  # transition: Pending->Running
                _in_dir, _out_dir, log_dir = self._setup_step_paths(run_dir, False)

                # We start all of the RunSICs in motion.  If they all successfully reuse,
                # we advance the sub-pipeline.
                self.sub_pipeline_cable_tracker[curr_RS] = set()

                all_RSICs_done = True
                cable_info_list = []
                for input_cable in step.cables_in.order_by("dest__dataset_idx"):

                    cable_path = self.step_xput_path(curr_RS, input_cable.dest, run_dir)

                    cable_exec_info = self.reuse_or_prepare_cable(
                        input_cable,
                        curr_RS,
                        step_inputs[input_cable.dest.dataset_idx-1],
                        cable_path,
                        log_dir,
                        run_dir
                    )
                    cable_info_list.append(cable_exec_info)

                    cable_record = cable_exec_info.cable_record
                    if cable_record.is_complete():
                        incables_completed.append(cable_record)
                    elif cable_exec_info.could_be_reused:
                        cable_record.finish_successfully()
                        incables_completed.append(cable_record)
                    else:
                        self.sub_pipeline_cable_tracker[curr_RS].add(cable_record)
                        all_RSICs_done = False

                    # If the cable was cancelled (e.g. due to bad input), we bail.
                    return_because_fail = False
                    if cable_exec_info.cancelled:
                        self.logger.debug("Input cable %s to sub-pipeline step %s was cancelled",
                                          cable_exec_info.cable_record,
                                          curr_RS)
                        return_because_fail = True
                    elif (cable_exec_info.cable_record.reused and
                          not cable_exec_info.could_be_reused):
                        self.logger.debug("Input cable %s to sub-pipeline step %s failed on reuse",
                                          cable_exec_info.cable_record,
                                          curr_RS)
                        return_because_fail = True

                    if return_because_fail:
                        curr_RS.refresh_from_db()
                        curr_RS.child_run.cancel()
                        curr_RS.child_run.stop(save=True)

                        if cable_exec_info.cancelled:
                            curr_RS.cancel_running(save=True)
                            run_to_resume.cancel(save=True)
                        else:
                            curr_RS.finish_failure(save=True)
                            run_to_resume.mark_failure(save=True)
                        curr_RS.complete_clean()

                        # We don't mark the Run as complete in case something is still running.
                        return incables_completed, steps_completed, outcables_completed

                # Bundle up the information required to process this step.
                # Construct output_paths.
                output_paths = [self.step_xput_path(curr_RS, x, run_dir) for x in step.outputs]
                execute_info = RunStepExecuteInfo(curr_RS, self.user, cable_info_list, None, run_dir, log_dir,
                                                  output_paths)
                self.step_execute_info[(run_to_resume, step)] = execute_info

                if not all_RSICs_done:
                    all_complete = False
                else:
                    incables_completed, steps_completed, outcables_completed = self.advance_pipeline(
                        run_to_advance=curr_RS.child_run,
                        incables_completed=incables_completed,
                        steps_completed=steps_completed,
                        outcables_completed=outcables_completed
                    )

                    # Update states for curr_RS and run_to_resume if necessary.
                    curr_RS.refresh_from_db()
                    if curr_RS.child_run.is_cancelled():
                        curr_RS.cancel_running(save=True)
                        run_to_resume.cancel(save=True)
                        return incables_completed, steps_completed, outcables_completed
                    elif curr_RS.child_run.is_failed():
                        curr_RS.finish_failure(save=True)
                        run_to_resume.mark_failure(save=True)
                        return incables_completed, steps_completed, outcables_completed
                    elif curr_RS.child_run.is_successful():
                        curr_RS.finish_successfully(save=True)
                    else:
                        # Nothing wrong, but the step is not finished.
                        all_complete = False

                # We've done all we can for this step.
                continue

            # At this point, we know the step is not a sub-Pipeline, so we go about our business.
            curr_RS = self.reuse_or_prepare_step(step, run_to_resume, step_inputs, run_dir)

            # If the step we just started is for a Method, and it was successfully reused, then we add its step
            # number to the list of those just completed.  This may then allow subsequent steps to also be started.
            if curr_RS.is_cancelled():
                # If the RunStep is cancelled after reuse, that means that one of
                # its input cables failed on reuse, or a cable cancelled because it
                # was unable to copy a file into the sandbox.
                failed_cables = curr_RS.RSICs.filter(_runcomponentstate__pk=runcomponentstates.FAILED_PK)
                cancelled_cables = curr_RS.RSICs.filter(_runcomponentstate__pk=runcomponentstates.CANCELLED_PK)
                assert failed_cables.exists() or cancelled_cables.exists()

                if failed_cables.exists():
                    self.logger.debug("Input cable(s) %s to step %d (%s) failed",
                                      failed_cables, step.step_num, step)
                    run_to_resume.mark_failure(save=True)
                if cancelled_cables.exists():
                    self.logger.debug("Input cable(s) %s to step %d (%s) cancelled",
                                      cancelled_cables, step.step_num, step)
                    if not failed_cables.exists():
                        run_to_resume.cancel(save=True)

                return incables_completed, steps_completed, outcables_completed

            elif curr_RS.reused and not curr_RS.is_successful():
                self.logger.debug("Step %d (%s) failed on reuse", step.step_num, step)
                run_to_resume.mark_failure(save=True)
                return incables_completed, steps_completed, outcables_completed

            elif curr_RS.is_successful():
                step_nums_completed.append(step.step_num)

            elif curr_RS.is_running():
                all_complete = False

            else:
                # FIXME check that this is all the possible states that it can be in after r_o_p_s
                raise RuntimeError("unhandled case !\n")

        # Now go through the output cables and do the same.
        for outcable in pipeline_to_resume.outcables.order_by("output_idx"):
            curr_cable = run_to_resume.runoutputcables.filter(pipelineoutputcable=outcable).first()

            # First, if this is already running or complete, we skip it.
            if curr_cable is not None and not curr_cable.is_pending():
                if curr_cable.is_running():
                    all_complete = False
                continue

            # At this point we know this cable has not already been run; i.e. either
            # there is no record or the record is still pending.

            # Check if this cable has just had its input made available.  First, check steps that
            # just finished.
            source_dataset = None
            fed_by_newly_completed = False

            feeder_pipeline_step = pipeline_to_resume.steps.get(step_num=outcable.source_step)
            if outcable.source_step in step_nums_completed:
                source_dataset = self.socket_map[(
                    run_to_resume,
                    feeder_pipeline_step,
                    outcable.source
                )]
                fed_by_newly_completed = True

            # Next, check outcables of sub-pipelines to see if they provide what we need.
            if not fed_by_newly_completed:
                for cable in outcables_completed:
                    parent_runstep = cable.parent_run.parent_runstep
                    if parent_runstep is None or parent_runstep.run != run_to_resume:
                        continue
                    output_fed = parent_runstep.transformation.outputs.get(
                        dataset_idx=cable.pipelineoutputcable.output_idx
                    )
                    if outcable.source_step == parent_runstep.step_num and outcable.source == output_fed:
                        source_dataset = self.socket_map[(cable.parent_run, cable.pipelineoutputcable, output_fed)]
                        fed_by_newly_completed = True
                        break

            if not fed_by_newly_completed:
                # This outcable cannot be run yet.
                all_complete = False
                continue

            # We can now start this cable.
            file_suffix = "raw" if outcable.is_raw() else "csv"
            out_file_name = "run{}_{}.{}".format(run_to_resume.pk, outcable.output_name, file_suffix)
            output_path = os.path.join(self.out_dir, out_file_name)
            source_step_execution_info = self.step_execute_info[(run_to_resume, feeder_pipeline_step)]
            cable_exec_info = self.reuse_or_prepare_cable(
                outcable,
                run_to_resume,
                source_dataset,
                output_path,
                source_step_execution_info.log_dir,
                source_step_execution_info.step_run_dir
            )

            cr = cable_exec_info.cable_record

            if cr.is_failed():
                self.logger.debug("Cable %s failed on reuse", cr.pipelineoutputcable)
                run_to_resume.mark_failure(save=True)
                return incables_completed, steps_completed, outcables_completed

            elif cable_exec_info.could_be_reused:
                cr.finish_successfully(save=True)
                outcables_completed.append(cr)

            elif cr.is_running():
                all_complete = False

            # FIXME check that this is all the possible conditions coming out of r_o_p_c

        if all_complete and not run_to_resume.is_complete():
            self.logger.debug("Run (coordinates %s) completed.", run_to_resume.get_coordinates())
            with transaction.atomic():
                run_to_resume.stop(save=True)  # this transitions the state appropriately.

        return incables_completed, steps_completed, outcables_completed

    def reuse_or_prepare_cable(self, cable, parent_record, input_dataset, output_path,
                               log_dir, cable_info_dir):
        """
        Attempt to reuse the cable; prepare it for finishing if unable.
        """
        assert input_dataset in self.dataset_fs_map

        # Create new RSIC/ROC.
        curr_record = RunCable.create(cable, parent_record)  # this start()s it
        self.logger.debug("Not recovering - created {}".format(curr_record.__class__.__name__))
        self.logger.debug("Cable keeps output? {}".format(curr_record.keeps_output()))

        by_step = parent_record if isinstance(parent_record, RunStep) else None

        # We bail out if the input has somehow been corrupted.
        if not input_dataset.initially_OK():
            # FIXME this should never happen because if it's an input, it will have been
            # checked by Sandbox, and if it's a cable inside the Pipeline, whatever fed
            # this cable should have failed.
            self.logger.debug("Input %s failed its initial check and should not be used.  Cancelling.",
                              input_dataset)

            # Update state variables.
            curr_record.cancel_running(save=True)
            curr_record.complete_clean()

            # Return a RunCableExecuteInfo that is marked as cancelled.
            exec_info = RunCableExecuteInfo(curr_record,
                                            self.user,
                                            None,
                                            input_dataset,
                                            self.dataset_fs_map[input_dataset],
                                            output_path,
                                            log_dir=log_dir,
                                            by_step=by_step)
            exec_info.cancel()
            self.cable_execute_info[(curr_record.parent_run, cable)] = exec_info
            return exec_info

        # Attempt to reuse this PipelineCable.
        return_now = False

        succeeded_yet = False
        self.logger.debug("Checking whether cable can be reused")
        could_be_reused = False
        while not succeeded_yet:
            try:
                with transaction.atomic():
                    curr_ER, can_reuse = curr_record.get_suitable_ER(input_dataset, reuse_failures=False)

                    if curr_ER is not None:
                        output_dataset = curr_ER.execrecordouts.first().dataset

                        if curr_ER.generator.record.is_quarantined():
                            # We will re-attempt; if it's fixed, then we un-quarantine the ExecRecord.
                            self.logger.debug(
                                "Found quarantined ExecRecord %s; will decontaminate if successful",
                                curr_ER
                            )

                        elif not can_reuse["successful"] or can_reuse["fully reusable"]:
                            # In this case, we can return now (either successfully or not).
                            self.logger.debug(
                                "ExecRecord %s is reusable (successful = %s)",
                                curr_ER,
                                can_reuse["successful"]
                            )
                            curr_record.reused = True
                            curr_record.execrecord = curr_ER

                            if can_reuse["successful"]:  # and therefore fully reusable
                                # curr_record.finish_successfully(save=True)
                                could_be_reused = True
                            else:
                                curr_record.finish_failure(save=True)
                                # curr_record.complete_clean()

                            self.update_cable_maps(curr_record, output_dataset, output_path)
                            return_now = True
                succeeded_yet = True
            except (OperationalError, InternalError):
                wait_time = random.random()
                self.logger.debug("Database conflict.  Waiting for %f seconds before retrying.", wait_time)
                time.sleep(wait_time)

        # Bundle up execution info in case this needs to be run, either by recovery, as a first execution,
        # or as a "filling-in".
        exec_info = RunCableExecuteInfo(curr_record,
                                        self.user,
                                        curr_ER,
                                        input_dataset,
                                        self.dataset_fs_map[input_dataset],
                                        output_path,
                                        log_dir=log_dir,
                                        by_step=by_step,
                                        could_be_reused=could_be_reused)

        exec_info.set_cable_info_dir(cable_info_dir)

        self.cable_execute_info[(curr_record.parent_run, cable)] = exec_info

        if return_now:
            curr_record.save()
            return exec_info

        # We didn't find a compatible and reusable ExecRecord, so we are committed to executing
        # this cable.
        curr_record.reused = False
        self.logger.debug("No ER to completely reuse - preparing execution of this cable")

        # Check the availability of input_dataset; recover if necessary.  Queue for execution
        # if cable is an outcable or an incable that feeds a sub-Pipeline.
        try:
            exec_info.ready_to_go = self.enqueue_cable(exec_info, force=False)
        except Sandbox.RunInputEmptyException:
            # This should have been cancelled already.
            curr_record.refresh_from_db()
            assert curr_record.is_cancelled()
            curr_record.complete_clean()

            # Mark exec_info as cancelled.
            exec_info.cancel()

        return exec_info

    # We'd call this when we need to prepare a cable for recovery.  This is essentially a "force" version of
    # reuse_or_prepare_cable, where we don't attempt at all to reuse (we're past that point and we know we need
    # to produce real data here).  We call this function if a non-trivial cable produces data that's fed into
    # another step, e.g. an outcable from a sub-pipeline.
    def enqueue_cable(self, cable_info, force=False):
        """
        Recursive helper for recover that handles recovery of a cable.

        RAISES:
        If this encounters a Run input that cannot be recovered, it raises
        Sandbox.RunInputEmptyException.
        """
        # Unpack info from cable_info.
        cable_record = cable_info.cable_record
        input_dataset = cable_info.input_dataset
        curr_ER = cable_info.execrecord
        recovering_record = cable_info.recovering_record
        by_step = cable_info.by_step

        # If this cable is already on the queue, we can return.
        if cable_record in self.queue_for_processing and by_step is None:
            self.logger.debug("Cable is already slated for recovery")
            return cable_record

        ready_to_go = False
        if force:
            assert curr_ER is not None
        dataset_path = self.find_dataset(input_dataset)
        # Add this cable to the queue, unlike in reuse_or_prepare_cable.
        # If this is independent of any step recovery, we add it to the queue; either by marking it as
        # waiting for stuff that's going to recover, or by throwing it directly onto the list of tasks to
        # perform.
        queue_cable = (cable_record.component.is_outcable() or
                       cable_record.dest_runstep.pipelinestep.is_subpipeline() or
                       (force and by_step is None))
        file_access_start = timezone.now()
        if dataset_path is None and not input_dataset.has_data():
            file_access_end = timezone.now()

            # Bail out here if the input dataset is a Pipeline input and cannot be recovered
            # (at this point we are probably in the case where an external file was removed).
            if (cable_record.component.is_incable() and
                    cable_record.top_level_run == cable_record.parent_run and
                    cable_record.component.definite.source_step == 0):
                self.logger.debug("Cannot recover cable input: it is a Run input")
                self.logger.debug("Cancelling cable with pk=%d.", cable_record.pk)

                with transaction.atomic():
                    # Create a failed IntegrityCheckLog.
                    iic = IntegrityCheckLog(
                        dataset=input_dataset,
                        runcomponent=cable_record,
                        read_failed=True,
                        start_time=file_access_start,
                        end_time=file_access_end,
                        user=self.user
                    )
                    iic.clean()
                    iic.save()

                cable_record.cancel_running(save=True)
                # Making sure the invoking record (either cable_record or something else
                # that's recovering) is handled by the calling function.

                raise Sandbox.RunInputEmptyException()

            self.logger.debug("Cable input requires non-trivial recovery")
            self.queue_recovery(input_dataset, recovering_record=recovering_record)

            if queue_cable:
                self.tasks_waiting[input_dataset].append(cable_record)
                self.waiting_for[cable_record] = [input_dataset]

        else:
            ready_to_go = True
            if queue_cable:
                self.queue_for_processing.append(cable_record)

        return ready_to_go

    # Function that reuses or prepares a step, which will later be complemented by a finish_step
    # method.  This would not be called if you were recovering.  This will not be called
    # on steps that are sub-Pipelines.
    def reuse_or_prepare_step(self, pipelinestep, parent_run, inputs, step_run_dir):
        """
        Reuse step if possible; prepare it for execution if not.

        As in execute_step:
        Inputs written to:  [step run dir]/input_data/step[step num]_[input name]
        Outputs written to: [step run dir]/output_data/step[step num]_[output name]
        Logs written to:    [step run dir]/logs/step[step num]_std(out|err).txt
        """

        # Start execution of this step.
        # Retrieve the RunStep from the run plan.
        curr_run_coords = parent_run.get_coordinates()
        curr_run_plan = self.run_plan
        for coord in curr_run_coords:
            curr_run_plan = curr_run_plan.step_plans[coord-1].subrun_plan

        step_plan = curr_run_plan.step_plans[pipelinestep.step_num-1]
        curr_RS = step_plan.run_step
        curr_RS.start()

        _in_dir, _out_dir, log_dir = self._setup_step_paths(step_run_dir, False)

        assert not pipelinestep.is_subpipeline()

        # Note: bad inputs will be caught by the cables.
        input_names = ", ".join(str(i) for i in inputs)
        self.logger.debug("Beginning execution of step {} in directory {} on inputs {}"
                          .format(pipelinestep, step_run_dir, input_names))

        # Check which steps we're waiting on.
        # Make a note of those steps that feed cables that are reused, but do not retain their output,
        # i.e. those cables that are *symbolically* reusable but not *actually* reusable.
        datasets_to_recover = []
        symbolically_okay_datasets = []
        cable_info_list = []
        for i, curr_input in enumerate(pipelinestep.inputs):  # This is already ordered!
            # The cable that feeds this input and where it will write its eventual output.
            corresp_cable = pipelinestep.cables_in.get(dest=curr_input)
            cable_path = self.step_xput_path(curr_RS, curr_input, step_run_dir)
            cable_exec_info = self.reuse_or_prepare_cable(
                corresp_cable,
                curr_RS,
                inputs[i],
                cable_path,
                log_dir,
                step_run_dir
            )
            cable_info_list.append(cable_exec_info)

            # If the cable was cancelled (e.g. due to bad input), we bail.
            return_because_fail = False
            if cable_exec_info.cable_record.is_cancelled():
                self.logger.debug("Input cable %s to step %s was cancelled", cable_exec_info.cable_record,
                                  curr_RS)
                return_because_fail = True
            elif (cable_exec_info.cable_record.reused and
                  cable_exec_info.cable_record.is_failed()):
                self.logger.debug("Input cable %s to step %s failed on reuse", cable_exec_info.cable_record,
                                  curr_RS)
                return_because_fail = True
            # If the cable is not fit to be reused and not ready to go, we need to recover its input.
            # elif not cable_exec_info.cable_record.is_complete():
            elif not cable_exec_info.could_be_reused:
                if not cable_exec_info.ready_to_go:
                    datasets_to_recover.append(inputs[i])
            elif not cable_exec_info.execrecord.execrecordins.first().dataset.has_data():
                symbolically_okay_datasets.append(inputs[i])

            if return_because_fail:
                curr_RS.cancel_running(save=True)
                curr_RS.complete_clean()
                return curr_RS

        # Having reached here, we know that all input cables are either fit for reuse or ready to go.

        # Construct output_paths.
        output_paths = [self.step_xput_path(curr_RS, x, step_run_dir) for x in pipelinestep.outputs]
        execute_info = RunStepExecuteInfo(
            curr_RS,
            self.user,
            cable_info_list,
            None,
            step_run_dir,
            log_dir,
            output_paths
        )
        self.step_execute_info[(parent_run, pipelinestep)] = execute_info

        # If we're waiting on feeder steps, register this step as waiting for other steps,
        # and return the (incomplete) RunStep.  Note that if this happens, we will have to
        # recover the feeders that were only symbolically OK, so we add those to the pile.
        # The steps that need to precede it have already been added to the queue above by
        # reuse_or_prepare_cable.
        if len(datasets_to_recover) > 0:
            for input_dataset in datasets_to_recover + symbolically_okay_datasets:
                self.tasks_waiting[input_dataset].append(curr_RS)
            self.waiting_for[curr_RS] = datasets_to_recover + symbolically_okay_datasets
            return curr_RS

        # At this point we know that we're at least symbolically OK to proceed.
        # Check if all of the cables have known outputs; if they do, then we can
        # attempt to reuse an ExecRecord.
        inputs_after_cable = []
        all_inputs_present = True
        for i, curr_input in enumerate(pipelinestep.inputs):
            if not cable_info_list[i].could_be_reused:
                all_inputs_present = False
                break
            inputs_after_cable.append(cable_info_list[i].cable_record.execrecord.execrecordouts.first().dataset)

        if all_inputs_present:

            # Look for a reusable ExecRecord.  If we find it, then complete the RunStep.
            succeeded_yet = False
            while not succeeded_yet:
                try:
                    with transaction.atomic():
                        curr_ER = step_plan.execrecord
                        can_reuse = curr_ER and curr_RS.check_ER_usable(curr_ER)

                        if curr_ER is not None:
                            execute_info.execrecord = curr_ER

                            if curr_ER.generator.record.is_quarantined():
                                # We will re-attempt; if it's fixed, then we un-quarantine the ExecRecord.
                                self.logger.debug(
                                    "Found quarantined ExecRecord %s; will decontaminate if successful",
                                    curr_ER
                                )
                            elif can_reuse["successful"] and not can_reuse["fully reusable"]:
                                self.logger.debug("Filling in ExecRecord {}".format(curr_ER))

                            else:
                                # This is either unsuccessful or fully reusable, so we can return.
                                self.logger.debug(
                                    "ExecRecord {} is reusable (successful = {})".format(
                                        curr_ER, can_reuse["successful"])
                                )
                                curr_RS.reused = True
                                curr_RS.execrecord = curr_ER

                                for cable_info in cable_info_list:
                                    # Being here means all cables were able to be fully reused.
                                    cable_info.cable_record.finish_successfully(save=True)

                                if can_reuse["successful"]:
                                    curr_RS.finish_successfully(save=True)
                                else:
                                    curr_RS.finish_failure(save=True)

                                self.update_step_maps(curr_RS, step_run_dir, output_paths)
                                return curr_RS

                        else:
                            self.logger.debug("No compatible ExecRecord found yet")

                        curr_RS.reused = False
                        curr_RS.save()
                    succeeded_yet = True
                except (OperationalError, InternalError):
                    wait_time = random.random()
                    self.logger.debug("Database conflict.  Waiting for %f seconds before retrying.", wait_time)
                    time.sleep(wait_time)

        # We found no reusable ER, so we add this step to the queue.
        # If there were any inputs that were only symbolically OK, we call queue_recover on them and register
        # this step as waiting for them.
        if len(symbolically_okay_datasets) > 0:
            for missing_data in symbolically_okay_datasets:
                try:
                    self.queue_recovery(missing_data, invoking_record=curr_RS)
                except Sandbox.RunInputEmptyException:
                    self.logger.debug("Cancelling RunStep with pk=%d.", curr_RS.pk)

                    # Update state variables.
                    curr_RS.cancel_running(save=True)
                    curr_RS.complete_clean()
                    execute_info.cancel()
                    return curr_RS

                self.tasks_waiting[missing_data].append(curr_RS)
            self.waiting_for[curr_RS] = symbolically_okay_datasets
        else:
            # We're not waiting for any inputs.  Add this step to the queue.
            self.queue_for_processing.append(curr_RS)

        return curr_RS

    def step_recover_h(self, execute_info):
        """
        Helper for recover that's responsible for forcing recovery of a step.

        RAISES:
        Sandbox.RunInputEmptyException if it encounters an empty Run input.
        """
        # Break out execute_info.
        runstep = execute_info.runstep
        cable_info_list = execute_info.cable_info_list
        recovering_record = execute_info.recovering_record

        pipelinestep = runstep.pipelinestep
        assert not pipelinestep.is_subpipeline()

        # If this runstep is already on the queue, we can return.
        if runstep in self.queue_for_processing:
            self.logger.debug("Step already in queue for execution")
            return

        # Check which cables need to be re-run.  Since we're forcing this step, we can't have
        # cables which are only symbolically OK, we need their output to be available either in the
        # sandbox or in the database.
        datasets_to_recover_first = []
        for cable in cable_info_list:
            # We use the sandbox's version of the execute information for this cable.
            cable_info = self.cable_execute_info[(cable.cable_record.parent_run, cable.cable_record.PSIC)]

            # If the cable needs its feeding steps to recover, we throw them onto the queue if they're not there
            # already.
            cable_out_dataset = cable_info.execrecord.execrecordouts.first().dataset
            if not cable_out_dataset.has_data() and not self.find_dataset(cable_out_dataset):
                datasets_to_recover_first.append(cable_info.input_dataset)
                execute_info.flag_for_recovery(recovering_record, by_step=runstep)
                self.enqueue_cable(execute_info, force=True)

        if len(datasets_to_recover_first) > 0:
            for dataset in datasets_to_recover_first:
                self.tasks_waiting[dataset].append(runstep)
            self.waiting_for[runstep] = datasets_to_recover_first
        else:
            self.queue_for_processing.append(runstep)

    def queue_recovery(self, dataset_to_recover, invoking_record):
        """
        Determines and enqueues the steps necessary to reproduce the specified Dataset.

        This is an MPI-friendly version of recover.  It only ever handles non-trivial recoveries,
        as trivial recoveries are now performed by cables themselves.

        @param dataset_to_recover: dataset that needs to be recovered
        @param invoking_record: the run component that needs the
            dataset as an input

        RAISES:
        Sandbox.RunInputEmptyException if it encounters an empty Run input.

        PRE
        dataset_to_recover is in the maps but no corresponding file is on the file system.
        """
        # NOTE before we recover an dataset, we should look to see if it's already being recovered
        # by something else.  If so we can just wait for that to finish.
        assert dataset_to_recover in self.dataset_fs_map
        assert not self.find_dataset(dataset_to_recover)

        self.logger.debug("Performing computation to create missing Dataset")

        # Search for the generator of the dataset in the Pipeline.
        curr_run, generator = self.first_generator_of_dataset(dataset_to_recover)

        if curr_run is None:
            raise ValueError('Dataset "{}" was not found in Pipeline "{}" and cannot be recovered'
                             .format(dataset_to_recover, self.pipeline))
        elif generator is None:
            raise ValueError('Dataset "{}" is an input to Pipeline "{}" and cannot be recovered'
                             .format(dataset_to_recover, self.pipeline))

        # We're now going to look up what we need to run from cable_execute_info and step_execute_info.

        self.logger.debug('Processing {} "{}" in recovery mode'.format(generator.__class__.__name__, generator))
        if type(generator) == pipeline.models.PipelineStep:
            curr_execute_info = self.step_execute_info[(curr_run, generator)]
            curr_execute_info.flag_for_recovery(invoking_record)
            self.step_recover_h(curr_execute_info)
        else:
            # Look for this cable (may be PSIC or POC) in curr_execute_info.
            curr_execute_info = self.cable_execute_info[(curr_run, generator)]
            curr_execute_info.flag_for_recovery(invoking_record)
            self.enqueue_cable(curr_execute_info, force=True)

    def hand_tasks_to_fleet(self):
        ready_tasks = self.queue_for_processing
        self.queue_for_processing = []
        return ready_tasks

    def get_task_info(self, task):
        """
        Helper that retrieves the task information for the specified RunStep/RunCable.
        """
        assert task.top_level_run == self.run
        if type(task) == RunStep:
            return self.step_execute_info[(task.run, task.pipelinestep)]
        return self.cable_execute_info[(task.parent_run, task.component)]

    def update_sandbox(self, task_finished):
        """
        Helper that updates the sandbox maps to reflect the information from the specified task_finished.

        PRE: task_finished is a RunStep/RunCable belonging to this sandbox's run, and it already has
        execution info available in step_execute_info or cable_execute_info.
        """
        assert task_finished.top_level_run == self.run
        if type(task_finished) == RunStep:
            assert (task_finished.run, task_finished.pipelinestep) in self.step_execute_info
        else:
            assert (task_finished.parent_run, task_finished.component) in self.cable_execute_info

        task_execute_info = self.get_task_info(task_finished)

        # Update the sandbox with this information.
        if type(task_finished) == RunStep:
            # Update the sandbox maps with the input cables' information as well as that
            # of the step itself.
            for rsic in task_finished.RSICs.all():
                curr_cable_task_info = self.get_task_info(rsic)
                curr_cable_out_dataset = rsic.execrecord.execrecordouts.first().dataset
                self.update_cable_maps(rsic, curr_cable_out_dataset, curr_cable_task_info.output_path)

            self.update_step_maps(task_finished, task_execute_info.step_run_dir, task_execute_info.output_paths)

        else:
            cable_out_dataset = task_finished.execrecord.execrecordouts.first().dataset
            self.update_cable_maps(task_finished, cable_out_dataset, task_execute_info.output_path)

    @staticmethod
    def _setup_step_paths(step_run_dir, recover):
        """Set up paths for running a PipelineStep.

        INPUTS
        step_run_dir    root directory where step will be run
        recover         are we recovering? (if so, test if the
                        directories are there instead of creating them)

        OUTPUTS
        in_dir          directory to put step's inputs
        out_dir         directory where step will put its outputs
        log_dir         directory to put logs in
        """
        log_dir = os.path.join(step_run_dir, dirnames.LOG_DIR)
        out_dir = os.path.join(step_run_dir, dirnames.OUT_DIR)
        in_dir = os.path.join(step_run_dir, dirnames.IN_DIR)

        for workdir in [step_run_dir, log_dir, out_dir, in_dir]:
            file_access_utils.set_up_directory(workdir, tolerate=recover)
            file_access_utils.configure_sandbox_permissions(workdir)
        return (in_dir, out_dir, log_dir)

    @staticmethod
    def finish_cable(cable_execute_dict):
        """
        Finishes an un-reused cable that has already been prepared for execution.

        This code is intended to be run as a Slurm task, so it's a static method that
        takes a dictionary rather than a RunCableExecuteInfo object.

        If we are reaching this point, we know that the data required for input_dataset is either
        in place in the sandbox or available in the database.

        This function is called by finish_step, because we want it to be called by the same
        worker(s) as the step is, so its output is on the local filesystem of the worker, which
        may be a remote MPI host.
        """
        # Retrieve info from the database using the PKs passed.
        curr_record = RunComponent.objects.get(pk=cable_execute_dict["cable_record_pk"]).definite
        input_dataset = Dataset.objects.get(pk=cable_execute_dict["input_dataset_pk"])
        output_path = cable_execute_dict["output_path"]
        recovering_record = None
        if cable_execute_dict["recovering_record_pk"] is not None:
            recovering_record = RunComponent.objects.get(
                pk=cable_execute_dict["recovering_record_pk"]
            ).definite
        curr_ER = None

        make_dataset = curr_record.keeps_output()
        dataset_name = curr_record.output_name()
        dataset_desc = curr_record.output_description()

        recover = recovering_record is not None
        invoking_record = recovering_record or curr_record
        if recover:
            curr_record.begin_recovery(save=True)

        # It's possible that this cable was completed in the time between the Manager queueing this task
        # and the worker starting it.  If so we can use the ExecRecord, and maybe even fully reuse it
        # if this was not called by finish_step.
        succeeded_yet = False
        while not succeeded_yet:
            try:
                with transaction.atomic():
                    if cable_execute_dict["execrecord_pk"] is not None:
                        curr_ER = ExecRecord.objects.get(pk=cable_execute_dict["execrecord_pk"])
                        can_reuse = curr_record.check_ER_usable(curr_ER)
                    else:
                        curr_ER, can_reuse = curr_record.get_suitable_ER(input_dataset, reuse_failures=False)

                    if curr_ER is not None:
                        if curr_ER.generator.record.is_quarantined():
                            # We will re-attempt; if it's fixed, then we un-quarantine the ExecRecord.
                            logger.debug(
                                "Found quarantined ExecRecord %s; will decontaminate if successful",
                                curr_ER
                            )

                        # If it was unsuccessful, we bail.  Alternately, if we can fully reuse it now and don't need to
                        # execute it for a parent step, we can return.
                        if (not can_reuse["successful"] or
                                (can_reuse["fully reusable"] and cable_execute_dict["by_step_pk"] is None)):
                            logger.debug("ExecRecord %s is reusable (successful = %s)",
                                         curr_ER, can_reuse["successful"])
                            curr_record.reused = True
                            curr_record.execrecord = curr_ER

                            if can_reuse["successful"]:
                                # This is a fully-reusable record.
                                curr_record.finish_successfully(save=True)
                            else:
                                # Mark curr_record as failed; if this is a recovery, recovering_record
                                # will be handled elsewhere
                                curr_record.finish_failure(save=True)

                            curr_record.complete_clean()
                            return curr_record
                succeeded_yet = True
            except (OperationalError, InternalError):
                wait_time = random.random()
                logger.debug("Database conflict.  Waiting for %f seconds before retrying.",
                             wait_time)
                time.sleep(wait_time)

        # We're now committed to actually running this cable.
        input_dataset_path = cable_execute_dict["input_dataset_path"]
        user = User.objects.get(pk=cable_execute_dict["user_pk"])

        # Preconditions to test.
        # assert curr_record is not None
        input_dataset_in_sdbx = file_access_utils.file_exists(input_dataset_path)

        cable = curr_record.definite.component

        # Write the input dataset to the sandbox if necessary.
        # FIXME at some point in the future this will have to be updated to mean "write to the local sandbox".
        if not input_dataset_in_sdbx:
            logger.debug("Dataset is in the DB - writing it to the file system")
            file_path = None
            if bool(input_dataset.dataset_file):
                file_path = input_dataset.dataset_file.path
            elif input_dataset.external_path:
                file_path = input_dataset.external_absolute_path()

            copy_start = timezone.now()
            fail_now = False

            try:
                copy_and_confirm(file_path, input_dataset_path)
            except (IOError, FileCreationError):
                logger.error("Could not copy file %s to file %s.",
                             file_path,
                             input_dataset_path,
                             exc_info=True)
                fail_now = True
            finally:
                copy_end = timezone.now()

            if fail_now:
                with transaction.atomic():
                    # Create a failed IntegrityCheckLog.
                    iic = IntegrityCheckLog(
                        dataset=input_dataset,
                        runcomponent=curr_record,
                        read_failed=True,
                        start_time=copy_start,
                        end_time=copy_end,
                        user=user
                    )
                    iic.clean()
                    iic.save()

            else:
                # Perform an integrity check since we've just copied this file to the sandbox for the
                # first time.
                logger.debug("Checking file just copied to sandbox for integrity.")
                check = input_dataset.check_integrity(
                    input_dataset_path,
                    user,
                    execlog=None,
                    runcomponent=curr_record
                )
                fail_now = check.is_fail()

            # Check again in case it failed in the previous else block.
            if fail_now:
                curr_record.cancel_running(save=True)
                return curr_record

        if not recover:
            # Get or create CDT for cable output (Evaluate cable wiring)
            output_CDT = input_dataset.get_cdt()
            if not cable.is_trivial():
                output_CDT = cable.find_compounddatatype() or cable.create_compounddatatype()

        else:
            logger.debug("Recovering - will update old ER")
            output_dataset = curr_ER.execrecordouts.first().dataset
            output_CDT = output_dataset.get_cdt()

        curr_log = ExecLog.create(curr_record, invoking_record)

        # Run cable (this completes the ExecLog).
        cable_failed = False
        file_size_unstable = False
        try:
            md5 = cable.run_cable(input_dataset_path, output_path, curr_record, curr_log)
        except (OSError, FileCreationError) as e:
            cable_failed = True
            logger.error("could not run cable %s to file %s.",
                         input_dataset_path,
                         output_path,
                         exc_info=True)
            if hasattr(e, "md5"):
                # Cable failed on running so there's an MD5 attached.
                md5 = e.md5
                file_size_unstable = True

        # Here, we're authoring/modifying an ExecRecord, so we use a transaction.
        preexisting_ER = curr_ER is not None
        succeeded_yet = False
        while not succeeded_yet:
            try:
                with transaction.atomic():
                    bad_output = False
                    start_time = timezone.now()
                    if cable_failed:
                        end_time = timezone.now()
                        bad_output = True

                        # It's conceivable that the linking could fail in the
                        # trivial case; in which case we should associate a "missing data"
                        # check to input_dataset == output_dataset.
                        if cable.is_trivial():
                            output_dataset = input_dataset
                        elif curr_ER is None:
                            if not file_size_unstable:
                                output_dataset = Dataset.create_empty(
                                    cdt=output_CDT,
                                    file_source=curr_record
                                )
                            else:
                                output_dataset = Dataset.create_dataset(
                                    output_path,
                                    cdt=output_CDT,
                                    keep_file=make_dataset,
                                    name=dataset_name,
                                    description=dataset_desc,
                                    file_source=curr_record,
                                    check=False,
                                    precomputed_md5=md5
                                )
                        else:
                            output_dataset = curr_ER.execrecordouts.first().dataset

                        if not file_size_unstable:
                            output_dataset.mark_missing(start_time, end_time, curr_log, user)
                        else:
                            output_dataset.mark_file_not_stable(start_time, end_time, curr_log, user)

                        # Update state variables.
                        curr_record.finish_failure(save=True)
                        if preexisting_ER:
                            curr_ER.quarantine_runcomponents()

                    elif cable.is_trivial():
                        output_dataset = input_dataset

                    else:
                        # Do we need to keep this output?
                        if not make_dataset:
                            logger.debug("Cable doesn't keep output: not creating a dataset")

                        if curr_ER is not None:
                            output_dataset = curr_ER.execrecordouts.first().dataset
                            if make_dataset:
                                output_dataset.register_file(output_path)

                        else:
                            output_dataset = Dataset.create_dataset(
                                output_path,
                                cdt=output_CDT,
                                keep_file=make_dataset,
                                name=dataset_name,
                                description=dataset_desc,
                                file_source=curr_record,
                                check=False,
                                precomputed_md5=md5
                            )

                    # Link the ExecRecord to curr_record if necessary, creating it if necessary also.
                    if not recover:
                        if curr_ER is None:
                            logger.debug("No ExecRecord already in use - creating fresh cable ExecRecord")
                            # Make ExecRecord, linking it to the ExecLog.
                            curr_ER = ExecRecord.create(
                                curr_log,
                                cable,
                                [input_dataset],
                                [output_dataset]
                            )
                        # Link ER to RunCable (this may have already been linked; that's fine).
                        curr_record.link_execrecord(curr_ER, reused=False)

                    else:
                        logger.debug("This was a recovery - not linking RSIC/RunOutputCable to ExecRecord")
                succeeded_yet = True
            except (OperationalError, InternalError):
                wait_time = random.random()
                logger.debug("Database conflict.  Waiting for %f seconds before retrying.", wait_time)
                time.sleep(wait_time)

        ####
        # Check outputs
        ####
        if not bad_output:
            # Case 1: the cable is trivial.  Don't check the integrity, it was already checked
            # when it was first written to the sandbox.
            if cable.is_trivial():
                logger.debug("Cable is trivial; skipping integrity check")

            else:
                # Case 2a: ExecRecord already existed and its output had been properly vetted.
                # Case 2b: this was a recovery.
                # Check the integrity of the output.
                if ((preexisting_ER and (output_dataset.is_OK() or
                                         output_dataset.any_failed_checks())) or
                        cable.is_trivial() or recover):
                    logger.debug("Performing integrity check of trivial or previously generated output")
                    # Perform integrity check.  Note: if this fails, it will notify all RunComponents using it.
                    check = output_dataset.check_integrity(output_path,
                                                           user,
                                                           curr_log,
                                                           newly_computed_MD5=md5)

                # Case 3: the Dataset, one way or another, is not properly vetted.
                else:
                    logger.debug("Output has no complete content check; performing content check")
                    summary_path = "{}_summary".format(output_path)
                    # Perform content check.  Note: if this fails, it will notify all RunComponents using it.
                    check = output_dataset.check_file_contents(
                        output_path,
                        summary_path,
                        cable.min_rows_out,
                        cable.max_rows_out,
                        curr_log,
                        user
                    )

                if check.is_fail():
                    curr_record.finish_failure(save=True)

        logger.debug("DONE EXECUTING %s '%s'", type(cable).__name__, cable)

        # End. Return curr_record.  Update state if necessary (i.e. if it hasn't already failed).
        if curr_record.is_running():
            curr_record.finish_successfully(save=True)
        curr_record.complete_clean()
        return curr_record

    @staticmethod
    def step_execution_setup(step_execute_dict):
        """
        Prepare to carry out the task specified by step_execute_dict.

        This is intended to be run as a Slurm job, so is a static method.
        I.e. in fleet.workers.Managersubmit_runstep():
            a dict containing pertinent information about the required run is written into a JSON file.
            Then, using sbatch, a slurm job that calls  'manage.py step_helper' is submitted.
        Upon execution by slurm, that command retrieves the dict from the json file and calls
        this static method with the dict as an argument.

        Precondition: the task must be ready to go, i.e. its inputs must all be in place.  Also
        it should not have been run previously.  This should not be a RunStep representing a Pipeline.
        """
        # Break out the execution info.
        curr_ER = None
        preexisting_ER = step_execute_dict["execrecord_pk"] is not None
        cable_info_dicts = step_execute_dict["cable_info_dicts"]
        step_run_dir = step_execute_dict["step_run_dir"]

        recovering_record = None
        if step_execute_dict["recovering_record_pk"] is not None:
            recovering_record = RunComponent.objects.get(
                pk=step_execute_dict["recovering_record_pk"]
            ).definite

        curr_RS = RunStep.objects.get(pk=step_execute_dict["runstep_pk"])  # already start()ed
        assert not curr_RS.pipelinestep.is_subpipeline()
        method = curr_RS.pipelinestep.transformation.definite

        try:
            recover = recovering_record is not None
            invoking_record = recovering_record or curr_RS

            if recover:
                curr_RS.begin_recovery(save=True)

            ####
            # Gather inputs: finish all input cables -- we want them written to the sandbox now, which is never
            # done by reuse_or_prepare_cable.
            input_paths = []
            completed_cable_pks = []
            for curr_execute_dict in cable_info_dicts:
                # Update the cable execution information with the recovering record if applicable.
                if recover:
                    curr_execute_dict["recovering_record_pk"] = recovering_record.pk

                curr_RSIC = Sandbox.finish_cable(curr_execute_dict)
                completed_cable_pks.append(curr_RSIC.pk)

                # Cable failed, return incomplete RunStep.
                curr_RSIC.refresh_from_db()
                if not curr_RSIC.is_successful():
                    logger.error("PipelineStepInputCable %s %s.", curr_RSIC, curr_RSIC.get_state_name())

                    # Cancel the other RunSICs for this step.
                    for rsic in curr_RS.RSICs.exclude(pk__in=completed_cable_pks):
                        rsic.cancel_pending(save=True)

                    # Update state variables.
                    curr_RS.refresh_from_db()
                    curr_RS.cancel_running(save=True)  # Transition: Running->Cancelled
                    curr_RS.complete_clean()
                    return curr_RS

                # Cable succeeded.
                input_paths.append(curr_execute_dict["output_path"])

            # Check again to see if the ExecRecord was completed while this task
            # waited on the queue.  If this isn't a recovery, we can just stop.
            if recover:
                assert preexisting_ER
                curr_ER = ExecRecord.objects.get(pk=step_execute_dict["execrecord_pk"])
            else:
                curr_RS.reused = False
                curr_RS.save()

            pipelinestep = curr_RS.pipelinestep

            # Run code, creating ExecLog and MethodOutput.
            curr_log = ExecLog.create(curr_RS, invoking_record)

            # Check the integrity of the code before we run.
            if not pipelinestep.transformation.definite.check_md5():  # this checks the driver and dependencies
                logger.error("Method code has gone corrupt for %s or its "
                             "dependencies; stopping step",
                             pipelinestep.transformation.definite.driver)
                # Stop everything!
                curr_log.start()
                curr_log.methodoutput.are_checksums_OK = False
                curr_log.methodoutput.save()
                curr_log.stop(save=True, clean=False)

                # Update state variables:
                curr_RS.finish_failure(save=True)
                curr_RS.complete_clean()

                logger.debug("Quarantining any other RunComponents using the same ExecRecord")
                if preexisting_ER:
                    curr_ER.quarantine_runcomponents()  # this is transaction'd

            # Install the code.
            try:
                method.install(step_run_dir)
            except (IOError, FileCreationError):
                logger.error("Method code failed to install; stopping step.",
                             exc_info=True)
                curr_log.start()
                curr_log.methodoutput.install_failed = True
                curr_log.methodoutput.save()
                curr_log.stop(save=True, clean=False)
                curr_RS.finish_failure(save=True)
                curr_RS.complete_clean()

        except KeyboardInterrupt:
            curr_RS.cancel_running(save=True)
            raise StopExecution(
                "Execution of step {} (method {}) was stopped during setup.".format(
                    curr_RS,
                    method
                )
            )

        return curr_RS

    def submit_step_execution(self, step_execute_info, after_okay,
                              slurm_sched_class,
                              docker_handler_class):
        """
        Submit the step execution to Slurm.

        step_execute_info is an object of class RunStepExecuteInfo.
        after_okay is a list of Slurm job handles; These are submitted jobs that must be
        completed (successfully) before the execution of this driver can proceed.

        NOTE: when submitting the driver to slurm, the jobscript to run must be wrapped.
        This is because at the time of submission, the driver code has not been installed yet,
        and slurm submission will fail.
        The way of getting around this problem is the following:
        a) at slurm submission time:
            create a small shell script wrapper which calls the not-yet existing driver.
        b) at slurm run time:
           the setup script, which must run successfully before this one is started,
           has copied the driver into place, which the wrapper code now can run.

        NOTE: for docker support, we wrap the wrapper script again in order to launch it
        within a docker container.
        """
        # From here on the code is assumed not to be corrupted, and all the required files
        # have been placed in their right places.
        curr_RS = step_execute_info.runstep

        input_paths = [x.output_path for x in step_execute_info.cable_info_list]
        dependencies = curr_RS.pipelinestep.transformation.definite.dependencies
        dependency_paths = [os.path.join(dep.path, dep.get_filename())
                            for dep in dependencies.all()]
        # Driver name
        driver = curr_RS.pipelinestep.transformation.definite.driver
        driver_filename = driver.coderesource.filename

        coordinates = curr_RS.get_coordinates()
        if len(coordinates) == 1:
            coord_str = coordinates[0]
        else:
            coord_str = "({})".format(",".join(str(x) for x in coordinates))
        job_name = "r{}s{}driver[{}]".format(
            curr_RS.top_level_run.pk,
            coord_str,
            driver_filename
        )
        logger.debug("Submitting driver '%s', task_pk %d", driver_filename, curr_RS.pk)
        # Collect information we need for the wrapper script
        host_rundir = step_execute_info.step_run_dir
        # NOTE: currently, we always launch a driver with the default image_id
        launch_args = docker_handler_class.generate_launch_args(host_rundir,
                                                                input_paths,
                                                                step_execute_info.output_paths,
                                                                driver_filename,
                                                                dependency_paths)
        job_handle = slurm_sched_class.submit_job(
            host_rundir,
            launch_args[0],
            launch_args[1:],
            self.uid,
            self.gid,
            self.run.priority,
            step_execute_info.threads_required,
            step_execute_info.driver_stdout_path(),
            step_execute_info.driver_stderr_path(),
            after_okay=after_okay,
            job_name=job_name,
            mem=curr_RS.pipelinestep.transformation.definite.memory
        )

        return job_handle

    @staticmethod
    def step_execution_bookkeeping(step_execute_dict):
        """
        Perform bookkeeping after step execution.

        This is intended to run as a Slurm task, so it's coded as a static method.
        """
        # Break out step_execute_dict.
        curr_RS = RunStep.objects.get(pk=step_execute_dict["runstep_pk"])
        curr_log = curr_RS.log
        curr_ER = (None if step_execute_dict["execrecord_pk"] is None
                   else ExecRecord.objects.get(pk=step_execute_dict["execrecord_pk"]))
        pipelinestep = curr_RS.pipelinestep
        output_paths = step_execute_dict["output_paths"]
        user = User.objects.get(pk=step_execute_dict["user_pk"])

        recovering_record = None
        if step_execute_dict["recovering_record_pk"] is not None:
            recovering_record = RunComponent.objects.get(
                pk=step_execute_dict["recovering_record_pk"]
            ).definite
        recover = recovering_record is not None

        cable_info_dicts = step_execute_dict["cable_info_dicts"]
        inputs_after_cable = []
        for curr_execute_dict in cable_info_dicts:
            curr_RSIC = RunSIC.objects.get(pk=curr_execute_dict["cable_record_pk"])
            inputs_after_cable.append(curr_RSIC.execrecord.execrecordouts.first().dataset)

        bad_execution = False
        bad_output_found = False
        integrity_checks = {}  # {output_idx: check}
        md5s = {}  # {output_idx: md5}
        try:
            succeeded_yet = False
            while not succeeded_yet:
                try:
                    with transaction.atomic():
                        # Create outputs.
                        # bad_output_found indicates we have detected problems with the output.
                        bad_output_found = False
                        bad_execution = not curr_log.is_successful()
                        output_datasets = []
                        logger.debug("ExecLog.is_successful() == %s", not bad_execution)

                        # if not recover:
                        if curr_ER is not None:
                            if not recover:
                                logger.debug("Filling in pre-existing ExecRecord with PipelineStep outputs")
                            else:
                                logger.debug("Examining outputs of pre-existing ExecRecord after recovery")
                        else:
                            logger.debug("Creating new Datasets for PipelineStep outputs")

                        for i, curr_output in enumerate(pipelinestep.outputs):
                            output_path = output_paths[i]
                            output_CDT = curr_output.get_cdt()
                            dataset_name = curr_RS.output_name(curr_output)
                            dataset_desc = curr_RS.output_description(curr_output)
                            make_dataset = curr_RS.keeps_output(curr_output)

                            # Check that the file exists, as we did for cables.
                            start_time = timezone.now()
                            file_confirmed = True

                            try:
                                md5s[i] = file_access_utils.confirm_file_created(output_path)
                            except FileCreationError as e:
                                logger.warn("File at %s was not properly created.", output_path, exc_info=True)
                                file_confirmed = False

                                if hasattr(e, "md5"):
                                    md5s[i] = e.md5
                                else:
                                    md5s[i] = None

                            if not file_confirmed:
                                end_time = timezone.now()
                                bad_output_found = True

                                if curr_ER is not None:
                                    output_dataset = curr_ER.get_execrecordout(curr_output).dataset
                                    if md5s[i] is None:
                                        output_dataset.mark_missing(start_time, end_time, curr_log, user)
                                    else:
                                        output_dataset.mark_file_not_stable(start_time, end_time, curr_log, user)

                                elif md5s[i] is None:
                                    output_dataset = Dataset.create_empty(
                                        cdt=output_CDT,
                                        file_source=curr_RS
                                    )
                                    output_dataset.mark_missing(start_time, end_time, curr_log, user)
                                else:
                                    output_dataset = Dataset.create_dataset(
                                        output_path,
                                        cdt=output_CDT,
                                        keep_file=make_dataset,
                                        name=dataset_name,
                                        description=dataset_desc,
                                        file_source=curr_RS,
                                        check=False,
                                        precomputed_md5=md5s[i]
                                    )
                                    output_dataset.mark_file_not_stable(start_time, end_time, curr_log, user)

                            else:
                                # If necessary, create new Dataset for output, and create the Dataset
                                # if it's to be retained.
                                if curr_ER is not None:
                                    output_ERO = curr_ER.get_execrecordout(curr_output)
                                    if not make_dataset:
                                        output_dataset = output_ERO.dataset
                                    else:
                                        # Wrap in a transaction to prevent
                                        # concurrent authoring of Datasets to
                                        # an existing Dataset.
                                        with transaction.atomic():
                                            output_dataset = Dataset.objects.select_for_update().filter(
                                                pk=output_ERO.dataset.pk).first()
                                            if not output_dataset.has_data():
                                                check = output_dataset.check_integrity(
                                                    output_path,
                                                    checking_user=user,
                                                    execlog=curr_log,
                                                    notify_all=True,
                                                    newly_computed_MD5=md5s[i]
                                                )
                                                integrity_checks[i] = check
                                                if not check.is_fail():
                                                    output_dataset.register_file(output_path)

                                else:
                                    output_dataset = Dataset.create_dataset(
                                        output_path,
                                        cdt=output_CDT,
                                        keep_file=make_dataset,
                                        name=dataset_name,
                                        description=dataset_desc,
                                        file_source=curr_RS,
                                        check=False,
                                        precomputed_md5=md5s[i]
                                    )
                                    logger.debug("First time seeing file: saved md5 %s",
                                                 output_dataset.MD5_checksum)

                            output_datasets.append(output_dataset)

                        # Create ExecRecord if there isn't already one.
                        if curr_ER is None:
                            # Make new ExecRecord, linking it to the ExecLog
                            logger.debug("Creating fresh ExecRecord")
                            curr_ER = ExecRecord.create(
                                curr_log,
                                pipelinestep,
                                inputs_after_cable,
                                output_datasets
                            )

                        # Link ExecRecord to RunStep (it may already have been linked; that's fine).
                        # It's possible that reused may be either True or False (e.g. this is a recovery).
                        curr_RS.link_execrecord(curr_ER, curr_RS.reused)

                    succeeded_yet = True
                except (OperationalError, InternalError):
                    wait_time = random.random()
                    logger.debug(
                        "Database conflict.  Waiting for %f seconds before retrying.",
                        wait_time
                    )
                    time.sleep(wait_time)

            # Having confirmed their existence, we can now perform proper integrity/content
            # checks on the outputs.
            for i, curr_output in enumerate(pipelinestep.outputs):
                output_path = output_paths[i]
                output_dataset = curr_ER.get_execrecordout(curr_output).dataset
                check = None

                if bad_execution:
                    logger.debug("Execution was unsuccessful; no check on %s was done", output_path)
                elif bad_output_found:
                    logger.debug("Bad output found; no check on %s was done", output_path)

                # Recovering or filling in old ER? Yes.
                elif curr_ER is not None:
                    # Perform integrity check.
                    logger.debug("Dataset has been computed before, checking integrity of %s",
                                 output_dataset)
                    check = integrity_checks.get(i)
                    if check is None:
                        check = output_dataset.check_integrity(output_path, user, curr_log,
                                                               newly_computed_MD5=md5s[i])

                    # We may also need to perform a content check if there isn't a complete
                    # one already.
                    if not check.is_fail() and not output_dataset.content_checks.filter(
                            end_time__isnull=False).exists():
                        logger.debug("Output has no complete content check; performing content check")
                        summary_path = "{}_summary".format(output_path)
                        check = output_dataset.check_file_contents(
                            output_path,
                            summary_path,
                            curr_output.get_min_row(),
                            curr_output.get_max_row(),
                            curr_log,
                            user
                        )

                # Recovering or filling in old ER? No.
                else:
                    # Perform content check.
                    logger.debug("%s is new data - performing content check", output_dataset)
                    summary_path = "{}_summary".format(output_path)
                    check = output_dataset.check_file_contents(
                        output_path,
                        summary_path,
                        curr_output.get_min_row(),
                        curr_output.get_max_row(),
                        curr_log,
                        user
                    )

                # Check OK? No.
                if check and check.is_fail():
                    logger.warn("%s failed for %s", check.__class__.__name__, output_path)
                    bad_output_found = True

                # Check OK? Yes.
                elif check:
                    logger.debug("%s passed for %s", check.__class__.__name__, output_path)

            curr_ER.complete_clean()

            # End.  Return curr_RS.  Update the state.
            if not bad_output_found and not bad_execution:
                curr_RS.finish_successfully(save=True)
            else:
                curr_RS.finish_failure(save=True)
                logger.debug("Quarantining any other RunComponents using the same ExecRecord")
                curr_ER.quarantine_runcomponents()  # this is transaction'd

            curr_RS.complete_clean()
            return curr_RS

        except KeyboardInterrupt:
            # Execution was stopped somewhere outside of run_code (that would
            # have been caught above).
            curr_RS.cancel_running(save=True)
            raise StopExecution(
                "Execution of step {} (method {}) was stopped during bookkeeping.".format(
                    curr_RS,
                    curr_RS.pipelinestep.transformation.definite
                )
            )


class RunPlan(object):
    """
    Hold the plan for which steps will be executed in a sandbox.

    Also holds the dependencies between steps and cables, as well as the
    ExecRecord that will be reused for each step and cable that doesn't have
    to be run this time.

    This is required to avoid incoherencies where steps use different results from
    the same preceding step in a Pipeline (for example, if that step is not deterministic.
    """
    def load(self, top_level_run, inputs, subpipeline_step=None):
        """ Load the steps from the pipeline and dataset dependencies.

        Links pipeline inputs and step outputs to the inputs of other steps.

        top_level_run refers to the top-level top_level_run.  subpipeline is None or the PipelineStep
        that represents the sub-Pipeline.  This may be more than one layer deep in
        the top-level Pipeline.
        """
        self.top_level_run = top_level_run
        self.run = top_level_run if not subpipeline_step else None
        self.step_plans = []
        self.inputs = [DatasetPlan(input_item) for input_item in inputs]
        if subpipeline_step:
            assert subpipeline_step.transformation.is_pipeline()
            self.pipeline = subpipeline_step.transformation.definite
        else:
            self.pipeline = top_level_run.pipeline

        for step in self.pipeline.steps.all():
            step_plan = StepPlan(step.step_num)
            step_plan.pipeline_step = step
            self.step_plans.append(step_plan)
            for output in step.transformation.outputs.all():
                step_plan.outputs.append(DatasetPlan(step_num=step.step_num,
                                                     output_num=output.dataset_idx))
            for cable in step.cables_in.order_by("dest__dataset_idx"):
                if cable.source_step == 0:
                    input_index = cable.source.definite.dataset_idx-1
                    input_plan = self.inputs[input_index]
                else:
                    step_index = cable.source_step-1
                    output_index = cable.source.definite.dataset_idx-1
                    input_plan = self.step_plans[step_index].outputs[output_index]
                step_plan.inputs.append(input_plan)

            if step.is_subpipeline():
                step_plan.subrun_plan = RunPlan()
                step_plan.subrun_plan.load(top_level_run, inputs, subpipeline_step=step)

        self.outputs = []
        for cable in self.pipeline.outcables.order_by("output_idx"):
            step_index = cable.source_step-1
            output_index = cable.source.definite.dataset_idx-1
            output_plan = self.step_plans[step_index].outputs[output_index]
            self.outputs.append(output_plan)

    def find_consistent_execution(self):
        """
        Flesh out the plan for executing this run.

        First, StepPlans are created for each RunStep, creating RunSteps if necessary.
        Then, suitable ExecRecords are identified where possible and added to the StepPlans.
        Lastly, we iterate over all of the StepPlans until we've identified a consistent
        plan for execution that will not lead to incoherent results.
        """
        self.create_run_steps()
        self.find_ERs()
        self.identify_changes()

    def create_run_steps(self, subrun=None):
        """
        Find or create the RunSteps in the Run.  Sub-run RunPlans are also assigned Runs at this point.
        """
        if subrun:
            self.run = subrun

        for step_plan in self.step_plans:
            step_subrun = None
            run_step = self.run.runsteps.filter(
                pipelinestep__step_num=step_plan.step_num).first()
            if run_step is None:
                run_step = RunStep.create(step_plan.pipeline_step, self.run, start=False)

                if run_step.pipelinestep.transformation.is_pipeline():
                    step_subrun = Run(
                        user=self.top_level_run.user,
                        pipeline=run_step.pipelinestep.transformation.definite,
                        parent_runstep=run_step
                    )
                    step_subrun.save()
                    step_subrun.users_allowed.add(*self.top_level_run.users_allowed.all())
                    step_subrun.groups_allowed.add(*self.top_level_run.groups_allowed.all())

            elif run_step.pipelinestep.transformation.is_pipeline():
                step_subrun = run_step.child_run

            if step_subrun:
                step_plan.subrun_plan.create_run_steps(step_subrun)

            step_plan.run_step = run_step

    def find_ERs(self):
        """
        Looks for suitable ExecRecords for the RunSteps and populates step_plan with them.
        """
        for step_plan in self.step_plans:
            if step_plan.pipeline_step.is_subpipeline():
                step_plan.subrun_plan.find_ERs()
                continue
            elif step_plan.run_step.execrecord is not None:
                step_plan.execrecord = step_plan.run_step.execrecord
            else:
                input_datasets = [plan.dataset for plan in step_plan.inputs]
                if all(input_datasets):
                    method = step_plan.pipeline_step.transformation.definite
                    if method.reusable == Method.NON_REUSABLE:
                        continue
                    execrecord, summary = step_plan.run_step.get_suitable_ER(input_datasets, reuse_failures=False)

                    if not summary:
                        # no exec record, have to run
                        continue
                    if (not summary['fully reusable'] and
                            method.reusable != Method.DETERMINISTIC):
                        continue
                    step_plan.execrecord = execrecord

                if step_plan.execrecord is None:
                    # We couldn't find a suitable ExecRecord.
                    continue

            execrecordouts = step_plan.execrecord.execrecordouts.all()
            for execrecordout in execrecordouts:
                generic_output = execrecordout.generic_output
                dataset_idx = generic_output.transformationoutput.dataset_idx
                output = step_plan.outputs[dataset_idx-1]
                output.dataset = execrecordout.dataset

    def identify_changes(self):
        is_changed = True
        while is_changed:
            is_changed = self.walk_backward() or self.walk_forward()

    def walk_backward(self):
        """
        Walk backward through the steps, flagging steps that need to be run.

        @return: True if any new steps were flagged for running.
        """
        is_changed = False
        for step_plan in reversed(self.step_plans):
            if step_plan.pipeline_step.is_subpipeline():
                is_changed = step_plan.subrun_plan.walk_backward() or is_changed

            elif not step_plan.execrecord:
                for input_plan in step_plan.inputs:
                    if not input_plan.has_data() and input_plan.step_num is not None:
                        # Note: if input_plan.step_num is None (i.e. this is an input to the Run)
                        # then we leave it -- it will be caught later in the execution.

                        source_plan = self.step_plans[input_plan.step_num-1]
                        is_changed = source_plan.check_rerun(input_plan.output_num) or is_changed

        return is_changed

    def walk_forward(self):
        """
        Walk forward through the steps, flagging steps that need to be run.

        @return: True if any new steps were flagged for running.
        """
        is_changed = False
        for step_plan in self.step_plans:
            if step_plan.pipeline_step.is_subpipeline():
                is_changed = step_plan.subrun_plan.walk_forward() or is_changed

            else:
                for input_plan in step_plan.inputs:
                    if not input_plan.dataset and step_plan.execrecord:
                        step_plan.execrecord = None
                        for output_plan in step_plan.outputs:
                            output_plan.dataset = None
                        is_changed = True

        return is_changed


class StepPlan(object):
    """ Plan whether a step will actually be executed.
    """
    def __init__(self, step_num):
        self.step_num = step_num
        self.execrecord = None
        self.pipeline_step = None
        self.run_step = None
        self.inputs = []
        self.outputs = []

    def check_rerun(self, output_idx=None):
        """
        Check that this step can recreate one of its missing outputs.

        If the execrecord cannot be restored, don't use it, and mark all
        the outputs as having no data.  If this StepPlan is for a sub-Pipeline,
        then output_idx must be specified; otherwise, it is ignored.

        @return: True if the execrecord had to be abandoned.
        """
        if self.pipeline_step.is_subpipeline():
            assert output_idx is not None
            # Look up the step that produced this output.
            curr_output_plan = self.subrun_plan.outputs[output_idx-1]
            source_step_plan = self.subrun_plan.step_plans[curr_output_plan.step_num-1]

            return source_step_plan.check_rerun(output_idx=output_idx)

        else:
            if not self.execrecord:
                # It didn't have an ExecRecord so there is no change to whether this needs to be rerun or not.
                return False

            # At this point, we know self.execrecord exists.
            method = self.run_step.transformation.definite
            if method.reusable == Method.DETERMINISTIC:
                # There will be no trouble reusing the execrecord.
                return False

            # At this point we know the Method is not deterministic, so we'll have to re-run it.
            self.execrecord = None
            for output_plan in self.outputs:
                output_plan.dataset = None
            return True

    def __repr__(self):
        return 'StepPlan({})'.format(self.step_num)

    def __eq__(self, other):
        return isinstance(other, StepPlan) and other.step_num == self.step_num

    def __hash__(self):
        return hash(self.step_num)


class DatasetPlan(object):
    def __init__(self, dataset=None, step_num=None, output_num=None):
        self.dataset = dataset
        self.step_num = step_num
        self.output_num = output_num

    def has_data(self):
        return self.dataset and self.dataset.has_data()

    def __repr__(self):
        if self.dataset is not None:
            args = repr(self.dataset)
        elif self.step_num is not None:
            args = 'step_num={}, output_num={}'.format(self.step_num,
                                                       self.output_num)
        else:
            args = ''
        return 'DatasetPlan({})'.format(args)

    def __eq__(self, other):
        if not isinstance(other, DatasetPlan):
            return False
        if self.dataset is not None or other.dataset is not None:
            return self.dataset == other.dataset
        if self.step_num is not None:
            return (self.step_num == other.step_num and
                    self.output_num == other.output_num)
        return other is self

    def __hash__(self):
        if self.dataset is not None:
            return hash(self.dataset)
        if self.step_num is not None:
            return hash((self.step_num, self.output_num))
        return hash(self)


# A simple struct that holds the information required to perform a RunStep.
class RunStepExecuteInfo:
    def __init__(self, runstep, user, cable_info_list, execrecord, step_run_dir, log_dir, output_paths,
                 recovering_record=None):
        """
        Constructor.

        INPUTS
        cable_info_list: an ordered list of RunCableExecuteInfo objects, with each
         one corresponding to the input cable of the step with the same index.
        """
        self.runstep = runstep
        self.user = user
        self.cable_info_list = cable_info_list
        self.execrecord = execrecord
        self.step_run_dir = step_run_dir
        self.log_dir = log_dir
        self.recovering_record = recovering_record
        self.output_paths = output_paths
        # FIXME in the future this number may vary across runs.
        self.threads_required = None
        if not runstep.pipelinestep.is_subpipeline():
            self.threads_required = runstep.transformation.definite.threads

    def flag_for_recovery(self, recovering_record):
        assert self.recovering_record is None
        self.recovering_record = recovering_record

    def is_recovery(self):
        return self.recovering_record is not None

    def dict_repr(self):
        return {
            "runstep_pk": self.runstep.pk,
            "user_pk": self.user.pk,
            "cable_info_dicts": [x.dict_repr() for x in self.cable_info_list],
            "execrecord_pk": None if self.execrecord is None else self.execrecord.pk,
            "step_run_dir": self.step_run_dir,
            "log_dir": self.log_dir,
            "recovering_record_pk": self.recovering_record.pk if self.recovering_record is not None else None,
            "output_paths": self.output_paths,
            "threads_required": self.threads_required
        }

    def driver_stdout_path_prefix(self):
        """
        Return the filename prefix for the stdout log file.

        This is used not only to assemble the actual path of the file, but also
        for finding it in the sandbox after it's complete, as the actual path
        contains some Slurm macros.
        """
        return "step{}_stdout".format(self.runstep.pipelinestep.step_num)

    def driver_stderr_path_prefix(self):
        """
        Counterpart to driver_stdout_path_prefix for the stderr log file.
        """
        return "step{}_stderr".format(self.runstep.pipelinestep.step_num)

    def driver_stdout_path(self):
        return os.path.join(
            self.log_dir,
            "{}_slurmID%J_node%N.txt".format(self.driver_stdout_path_prefix())
        )

    def driver_stderr_path(self):
        return os.path.join(
            self.log_dir,
            "{}_slurmID%J_node%N.txt".format(self.driver_stderr_path_prefix())
        )

    def setup_stdout_path(self):
        return os.path.join(self.log_dir, "setup_out_slurmID%J_node%N.txt")

    def setup_stderr_path(self):
        return os.path.join(self.log_dir, "setup_err_slurmID%J_node%N.txt")

    def bookkeeping_stdout_path(self):
        return os.path.join(self.log_dir, "bookkeeping_out_slurmID%J_node%N.txt")

    def bookkeeping_stderr_path(self):
        return os.path.join(self.log_dir, "bookkeeping_err_slurmID%J_node%N.txt")


class RunCableExecuteInfo:
    def __init__(self, cable_record, user, execrecord, input_dataset, input_dataset_path, output_path,
                 log_dir, recovering_record=None, by_step=None, could_be_reused=False):
        """
        Constructor.
        """
        self.cable_record = cable_record
        self.user = user
        self.execrecord = execrecord
        self.input_dataset = input_dataset
        self.input_dataset_path = input_dataset_path
        self.output_path = output_path
        self.recovering_record = recovering_record
        self.by_step = by_step
        # FIXME will we ever need more than 1 thread for this?
        self.threads_required = 1
        self.ready_to_go = False
        self.cancelled = False
        self.could_be_reused = could_be_reused
        self.cable_info_dir = None
        self.log_dir = log_dir

    def flag_for_recovery(self, recovering_record, by_step=None):
        assert self.recovering_record is None
        self.recovering_record = recovering_record
        self.by_step = by_step

    def cancel(self):
        self.cancelled = True

    def is_recovery(self):
        return self.recovering_record is not None

    def dict_repr(self):
        return {
            "cable_record_pk": self.cable_record.pk,
            "user_pk": self.user.pk,
            "execrecord_pk": self.execrecord.pk if self.execrecord is not None else None,
            "input_dataset_pk": self.input_dataset.pk,
            "input_dataset_path": self.input_dataset_path,
            "output_path": self.output_path,
            "recovering_record_pk": self.recovering_record.pk if self.recovering_record is not None else None,
            "by_step_pk": None if self.by_step is None else self.by_step.pk,
            "threads_required": self.threads_required,
            "ready_to_go": self.ready_to_go,
            "cable_info_dir": self.cable_info_dir,
            "log_dir": self.log_dir
        }

    def stdout_prefix(self):
        """
        Reports the prefix of the stdout log file that should be produced.

        This is useful because we use this to identify the resulting log file,
        which will have some Slurm-specific information added to it.
        """
        if isinstance(self.cable_record, RunSIC):
            cable_idx = self.cable_record.component.dest.dataset_idx
            cable_type_str = "input"
        else:
            cable_idx = self.cable_record.component.source.dataset_idx
            cable_type_str = "output"

        return "{}{}_stdout".format(cable_type_str, cable_idx)

    def stderr_prefix(self):
        """
        Counterpart of stdout_prefix for the stderr log file.
        """
        if isinstance(self.cable_record, RunSIC):
            cable_idx = self.cable_record.component.dest.dataset_idx
            cable_type_str = "input"
        else:
            cable_idx = self.cable_record.component.source.dataset_idx
            cable_type_str = "output"

        return "{}{}_stderr".format(cable_type_str, cable_idx)

    def stdout_path(self):
        """
        Produces the actual path with Slurm macros that will be used for stdout logging.
        """
        return os.path.join(self.log_dir, "{}_slurmID%J_node%N.txt".format(self.stdout_prefix()))

    def stderr_path(self):
        """
        Produces the actual path with Slurm macros that will be used for stderr logging.
        """
        return os.path.join(self.log_dir, "{}_slurmID%J_node%N.txt".format(self.stderr_prefix()))

    def set_cable_info_dir(self, cable_info_dir):
        self.cable_info_dir = cable_info_dir
