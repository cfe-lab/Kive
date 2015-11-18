"""Code that is responsible for the execution of Pipelines."""

from collections import defaultdict
import logging
import os.path
import random
import shutil
import tempfile
import time

from django.utils import timezone
from django.db import transaction, OperationalError, InternalError
from django.contrib.auth.models import User

import archive.models
from archive.models import RunStep
from constants import dirnames, extensions
import file_access_utils
import librarian.models
import pipeline.models
from method.models import Method
from fleet.exceptions import StopExecution


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

    def __init__(self, user=None, my_pipeline=None, inputs=None, users_allowed=None, groups_allowed=None,
                 sandbox_path=None, run=None):
        """
        Sets up a sandbox environment to run a Pipeline: space on
        the file system, along with dataset_fs_map/socket_map/etc.

        INPUTS
        user          User running the pipeline.*
        my_pipeline   Pipeline to run.*
        inputs        Ordered list of datasets to feed into the pipeline.*
        users_allowed   Iterable (e.g. list or QuerySet) of Users.*
        groups_allowed  Iterable of Groups.*
        sandbox_path  Where on the filesystem to execute.*
        run           A Run object to fill in (e.g. if we're starting this using the fleet);
                      if None, we create our own.

        * parameter is ignored if run is specified

        PRECONDITIONS
        inputs must have real data
        """
        if run:
            self.run = run
            user = run.user
            my_pipeline = run.pipeline
            inputs = [x.dataset for x in run.inputs.order_by("index")]
            sandbox_path = run.sandbox_path
        else:
            self.run = my_pipeline.pipeline_instances.create(start_time=timezone.now(), user=user)
            users_allowed = users_allowed or []
            groups_allowed = groups_allowed or []
            self.run.users_allowed.add(*users_allowed)
            self.run.groups_allowed.add(*groups_allowed)
            self.run.save()

        assert all([i.has_data() for i in inputs])

        self.logger = logging.getLogger(self.__class__.__name__)
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
            dir=file_access_utils.sandbox_base_path())

        self.run.sandbox_path = self.sandbox_path
        self.run.save()

        in_dir = os.path.join(self.sandbox_path, dirnames.IN_DIR)
        self.out_dir = os.path.join(self.sandbox_path, dirnames.OUT_DIR)

        self.logger.debug("initializing maps")
        for i, pipeline_input in enumerate(inputs, start=1):
            corresp_pipeline_input = self.pipeline.inputs.get(dataset_idx=i)
            self.socket_map[(self.run, None, corresp_pipeline_input)] = pipeline_input
            self.dataset_fs_map[pipeline_input] = os.path.join(in_dir,
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

    def execute_cable(self, cable, parent_record, recovering_record=None, input_dataset=None, output_path=None):
        """Execute cable on the input.

        INPUTS
        cable           PSIC/POC to execute
        input_dataset        dataset fed into the PSIC/POC
        output_path     Where the output file should be written
        parent_record   If not a recovery, the Run or RunStep
                        executing the cable; if a recovery, the
                        RunAtomic invoking the recovery
        recover         whether or not to execute the step in recovery
                        mode

        OUTPUTS
        curr_record     RSIC/ROC that describes this execution.

        NOTES
        Recovering is to re-compute something reused to recover the data
        output_path is recovered using the maps.
        dataset_fs_map and cable_map will have been updated.

        PRECONDITIONS
        1) input_dataset has an appropriate CDT for feeding this cable.
        2) All the _maps are "up to date" for this step
        3) input_dataset is clean
        4) If not recovering, input_dataset and output_path must both be set
        """
        recover = recovering_record is not None

        # TODO: assertion for precondition 1?
        # FIXME: do we need this clean?
        assert input_dataset is None or input_dataset.clean() is None
        assert recover or (input_dataset and output_path)

        self.logger.debug("STARTING EXECUTING CABLE")

        # Recovering? No.
        if not recover:

            # Create new RSIC/ROC
            curr_record = archive.models.RunCable.create(cable, parent_record)
            self.logger.debug("Not recovering - created {}".format(curr_record.__class__.__name__))
            self.logger.debug("Cable keeps output? {}".format(curr_record.keeps_output()))

            # We bail out if the input has somehow been corrupted.
            if not input_dataset.is_OK():
                self.logger.debug("Input %s has corrupted.  Cancelling.", input_dataset)
                curr_record.is_cancelled = True
                curr_record.stop(save=True, clean=False)
                curr_record.complete_clean()

            # Attempt to reuse this PipelineCable.
            curr_ER = None

            succeeded_yet = False
            while not succeeded_yet:
                try:
                    with transaction.atomic():
                        curr_ER, can_reuse = curr_record.get_suitable_ER(input_dataset)
                        if curr_ER is not None:
                            output_dataset = curr_ER.execrecordouts.first().dataset
                            # If it was unsuccessful, we bail.  Alternately, if we can fully reuse it now
                            # and don't need to execute it for a parent step, we can return.
                            if not can_reuse["successful"] or can_reuse["fully reusable"]:
                                self.logger.debug(
                                    "ExecRecord {} is reusable (successful = {})".format(
                                        curr_ER, can_reuse["successful"])
                                )
                                curr_record.reused = True
                                curr_record.execrecord = curr_ER
                                curr_record.stop(save=False, clean=False)
                                curr_record.complete_clean()
                                curr_record.save()
                                self.update_cable_maps(curr_record, output_dataset, output_path)
                                return curr_record
                    succeeded_yet = True
                except (OperationalError, InternalError):
                    wait_time = random.random()
                    self.logger.debug("Database conflict.  Waiting for %f seconds before retrying.", wait_time)
                    time.sleep(wait_time)

            # At this point, we know we did not reuse an ExecRecord -- we're either filling one in
            # or creating a new one.
            curr_record.reused = False
            self.logger.debug("No ER to completely reuse - committed to executing cable")

            # Get or create CDT for cable output (Evaluate cable wiring)
            if cable.is_trivial():
                output_CDT = input_dataset.get_cdt()
            else:
                output_CDT = cable.find_compounddatatype() or cable.create_compounddatatype()

        # Recovering? Yes.
        else:
            self.logger.debug("Recovering - will update old ER")

            # Retrieve appropriate RSIC/ROC
            curr_record = self.cable_map[(parent_record, cable)]

            # Retrieve input_dataset and output_path from maps
            curr_ER = curr_record.execrecord
            input_dataset = curr_ER.execrecordins.first().dataset
            output_dataset = curr_ER.execrecordouts.first().dataset
            output_CDT = output_dataset.get_cdt()
            output_path = self.find_dataset(output_dataset)

        dataset_path = self.find_dataset(input_dataset)
        # Is input in the sandbox? No.
        if dataset_path is None:

            # Recover dataset.
            self.logger.debug("Symbolic only: running recover({})".format(input_dataset))

            successful_recovery = self.recover(input_dataset, curr_record)
            successful_str = "successful" if successful_recovery else "unsuccessful"
            self.logger.debug("Recovery was {}".format(successful_str))
            # Success? No.
            if not successful_recovery:

                # End. Return incomplete curr_record.
                self.logger.warn("Recovery failed - returning RSIC/ROC without ExecLog")
                if not recover:
                    curr_record.stop(save=True, clean=False)
                curr_record.clean()
                return curr_record

            # Success? Yes.
            dataset_path = self.find_dataset(input_dataset)
            self.logger.debug("Dataset recovered: running run_cable({})".format(dataset_path))

        # This is only meant for single-threaded execution so we set worker rank to 1.
        return _finish_cable_h(1, curr_record, cable, self.user, curr_ER, input_dataset, dataset_path,
                               output_path, output_CDT, recovering_record=recovering_record,
                               sandbox_to_update=self)

    def execute_step(self, pipelinestep, parent_run, recovering_record=None, inputs=None,
                     step_run_dir=None):
        """Execute the PipelineStep on the inputs.

        * If code is run, outputs go to paths specified in output_paths.
        * Requisite code is placed in step_run_dir (must be specified if not recovering).
        * If recovering, doesn't create new RS/ER; fills in old RS with a new EL
          In this case, step_run_dir is ignored and retrieved using the maps.

        If this is called during recovery, recovering_record must be the RunAtomic
        which is recovering this step.  If recovering_record is None then this is
        not a recovery.

        Inputs written to:  [step run dir]/input_data/step[step num]_[input name]
        Outputs written to: [step run dir]/output_data/step[step num]_[output name]
        Logs written to:    [step run dir]/logs/step[step num]_std(out|err).txt
        """
        recover = recovering_record is not None

        # Note: corrupt inputs will be caught by the input cables.
        assert recover or inputs is not None
        assert recover or step_run_dir

        # Create or retrieve RunStep and set up run/input/output/log directories.
        if recover:
            step_run_dir, curr_RS = self.ps_map[(parent_run, pipelinestep)]
            self.logger.debug("Recovering step {} in directory {}".format(pipelinestep, step_run_dir))
        else:
            curr_RS = archive.models.RunStep.create(pipelinestep, parent_run)
            input_names = ", ".join(str(i) for i in inputs)
            self.logger.debug("Executing step {} in directory {} on inputs {}"
                              .format(pipelinestep, step_run_dir, input_names))

        _in_dir, _out_dir, log_dir = _setup_step_paths(step_run_dir, recover)

        # Construct or retrieve output_paths.
        output_paths = []
        for curr_output in pipelinestep.outputs:
            if recover:
                corresp_dataset = self.socket_map[(parent_run, pipelinestep, curr_output)]
                output_paths.append(self.dataset_fs_map[corresp_dataset])
            else:
                output_paths.append(self.step_xput_path(curr_RS, curr_output, step_run_dir))

        # Run step's input cables, or retrieve inputs_after_cable from maps.
        inputs_after_cable = []
        for i, curr_input in enumerate(pipelinestep.inputs):
            if not recover:
                # Run a cable.
                corresp_cable = pipelinestep.cables_in.get(dest=curr_input)
                cable_path = self.step_xput_path(curr_RS, curr_input, step_run_dir)
                curr_RSIC = self.execute_cable(corresp_cable, curr_RS, recovering_record=None,
                                               input_dataset=inputs[i], output_path=cable_path)

                # Cable failed, return incomplete RunStep.
                if not curr_RSIC.is_successful():
                    if curr_RSIC.is_cancelled:
                        self.logger.error("PipelineStepInputCable {} cancelled.".format(curr_RSIC))
                    elif curr_RSIC.reused:
                        self.logger.error("PipelineStepInputCable {} failed on reuse.".format(curr_RSIC))
                    else:
                        self.logger.error("PipelineStepInputCable {} failed.".format(curr_RSIC))
                    curr_RS.stop(save=True, clean=True)
                    return curr_RS

                # Cable succeeded.
                inputs_after_cable.append(curr_RSIC.execrecord.execrecordouts.first().dataset)
            else:
                corresp_ERI = curr_RS.execrecord.execrecordins.get(generic_input=curr_input)
                inputs_after_cable.append(corresp_ERI.dataset)

        # Look for ExecRecord.
        succeeded_yet = False
        while not succeeded_yet:
            try:
                with transaction.atomic():
                    if pipelinestep.is_subpipeline:
                        curr_ER = None
                        self.logger.debug("Step {} is a sub-pipeline, so no ExecRecord is applicable".
                                          format(pipelinestep))
                    else:
                        if recover:
                            curr_ER = librarian.models.ExecRecord.objects.get(pk=curr_RS.execrecord.pk)
                        else:
                            curr_ER, can_reuse = curr_RS.get_suitable_ER(inputs_after_cable)
                            if curr_ER is not None:
                                # If it was unsuccessful, we bail.  Alternately, if we can fully reuse it
                                # now, we can return.
                                if not can_reuse["successful"] or can_reuse["fully reusable"]:
                                    self.logger.debug(
                                        "ExecRecord {} is reusable (successful = {})".format(
                                            curr_ER, can_reuse["successful"])
                                    )
                                    curr_RS.reused = True
                                    curr_RS.execrecord = curr_ER
                                    curr_RS.stop(save=False, clean=False)
                                    curr_RS.complete_clean()
                                    curr_RS.save()
                                    self.update_step_maps(curr_RS, step_run_dir, output_paths)
                                    return curr_RS

                                else:
                                    self.logger.debug("Filling in ExecRecord {}".format(curr_ER))

                            else:
                                self.logger.debug("No compatible ExecRecord found - will create new ExecRecord")

                            curr_RS.reused = False
                            curr_RS.save()
                succeeded_yet = True
            except (OperationalError, InternalError):
                wait_time = random.random()
                self.logger.debug("Database conflict.  Waiting for %f seconds before retrying.", wait_time)
                time.sleep(wait_time)

        invoking_record = recovering_record if recover else curr_RS

        # Gather inputs.
        for curr_in_dataset in inputs_after_cable:

            # Check if required Datasets are on the file system for running code.
            if self.find_dataset(curr_in_dataset) is None:
                self.logger.debug("Dataset {} not on file system: recovering".format(curr_in_dataset))

                # Run recover() on missing datasets.
                if not self.recover(curr_in_dataset, invoking_record):

                    # Failed recovery. Return RunStep with failed ExecLogs.
                    self.logger.warn("Recovery of Dataset {} failed".format(curr_in_dataset))
                    curr_RS.stop(save=True, clean=True)
                    return curr_RS

        # Run code.
        # If the step is a sub-Pipeline, execute it and return the RunStep.
        if pipelinestep.is_subpipeline:
            self.execute_pipeline(pipeline=pipelinestep.transformation.pipeline, input_datasets=inputs_after_cable,
                                  sandbox_path=step_run_dir, parent_runstep=curr_RS)
            curr_RS.stop(save=True, clean=True)
            return curr_RS

        input_paths = [self.dataset_fs_map[x] for x in inputs_after_cable]

        # This is only meant for single-threaded use so we just say the worker rank is 1.
        return _finish_step_h(1, self.user, curr_RS, step_run_dir, curr_ER, inputs_after_cable, input_paths,
                              output_paths, log_dir, recovering_record, sandbox_to_update=self)

    def execute_pipeline(self, pipeline=None, input_datasets=None, sandbox_path=None, parent_runstep=None):
        """
        Execute the specified Pipeline with the given inputs.

        INPUTS
        If a top level pipeline, pipeline, input_datasets, sandbox_path,
        and parent_runstep are all None.

        Outputs written to: [sandbox_path]/output_data/run[run PK]_[output name].(csv|raw)
        """
        is_set = (pipeline is not None, input_datasets is not None, sandbox_path is not None, parent_runstep is not None)
        if any(is_set) and not all(is_set):
            raise ValueError("Either none or all parameters must be None")

        if self.run.is_complete():
            self.logger.warn('A Pipeline has already been run in Sandbox "{}", returning the previous Run'.format(self))
            return self.run

        curr_run = self.run

        if pipeline:
            pipeline.check_inputs(input_datasets)
            self.logger.debug("executing a sub-pipeline with input_datasets {}".format(input_datasets))
            curr_run = pipeline.pipeline_instances.create(user=self.user, parent_runstep=parent_runstep)
            curr_run.users_allowed.add(*self.run.users_allowed.all())
            curr_run.groups_allowed.add(*self.run.groups_allowed.all())
        else:
            pipeline = self.pipeline

        curr_run.start(save=True, clean=False)
        sandbox_path = sandbox_path or self.sandbox_path

        for step in pipeline.steps.order_by("step_num"):
            self.logger.debug("Executing step {} - looking for cables feeding into this step".format(step))

            step_inputs = []
            run_dir = os.path.join(sandbox_path, "step{}".format(step.step_num))

            # Before executing a step, we need to know what input datasets to feed into the step for execution.
            # Because pipeline steps includes the cable execution prior to the transformation,
            # the datasets we need are upstream of the *PSIC* leading to this step.

            # For each PSIC leading to this step...
            for psic in step.cables_in.order_by("dest__dataset_idx"):
                # The socket is upstream of that PSIC.
                socket = psic.source.definite

                run_to_query = curr_run
                # If the PSIC comes from another step, the generator is the source pipeline step,
                # or the output cable if it's a sub-pipeline.
                if psic.source_step != 0:
                    generator = pipeline.steps.get(step_num=psic.source_step)
                    if socket.transformation.is_pipeline:
                        run_to_query = curr_run.runsteps.get(pipelinestep=generator).child_run
                        generator = generator.transformation.pipeline.outcables.get(output_idx=socket.dataset_idx)

                # Otherwise, the psic comes from step 0
                else:
                    # The dataset was uploaded...
                    generator = None
                    # ... unless this step is a subpipeline.
                    if parent_runstep is not None:
                        # Get the cable in the outer pipeline step that feeds this PSIC.
                        run_to_query = parent_runstep.run
                        cables_into_subpipeline = parent_runstep.pipelinestep.cables_in
                        generator = cables_into_subpipeline.get(dest=psic.source)

                step_inputs.append(self.socket_map[(run_to_query, generator, socket)])

            curr_RS = self.execute_step(step, curr_run, recovering_record=None, inputs=step_inputs,
                                        step_run_dir=run_dir)
            self.logger.debug("DONE EXECUTING STEP {}".format(step))

            # Bail if we failed -- either due to a bad input, during execution, or by reusing a failed ExecRecord.
            stop_now = False
            if curr_RS.is_cancelled:
                self.logger.warn("Step cancelled: returning the run")
                stop_now = True
            elif not curr_RS.reused:
                if not curr_RS.is_complete() or not curr_RS.successful_execution():
                    self.logger.warn("Step failed to execute: returning the run")
                    stop_now = True
            else:
                if curr_RS.execrecord is None:
                    self.logger.critical("Step is reused but has no ExecRecord: THIS SHOULD NEVER HAPPEN")
                    stop_now = True
                elif not curr_RS.check_ER_usable(curr_RS.execrecord)["successful"]:
                    self.logger.warn("Step reuses a failed ExecRecord: returning the run")
                    stop_now = True

            if stop_now:
                curr_run.stop(save=True, clean=True)
                return curr_run

        self.logger.debug("Finished executing steps, executing POCs")
        for outcable in pipeline.outcables.all():
            generator = None
            run_to_query = curr_run

            # Consider the source step of this POC.  The socket is the TO from a pipeline step.
            source_step = pipeline.steps.get(step_num=outcable.source_step)
            socket = outcable.source

            # The generator of interest is usually just the source pipeline step.
            if not source_step.transformation.is_pipeline:
                generator = source_step
            # But if this step contains a subpipeline...
            else:
                # The generator is the subpipeline's output cable.  Retrieve the subrun so we can
                # look up the right Dataset.
                generator = source_step.transformation.pipeline.outcables.get(output_idx=socket.dataset_idx)
                runstep_containing_subrun = curr_run.runsteps.get(pipelinestep__step_num=outcable.source_step)
                run_to_query = archive.models.Run.objects.get(parent_runstep=runstep_containing_subrun)

            source_dataset = self.socket_map[(run_to_query, generator, socket)]
            file_suffix = "raw" if outcable.is_raw() else "csv"
            out_file_name = "run{}_{}.{}".format(curr_run.pk, outcable.output_name, file_suffix)
            output_path = os.path.join(self.out_dir, out_file_name)
            curr_ROC = self.execute_cable(outcable, curr_run, recovering_record=None,
                                          input_dataset=source_dataset, output_path=output_path)

            if not curr_ROC.is_successful():
                if curr_ROC.is_cancelled:
                    self.logger.debug("Cable %s cancelled due to corrupted input", curr_ROC)
                elif curr_ROC.reused:
                    self.logger.debug("Reuse of cable %s failed", curr_ROC)
                else:
                    self.logger.debug("Execution of cable %s failed", curr_ROC)
                curr_run.stop(save=True, clean=True)
                return curr_run

        self.logger.debug("Finished executing output cables")
        curr_run.stop(save=True, clean=False)
        curr_run.complete_clean()
        self.logger.debug("DONE EXECUTING PIPELINE - Run is complete, clean, and saved")

        return curr_run

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

    def recover(self, dataset_to_recover, invoking_record):
        """
        Writes dataset_to_recover to the file system.

        INPUTS
        dataset_to_recover   The dataset we want to recover.
        invoking_record RunAtomic initiating the recovery

        OUTPUTS
        Returns True if successful - otherwise False.

        PRE
        dataset_to_recover is in the maps but no corresponding file is on the file system.
        """
        if dataset_to_recover.has_data():
            self.logger.debug("Dataset is in the DB - writing it to the file system")
            location = self.dataset_fs_map[dataset_to_recover]
            saved_data = dataset_to_recover.dataset
            try:
                shutil.copyfile(saved_data.dataset_file.path, location)
            except IOError:
                self.logger.error("could not copy file {} to file {}.".format(saved_data.dataset_file.path, location))
                return False
            return True

        self.logger.debug("Performing computation to create missing Dataset")

        # Search for the generator of the dataset in the Pipeline.
        curr_run, generator = self.first_generator_of_dataset(dataset_to_recover)

        if curr_run is None:
            raise ValueError('Dataset "{}" was not found in Pipeline "{}" and cannot be recovered'
                             .format(dataset_to_recover, self.pipeline))
        elif generator is None:
            raise ValueError('Dataset "{}" is an input to Pipeline "{}" and cannot be recovered'
                             .format(dataset_to_recover, self.pipeline))

        self.logger.debug('Executing {} "{}" in recovery mode'.format(generator.__class__.__name__, generator))
        if type(generator) == pipeline.models.PipelineStep:
            curr_record = self.execute_step(generator, curr_run, recovering_record=invoking_record)
        elif type(generator) == pipeline.models.PipelineStepInputCable:
            parent_rs = curr_run.runsteps.filter(pipelinestep=generator.pipelinestep)
            curr_record = self.execute_cable(generator, parent_rs, recovering_record=invoking_record)
        else:
            curr_record = self.execute_cable(generator, curr_run, recovering_record=invoking_record)

        # Determine whether recovery was successful:
        # - if there is no ExecLog in curr_record, then it was unsuccessful (failed on a deeper recovery)
        # - if there is an ExecLog and it is unsuccessful, then it was unsuccessful (failed on this recovery)
        log_of_recovery = curr_record.log
        if log_of_recovery is None:
            return False
        return (log_of_recovery.is_complete() and log_of_recovery.is_successful() and
                log_of_recovery.all_checks_passed())

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

    def advance_pipeline(self, run_to_start=None, task_completed=None):
        """
        Proceed through a pipeline, seeing what can run now that a step or cable has just completed.

        Note that if a sub-pipeline of the pipeline finishes, we report that the parent runstep
        has finished, not the cables.

        PRE:
        at most one of run_to_start and task_completed may not be None.
         - if both are None then we're starting the top-level run
         - if run_to_start is not None then we're starting a sub-pipeline
         - if task_completed is not None then we're resuming either the top-level run or a sub-run.
        """
        assert (type(task_completed) in (archive.models.RunStep,
                                         archive.models.RunSIC,
                                         archive.models.RunOutputCable) or
                task_completed is None)
        assert not (run_to_start is not None and task_completed is not None)

        run_to_resume = self.run
        if run_to_start is not None:
            run_to_resume = run_to_start
        elif task_completed is not None:
            self.logger.debug("Advancing pipeline after completion of task %s", task_completed)
            run_to_resume = task_completed.parent_run

        if task_completed is None:
            run_to_resume.start(save=True)

        self.run_plan = RunPlan()
        self.run_plan.load(self.run, self.inputs)

        self.run_plan.create_run_steps()

        pipeline_to_resume = run_to_resume.pipeline

        if run_to_resume != self.run:
            assert run_to_resume.top_level_run == self.run

        sandbox_path = self.sandbox_path
        if run_to_resume != self.run:
            sandbox_path = (self.step_execute_info[(run_to_resume, run_to_resume.parent_runstep.pipelinestep)]
                            .step_run_dir)

        # A list of steps/tasks that have just completed, including those that may have
        # just successfully been reused during this call to advance_pipeline.
        step_nums_completed = []
        cables_completed = []
        if type(task_completed) == archive.models.RunStep:
            step_nums_completed = [task_completed.step_num]
        elif task_completed is None:
            step_nums_completed = [0]
        else:
            cables_completed = [task_completed]

        # Go through steps in order, looking for input cables pointing at the task(s) that have completed.
        # If task_completed is None, then we are starting the pipeline and we look at the pipeline inputs.
        for step in pipeline_to_resume.steps.order_by("step_num"):
            # If this is already running, we skip it.
            corresp_runstep = run_to_resume.runsteps.filter(pipelinestep=step,
                                                            RSICs__isnull=False)
            if corresp_runstep.exists():
                # We don't advance sub-pipelines -- if those are waiting on tasks in their parent run,
                # then that would be a case for enqueue_runnable_tasks.
                continue

            # If this step is not fed at all by any of the tasks that just completed,
            # we skip it -- it can't have just become ready to go.
            fed_by_newly_completed = False
            for idx in step_nums_completed:
                if step.cables_in.filter(source_step=idx).exists():
                    fed_by_newly_completed = True
                    break
            if not fed_by_newly_completed:
                for cable in cables_completed:
                    if type(cable) is not archive.models.RunOutputCable:
                        continue

                    parent_runstep = cable.parent_run.parent_runstep
                    if parent_runstep is None:
                        continue
                    output_fed = parent_runstep.transformation.outputs.get(dataset_idx=cable.output_idx)
                    if step.cables_in.filter(source_step=parent_runstep.step_num, source=output_fed):
                        fed_by_newly_completed = True
                        break

            if not fed_by_newly_completed:
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
                    if socket.transformation.is_pipeline:
                        run_to_query = run_to_resume.runsteps.get(pipelinestep=generator).child_run
                        generator = generator.transformation.pipeline.outcables.get(output_idx=socket.dataset_idx)

                # Otherwise, the psic comes from step 0.
                else:
                    # If this step is not a subpipeline, the dataset was uploaded.
                    generator = None
                    # If this step is a subpipeline, then the run we are interested in is the parent run.
                    # Get the run and cable that feeds this PSIC.
                    if run_to_resume.parent_runstep is not None:
                        run_to_query = parent_runstep.run
                        cables_into_subpipeline = parent_runstep.pipelinestep.cables_in
                        generator = cables_into_subpipeline.get(dest=psic.source)

                if (run_to_query, generator, socket) in self.socket_map:
                    step_inputs.append(self.socket_map[(run_to_query, generator, socket)])
                else:
                    all_inputs_fed = False
                    break

            if not all_inputs_fed:
                # This step cannot be run yet, so we move on.
                continue

            # Start execution of this step.
            self.logger.debug("Beginning execution of step %d (%s)", step.step_num, step)
            run_dir = os.path.join(sandbox_path, "step{}".format(step.step_num))
            curr_RS = self.reuse_or_prepare_step(step, run_to_resume, step_inputs, run_dir)

            # If the step we just started is for a Method, and it was successfully reused, then we add its step
            # number to the list of those just completed.  This may then allow subsequent steps to also be started.
            if not curr_RS.pipelinestep.is_subpipeline:
                if curr_RS.is_cancelled:
                    self.logger.debug("Step %d (%s) cancelled", step.step_num, step)
                    return
                elif curr_RS.reused and not curr_RS.successful_reuse():
                    self.logger.debug("Step %d (%s) failed on reuse", step.step_num, step)
                    return
                elif curr_RS.is_complete():
                    step_nums_completed.append(step.step_num)
            # Otherwise, we look and see if any of its outcables are complete.  If so, then add them to the
            # list -- they may allow stuff to run.
            else:
                for roc in curr_RS.child_run.runoutputcables.all():
                    if roc.is_cancelled:
                        self.logger.debug("Cable %s cancelled", roc.pipelineoutputcable)
                        return
                    elif roc.reused and not roc.successful_reuse():
                        self.logger.debug("Cable %s failed on reuse", roc.pipelineoutputcable)
                        return
                    elif roc.is_complete():
                        cables_completed.append(roc)

        # Now go through the output cables and do the same.
        for outcable in pipeline_to_resume.outcables.order_by("output_idx"):
            # First, if this is already running, we skip it.
            if run_to_resume.runoutputcables.filter(pipelineoutputcable=outcable).exists():
                continue

            # Check if this cable has just had its input made available.
            source_dataset = None
            fed_by_newly_completed = False
            for idx in step_nums_completed:
                if outcable.source_step == idx:
                    source_dataset = self.socket_map[(run_to_resume, pipeline_to_resume.steps.get(step_num=idx),
                                                 outcable.source)]
                    fed_by_newly_completed = True
                    break
            if not fed_by_newly_completed:
                for cable in cables_completed:
                    if type(cable) is not archive.models.RunOutputCable:
                        continue

                    parent_runstep = cable.parent_run.parent_runstep
                    if parent_runstep is None:
                        continue
                    output_fed = parent_runstep.transformation.outputs.get(dataset_idx=cable.output_idx)
                    if outcable.source_step == parent_runstep.step_num and outcable.source == output_fed:
                        source_dataset = self.socket_map[(cable.parent_run, cable.pipelineoutputcable, output_fed)]
                        fed_by_newly_completed = True
                        break

            if fed_by_newly_completed:
                file_suffix = "raw" if outcable.is_raw() else "csv"
                out_file_name = "run{}_{}.{}".format(run_to_resume.pk, outcable.output_name, file_suffix)
                output_path = os.path.join(self.out_dir, out_file_name)
                cable_exec_info = self.reuse_or_prepare_cable(outcable, run_to_resume, source_dataset, output_path)
                cr = cable_exec_info.cable_record
                if cr.is_cancelled:
                    self.logger.debug("Cable %s cancelled", roc.pipelineoutputcable)
                    return
                elif cr.reused and not cr.successful_reuse():
                    self.logger.debug("Cable %s failed on reuse", roc.pipelineoutputcable)
                    return

    # Modified from execute_cable.
    def reuse_or_prepare_cable(self, cable, parent_record, input_dataset, output_path):
        """
        Attempt to reuse the cable; prepare it for finishing if unable.
        """
        assert input_dataset.clean() is None
        assert input_dataset in self.dataset_fs_map

        self.logger.debug("Checking whether cable can be reused")

        # Create new RSIC/ROC.
        curr_record = archive.models.RunCable.create(cable, parent_record)
        self.logger.debug("Not recovering - created {}".format(curr_record.__class__.__name__))
        self.logger.debug("Cable keeps output? {}".format(curr_record.keeps_output()))

        by_step = parent_record if isinstance(parent_record, archive.models.RunStep) else None

        # We bail out if the input has somehow been corrupted.
        if not input_dataset.is_OK():
            self.logger.debug("Input %s has corrupted.  Cancelling.", input_dataset)
            curr_record.is_cancelled = True
            curr_record.stop(save=True, clean=False)
            curr_record.complete_clean()

            # Return a RunCableExecuteInfo that is marked as cancelled.
            exec_info = RunCableExecuteInfo(curr_record, self.user, None, input_dataset, self.dataset_fs_map[input_dataset],
                                            output_path, by_step=by_step)
            exec_info.cancel()
            self.cable_execute_info[(curr_record.parent_run, cable)] = exec_info
            return exec_info

        # Attempt to reuse this PipelineCable.
        return_now = False

        succeeded_yet = False
        while not succeeded_yet:
            try:
                with transaction.atomic():
                    curr_ER, can_reuse = curr_record.get_suitable_ER(input_dataset)

                    if curr_ER is not None:
                        output_dataset = curr_ER.execrecordouts.first().dataset
                        # If it was unsuccessful, we bail.  Alternately, if we can fully reuse it now and don't need to
                        # execute it for a parent step, we can return.
                        if not can_reuse["successful"] or can_reuse["fully reusable"]:
                            self.logger.debug(
                                "ExecRecord {} is reusable (successful = {})".format(curr_ER, can_reuse["successful"])
                            )
                            curr_record.reused = True
                            curr_record.execrecord = curr_ER
                            curr_record.stop(save=False, clean=False)
                            curr_record.complete_clean()
                            curr_record.save()
                            self.update_cable_maps(curr_record, output_dataset, output_path)
                            return_now = True
                succeeded_yet = True
            except (OperationalError, InternalError):
                wait_time = random.random()
                self.logger.debug("Database conflict.  Waiting for %f seconds before retrying.", wait_time)
                time.sleep(wait_time)

        # Bundle up execution info in case this needs to be run, either by recovery or as a first execution.
        exec_info = RunCableExecuteInfo(curr_record, self.user, curr_ER, input_dataset, self.dataset_fs_map[input_dataset],
                                        output_path, by_step=by_step)
        self.cable_execute_info[(curr_record.parent_run, cable)] = exec_info
        if return_now:
            return exec_info

        # We didn't find a compatible and reusable ExecRecord, so we are committed to executing
        # this cable.
        curr_record.reused = False
        self.logger.debug("No ER to completely reuse - preparing execution of this cable")

        # Check the availability of input_dataset; recover if necessary.  Queue for execution
        # if cable is an outcable (incables are handled by their parent step to ensure
        # that the data is written onto the host and filesystem handling the step).
        exec_info.ready_to_go = self.enqueue_cable(exec_info, force=False)
        return exec_info

    # We'd call this when we need to prepare a cable for recovery.  This is essentially a "force" version of
    # reuse_or_prepare_cable, where we don't attempt at all to reuse (we're past that point and we know we need
    # to produce real data here).  We call this function if a non-trivial cable produces data that's fed into
    # another step, e.g. an outcable from a sub-pipeline.
    def enqueue_cable(self, cable_info, force=False):
        """
        Recursive helper for recover that handles recovery of a cable.
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
        if dataset_path is None and not input_dataset.has_data():
            self.logger.debug("Cable input requires non-trivial recovery")
            self.queue_recovery(input_dataset, recovering_record=recovering_record)

            if cable_record.component.is_outcable or (force and by_step is None):
                self.tasks_waiting[input_dataset].append(cable_record)
                self.waiting_for[cable_record] = [input_dataset]

        else:
            ready_to_go = True
            if cable_record.component.is_outcable or (force and by_step is None):
                self.queue_for_processing.append(cable_record)

        return ready_to_go

    # Function that reuses or prepares a step, which will later be complemented by a finish_step
    # method.  We make these by breaking execute_step into two components.
    # This would not be called if you were recovering.
    def reuse_or_prepare_step(self, pipelinestep, parent_run, inputs, step_run_dir):
        """
        Reuse step if possible; prepare it for execution if not.

        As in execute_step:
        Inputs written to:  [step run dir]/input_data/step[step num]_[input name]
        Outputs written to: [step run dir]/output_data/step[step num]_[output name]
        Logs written to:    [step run dir]/logs/step[step num]_std(out|err).txt
        """
        step_plan = self.run_plan.step_plans[pipelinestep.step_num-1]
        curr_RS = step_plan.run_step
        curr_RS.start()

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

            cable_exec_info = self.reuse_or_prepare_cable(corresp_cable, curr_RS, inputs[i], cable_path)

            cable_info_list.append(cable_exec_info)

            # If the cable was cancelled (e.g. due to bad input), we bail.
            if cable_exec_info.cancelled:
                self.logger.debug("Input cable %s to step %s was cancelled", cable_exec_info.cable_record,
                                  curr_RS)
                curr_RS.stop(save=True, clean=False)
                curr_RS.complete_clean()
            # If the cable is not complete and not ready to go, we need to recover its input.
            elif not cable_exec_info.cable_record.is_complete():
                if not cable_exec_info.ready_to_go:
                    datasets_to_recover.append(inputs[i])
            elif not cable_exec_info.execrecord.execrecordouts.first().dataset.has_data():
                symbolically_okay_datasets.append(inputs[i])

        # Bundle up the information required to process this step.
        _in_dir, _out_dir, log_dir = _setup_step_paths(step_run_dir, False)

        # Construct output_paths.
        output_paths = [self.step_xput_path(curr_RS, x, step_run_dir) for x in pipelinestep.outputs]

        execute_info = RunStepExecuteInfo(curr_RS, self.user, cable_info_list, None, step_run_dir, log_dir,
                                          output_paths)
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
            curr_RSIC = cable_info_list[i].cable_record
            if not curr_RSIC.is_complete():
                all_inputs_present = False
                break
            inputs_after_cable.append(curr_RSIC.execrecord.execrecordouts.first().dataset)

        if all_inputs_present:
            # Recurse if this step is a sub-pipeline.
            if pipelinestep.is_subpipeline:
                # Start the sub-pipeline.
                self.logger.debug("Executing a sub-pipeline with input_dataset(s): {}".format(inputs_after_cable))
                subpipeline_to_run = pipelinestep.transformation.definite
                curr_run = subpipeline_to_run.pipeline_instances.create(user=self.user, parent_runstep=curr_RS)
                curr_run.users_allowed.add(self.run.users_allowed.all())
                curr_run.groups_allowed.add(self.run.groups_allowed.all())
                self.advance_pipeline(run_to_start=curr_run)
                return curr_RS

            # Look for a reusable ExecRecord.  If we find it, then complete the RunStep.
            succeeded_yet = False
            while not succeeded_yet:
                try:
                    with transaction.atomic():
                        curr_ER = step_plan.execrecord
                        can_reuse = curr_ER and curr_RS.check_ER_usable(curr_ER)
                        if curr_ER is not None:
                            # If it was unsuccessful, we bail.  Alternately, if we can fully reuse it now,
                            # we can return.
                            if not can_reuse["successful"] or can_reuse["fully reusable"]:
                                self.logger.debug(
                                    "ExecRecord {} is reusable (successful = {})".format(
                                        curr_ER, can_reuse["successful"])
                                )
                                curr_RS.reused = True
                                curr_RS.execrecord = curr_ER
                                curr_RS.stop(save=True, clean=False)
                                curr_RS.complete_clean()
                                self.update_step_maps(curr_RS, step_run_dir, output_paths)
                                execute_info.execrecord = curr_ER
                                return curr_RS

                            else:
                                self.logger.debug("Filling in ExecRecord {}".format(curr_ER))
                                execute_info.execrecord = curr_ER

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
                self.queue_recovery(missing_data, invoking_record=curr_RS)
                self.tasks_waiting[missing_data].append(curr_RS)
            self.waiting_for[curr_RS] = symbolically_okay_datasets
        else:
            # We're not waiting for any inputs.  Add this step to the queue.
            self.queue_for_processing.append(curr_RS)

        return curr_RS

    def step_recover_h(self, execute_info):
        """
        Helper for recover that's responsible for forcing recovery of a step.
        """
        # Break out execute_info.
        runstep = execute_info.runstep
        cable_info_list = execute_info.cable_info_list
        recovering_record = execute_info.recovering_record

        pipelinestep = runstep.pipelinestep
        assert not pipelinestep.is_subpipeline

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
        if type(task) == archive.models.RunStep:
            return self.step_execute_info[(task.run, task.pipelinestep)]
        return self.cable_execute_info[(task.parent_run, task.component)]

    def update_sandbox(self, task_finished):
        """
        Helper that updates the sandbox maps to reflect the information from the specified task_finished.

        PRE: task_finished is a RunStep/RunCable belonging to this sandbox's run, and it already has
        execution info available in step_execute_info or cable_execute_info.
        """
        assert task_finished.top_level_run == self.run
        if type(task_finished) == archive.models.RunStep:
            assert (task_finished.run, task_finished.pipelinestep) in self.step_execute_info
        else:
            assert (task_finished.parent_run, task_finished.component) in self.cable_execute_info

        task_execute_info = self.get_task_info(task_finished)

        # Update the sandbox with this information.
        if type(task_finished) == archive.models.RunStep:
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


# This function will be called by fleet Workers.  Since they do not have access
# to the Sandboxes, we need to pass in all the information they need via a dictionary
# representation of a RunCableExecuteInfo.
def finish_cable(cable_execute_dict, worker_rank):
    """
    Finishes an un-reused cable that has already been prepared for execution.

    If we are reaching this point, we know that the data required for input_dataset is either
    in place in the sandbox or available in the database.

    This function is called by finish_step, because we want it to be called by the same
    worker(s) as the step is, so its output is on the local filesystem of the worker, which
    may be a remote MPI host.
    """
    # Retrieve info from the database using the PKs passed.
    curr_record = archive.models.RunComponent.objects.get(pk=cable_execute_dict["cable_record_pk"]).definite
    input_dataset = librarian.models.Dataset.objects.get(pk=cable_execute_dict["input_dataset_pk"])
    output_path = cable_execute_dict["output_path"]
    recovering_record = None
    if cable_execute_dict["recovering_record_pk"] is not None:
        recovering_record = archive.models.RunComponent.objects.get(
            pk=cable_execute_dict["recovering_record_pk"]
        ).definite
    curr_ER = None

    # It's possible that this cable was completed in the time between the Manager queueing this task
    # and the worker starting it.  If so we can use the ExecRecord, and maybe even fully reuse it
    # if this was not called by finish_step.
    succeeded_yet = False
    while not succeeded_yet:
        try:
            with transaction.atomic():
                if cable_execute_dict["execrecord_pk"] is not None:
                    curr_ER = librarian.models.ExecRecord.objects.get(pk=cable_execute_dict["execrecord_pk"])
                    can_reuse = curr_record.check_ER_usable(curr_ER)
                else:
                    curr_ER, can_reuse = curr_record.get_suitable_ER(input_dataset)

                if curr_ER is not None:
                    # If it was unsuccessful, we bail.  Alternately, if we can fully reuse it now and don't need to
                    # execute it for a parent step, we can return.
                    if (not can_reuse["successful"] or
                            (can_reuse["fully reusable"] and cable_execute_dict["by_step_pk"] is None)):
                        logger.debug("[%d] ExecRecord %s is reusable (successful = %s)",
                                     worker_rank, curr_ER, can_reuse["successful"])
                        curr_record.reused = True
                        curr_record.execrecord = curr_ER
                        curr_record.stop(save=True, clean=False)
                        curr_record.complete_clean()
                        return curr_record
            succeeded_yet = True
        except (OperationalError, InternalError):
            wait_time = random.random()
            logger.debug("[%d] Database conflict.  Waiting for %f seconds before retrying.",
                         worker_rank, wait_time)
            time.sleep(wait_time)

    # We're now committed to actually running this cable.
    input_dataset_path = cable_execute_dict["input_dataset_path"]
    user = User.objects.get(pk=cable_execute_dict["user_pk"])

    # Preconditions to test.
    # assert curr_record is not None
    input_dataset_in_sdbx = file_access_utils.file_exists(input_dataset_path)
    assert input_dataset_in_sdbx or input_dataset.has_data()

    cable = curr_record.definite.component

    recover = recovering_record is not None

    # Write the input dataset to the sandbox if necessary.
    # FIXME at some point in the future this will have to be updated to mean "write to the local sandbox".
    if not input_dataset_in_sdbx:
        logger.debug("[%d] Dataset is in the DB - writing it to the file system", worker_rank)
        saved_data = input_dataset.dataset
        try:
            shutil.copyfile(saved_data.dataset_file.path, input_dataset_path)
        except IOError:
            logger.error("[%d] could not copy file %s to file %s.",
                         worker_rank, saved_data.dataset_file.path, input_dataset_path)
            curr_record.stop(save=True, clean=True)
            return curr_record

    if not recover:
        # Get or create CDT for cable output (Evaluate cable wiring)
        output_CDT = input_dataset.get_cdt()
        if not cable.is_trivial():
            output_CDT = cable.find_compounddatatype() or cable.create_compounddatatype()

    else:
        logger.debug("[%d] Recovering - will update old ER", worker_rank)
        output_dataset = curr_ER.execrecordouts.first().dataset
        output_CDT = output_dataset.get_cdt()

    return _finish_cable_h(worker_rank, curr_record, cable, user, curr_ER, input_dataset, input_dataset_path,
                           output_path, output_CDT, recovering_record)


def _finish_cable_h(worker_rank, curr_record, cable, user, execrecord, input_dataset, input_dataset_path, output_path,
                    output_CDT, recovering_record, sandbox_to_update=None):
    """
    Helper for finish_cable and execute_cable.
    """
    recover = recovering_record is not None
    # Create ExecLog invoked by...
    if not recover:
        # ...this RunCable.
        invoking_record = curr_record
    else:
        # ...the recovering RunAtomic.
        invoking_record = recovering_record
    curr_log = archive.models.ExecLog.create(curr_record, invoking_record)

    # Run cable (this completes EL).
    cable.run_cable(input_dataset_path, output_path, curr_record, curr_log)

    # Here, we're authoring/modifying an ExecRecord, so we use a transaction.
    preexisting_ER = execrecord is not None
    succeeded_yet = False
    while not succeeded_yet:
        try:
            with transaction.atomic():
                missing_output = False
                start_time = timezone.now()
                if not file_access_utils.file_exists(output_path):
                    end_time = timezone.now()
                    # It's conceivable that the linking could fail in the
                    # trivial case; in which case we should associate a "missing data"
                    # check to input_dataset == output_dataset.
                    if cable.is_trivial():
                        output_dataset = input_dataset
                    if execrecord is None:
                        output_dataset = librarian.models.Dataset.create_empty(
                            cdt=output_CDT,
                            created_by=curr_record
                        )
                    else:
                        output_dataset = execrecord.execrecordouts.first().dataset
                    output_dataset.mark_missing(start_time, end_time, curr_log, user)
                    missing_output = True

                elif cable.is_trivial():
                    output_dataset = input_dataset

                else:
                    # Do we need to keep this output?
                    make_dataset = curr_record.keeps_output()
                    dataset_name = curr_record.output_name()
                    dataset_desc = curr_record.output_description()
                    if not make_dataset:
                        logger.debug("[%d] Cable doesn't keep output: not creating a dataset", worker_rank)

                    if execrecord is not None:
                        output_dataset = execrecord.execrecordouts.first().dataset
                        if make_dataset:
                            output_dataset.register_file(output_path)

                    else:
                        output_dataset = librarian.models.Dataset.create_dataset(
                            output_path,
                            cdt=output_CDT,
                            keep_file=make_dataset,
                            name=dataset_name,
                            description=dataset_desc,
                            created_by=curr_record,
                            check=False
                        )

                # Link the ExecRecord to curr_record if necessary, creating it if necessary also.
                if not recover:
                    if execrecord is None:
                        logger.debug("[%d] No ExecRecord already in use - creating fresh cable ExecRecord",
                                     worker_rank)
                        # Make ExecRecord, linking it to the ExecLog.
                        execrecord = librarian.models.ExecRecord.create(curr_log, cable, [input_dataset], [output_dataset])
                    # Link ER to RunCable (this may have already been linked; that's fine).
                    curr_record.link_execrecord(execrecord, reused=False)

                else:
                    logger.debug("[%d] This was a recovery - not linking RSIC/RunOutputCable to ExecRecord",
                                 worker_rank)
            succeeded_yet = True
        except (OperationalError, InternalError):
            wait_time = random.random()
            logger.debug("[%d] Database conflict.  Waiting for %f seconds before retrying.",
                         worker_rank, wait_time)
            time.sleep(wait_time)

    ####
    # Check outputs
    ####

    if not missing_output:
        # Did ER already exist (with vetted output), or is cable trivial, or recovering? Yes.
        if (preexisting_ER and (output_dataset.is_OK() or output_dataset.any_failed_checks())) or cable.is_trivial() or recover:
            logger.debug("[%d] Performing integrity check of trivial or previously generated output", worker_rank)
            # Perform integrity check.
            output_dataset.check_integrity(output_path, user, curr_log, output_dataset.MD5_checksum)

        # Did ER already exist, or is cable trivial, or recovering? No.
        else:
            logger.debug("[%d] Performing content check for output generated for the first time", worker_rank)
            summary_path = "{}_summary".format(output_path)
            # Perform content check.
            output_dataset.check_file_contents(output_path, summary_path, cable.min_rows_out,
                                          cable.max_rows_out, curr_log, user)

        # If a sandbox was specified and we were successful, update the sandbox.
        if sandbox_to_update is not None and output_dataset.is_OK() and not recover:
            # Success! Update dataset_fs/socket/cable_map.
            sandbox_to_update.update_cable_maps(curr_record, output_dataset, output_path)

    logger.debug("[%d] DONE EXECUTING %s '%s'", worker_rank, type(cable).__name__, cable)

    # End. Return curr_record.  Stop the clock if this was not a recovery.
    if not recover:
        curr_record.stop(save=True, clean=False)
    curr_record.complete_clean()
    return curr_record


# The actual running of code happens here.  We copy and modify this from execute_step.
def finish_step(step_execute_dict, worker_rank, stop_execution_callback=None):
    """
    Carry out the task specified by step_execute_dict.

    Precondition: the task must be ready to go, i.e. its inputs must all be in place.  Also
    it should not have been run previously.  This should not be a RunStep representing a Pipeline.
    """
    # Break out the execution info.
    curr_RS = archive.models.RunStep.objects.get(pk=step_execute_dict["runstep_pk"])
    curr_RS.start()
    step_run_dir = step_execute_dict["step_run_dir"]
    curr_ER = None
    if step_execute_dict["execrecord_pk"] is not None:
        curr_ER = librarian.models.ExecRecord.objects.get(pk=step_execute_dict["execrecord_pk"])
    cable_info_dicts = step_execute_dict["cable_info_dicts"]
    output_paths = step_execute_dict["output_paths"]
    user = User.objects.get(pk=step_execute_dict["user_pk"])
    log_dir = step_execute_dict["log_dir"]

    recovering_record = None
    if step_execute_dict["recovering_record_pk"] is not None:
        recovering_record = archive.models.RunComponent.objects.get(
            pk=step_execute_dict["recovering_record_pk"]
        ).definite

    assert not curr_RS.pipelinestep.is_subpipeline

    recover = recovering_record is not None

    ####
    # Gather inputs: finish all input cables -- we want them written to the sandbox now, which is never
    # done by reuse_or_prepare_cable.
    inputs_after_cable = []
    input_paths = []
    for curr_execute_dict in cable_info_dicts:
        # Update the cable execution information with the recovering record if applicable.
        if recovering_record is not None:
            curr_execute_dict["recovering_record_pk"] = recovering_record.pk

        curr_RSIC = finish_cable(curr_execute_dict, worker_rank)

        # Cable failed, return incomplete RunStep.
        if not curr_RSIC.is_successful():
            logger.error("[%d] PipelineStepInputCable %s failed.", worker_rank, curr_RSIC)
            curr_RS.stop(save=True, clean=False)
            curr_RS.complete_clean()
            return curr_RS

        # Cable succeeded.
        inputs_after_cable.append(curr_RSIC.execrecord.execrecordouts.first().dataset)
        input_paths.append(curr_execute_dict["output_path"])

    # Check again to see if a compatible ER was completed while this task
    # waited on the queue.  If this isn't a recovery, we can just stop.
    if recover:
        assert curr_ER is not None
    else:
        succeeded_yet = False
        while not succeeded_yet:
            try:
                with transaction.atomic():
                    if curr_ER is not None:
                        can_reuse = curr_RS.check_ER_usable(curr_ER)
                        # If it was unsuccessful, we bail.  Alternately, if we can fully reuse it now, we can return.
                        if not can_reuse["successful"] or can_reuse["fully reusable"]:
                            logger.debug("[%d] ExecRecord %s is reusable (successful = %s)",
                                         worker_rank, curr_ER, can_reuse["successful"])
                            curr_RS.reused = True
                            curr_RS.execrecord = curr_ER
                            curr_RS.stop(save=True, clean=False)
                            curr_RS.complete_clean()
                            return curr_RS

                        else:
                            logger.debug("[%d] ExecRecord not reusable %s", worker_rank, curr_ER)
                            curr_ER = None

                    else:
                        logger.debug("[%d] No compatible ExecRecord found yet", worker_rank)

                    curr_RS.reused = False
                    curr_RS.save()
                succeeded_yet = True
            except (OperationalError, InternalError):
                wait_time = random.random()
                logger.debug("[%d] Database conflict.  Waiting for %f seconds before retrying.",
                             worker_rank, wait_time)
                time.sleep(wait_time)

    return _finish_step_h(worker_rank, user, curr_RS, step_run_dir, curr_ER, inputs_after_cable, input_paths,
                          output_paths, log_dir, recovering_record, stop_execution_callback=stop_execution_callback)


def _finish_step_h(worker_rank, user, runstep, step_run_dir, execrecord, inputs_after_cable, input_paths, output_paths,
                   log_dir, recovering_record, sandbox_to_update=None, stop_execution_callback=None):
    """
    Helper for execute_step and finish_step.
    """
    recover = recovering_record is not None
    invoking_record = recovering_record if recover else runstep
    pipelinestep = runstep.pipelinestep

    # Run code, creating ExecLog and MethodOutput.
    curr_log = archive.models.ExecLog.create(runstep, invoking_record)
    stdout_path = os.path.join(log_dir, "step{}_stdout.txt".format(pipelinestep.step_num))
    stderr_path = os.path.join(log_dir, "step{}_stderr.txt".format(pipelinestep.step_num))

    # Check the integrity of the code before we run.
    if not pipelinestep.transformation.definite.driver.check_md5():
        logger.error("[%d] Method code has gone corrupt for %s or its " +
                     "dependencies; stopping step",
                     worker_rank,
                     pipelinestep.transformation.definite.driver)
        # Stop everything!
        curr_log.start()
        curr_log.methodoutput.are_checksums_OK = False
        curr_log.methodoutput.save()
        curr_log.stop(save=True, clean=False)

        if not recover:
            runstep.stop(save=True, clean=False)
        runstep.complete_clean()
        return runstep

    # From here on the code is assumed to not be corrupted.
    try:
        with open(stdout_path, "w+") as out_write, open(stderr_path, "w+") as err_write:
            pipelinestep.transformation.definite.run_code(
                step_run_dir, input_paths,
                output_paths, [out_write], [err_write],
                curr_log, curr_log.methodoutput,
                stop_execution_callback=stop_execution_callback
            )
    except StopExecution as e:
        # Immediately end, marking the RunStep as cancelled, and re-raise e.
        logger.debug("[%d] Method execution stopped.", worker_rank)
        runstep.is_cancelled = True
        runstep.save()
        raise e

    logger.debug("[%d] Method execution complete, ExecLog saved (started = %s, ended = %s)",
                 worker_rank, curr_log.start_time, curr_log.end_time)

    preexisting_ER = execrecord is not None
    succeeded_yet = False
    while not succeeded_yet:
        try:
            with transaction.atomic():
                # Create outputs.
                # bad_output_found indicates we have detected problems with the output.
                bad_output_found = not curr_log.is_successful()
                output_datasets = []
                logger.debug("[%d] ExecLog.is_successful() == %s", worker_rank, curr_log.is_successful())

                if not recover:
                    if preexisting_ER:
                        logger.debug("[%d] Filling in pre-existing ExecRecord with PipelineStep outputs", worker_rank)
                    else:
                        logger.debug("[%d] Creating new Datasets for PipelineStep outputs", worker_rank)

                    for i, curr_output in enumerate(pipelinestep.outputs):
                        output_path = output_paths[i]
                        output_CDT = curr_output.get_cdt()

                        # Check that the file exists, as we did for cables.
                        start_time = timezone.now()
                        if not file_access_utils.file_exists(output_path):
                            end_time = timezone.now()
                            if preexisting_ER:
                                output_dataset = execrecord.get_execrecordout(curr_output).dataset
                            else:
                                output_dataset = librarian.models.Dataset.create_empty(
                                    cdt=output_CDT, created_by=runstep)
                            output_dataset.mark_missing(start_time, end_time, curr_log, user)

                            bad_output_found = True

                        else:
                            # If necessary, create new Dataset for output, and create the Dataset
                            # if it's to be retained.
                            dataset_name = runstep.output_name(curr_output)
                            dataset_desc = runstep.output_description(curr_output)
                            make_dataset = runstep.keeps_output(curr_output)

                            if preexisting_ER:
                                # Wrap in a transaction to prevent concurrent authoring of Datasets to
                                # an existing Dataset.
                                output_ERO = execrecord.get_execrecordout(curr_output)
                                with transaction.atomic():
                                    output_dataset = librarian.models.Dataset.objects.select_for_update().filter(
                                        pk=output_ERO.dataset.pk
                                    ).first()
                                    if make_dataset and not output_dataset.has_data():
                                        output_dataset.register_file(output_path)

                            else:
                                output_dataset = librarian.models.Dataset.create_dataset(
                                    output_path,
                                    cdt=output_CDT,
                                    keep_file=make_dataset,
                                    name=dataset_name,
                                    description=dataset_desc,
                                    created_by=runstep,
                                    check=False
                                )
                                logger.debug("[%d] First time seeing file: saved md5 %s",
                                             worker_rank, output_dataset.MD5_checksum)
                        output_datasets.append(output_dataset)

                    # Create ExecRecord if there isn't already one.
                    if not preexisting_ER:
                        # Make new ExecRecord, linking it to the ExecLog
                        logger.debug("[%d] Creating fresh ExecRecord", worker_rank)
                        execrecord = librarian.models.ExecRecord.create(curr_log, pipelinestep,
                                                                        inputs_after_cable, output_datasets)

                    # Link ExecRecord to RunStep (it may already have been linked; that's fine).
                    runstep.link_execrecord(execrecord, False)

            succeeded_yet = True
        except (OperationalError, InternalError):
            wait_time = random.random()
            logger.debug("[%d] Database conflict.  Waiting for %f seconds before retrying.", worker_rank, wait_time)
            time.sleep(wait_time)

    # Check outputs.
    for i, curr_output in enumerate(pipelinestep.outputs):
        output_path = output_paths[i]
        output_dataset = execrecord.get_execrecordout(curr_output).dataset
        check = None

        if bad_output_found:
            logger.debug("[%d] Bad output found; no check on %s was done", worker_rank, output_path)

        # Recovering or filling in old ER? Yes.
        elif preexisting_ER:

            file_is_present = True
            if recover:
                # Check that the file exists as in the non-recovery case.
                start_time = timezone.now()
                if not file_access_utils.file_exists(output_path):
                    end_time = timezone.now()
                    check = output_dataset.mark_missing(start_time, end_time, curr_log, user)
                    logger.debug("[%d] During recovery, output (%s) is missing", worker_rank, output_path)
                    file_is_present = False

            if file_is_present:
                # Perform integrity check.
                logger.debug("[%d] Dataset has been computed before, checking integrity of %s",
                             worker_rank, output_dataset)
                check = output_dataset.check_integrity(output_path, user, curr_log)

                if check.is_fail():
                    logger.warn("[%d] IntegrityCheckLog failed for %s", worker_rank, output_path)
                    bad_output_found = True

                elif not output_dataset.content_checks.exists():
                    summary_path = "{}_summary".format(output_path)
                    check = output_dataset.check_file_contents(
                        output_path, summary_path, curr_output.get_min_row(),
                        curr_output.get_max_row(), curr_log, user)

                if check.is_fail():
                    logger.warn("[%d] ContentCheckLog failed for %s", worker_rank, output_path)
                    bad_output_found = True

        # Recovering or filling in old ER? No.
        else:
            # Perform content check.
            logger.debug("[%d] %s is new data - performing content check", worker_rank, output_dataset)
            summary_path = "{}_summary".format(output_path)
            check = output_dataset.check_file_contents(output_path, summary_path, curr_output.get_min_row(),
                                                  curr_output.get_max_row(), curr_log, user)

        # Check OK? No.
        if check and check.is_fail():
            logger.warn("[%d] %s failed for %s", worker_rank, check.__class__.__name__, output_path)
            bad_output_found = True

        # Check OK? Yes.
        elif check:
            logger.debug("[%d] %s passed for %s", worker_rank, check.__class__.__name__, output_path)

    execrecord.complete_clean()

    # End. Return runstep.  Stop the clock if this was a recovery.
    if not recover:
        if sandbox_to_update is not None:
            # Since reused=False, step_run_dir represents where the step *actually is*.
            sandbox_to_update.update_step_maps(runstep, step_run_dir, output_paths)

        runstep.stop(save=True, clean=False)
    runstep.complete_clean()
    return runstep


class RunPlan(object):
    """ Hold the plan for which steps will be executed in a sandbox.

    Also holds the dependencies between steps and cables, as well as the
    ExecRecord that will be reused for each step and cable that doesn't have
    to be run this time.
    """
    def load(self, run, inputs):
        """ Load the steps from the pipeline and dataset dependencies.

        Links pipeline inputs and step outputs to the inputs of other steps.
        """
        self.run = run
        self.step_plans = []
        self.inputs = [DatasetPlan(input_item) for input_item in inputs]
        for step in run.pipeline.steps.all():
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
        self.outputs = []
        for cable in run.pipeline.outcables.order_by("output_idx"):
            step_index = cable.source_step-1
            output_index = cable.source.definite.dataset_idx-1
            output_plan = self.step_plans[step_index].outputs[output_index]
            self.outputs.append(output_plan)

    def create_run_steps(self):
        """ Create run steps and find suitable execrecords. """
        for step_plan in self.step_plans:
            run_step = self.run.runsteps.filter(
                pipelinestep__step_num=step_plan.step_num).first()
            if run_step is None:
                run_step = RunStep.create(step_plan.pipeline_step, self.run, start=False)
            step_plan.run_step = run_step
            input_datasets = [plan.dataset for plan in step_plan.inputs]
            if all(input_datasets):
                method = step_plan.pipeline_step.transformation.definite
                if method.reusable == Method.NON_REUSABLE:
                    continue
                execrecord, summary = run_step.get_suitable_ER(input_datasets)
                if not summary:
                    # no exec record, have to run
                    continue
                if (not summary['fully reusable'] and
                        method.reusable != Method.DETERMINISTIC):

                    continue
                step_plan.execrecord = execrecord
                execrecordouts = execrecord.execrecordouts.all()
                for i, execrecordout in enumerate(execrecordouts):
                    output = step_plan.outputs[i]
                    output.dataset = execrecordout.dataset

        is_changed = True
        while is_changed:
            is_changed = self._walk_backward() or self._walk_forward()

    def _walk_backward(self):
        """ Walk backward through the steps, flagging needed runs.

        @return: True if any new steps were flagged for running.
        """
        is_changed = False
        for step_plan in reversed(self.step_plans):
            if not step_plan.execrecord:
                for input_plan in step_plan.inputs:
                    if not input_plan.has_data():
                        source_plan = self.step_plans[input_plan.step_num-1]
                        is_changed = source_plan.check_rerun() or is_changed
        return is_changed

    def _walk_forward(self):
        """ Walk forward through the steps, flagging needed runs.

        @return: True if any new steps were flagged for running.
        """
        is_changed = False
        for step_plan in self.step_plans:
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

    def check_rerun(self):
        """ Check that this step can recreate one of its missing outputs.

        If the execrecord cannot be restored, don't use it, and mark all
        the outputs as having no data.
        @return: True if the execrecord had to be abandoned.
        """
        if not self.execrecord:
            return False
        method = self.run_step.transformation.definite
        if method.reusable == Method.DETERMINISTIC:
            return False

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


class RunCableExecuteInfo:
    def __init__(self, cable_record, user, execrecord, input_dataset, input_dataset_path, output_path,
                 recovering_record=None, by_step=None):
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
            "ready_to_go": self.ready_to_go
        }
