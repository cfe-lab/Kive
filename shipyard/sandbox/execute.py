"""Code that is responsible for the execution of Pipelines."""

from django.core.files import File
from django.contrib.contenttypes.models import ContentType
from django.utils import timezone
from django.db import transaction

import archive.models
import librarian.models
import metadata.models
import pipeline.models 

import file_access_utils
from constants import dirnames, extensions

import os.path
import shutil
import logging
import sys
import tempfile


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

    # sd_fs_map: maps SymDS to a FS path: the path where a data
    # file would be if it were created (Whether or not it is there)
    # If the path is None, the SD is on the DB.

    # socket_map: maps (run, generator, socket) to SDs.
    # A generator is a cable, or a pipeline step. A socket is a TI/TO.
    # If the generator is none, the socket is a pipeline input.
    # This will be used to look up inputs when running a pipeline.
    
    # ps_map: maps PS to (path, RunStep of PS): the path tells you
    # the directory that the PS would have been run in
    # (whether or not it was): the RunStep tells you what inputs are
    # needed (Which in turn will lead back to an sd_fs_map lookup),
    # and allows you to fill it in on recovery.

    # queue_for_processing: list of tasks that are ready to be processed, i.e.
    # all of the required inputs are available and ready to go.

    # step_execute_info: table of RunStep "bundles" giving all the information
    # necessary to process a RunStep.
        
    # cable_map maps cables to ROC/RSIC.

    def __init__(self, user, my_pipeline, inputs, sandbox_path=None):
        """
        Sets up a sandbox environment to run a Pipeline: space on
        the file system, along with sd_fs_map/socket_map/etc.

        INPUTS
        user          User running the pipeline.
        my_pipeline   Pipeline to run.
        inputs        List of SDs to feed into the pipeline.
        sandbox_path  Where on the filesystem to execute.

        PRECONDITIONS
        inputs must have real data
        """
        assert all([i.has_data() for i in inputs])

        self.run = my_pipeline.pipeline_instances.create(start_time=timezone.now(), user=user)

        self.logger = logging.getLogger(self.__class__.__name__)
        self.user = user
        self.pipeline = my_pipeline
        self.inputs = inputs
        self.sd_fs_map = {}
        self.socket_map = {}
        self.cable_map = {}
        self.ps_map = {}
        self.pipeline.check_inputs(self.inputs)

        # Determine a sandbox path, and input/output directories for 
        # top-level Pipeline.
        self.sandbox_path = sandbox_path or tempfile.mkdtemp(prefix="user{}_run{}_".format(self.user, self.run.pk))
        self.in_dir = os.path.join(self.sandbox_path, dirnames.IN_DIR)
        self.out_dir = os.path.join(self.sandbox_path, dirnames.OUT_DIR)

        self.logger.debug("initializing maps")
        for i, pipeline_input in enumerate(inputs, start=1):
            corresp_pipeline_input = self.pipeline.inputs.get(dataset_idx=i)
            self.socket_map[(self.run, None, corresp_pipeline_input)] = pipeline_input
            self.sd_fs_map[pipeline_input] = os.path.join(self.in_dir, 
                                                          "run{}_{}".format(self.run.pk, corresp_pipeline_input.pk))

        # Make the sandbox directory.
        self.logger.debug("file_access_utils.set_up_directory({})".format(self.sandbox_path))
        file_access_utils.set_up_directory(self.sandbox_path)

        # Queue of RunSteps/RunCables to process.
        self.queue_for_processing = []

        # PipelineSteps and PipelineCables "bundled" with necessary information for running them.
        # This will be used when it comes time to finish these cables, either as a first execution or as a recovery.
        # Keys are (run, generator) pairs where run is None if the Run that the generator is a part of is the
        # top-level run, and the appropriate sub-run otherwise.
        self.step_execute_info = {}
        self.cable_execute_info = {}

        # A table of RunSteps/RunCables completed.
        self.tasks_completed = {}

        # A table keyed by SymbolicDatasets, whose values are lists of the RunSteps/RunCables waiting on them.
        self.tasks_waiting = {}

        # The inverse table to the above: the keys are RunSteps/RunCables waiting on recovering SymbolicDatasets,
        # and the values are all of the SymbolicDatasets they're waiting for.
        self.waiting_for = {}

        # A table of sub-pipelines that are currently in progress.
        self.subpipelines_in_progress = {}

    def step_xput_path(self, runstep, transformationxput, step_run_dir):
        """Path in Sandbox for PipelineStep TransformationXput."""
        file_suffix = extensions.RAW if transformationxput.is_raw() else extensions.CSV
        file_name = "step{}_{}.{}".format(runstep.step_num, transformationxput.dataset_name, file_suffix)

        if transformationxput.is_input:
            xput_dir = dirnames.IN_DIR
        else:
            xput_dir = dirnames.OUT_DIR
        return os.path.join(step_run_dir, xput_dir, file_name)

    def register_symbolicdataset(self, symbolicdataset, location):
        """Set the location of a SymbolicDataset on the file system.

        If the SymbolicDataset is already in the Sandbox (ie. it is in
        sd_fs_map), do not update the existing location.

        INPUTS
        symbolicdataset     SymbolicDataset to register
        location            file path of symbolicdataset in the Sandbox
        """
        try:
            self.sd_fs_map[symbolicdataset] = self.sd_fs_map[symbolicdataset] or location
        except KeyError:
            self.sd_fs_map[symbolicdataset] = location

    def find_symbolicdataset(self, symbolicdataset):
        """Find the location of a SymbolicDataset on the file system.

        INPUTS
        symbolicdataset     SymbolicDataset to locate

        OUTPUTS
        location            the path of symbolicdataset in the Sandbox,
                            or None if it's not there
        """
        try:
            location = self.sd_fs_map[symbolicdataset]
        except KeyError:
            self.logger.debug("Dataset {} is not in the Sandbox".format(symbolicdataset))
            return None

        return (location if location and file_access_utils.file_exists(location) else None)

    def _setup_step_paths(self, step_run_dir, recover):
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
        return (in_dir, out_dir, log_dir)

    # TODO: module function of librarian?
    def _find_cable_execrecord(self, runcable, input_SD):
        """Find an ExecRecord which may be reused by a RunCable

        INPUTS
        runcable        RunCable trying to reuse an ExecRecord
        input_SD        SymbolicDataset to feed the cable

        OUTPUTS
        execrecord      ExecRecord which may be reused, or None if no
                        ExecRecord exists
        """
        cable = runcable.component

        # Look at ERIs with matching input SD.
        candidate_ERIs = librarian.models.ExecRecordIn.objects.filter(symbolicdataset=input_SD)

        for candidate_ERI in candidate_ERIs:
            candidate_execrecord = candidate_ERI.execrecord
            candidate_component = candidate_execrecord.general_transf()

            if not candidate_component.is_cable:
                continue

            if cable.is_compatible(candidate_component):
                self.logger.debug("Compatible ER found")
                return candidate_execrecord

    # TODO: this needs testing
    # TODO: member function of Pipeline*Cable?
    def _find_cable_compounddatatype(self, cable):
        """Find a CompoundDatatype for the output of a cable.

        INPUTS
        cable       the cable we want a CompoundDatatype for

        OUTPUTS
        output_CDT  a compatible CompoundDatatype for the cable's 
                    output, or None if one doesn't exist

        PRE
        cable is neither raw nor trivial
        """
        wires = cable.custom_wires.all()

        # Use wires to determine the CDT of the output of this cable
        all_members = metadata.models.CompoundDatatypeMember.objects # shorthand
        compatible_CDTs = None
        for wire in wires:
            # Find all CompoundDatatypes with correct members.
            candidate_members = all_members.filter(datatype=wire.source_pin.datatype,
                                                   column_name=wire.dest_pin.column_name,
                                                   column_idx=wire.dest_pin.column_idx)
            candidate_CDTs = set([m.compounddatatype for m in candidate_members])
            if compatible_CDTs is None:
                compatible_CDTs = candidate_CDTs
            else:
                compatible_CDTs &= candidate_CDTs # intersection
            if not compatible_CDTs:
                return None

        for output_CDT in compatible_CDTs:
            if output_CDT.members.count() == len(wires):
                return output_CDT

        return None

    # TODO: member function of Pipeline*Cable?
    def _create_cable_compounddatatype(self, cable):
        """Create a CompoundDatatype for the output of a cable.

        INPUTS
        cable       the cable we want a CompoundDatatype for

        OUTPUTS
        output_CDT  a new CompoundDatatype for the cable's output

        PRE
        cable is neither raw nor trivial
        """

        output_CDT = metadata.models.CompoundDatatype()
        wires = cable.custom_wires.all()

        # Use wires to determine the CDT of the output of this cable
        for wire in wires:
            self.logger.debug("Adding CDTM: {} {}".format(wire.dest_pin.column_name, wire.dest_pin.column_idx))
            output_SD_CDT.members.create(datatype=wire.source_pin.datatype,
                                         column_name=wire.dest_pin.column_name,
                                         column_idx=wire.dest_pin.column_idx)

        output_CDT.clean()
        output_CDT.save()
        return output_CDT

    def _update_cable_maps(self, runcable, output_SD, output_path):
        """Update maps after cable execution.
        
        INPUTS
        runcable        RunCable created for cable execution
        output_SD       SymbolicDataset output by cable
        output_path     where the cable wrote its output
        """
        self.register_symbolicdataset(output_SD, output_path)
        cable = runcable.component
        self.socket_map[(runcable.parent_run, cable, cable.dest)] = output_SD
        self.cable_map[(runcable.parent, cable)] = runcable

    def _update_step_maps(self, runstep, step_run_dir, output_paths):
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
            corresp_SD = corresp_ero.symbolicdataset
            self.register_symbolicdataset(corresp_SD, output_paths[i])

            # This pipeline step, with the downstream TI, maps to corresp_SD
            self.socket_map[(runstep.parent_run, pipelinestep, step_output)] = corresp_SD

    def _register_missing_output(self, output_SD, execlog, start_time, end_time):
        """Create a failed ContentCheckLog for missing cable output
        
        INPUTS
        output_SD       SymbolicDataset cable was supposed to output
        execlog         ExecLog for cable execution which didn't produce
                        output
        start_time      time when we started checking for missing output
        end_time        time when we finished checking for missing output
        """
        self.logger.error("File doesn't exist - creating CCL with BadData")
        ccl = output_SD.content_checks.create(start_time=start_time, end_time=end_time, execlog=execlog)
        ccl.add_missing_output()
            
    def execute_cable(self, cable, parent_record, recovering_record=None, input_SD=None, output_path=None):
        """Execute cable on the input.

        INPUTS
        cable           PSIC/POC to execute
        input_SD        SD fed into the PSIC/POC
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
        sd_fs_map and cable_map will have been updated.

        PRECONDITIONS
        1) input_SD has an appropriate CDT for feeding this cable.
        2) All the _maps are "up to date" for this step
        3) input_SD is clean and not sour
        4) If not recovering, input_SD and output_path must both be set
        """
        recover = recovering_record is not None

        # TODO: assertion for precondition 1?
        assert input_SD is None or input_SD.clean() is None
        assert input_SD is None or input_SD.is_OK()
        assert recover or (input_SD and output_path)

        self.logger.debug("STARTING EXECUTING CABLE")

        # Recovering? No.
        if not recover:

            # Create new RSIC/ROC
            curr_record = archive.models.RunCable.create(cable, parent_record)
            self.logger.debug("Not recovering - created {}".format(curr_record.__class__.__name__))
            self.logger.debug("Cable keeps output? {}".format(curr_record.keeps_output()))
    
            curr_ER = self._find_cable_execrecord(curr_record, input_SD)
    
            # ER with compatible cable exists? Yes.
            if curr_ER:
                output_SD = curr_ER.execrecordouts.first().symbolicdataset
    
                # ER is completely reusable? Yes.
                if not curr_record.keeps_output() or output_SD.has_data():
                    self.logger.debug("Reusing ER {}".format(curr_ER))
                    curr_record.link_execrecord(curr_ER, True)
                    self._update_cable_maps(curr_record, output_SD, output_path)
                    curr_record.stop()
                    curr_record.complete_clean()
                    return curr_record
    
            # ER with compatible cable exists and completely reusable? No.
            curr_record.reused = False
            self.logger.debug("No ER to completely reuse - committed to executing cable")

            # Get or create CDT for cable output (Evaluate cable wiring)
            if cable.is_trivial():
                output_CDT = input_SD.get_cdt()
            else:
                output_CDT = self._find_cable_compounddatatype(cable) or self._create_cable_compounddatatype(cable) 

        # Recovering? Yes.
        else:
            self.logger.debug("Recovering - will update old ER")

            # Retrieve appropriate RSIC/ROC
            curr_record = self.cable_map[(parent_record, cable)]

            # Retrieve input_SD and output_path from maps
            curr_ER = curr_record.execrecord
            input_SD = curr_ER.execrecordins.first().symbolicdataset
            output_SD = curr_ER.execrecordouts.first().symbolicdataset
            output_CDT = output_SD.get_cdt()
            output_path = self.find_symbolicdataset(output_SD)

        had_ER_at_beginning = curr_ER is not None

        dataset_path = self.find_symbolicdataset(input_SD)
        # Is input in the sandbox? No.
        if dataset_path is None:

            # Recover dataset.
            self.logger.debug("Symbolic only: running recover({})".format(input_SD))

            successful_recovery = self.recover(input_SD, curr_record)
            successful_str = "successful" if successful_recovery else "unsuccessful"
            self.logger.debug("Recovery was {}".format(successful_str))
            # Success? No.
            if not successful_recovery:

                # End. Return incomplete curr_record.
                self.logger.warn("Recovery failed - returning RSIC/ROC without ExecLog")
                if not recover:
                    curr_record.stop()
                curr_record.clean()
                return curr_record

            # Success? Yes.
            dataset_path = self.find_symbolicdataset(input_SD)
            self.logger.debug("Dataset recovered: running run_cable({})".format(dataset_path))

        # Create ExecLog invoked by...
        if not recover:
            # ...this RunCable.
            invoking_record = curr_record
        else:
            # ...the recovering RunAtomic.
            invoking_record = recovering_record
        curr_log = archive.models.ExecLog.create(curr_record, invoking_record)

        # Run cable (this completes EL).
        cable.run_cable(dataset_path, output_path, curr_record, curr_log)

        missing_output = False
        start_time = timezone.now()
        if not file_access_utils.file_exists(output_path):
            end_time = timezone.now()
            # May 21, 2014: it's conceivable that the linking could fail in the
            # trivial case; in which case we should associate a "missing data"
            # check to input_SD == output_SD.
            if cable.is_trivial():
                output_SD = input_SD
            if curr_ER is None:
                output_SD = librarian.models.SymbolicDataset.create_empty(output_CDT)
            else:
                output_SD = curr_ER.execrecordouts.first().symbolicdataset
            output_SD.mark_missing(start_time, end_time, curr_log)
            missing_output = True

        elif cable.is_trivial():
            output_SD = input_SD

        else:
            # Do we need to keep this output?
            make_dataset = curr_record.keeps_output()
            dataset_name = curr_record.output_name()
            dataset_desc = curr_record.output_description()
            if not make_dataset:
                self.logger.debug("Cable doesn't keep output: not creating a dataset")

            if curr_ER is not None:
                output_SD = curr_ER.execrecordouts.first().symbolicdataset
                if make_dataset:
                    output_SD.register_dataset(output_path, self.user, dataset_name, dataset_desc, curr_RS)

            else:
                output_SD = librarian.models.SymbolicDataset.create_SD(output_path, output_CDT, make_dataset, self.user, 
                                                                       dataset_name, dataset_desc, curr_record, False)

        # Recovering? No.
        if not recover:

            # Creating a new ER, or filling one in? Creating new.
            if curr_ER is None:
                self.logger.debug("No ExecRecord already in use - creating fresh cable ExecRecord")

                # Make ExecRecord, linking it to the ExecLog.
                curr_ER = librarian.models.ExecRecord.create(curr_log, cable, [input_SD], [output_SD])

            # Link ER to RunCable.
            curr_record.link_execrecord(curr_ER, False)

        # Recovering? Yes.
        else:
            self.logger.debug("This was a recovery - not linking RSIC/RunOutputCable to ExecRecord")

        ####
        # Check outputs
        ####

        if not missing_output:
            # Did ER already exist, or is cable trivial, or recovering? Yes.
            if had_ER_at_beginning or cable.is_trivial() or recover:
                self.logger.debug("Performing integrity check of trivial or previously generated output")

                # Perform integrity check.
                output_SD.check_integrity(output_path, self.user, curr_log, output_SD.MD5_checksum)

            # Did ER already exist, or is cable trivial, or recovering? No.
            else:
                self.logger.debug("Performing content check for output generated for the first time")
                summary_path = "{}_summary".format(output_path)
                # Perform content check.
                output_SD.check_file_contents(output_path, summary_path, cable.min_rows_out,
                                              cable.max_rows_out, curr_log)

            # Check OK, and not recovering? Yes.
            if output_SD.is_OK() and not recover:
                # Success! Update sd_fs/socket/cable_map.
                self._update_cable_maps(curr_record, output_SD, output_path)

        self.logger.debug("DONE EXECUTING {} '{}'".format(type(cable).__name__, cable))

        # End. Return curr_record.  Stop the clock if this was not a recovery.
        if not recover:
            curr_record.stop()
        curr_record.complete_clean()
        return curr_record

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

        assert recover or (inputs and all([i.is_OK() for i in inputs]))
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

        in_dir, out_dir, log_dir = self._setup_step_paths(step_run_dir, recover)

        # Construct or retrieve output_paths.
        output_paths = []
        for curr_output in pipelinestep.outputs:
            if recover:
                corresp_SD = self.socket_map[(parent_run, pipelinestep, curr_output)]
                output_paths.append(self.sd_fs_map[corresp_SD])
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
                                               input_SD=inputs[i], output_path=cable_path)

                # Cable failed, return incomplete RunStep.
                if not curr_RSIC.successful_execution():
                    self.logger.error("PipelineStepInputCable {} failed.".format(curr_RSIC))
                    curr_RS.stop()
                    return curr_RS

                # Cable succeeded.
                inputs_after_cable.append(curr_RSIC.execrecord.execrecordouts.first().symbolicdataset)
            else:
                corresp_ERI = curr_RS.execrecord.execrecordins.get(generic_input=curr_input)
                inputs_after_cable.append(corresp_ERI.symbolicdataset)

        # Look for ExecRecord.
        if recover:
            curr_ER = curr_RS.execrecord
        elif pipelinestep.is_subpipeline:
            curr_ER = None
            self.logger.debug("Step {} is a sub-pipeline, so no ExecRecord is applicable".format(pipelinestep))
        else:
            curr_ER = pipelinestep.transformation.definite.find_compatible_ER(inputs_after_cable)

            if curr_ER is not None:
                had_ER_at_beginning = True

                # ER is completely reusable? Yes.
                if curr_ER.provides_outputs(pipelinestep.outputs_to_retain()):
                    self.logger.debug("Completely reusing ExecRecord {}".format(curr_ER))

                    # Set RunStep as reused and link ExecRecord; update maps; return RunStep.
                    with transaction.atomic():
                        curr_RS.link_execrecord(curr_ER, True)
                        curr_RS.stop()
                    self._update_step_maps(curr_RS, step_run_dir, output_paths)
                    return curr_RS

                self.logger.debug("Filling in ExecRecord {}".format(curr_ER))

            else:
                self.logger.debug("No compatible ExecRecord found - will create new ExecRecord")
                had_ER_at_beginning = False

            curr_RS.reused = False
            curr_RS.save()

        invoking_record = recovering_record if recover else curr_RS

        # Gather inputs.
        for curr_in_SD in inputs_after_cable:

            # Check if required SymbolicDatasets are on the file system for running code.
            if self.find_symbolicdataset(curr_in_SD) is None:
                self.logger.debug("Dataset {} not on file system: recovering".format(curr_in_SD))

                # Run recover() on missing datasets.
                if not self.recover(curr_in_SD, invoking_record):

                    # Failed recovery. Return RunStep with failed ExecLogs.
                    self.logger.warn("Recovery of SymbolicDataset {} failed".format(curr_in_SD))
                    curr_RS.stop()
                    return curr_RS

        # Run code.
        # If the step is a sub-Pipeline, execute it and return the RunStep.
        if pipelinestep.is_subpipeline:
            self.execute_pipeline(pipeline=pipelinestep.transformation.pipeline, input_SDs=inputs_after_cable,
                                  sandbox_path=step_run_dir, parent_runstep=curr_RS)
            curr_RS.stop()
            return curr_RS

        # Create ExecLog and MethodOutput; run code.
        curr_log = archive.models.ExecLog.create(curr_RS, invoking_record)
        stdout_path = os.path.join(log_dir, "step{}_stdout.txt".format(pipelinestep.step_num))
        stderr_path = os.path.join(log_dir, "step{}_stderr.txt".format(pipelinestep.step_num))
        input_paths = [self.sd_fs_map[x] for x in inputs_after_cable]
        with open(stdout_path, "w+") as outwrite, open(stderr_path, "w+") as errwrite:
            pipelinestep.transformation.definite.run_code(step_run_dir, input_paths,
                    output_paths, [outwrite], [errwrite],
                    curr_log, curr_log.methodoutput)
        self.logger.debug("Method execution complete, ExecLog saved (started = {}, ended = {})".
                format(curr_log.start_time, curr_log.end_time))

        # Create outputs.
        # bad_output_found indicates we have detected problems with the output.
        bad_output_found = not curr_log.is_successful()
        output_SDs = []
        self.logger.debug("ExecLog.is_successful() == {}".format(curr_log.is_successful()))

        if not (recover or had_ER_at_beginning):
            self.logger.debug("Creating new SymbolicDatasets for PipelineStep outputs")

        for i, curr_output in enumerate(pipelinestep.outputs):
            output_path = output_paths[i]
            output_CDT = curr_output.get_cdt()

            # Check that the file exists, as we did for cables.
            start_time = timezone.now()
            if not file_access_utils.file_exists(output_path):
                end_time = timezone.now()
                if curr_ER is None:
                    output_SD = librarian.models.SymbolicDataset.create_empty(output_CDT)
                else:
                    output_SD = curr_ER.get_execrecordout(curr_output).symbolicdataset
                output_SD.mark_missing(start_time, end_time, curr_log)

                # FIXME continue from here -- for whatever reason an integrity check is still
                # happening after this!
                bad_output_found = True

            else:
                make_dataset = curr_RS.keeps_output(curr_output)
                if (recover or had_ER_at_beginning):
                    output_ERO = curr_ER.get_execrecordout(curr_output)
                    make_dataset = make_dataset and not output_ERO.has_data()
    
                # Create new SymbolicDataset for output, along with Dataset
                # if necessary.
                dataset_name = curr_RS.output_name(curr_output)
                dataset_desc = curr_RS.output_description(curr_output)

                if not (recover or had_ER_at_beginning):
                    output_SD = librarian.models.SymbolicDataset.create_SD(output_path, output_CDT, make_dataset,
                            self.user, dataset_name, dataset_desc, curr_RS, False)
                    self.logger.debug("First time seeing file: saved md5 {}".format(output_SD.MD5_checksum))
                else:
                    output_SD = output_ERO.symbolicdataset
                    if make_dataset:
                        output_SD.register_dataset(output_path, self.user, dataset_name, dataset_desc, curr_RS)
            output_SDs.append(output_SD)

        # Link ExecRecord.
        if not recover:
            if not had_ER_at_beginning:

                # Make new ExecRecord, linking it to the ExecLog
                self.logger.debug("Creating fresh ExecRecord")
                curr_ER = librarian.models.ExecRecord.create(curr_log, pipelinestep, inputs_after_cable, output_SDs)
            # Link ExecRecord to RunStep.
            curr_RS.link_execrecord(curr_ER, False)

        # Check outputs.
        for i, curr_output in enumerate(pipelinestep.outputs):
            output_path = output_paths[i]
            output_SD = curr_ER.get_execrecordout(curr_output).symbolicdataset
            check = None

            if bad_output_found:
                self.logger.debug("Bad output found; no check on {} was done".format(output_path))

            # Recovering or filling in old ER? Yes.
            elif recover or had_ER_at_beginning:
                # Perform integrity check.
                self.logger.debug("SD has been computed before, checking integrity of {}".format(output_SD))
                check = output_SD.check_integrity(output_path, self.user, curr_log)

            # Recovering or filling in old ER? No.
            else:
                # Perform content check.
                self.logger.debug("{} is new data - performing content check".format(output_SD))
                summary_path = "{}_summary".format(output_path)
                check = output_SD.check_file_contents(output_path, summary_path, curr_output.get_min_row(),
                                                      curr_output.get_max_row(), curr_log)

            # Check OK? No.
            if check and check.is_fail():
                self.logger.warn("{} failed for {}".format(check.__class__.__name__, output_path))
                bad_output_found = True

            # Check OK? Yes.
            elif check:
                self.logger.debug("{} passed for {}".format(check.__class__.__name__, output_path))
                    
        curr_ER.complete_clean()

        if not recover:
            # Since reused=False, step_run_dir represents where the step *actually is*
            self._update_step_maps(curr_RS, step_run_dir, output_paths)

        # End. Return curr_RS.  Stop the clock if this was a recovery.
        if not recover:
            curr_RS.stop()
        curr_RS.complete_clean()
        return curr_RS

    def execute_pipeline(self,pipeline=None,input_SDs=None,sandbox_path=None,parent_runstep=None):
        """
        Execute the specified Pipeline with the given inputs.

        INPUTS
        If a top level pipeline, pipeline, input_SDs, sandbox_path,
        and parent_runstep are all None.

        Outputs written to: [sandbox_path]/output_data/run[run PK]_[output name].(csv|raw)
        """
        is_set = (pipeline is not None, input_SDs is not None, sandbox_path is not None, parent_runstep is not None)
        if any(is_set) and not all(is_set):
            raise ValueError("Either none or all parameters must be None")

        if pipeline:
            self.pipeline.check_inputs(input_SDs)
        else:
            pipeline = self.pipeline
        sandbox_path = sandbox_path or self.sandbox_path

        curr_run = self.run

        if (curr_run.is_complete()):
            self.logger.warn('A Pipeline has already been run in Sandbox "{}", returning the previous Run'.format(self))
            return curr_run

        if parent_runstep is not None:
            self.logger.debug("executing a sub-pipeline with input_SD {}".format(input_SDs))
            curr_run = pipeline.pipeline_instances.create(user=self.user, parent_runstep=parent_runstep)
    
        in_dir = os.path.join(sandbox_path, dirnames.IN_DIR)
        out_dir = os.path.join(sandbox_path, dirnames.OUT_DIR)

        if parent_runstep is None:
            file_access_utils.set_up_directory(in_dir)
            file_access_utils.set_up_directory(out_dir)

        for step in pipeline.steps.all().order_by("step_num"):
            self.logger.debug("Executing step {} - looking for cables feeding into this step".format(step))

            step_inputs = []
            run_dir = os.path.join(sandbox_path,"step{}".format(step.step_num))

            # Before executing a step, we need to know what input SDs to feed into the step for execution

            # Because pipeline steps includes the cable execution prior to the transformation,
            # the SDs we need are upstream of the *PSIC* leading to this step

            # For each PSIC leading to this step
            for psic in step.cables_in.all().order_by("dest__dataset_idx"):

                # The socket is upstream of that PSIC
                socket = psic.source.definite

                run_to_query = curr_run

                # If the PSIC comes from another step, the generator is the source pipeline step,
                # or the output cable if it's a sub-pipeline
                if psic.source_step != 0:
                    generator = pipeline.steps.get(step_num=psic.source_step)
                    if socket.transformation.is_pipeline:
                        run_to_query = curr_run.runsteps.get(pipelinestep=generator).child_run
                        generator = generator.transformation.pipeline.outcables.get(output_idx=socket.dataset_idx)

                # Otherwise, the psic comes from step 0
                else:

                    # If this step is not a subpipeline, the dataset was uploaded
                    generator = None

                    # If this step is a subpipeline...
                    if parent_runstep is not None:

                        # Then the run we are interested in is the parent run
                        run_to_query = parent_runstep.run

                        # Get cables in the outer pipeline step leading to this subrun
                        cables_into_subpipeline = parent_runstep.pipelinestep.cables_in

                        # Find the particular cable leading to this PSIC's source
                        generator = cables_into_subpipeline.get(dest=psic.source)

                step_inputs.append(self.socket_map[(run_to_query, generator, socket)])

            curr_RS = self.execute_step(step, curr_run, recovering_record=None, inputs=step_inputs,
                                        step_run_dir=run_dir)
            self.logger.debug("DONE EXECUTING STEP {}".format(step))

            if not curr_RS.is_complete() or not curr_RS.successful_execution():
                self.logger.warn("Step failed to execute: returning the run")
                return curr_run

        self.logger.debug("Finished executing steps, executing POCs")
        for outcable in pipeline.outcables.all():

            generator = None
            run_to_query = curr_run

            # Consider the source step of this POC
            source_step = pipeline.steps.get(step_num=outcable.source_step)

            # By default, the socket is the TO from a pipeline step
            socket = outcable.source

            # The generator of interest is usually just the source pipeline step
            if not source_step.transformation.is_pipeline:
                generator = source_step

            # But if this step contains a subpipeline...
            else:

                # The generator is the subpipeline's output cable
                generator = source_step.transformation.pipeline.outcables.get(output_idx = outcable.source.dataset_idx)

                # And we need the subrun (Get the RS in this run linked to the PS linked by this POCs source)
                runstep_containing_subrun = curr_run.runsteps.get(pipelinestep__step_num=outcable.source_step)

                # Get the run with the above runstep as it's parent
                run_to_query = archive.models.Run.objects.all().get(parent_runstep=runstep_containing_subrun)

            source_SD = self.socket_map[(run_to_query, generator, socket)]
            file_suffix = "raw" if outcable.is_raw() else "csv"
            out_file_name = "run{}_{}.{}".format(curr_run.pk, outcable.output_name,file_suffix)
            output_path = os.path.join(out_dir,out_file_name)
            curr_ROC = self.execute_cable(outcable, curr_run, recovering_record=None,
                                          input_SD=source_SD, output_path=output_path)

            if not curr_ROC.is_complete() or not curr_RS.successful_execution():
                curr_run.clean()
                self.logger.debug("Execution failed")
                return curr_run

        self.logger.debug("Finished executing output cables")
        curr_run.save()
        curr_run.complete_clean()
        self.logger.debug("DONE EXECUTING PIPELINE - Run is complete, clean, and saved")

        return curr_run

    def first_generator_of_SD(self, SD_to_find, curr_run=None):
        """
        Find the (run, generator) pair which first produced a SymbolicDataset.
        If generator is None, it indicates the socket is a Pipeline input. If
        both generator and run are None, it means the SD wasn't found in the
        Pipeline.
        """
        # Is this a sub-run or a top-level run?
        if curr_run is None:
            # This is a top-level run.  Set curr_run accordingly.
            curr_run = self.run
        pipeline = curr_run.pipeline

        # First check if the SD we're looking for is a Pipeline input.
        if curr_run == self.run:
            for socket in pipeline.inputs.order_by("dataset_idx"):
                key = (self.run, None, socket)
                if key in self.socket_map and self.socket_map[key] == SD_to_find:
                    return (self.run, None)

        # If it's not a pipeline input, check all the steps.
        steps = curr_run.runsteps.all()
        steps = sorted(steps, key = lambda step: step.pipelinestep.step_num)

        for step in steps:
            # First check if the SD is an input to this step. In that case, it
            # had to come in from a nontrivial cable (since we're checking the
            # steps in order, and we already checked everything produced prior to
            # this step).
            pipelinestep = step.pipelinestep
            for socket in pipelinestep.transformation.inputs.order_by("dataset_idx"):
                generator = pipelinestep.cables_in.get(dest=socket)
                key = (curr_run, generator, socket)
                if key in self.socket_map and self.socket_map[key] == SD_to_find:
                    return (curr_run, generator)

            # If it wasn't an input to this step, but this step is a sub-pipeline,
            # it might be somewhere within the sub-pipeline. Search recursively.
            if hasattr(step, "child_run") and step.child_run is not None:
                run, generator = self.first_generator_of_SD(SD_to_find, step.child_run)
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
                if key in self.socket_map and self.socket_map[key] == SD_to_find:
                    return (curr_run, generator)

        # Finally, check if it's at the end of a nontrivial Pipeline output cable.
        for outcable in pipeline.outcables.order_by("output_idx"):
            socket = cable.dest
            key = (curr_run, outcable, socket)
            if key in self.socket_map and socket_map[key] == SD_to_find:
                return (curr_run, outcable)

        # If we're here, we didn't find it.
        return (None, None)

    def recover(self, SD_to_recover, invoking_record):
        """
        Writes SD_to_recover to the file system.

        INPUTS
        SD_to_recover   The symbolic dataset we want to recover.
        invoking_record RunAtomic initiating the recovery

        OUTPUTS
        Returns True if successful - otherwise False.

        PRE
        SD_to_recover is in the maps but no corresponding file is on the file system.
        """
        if SD_to_recover.has_data():
            self.logger.debug("Dataset is in the DB - writing it to the file system")
            location = self.sd_fs_map[SD_to_recover]
            saved_data = SD_to_recover.dataset
            try:
                saved_data.dataset_file.open()
                shutil.copyfile(saved_data.dataset_file.name, location)
            except IOError:
                self.logger.error("could not copy file {} to file {}.".format(saved_data.dataset_file, location))
                return False
            finally:
                saved_data.dataset_file.close()
            return True

        self.logger.debug("Performing computation to create missing Dataset")

        # Search for the generator of the SD in the Pipeline.
        curr_run, generator = self.first_generator_of_SD(SD_to_recover)

        if curr_run is None:
            raise ValueError('SymbolicDataset "{}" was not found in Pipeline "{}" and cannot be recovered'
                             .format(SD_to_recover, self.pipeline))
        elif generator is None:
            raise ValueError('SymbolicDataset "{}" is an input to Pipeline "{}" and cannot be recovered'
                             .format(SD_to_recover, self.pipeline))

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

    # Code to execute code in an MPI environment.
    def register_completed_task(self, runcomponent):
        """Report to the Sandbox that the specified task has completed."""
        self.tasks_completed[runcomponent] = True

    # This function looks for stuff that can run now that things are complete.
    def get_runnable_tasks(self, data_newly_available):
        """
        Function that goes through execution, creating a list of steps/outcables that are ready to run.

        Rather than farm out the easy task of reusing steps to workers or executing trivial cables,
        this method does the reuse itself (via helpers) and proceeds.  Also, this method should do the drilling
        down into sub-pipelines.

        PRE:
        the step that just finished is complete and successful.
        """
        # assert step_newly_finished.is_complete()
        # assert step_newly_finished.successful_execution()
        #
        # data_newly_available = [x.symbolicdataset for x in step_newly_finished.execrecord.execrecordouts_in_order]
        run = run or self.run

        for SD in data_newly_available:
            assert SD.has_data() or self.find_symbolicdataset(SD) is not None

        # First, get anything that was waiting on this data to proceed.
        taxiing_for_takeoff = []
        for SD in data_newly_available:
            if SD in self.tasks_waiting:
                # Notify all tasks waiting on this SD that it's now available.
                # Trigger any tasks for which that was the last piece it was waiting on.
                for task in self.tasks_waiting[SD]:
                    self.waiting_for[task].remove(SD)
                    if len(self.waiting_for[task]) == 0:
                        # Add this to the list of things that are ready to go.
                        taxiing_for_takeoff.append(task)

                # Remove this entry from self.tasks_waiting.
                self.tasks_waiting.pop(SD)

        self.queue_for_processing = self.queue_for_processing + taxiing_for_takeoff

    def advance_pipeline(self, run_to_resume=None, step_completed=None, cable_completed=None):
        """
        Proceed through a pipeline, seeing what can run now that a step or cable has just completed.

        Note that if a sub-pipeline of the pipeline finishes, we report that the parent runstep
        has finished, not the cables.
        """
        assert type(step_completed) == archive.models.RunStep or step_completed is None
        assert (type(cable_completed) in (archive.models.RunSIC, archive.models.RunOutputCable) or
                cable_completed is None)
        run_to_resume = run_to_resume or self.run
        pipeline_to_resume = run_to_resume.pipeline

        step_num_completed = 0
        if step_completed is not None:
            step_num_completed = step_completed.step_num

        # A list of steps/tasks that have just completed, including those that may have
        # just successfully been reused during this call to advance_pipeline.
        step_nums_completed = [step_num_completed]

        cables_completed = [cables_completed] if cable_completed is not None else []

        # Go through steps in order, looking for input cables pointing at the task(s) that have just completed.
        # If step_completed is None, then we are starting the pipeline and we look at the pipeline inputs.
        for step in pipeline_to_resume.steps.order_by("step_num"):
            # First, if this is already running, we skip it.
            corresp_runstep = run_to_resume.runsteps.filter(pipelinestep=step)
            if len(corresp_runstep) > 0:
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
                    output_fed = parent_runstep.transformation.outputs.get(dataset_idx=cable.output_idx)
                    if step.cables_in.filter(source_step=parent_runstep.step_num, source=output_fed):
                        fed_by_newly_completed = True
                        break

            if not fed_by_newly_completed:
                continue

            # Examine this step and see if all of the inputs are (at least symbolically) available.
            step_inputs = []

            # For each PSIC leading to this step, check if its required SD is in the maps.
            all_inputs_fed = True
            for psic in step.cables_in.all().order_by("dest__dataset_idx"):
                socket = psic.source.definite

                run_to_query = run_to_resume

                # If the PSIC comes from another step, the generator is the source pipeline step,
                # or the output cable if it's a sub-pipeline.
                if psic.source_step != 0:
                    generator = pipeline.steps.get(step_num=psic.source_step)
                    if socket.transformation.is_pipeline:
                        run_to_query = run_to_resume.runsteps.get(pipelinestep=generator).child_run
                        generator = generator.transformation.pipeline.outcables.get(output_idx=socket.dataset_idx)

                # Otherwise, the psic comes from step 0
                else:
                    # If this step is not a subpipeline, the dataset was uploaded
                    generator = None

                    # If this step is a subpipeline, then the run we are interested in is the parent run.
                    # Get the PSICs feeding into the parent runstep.
                    if run_to_resume.parent_runstep is not None:
                        run_to_query = parent_runstep.run

                        # Get cables in the outer pipeline step leading to this subrun
                        cables_into_subpipeline = parent_runstep.pipelinestep.cables_in

                        # Find the particular cable leading to this PSIC's source
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
            self.logger.debug("Beginning execution of step")
            run_dir = os.path.join(sandbox_path,"step{}".format(step.step_num))
            curr_RS = self.reuse_or_prepare_step(step, run_to_resume, step_inputs, run_dir)

            # If the step we just started is for a Method, and it was successfully reused, then we add its step
            # number to the list of those just completed.  This may then allow subsequent steps to also be started.
            if not curr_RS.is_subpipeline:
                if curr_RS.is_complete() and curr_RS.successful_execution():
                    step_nums_completed.append(step.step_num)
            # Otherwise, we look and see if any of its outcables are complete.  If so, then add them to the
            # list -- they may allow stuff to run.
            else:
                for roc in curr_RS.child_run.runoutputcables.all():
                    if roc.is_complete() and roc.successful_execution():
                        cables_completed.append(roc)

        # Now go through the output cables and do the same.
        for outcable in pipeline_to_resume.outcables.order_by("output_idx"):
            # First, if this is already running, we skip it.
            if run_to_resume.runoutputcables.filter(pipelineoutputcable=outcable).exists():
                continue

            # Check if this cable has just had its input made available.
            source_SD = None
            fed_by_newly_completed = False
            for idx in step_nums_completed:
                if outcable.source_step == idx:
                    source_SD = self.socket_map[(run_to_resume, pipeline_to_resume.steps.get(step_num=idx),
                                                 outcable.source)]
                    fed_by_newly_completed = True
                    break
            if not fed_by_newly_completed:
                for cable in cables_completed:
                    if type(cable) is not archive.models.RunOutputCable:
                        continue

                    parent_runstep = cable.parent_run.parent_runstep
                    output_fed = parent_runstep.transformation.outputs.get(dataset_idx=cable.output_idx)
                    if outcable.source_step == parent_runstep.step_num and outcable.source == output_fed:
                        source_SD = self.socket_map[(cable.parent_run, cable.pipelineoutputcable, output_fed)]
                        fed_by_newly_completed = True
                        break

            if fed_by_newly_completed:
                file_suffix = "raw" if outcable.is_raw() else "csv"
                out_file_name = "run{}_{}.{}".format(curr_run.pk, outcable.output_name,file_suffix)
                output_path = os.path.join(out_dir,out_file_name)
                self.reuse_or_prepare_cable(outcable, run_to_resume, source_SD, output_path)


    # Modified from execute_cable.
    #
    # We'd invoke this the first time we want to execute this cable.  Either it gets reused, or it later gets
    # finished with finish_cable.
    #
    # Return value (in flux right now): curr_record (which has now been started), curr_ER (which may be None).
    # Rather than returning lists of steps to queue and information about what to queue up, instead we
    # directly write to the sandbox's tables of that information.
    #
    # The goal of this function is really to a) reuse cable if possible b) organize recovery of input if necessary
    # c) inform reuse_or_prepare_step whether this cable is ready to go or not.  For c) the fact that we return
    # curr_record allows the calling RunStep to determine whether it's done or not.
    def reuse_or_prepare_cable(self, cable, parent_record, input_SD, output_path):
        """
        Reuse cable, create a mission for it, or recover the input for this cable.
        """
        assert input_SD.clean() is None
        assert input_SD.is_OK()

        recover = recovering_record is not None

        self.logger.debug("Checking whether cable can be reused")

        # Create new RSIC/ROC.
        curr_record = archive.models.RunCable.create(cable, parent_record)
        self.logger.debug("Not recovering - created {}".format(curr_record.__class__.__name__))
        self.logger.debug("Cable keeps output? {}".format(curr_record.keeps_output()))

        curr_ER = self._find_cable_execrecord(curr_record, input_SD)

        # Bundle up execution info in case this needs to be run, either by recovery or as a first execution.
        exec_info = RSICExecuteInfo(curr_record, curr_ER, input_SD, output_path)
        self.cable_execute_info[(parent_record, cable)] = exec_info

        # ER with compatible cable exists? Yes.
        if curr_ER:
            output_SD = curr_ER.execrecordouts.first().symbolicdataset

            # ER is completely reusable? Yes.
            if not curr_record.keeps_output() or output_SD.has_data():
                # Return curr_record.  The calling method will know that this was successfully reused.
                self.logger.debug("Reusing ER {}".format(curr_ER))
                curr_record.link_execrecord(curr_ER, True)
                self._update_cable_maps(curr_record, output_SD, output_path)
                curr_record.stop()
                curr_record.complete_clean()
                return exec_info

        # ER with compatible cable exists and completely reusable? No.
        curr_record.reused = False
        self.logger.debug("No ER to completely reuse - preparing execution of this cable")

        # If input_SD is in the maps or already has input, we are good, and if not, we call
        # queue_recovery to set up what we need.
        dataset_path = self.find_symbolicdataset(input_SD)
        if dataset_path is None and not input_SD.has_data():
            self.logger.debug("Cable input requires non-trivial recovery")
            # This sets up everything necessary for recovering input_SD.
            self.queue_recovery(input_SD, recovering_record=curr_record)

        # Report back to the calling method what needs to be done to proceed.
        # We don't queue up this cable because we prefer that cables be executed on the same
        # host as the step they feed, so that the data will be in the right place.
        return exec_info

    # We'd call this when we need to prepare a cable for recovery.  This is essentially a "force" version of
    # reuse_or_prepare_cable, where we don't attempt at all to reuse (we're past that point and we know we need
    # to produce real data here).  We only call this function unless somehow a non-trivial
    # cable manages to produce some data that's fed into another step.  This might happen for an outcable.
    def cable_recover_h(self, cable_info):
        """
        Recursive helper for recover that handles recovery of a cable.
        """
        # Unpack info from cable_info.
        cable_record = cable_info.cable_record
        cable = cable_record.definite
        parent_record = cable_record.parent
        input_SD = cable_info.input_SD
        curr_ER = cable_info.execrecord
        output_path = cable_info.output_path
        recovering_record = cable_info.recovering_record
        by_step = cable_info.by_step

        # If this cable is already on the queue, we can return.
        if cable_record in self.queue_for_processing and by_step is None:
            self.logger.debug("Cable is already slated for recovery")
            return

        assert curr_ER is not None
        dataset_path = self.find_symbolicdataset(input_SD)
        # Add this cable to the queue, unlike in reuse_or_prepare_cable.
        # If this is independent of any step recovery, we add it to the queue; either by marking it as
        # waiting for stuff that's going to recover, or by throwing it directly onto the list of tasks to
        # perform.
        if dataset_path is None and not input_SD.has_data():
            self.logger.debug("Cable input requires non-trivial recovery")
            self.queue_recovery(input_SD, recovering_record=recovering_record)

            if by_step is None:
                if input_SD in self.tasks_waiting:
                    self.tasks_waiting[input_SD].append(cable_record)
                else:
                    self.tasks_waiting[input_SD] = [cable_record]
        elif by_step is None:
            self.queue_for_processing.append(cable_record)

        return cable_record

    def finish_cable(self, cable_info):
        """
        Finishes an un-reused cable that has already been prepared for execution.

        If we are reaching this point, we know that the data required for input_SD is either
        in place in the sandbox or available in the database.

        This function is called by finish_step, because we want it to be called by the same
        worker(s) as the step is, so its output is on the local filesystem of the worker, which
        may be a remote MPI host.  It may also be called by cable_recover_h.
        """
        # Break out cable_info.
        curr_record = cable_info.cable_record
        input_SD = cable_info.input_SD
        recovering_record = cable_info.recovering_record
        curr_ER = cable_info.execrecord
        output_path = cable_info.output_path

        # Preconditions to test.
        assert curr_record is not None
        dataset_path = self.find_symbolicdataset(input_SD)
        assert dataset_path is not None or input_SD.has_data()

        cable = curr_record.definite

        recover = recovering_record is not None
        had_ER_at_beginning = curr_ER is not None

        # Write the input SD to the sandbox if necessary.
        # FIXME at some point in the future this will have to be updated to mean "write to the local sandbox".
        if dataset_path is None:
            self.logger.debug("Dataset is in the DB - writing it to the file system")
            location = self.sd_fs_map[SD_to_recover]
            saved_data = SD_to_recover.dataset
            try:
                saved_data.dataset_file.open()
                shutil.copyfile(saved_data.dataset_file.name, location)
            except IOError:
                self.logger.error("could not copy file {} to file {}.".format(saved_data.dataset_file, location))
                return False
            finally:
                saved_data.dataset_file.close()

        output_CDT = None
        output_SD = None
        if not recover:
            # Get or create CDT for cable output (Evaluate cable wiring)
            output_CDT = input_SD.get_cdt()
            if not cable.is_trivial():
                output_CDT = self._find_cable_compounddatatype(cable) or self._create_cable_compounddatatype(cable)

        else:
            self.logger.debug("Recovering - will update old ER")
            output_SD = curr_ER.execrecordouts.first().symbolicdataset
            output_CDT = output_SD.get_cdt()

        # Create ExecLog invoked by...
        if not recover:
            # ...this RunCable.
            invoking_record = curr_record
        else:
            # ...the recovering RunAtomic.
            invoking_record = recovering_record
        curr_log = archive.models.ExecLog.create(curr_record, invoking_record)

        # Run cable (this completes EL).
        cable.run_cable(dataset_path, output_path, curr_record, curr_log)

        missing_output = False
        start_time = timezone.now()
        if not file_access_utils.file_exists(output_path):
            end_time = timezone.now()
            # It's conceivable that the linking could fail in the
            # trivial case; in which case we should associate a "missing data"
            # check to input_SD == output_SD.
            if cable.is_trivial():
                output_SD = input_SD
            if curr_ER is None:
                output_SD = librarian.models.SymbolicDataset.create_empty(output_CDT)
            else:
                output_SD = curr_ER.execrecordouts.first().symbolicdataset
            output_SD.mark_missing(start_time, end_time, curr_log)
            missing_output = True

        elif cable.is_trivial():
            output_SD = input_SD

        else:
            # Do we need to keep this output?
            make_dataset = curr_record.keeps_output()
            dataset_name = curr_record.output_name()
            dataset_desc = curr_record.output_description()
            if not make_dataset:
                self.logger.debug("Cable doesn't keep output: not creating a dataset")

            if had_ER_at_beginning:
                output_SD = curr_ER.execrecordouts.first().symbolicdataset
                if make_dataset:
                    output_SD.register_dataset(output_path, self.user, dataset_name, dataset_desc, curr_RS)

            else:
                output_SD = librarian.models.SymbolicDataset.create_SD(output_path, output_CDT, make_dataset, self.user,
                                                                       dataset_name, dataset_desc, curr_record, False)

        # Link the ExecRecord to curr_record if necessary, creating it if necessary also.
        if not recover:

            if not had_ER_at_beginning:
                self.logger.debug("No ExecRecord already in use - creating fresh cable ExecRecord")

                # Make ExecRecord, linking it to the ExecLog.
                curr_ER = librarian.models.ExecRecord.create(curr_log, cable, [input_SD], [output_SD])

            # Link ER to RunCable (this may have already been linked; that's fine).
            curr_record.link_execrecord(curr_ER, False)

        else:
            self.logger.debug("This was a recovery - not linking RSIC/RunOutputCable to ExecRecord")

        ####
        # Check outputs
        ####

        if not missing_output:
            # Did ER already exist, or is cable trivial, or recovering? Yes.
            if had_ER_at_beginning or cable.is_trivial() or recover:
                self.logger.debug("Performing integrity check of trivial or previously generated output")

                # Perform integrity check.
                output_SD.check_integrity(output_path, self.user, curr_log, output_SD.MD5_checksum)

            # Did ER already exist, or is cable trivial, or recovering? No.
            else:
                self.logger.debug("Performing content check for output generated for the first time")
                summary_path = "{}_summary".format(output_path)
                # Perform content check.
                output_SD.check_file_contents(output_path, summary_path, cable.min_rows_out,
                                              cable.max_rows_out, curr_log)

            # Check OK, and not recovering? Yes.
            if output_SD.is_OK() and not recover:
                # Success! Update sd_fs/socket/cable_map.
                self._update_cable_maps(curr_record, output_SD, output_path)

        self.logger.debug("DONE EXECUTING {} '{}'".format(type(cable).__name__, cable))

        # End. Return curr_record.  Stop the clock if this was not a recovery.
        if not recover:
            curr_record.stop()
        curr_record.complete_clean()
        return curr_record

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
        assert all([i.is_OK() for i in inputs])
        curr_RS = archive.models.RunStep.create(pipelinestep, parent_run)

        if pipelinestep.is_subpipeline:
            self.subpipelines_in_progress[curr_RS] = True

        input_names = ", ".join(str(i) for i in inputs)
        self.logger.debug("Beginning execution of step {} in directory {} on inputs {}"
                          .format(pipelinestep, step_run_dir, input_names))

        # Check which steps we're waiting on.
        # Make a note of those steps that feed cables that are reused, but do not retain their output,
        # i.e. those cables that are *symbolically* reusable but not *actually* reusable.
        SDs_to_recover = []
        symbolically_okay_SDs = []
        cable_table = {}
        for i, curr_input in enumerate(pipelinestep.inputs):
            # The cable that feeds this input and where it will write its eventual output.
            corresp_cable = pipelinestep.cables_in.get(dest=curr_input)
            cable_path = self.step_xput_path(curr_RS, curr_input, step_run_dir)

            cable_exec_info = self.reuse_or_prepare_cable(
                corresp_cable, curr_RS, inputs[i], cable_path
                )
            cable_record = cable_exec_info.cable_record
            cable_ER = cable_exec_info.execrecord

            cable_table[corresp_cable] = cable_exec_info

            if not cable_exec_info.cable_record.is_complete():
                SDs_to_recover.append(inputs[i])
            elif not cable_exec_info.execrecord.execrecordouts.first().symbolicdataset.has_data():
                symbolically_okay_SDs.append(inputs[i])

        # First, we bundle up the information required to process this step:
        execute_info = RunStepExecuteInfo(cable_table, None, inputs, step_run_dir)
        self.step_execute_info[(parent_run, pipelinestep)] = execute_info

        # If we're waiting on feeder steps, register this step as waiting for other steps,
        # and return the (incomplete) RunStep.  Note that if this happens, we will have to
        # recover the feeders that were only symbolically OK, so we add those to the pile.
        # The steps that need to precede it have already been added to the queue above by
        # reuse_or_prepare_cable.
        if len(SDs_to_recover) > 0:
            for input_SD in SDs_to_recover + symbolically_okay_SDs:
                if input_SD not in self.tasks_waiting:
                    self.tasks_waiting[input_SD] = [curr_RS]
                else:
                    self.tasks_waiting[input_SD].append(curr_RS)
            self.waiting_for[curr_RS] = SDs_to_recover + symbolically_okay_SDs
            return execute_info

        # At this point we know that we're at least symbolically OK to proceed.

        # Recurse if this step is a sub-pipeline.
        if pipelinestep.is_subpipeline:
            # Recurse -- call a routine that will start the sub-pipeline.
            # FIXME fill this out when we figure out what it does
            self.get_runnable_steps(execute_info)
            return execute_info

        # Look for a reusable ExecRecord.  If we find it, then complete the RunStep.
        inputs_after_cable = []
        for i, curr_input in enumerate(pipelinestep.inputs):
            corresp_cable = pipelinestep.cables_in.get(dest=curr_input)
            curr_RSIC = cable_table[corresp_cable].cable_record
            inputs_after_cable.append(curr_RSIC.execrecord.execrecordouts.first().symbolicdataset)

        curr_ER = pipelinestep.transformation.definite.find_compatible_ER(inputs_after_cable)
        if curr_ER is not None:
            execute_info.execrecord = curr_ER
            if curr_ER.provides_outputs(pipelinestep.outputs_to_retain()):
                self.logger.debug("Completely reusing ExecRecord {}".format(curr_ER))

                # Set RunStep as reused and link ExecRecord; update maps; return RunStep.
                with transaction.atomic():
                    curr_RS.link_execrecord(curr_ER, True)
                    curr_RS.stop()
                self._update_step_maps(curr_RS, step_run_dir, output_paths)
                return curr_RS

        # We found no reusable ER, so we add this step to the queue.
        # If there were any inputs that were only symbolically OK, we call queue_recover on them and register
        # this step as waiting for them.
        if len(symbolically_okay_SDs) > 0:
            for missing_data in symbolically_okay_SDs:
                # FIXME fill this in later
                self.queue_recovery(missing_data, recovering_record=curr_RS)

                if missing_data not in self.tasks_waiting:
                    self.tasks_waiting[missing_data] = [curr_RS]
                else:
                    self.tasks_waiting[missing_data].append(curr_RS)
            self.waiting_for[curr_RS] = symbolically_okay_SDs
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
        step_run_dir = execute_info.step_run_dir
        curr_ER = execute_info.execrecord
        cable_table = execute_info.cable_table
        inputs = execute_info.inputs
        recovering_record = execute_info.recovering_record
        cable_table = execute_info.cable_table

        pipelinestep = runstep.pipelinestep
        parent_run = runstep.parent_run
        assert not pipelinestep.is_subpipeline

        # If this runstep is already on the queue, we can return.
        if runstep in self.queue_for_processing:
            self.logger.debug("Step already in queue for execution")
            return

        # Check which cables need to be re-run.  Since we're forcing this step, we can't have
        # cables which are only symbolically OK, we need their output to be available either in the
        # sandbox or in the database.
        SDs_to_recover_first = []
        for cable in cable_table:
            # We use the sandbox's version of the execute information for this cable.
            cable_info = self.cable_execute_info[(cable_table[cable].cable_record.parent_run, cable)]

            # If the cable needs its feeding steps to recover, we throw them onto the queue if they're not there
            # already.
            cable_out_SD = cable_info.execrecord.execrecordouts.first().symbolicdataset
            if not cable_out_SD.has_data() and not self.find_symbolicdataset(cable_out_SD):
                SDs_to_recover_first.append(cable_info.input_SD)
                execute_info.flag_for_recovery(recovering_record, by_step=runstep)
                self.cable_recover_h(execute_info)

        if len(SDs_to_recover_first) > 0:
            for SD in SDs_to_recover_first:
                if SD in self.tasks_waiting:
                    self.tasks_waiting.append(runstep)
                else:
                    self.tasks_waiting = [runstep]
            self.waiting_for[runstep] = SDs_to_recover_first
        else:
            self.queue_for_processing.append(runstep)

    # The actual running of code happens here.  We copy and modify this from execute_step.
    def finish_step(self, execute_info):
        """
        Carry out the task specified by runstep and execute_info.

        Precondition: the task must be ready to go, i.e. its inputs must all be in place.  Also
        it should not have been run previously.  This should not be a RunStep representing a Pipeline.
        """
        # Break out execute_info.
        runstep = execute_info.runstep
        step_run_dir = execute_info.step_run_dir
        curr_ER = execute_info.execrecord
        cable_table = execute_info.cable_table
        inputs = execute_info.inputs
        recovering_record = execute_info.recovering_record

        pipelinestep = runstep.pipelinestep
        parent_run = runstep.parent_run
        assert not pipelinestep.is_subpipeline

        had_ER_at_beginning = curr_ER is not None
        recover = recovering_record is not None

        in_dir, out_dir, log_dir = self._setup_step_paths(step_run_dir, recover)

        # Construct or retrieve output_paths.
        output_paths = []
        for curr_output in pipelinestep.outputs:
            if recover:
                corresp_SD = self.socket_map[(parent_run, pipelinestep, curr_output)]
                output_paths.append(self.sd_fs_map[corresp_SD])
            else:
                output_paths.append(self.step_xput_path(runstep, curr_output, step_run_dir))

        ####

        # Gather inputs: finish all input cables -- we want them written to the sandbox now, which is never
        # done by reuse_or_prepare_cable.
        inputs_after_cable = []
        for cable in cable_table:
            curr_RSIC = self.finish_cable(cable_table[cable], recovering_record)

            # Cable failed, return incomplete RunStep.
            if not curr_RSIC.successful_execution():
                self.logger.error("PipelineStepInputCable {} failed.".format(curr_RSIC))
                curr_RS.stop()
                return curr_RS

            # Cable succeeded.
            curr_in_SD = curr_RSIC.execrecord.execrecordouts.first().symbolicdataset
            assert self.find_symbolicdataset(curr_in_SD) is not None
            inputs_after_cable.append(curr_RSIC.execrecord.execrecordouts.first().symbolicdataset)

        invoking_record = recovering_record if recover else curr_RS

        # Run code, creating ExecLog and MethodOutput.
        curr_log = archive.models.ExecLog.create(curr_RS, invoking_record)
        stdout_path = os.path.join(log_dir, "step{}_stdout.txt".format(pipelinestep.step_num))
        stderr_path = os.path.join(log_dir, "step{}_stderr.txt".format(pipelinestep.step_num))
        input_paths = [self.sd_fs_map[x] for x in inputs_after_cable]
        with open(stdout_path, "w+") as outwrite, open(stderr_path, "w+") as errwrite:
            pipelinestep.transformation.definite.run_code(step_run_dir, input_paths,
                    output_paths, [outwrite], [errwrite],
                    curr_log, curr_log.methodoutput)
        self.logger.debug("Method execution complete, ExecLog saved (started = {}, ended = {})".
                format(curr_log.start_time, curr_log.end_time))

        # Create outputs.
        # bad_output_found indicates we have detected problems with the output.
        bad_output_found = not curr_log.is_successful()
        output_SDs = []
        self.logger.debug("ExecLog.is_successful() == {}".format(curr_log.is_successful()))

        if not (recover or had_ER_at_beginning):
            self.logger.debug("Creating new SymbolicDatasets for PipelineStep outputs")

        for i, curr_output in enumerate(pipelinestep.outputs):
            output_path = output_paths[i]
            output_CDT = curr_output.get_cdt()

            # Check that the file exists, as we did for cables.
            start_time = timezone.now()
            if not file_access_utils.file_exists(output_path):
                end_time = timezone.now()
                if curr_ER is None:
                    output_SD = librarian.models.SymbolicDataset.create_empty(output_CDT)
                else:
                    output_SD = curr_ER.get_execrecordout(curr_output).symbolicdataset
                output_SD.mark_missing(start_time, end_time, curr_log)

                # FIXME continue from here -- for whatever reason an integrity check is still
                # happening after this!
                bad_output_found = True

            else:
                make_dataset = curr_RS.keeps_output(curr_output)
                if (recover or had_ER_at_beginning):
                    output_ERO = curr_ER.get_execrecordout(curr_output)
                    make_dataset = make_dataset and not output_ERO.has_data()

                # Create new SymbolicDataset for output, along with Dataset
                # if necessary.
                dataset_name = curr_RS.output_name(curr_output)
                dataset_desc = curr_RS.output_description(curr_output)

                if not (recover or had_ER_at_beginning):
                    output_SD = librarian.models.SymbolicDataset.create_SD(output_path, output_CDT, make_dataset,
                            self.user, dataset_name, dataset_desc, curr_RS, False)
                    self.logger.debug("First time seeing file: saved md5 {}".format(output_SD.MD5_checksum))
                else:
                    output_SD = output_ERO.symbolicdataset
                    if make_dataset:
                        output_SD.register_dataset(output_path, self.user, dataset_name, dataset_desc, curr_RS)
            output_SDs.append(output_SD)

        # Link ExecRecord.
        if not recover:
            if not had_ER_at_beginning:

                # Make new ExecRecord, linking it to the ExecLog
                self.logger.debug("Creating fresh ExecRecord")
                curr_ER = librarian.models.ExecRecord.create(curr_log, pipelinestep, inputs_after_cable, output_SDs)
            # Link ExecRecord to RunStep (it may already have been linked; that's fine).
            curr_RS.link_execrecord(curr_ER, False)

        # Check outputs.
        for i, curr_output in enumerate(pipelinestep.outputs):
            output_path = output_paths[i]
            output_SD = curr_ER.get_execrecordout(curr_output).symbolicdataset
            check = None

            if bad_output_found:
                self.logger.debug("Bad output found; no check on {} was done".format(output_path))

            # Recovering or filling in old ER? Yes.
            elif recover or had_ER_at_beginning:
                # Perform integrity check.
                self.logger.debug("SD has been computed before, checking integrity of {}".format(output_SD))
                check = output_SD.check_integrity(output_path, self.user, curr_log)

            # Recovering or filling in old ER? No.
            else:
                # Perform content check.
                self.logger.debug("{} is new data - performing content check".format(output_SD))
                summary_path = "{}_summary".format(output_path)
                check = output_SD.check_file_contents(output_path, summary_path, curr_output.get_min_row(),
                                                      curr_output.get_max_row(), curr_log)

            # Check OK? No.
            if check and check.is_fail():
                self.logger.warn("{} failed for {}".format(check.__class__.__name__, output_path))
                bad_output_found = True

            # Check OK? Yes.
            elif check:
                self.logger.debug("{} passed for {}".format(check.__class__.__name__, output_path))

        curr_ER.complete_clean()

        if not recover:
            # Since reused=False, step_run_dir represents where the step *actually is*
            self._update_step_maps(curr_RS, step_run_dir, output_paths)

        # End. Return curr_RS.  Stop the clock if this was a recovery.
        if not recover:
            curr_RS.stop()
        curr_RS.complete_clean()
        return curr_RS

    def queue_recovery(self, SD_to_recover, invoking_record):
        """
        Determines and enqueues the steps necessary to reproduce the specified SymbolicDataset.

        This is an MPI-friendly version of recover.  It only ever handles non-trivial recoveries,
        as trivial recoveries are now performed by cables themselves.

        PRE
        SD_to_recover is in the maps but no corresponding file is on the file system.
        """
        # NOTE before we recover an SD, we should look to see if it's already being recovered
        # by something else.  If so we can just wait for that to finish.
        assert SD_to_recover in self.sd_fs_map
        assert not self.find_symbolicdataset(SD_to_recover)

        self.logger.debug("Performing computation to create missing Dataset")

        # Search for the generator of the SD in the Pipeline.
        curr_run, generator = self.first_generator_of_SD(SD_to_recover)

        if curr_run is None:
            raise ValueError('SymbolicDataset "{}" was not found in Pipeline "{}" and cannot be recovered'
                             .format(SD_to_recover, self.pipeline))
        elif generator is None:
            raise ValueError('SymbolicDataset "{}" is an input to Pipeline "{}" and cannot be recovered'
                             .format(SD_to_recover, self.pipeline))

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
            self.cable_recover_h(curr_execute_info)


# A simple struct that holds the information required to perform a RunStep.
class RunStepExecuteInfo:
    def __init__(self, runstep, cable_table, execrecord, inputs, step_run_dir, recovering_record=None):
        self.runstep = runstep
        self.cable_table = cable_table
        self.execrecord = execrecord
        self.inputs = inputs
        self.step_run_dir = step_run_dir
        self.recovering_record = recovering_record

    def flag_for_recovery(self, recovering_record):
        assert self.recovering_record is None
        self.recovering_record = recovering_record


class RSICExecuteInfo:
    def __init__(self, cable_record, execrecord, input_SD, output_path, recovering_record=None, by_step=None):
        self.cable_record = cable_record
        self.execrecord = execrecord
        self.input_SD = input_SD
        self.output_path = output_path
        self.recovering_record = recovering_record
        self.by_step = by_step

    def flag_for_recovery(self, recovering_record, by_step=None):
        assert self.recovering_record is None
        self.recovering_record = recovering_record
        self.by_step = by_step