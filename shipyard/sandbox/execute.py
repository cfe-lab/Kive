"""Code that is responsible for the execution of Pipelines."""

from django.core.files import File
from django.contrib.contenttypes.models import ContentType
from django.utils import timezone
from django.db import transaction

import archive.models
import librarian.models
import metadata.models
import pipeline.models 
import transformation.models
import datachecking.models

import file_access_utils
import logging_utils
from constants import dirnames, extensions

import os.path
import shutil
import logging
import sys
import time
import tempfile
import errno


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

    # socket_map: maps (generator, socket) to SDs.
    # A generator is a cable, or a pipeline step. A socket is a TI/TO.
    # If the generator is none, the socket is a pipeline input.
    # This will be used to look up inputs when running a pipeline.
    
    # ps_map: maps PS to (path, RunStep of PS): the path tells you
    # the directory that the PS would have been run in
    # (whether or not it was): the RunStep tells you what inputs are
    # needed (Which in turn will lead back to an sd_fs_map lookup),
    # and allows you to fill it in on recovery.
        
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
        self.check_inputs()

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

    def step_xput_path(self, runstep, transformationxput, step_run_dir):
        """Path in Sandbox for PipelineStep TransformationXput."""
        file_suffix = extensions.RAW if transformationxput.is_raw() else extensions.CSV
        file_name = "step{}_{}.{}".format(runstep.step_num, transformationxput.dataset_name, file_suffix)

        if transformationxput.is_input:
            xput_dir = dirnames.IN_DIR
        else:
            xput_dir = dirnames.OUT_DIR
        return os.path.join(step_run_dir, xput_dir, file_name)

    def check_inputs(self):
        """
        Are the supplied inputs are appropriate for the supplied pipeline?

        We check if the input CDT's are restrictions of the pipeline's expected
        input CDT's, and that the number of rows is in the range that the pipeline
        expects. We don't rearrange inputs that are in the wrong order.
        """
        # First quick check that the number of inputs are the same.
        if len(self.inputs) != self.pipeline.inputs.count():
            raise ValueError('Pipeline "{}" expects {} inputs, but {} were supplied'
                             .format(self.pipeline, self.pipeline.inputs.count(), len(self.inputs)))
        
        # Check each individual input.
        for i, supplied_input in enumerate(self.inputs, start=1):
            pipeline_input = self.pipeline.inputs.get(dataset_idx=i)
            pipeline_raw = pipeline_input.is_raw()
            supplied_raw = supplied_input.is_raw()

            if pipeline_raw != supplied_raw:
                if pipeline_raw:
                    raise ValueError('Pipeline "{}" expected input {} to be raw, but got one with CompoundDatatype '
                                     '"{}"'.format(self.pipeline, i, supplied_input.get_cdt()))
                raise ValueError('Pipeline "{}" expected input {} to be of CompoundDatatype "{}", but got raw'
                                 .format(self.pipeline, i, pipeline_input.get_cdt()))

            # Neither is raw.
            supplied_cdt = supplied_input.get_cdt()
            pipeline_cdt = pipeline_input.get_cdt()

            if not supplied_cdt.is_restriction(pipeline_cdt):
                raise ValueError('Pipeline "{}" expected input {} to be of CompoundDatatype "{}", but got one with '
                                 'CompoundDatatype "{}"'.format(self.pipeline, i, pipeline_cdt, supplied_cdt))

            # The CDT's match. Is the number of rows okay?
            minrows = pipeline_input.get_min_row() or 0
            maxrows = pipeline_input.get_max_row() 
            maxrows = maxrows if maxrows is not None else sys.maxint

            if not minrows <= supplied_input.num_rows() <= maxrows:
                raise ValueError('Pipeline "{}" expected input {} to have between {} and {} rows, but got one with {}'
                                 .format(self.pipeline, i, minrows, maxrows, supplied_input.num_rows()))

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

        If an ExecRecord with real data exists, it will be returned
        preferentially over those without data.

        INPUTS
        runcable        RunCable trying to reuse an ExecRecord
        input_SD        SymbolicDataset to feed the cable

        OUTPUTS
        execrecord      ExecRecord which may be reused, or None if no
                        ExecRecord exists
        """
        execrecord = None
        cable = runcable.component

        # Get the content type (RSIC/ROC) of the ExecRecord's ExecLog
        record_contenttype = ContentType.objects.get_for_model(type(runcable))
        self.logger.debug("Searching for reusable cable ER - (linked to a '{}' ExecLog)".format(record_contenttype))

        # Look at ERIs linked to the same cable type, with matching input SD
        all_ERIs = librarian.models.ExecRecordIn.objects
        candidate_ERIs = all_ERIs.filter(execrecord__generator__content_type=record_contenttype, 
                                         symbolicdataset=input_SD)

        for candidate_ERI in candidate_ERIs:
            self.logger.debug("Considering ERI {} for ER reuse or update".format(candidate_ERI))
            candidate_execrecord = candidate_ERI.execrecord
            candidate_cable = candidate_execrecord.general_transf()

            if type(cable).__name__ == "PipelineOutputCable":
                compatible = cable.is_compatible(candidate_cable)
            else:
                compatible = cable.is_compatible(candidate_cable, input_SD.structure.compounddatatype)

            if compatible:
                self.logger.debug("Compatible ER found")
                if candidate_execrecord.execrecordouts.first().has_data():
                    return candidate_execrecord

                # No data, so we keep it in mind and continue searching.
                execrecord = candidate_execrecord

        return execrecord

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
        if cable.is_incable:
            wires = cable.custom_wires.all()
        else:
            wires = cable.custom_outwires.all()

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
        if type(cable) == pipeline.models.PipelineStepInputCable:
            wires = cable.custom_wires.all()
        else:
            wires = cable.custom_outwires.all()

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
        self.cable_map[cable] = runcable

    def _update_step_maps(self, runstep, step_run_dir, output_paths):
        """Update maps after pipeline step execution.
        
        INPUTS
        runstep         RunStep responsible for execution
        step_run_dir    directory where execution was done
        output_paths    paths where RunStep outputs were written,
                        ordered by index
        """
        pipelinestep = runstep.component
        self.ps_map[pipelinestep] = (step_run_dir, runstep)
        execrecordouts = runstep.execrecord.execrecordouts

        for i, step_output in enumerate(pipelinestep.transformation.outputs.order_by("dataset_idx")):
            corresp_ero = execrecordouts.get(content_type=ContentType.objects.get_for_model(step_output), 
                                             object_id=step_output.pk)
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
            
    def execute_cable(self, cable, parent_record, recover=True, input_SD=None, output_path=None):
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
                    self._update_cable_maps(curr_record, output_SD, output_path)
                    curr_record.link_execrecord(curr_ER, True)
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
            curr_record = self.cable_map[cable]

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

            # Success? No.
            if not self.recover(input_SD, curr_record):

                # End. Return incomplete curr_record.
                self.logger.warn("Recovery failed - returning incomplete RSIC/ROC (missing ExecLog)")
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
            invoking_record = parent_record
        curr_log = archive.models.ExecLog.create(curr_record, invoking_record)

        # Run cable (this completes EL).
        cable.run_cable(dataset_path, output_path, curr_record, curr_log)

        self.logger.debug("Checking for file created by execute_cable")
        missing_output = False
        start_time = timezone.now()
        if not file_access_utils.file_exists(output_path):
            end_time = timezone.now()
            if curr_ER is None:
                output_SD = librarian.models.SymbolicDataset.create_empty(output_CDT)
            else:
                output_SD = curr_ER.execrecordouts.first().symbolicdataset
            output_SD.mark_missing(start_time, end_time, curr_log)
            missing_output = True

        elif cable.is_trivial():
            self.logger.debug("Cable is trivial: not creating a dataset")
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
                curr_ER = librarian.models.ExecRecord.create_complete(curr_log, cable, [input_SD], [output_SD])
            # Link ER to RunCable.
            curr_record.link_execrecord(curr_ER, False)

        # Recovering? Yes.
        else:
            self.logger.debug("This was a recovery - not linking RSIC/RunOutputCable to ExecRecord")

        # Did ER already exist, or is cable trivial, or recovering? Yes.
        if had_ER_at_beginning or cable.is_trivial() or recover:
            self.logger.debug("Performing integrity check of trivial or previously generated output")

            # Perform integrity check.
            output_SD.check_integrity(output_path, curr_log, output_SD.MD5_checksum)

        # Did ER already exist, or is cable trivial, or recovering? No.
        elif not missing_output:
            self.logger.debug("Performing content check for output generated for the first time")
            summary_path = "{}_summary".format(output_path)

            # Perform content check.
            output_SD.check_file_contents(output_path, summary_path, cable.min_rows_out, cable.max_rows_out, curr_log)

        # Check OK, and not recovering? Yes.
        if output_SD.is_OK() and not recover:

            # Success! Update sd_fs/socket/cable_map.
            self._update_cable_maps(curr_record, output_SD, output_path)

        self.logger.debug("DONE EXECUTING {} '{}'".format(type(cable).__name__, cable))

        # End. Return curr_record.
        curr_record.stop()
        curr_record.complete_clean()
        return curr_record

    def execute_step(self, pipelinestep, parent_record, recover=True, inputs=None, step_run_dir=None):
        """Execute the PipelineStep on the inputs.

        * If code is run, outputs go to paths specified in output_paths.
        * Requisite code is placed in step_run_dir (must be specified if not recovering).
        * If recovering, doesn't create new RS/ER; fills in old RS with a new EL
          In this case, step_run_dir is ignored and retrieved using the maps.

        Inputs written to:  [step run dir]/input_data/step[step num]_[input name]
        Outputs written to: [step run dir]/output_data/step[step num]_[output name]
        Logs written to:    [step run dir]/logs/step[step num]_std(out|err).txt
        """
        assert recover or (inputs and all([i.is_OK() for i in inputs]))
        assert recover or step_run_dir 

        # Create or retrieve RunStep.
        if recover:
            step_run_dir, curr_RS = self.ps_map[pipelinestep]
            self.logger.debug("Recovering step {} in directory {}".format(pipelinestep, step_run_dir))
        else:
            curr_RS = archive.models.RunStep.create(pipelinestep, parent_record)
            input_names = ", ".join(str(i) for i in inputs)
            self.logger.debug("Executing step {} in directory {} on inputs {}"
                              .format(pipelinestep, step_run_dir, input_names))

        # Set up run/input/output/log directories.
        if recover:
            step_run_dir, curr_RS = self.ps_map[pipelinestep]
        in_dir, out_dir, log_dir = self._setup_step_paths(step_run_dir, recover)

        # Construct or retrieve output_paths.
        output_paths = []
        for curr_output in pipelinestep.outputs:
            if recover:
                corresp_SD = self.socket_map[(parent_record.parent_run, pipelinestep, curr_output)]
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
                curr_RSIC = self.execute_cable(corresp_cable, curr_RS, False, inputs[i], cable_path)

                # Cable failed, return incomplete RunStep.
                if not curr_RSIC.successful_execution():
                    self.logger.error("PipelineStepInputCable {} failed.".format(curr_RSIC))
                    curr_RS.stop()
                    return curr_RS

                # Cable succeeded.
                inputs_after_cable.append(curr_RSIC.execrecord.execrecordouts.first().symbolicdataset)
            else:
                input_content_type = ContentType.objects.get_for_model(curr_input)
                corresp_ERI = curr_RS.execrecord.execrecordins.get(content_type=input_content_type, 
                                                                   object_id=curr_input.pk)
                inputs_after_cable.append(corresp_ERI.symbolicdataset)

        # Look for ExecRecord.
        if recover:
            curr_ER = curr_RS.execrecord
        elif pipelinestep.is_subpipeline:
            curr_ER = None
            self.logger.debug("Step {} is a sub-pipeline, so no ExecRecord is applicable".format(pipelinestep))
        else:
            curr_ER = pipelinestep.transformation.find_compatible_ER(inputs_after_cable)

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
        invoking_record = parent_record if recover else curr_RS

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
            self.execute_pipeline(pipeline=pipelinestep.transformation, input_SDs=inputs_after_cable,
                                  sandbox_path=step_run_dir, parent_runstep=curr_RS)
            curr_RS.stop()
            return curr_RS

        # Create ExecLog and MethodOutput; run code.
        curr_log = archive.models.ExecLog.create(curr_RS, invoking_record)
        stdout_path = os.path.join(log_dir, "step{}_stdout.txt".format(pipelinestep.step_num))
        stderr_path = os.path.join(log_dir, "step{}_stderr.txt".format(pipelinestep.step_num))
        input_paths = [self.sd_fs_map[x] for x in inputs_after_cable]
        with open(stdout_path, "w+") as outwrite, open(stderr_path, "w+") as errwrite:
            pipelinestep.transformation.run_code(step_run_dir, input_paths,
                    output_paths, [outwrite, sys.stdout], [errwrite, sys.stderr],
                    curr_log, curr_log.methodoutput)
        self.logger.debug("Method execution complete, ExecLog saved (started = {}, ended = {})".
                format(curr_log.start_time, curr_log.end_time))

        # bad_output_found indicates we have detected problems with the output.
        bad_output_found = not curr_log.is_successful()
        output_SDs = []

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

        # Recovering? No.
        if not recover:

            # Creating a new ER, or filling one in? Creating new.
            if not had_ER_at_beginning:

                # Make new ExecRecord, linking it to the ExecLog
                self.logger.debug("Creating fresh ExecRecord")
                curr_ER = librarian.models.ExecRecord.create_complete(curr_log, pipelinestep, inputs_after_cable,
                                                                      output_SDs)
            # Link ExecRecord to RunStep.
            curr_RS.link_execrecord(curr_ER, False)

        # Check outputs.
        for i, curr_output in enumerate(pipelinestep.outputs):
            output_path = output_paths[i]
            output_SD = curr_ER.get_execrecordout(curr_output).symbolicdataset
            check = None
        
            # Recovering or filling in old ER? Yes.
            if recover or had_ER_at_beginning:

                # Perform integrity check.
                self.logger.debug("SD has been computed before, checking integrity of {}".format(output_SD))
                check = output_SD.check_integrity(output_path, curr_log, output_SD.MD5_checksum)

            # Recovering or filling in old ER? No.
            elif not bad_output_found:

                # Perform content check.
                self.logger.debug("New data - performing content check")
                summary_path = "{}_summary".format(output_path)
                check = output_SD.check_file_contents(output_path, summary_path, curr_output.get_min_row(),
                                                      curr_output.get_max_row(), curr_log)

            else:
                self.logger.debug("Bad output found; no check on {} was done".format(output_path))

            # Check OK? No.
            if check and check.is_fail():
                self.logger.warn("{} failed for {}".format(check.__class__.__name__, output_path))
                bad_output_found = True

            # Check OK? Yes.
            elif check:
                self.logger.debug("{} passed for {}".format(check.__class__.__name__, output_path))
                    
        curr_ER.complete_clean()

        if not recover:
            self.logger.debug("Not recovering: finishing bookkeeping and updating maps")
            # Since reused=False, step_run_dir represents where the step *actually is*
            self._update_step_maps(curr_RS, step_run_dir, output_paths)

        # End. Return curr_RS.
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

        is_set = (pipeline != None,input_SDs != None,sandbox_path != None,parent_runstep != None)
        if any(is_set) and not all(is_set):
            raise ValueError("Either none or all parameters must be None")

        pipeline = pipeline or self.pipeline
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
            self.logger.debug("Setting up input and output directories")
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
                socket = psic.source

                run_to_query = curr_run

                # If the PSIC comes from another step, the generator is the source pipeline step,
                # or the output cable if it's a sub-pipeline
                if psic.source_step != 0:
                    generator = pipeline.steps.get(step_num=psic.source_step)
                    if socket.transformation.__class__.__name__ == "Pipeline":
                        run_to_query = curr_run.runsteps.get(pipelinestep=generator).child_run
                        generator = generator.transformation.outcables.get(output_idx=socket.dataset_idx)

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

            curr_RS = self.execute_step(step, curr_run, False, step_inputs, run_dir)
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
            if type(source_step.transformation).__name__ != "Pipeline":
                generator = source_step

            # But if this step contains a subpipeline
            else:

                # The generator is the subpipeline's output cable
                generator = source_step.transformation.outcables.get(output_idx = outcable.source.dataset_idx)

                # And we need the subrun (Get the RS in this run linked to the PS linked by this POCs source)
                runstep_containing_subrun = curr_run.runsteps.get(pipelinestep__step_num=outcable.source_step)

                # Get the run with the above runstep as it's parent
                run_to_query = archive.models.Run.objects.all().get(parent_runstep=runstep_containing_subrun)

            source_SD = self.socket_map[(run_to_query, generator, socket)]
            file_suffix = "raw" if outcable.is_raw() else "csv"
            out_file_name = "run{}_{}.{}".format(curr_run.pk, outcable.output_name,file_suffix)
            output_path = os.path.join(out_dir,out_file_name)
            curr_ROC = self.execute_cable(outcable, curr_run, False, source_SD, output_path)

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
        if curr_run is None:
            curr_run = self.run

        pipeline = curr_run.pipeline

        # First check if the SD we're looking for is a Pipeline input.
        for socket in pipeline.inputs.order_by("dataset_idx"):
            key = (curr_run, None, socket)
            if key in self.socket_map and self.socket_map[key] == SD_to_find:
                return (curr_run, None)

        # If it's not a pipeline input, check all the steps.
        steps = curr_run.runsteps.all()
        steps = sorted(steps, key = lambda step: step.pipelinestep.step_num)

        for step in steps:
            # First check if the SD is an input to this step. In that case, it
            # had to come in from a nontrivial cable (since we're checking the
            # steps in order, and we already checked the inputs).
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
                    else:
                        generator = pipelinestep.cables_in.get(dest=socket)
                        return (curr_run, generator)

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
        curr_run = invoking_record.parent_run

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
            curr_record = self.execute_step(generator, invoking_record)
        elif type(generator) == pipeline.models.PipelineOutputCable:
            curr_record = self.execute_cable(generator, invoking_record)
        elif type(generator) == pipeline.models.PipelineStepInputCable:
            parent_record = curr_run.runsteps.get(pipelinestep=generator.pipelinestep)
            curr_record = self.execute_cable(generator, invoking_record)

        return curr_record.is_complete() and curr_record.successful_execution()
