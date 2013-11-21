"""Code that is responsible for the execution of Pipelines."""

# Import our Shipyard models module.
import models
import file_access_utils
import os.path
import sys
import time

from datetime import datetime

import transformation.models
import pipeline.models
import method.models
import metadata.models

class Sandbox:
    """
    Represents the state of a run.

    This includes keeping track of where the sandbox is, as well
    as where stuff is within the sandbox.
    """

    # sd_fs_map is a dict mapping SymDSs to paths.  The path
    # represents where a data file *should be* (whether or not it 
    # is there).  If the path is None, that means that the SD
    # is available on the database.

    # socket_map is a dict mapping (generator, socket) to SDs.
    # generator is whatever generated this SD (e.g. cable,
    # PipelineStep) and socket is the specific TI/TO.  If generator is
    # None then this means that the socket in question is a Pipeline
    # input.  This will be used to look up inputs when running a
    # pipeline.
    
    # ps_map maps pipelinestep to (path, RunStep of PS): the path
    # tells you the directory that the PS would have been run in
    # (whether or not it was): the RunStep tells you what inputs are
    # needed (Which in turn will lead back to an sd_fs_map lookup),
    # and allows you to fill it in on recovery.
        
    # cable_map maps cables to ROC/RSIC.
    
    def __init__(self, user, pipeline, inputs, sandbox_path=None):
        """
        Sets up a sandboxed environment to run the specified Pipeline.

        user is the user running it; pipeline is the Pipeline to run;
        inputs is a list of SymbolicDatasets to feed pipeline.

        All inputs must either have real data (especially if
        this Sandbox represents a 'top-level' run), or it is
        is in sd_fs_map (e.g. if this is a sub-run) and therefore
        can be recovered using the info in the maps.

        PRE: the inputs are all appropriate for pipeline.
        """
        self.user = user
        self.pipeline = pipeline
        self.parent_sandbox = parent_sandbox
        self.inputs = inputs

        # Set up our maps.
        self.sd_fs_map = {}
        self.socket_map = {}
        self.cable_map = {}

        self.ps_map = {}

        # NEW FOR ERIC
        # Initialize the maps ourselves.
        for i, pipeline_input in enumerate(inputs):
            # Get the corresponding pipeline input, compensating for
            # 0-basedness.
            corresp_pipeline_input = pipeline.inputs.get(
                dataset_idx=i+1)

            self.socket_map[(None, corresp_pipeline_input)] = pipeline_input
            
            self.sd_fs_map[pipeline_input] = None

        # Determine a sandbox path.
        self.sandbox_path = sandbox_path

        # FIXME come up with something more sophisticated later.
        self.run = pipeline.pipeline_instances.create()

        if sandbox_path == None:
            self.sandbox_path = os.path.join(
                "/tmp", "run{}".format(self.run.pk))

        # Make the sandbox directory.
        file_access_utils.set_up_directory(self.sandbox_path)

    def execute_cable(self, cable, input_SD, output_path, parent_record,
                      recover=False):
        """
        Execute the specified PSIC/POC on the given input.

         - input_SD is the SymbolicDataset fed into this cable.
         - output_path is where the output file should be written.
         - parent_record is the record containing this cable: a RunStep
           if this cable is a PSIC; a Run if this cable is a POC.
         - recover is False if this is a normal execution; True if it
           is a recovery operation (i.e. re-running something that was
           reused in order to recover some non-persistent output).

        If recover == True, then input_SD, output_path, and
        parent_record are ignored; output_path is recovered using the
        maps.

        Returns an RSIC/ROC that describes this cable's running.
        Also, sd_fs_map and cable_map will have been updated.

        PRE: whether or not input_SD has real data associated,
        it has an appropriate CDT for feeding this cable.
        PRE: if input_SD has data, and input_path refers to a real file,
        they are the same.
        PRE: input_SD is in sd_fs_map
        PRE: more generally, all the _maps are "up to date" for this step
        PRE: input_SD is currently considered valid (i.e. has not failed
        any integrity or contents checks at this time).
        """
        ####
        # CREATE/RETRIEVE RECORD
        
        # The record that we create/update.
        curr_record = None
        curr_ER = None
        output_SD = None
        # What comes out of the cable will have the following CDT:
        output_SD_CDT = None

        # If this is a regular execution, we create a new record.
        if not recover:
            if type(cable) == PipelineStepInputCable:
                curr_record = cable.psic_instances.create()
            else:
                curr_record = cable.poc_instances.create()
    
            ####
            # LOOK FOR REUSABLE ER
    
            # FIXME now we have to redefine what "reusability" means: an
            # ER should only be considered for reuse if all of its inputs
            # and outputs are still considered valid *at this time*.
            
            # First: we look for an ExecRecord that we can reuse.
            # We first search for ERIs of cables that take input_SD
            # as an input.
            cable_contenttype = ContentType.objects.get_for_model(
                type(cable))
            candidate_ERIs = ExecRecordIn.objects.filter(
                symbolicdataset=input_SD,
                execrecord__content_type=cable_contenttype)
    
            # Check if this cable keeps its output.
            cable_keeps_output = None
            if type(cable) == PipelineStepInputCable:
                cable_keeps_output = cable.keep_output
            else:
                # Check parent_record (which is a Run) whether or
                # not this POC's output is to be deleted.
                if parent_record.parent_runstep != None:
                    cable_keeps_output = not (
                        parent_record.parent_runstep.pipelinestep.
                        outputs_to_delete.filter(
                            dataset_name=cable.output_name).
                        exists())
    
            curr_record.reused = False
            
            # FIXME can we speed this up using a prefetch?
            # Search for an execrecord that we can reuse OR fill in.
            for candidate_ERI in candidate_ERIs:
                candidate_cable = candidate_ERI.execrecord.general_transf
    
                if cable.is_compatible(candidate_cable):
                    cable_out_SD = (candidate_ERI.execrecord.execrecordouts.
                                    all()[0].symbolicdataset)
                    
                    # If you're not keeping the output, or you are and
                    # there is existent data, you can successfully reuse
                    # the ER.
                    if not cable_keeps_output or cable_out_SD.has_data():
                        curr_record.reused = True
                        curr_record.execrecord = candidate_ERI.execrecord
                        curr_record.complete_clean()
                        curr_record.save()
                        
                        # Add the ERO's SD to sd_fs_map if this SD was not
                        # already in sd_fs_map; if it was but had never
                        # been written to the FS, update it with the path.
                        if (cable_out_SD not in self.sd_fs_map or
                                self.sd_fs_map[cable_out_SD] == None):
                            self.sd_fs_map[cable_out_SD] = output_path
    
                        # Add (cable, destination socket) to socket_map.
                        # FIXME: the socket for this cable is determined by looking at the schematic?
                        socket_map[(cable, cable.generic_output)] = cable_out_SD
                        
                        # Add this cable to cable_map.
                        self.cable_map[cable] = curr_record
                        return curr_record
                        
                    # Otherwise (i.e. you are keeping output but the ERO
                    # doesn't have any), we proceed, filling in this ER.
                    else:
                        curr_ER = candidate_ERI.execrecord
                
                        if not cable.is_raw():
                                
                            # Determine the compounddatatype
                            source_CDT = input_SD.structure.compounddatatype
                            wires = None
                            if type(cable) == PipelineStepInputCable:
                                wires = cable.custom_wires.all()
                            else:
                                wires = cable.custom_outwires.all()
                            
                            # This is the new CDT
                            output_SD_CDT = CompoundDatatype()
                            output_SD_CDT.save()
                            
                            # Look at each wire, take the DT from
                            # source_pin, assign the name and index of
                            # dest_pin.
                            for wire in wires:
                                output_SD_CDT.members.create(
                                    datatype=wire.source_pin.datatype,
                                    column_name=wire.dest_pin.column_name,
                                    column_idx=wire.dest_pin.column_idx)
                                
                            output_SD_CDT.clean()
                                        
                        break
                    
            # FINISHED LOOKING FOR REUSABLE ER
            ####
    
        # Recovery case: we update an old one.
        else:
            curr_record = self.cable_map[cable]
            curr_ER = curr_record.execrecord

            input_SD = curr_ER.execrecordins.all()[0].symbolicdataset

            output_SD = curr_ER.execrecordouts.all()[0].symbolicdataset
            output_SD_CDT = output_SD.get_cdt()
            output_path = self.sd_fs_map[output_SD]

        # FINISHED CREATING/RETRIEVING RECORD
        ####
        
        ####
        # RUN CABLE
        
        # At this point, we know we cannot reuse an ER, so we
        # will have to run the cable.

        # Since this is where the run will happen, we collect the
        # produced ExecLog.
        curr_log = None

        # The input contents are not on the file system, and:
        if (self.sd_fs_map[input_SD] == None or 
                not os.access(self.sd_fs_map[input_SD], os.R_OK):

            # 1A) input_SD has data (Data uploaded or from reused step)
            # --> Use input_SD.dataset for computation
            if input_SD.has_data():
                curr_log = cable.run_cable(
                    input_SD.dataset, output_path,
                    curr_record)
                
            # 1B) input_SD doesn't have data (It was symbolically-reused)
            # --> We backtrack to this point, then run the cable
            else:
                successful_recovery = self.recover(input_SD)

                if not successful_recovery:
                    # We return the incomplete curr_record (missing an
                    # ExecLog).
                    return curr_record

                curr_log = cable.run_cable(self.sd_fs_map[input_SD],
                                           output_path, curr_record)

        # 2) Input contents are on the file system due, so whether
        # or not input_SD has data (IE, was transient), we can use it
        # --> Use the existing data on the filesystem for computation
        # Pre: file system copy must match the database version if it exists
        else:
            if os.access(self.sd_fs_map[input_SD]), os.R_OK):
                curr_log = cable.run_cable(
                    self.sd_fs_map[input_SD],
                    output_path,
                    curr_record)
        
        # FINISHED RUNNING CABLE
        ####


        ####
        # CREATE EXECRECORD

        # Since we attempted to run code, regardless of the outcome,
        # we create an ExecRecord.  Then we will fill in details on
        # anything that went wrong.  For now, fill in the MD5_checksum
        # and num_rows with default values of "" and -1.
        had_ER_at_beginning = curr_ER != None
        
        if not recover and curr_ER == None:
            # No ER was found; create a new one.

            # Create a new ER, generated by the above ExecLog.
            curr_ER = cable.execrecords.create(
                generator=curr_log)
            curr_ER.execrecordins.create(
                generic_input=cable.source,
                symbolicdataset=input_SD)

            if cable.is_trivial():
                output_SD = input_SD
            else:
                output_SD = SymbolicDataset(MD5_checksum="")
                output_SD.save()

                # Add this structure to the symbolic dataset.
                if output_SD_CDT != None:
                    output_SD.structure.create(compounddatatype=output_SD_CDT,
                                               num_rows=-1)

            ero_xput = None
            if type(cable) == PipelineStepInputCable:
                ero_xput = cable.dest
            else:
                ero_xput = cable.pipeline.outputs.get(
                    dataset_name=cable.output_name)
            
            curr_ER.execrecordouts.create(
                generic_output=ero_xput,
                symbolicdataset=output_SD)

            curr_ER.complete_clean()

        # FINISHED CREATING EXECRECORD
        ####

        ####
        # CHECK OUTPUT

        # FIXME Probably this will involve some transactions.

        ####
        # CHECK IF FILE EXISTS

        # At this point we know output_SD points to the output of this
        # cable.
        
        if not os.access(output_path, os.R_OK):
            # Create a ContentCheckLog denoting this as missing;
            # we leave num_rows = -1 and MD5_checksum = "".
            ccl = output_SD.content_checks.create(execlog=curr_log)
            ccl.baddata.create(missing_output=True)
            
        else:
            # Extract the MD5.
            output_md5 = None
            with open(output_path, "rb") as f:
                output_md5 = file_access_utils.compute_md5(f)

            if not had_ER_at_beginning:
                output_SD.MD5_checksum = output_md5
            
            # First, the non-recovery case.
            if not recover:

                ####
                # REGISTER REAL DATA (if applicable)
        
                # If we are retaining this data, we create a dataset
                if cable_keeps_output:
                    new_dataset = Dataset(
                        user=user,
                        name="{} {} {}".format(self.run.name,
                                               type(cable).__name__,
                                               curr_record.pk),
                        symbolicdataset=output_SD,
                        created_by=cable)
                    with open(output_path, "rb") as f:
                        new_dataset.dataset_file = File(f)
                    new_dataset.save()
        
                # FINISHED REGISTERING REAL DATA
                ####

                if not had_ER_at_beginning:
                    ####
                    # PERFORM CONTENT CHECK ON FIRST TIME OF CREATION
                    
                    # A path to perform the CSV check if necessary.
                    summary_path = "{}_summary".format(output_path)

                    cable_min_row = None
                    cable_max_row = None
                    # Set these if this cable is not raw.
                    if not cable.is_raw():
                        if type(cable) == pipeline.models.PipelineStepInputCable:
                            cable_min_row = cable.dest.get_min_row()
                            cable_max_row = cable.dest.get_max_row()
                        else:
                            cable_min_row = cable.source.get_min_row()
                            cable_max_row = cable.source.get_max_row()

                    ccl = output_SD.check_file_contents(
                        output_path, summary_path, cable_min_row,
                        cable_max_row, curr_log)
                                    
                    # FINISHED CONTENT CHECK ON FIRST TIME OF CREATION
                    ####
                                    
            # Next, the case where either we are recovering or the ER
            # already existed: we perform an integrity check.
            elif recover or had_ER_at_beginning:
                ####
                # PERFORM INTEGRITY CHECK

                icl = output_SD.check_integrity(output_path, curr_log,
                                                output_md5)
                
                # FINISHED INTEGRITY CHECK
                ####
                            
        # FINISHED CHECKING OUTPUT
        ####
            
        ####
        # PERFORM BOOKKEEPING
        
        curr_record.execrecord = curr_ER
        curr_record.complete_clean()
        curr_record.save()


        # Update maps as in the reused == True case (see above) if we
        # are not recovering.
        if not recover:
            if (cable_out_SD not in self.sd_fs_map or
                    self.sd_fs_map[cable_out_SD] == None:
                self.sd_fs_map[cable_out_SD] = output_path
            
            socket_map[(cable, cable.generic_output)] = cable_out_SD
            
            self.cable_map[cable] = curr_record

        # FINISHED BOOKKEEPING
        ####
        return curr_record

    def execute_step(self, pipelinestep, inputs, step_run_dir=None,
                     recover=False):
        """
        Execute the specified PipelineStep with the given inputs.

        If code is actually run, the outputs go to the paths
        specified in output_paths.  The requisite code is placed
        in step_run_dir; if step_run_dir is None, then the default
        is [sandbox path]/step[stepnum].

        If recover == True, then we perform this in recovery mode,
        where we don't create a new RS or ER, but we fill in an old RS
        with a new ExecLog and we confirm the output in the ER.  In
        this case, the parameter value of step_run_dir is ignored and
        retrieved using the maps.

        Outputs get written to
        [step run dir]/output_data/step[step number]_[output name]

        Inputs get written to 
        [step run dir]/input_data/step[step number]_[input name]
        (Note that this may simply be a link to data that was already
        in the sandbox elsewhere.)

        Logs get written to
        [step run dir]/logs/step[step number]_std(out|err).txt
        """
        # Some preamble.
        curr_RS = None
        curr_ER = None
        output_paths = []
        inputs_after_cable = []
        in_dir = ""
        out_dir = ""
        log_dir = ""
        had_ER_at_beginning = False

        ####
        # SET UP RUNSTEP, OUTPUT PATHS, INPUTS, ER....

        ####
        # NON-RECOVERY CASE
        if not recover:
            curr_RS = pipelinestep.pipelinestep_instances.create()
            step_run_dir = step_run_dir or os.path.join(
                self.sandbox_path, "step{}".format(pipelinestep.step_num))

            # Set up run directory.
            file_access_utils.set_up_directory(step_run_dir)
            # Set up inputs, outputs, and logs directories.
            in_dir = os.path.join(step_run_dir, "input_data")
            file_access_utils.set_up_directory(in_dir)
            out_dir = os.path.join(step_run_dir, "output_data")
            file_access_utils.set_up_directory(out_dir)
            log_dir = os.path.join(step_run_dir, "logs")
            file_access_utils.set_up_directory(log_dir)

            # Set up output paths.
            for (curr_output in pipelinestep.transformation.outputs.all().
                 order_by("dataset_idx")):
                file_suffix = "raw" if curr_output.is_raw() else "csv"
                    
                output_paths.append(os.path.join(
                    out_dir, "step{}_{}.{}".format(
                        pipelinestep.step_num, curr_output.dataset_name,
                        file_suffix)))

    
            # Run all PSICs.  This list stores the SDs that come out of the
            # cables (and get fed directly into the transformation).
            for (curr_input in pipelinestep.transformation.inputs.all().
                 order_by("dataset_idx")):
                corresp_cable = pipelinestep.cables_in.get(
                    transf_input=curr_input)
                
                curr_RSIC = self.execute_cable(
                    corresp_cable, inputs[curr_input.dataset_idx-1],
                    os.path.join(
                        in_dir,
                        "step{}_{}".format(
                            pipelinestep.step_num,
                            curr_input.dataset_name)),
                    curr_RS)
    
                inputs_after_cable.append(
                    curr_RSIC.execrecord.execrecordouts.
                    all()[0].symbolicdataset)
                
            # Sanity check
            curr_RS.clean()
    
            # Look for an ER that we can reuse.  It must represent the same
            # transformation, and take the same input SDs.
            curr_ER = pipelinestep.transformation.find_compatible_ER(
                inputs_after_cable)
            
            # If it found an ER, check that the ER provides all of the
            # output that we need.
            if curr_ER != None:
                had_ER_at_beginning = True
                outputs_needed = pipelinestep.outputs_to_retain()
                if curr_ER.provides_outputs(outputs_needed):
                    ####
                    # REUSE AN ER
                    
                    # The ER found has what we need, so we can reuse it.
                    curr_RS.reused = True
                    curr_RS.execrecord = curr_ER
                    curr_RS.complete_clean()
                    curr_RS.save()
    
                    # Update the maps.
    
                    # Since this is the reused = True case, step_run_dir
                    # represents where the code *should be* -- later you
                    # might actually fill it in.
                    self.ps_map[pipelinestep] = (step_run_dir, curr_RS)
    
                    # Add every output of this transformation to
                    # sd_fs_map.
                    for step_output in pipelinestep.transformation.outputs.all():
                        corresp_ero = curr_ER.execrecordouts.get(
                            content_type=ContentType.objects.get_for_model(
                                type(step_output)),
                            object_id=step_output.pk)
    
                        corresp_SD = corresp_ero.symbolicdataset
    
                        # Compensate for 0-basedness.
                        corresp_path = output_paths[step_output.dataset_idx-1]
    
                        # Update sd_fs_map and socket_map.
                        if corresp_SD not in self.sd_fs_map:
                            # If this is the first time this file would have
                            # appeared on the filesystem, this is where it
                            # *should* go -- later it might actually be filled
                            # in here during a "recover" operation.
                            self.sd_fs_map[corresp_SD] = corresp_path
    
                        self.socket_map[(pipelinestep, step_output)] = corresp_SD
    
                    return curr_RS
                    
                    # FINISHED REUSING AN ER
                    ####
                
        ####
        # RECOVERY CASE
        else:
            step_run_dir, curr_RS = self.ps_map(pipelinestep)

            # We will use these:
            in_dir = os.path.join(step_run_dir, "input_data")
            out_dir = os.path.join(step_run_dir, "output_data")
            log_dir = os.path.join(step_run_dir, "logs")

            for (curr_output in pipelinestep.transformation.outputs.all().
                 order_by("dataset_idx")):
                # Get the SymbolicDataset that comes from this output
                # using socket_map; then use sd_fs_map to get its
                # path.
                corresp_SD = self.socket_map(pipelinestep, curr_output)
                output_paths.append(self.sd_fs_map[corresp_SD])

            # Retrieve the input SDs from the ER.
            for (curr_input in pipelinestep.transformation.inputs.all().
                 order_by("dataset_idx")):
                corresp_ERI = curr_ER.execrecordins.get(
                    content_type=ContentType.objects.get_for_model(
                        transformation.models.TransformationInput),
                    object_id=curr_input.pk)
                inputs_after_cable.append(corresp_ERI.symbolicdataset)

            curr_ER = curr_RS.execrecord
            had_ER_at_beginning = True

        # FINISHED SETTING UP RUNSTEP, OUTPUT PATHS, INPUTS, ER....
        ####
        
        # Having reached this point, we know we can't reuse an ER.
        # We will have to actually run code.

        ####
        # PUT ALL DATASETS INTO PLACE

        # First, make sure all the input files have been written to
        # the sandbox.  Note that by this point, any inputs that we
        # need should have non-blank PATH entries in sd_fs_map.
        for curr_in_SD in inputs_after_cables:
            curr_path = sd_fs_map[curr_in_SD]['PATH']
            if not os.access(curr_path, "F_OK"):
                successful_recovery = self.recover(curr_in_SD)

                if not successful_recovery:
                    # We return the incomplete curr_record (missing an
                    # ExecLog).
                    return curr_record

        # FINISHED PUTTING ALL DATASETS INTO PLACE
        ####
            

        ####
        # ACTUALLY RUN CODE

        # First, the easy case when this step is a sub-Pipeline.  Note
        # that this case never occurs when we are recovering.
        if type(pipelinestep.transformation) == pipeline.models.Pipeline:
            ####
            # RUN PIPELINE
            
            child_run = self.execute_pipeline(
                pipeline=pipelinestep.transformation,
                inputs=inputs_after_cables,
                parent_runstep=curr_RS)

            # This is implicit from the above.
            # curr_RS.child_run = child_run

            return curr_RS

            # FINISHED RUNNING PIPELINE
            ####

        # From this point on we know that this step was a Method.
        
        ####
        # RUN CODE: METHOD
        #
        # Log paths.
            
        stdout_path = os.path.join(log_dir, "step{}_stdout.txt".
                                   format(pipelinestep.step_num))
        stderr_path = os.path.join(log_dir, "step{}_stderr.txt".
                                   format(pipelinestep.step_num))

        method_popen = None

        # Create an ExecLog; this sets its start_time.
        curr_log = archive.models.ExecLog(record=curr_RS)
        
        with (open(stdout_path, "wb", 1), 
              open(stderr_path, "wb", 0) as (outwrite, errwrite):
            method_popen = pipelinestep.transformation.run_code(
                step_run_dir,
                [sd_fs_map[x]['PATH'] for x in inputs_after_cables],
                output_paths, outwrite, errwrite)

            # While it's running, print the captured stdout and
            # stderr to the console.
            with (open(stdout_path, "rb", 1), 
                  open(stderr_path, "rb", 0)) as (outread, errread):
                while method_open.poll() != None:
                    sys.stdout.write(outread.read())
                    sys.stderr.write(errread.read())
                    time.sleep(1)
                
                # One last write....
                outwrite.flush()
                errwrite.flush()
                sys.stdout.write(outread.read())
                sys.stderr.write(errread.read())

        # The method has finished running.  Make sure all output
        # has been flushed.
        sys.stdout.flush()
        sys.stderr.flush()

        # Mark the end time in the ExecLog and save; then
        # create a MethodOutput object with all of the output.
        curr_log.end_time = datetime.now()
        curr_log.clean()
        curr_log.save()

        curr_mo = MethodOutput(
            execlog=curr_log,
            return_code = method_popen.returncode)
        with (open(stdout_path, "rb"), 
              open(stderr_path, "rb")) as (outread, errread):
            curr_mo.output_log.save(stdout_path, File(outread))
            curr_mo.error_log.save(stderr_path, File(errread))
        curr_mo.clean()
        curr_mo.save()

        # Sanity check.
        curr_log.complete_clean()

        # FINISHED RUNNING METHOD
        ####

        
        ####
        # CREATE EXECRECORD IF NECESSARY
        
        # Create a fresh ER if none was found.
        if curr_ER == None:
            curr_ER = pipelinestep.transformation.execrecords.create()

            for curr_input in pipelinestep.transformation.inputs.all():
                corresp_input_SD = inputs_after_cable[curr_input.dataset_idx-1]
                curr_ER.execrecordins.create(
                    generic_input=curr_input,
                    symbolicdataset=corresp_input_SD)

            for curr_output in pipelinestep.transformation.outputs.all():
                # Make new outputs with blank MD5s and num_rows = -1
                # for now (we'll fill them in later).
                corresp_output_SD = SymbolicDataset(MD5_checksum="")
                corresp_output_SD.save()

                # If the output was not raw, create a structure as well.
                if not curr_output.is_raw():
                    corresp_output_SD.structure.create(
                        compounddatatype=curr_output.get_cdt(),
                        num_rows=-1)

                corresp_output_SD.clean()

                curr_ER.execrecordouts.create(
                    generic_output=curr_output,
                    symbolicdataset=corresp_output_SD)

            # Sanity check
            curr_ER.complete_clean()

        # FINISHED CREATING ER
        ####

        # From here on, curr_ER is appropriately set.

        ####
        # CHECK OUTPUTS

        # Flag that indicates whether we have detected any problems
        # with the output.  If so, we then exit without checking the
        # rest of the data.
        bad_output_found = False
        for curr_output in pipelinestep.transformation.outputs.all():
            output_path = output_paths[curr_output.dataset_idx-1]
            output_ERO = curr_ER.execrecordouts.get(
                content_type=ContentType.objects.get_for_model(
                    transformation.models.TransformationOutput),
                object_id=curr_output.pk)
            output_SD = output_ERO.symbolicdataset
        
            # Check that the file exists, as we did for cables.
            if not os.access(output_path, os.R_OK):
                ccl = output_SD.content_checks.create(execlog=curr_log)
                ccl.baddata.create(missing_output=True)

                bad_output_found = True
                continue

            # Compute the MD5 checksum of this file.
            output_md5 = ""
            with open(output_path, "rb") as f:
                output_md5 = file_access_utils.compute_MD5(f)

            # If this is the first time we've ever seen this file,
            # save its MD5 checksum.
            if not had_ER_at_beginning:
                output_SD.MD5_checksum = curr_MD5
            
            # Create a Dataset for this file if we are retaining this
            # output and if the corresponding ERO didn't already have
            # a Dataset associated.  Note: this would never happen
            # when in recovery mode.
            if (not pipelinestep.outputs_to_delete.filter(
                    pk=curr_output.pk).exists() and 
                    not output_ERO.has_data()):
                desc = "run: {}\nuser: {}\nstep: {}\nmethod: {}\noutput: {}"
                desc = desc.format(
                    self.run.name, self.user, pipelinestep.step_num,
                    pipelinestep.transformation, curr_output.dataset_name)
                new_DS = Dataset(
                    user=self.user,
                    name=("run:{}__user:{}__step:{}__method:{}__output:{}".
                          format(self.run.name, pipelinestep.step_num,
                                 pipelinestep.transformation,
                                 curr_output.dataset_name)),
                    description=desc,
                    symbolicdataset=output_SD,
                    created_by=curr_RS)

                # Recall that dataset_idx is 1-based, and
                # output_paths is 0-based.
                with open(output_path, "rb") as f:
                    new_DS.dataset_file.save(os.path.basename(output_path),
                                             File(f))
                new_DS.clean()
                new_DS.save()

            # Don't bother with any more checks if we've already found
            # bad data (i.e. we've already bailed out and are just
            # tidying up).
            if bad_output_found:
                continue

            ####
            # CONTENT CHECK OF NEW FILE

            # Note that if we are in recovery mode, we wouldn't do
            # this, and had_ER_at_beginning is True.
            if not had_ER_at_beginning:
                # A place to validate this output.
                summary_path = "{}_summary".format(output_path)

                # Note that get_min_row() and get_max_row() return
                # None if the output is raw.
                ccl = output_SD.check_file_contents(
                    output_path, summary_path, curr_output.get_min_row(),
                    curr_output.get_max_row())

                if ccl.is_fail():
                    bad_output_found = True

            # FINISHED CONTENT CHECK OF NEW CSV
            ####

            ####
            # INTEGRITY CHECK OF RECREATED DATA
            
            # Second case: this file already had an existing SD
            # representing it.  In this case, check its integrity.
            elif had_ER_at_beginning:
                icl = output_SD.check_integrity(output_path, curr_log,
                                                output_md5)

                if icl.is_fail():
                    bad_output_found = True
                    
            # FINISHED INTEGRITY CHECK
            ####

        # FINISHED CHECKING OUTPUTS
        ####
                
        # Make sure the ER is clean and complete.
        curr_ER.complete_clean()

        ####
        # FINISH BOOKKEEPING (NON-RECOVERY CASE)
        if not recover:
            curr_RS.execrecord = curr_ER
            
            # Finish curr_RS.
            curr_RS.complete_clean()
            curr_RS.save()
            
            # Update the maps as we did in the reused case.
            # Since this is the reused=False case, step_run_dir
            # represents where the step *actually is*.
            self.ps_map[pipelinestep] = (step_run_dir, curr_RS)
    
            for step_output in pipelinestep.transformation.outputs.all():
                corresp_ero = curr_ER.execrecordouts.get(
                    content_type=ContentType.objects.get_for_model(
                        type(step_output)),
                    object_id=step_output.pk)
    
                corresp_SD = corresp_ero.symbolicdataset
    
                # Compensate for 0-basedness.
                corresp_path = output_paths[step_output.dataset_idx-1]
    
                
                # Update sd_fs_map and socket_map as in the reused =
                # True case.
                # If this is the first time the data has ever been
                # written to the filesystem, then this represents
                # where the data *actually is*.
                if corresp_SD not in self.sd_fs_map:
                    self.sd_fs_map[corresp_SD] = corresp_path
                self.socket_map[(pipelinestep, step_output)] = corresp_SD

        # FINISHED BOOKKEEPING (NON-RECOVERY CASE)
        ####
        
        return curr_RS

    def execute_pipeline(self, pipeline=None, input_SDs=None, 
                         sandbox_path=None, parent_runstep=None):
        """
        Execute the specified Pipeline with the given inputs.

        If any of pipeline, input_SDs, sandbox_path, parent_runstep 
        are None, then *all* of them have to be None; in this case,
        we are running the Sandbox's top-level Pipeline with the
        top-level inputs in the Sandbox's specified path.

        Outputs get written to
        [sandbox_path]/output_data/run[run PK]_[output name].(csv|raw)

        At the end of this function, the outputs of the pipeline
        will be added to sd_fs_map, so you can determine where
        to find your output.
        """
        # Check whether all of the inputs are none or not.
        is_set = (
            pipeline != None, input_SDs != None, sandbox_path != None,
            parent_runstep != None
        )
        if any(is_set) and not all(is_set):
            raise ValueError(
                "Either none or all of the parameters must be None")
        
        # Initialize the defaults: this is adapted from:
        # http://stackoverflow.com/questions/8131942/python-how-to-pass-default-argument-to-instance-method-with-an-instance-variab
        pipeline = pipeline or self.pipeline
        input_SDs = input_SDs or self.inputs
        sandbox_path = sandbox_path or self.sandbox_path

        curr_run = self.run
        if parent_runstep != None:
            curr_run = pipeline.pipeline_instances.create(
                user=self.user, parent_runstep=parent_runstep)

        ####
        # SET UP SANDBOX AND PATHS

        # Set up an output directory (or make sure it's usable).
        out_dir = os.path.join(sandbox_path, "output_data")
        file_access_utils.set_up_directory(out_dir)

        # FINISHED SETTING UP SANDBOX AND PATHS
        ####
        
        ####
        # RUN STEPS

        for step in pipeline.steps.all().order_by("step_num"):
            # Look at the cables for this step and identify 
            # what inputs we need.

            step_inputs = []
            for cable in step.cables_in.all().order_by(dest__dataset_idx):
                # Find the SD that feeds this cable.  First, identify
                # the generating step.  If it was a Pipeline input,
                # leave generator == None.
                generator = None
                if cable.source_step != 0:
                    generator = pipeline.steps.get(
                        step_num=cable.source_step)
                
                # Look up the symDS that is associated with this socket
                # (The generator PS must already have been executed)
                step_inputs.append(socket_map[(generator, cable.source)])
        
            curr_RS = self.execute_step(
                step, step_inputs,
                step_run_dir=os.path.join(sandbox_path,
                                          "step{}".format(step.step_num)))

            # If this RS returns without completing or if the step
            # failed (i.e. the Method returned with an error code or
            # any of the outputs didn't check out), we bail.
            if not curr_RS.is_complete() or not curr_RS.successful_execution():
                curr_run.clean()
                return curr_run

        # FINISH RUNNING STEPS
        ####

        ####
        # RUN OUTPUT CABLES

        for outcable in pipeline.outcables.all():
            # Identify the SD that feeds this outcable.
            generator = pipeline.steps.get(
                step_num=outcable.source_step)

            source_SD = socket_map[(generator, outcable.source)]

            file_suffix = "raw" if outcable.is_raw() else "csv"

            output_path = os.path.join(
                out_dir,
                "run{}_{}.{}".format(curr_run.pk, outcable.output_name,
                                     file_suffix))

            curr_ROC = self.execute_cable(outcable, source_SD,
                                          output_path, curr_run)

            # As above, bail if this returned without completing or
            # failed.
            if not curr_ROC.is_complete() or not curr_RS.successful_execution():
                curr_run.clean()
                return curr_run

        # FINISH RUNNING OUTPUT CABLES
        ####
        curr_run.complete_clean()
        curr_run.save()
        
        # FINISH LAST BIT OF BOOKKEEPING
        ####

        return curr_run

    def recover(self, SD_to_recover):
        """
        Fills in SD_to_recover onto the file system.

        Returns True if it succeeds; False otherwise.

        PRE: SD_to_recover is in the maps but no corresopnding file is
        on the file system.
        """
        # Base case: there is an appropriate Dataset in the database.
        # Simply write it to the correct location.
        if SD_to_recover.has_data():
            # Read/write binary files in chunks of 8 megabytes
            chunk_size = 1024*8
            location = self.sd_fs_map[SD_to_recover]
            saved_data = SD_to_recover.dataset
            try:
                saved_data.dataset_file.open()
                with open(location,"wb") as outfile:
                    chunk = saved_data.dataset_file.read(chunk_size)
                    while chunk != "":
                        outfile.write(chunk)
                        chunk = saved_data.dataset_file.read(chunk_size)
            except:
                return False
            finally:
                saved_data.dataset_file.close()
            return True

        # Recursive case: look up how to generate SD_to_recover,
        # and then do that.
        generator = None
        socket = None
        for generator, socket in socket_map:
            if socket_map[(generator, socket)] == SD_to_recover:
                break

        curr_record = None
        if type(generator) == pipeline.models.PipelineStep:
            curr_record = self.execute_step(generator, None, recover=True)

        else:
            curr_record = self.execute_cable(
                generator, None, None, None, recover=True)

        return curr_record.is_complete() and curr_record.successful_execution()
        
