"""Code that is responsible for the execution of Pipelines."""

# Import our Shipyard models module.
import models
import file_access_utils
import os.path
import sys
import time

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
    
    # ps_map maps pipelinestep to (path, ER of method): the path tells you
    # the directory that the PS would have been run in (whether or not
    # it was): the ER tells you what inputs are needed (Which in turn
    # will lead back to an sd_fs_map lookup)
        
    # cable_map maps cables to ER
    
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
        self.method_map = {}

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

    def execute_cable(self, cable, input_SD, output_path, parent_record):
        """
        Execute the specified PSIC/POC on the given input.

         - input_SD is the SymbolicDataset fed into this cable.
         - output_path is where the output file should be written.
         - parent_record is the record containing this cable: a RunStep
           if this cable is a PSIC; a Run if this cable is a POC.

        Returns an RSIC/ROC that describes this cable's running.
        If real data was provided, then the re-multiplexed
        real data has been written to output_path.  Also,
        sd_fs_map and cable_map will have been updated.

        PRE: whether or not input_SD has real data associated,
        it has an appropriate CDT for feeding this cable.
        PRE: if input_SD has data, and input_path refers to a real file,
        they are the same.
        PRE: input_SD is in sd_fs_map
        PRE: more generally, all the _maps are "up to date" for this step
        """
        # Create a record for this.
        curr_record = None
        if type(cable) == PipelineStepInputCable:
            curr_record = cable.psic_instances.create()
        else:
            curr_record = cable.poc_instances.create()
        curr_ER = None

        ####
        # LOOK FOR REUSABLE ER
        
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
                            self.sd_fs_map[cable_out_SD] == None:
                        self.sd_fs_map[cable_out_SD] = output_path

                    # Add (cable, destination socket) to socket_map.
                    socket_map[(cable, cable.generic_output)] = cable_out_SD
                    
                    # Add this cable to cable_map.
                    self.cable_map[cable] = candidate_ERI.execrecord
                    return curr_record
                    
                # Otherwise (i.e. you are keeping output but the ERO
                # doesn't have any), we proceed, filling in this ER.
                else:
                    curr_ER = candidate_ERI.execrecord
                    break
                
        # FINISHED LOOKING FOR REUSABLE ER
        ####

        ####
        # RUN CABLE
        
        # At this point, we know we cannot reuse an ER, so we
        # will have to run the cable.

        # What comes out of the cable will have the following CDT:
        output_SD_CDT = None
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
            
            # Look at each wire, take the DT from source_pin, assign the name and index of dest_pin
            for wire in wires:
                output_SD_CDT.members.create(
                    datatype=wire.source_pin.datatype,
                    column_name=wire.dest_pin.column_name,
                    column_idx=wire.dest_pin.column_idx)
                
            output_SD_CDT.clean()
            
        # There are four cases:
        
        # 1) input_SD has real data and does not contain written data on the filesystem
        # --> The data was uploaded OR derived from a previous reused step
        # --> We will use input_SD.dataset for the computation
        if (input_SD.has_data() and
                not os.access(self.sd_fs_map[input_SD], os.R_OK)):
            cable.run_cable(input_SD.dataset, output_path)
        
        # 2) input_SD has real data and there is data on the filesystem:
        # --> The data was calculated from a previous step
        # --> We use the data on the filesystem
        #     (PRE: It must be equal to input_SD.dataset)
        
        # 3) input_SD does not have real data but there is data on the
        # filesystem: The data was calculated but is transient (time
        # bomb) --> We use the data on the filesystem

        elif os.access(self.sd_fs_map[input_SD]), os.R_OK):
            cable.run_cable(self.sd_fs_map[input_SD], output_path)

        # 4) input_SD does not have real data and is not on the filesystem
        # (And there's nothing to reuse)
        # --> We have to backtrack.
        else:
            # Backtrack to 'fill in' our maps appropriately.
            self.recover(input_SD)

            # And now we have what we need to run this cable
            cable.run_cable(self.sd_fs_map[input_SD], output_path)
            
        # FINISHED RUNNING CABLE
        ####

        ####
        # CHECK OUTPUT

        # Make an ER to represent the execution above.
        output_SD = None
        output_md5 = None
        
        with open(output_path, "rb") as f:
            output_md5 = file_access_utils.compute_md5(f)

        output_summary = None
        if not cable.is_raw():
            # A run path for summarize_CSV.
            val_dir = "{}_validation".format(output_path)
            with open(output_path, "rb") as f:
                output_summary = file_access_utils.summarize_CSV(
                    f, output_SD_CDT, val_dir)
            
            if output_summary.has_key("bad_num_cols"):
                raise ValueError(
                    "Output of cable \"{}\" had the wrong number of columns".
                    format(cable))

            if output_summary.has_key("bad_col_indices"):
                raise ValueError(
                    "Output of cable \"{}\" had a malformed header".
                    format(cable))

            if output_summary.has_key("failing_cells"):
                raise ValueError(
                    "Output of cable \"{}\" had malformed entries".
                    format(cable))
            
        if curr_ER == None:
            # No ER was found; create a new one.
            
            curr_ER = cable.execrecords.create()
            curr_ER.execrecordins.create(
                generic_input=cable.provider_output,
                symbolicdataset=input_SD)

            if cable.is_trivial():
                output_SD = input_SD

                # Since this cable was trivial, either the resulting
                # file sitting at output_path is simply linked to
                # something else that's already on the filesystem, or
                # it was copied from the database.
                if output_md5 != output_SD.MD5_checksum:
                    raise ValueError(
                        "Output of cable \"{}\" failed MD5 integrity check".
                        format(cable))
                
            else:
                output_SD = SymbolicDataset(
                    MD5_checksum=output_md5)
                output_SD.save()

                # Add this structure to the symbolic dataset
                if output_SD_CDT != None:
                    output_SD.structure.create(
                        compounddatatype=output_SD_CDT,
                        num_rows=output_summary["num_rows"])

                ero_xput = None
                if type(cable) == PipelineStepInputCable:
                    ero_xput = cable.transf_input
                else:
                    ero_xput = cable.pipeline.outputs.get(
                        dataset_name=cable.output_name)
            
            curr_ER.execrecordouts.create(
                generic_output=ero_xput,
                symbolicdataset=output_SD)
            
        else:
            # In this case, we did find an ER, so we can check the MD5
            # checksum against the stored value.
            output_SD = curr_ER.execrecordouts.all()[0].symbolicdataset
            if output_md5 != output_SD.MD5_checksum:
                raise ValueError(
                    "Output of cable \"{}\" failed MD5 integrity check".
                    format(cable))

        # FINISHED CHECKING OUTPUT
        ####

        ####
        # PERFORM BOOKKEEPING

        # Update maps as in the reused == True case (see above).
        if (cable_out_SD not in self.sd_fs_map or
                self.sd_fs_map[cable_out_SD] == None:
            self.sd_fs_map[cable_out_SD] = output_path

        socket_map[(cable, cable.generic_output)] = cable_out_SD
        
        self.cable_map[cable] = curr_ER
        
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
            new_dataset.clean()
            new_dataset.save()

        # Complete the ER and record, then return the record.
        curr_ER.complete_clean()

        # FINISHED BOOKKEEPING
        ####
        
        curr_record.execrecord = curr_ER
        curr_record.complete_clean()
        curr_record.save()
        return curr_record

    def execute_step(self, pipelinestep, inputs, step_run_dir=None):
        """
        Execute the specified PipelineStep with the given inputs.

        If code is actually run, the outputs go to the paths
        specified in output_paths.  The requisite code is placed
        in step_run_dir; if step_run_dir is None, then the default
        is [sandbox path]/step[stepnum].

        Outputs get written to
        [step run dir]/output_data/step[step number]_[output name]

        Inputs get written to 
        [step run dir]/input_data/step[step number]_[input name]
        (Note that this may simply be a link to data that was already
        in the sandbox elsewhere.)

        Logs get written to
        [step run dir]/logs/step[step number]_std(out|err).txt
        """
        curr_RS = pipelinestep.pipelinestep_instances.create()

        ####
        # SET UP DIRECTORY FOR RUNNING THIS STEP AND PATHS
        step_run_dir = step_run_dir or os.path.join(
            self.sandbox_path, "step{}".format(pipelinestep.step_num))

        file_access_utils.set_up_directory(step_run_dir)
        # Set up inputs, outputs, and logs directories.
        in_dir = os.path.join(step_run_dir, "input_data")
        out_dir = os.path.join(step_run_dir, "output_data")
        file_access_utils.set_up_directory(in_dir)
        file_access_utils.set_up_directory(out_dir)

        output_paths = []
        for (curr_output in pipelinestep.transformation.outputs.all().
             order_by("dataset_idx")):
            file_suffix = "raw" if curr_output.is_raw() else "csv"
                
            output_paths.append(os.path.join(
                out_dir, "step{}_{}.{}".format(
                    pipelinestep.step_num, curr_output.dataset_name,
                    file_suffix)))

        # FINISHED SETTING UP DIRECTORY AND PATHS
        ####

        ####
        # RUN CABLES

        # Run all PSICs.  This list stores the SDs that come out of the
        # cables (and get fed directly into the transformation).
        inputs_after_cable = []
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

        # FINISHED RUNNING CABLES
        ####

        ####
        # CHECK WHETHER WE CAN REUSE AN ER FOR A METHOD

        # Look for an ER that we can reuse.  It must represent the same
        # transformation, and take the same input SDs.
        curr_ER = find_compatible_ER(pipelinestep.transformation,
                                     inputs_after_cable)
        
        # If it found an ER, check that the ER provides all of the
        # output that we need.
        if curr_ER != None:
            outputs_needed = ps_outputs_to_retain(pipelinestep)
            if transf_ER_provides_outputs(curr_ER, outputs_needed):
                ####
                # REUSE AN ER
                
                # The ER found has what we need, so we can reuse it.
                curr_RS.reused = True
                curr_RS.execrecord = curr_ER

                # Update the maps.

                # Since this is the reused = True case, step_run_dir
                # represents where the code *should be* -- later you
                # might actually fill it in.
                self.ps_map[pipelinestep] = (step_run_dir, curr_ER)

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
                
                curr_RS.complete_clean()
                curr_RS.save()

                return curr_RS
                
                # FINISHED REUSING AN ER
                ####

        # FINISHED LOOKING FOR REUSABLE ER
        ####

        ####
        # ACTUALLY RUN CODE
        
        # Having reached this point, we know we can't reuse an ER.
        # We will have to actually run code.

        # First, make sure all the input files have been written to
        # the sandbox.  Note that by this point, any inputs that we
        # need should have non-blank PATH entries in sd_fs_map.
        for curr_in_SD in inputs_after_cables:
            curr_path = sd_fs_map[curr_in_SD]['PATH']
            if not os.access(curr_path, "F_OK"):
                self.recover(curr_in_SD)

        # If it's a method, run the code; if not, call execute on the
        # pipeline.
        if type(pipelinestep.transformation) == Method:
            ####
            # RUN CODE: METHOD
            #
            # We need to then register the output paths with the
            # appropriate SDs, creating Datasets as necessary.

            # Set up a log directory.
            log_dir = os.path.join(step_run_dir, "logs")
            file_access_utils.set_up_directory(log_dir)
            
            stdout_path = os.path.join(log_dir, "step{}_stdout.txt".
                                       format(pipelinestep.step_num))
            stderr_path = os.path.join(log_dir, "step{}_stderr.txt".
                                       format(pipelinestep.step_num))

            method_popen = None
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

            # If the return code was not 0, we bail.
            if method_popen.returncode != 0:
                raise ValueError(
                    "Step {} of run {} returned with exit code {}".
                    format(pipelinestep.step_num,
                           self.run, method_popen.returncode))

            # FINISHED RUNNING METHOD
            ####

            ####
            # CHECK OUTPUTS
            
            # Now, we need to confirm that all of the outputs are present.
            # If they are all present, then:
            # 1) if they can be confirmed against past data, do it
            # 2) check the contents of the CSV.

            # This is keyed by the position index of the generating
            # output and stores the computed MD5s of the corresponding
            # output files.
            output_MD5s = {}
            output_nums_rows = {}
            for i, output_path in enumerate(output_paths):
                # i is 0-based; dataset_idx is 1-based.
                output_idx = i + 1
                corresp_output = pipelinestep.transformation.outputs.get(
                    dataset_idx=output_idx)
                
                if not os.access(output_path, "F_OK"):
                    raise ValueError(
                        "Step {} of run {} did not create output file {}".
                        format(pipelinestep.step_num, self.run,
                               output_path))

                # Compute the MD5 checksum.
                with open(output_path, "rb") as f:
                    output_MD5s[output_idx] = file_access_utils.compute_MD5(f)

                if not corresp_output.is_raw():
                    output_summary = None
                    # A place to validate this output.
                    val_dir = "{}_validation".format(output_path)
                    
                    with open(output_path, "rb") as f:
                        output_summary = file_access_utils.summarize_CSV(
                            f, corresp_output.get_cdt(), val_dir)

                    if output_summary.has_key("bad_num_cols"):
                        raise ValueError(
                            "Output of Method \"{}\" had the wrong number of columns".
                            format(pipelinestep.transformation))

                    if output_summary.has_key("bad_col_indices"):
                        raise ValueError(
                            "Output of Method \"{}\" had a malformed header".
                            format(pipelinestep.transformation))

                    if output_summary.has_key("failing_cells"):
                        raise ValueError(
                            "Output of Method \"{}\" had malformed entries".
                            format(pipelinestep.transformation))

                    output_nums_rows[output_idx] = output_summary["num_rows"]

                        
                # If an ER was found but insufficient, there will be
                # SymbolicDatasets representing the outputs; this
                # allows us to check the MD5 checksum.
                if curr_ER != None:
                    corresp_ERO = curr_ER.execrecordouts.get(
                        content_type=ContentType.objects.get_for_model(
                            TransformationOutput),
                        object_id=corresp_output.pk)
                    if (output_MD5s[output_idx] != 
                            corresp_ERO.symbolicdataset.MD5_checksum):
                        raise ValueError(
                            "Output \"{}\" of Method \"{}\" failed integrity check".
                            format(output_path, pipelinestep.transformation))

            # FINISHED CHECKING OUTPUTS
            #### 
            
            ####
            # CREATE EXECRECORD AND REGISTER DATASETS (bookkeeping)
            
            # Create a fresh ER if none was found.
            if curr_ER == None:
                curr_ER = pipelinestep.transformation.execrecords.create()

                for curr_input in pipelinestep.transformation.inputs.all():
                    corresp_input_SD = inputs_after_cable[curr_input.dataset_idx-1]
                    curr_ER.execrecordins.create(
                        generic_input=curr_input,
                        symbolicdataset=corresp_input_SD)

                for curr_output in pipelinestep.transformation.outputs.all():
                    # Make new outputs.
                    corresp_output_SD = SymbolicDataset(
                        MD5_checksum=output_MD5s[curr_output.dataset_idx])
                    corresp_output_SD.save()

                    # If the output was not raw, create a structure as well.
                    if not curr_output.is_raw():
                        corresp_output_SD.structure.create(
                            compounddatatype=curr_output.get_cdt(),
                            num_rows=output_nums_rows[curr_output.dataset_idx])

                    corresp_output_SD.clean()

                    curr_ER.execrecordouts.create(
                        generic_output=curr_output,
                        symbolicdataset=corresp_output_SD)

            # Go through the outputs: if an output is not marked for
            # deletion, *and* there was no data in the corresponding
            # ERO, create a Dataset.
            for curr_output in pipelinestep.transformation.outputs.all():
                corresp_ERO = curr_ER.execrecordouts.get(
                    content_type=ContentType.objects.get_for_model(
                        TransformationOutput),
                    object_id=curr_output.pk)
                if (not pipelinestep.outputs_to_delete.filter(
                        pk=curr_output.pk).exists() and 
                        not corresp_ERO.has_data()):
                    desc = """run: {}
user: {}
step: {}
method: {}
output: {}""".format(self.run.name, self.user, pipelinestep.step_num,
                     pipelinestep.transformation, curr_output.dataset_name)
                    new_DT = Dataset(
                        user=self.user,
                        name="run:{}__step:{}__method:{}__output:{}".format(
                            self.run.name, pipelinestep.step_num,
                            pipelinestep.transformation,
                            curr_output.dataset_name),
                        description=desc,
                        symbolicdataset=corresp_ERO.symbolicdataset,
                        created_by=curr_RS)

                    # Recall that dataset_idx is 1-based, and
                    # output_paths is 0-based.
                    with open(output_paths[curr_output.dataset_idx-1], "rb") as f:
                        new_DT.dataset_file.save(
                            os.path.basename(output_path),
                            File(f))
                    new_DT.clean()
                    new_DT.save()

            # Add the output log and error log with the ER (if it already
            # had logs, replace them).
            if curr_ER.output_log != None:
                curr_ER.output_log.delete()
            with open(stdout_path, "rb") as out:
                curr_ER.output_log.save(os.path.basename(stdout_path),
                                        File(out))
                                        
            if curr_ER.error_log != None:
                curr_ER.error_log.delete()
            with open(stderr_path, "rb") as err:
                curr_ER.error_log.save(os.path.basename(stderr_path),
                                       File(err))

            # Make sure the ER is clean and complete.
            curr_ER.complete_clean()
            curr_RS.execrecord = curr_ER
            
            # Update the maps as we did in the reused case.
            # Since this is the reused=False case, step_run_dir
            # represents where the step *actually is*.
            self.ps_map[pipelinestep] = (step_run_dir, curr_ER)

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

            # FINISHED BOOKKEEPING
            ####
                        
        else:
            ####
            # RUN PIPELINE
            
            # FIXME fill this in when we figure out what to do here.
            child_run = self.execute_pipeline(
                pipeline=pipelinestep.transformation,
                inputs=inputs_after_cables,
                parent_runstep=curr_RS)

            # This is implicit from the above.
            # curr_RS.child_run = child_run

            # FINISHED RUNNING PIPELINE
            ####

        # FINISHED RUNNING CODE
        ####
            
        # Finish curr_RS.
        curr_RS.complete_clean()
        curr_RS.save()
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
        # LOOK FOR ER TO REUSE

        curr_er = find_compatible_ER(pipeline, input_SDs)

        if curr_er != None:
            # An appropriate ER was found.  Does it have all the
            # inputs we need?
            outputs_needed = pipeline.outputs.all()
            if parent_runstep != None:
                outputs_needed = ps_outputs_to_retain(
                    parent_runstep)

            if transf_ER_provides_outputs(curr_er, outputs_needed):
                ####
                # REUSE AN ER

                # The ER found has what we need, so we can reuse it.
                curr_run.reused = True
                curr_run.execrecord = curr_ER

                # Register the outputs with sd_fs_map -- and
                # socket_map if this is not a top-level run.
                for p_out in pipeline.outputs.all():
                    corresp_ero = curr_ER.execrecordouts.get(
                        content_type=ContentType.objects.get_for_model(
                            type(p_out)),
                        object_id=p_out.pk)

                    corresp_SD = corresp_ero.symbolicdataset

                    # Compensate for 0-basedness.
                    file_suffix = "raw" if p_out.is_raw() else "csv"
                    output_path = os.path.join(
                        out_dir,
                        "run{}_{}.{}".format(curr_run.pk, p_out.dataset_name,
                                             file_suffix))


                    # Add it to sd_fs_map if it isn't already in there.
                    if corresp_SD not in self.sd_fs_map:
                        self.sd_fs_map[corresp_SD] = output_path

                    # Update socket_map if necessary.
                    if parent_runstep != None:
                        socket_map[(parent_runstep, p_out)] = corresp_SD

                # Update ps_map if this is not a top-level run.
                if parent_runstep != None:
                    self.ps_map[parent_runstep] = (sandbox_path, curr_ER)

                curr_run.complete_clean()
                curr_run.save()

                return curr_run

                # FINISH REUSING AN ER
                ####

        # FINISHED LOOKING FOR ER TO REUSE
        ####
        
        ####
        # RUN STEPS

        for step in pipeline.steps.all().order_by("step_num"):
            # Look at the cables for this step and identify 
            # what inputs we need.

            step_inputs = []
            for cable in step.cables_in.all().order_by(
                    transf_input__dataset_idx):
                # Find the SD that feeds this cable.  First, identify
                # the generating step.  If it was a Pipeline input,
                # leave generator == None.
                generator = None
                if cable.step_providing_input != 0:
                    generator = pipeline.steps.get(
                        step_num=cable.step_providing_input)
                
                step_inputs.append(
                    socket_map[(generator, cable.provider_output)])
        
            curr_RS = self.execute_step(
                step, step_inputs,
                step_run_dir=os.path.join(
                    sandbox_path,
                    "step{}".format(step.step_num)))

        # FINISH RUNNING STEPS
        ####

        ####
        # RUN OUTPUT CABLES

        for outcable in pipeline.outcables.all():
            # Identify the SD that feeds this outcable.
            generator = pipeline.steps.get(
                step_num=outcable.step_providing_output)

            source_SD = socket_map[(generator, outcable.provider_output)]

            file_suffix = "raw" if outcable.is_raw() else "csv"

            output_path = os.path.join(
                out_dir,
                "run{}_{}.{}".format(curr_run.pk, outcable.output_name,
                                     file_suffix))

            curr_ROC = self.execute_cable(outcable, source_SD,
                                          output_path, curr_run)

        # FINISH RUNNING OUTPUT CABLES
        ####

        ####
        # LAST BIT OF BOOKKEEPING

        # At this point, we either need to create an ER or had an ER
        # that has now been filled in by running the POCs.  If we need
        # to create an ER, we can use socket_map to fill it in.

        if curr_ER == None:
            curr_ER = pipeline.execrecords.create()
            
            for curr_input in pipeline.inputs.all():
                corresp_input_SD = self.socket_map[(None, curr_input)]
                curr_ER.execrecordins.create(
                    generic_input=curr_input,
                    symbolicdataset=corresp_input_SD)

            for outcable in pipeline.outcables.all():
                corresp_output = pipeline.outputs.get(
                    dataset_name=outcable.output_name)
                corresp_output_SD = self.socket_map[(outcable, corresp_output)]
                curr_ER.execrecordouts.create(
                    generic_output=corresp_output,
                    symbolicdataset=corresp_output_SD)

        curr_ER.complete_clean()
        curr_run.execrecord = curr_ER
        curr_run.complete_clean()
        curr_run.save()
        
        # FINISH LAST BIT OF BOOKKEEPING
        ####

        return curr_run

# FIXME these helpers should probably be in models, associated to
# appropriate classes.
# CONTINUE FROM HERE!  Fix this and write recover().
def find_compatible_ER(transformation, input_SDs):
    """
    Helper that finds an ER that we can reuse.

    transformation must be a Method or Pipeline; input_SDs is a list
    of inputs to transformation in the proper order.
    """
    for candidate_ER in transformation.execrecords.all():
        ER_matches = True
        for ERI in candidate_ER.execrecordins.all():
            # Get the input index of this ERI.
            input_idx = ERI.generic_input.dataset_idx
            if ERI.symbolicdataset != input_SDs[input_idx-1]:
                ER_matches = False
                break
                
        # At this point all the ERIs have matched the inputs.  So,
        # we have found our candidate.
        if ER_matches:
            return candidate_ER

    # We didn't find anything.
    return None

def ps_outputs_to_retain(pipelinestep):
    """Returns a list of TOs this PS doesn't delete."""
    outputs_needed = []
    
    for step_output in pipelinestep.transformation.outputs.all():
        if not pipelinestep.outputs_to_delete.filter(
                step_output).exists():
            outputs_needed.append(step_output)
            
    return outputs_needed

def transf_ER_provides_outputs(ER, outputs):
    """
    Determines whether the ER has existent data for these outputs.
    
    outputs is an iterable of TOs that we want the ER to have real
    data for.
    """    
    for curr_output in outputs:
        corresp_ero = ER.execrecordouts.get(generic_output=curr_output)

        if not corresp_ero.has_data():
            return False

    return True
