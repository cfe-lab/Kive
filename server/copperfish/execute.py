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

    # sd_fs_map is a dict mapping symDS to the file system
    #
    # The mapped value is (path, generator|"DATABASE") or
    # ("","DATABASE")
    #
    # (path, generator) tells you what should have
    # generated this data file and where it SHOULD go;
    #
    # For cases where generator="DATABASE", if the path is
    # specified, then data MUST be at that location. If it is not
    # specified, then the data file is available in the database
    # but has not been written to the filesystem yet.

    # method_map maps methods to (path, ER): the path tells you
    # where the code SHOULD go (But may not be due to recycling):
    # the ER tells you what inputs are needed (Which in turn will
    # lead back to an sd_fs_map lookup)
        
    # cable_map maps cables to ER
    
    def __init__(self, user, pipeline, inputs, sandbox_path=None,
                 parent_sandbox=None):
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

        # Set up our maps.
        self.sd_fs_map = {}
        self.cable_map = {}
        self.method_map = {}
        if parent_sandbox != None:
            # Initialize with the values from the parent sandbox.
            self.sd_fs_map = parent_sandbox.sd_fs_map
            self.cable_map = parent_sandbox.cable_map
            self.method_map = parent_sandbox.method_map

        else:
            # This is a top-level sandbox, so we initialize the
            # maps ourselves.
            for input in inputs:
                self.sd_fs_map[input] = {
                    'PATH'=None,
                    'GENERATOR'="DATABASE"
                }

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
                    
                    # Add the ERO's SD to sd_fs_map; add this
                    # cable to cable_map.
                    self.sd_fs_map[cable_out_SD] = {
                        'PATH': output_path,
                        'GENERATOR': cable
                    }
                    self.cable_map[cable] = candidate_ERI.execrecord
                    return curr_record
                    
                # Otherwise (i.e. you are keeping output but the ERO
                # doesn't have any), we proceed, filling in this ER.
                else:
                    curr_ER = candidate_ERI.execrecord
                    break

                
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
                not os.access(self.sd_fs_map[input_SD]['PATH'], os.R_OK)):
            cable.run_cable(input_SD.dataset, output_path)
        
        # 2) input_SD has real data and there is data on the filesystem:
        # --> The data was calculated from a previous step
        # --> We use the data on the filesystem
        #     (PRE: It must be equal to input_SD.dataset)
        
        # 3) input_SD does not have real data but there is data on the
        # filesystem: The data was calculated but is transient (time
        # bomb) --> We use the data on the filesystem

        elif os.access(self.sd_fs_map[input_SD]['PATH']), os.R_OK):
            cable.run_cable(self.sd_fs_map[input_SD]['PATH'], output_path)

        # 4) input_SD does not have real data and is not on the filesystem
        # (And there's nothing to reuse)
        # --> We have to backtrack.
        else:
            # Backtrack to 'fill in' our maps appropriately.
            self.recover(input_SD)

            # And now we have what we need to run this cable
            cable.run_cable(self.sd_fs_map[input_SD]['PATH'], output_path)

        # Make an ER to represent the execution above.
        output_SD = None
        output_md5 = None
        with open(output_path, "rb") as f:
            output_md5 = file_access_utils.compute_md5(f)
            
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
                    output_SD.structure.create(output_SD_CDT)

                ero_xput = None
                if type(cable) == PipelineStepInputCable:
                    ero_xput = cable.transf_input
                else:
                    ero_xput = cable.pipeline.outputs.get(
                        dataset_name=cable.output_name)

                # Add output_SD to sd_fs_map.
                self.sd_fs_map[output_SD] = {
                    'PATH': output_path, 'GENERATOR': cable
                }
            
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
            
            self.sd_fs_map[output_SD] = {
                'PATH': output_path, 'GENERATOR': cable
            }
            
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
        curr_record.execrecord = curr_ER
        curr_record.complete_clean()
        curr_record.save()
        return curr_record

    def execute_step(self, pipelinestep, inputs, step_sandbox=None):
        """
        Execute the specified PipelineStep with the given inputs.

        If code is actually run, the outputs go to the paths
        specified in output_paths.  The requisite code is placed
        in step_sandbox; if step_sandbox is None, then the default
        is [sandbox path]/step[stepnum].

        Outputs get written to
        [step sandbox]/output_data/step[step number]_[output name]

        Inputs get written to 
        [step sandbox]/input_data/step[step number]_[input name]
        (Note that this may simply be a link to data that was already
        in the sandbox elsewhere.)

        Logs get written to
        [step sandbox]/logs/step[step number]_std(out|err).txt
        """
        curr_RS = pipelinestep.pipelinestep_instances.create()
        
        if step_sandbox == None:
            step_sandbox = os.path.join(
                self.sandbox_path,
                "step{}".format(pipelinestep.step_num))

        file_access_utils.set_up_directory(step_sandbox)
        # Set up inputs, outputs, and logs directories.
        in_dir = os.path.join(step_sandbox, "input_data")
        out_dir = os.path.join(step_sandbox, "output_data")
        log_dir = os.path.join(step_sandbox, "logs")
        file_access_utils.set_up_directory(in_dir)
        file_access_utils.set_up_directory(out_dir)
        file_access_utils.set_up_directory(log_dir)

        output_paths = []
        for curr_output in pipelinestep.transformation.outputs.all():
            output_paths.append(os.path.join(
                step_sandbox, "step{}_{}".format(
                    pipelinestep.step_num, curr_output.dataset_name)))

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

        # Look for an ER that we can reuse.  It must represent the same
        # transformation, and take the same input SDs.
        possible_ERs = pipelinestep.transformation.execrecords.all()

        curr_ER = None
        for candidate_ER in possible_ERs:
            ER_matches = True
            for ERI in candidate_ER.execrecordins.all():
                # Get the input index of this ERI.
                input_idx = ERI.generic_input.dataset_idx
                if ERI.symbolicdataset != inputs_after_cable[input_idx-1]:
                    ER_matches = False
                    break
                
            # At this point all the ERIs have matched the inputs.  So,
            # we would break.
            if ER_matches:
                curr_ER = candidate_ER
                break

        # If it found an ER, check that the ER provides all of the
        # output that we need.
        if curr_ER != None:
            has_required_outputs = True
            for step_output in pipelinestep.transformation.outputs.all():
                # Check whether this output is deleted.
                if pipelinestep.outputs_to_delete.filter(step_output).exists():
                    continue

                corresp_ero = curr_ER.execrecordouts.get(
                    generic_output=step_output)

                if not corresp_ero.has_data():
                    has_required_outputs = False
                    break

            if has_required_outputs:
                # The ER found has what we need, so we can reuse it.
                curr_RS.reused = True
                curr_RS.execrecord = curr_ER
                curr_RS.complete_clean()
                curr_RS.save()

                return curr_RS

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
            #
            # We need to then register the output paths with the
            # appropriate SDs, creating Datasets as necessary.
            stdout_path = os.path.join(log_dir, "step{}_stdout.txt".
                                       format(pipelinestep.step_num))
            stderr_path = os.path.join(log_dir, "step{}_stderr.txt".
                                       format(pipelinestep.step_num))

            method_popen = None
            with (open(stdout_path, "wb", 1), 
                  open(stderr_path, "wb", 0) as (outwrite, errwrite):
                method_popen = pipelinestep.transformation.run_code(
                    step_sandbox,
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

            # Now, we need to confirm that all of the outputs are present.
            # If they are all present, then:
            # 1) if they can be confirmed against past data, do it
            # 2) Create a Dataset and clean it; that will ensure that
            #    it conforms to its specification.
            #    FIXME which we still need to fill in

            # This is keyed by the position index of the generating
            # output and stores the computed MD5s of the corresponding
            # output files.
            output_MD5s = {}
            for i, output_path in enumerate(output_paths):
                # i is 0-based; dataset_idx is 1-based.
                output_idx = i + 1
            
                if not os.access(output_path, "F_OK"):
                    raise ValueError(
                        "Step {} of run {} did not create output file {}".
                        format(pipelinestep.step_num, self.run,
                               output_path))

                # Compute the MD5 checksum.
                with open(output_path, "rb") as f:
                    output_MD5s[output_idx] = file_access_utils.compute_MD5(f)

                # If an ER was found but insufficient, there will be
                # SymbolicDatasets representing the outputs; this
                # allows us to check the MD5 checksum.
                if curr_ER != None:
                    corresp_output = pipelinestep.transformation.outputs.get(
                        dataset_idx=output_idx)
                    corresp_ERO = curr_ER.execrecordouts.get(
                        content_type=ContentType.objects.get_for_model(
                            TransformationOutput),
                        object_id=corresp_output.pk)
                    if (output_MD5s[output_idx] != 
                            corresp_ERO.symbolicdataset.MD5_checksum):
                        raise ValueError(
                            "Output \"{}\" of Method \"{}\" failed integrity check".
                            format(output_path, pipelinestep.transformation))
            
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
                            compounddatatype=curr_output.get_cdt())

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

            # Make sure the ER is clean and complete.
            curr_ER.complete_clean()
            # Finish curr_RS.
            curr_RS.execrecord = curr_ER
            curr_RS.complete_clean()
            curr_RS.save()
                        
        else:
            # FIXME fill this in when we figure out what to do here.
            self.execute_pipeline(pipeline=pipelinestep.transformation,
                                  inputs=inputs_after_cables,
                                  parent_runstep=curr_RS)


        return curr_RS
