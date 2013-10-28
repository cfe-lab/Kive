"""Code that is responsible for the execution of Pipelines."""

# Import our Shipyard models module.
import models
import file_access_utils
import os.path

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
        output_SD = None
        if curr_ER == None:
            # No ER was found; create a new one.
            curr_ER = cable.execrecords.create()
            curr_ER.execrecordins.create(
                generic_input=cable.provider_output,
                symbolicdataset=input_SD)
            output_SD = SymbolicDataset()
            output_SD.save()

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
            output_SD = curr_ER.execrecordouts.all()[0].symbolicdataset

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

            # Add this structure to the symbolic dataset
            output_SD_CDT.clean()
            output_SD.structure.create(output_SD_CDT)
            
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

        # Annotate sd_fs_map with the output created by run_cable
        self.sd_fs_map[output_SD] = {'PATH': output_path, 'GENERATOR': cable}
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
            new_dataset.set_md5()
            new_dataset.clean()
            new_dataset.save()

        # Complete the ER and record, then return the record.
        curr_ER.complete_clean()
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
        [step sandbox]/output_data/step[step number]_[output name].

        Inputs get written to 
        [step sandbox]/input_data/step[step number]_[input name].
        (Note that this may simply be a link to data that was already
        in the sandbox elsewhere.)
        """
        curr_RS = pipelinestep.pipelinestep_instances.create()
        
        if step_sandbox == None:
            step_sandbox = os.path.join(
                self.sandbox_path,
                "step{}".format(pipelinestep.step_num))

        file_access_utils.set_up_directory(step_sandbox)
        # Set up an inputs and an outputs directory.
        in_dir = os.path.join(step_sandbox, "input_data")
        out_dir = os.path.join(step_sandbox, "output_data")
        file_access_utils.set_up_directory(in_dir)
        file_access_utils.set_up_directory(out_dir)

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
            for step_output in pipelinestep.transformation.outputs.all():
                # Check whether this output is deleted.
                if pipelinestep.outputs_to_delete.filter(step_output).exists():
                    continue

                # FIXME continue from here.
                #corresp_ero = curr_ER.

        # If it's a method, run the code; if not, call execute on the
        # pipeline.
        if type(pipelinestep.transformation) == Method:
            
        else:
            # FIXME fill this in when we figure out what to do here.
            self.execute_pipeline()
