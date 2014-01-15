"""Code that is responsible for the execution of Pipelines."""

from django.core.files import File
from django.contrib.contenttypes.models import ContentType
from librarian.models import ExecRecordIn

# Import our Shipyard models module.
import file_access_utils, logging_utils
import os.path
import logging, sys, time
import tempfile
import archive.models, librarian.models, metadata.models, pipeline.models, transformation.models

class Sandbox:
    """
    Represents the state of a run.
    Keeps track of where the sandbox is, and stuff within the sandbox.
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

    def __init__(self, user, pipeline, inputs, sandbox_path=None):
        """
        Sets up a sandboxed environment to run the specified Pipeline.

        user is the user running it; pipeline is the Pipeline to run;
        inputs is a list of SymbolicDatasets to feed pipeline.

        All inputs must either have real data (especially if
        this Sandbox represents a 'top-level' run), or it is
        is in sd_fs_map (e.g. if this is a sub-run) and therefore
        can be recovered using the info in the maps.i

        PRE: the inputs are all appropriate for pipeline.
        """
        logging_utils.setup_logging()
        import inspect
        fn = "{}.{}()".format(self.__class__.__name__, inspect.stack()[0][3])
        logger = logging.getLogger()

        self.user = user
        self.pipeline = pipeline
        self.inputs = inputs
        self.sd_fs_map = {}
        self.socket_map = {}
        self.cable_map = {}
        self.ps_map = {}

        # Initialize the maps ourselves.

        logger.debug("{}: initializing maps".format(fn))
        for i, pipeline_input in enumerate(inputs):
            # Get the corresponding pipeline input, compensating for
            # 0-basedness.
            corresp_pipeline_input = pipeline.inputs.get(dataset_idx=i+1)
            self.socket_map[(None, corresp_pipeline_input)] = pipeline_input
            self.sd_fs_map[pipeline_input] = None

        # Determine a sandbox path.
        self.sandbox_path = sandbox_path
        self.run = pipeline.pipeline_instances.create(user=self.user)

        if sandbox_path == None:
            self.sandbox_path = tempfile.mkdtemp(prefix="user{}_run{}_".format(self.user, self.run.pk))

        # Make the sandbox directory.
        logger.debug("{}: file_access_utils.set_up_directory({})".format(fn, self.sandbox_path))
        file_access_utils.set_up_directory(self.sandbox_path)

    def execute_cable(self, cable, input_SD, output_path, parent_record,
                      recover=False):
        """
        Execute the cable on the input.

        INPUTS
        input_SD        SD fed into the PSIC/POC.
        output_path     Where the output file should be written.
        parent_record   The RS for this PSIC / run for this POC.
        recover         True if recovering + ignore input_SD/output_path/parent_record

        OUTPUTS
        RSIC/ROC that describes this execution.

        NOTES
        Recovering is to re-compute something reused to recover the data
        output_path is recovered using the maps.
        sd_fs_map and cable_map will have been updated.

        PRECONDITIONS
        1) input_SD has an appropriate CDT for feeding this cable.
        2) if input_SD has data, and input_path refers to a file, they are the same.
        3) input_SD is in sd_fs_map
        4) All the _maps are "up to date" for this step
        5) input_SD is clean
        """

        import inspect
        fn = "{}.{}()".format(self.__class__.__name__, inspect.stack()[0][3])

        ####
        # CREATE/RETRIEVE RECORD
        
        # The record that we create/update.
        curr_record = None
        curr_ER = None
        output_SD = None
        # What comes out of the cable will have the following CDT:
        output_SD_CDT = None

        # If not recovering, we create a new record.
        if not recover:
            logging.debug("{}: Not recovering cable".format(fn))

            if type(cable) == pipeline.models.PipelineStepInputCable:
                logging.debug("{}: Creating RSIC".format(fn))
                curr_record = cable.psic_instances.create(runstep=parent_record)
                required_record_type = archive.models.RunSIC
            else:
                logging.debug("{}: Creating ROC".format(fn))
                curr_record = cable.poc_instances.create(run=parent_record)
                required_record_type = archive.models.RunOutputCable

            ####
            # LOOK FOR REUSABLE ER

            # Check if this cable keeps its output.
            cable_keeps_output = None
            if type(cable) == pipeline.models.PipelineStepInputCable:
                cable_keeps_output = cable.keep_output
            else:
                # Check parent_record (which is a Run) whether or
                # not this POC's output is to be deleted.
                if parent_record.parent_runstep != None:
                    cable_keeps_output = not (
                        parent_record.parent_runstep.pipelinestep.
                        outputs_to_delete.filter(dataset_name=cable.output_name).exists())
            logging.debug("{}: Cable keeps output? {}".format(fn, cable_keeps_output))

            # FIXME now we have to redefine what "reusability" means: an
            # ER should only be considered for reuse if all of its inputs
            # and outputs are still considered valid *at this time*.
            
            # To find cable ER to reuse, load cable ERIs of the same cable
            # type taking inputSD as input

            # A) Get the content type (RSIC or ROC?) of the ExecRecord's ExecLog
            record_contenttype = ContentType.objects.get_for_model(required_record_type)
            logging.debug("{}: Searching for reusable cable ER - (linked to an '{}' ExecLog)".format(fn, record_contenttype))

            # B) Look at ERIs linked to the same cable type, with matching input SD
            candidate_ERIs = ExecRecordIn.objects.filter(execrecord__generator__content_type=record_contenttype, symbolicdataset=input_SD)


            curr_record.reused = False
            
            # FIXME can we speed this up using a prefetch?
            # Search for an execrecord that we can reuse OR fill in.
            for candidate_ERI in candidate_ERIs:
                logging.debug("{}: Considering ERI {}".format(fn, candidate_ERI))
                candidate_cable = candidate_ERI.execrecord.general_transf
    
                if cable.is_compatible(candidate_cable):
                    output_SD = (candidate_ERI.execrecord.execrecordouts.
                                    all()[0].symbolicdataset)
                    
                    # If you're not keeping the output, or you are and
                    # there is existent data, you can successfully reuse
                    # the ER.
                    if not cable_keeps_output or output_SD.has_data():
                        curr_record.reused = True
                        curr_record.execrecord = candidate_ERI.execrecord
                        curr_record.complete_clean()
                        curr_record.save()
                        
                        # Add the ERO's SD to sd_fs_map if this SD was not
                        # already in sd_fs_map; if it was but had never
                        # been written to the FS, update it with the path.
                        if (output_SD not in self.sd_fs_map or
                                self.sd_fs_map[output_SD] == None):
                            self.sd_fs_map[output_SD] = output_path
    
                        # Add (cable, destination socket) to socket_map.
                        # FIXME: the socket for this cable is determined by looking at the schematic?
                        socket_map[(cable, cable.generic_output)] = output_SD
                        
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

                            output_SD = curr_ER.execrecordouts.all()[0].symbolicdataset
                            output_SD_CDT = output_SD.get_cdt()

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
        
        logging.debug("{}: No ER to reuse - committed to executing cable".format(fn))
        curr_log = None

        # The input contents are not on the file system, and:
        if (self.sd_fs_map[input_SD] == None) or not os.access(self.sd_fs_map[input_SD], os.R_OK):
            logging.debug("{}: Dataset unavailable on file system".format(fn))

            if input_SD.has_data():
                logging.debug("{}: Dataset has real data: using it for cable execution".format(fn))
                logging.debug("{}: PSIC.run_cable('{}','{}','{}')".format(fn, input_SD.dataset, output_path, curr_record))
                curr_log = cable.run_cable(input_SD.dataset,output_path,curr_record)

            else:
                logging.debug("{}: Dataset is symbolic: need to recover it".format(fn))
                logging.debug("{}: recover({})".format(fn, input_SD))
                successful_recovery = self.recover(input_SD)

                if not successful_recovery:
                    logging.warn("{}: Recovery failed, returning incomplete RSIC/ROC (IE, missing ExecLog)".format(fn))
                    return curr_record

                logging.debug("{}: PSIC.run_cable('{}','{}','{}')".format(fn, self.sd_fs_map[input_SD], output_path, curr_record))
                curr_log = cable.run_cable(self.sd_fs_map[input_SD],output_path, curr_record)


        else:
            logging.debug("{}: Dataset contents found on file system: using it for cable execution".format(fn))
            dataset_path = self.sd_fs_map[input_SD]

            if os.access(dataset_path, os.R_OK):
                logging.debug("{}: cable.run_cable('{}','{}','{}')".format(fn, dataset_path,output_path,curr_record))
                curr_log = cable.run_cable(dataset_path,output_path,curr_record)
            else:
                logging.error("{}: Can't access dataset on file system anymore! Returning incomplete RSIC/ROC".format(fn))
                return curr_record

        # FINISHED RUNNING CABLE
        ####

        ####
        # CREATE EXECRECORD

        # Since we attempted to run code, regardless outcome, create an ER.
        # For now, fill in the MD5 and num_rows with default values.
        had_ER_at_beginning = curr_ER != None
        
        if not recover and curr_ER == None:
            logging.debug("{}: No ER already in use - creating fresh cable ER + ERI".format(fn))

            # Create a new ER, generated by the above ExecLog.
            curr_ER = curr_log.execrecords.create()
            curr_ER.execrecordins.create(generic_input=cable.source,symbolicdataset=input_SD)

            if cable.is_trivial():
                output_SD = input_SD
            else:
                output_SD = librarian.models.SymbolicDataset(MD5_checksum="")
                output_SD.save()

                output_SD_CDT = metadata.models.CompoundDatatype()
                output_SD_CDT.save()

                wires = None
                if type(cable) == pipeline.models.PipelineStepInputCable:
                    wires = cable.custom_wires.all()
                else:
                    wires = cable.custom_outwires.all()

                # Look at each wire, take the DT from
                # source_pin, assign the name and index of
                # dest_pin.
                for wire in wires:
                    logging.debug("{}: Adding CDTM: {} {}".format(fn, wire.dest_pin.column_name, wire.dest_pin.column_idx))
                    output_SD_CDT.members.create(datatype=wire.source_pin.datatype,
                                                 column_name=wire.dest_pin.column_name,
                                                 column_idx=wire.dest_pin.column_idx)


                output_SD_CDT.clean()


                # Add this structure to the symbolic dataset.
                if output_SD_CDT != None:
                    logging.debug("{}: Registering output SD's CDT structure".format(fn))

                    # You need to work with this 1-to-1 properly
                    output_SD_structure = librarian.models.DatasetStructure(symbolicdataset=output_SD,
                                                                     compounddatatype=output_SD_CDT,
                                                                     num_rows=-1)
                    output_SD_structure.save()
                    logging.debug("{}: output_SD is raw? {}".format(fn, output_SD.is_raw()))
                else:
                    logging.debug("{}: Output SD is raw".format(fn))

            ero_xput = None
            if type(cable) == pipeline.models.PipelineStepInputCable:
                ero_xput = cable.dest
            else:
                ero_xput = cable.pipeline.outputs.get(
                    dataset_name=cable.output_name)
            
            curr_ER.execrecordouts.create(generic_output=ero_xput,
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
            if (output_SD not in self.sd_fs_map or self.sd_fs_map[output_SD] == None):
                self.sd_fs_map[output_SD] = output_path

            # FIXME: If this is a POC, cable.dest doesn't exist
            # Normally, cable.dest points to the TI/TO
            # But in the case of a POC, we must get the TO from this pipeline that has the output_idx and output_name
            cable_dest = ""
            if type(cable).__name__ == "PipelineOutputCable":
                cable_dest = self.pipeline.outputs.filter(dataset_name=cable.output_name, dataset_idx=cable.output_idx)
            else:
                cable_dest = cable.dest

            #self.socket_map[(cable, cable.dest)] = output_SD
            self.socket_map[(cable, cable_dest)] = output_SD
            self.cable_map[cable] = curr_record

        # FINISHED BOOKKEEPING
        ####
        return curr_record

    def execute_step(self, curr_run, pipelinestep, inputs, step_run_dir=None,
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

        import inspect, logging
        import django.utils.timezone

        fn = "{}.{}()".format(self.__class__.__name__, inspect.stack()[0][3])

        curr_ER = None
        output_paths = []
        inputs_after_cable = []
        had_ER_at_beginning = False

        if not recover:
            logging.debug("{}: NON-RECOVERY CASE".format(fn))
            curr_RS = pipelinestep.pipelinestep_instances.create(run=curr_run)

            logging.debug("{}: Preparing file system for sandbox".format(fn))
            step_run_dir = step_run_dir or os.path.join(self.sandbox_path, "step{}".format(pipelinestep.step_num))
            out_dir = os.path.join(step_run_dir, "output_data")
            in_dir = os.path.join(step_run_dir, "input_data")
            log_dir = os.path.join(step_run_dir, "logs")
            file_access_utils.set_up_directory(step_run_dir)
            file_access_utils.set_up_directory(in_dir)
            file_access_utils.set_up_directory(out_dir)
            file_access_utils.set_up_directory(log_dir)

            for curr_output in pipelinestep.transformation.outputs.all().order_by("dataset_idx"):
                file_suffix = "raw" if curr_output.is_raw() else "csv"
                file_name = "step{}_{}.{}".format(pipelinestep.step_num, curr_output.dataset_name,file_suffix)
                output_path = os.path.join(out_dir,file_name)
                output_paths.append(output_path)

            logging.debug("{}: Running input cables (PSICs)".format(fn))
            for curr_input in pipelinestep.transformation.inputs.all().order_by("dataset_idx"):
                corresp_cable = pipelinestep.cables_in.get(dest=curr_input)
                cable_dir = os.path.join(in_dir,"step{}_{}".format(pipelinestep.step_num,curr_input.dataset_name))
                logging.debug("{}: execute_cable('{}','{}','{}','{}')".format(fn, corresp_cable, inputs[curr_input.dataset_idx-1], cable_dir, curr_RS))
                curr_RSIC = self.execute_cable(corresp_cable, inputs[curr_input.dataset_idx-1],cable_dir,curr_RS)
                inputs_after_cable.append(curr_RSIC.execrecord.execrecordouts.all()[0].symbolicdataset)
                
            curr_RS.clean()
            logging.debug("{}: Looking for ER with same transformation + input SDs".format(fn))
            curr_ER = pipelinestep.transformation.find_compatible_ER(inputs_after_cable)

            if curr_ER != None:
                logging.debug("{}: Found ER, checking it provides outputs needed".format(fn))
                had_ER_at_beginning = True
                outputs_needed = pipelinestep.outputs_to_retain()
                if curr_ER.provides_outputs(outputs_needed):

                    logging.debug("{}: Completely reusing ER {}".format(fn, curr_ER))

                    curr_RS.reused = True
                    curr_RS.execrecord = curr_ER
                    curr_RS.complete_clean()
                    curr_RS.save()
    
                    logging.debug("{}: Updating maps for where code *should be* (May have to fill in later)")

                    self.ps_map[pipelinestep] = (step_run_dir, curr_RS)
    
                    # Add every output of this transformation to sd_fs_map.
                    for step_output in pipelinestep.transformation.outputs.all():
                        corresp_ero = curr_ER.execrecordouts.get(content_type=ContentType.objects.get_for_model(type(step_output)),
                                                                 object_id=step_output.pk)
    
                        corresp_SD = corresp_ero.symbolicdataset
                        corresp_path = output_paths[step_output.dataset_idx-1]

                        logger.debug("{}: Updating sd_fs and socket maps".format(fn))
                        if corresp_SD not in self.sd_fs_map:
                            self.sd_fs_map[corresp_SD] = corresp_path
                        self.socket_map[(pipelinestep, step_output)] = corresp_SD
                    logging.debug("{}: Finished completely reusing ER".format(fn))
                    return curr_RS
                curr_RS.reused = False
            else:
                curr_RS.reused = False




        else:
            logger.debug("{}: Recovering step".format(fn))
            step_run_dir, curr_RS = self.ps_map(pipelinestep)

            for curr_output in (
                pipelinestep.transformation.outputs.all().order_by("dataset_idx")):
                # Get the SymbolicDataset that comes from this output
                # using socket_map; then use sd_fs_map to get its
                # path.
                corresp_SD = self.socket_map(pipelinestep, curr_output)
                output_paths.append(self.sd_fs_map[corresp_SD])

            # Retrieve the input SDs from the ER.
            for curr_input in (
                pipelinestep.transformation.inputs.all().order_by("dataset_idx")):
                corresp_ERI = curr_ER.execrecordins.get(
                    content_type=ContentType.objects.get_for_model(
                        transformation.models.TransformationInput),
                    object_id=curr_input.pk)
                inputs_after_cable.append(corresp_ERI.symbolicdataset)

            curr_ER = curr_RS.execrecord
            had_ER_at_beginning = True
        logging.debug("{}: Finished setting up runstep, output paths, inputs, ER ... (???)".format(fn))


        logging.debug("{}: checking required datasets are on the FS for running code".format(fn))
        for curr_in_SD in inputs_after_cable:
            curr_path = self.sd_fs_map[curr_in_SD]
            if not os.access(curr_path, os.F_OK):
                logging.debug("{}: File {} not on FS: recovering".format(fn, curr_path))
                successful_recovery = self.recover(curr_in_SD)

                if not successful_recovery:
                    logging.debug("{}: Failed to recover: quitting without creating ER".format(fn))
                    return curr_record
        logging.debug("{}: Finished putting datasets into place: proceeding to run code for this step".format(fn))


        if type(pipelinestep.transformation) == pipeline.models.Pipeline:
            logging.debug("{}: Executing sub-pipeline".format(fn))

            child_run = self.execute_pipeline(
                pipeline=pipelinestep.transformation,
                inputs=inputs_after_cables,
                parent_runstep=curr_RS)

            logging.debug("{}: Finished executing sub-pipeline".format(fn))
            return curr_RS


        curr_log = archive.models.ExecLog(record=curr_RS)
        logging.debug("{}: Created EL for method execution at {}".format(fn, curr_log))
        method_popen = None
        stdout_path = os.path.join(log_dir, "step{}_stdout.txt".format(pipelinestep.step_num))
        stderr_path = os.path.join(log_dir, "step{}_stderr.txt".format(pipelinestep.step_num))

        logging.debug("{}: Running code".format(fn))
        with open(stdout_path, "wb", 1) as outwrite:
            errwrite = open(stderr_path, "wb", 0)
            method_popen = pipelinestep.transformation.run_code(
                step_run_dir,
                [self.sd_fs_map[x] for x in inputs_after_cable],
                output_paths,
                outwrite,
                errwrite)

            logging.debug("{}: Polling Popen + displaying stdout/stderr to console".format(fn))
            # While running, print stdout/stderr to console
            with open(stdout_path, "rb", 1) as outread:
                errread = open(stderr_path, "rb", 0)
                while method_popen.poll() is None:
                    logging.debug("{}: Waiting for execution to finish...".format(fn))
                    sys.stdout.write(outread.read())
                    sys.stderr.write(errread.read())
                    time.sleep(2)
                outwrite.flush()
                errwrite.flush()
                sys.stdout.write(outread.read())
                sys.stderr.write(errread.read())
                errread.close()
            errwrite.close()

        sys.stdout.flush()
        sys.stderr.flush()

        curr_date_time = django.utils.timezone.now()
        curr_log.end_time = curr_date_time
        curr_log.clean()
        curr_log.save()
        logging.debug("{}: Method execution complete, saving ExecLog (started = {}, ended = {})".format(
                fn, curr_log.start_time, curr_log.end_time))

        logging.debug("{}: Storing stdout/stderr in MethodOutput".format(fn))
        curr_mo = archive.models.MethodOutput(execlog=curr_log, return_code=method_popen.returncode)
        with open(stdout_path, "rb") as outread:
            errread = open(stderr_path, "rb")
            curr_mo.output_log.save(stdout_path, File(outread))
            curr_mo.error_log.save(stderr_path, File(errread))
            errread.close()
        curr_mo.clean()
        curr_mo.save()
        curr_log.complete_clean()



        if curr_ER == None:
            logging.debug("{}: Creating fresh ER".format(fn))

            # FIXME: Have Richard review this
            curr_ER = librarian.models.ExecRecord(generator=curr_log)
            curr_ER.save()
            curr_RS.execrecord = curr_ER
            curr_RS.save()

            logging.debug("{}: Annotating ERIs for ER '{}'".format(fn, curr_ER))
            for curr_input in pipelinestep.transformation.inputs.all():
                corresp_input_SD = inputs_after_cable[curr_input.dataset_idx-1]
                curr_ER.execrecordins.create(
                    generic_input=curr_input,
                    symbolicdataset=corresp_input_SD)

            logging.debug("{}: Creating new SDs + EROs".format(fn))
            for curr_output in pipelinestep.transformation.outputs.all():
                corresp_output_SD = librarian.models.SymbolicDataset(MD5_checksum="")
                corresp_output_SD.save()

                if not curr_output.is_raw():
                    # FIXME: Have Richard review this
                    curr_structure = librarian.models.DatasetStructure(symbolicdataset = corresp_output_SD,
                                                                       compounddatatype=curr_output.get_cdt(),
                                                                       num_rows=-1)
                    curr_structure.save()
                    #corresp_output_SD.structure.create(compounddatatype=curr_output.get_cdt(),num_rows=-1)

                corresp_output_SD.clean()

                curr_ER.execrecordouts.create(
                    generic_output=curr_output,
                    symbolicdataset=corresp_output_SD)

            curr_ER.complete_clean()
        logging.debug("{}: Finished creating fresh ER, proceeding to check outputs".format(fn))

        # had_output_found indicates we have detected problems with the output.
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

            output_md5 = ""
            with open(output_path, "rb") as f:
                output_md5 = file_access_utils.compute_md5(f)

            if not had_ER_at_beginning:
                output_SD.MD5_checksum = output_md5
                output_SD.save()
                logging.debug("{}: First time seeing file: saving md5 {}".format(fn, output_md5))


            if (not pipelinestep.outputs_to_delete.filter(
                    pk=curr_output.pk).exists() and 
                    not output_ERO.has_data()):

                logging.debug("{}: Retaining output: creating Dataset".format(fn))

                desc = "run: {}\nuser: {}\nstep: {}\nmethod: {}\noutput: {}".format(
                    self.run.name,
                    self.user,
                    pipelinestep.step_num,
                    pipelinestep.transformation,
                    curr_output.dataset_name)

                new_DS = archive.models.Dataset(
                    user=self.user,
                    name=("run:{}__step:{}__output:{}".
                          format(self.run.name,
                                 pipelinestep.step_num,
                                 curr_output.dataset_name)),
                    description=desc,
                    symbolicdataset=output_SD,
                    created_by=curr_RS)

                # dataset_idx is 1-based, and output_paths is 0-based.
                with open(output_path, "rb") as f:
                    new_DS.dataset_file.save(os.path.basename(output_path),
                                             File(f))
                new_DS.clean()
                new_DS.save()

            if bad_output_found:
                logging.debug("{}: Already found bad data, not proceeding with anymore checks".format(fn))
                continue


            if not had_ER_at_beginning:
                logging.debug("{}: New data - performing content check".format(fn))
                summary_path = "{}_summary".format(output_path)

                ccl = output_SD.check_file_contents(
                        output_path, summary_path,
                        curr_output.get_min_row(), curr_output.get_max_row(),
                        curr_log)

                if ccl.is_fail():
                    logging.debug("{}: content check FAILED for {}".format(fn, output_path))
                    bad_output_found = True
                else:
                    logging.debug("{}: content check passed for {}".format(fn, output_path))

            elif had_ER_at_beginning:
                logging.debug("{}: SD has been computed before, checking integrity of {}".format(fn, output_SD))
                icl = output_SD.check_integrity(output_path,curr_log,output_md5)

                if icl.is_fail():
                    bad_output_found = True
                    
        logging.debug("{}: Finished checking outputs".format(fn))
        curr_ER.complete_clean()

        if not recover:
            logging.debug("{}: Not recovering: finishing bookkeeping".format(fn))
            curr_RS.execrecord = curr_ER
            curr_RS.complete_clean()
            curr_RS.save()

            # Since reused=False, step_run_dir represents where the step *actually is*
            logging.debug("Updating maps")
            self.ps_map[pipelinestep] = (step_run_dir, curr_RS)
    
            for step_output in pipelinestep.transformation.outputs.all():
                corresp_ero = curr_ER.execrecordouts.get(
                    content_type=ContentType.objects.get_for_model(type(step_output)),
                    object_id=step_output.pk)
    
                corresp_SD = corresp_ero.symbolicdataset
                corresp_path = output_paths[step_output.dataset_idx-1]
    
                if corresp_SD not in self.sd_fs_map:
                    self.sd_fs_map[corresp_SD] = corresp_path
                self.socket_map[(pipelinestep, step_output)] = corresp_SD

        return curr_RS

    def execute_pipeline(self, pipeline=None, input_SDs=None, 
                         sandbox_path=None, parent_runstep=None):
        """
        Execute the specified Pipeline with the given inputs.


        Top level pipeline: pipeline, input_SDs, sandbox_path, and parent_runstep are None,

        Outputs get written to
        [sandbox_path]/output_data/run[run PK]_[output name].(csv|raw)

        At the end of this function, the outputs of the pipeline
        will be added to sd_fs_map, so you can determine where
        to find your output.
        """

        import inspect
        fn = "{}.{}()".format(self.__class__.__name__, inspect.stack()[0][3])

        logger = logging.getLogger()
        is_set = (
            pipeline != None, input_SDs != None,
            sandbox_path != None, parent_runstep != None
        )
        if any(is_set) and not all(is_set):
            raise ValueError("Either none or all parameters must be None")
        
        pipeline = pipeline or self.pipeline
        sandbox_path = sandbox_path or self.sandbox_path

        curr_run = self.run
        if parent_runstep != None:
            logger.debug("{}: parent_runstep is not none".format(fn))
            curr_run = pipeline.pipeline_instances.create(user=self.user,
                                                          parent_runstep=parent_runstep)

        ####
        # SET UP SANDBOX AND PATHS

        # Set up an output directory (or make sure it's usable).
        out_dir = os.path.join(sandbox_path, "output_data")
        logger.debug("file_access_utils.set_up_directory({})".format(out_dir))
        file_access_utils.set_up_directory(out_dir)


        # FINISHED SETTING UP SANDBOX AND PATHS
        ####
        
        ####
        # RUN STEPS

        for step in pipeline.steps.all().order_by("step_num"):
            logger.debug("{}: Looking at cables for step '{}'".format(fn, step))
            step_inputs = []
            for cable in step.cables_in.all().order_by("dest__dataset_idx"):
                logger.debug("{}: Finding SD that feeds cable '{}'".format(fn, cable))

                # Find the SD that feeds this cable.  First, identify
                # the generating step.  If it was a Pipeline input,
                # leave generator == None.
                generator = None
                if cable.source_step != 0:
                    generator = pipeline.steps.get(step_num=cable.source_step)
                
                # Look up the symDS that is associated with this socket
                # (The generator PS must already have been executed)
                step_inputs.append(self.socket_map[(generator, cable.source)])

            run_dir = os.path.join(sandbox_path,"step{}".format(step.step_num))

            # FIXME: execute_step needs to know what run was created
            logger.debug("{}: execute_step('{}', '{}', '{}', '{}')".format(fn, curr_run, step, step_inputs, run_dir))
            curr_RS = self.execute_step(curr_run, step, step_inputs,step_run_dir=run_dir)
            logger.debug("{}: DONE EXECUTING STEP".format(fn))

            if not curr_RS.is_complete() or not curr_RS.successful_execution():
                logger.debug("{}: Step failed to execute: returning the run".format(fn))
                curr_run.clean()
                return curr_run

        logging.debug("{}: Finished executing steps, proceeding to run output cables".format(fn))

        for outcable in pipeline.outcables.all():
            # Identify the SD that feeds this outcable.
            generator = pipeline.steps.get(step_num=outcable.source_step)
            source_SD = self.socket_map[(generator, outcable.source)]
            file_suffix = "raw" if outcable.is_raw() else "csv"
            out_file_name = "run{}_{}.{}".format(curr_run.pk, outcable.output_name,file_suffix)
            output_path = os.path.join(out_dir,out_file_name)
            curr_ROC = self.execute_cable(outcable, source_SD,output_path, curr_run)

            if not curr_ROC.is_complete() or not curr_RS.successful_execution():
                curr_run.clean()
                logger.debug("Execution failed")
                return curr_run

        logging.debug("{}: Finished executing output cables".format(fn))
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
        
