"""
librarian.models

Shipyard data models pertaining to the lookup of the past: ExecRecord,
SymbolicDataset, etc.
"""

from django.db import models
from django.contrib.contenttypes.models import ContentType
from django.contrib.contenttypes import generic
from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator, RegexValidator
from django.core.files import File

import re
import archive.models, metadata.models, method.models, pipeline.models, transformation.models
import file_access_utils, logging_utils

class SymbolicDataset(models.Model):
    """
    Symbolic representation of a (possibly temporary) data file.

    That is to say, at some point, there was a data file uploaded to/
    generated by Shipyard, which was coherent with its
    specified/generating CDT and its producing
    TransformationOutput/cable (if it was generated), and this
    represents it, whether or not it was saved to the database.

    This holds metadata about the data file.

    PRE: the actual file that the SymbolicDataset represents (whether
    it still exists or not) is/was coherent (e.g. checked using
    file_access_utils.summarize_CSV()).
    """
    # For validation of Datasets when being reused, or when being
    # regenerated.  A blank MD5_checksum means that the file was
    # missing (not created when it was supposed to be created).
    MD5_checksum = models.CharField(
        max_length=64,
        validators=[RegexValidator(
            regex=re.compile("(^[1234567890AaBbCcDdEeFf]{32}$)|(^$)"),
            message="MD5 checksum is not either 32 hex characters or blank")],
        blank=True,
        default="",
        help_text="Validates file integrity")

    def __unicode__(self):
        """
        Unicode representation of a SymbolicDataset.

        This is S[pk] or S[pk]d if it has data.
        """
        has_data_suffix = "d" if self.has_data() else ""
        return "S{}{}".format(self.pk, has_data_suffix)

    def clean(self):
        """
        Checks coherence of this SymbolicDataset.

        If it has data (i.e. an associated Dataset), it cleans that
        Dataset.  Then, if there is an associated DatasetStructure,
        clean that.

        Note that the MD5 checksum is already checked via a validator.
        """
        if self.has_data():
            self.dataset.clean()
        
        # If there is an associated DatasetStructure, clean the structure
        # October 31, 2013: having simplified our checks on the structure
        # (i.e. removing them totally), this is no longer relevant.
        # if not self.is_raw():
        #     self.structure.clean()

    def has_data(self):
        """True if associated Dataset exists; False otherwise."""
        return hasattr(self, "dataset")
    
    def is_raw(self):
        """True if this SymbolicDataset is raw, i.e. not a CSV file."""
        return not hasattr(self, "structure")
            
    def num_rows(self):
        """
        Returns number of rows in the associated Dataset.

        This returns None if the Dataset is raw.
        """
        if self.is_raw():
            return None
        return self.structure.num_rows

    def get_cdt(self):
        """
        Retrieve the CDT of this SymbolicDataset (none if it is raw).
        """
        cdt = None if self.is_raw() else self.structure.compounddatatype
        return cdt
        

    @classmethod
    # FIXME what does it do for num_rows when file_path is unset?
    def create_SD(cls, file_path, cdt=None, make_dataset=True, user=None,
                  name=None, description=None):
        """
        Helper function to make defining SDs and Datasets faster.
    
        user, name, and description must all be set if make_dataset=True.
        make_dataset creates a Dataset from the given file path to go
        with the SD.
    
        Returns the SymbolicDataset created.
        """
        symDS = SymbolicDataset()
        with open(file_path, "rb") as f:
            symDS.MD5_checksum = file_access_utils.compute_md5(f)
        symDS.clean()
        symDS.save()
    
        structure = None
        if cdt != None:
            structure = DatasetStructure(symbolicdataset=symDS,
                                         compounddatatype=cdt)
            
            with open(file_path, "rb") as f:
                CSV_summary = cdt.summarize_CSV(f, "/tmp/SD{}".format(symDS.pk))
                print CSV_summary
                structure.num_rows = CSV_summary["num_rows"]
            structure.save()
    
        dataset = None
        if make_dataset:
            dataset = archive.models.Dataset(
                user=user, name=name, description=description,
                symbolicdataset=symDS)
            with open(file_path, "rb") as f:
                dataset.dataset_file.save(file_path, File(f))
            dataset.clean()
            dataset.save()
    
        symDS.clean()
    
        return symDS

    
    def check_file_contents(self, file_path_to_check, summary_path,
                            min_row, max_row, execlog):
        """
        Does a content check of a file that this SD represents.

        If this SD is raw, it just creates a clean CCL.

        This calls [CDT].summarize_CSV on the file and creates a
        ContentCheckLog.
        
        Returns the completed ContentCheckLog, and associates it with
        execlog.  If the file does not have a badly-formed header, it
        also sets this SD's num_rows.

        FIXME this should probably be invoked within a transaction!
        """
        import datachecking.models, inspect, logging
        fn = "{}.{}()".format(self.__class__.__name__, inspect.stack()[0][3])

        if self.is_raw():
            logging.debug("{}: SD is raw, creating clean CCL".format(fn))
            ccl = self.content_checks.create(execlog=execlog)
            ccl.clean()
            return ccl


        logging.debug("{}: SD is not raw, checking CSV".format(fn))
        csv_summary = None
        my_CDT = self.get_cdt()
        with open(file_path_to_check, "rb") as f:
            csv_summary = my_CDT.summarize_CSV(f, summary_path)
        ccl = self.content_checks.create(execlog=execlog)
        
        # Check for a malformed header (and thus a malformed file).
        if ("bad_num_cols" in csv_summary or "bad_col_indices" in csv_summary):
            bad_data = datachecking.models.BadData(contentchecklog = ccl, bad_header=True)
            bad_data.save()
            logging.debug("{}: malformed header".format(fn))
            return ccl

        # From here on we know that the header is OK.
        
        # Set and check the number of rows.
        csv_baddata = None
        self.structure.num_rows = csv_summary["num_rows"]
        if (csv_summary["num_rows"] > max_row or csv_summary["num_rows"] < min_row):
            logging.debug("{}: bad number of rows".format(fn))
            bad_data = datachecking.models.BadData(contentchecklog = ccl, bad_num_rows=True)
            bad_data.save()

        if "failing_cells" in csv_summary:
            # Create a BadData object if it doesn't already exist.
            if csv_baddata == None:
                csv_baddata = datachecking.models.BadData(contentchecklog = ccl)
                csv_baddata.save()

            # row, col are indices.
            for row, col in csv_summary["failing_cells"]:
                fails = csv_summary["failing_cells"][(row,col)]
                for failed_constr in fails:
                    new_cell_error = csv_baddata.cell_errors.create(
                        row_num=row,
                        column=my_CDT.get(column_idx=col))

                    # If the failure is a string (e.g.  "Was not
                    # integer"), then leave constraint_failed as null.
                    if type(failed_constr) != str:
                        new_cell_error.constraint_failed = failed_constr

                    new_cell_error.clean()
                    new_cell_error.save()

        # Our CCL should now be complete.
        ccl.clean()
        return ccl

    def check_integrity(self, new_file_path, execlog,
                        newly_computed_MD5=None):
        """
        Checks integrity of this SD against the specified new file.

        If newly_computed_MD5 is not None, use it; otherwise, compute
        it from new_file.

        Return the newly-created IntegrityCheckLog object (which is
        linked to execlog).
        """
        if newly_computed_MD5 == None:
            with open(new_file_path, "rb") as f:
                newly_computed_MD5 = file_access_utils.compute_md5(f)
        
        icl = self.integrity_checks.create(execlog=execlog)
                
        if output_md5 != self.MD5_checksum:
            evil_twin = SymbolicDataset.create_SD(
                new_file_path, cdt=self.get_cdt(), user=self.user,
                name="{}eviltwin".format(self),
                description="MD5 conflictor of {}".format(self))

            icl.usurper.create(conflicting_SD=evil_twin)

        icl.clean()
        return icl
    
    def is_OK(self):
        """
        Check that this SD has been checked for integrity and contents,
        and that they have never failed either such test.
        """
        icls = self.integrity_checks.all()
        ccls = self.content_checks.all()
        if not icls.exists() or not ccls.exists():
            return False

        for icl in icls:
            if icl.is_fail():
                return False
        for ccl in ccls:
            if ccl.is_fail():
                return False

        # At this point, we are comfortable with this SD.
        return True
    
class DatasetStructure(models.Model):
    """
    Data with a Shipyard-compliant structure: a CSV file with a header.
    Encodes the CDT, and the transformation output generating this data.

    Related to :model:`librarian.SymbolicDataset`
    Related to :model:`metadata.CompoundDatatype`
    """
    # Note: previously we were tracking the exact TransformationOutput
    # this came from (both for its Run and its RunStep) but this is
    # now done more cleanly using ExecRecord.

    symbolicdataset = models.OneToOneField(
        SymbolicDataset,
        related_name="structure")

    compounddatatype = models.ForeignKey(
        "metadata.CompoundDatatype",
        related_name="conforming_datasets")

    # A value of -1 means that the file is missing or the number of
    # rows has never been counted (e.g. if we never did a ContentCheck
    # on it).
    num_rows = models.IntegerField(
        "number of rows",
        validators=[MinValueValidator(-1)],
        default=-1)

    # October 31, 2013: we now think that it's too onerous to have 
    # a clean() function here that opens up the CSV file and checks it.
    # Instead we will make it a precondition that any SymbolicDataset
    # that represents a CSV file has to have confirmed using
    # file_access_utils.summarize_CSV() that the CSV file is coherent.

    # At a later date, we might want to put in some kind of
    # "force_check()" which actually opens the file and makes sure its
    # contents are OK.


# November 15, 2013: changed to not refer to Pipelines anymore.
# Now the ExecRecord only captures *atomic* transformations.
class ExecRecord(models.Model):
    """
    Record of a previous execution of a Method/PipelineOutputCable/PSIC.

    This record is specific to using given inputs.
    """
    generator = models.ForeignKey("archive.ExecLog", related_name="execrecords")

    def __unicode__(self):
        """Unicode representation of this ExecRecord."""
        inputs_list = [unicode(eri) for eri in self.execrecordins.all()]
        outputs_list = [unicode(ero) for ero in self.execrecordouts.all()]

        string_rep = u""
        if type(self.general_transf()) == method.models.Method:
            string_rep = u"{}({}) = ({})".format(self.general_transf(),
                                                 u", ".join(inputs_list),
                                                 u", ".join(outputs_list))
        else:
            # Return a representation for a cable.
            string_rep = (u"{}".format(u", ".join(inputs_list)) +
                          " ={" + u"{}".format(self.general_transf()) + "}=> " +
                          u"{}".format(u", ".join(outputs_list)))
        return string_rep

    def clean(self):
        """
        Checks coherence of the ExecRecord.

        Calls clean on all of the in/outputs.  (Multiple quenching is
        checked via a uniqueness condition and does not need to be
        coded here.)

        If this ER represents a trivial cable, then the single ERI and
        ERO should have the same SymbolicDataset.
        """
        eris = self.execrecordins.all()
        eros = self.execrecordouts.all()

        for eri in eris:
            eri.clean()
        for ero in eros:
            ero.clean()

        if type(self.general_transf()) != method.models.Method:
            # If the cable is quenched:
            if eris.exists() and eros.exists():
                
                # If the cable is trivial, then the ERI and ERO should
                # have the same SymbolicDataset (if they both exist).
                if (self.general_transf().is_trivial() and
                        eris[0].symbolicdataset != eros[0].symbolicdataset):
                    raise ValidationError(
                        "ER \"{}\" represents a trivial cable but its input and output do not match".
                        format(self))

                # If the cable is not trivial and both sides have
                # data, then the column *Datatypes* on the destination
                # side are the same as the corresponding column on the
                # source side.  For example, if a CDT like (DNA col1,
                # int col2) is fed through a cable that maps col1 to
                # produce (string foo), then the actual Datatype of
                # the column in the corresponding Dataset would be
                # DNA.

                # Note that because the ERI and ERO are both clean,
                # and because we checked general_transf is not
                # trivial, we know that both have well-defined
                # DatasetStructures.
                elif not self.general_transf().is_trivial():
                    cable_wires = None
                    if type(self.general_transf()) == pipeline.models.PipelineStepInputCable:
                        cable_wires = self.general_transf().custom_wires.all()
                    else:
                        cable_wires = self.general_transf().custom_outwires.all()

                    source_CDT = (eris[0].symbolicdataset.structure.
                                  compounddatatype)
                    dest_CDT = (eros[0].symbolicdataset.structure.
                                compounddatatype)

                    for wire in cable_wires:
                        source_idx = wire.source_pin.column_idx
                        dest_idx = wire.dest_pin.column_idx
                        
                        dest_dt = dest_CDT.members.get(column_idx=dest_idx).datatype
                        source_dt = source_CDT.members.get(
                            column_idx=source_idx).datatype

                        if source_dt != dest_dt:
                            raise ValidationError(
                                "ExecRecord \"{}\" represents a cable but Datatype of destination Dataset column {} does not match its source".
                                format(self, dest_dt))
                    

    def complete_clean(self):
        """
        Checks completeness of the ExecRecord.

        Calls clean, and then checks that all in/outputs of the
        Method/POC/PSIC are quenched.
        """
        self.clean()

        # Because we know that each ERI is clean (and therefore each
        # one maps to a valid input of our Method/POC/PSIC), and
        # because there is no multiple quenching (due to a uniqueness
        # constraint), all we have to do is check the number of ERIs
        # to make sure everything is quenched.
        if type(self.general_transf()) in (
                pipeline.models.PipelineOutputCable,
                pipeline.models.PipelineStepInputCable
                ):
            # In this case we check that there is an input and an output.
            if not self.execrecordins.all().exists():
                raise ValidationError(
                    "Input to ExecRecord \"{}\" is not quenched".format(self))
            if not self.execrecordouts.all().exists():
                raise ValidationError(
                    "Output of ExecRecord \"{}\" is not quenched".format(self))

        else:
            if self.execrecordins.count() != self.general_transf().inputs.count():
                raise ValidationError(
                    "Input(s) to ExecRecord \"{}\" are not quenched".format(self))
        
            # Similar for EROs.
            if self.execrecordouts.count() != self.general_transf().outputs.count():
                raise ValidationError(
                    "Output(s) of ExecRecord \"{}\" are not quenched".format(self))

    def general_transf(self):
        """Returns the Method/POC/PSIC represented by this ExecRecord."""
        desired_transf = None
        generating_record = self.generator.record
        if type(generating_record) == archive.models.RunStep:
            desired_transf = generating_record.pipelinestep.transformation
        elif type(generating_record) == archive.models.RunSIC:
            desired_transf = generating_record.PSIC
        elif type(generating_record) == archive.models.RunOutputCable:
            desired_transf = generating_record.pipelineoutputcable

        return desired_transf

    def provides_outputs(self, outputs):
        """
        Checks whether this ER has existent data for these outputs.
        
        outputs is an iterable of TOs that we want the ER to have real
        data for.
        """    
        for curr_output in outputs:
            corresp_ero = self.execrecordouts.get(generic_output=curr_output)
            if not corresp_ero.has_data():
                return False
        return True

    def outputs_OK(self):
        """
        Checks whether all of the EROs of this ER are OK.
        """

        for ero in self.execrecordouts.all():
            if not ero.is_OK():
                return False
        return True
        
class ExecRecordIn(models.Model):
    """
    Denotes a symbolic input fed to the Method/POC/PSIC in the parent ExecRecord.

    The symbolic input may map to deleted data, e.g. if it was a deleted output
    of a previous step in a pipeline.
    """
    execrecord = models.ForeignKey(ExecRecord, help_text="Parent ExecRecord",
                                   related_name="execrecordins")
    symbolicdataset = models.ForeignKey(
        SymbolicDataset,
        help_text="Symbol for the dataset fed to this input")
    
    content_type = models.ForeignKey(
        ContentType,
        limit_choices_to = {
            "model__in":
            ("TransformationInput", "TransformationOutput")
        })
    object_id = models.PositiveIntegerField()
    # For a Method/Pipeline, this denotes the input that this ERI refers to;
    # for a cable, this denotes the thing that "feeds" it.
    generic_input = generic.GenericForeignKey("content_type", "object_id")

    class Meta:
        unique_together = ("execrecord", "content_type", "object_id");

    def __unicode__(self):
        """
        Unicode representation.
        
        If this ERI represents the source of a POC/PSIC, then it looks like
        [symbolic dataset]
        If it represents a TI, then it looks like
        [symbolic dataset]=>[transformation (raw) input name]
        
        Examples:
        S552
        S552=>foo_bar

        PRE: the parent ER must exist and be clean.
        """
        dest_name = "";

        if (type(self.execrecord.general_transf()) in
                (pipeline.models.PipelineOutputCable,
                 pipeline.models.PipelineStepInputCable)):
            return unicode(self.symbolicdataset)
        else:
            dest_name = self.generic_input.dataset_name

        return "{}=>{}".format(self.symbolicdataset, dest_name)
            

    def clean(self):
        """
        Checks coherence of this ExecRecordIn.

        Checks that generic_input is appropriate for the parent
        ExecRecord's Method/POC/PSIC.
        - If execrecord is for a POC, then generic_input should be the TO that
          feeds it (i.e. the PipelineStep TO that is cabled to a Pipeline output).
        - If execrecord is for a PSIC, then generic_input should be the TO or TI
          that feeds it (TO if it's from a previous step; TI if it's from a Pipeline
          input).
        - If execrecord is for a Method, then generic_input is the TI
          that this ERI represents.
          
        Also, if symbolicdataset refers to existent data, check that it
        is compatible with the input represented.
        """
        parent_transf = self.execrecord.general_transf()

        # If ER links to POC, ERI must link to TO which the outcable runs from.
        if type(parent_transf) == pipeline.models.PipelineOutputCable:
            if self.generic_input != parent_transf.source:
                raise ValidationError(
                    "ExecRecordIn \"{}\" does not denote the TO that feeds the parent ExecRecord POC".
                    format(self))
        # Similarly for a PSIC.
        elif type(parent_transf) == pipeline.models.PipelineStepInputCable:
            if self.generic_input != parent_transf.source:
                raise ValidationError(
                    "ExecRecordIn \"{}\" does not denote the TO/TI that feeds the parent ExecRecord PSIC".
                    format(self))

        else:
            # The ER represents a Method (not a cable).  Therefore the
            # ERI must refer to a TI of the parent ER's Method.
            if (type(self.generic_input) ==
                    transformation.models.TransformationOutput):
                raise ValidationError(
                    "ExecRecordIn \"{}\" must refer to a TI of the Method of the parent ExecRecord".
                    format(self))

            transf_inputs = parent_transf.inputs
            if not transf_inputs.filter(pk=self.generic_input.pk).exists():
                raise ValidationError(
                    "Input \"{}\" does not belong to Method of ExecRecord \"{}\"".
                    format(self.generic_input, self.execrecord))


        # The ERI's SymbolicDataset raw/unraw state must match the
        # raw/unraw state of the generic_input that it feeds it (if ER is a cable)
        # or that it is fed into (if ER is a Method).
        if self.generic_input.is_raw() != self.symbolicdataset.is_raw():
            raise ValidationError(
                "SymbolicDataset \"{}\" cannot feed source \"{}\"".
                format(self.symbolicdataset, self.generic_input))

        if not self.symbolicdataset.is_raw():
            transf_xput_used = self.generic_input
            cdt_needed = self.generic_input.get_cdt()
            input_SD = self.symbolicdataset

            # CDT of input_SD must be a restriction of cdt_needed,
            # i.e. we can feed it into cdt_needed.
            if not input_SD.structure.compounddatatype.is_restriction(
                    cdt_needed):
                raise ValidationError(
                    "CDT of SymbolicDataset \"{}\" is not a restriction of the required CDT".
                    format(input_SD))

            # Check row constraints.
            if (transf_xput_used.get_min_row() != None and
                    input_SD.num_rows() < transf_xput_used.get_min_row()):
                error_str = ""
                if type(self.generic_input) == transformation.models.TransformationOutput:
                    error_str = "SymbolicDataset \"{}\" has too few rows to have come from TransformationOutput \"{}\""
                else:
                    error_str = "SymbolicDataset \"{}\" has too few rows for TransformationInput \"{}\""
                raise ValidationError(error_str.format(input_SD, transf_xput_used))
                    
            if (transf_xput_used.get_max_row() != None and
                input_SD.num_rows() > transf_xput_used.get_max_row()):
                error_str = ""
                if type(self.generic_input) == transformation.models.TransformationOutput:
                    error_str = "SymbolicDataset \"{}\" has too many rows to have come from TransformationOutput \"{}\""
                else:
                    error_str = "SymbolicDataset \"{}\" has too many rows for TransformationInput \"{}\""
                raise ValidationError(error_str.format(input_SD, transf_xput_used))

    def is_OK(self):
        """Checks if the associated SymbolicDataset is OK."""
        return self.symbolicdataset.is_OK()

class ExecRecordOut(models.Model):
    """
    Denotes a symbolic output from the Method/PSIC/POC in the parent ExecRecord.

    The symbolic output may map to deleted data, i.e. if it was deleted after
    being generated.
    """
    execrecord = models.ForeignKey(ExecRecord, help_text="Parent ExecRecord",
                                   related_name="execrecordouts")
    symbolicdataset = models.ForeignKey(
        SymbolicDataset,
        help_text="Symbol for the dataset coming from this output",
        related_name="execrecordouts")

    content_type = models.ForeignKey(
        ContentType,
        limit_choices_to = {
            "model__in":
            ("TransformationInput", "TransformationOutput")
        })
    object_id = models.PositiveIntegerField()
    # For a Method/Pipeline this represents the TO that produces this output.
    # For a cable, this represents the TO (for a POC) or TI (for a PSIC) that
    # this cable feeds into.
    generic_output = generic.GenericForeignKey("content_type", "object_id")

    class Meta:
        unique_together = ("execrecord", "content_type", "object_id");

    def __unicode__(self):
        """
        Unicode representation of this ExecRecordOut.

        If this ERO represented the output of a PipelineOutputCable, then this looks like
        [symbolic dataset]
        If it represents the input that a PSIC feeds into, then it looks like
        [symbolic dataset]
        Otherwise, it represents a TransformationOutput, and this looks like
        [TO name]=>[symbolic dataset]
        e.g.
        S458
        output_one=>S458
        """
        unicode_rep = u""
        if (type(self.execrecord.general_transf()) in
                (pipeline.models.PipelineOutputCable,
                 pipeline.models.PipelineStepInputCable)):
            unicode_rep = unicode(self.symbolicdataset)
        else:
            unicode_rep = u"{}=>{}".format(self.generic_output.dataset_name,
                                           self.symbolicdataset)
        return unicode_rep


    def clean(self):
        """
        If ER represents a POC, check output defined by the POC.
        If ER represents a PSIC, check output is the TI the cable feeds.
        If ER is not a cable, check output belongs to ER's Method.
        The SD is compatible with generic_output. (??)
        """

        import inspect
        fn = "{}.{}()".format(self.__class__.__name__, inspect.stack()[0][3])
        import logging

        # If the parent ER is linked with POC, the corresponding ERO TO must be coherent
        if (type(self.execrecord.general_transf()) ==
                pipeline.models.PipelineOutputCable):
            parent_er_outcable = self.execrecord.general_transf()

            # ERO TO must belong to the same pipeline as the ER POC
            if self.generic_output.transformation != parent_er_outcable.pipeline:
                raise ValidationError(
                    "ExecRecordOut \"{}\" does not belong to the same pipeline as its parent ExecRecord POC".
                    format(self))

            # And the POC defined output name must match the pipeline TO name
            if parent_er_outcable.output_name != self.generic_output.dataset_name:
                raise ValidationError(
                    "ExecRecordOut \"{}\" does not represent the same output as its parent ExecRecord POC".
                    format(self))

        # Second case: parent ER represents a PSIC.
        elif (type (self.execrecord.general_transf()) ==
              pipeline.models.PipelineStepInputCable):
            parent_er_psic = self.execrecord.general_transf()

            # This ERO must point to a TI.
            if (type(self.generic_output) !=
                    transformation.models.TransformationInput):
                raise ValidationError(
                    "Parent of ExecRecordOut \"{}\" represents a PSIC; ERO must be a TransformationInput".
                    format(self))

            # The TI this ERO points to must be the one fed by the PSIC.
            if parent_er_psic.dest != self.generic_output:
                raise ValidationError(
                    "Input \"{}\" is not the one fed by the PSIC of ExecRecord \"{}\"".
                    format(self.generic_output, self.execrecord))

        # Else the parent ER is linked with a method
        else:
            query_for_outs = self.execrecord.general_transf().outputs

            # The ERO output TO must be a member of the ER's method/pipeline
            if not query_for_outs.filter(pk=self.generic_output.pk).exists():
                raise ValidationError(
                    "Output \"{}\" does not belong to Method/Pipeline of ExecRecord \"{}\"".
                    format(self.generic_output, self.execrecord))

        # Check that the SD is compatible with generic_output.

        logging.debug("{}: ERO SD is raw? {}".format(fn, self.symbolicdataset.is_raw()))
        logging.debug("{}: ERO generic_output is raw? {}".format(fn, self.generic_output.is_raw()))

        # If SD is raw, the ERO output TO must also be raw
        if self.symbolicdataset.is_raw() != self.generic_output.is_raw():
            if type(self.generic_output) == pipeline.models.PipelineStepInputCable:
                raise ValidationError(
                    "SymbolicDataset \"{}\" cannot feed input \"{}\"".
                    format(self.symbolicdataset, self.generic_output))
            else:
                raise ValidationError(
                    "SymbolicDataset \"{}\" cannot have come from output \"{}\"".
                    format(self.symbolicdataset, self.generic_output))



        # The SD must satisfy the CDT / row constraints of the producing TO
        # (in the Method/Pipeline/POC case) or of the TI fed (in the PSIC case).
        if not self.symbolicdataset.is_raw():
            input_SD = self.symbolicdataset

            # If this execrecord refers to a Method, the SD CDT
            # must *exactly* be generic_output's CDT since it was
            # generated by this Method.
            if type(self.execrecord.general_transf()) == method.models.Method:
                if (input_SD.structure.compounddatatype !=
                        self.generic_output.get_cdt()):
                    raise ValidationError(
                        "CDT of SymbolicDataset \"{}\" is not the CDT of the TransformationOutput \"{}\" of the generating Method".
                        format(input_SD, self.generic_output))

            # If it refers to a POC, then SD CDT must be
            # identical to generic_output's CDT, because it was
            # generated either by this POC or by a compatible one,
            # and compatible ones must have a CDT identical to
            # this one.
            elif (type(self.execrecord.general_transf()) ==
                      pipeline.models.PipelineOutputCable):
                if not input_SD.structure.compounddatatype.is_identical(
                        self.generic_output.get_cdt()):
                    raise ValidationError(
                        "CDT of SymbolicDataset \"{}\" is not identical to the CDT of the TransformationOutput \"{}\" of the generating Pipeline".
                        format(input_SD, self.generic_output))
                    
            # If it refers to a PSIC, then SD CDT must be a
            # restriction of generic_output's CDT.
            else:
                if not input_SD.structure.compounddatatype.is_restriction(
                        self.generic_output.get_cdt()):
                    raise ValidationError(
                        "CDT of SymbolicDataset \"{}\" is not a restriction of the CDT of the fed TransformationInput \"{}\"".
                        format(input_SD, self.generic_output))

            if (self.generic_output.get_min_row() != None and
                    input_SD.num_rows() < self.generic_output.get_min_row()):
                if (type(self.execrecord.general_transf()) ==
                        pipeline.models.PipelineStepInputCable):
                    raise ValidationError(
                        "SymbolicDataset \"{}\" feeds TransformationInput \"{}\" but has too few rows".
                        format(input_SD, self.generic_output))
                else:
                    raise ValidationError(
                        "SymbolicDataset \"{}\" was produced by TransformationOutput \"{}\" but has too few rows".
                        format(input_SD, self.generic_output))

            if (self.generic_output.get_max_row() != None and 
                    input_SD.num_rows() > self.generic_output.get_max_row()):
                if (type(self.execrecord.general_transf()) ==
                        pipeline.models.PipelineStepInputCable):
                    raise ValidationError(
                        "SymbolicDataset \"{}\" feeds TransformationInput \"{}\" but has too many rows".
                        format(input_SD, self.generic_output))
                else:
                    raise ValidationError(
                        "SymbolicDataset \"{}\" was produced by TransformationOutput \"{}\" but has too many rows".
                        format(input_SD, self.generic_output))

    def has_data(self):
        """True if associated Dataset exists; False otherwise."""
        return self.symbolicdataset.has_data()

    def is_OK(self):
        """Checks if the associated SymbolicDataset is OK."""
        return self.symbolicdataset.is_OK()

