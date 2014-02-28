"""
pipeline.models

Shipyard data models relating to the (abstract) definition of
Pipeline.
"""

from django.db import models
from django.contrib.contenttypes.models import ContentType
from django.contrib.contenttypes import generic
from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator
from django.db import transaction
from django.utils import timezone

import os
import csv
import shutil
import logging
import archive.models, librarian.models, metadata.models, method.models, transformation.models

class PipelineFamily(transformation.models.TransformationFamily):
    """
    PipelineFamily groups revisions of Pipelines together.

    Inherits :model:`transformation.TransformationFamily`
    Related to :model:`pipeline.Pipeline`
    """

    # Implicitly defined:
    #   members (Pipeline/ForeignKey)

    pass



class Pipeline(transformation.models.Transformation):
    """
    A particular pipeline revision.

    Inherits from :model:`transformation.models.Transformation`
    Related to :model:`pipeline.models.PipelineFamily`
    Related to :model:`pipeline.models.PipelineStep`
    Related to :model:`pipeline.models.PipelineOutputCable`
    """

    family = models.ForeignKey(
        PipelineFamily,
        related_name="members")

    revision_parent = models.ForeignKey(
        "self",
        related_name = "descendants",
        null=True,
        blank=True)

    def __unicode__(self):
        """Represent pipeline by revision name and pipeline family"""

        string_rep = u"Pipeline {} {}".format("{}", self.revision_name)

        # If family isn't set (if created from family admin page)
        if hasattr(self, "family"):
            string_rep = string_rep.format(unicode(self.family))
        else:
            string_rep = string_rep.format("[family unset]")

        return string_rep

    def clean(self):
        """
        Validate pipeline revision inputs/outputs

        - Pipeline INPUTS must be consecutively numbered from 1
        - Pipeline STEPS must be consecutively starting from 1
        - Steps are clean
        - PipelineOutputCables are appropriately mapped from the pipeline's steps
        """
        # Transformation.clean() - check for consecutive numbering of
        # input/outputs for this pipeline as a whole
        super(self.__class__, self).clean();

        # Internal pipeline STEP numbers must be consecutive from 1 to n
        all_steps = self.steps.all();
        step_nums = [];

        for step in all_steps:
            step_nums += [step.step_num];

        if sorted(step_nums) != range(1, len(all_steps)+1):
            raise ValidationError(
                "Steps are not consecutively numbered starting from 1");

        # Check that steps are clean; this also checks the cabling between steps.
        # Note: we don't call *complete_clean* because this may refer to a
        # "transient" state of the Pipeline whereby it is not complete yet.
        for step in all_steps:
            step.clean();

        # Check pipeline output wiring for coherence
        output_indices = [];
        output_names = [];

        # Validate each PipelineOutput(Raw)Cable
        for outcable in self.outcables.all():
            outcable.clean()
            output_indices += [outcable.output_idx];
            output_names += [outcable.output_name];

        # PipelineOutputCables must be numbered consecutively
        if sorted(output_indices) != range(1, self.outcables.count()+1):
            raise ValidationError(
                "Outputs are not consecutively numbered starting from 1");

    def complete_clean(self):
        """
        Check that the pipeline is both coherent and complete.

        Coherence is checked using clean(); the tests for completeness are:
        - there is at least 1 step
        - steps are complete, not just clean
        """
        self.clean();
        
        all_steps = self.steps.all();
        if all_steps.count == 0:
            raise ValidationError("Pipeline {} has no steps".format(unicode(self)));

        for step in all_steps:
            step.complete_clean();

    def create_outputs(self):
        """
        Delete existing pipeline outputs, and recreate them from output cables.

        PRE: this should only be called after the pipeline has been verified by
        clean and the outcables are known to be OK.
        """
        # Be careful if customizing delete() of TransformationOutput.
        self.outputs.all().delete()

        # outcables is derived from (PipelineOutputCable/ForeignKey).
        # For each outcable, extract the cabling parameters.
        for outcable in self.outcables.all():
            output_requested = outcable.source

            new_pipeline_output = self.outputs.create(
                dataset_name=outcable.output_name,
                dataset_idx=outcable.output_idx)

            if not outcable.is_raw():
                # Define an XputStructure for new_pipeline_output.
                new_pipeline_output.structure.create(
                    compounddatatype=outcable.output_cdt,
                    min_row=output_requested.get_min_row(),
                    max_row=output_requested.get_max_row())

    # Helper to create raw outcables.  This is just so that our unit tests
    # can be easily amended to work in our new scheme, and wouldn't really
    # be used elsewhere.
    @transaction.atomic
    def create_raw_outcable(self, raw_output_name, raw_output_idx,
                            source_step, source):
        """Creates a raw outcable."""
        new_outcable = self.outcables.create(
            output_name=raw_output_name,
            output_idx=raw_output_idx,
            source_step=source_step,
            source=source)
        new_outcable.full_clean()

        return new_outcable

    # Helper to create non-raw outcables with a default output_cdt equalling
    # that of the providing TO.
    @transaction.atomic
    def create_outcable(self, output_name, output_idx, source_step,
                        source):
        """Creates a non-raw outcable taking output_cdt from the providing TO."""
        new_outcable = self.outcables.create(
            output_name=output_name,
            output_idx=output_idx,
            source_step=source_step,
            source=source,
            output_cdt=source.get_cdt())
        new_outcable.full_clean()

        return new_outcable

class PipelineStep(models.Model):
    """
    A step within a Pipeline representing a single transformation
    operating on inputs that are either pre-loaded (Pipeline inputs)
    or derived from previous pipeline steps within the same pipeline.

    Related to :model:`archive.models.Dataset`
    Related to :model:`pipeline.models.Pipeline`
    Related to :model:`transformation.models.Transformation`
    Related to :model:`pipeline.models.PipelineStepInput`
    Related to :model:`pipeline.models.PipelineStepDelete`
    """
    pipeline = models.ForeignKey(
            Pipeline,
            related_name="steps");

    # Pipeline steps are associated with a transformation
    content_type = models.ForeignKey(
            ContentType,
            limit_choices_to = {"model__in": ("method", "pipeline")});

    object_id = models.PositiveIntegerField();
    transformation = generic.GenericForeignKey("content_type", "object_id");
    step_num = models.PositiveIntegerField(validators=[MinValueValidator(1)]);

    # Which outputs of this step we want to delete.
    # Previously, this was done via another explicit class (PipelineStepDelete);
    # this is more compact.
    # -- August 21, 2013
    outputs_to_delete = models.ManyToManyField(
        "transformation.TransformationOutput",
        help_text="TransformationOutputs whose data should not be retained",
        related_name="pipeline_steps_deleting")

    def __unicode__(self):
        """ Represent with the pipeline and step number """

        pipeline_name = "[no pipeline assigned]";   
        if hasattr(self, "pipeline"):
            pipeline_name = unicode(self.pipeline);
        return "{} step {}".format(pipeline_name, self.step_num);


    def recursive_pipeline_check(self, pipeline):
        """Given a pipeline, check if this step contains it.

        PRECONDITION: the transformation at this step has been appropriately
        cleaned and does not contain any circularities.  If it does this
        function can be fragile!
        """
        contains_pipeline = False;

        # Base case 1: the transformation is a method and can't possibly contain the pipeline.
        if type(self.transformation) == method.models.Method:
            contains_pipeline = False;

        # Base case 2: this step's transformation exactly equals the pipeline specified
        elif self.transformation == pipeline:
            contains_pipeline = True;

        # Recursive case: go through all of the target pipeline steps and check if
        # any substeps exactly equal the transformation: if it does, we have circular pipeline references
        else:
            transf_steps = self.transformation.steps.all();
            for step in transf_steps:
                step_contains_pipeline = step.recursive_pipeline_check(pipeline);
                if step_contains_pipeline:
                    contains_pipeline = True;
        return contains_pipeline;

    def clean(self):
        """
        Check coherence of this step of the pipeline.

        - Does the transformation at this step contain the parent pipeline?
        - Are any inputs multiply-cabled?
        
        Also, validate each input cable, and each specified output deletion.

        A PipelineStep must be save()d before cables can be connected to
        it, but it should be clean before being saved. Therefore, this
        checks coherency rather than completeness, for which we call
        complete_clean() - such as cabling.
        """
        # Check recursively to see if this step's transformation contains
        # the specified pipeline at all.
        if self.recursive_pipeline_check(self.pipeline):
            raise ValidationError("Step {} contains the parent pipeline".
                                  format(self.step_num));

        # Check for multiple cabling to any of the step's inputs.
        for transformation_input in self.transformation.inputs.all():
            num_matches = self.cables_in.filter(dest=transformation_input).count()
            if num_matches > 1:
                raise ValidationError(
                    "Input \"{}\" to transformation at step {} is cabled more than once".
                    format(transformation_input.dataset_name, self.step_num))

        # Validate each cable (Even though we call PS.clean(), we want complete wires)
        for curr_cable in self.cables_in.all():
            curr_cable.clean_and_completely_wired()

        # Validate each PipelineStep output deletion
        for curr_del in self.outputs_to_delete.all():
            curr_del.clean()

        # Note that outputs_to_delete takes care of multiple deletions
        # (if a TO is marked for deletion several times, it will only
        # appear once anyway).  All that remains to check is that the
        # TOs all belong to the transformation at this step.
        for otd in self.outputs_to_delete.all():
            if not self.transformation.outputs.filter(pk=otd.pk).exists():
                raise ValidationError(
                    "Transformation at step {} does not have output \"{}\"".
                    format(self.step_num, otd));

    def complete_clean(self):
        """Executed after the step's wiring has been fully defined, and
        to see if all inputs are quenched exactly once.
        """
        self.clean()
            
        for transformation_input in self.transformation.inputs.all():
            # See if the input is specified more than 0 times (and
            # since clean() was called above, we know that therefore
            # it was specified exactly 1 time).
            num_matches = self.cables_in.filter(dest=transformation_input).count()
            if num_matches == 0:
                raise ValidationError(
                    "Input \"{}\" to transformation at step {} is not cabled".
                    format(transformation_input.dataset_name, self.step_num))

    # Helper to create *raw* cables.  This is really just so that all our
    # unit tests can be easily amended; going forwards, there's no real reason
    # to use this.
    @transaction.atomic
    def create_raw_cable(self, dest, source):
        """
        Create a raw cable feeding this PipelineStep.
        """
        new_cable = self.cables_in.create(
            dest=dest,
            source_step=0,
            source=source)
        # FIXME August 23, 2013:
        # Django is barfing on clean_fields.  Seems like this is a problem
        # with GenericForeignKeys, as this affected Transformation.create_input
        # and Transformation.create_output.
        # new_cable.full_clean()
        # new_cable.clean_fields()
        new_cable.clean()
        new_cable.validate_unique()
        return new_cable

    # Same for deletes.
    @transaction.atomic
    def add_deletion(self, output_to_delete):
        """
        Mark a TO for deletion.
        """
        self.outputs_to_delete.add(output_to_delete)
        
    def outputs_to_retain(self):
        """Returns a list of TOs this PipelineStep doesn't delete."""
        outputs_needed = []

        # Checking each TO of this PS and maintain TOs marked to be deleted
        for step_output in self.transformation.outputs.all():

            # Check if for this pipeline step we want to delete TO step_output
            if not self.outputs_to_delete.filter(pk=step_output.pk).exists():
                outputs_needed.append(step_output)
                
        return outputs_needed


# A helper function that will be called both by PSICs and
# POCs to tell whether they are trivial.
def cable_trivial_h(cable, cable_wires):
    """
    Helper called by PSICs and POCs to check triviality.

    INPUTS: cable_wires is a QuerySet containing cable's custom wires.

    Definition of trivial:
    1) All raw cables
    2) Cables without wiring
    3) Cables with wiring that doesn't change name/idx

    PRE: cable is clean
    """
    if cable.is_raw():
        return True
        
    if not cable_wires.exists():
        return True

    for wire in cable_wires:
        if (wire.source_pin.column_idx != wire.dest_pin.column_idx or
                wire.source_pin.column_name != wire.dest_pin.column_name):
            return False

    return True


# Helper that will be called by both PSIC and POC.
def run_cable_h(cable, source, output_path):
    """
    Perform cable transformation on the input.

    wires is the QuerySet containing wires for this cable.
    """
    logger = cable.logger

    wires = ""
    if (type(cable).__name__ == "PipelineOutputCable"):
        wires = cable.custom_outwires.all()
    else:
        wires = cable.custom_wires.all()

    if type(source) == str and cable.is_trivial():
        logger.debug("Cable source is a file path")
        logger.debug("Trivial cable, making sym link: os.link({},{})".format(source, output_path))
        os.link(source, output_path)
        return

    if type(source) == archive.models.Dataset and cable.is_trivial():
        logger.debug("Cable source is a dataset object")
        logger.debug("Trivial cable: writing dataset to the file system")
        shutil.copyfile(source.dataset_file.name, output_path)
        return
        
    # Make a dict encapsulating the mapping required: keyed by the output column name, with value
    # being the input column name.
    source_of = {}
    column_names_by_idx = {}

    mappings = ""
    for wire in wires:
        mappings += "{} wires to {}   ".format(wire.source_pin, wire.dest_pin)
        source_of[wire.dest_pin.column_name] = wire.source_pin.column_name
        column_names_by_idx[wire.dest_pin.column_idx] = wire.dest_pin.column_name

    logger.debug("Nontrivial cable. {}".format(mappings))

    # Construct a list with the column names in the appropriate order.
    output_fields = [column_names_by_idx[i] for i in sorted(column_names_by_idx)]

    try:
        if type(source) == archive.models.Dataset:
            infile = source.dataset_file
            infile.open()
        elif type(source) == str:
            infile = open(source, "rb")

        input_csv = csv.DictReader(infile)

        with open(output_path, "wb") as outfile:
            output_csv = csv.DictWriter(outfile,fieldnames=output_fields)
            output_csv.writeheader()
            
            for source_row in input_csv:
                # row = {col1name: col1val, col2name: col2val, ...}
                dest_row = {}

                # source_of = {outcol1: sourcecol5, outcol2: sourcecol1, ...}
                for out_col_name in source_of:
                    dest_row[out_col_name] = source_row[source_of[out_col_name]]

                output_csv.writerow(dest_row)

    finally:
        infile.close()

class PipelineStepInputCable(models.Model):
    """
    Represents the "cables" feeding into the transformation of a
    particular pipeline step, specifically:

    A) Destination of cable - step implicitly defined
    B) Source of the cable (source_step, source)

    Related to :model:`pipeline.models.PipelineStep`
    """
    # The step (Which has a transformation) where we define incoming cabling
    pipelinestep = models.ForeignKey(
        PipelineStep,
        related_name = "cables_in");
    
    # Input hole (TransformationInput) of the transformation
    # at this step to which the cable leads
    dest = models.ForeignKey(
        "transformation.TransformationInput",
        help_text="Wiring destination input hole");
    
    # (source_step, source) unambiguously defines
    # the source of the cable.  source_step can't refer to a PipelineStep
    # as it might also refer to the pipeline's inputs (i.e. step 0).
    source_step = models.PositiveIntegerField("Step providing the input source",
                                              help_text="Cabling source step");

    content_type = models.ForeignKey(
            ContentType,
            limit_choices_to = {"model__in": ("TransformationOutput",
                                              "TransformationInput")});
    object_id = models.PositiveIntegerField();
    # Wiring source output hole.
    source = generic.GenericForeignKey("content_type", "object_id");

    custom_wires = generic.GenericRelation("CustomCableWire")

    # October 15, 2013: allow the data coming out of a PSIC to be
    # saved.  Note that this is only relevant if the PSIC is not
    # trivial, and is false by default.
    keep_output = models.BooleanField(
        "Whether or not to retain the output of this PSIC",
        help_text="Keep or delete output",
        default=False)

    # source_step must be PRIOR to this step (Time moves forward)

    # Coherence of data is already enforced by Pipeline

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)

    def __unicode__(self):
        """
        Represent PipelineStepInputCable with the pipeline step, and the cabling destination input name.

        If cable is raw, this will look like:
        [PS]:[input name](raw)
        If not:
        [PS]:[input name]
        """
        step_str = "[no pipeline step set]"
        is_raw_str = ""
        if self.pipelinestep != None:
            step_str = unicode(self.pipelinestep)
        if self.is_raw():
            is_raw_str = "(raw)"
        return "{}:{}{}".format(step_str, self.dest.dataset_name, is_raw_str);

    
    def clean(self):
        """
        Check coherence of the cable.

        Check in all cases:
        - Are the source and destination either both raw or both
          non-raw?
        - Does the source come from a prior step or from the Pipeline?
        - Does the cable map to an (existent) input of this step's transformation?
        - Does the requested source exist?

        If the cable is raw:
        - Are there any wires defined?  (There shouldn't be!)

        If the cable is not raw:
        - Do the source and destination 'work together' (compatible min/max)?

        Whether the input and output have compatible CDTs or have valid custom
        wiring is checked via clean_and_completely_wired.
        """
        if self.source.is_raw() != self.dest.is_raw():
            raise ValidationError(
                "Cable \"{}\" has mismatched source (\"{}\") and destination (\"{}\")".
                format(self, self.source, self.dest))

        # input_requested = self.source;
        # requested_from = self.source_step;
        # feed_to_input = self.dest;
        # step_trans = self.pipelinestep.transformation

        # Does the source come from a step prior to this one?
        if self.source_step >= self.pipelinestep.step_num:
            raise ValidationError(
                "Step {} requests input from a later step".
                format(self.pipelinestep.step_num));

        # Does the specified input defined for this transformation exist?
        if not self.pipelinestep.transformation.inputs.filter(
                pk=self.dest.pk).exists():
            raise ValidationError(
                "Transformation at step {} does not have input \"{}\"".
                format(self.pipelinestep.step_num, unicode(self.dest)));

        # Check that the source is available.
        if self.source_step == 0:
            # Look for the desired input among the Pipeline inputs.
            pipeline_inputs = self.pipelinestep.pipeline.inputs.all();
            if self.source not in pipeline_inputs:
                raise ValidationError(
                    "Pipeline does not have input \"{}\"".
                    format(unicode(self.source)));

        # If not from step 0, input derives from the output of a pipeline step
        else:
            # Look for the desired input among this PS' inputs.
            source_ps = self.pipelinestep.pipeline.steps.get(
                step_num=self.source_step)

            source_ps_outputs = source_ps.transformation.outputs.all()
            if self.source not in source_ps_outputs:
                raise ValidationError(
                    "Transformation at step {} does not produce output \"{}\"".
                    format(self.source_step,
                           unicode(self.source)))
        
        # Propagate to more specific clean functions.
        if self.is_raw():
            self.raw_clean()
        else:
            self.non_raw_clean()

    def raw_clean(self):
        """
        Helper function called by clean() to deal with raw cables.
        
        PRE: the pipeline step's transformation is not the parent
        pipeline (this should never happen anyway).
        PRE: cable is raw (i.e. the source and destination are both
        raw); this is enforced by clean().
        """
        # Are there any wires defined?
        if self.custom_wires.all().exists():
            raise ValidationError(
                "Cable \"{}\" is raw and should not have custom wiring defined".
                format(self))

    def non_raw_clean(self):
        """Helper function called by clean() to deal with non-raw cables."""
        # Check that the input and output connected by the
        # cable are compatible re: number of rows.  Don't check for
        # ValidationError because this was checked in the
        # clean() of PipelineStep.

        # These are source and destination row constraints.
        source_min_row = (0 if self.source.get_min_row() == None
                          else self.source.get_min_row())
        dest_min_row = (0 if self.dest.get_min_row() == None
                        else self.dest.get_min_row())

        # Check for contradictory min row constraints
        if (source_min_row < dest_min_row):
            raise ValidationError(
                "Data fed to input \"{}\" of step {} may have too few rows".
                format(self.dest.dataset_name, self.pipelinestep.step_num))

        # Similarly, these are max-row constraints.
        source_max_row = (float("inf") if self.source.get_max_row() == None
                          else self.source.get_max_row())
        dest_max_row = (float("inf") if self.dest.get_max_row() == None
                        else self.dest.get_max_row())

        # Check for contradictory max row constraints
        if (source_max_row > dest_max_row):
            raise ValidationError(
                "Data fed to input \"{}\" of step {} may have too many rows".
                format(self.dest.dataset_name, self.pipelinestep.step_num))

        # Validate whatever wires there already are
        if self.custom_wires.all().exists():
            for wire in self.custom_wires.all():
                wire.clean()
        
    def clean_and_completely_wired(self):
        """
        Check coherence and wiring of this cable (if it is non-raw).

        This will call clean() as well as checking whether the input
        and output 'work together'.  That is, either both are raw, or
        neither are non-raw and:
         - the source CDT is a restriction of the destination CDT; or
         - there is good wiring defined.
        """
        # Check coherence of this cable otherwise.
        self.clean()

        # There are no checks to be done on wiring if this is a raw cable.
        if self.is_raw():
            return
        
        # If source CDT cannot feed (i.e. is not a restriction of)
        # destination CDT, check presence of custom wiring
        if not self.source.get_cdt().is_restriction(self.dest.get_cdt()):
            if not self.custom_wires.all().exists():
                raise ValidationError(
                    "Custom wiring required for cable \"{}\"".
                    format(unicode(self)))

        # Validate whatever wires there are.
        if self.custom_wires.all().exists():
            # Each destination CDT member of must be wired to exactly once.

            # Get the CDT members of dest.
            dest_members = self.dest.get_cdt().members.all()

            # For each CDT member, check that there is exactly 1
            # custom_wire leading to it (i.e. number of occurrences of
            # CDT member = dest_pin).
            for dest_member in dest_members:
                numwires = self.custom_wires.filter(dest_pin=dest_member).count()

                if numwires == 0:
                    raise ValidationError(
                        "Destination member \"{}\" has no wires leading to it".
                        format(unicode(dest_member)))

                if numwires > 1:
                    raise ValidationError(
                        "Destination member \"{}\" has multiple wires leading to it".
                        format(unicode(dest_member)))

    def is_raw(self):
        """True if this cable maps raw data; false otherwise."""
        return self.dest.is_raw()

    def is_trivial(self):
        """
        True if this cable is trivial; False otherwise.
        
        If a cable is raw, it is trivial.  If it is not raw, then it
        is trivial if it either has no wiring, or if the wiring is
        trivial (i.e. mapping corresponding pin to corresponding pin
        without changing names or anything).

        PRE: cable is clean.
        """
        return cable_trivial_h(self, self.custom_wires.all())

    def is_restriction(self, other_cable):
        """
        Returns whether this cable is a restriction of the specified.

        More specifically, this cable is a restriction of the
        parameter if they feed the same TransformationInput and, if
        they are not raw:
         - source CDT is a restriction of parameter's source CDT
         - wiring matches

        PRE: both self and other_cable are clean.
        """
        # Trivial case.
        if self == other_cable:
            return True

        if self.dest != other_cable.dest:
            return False

        # Now we know that they feed the same TransformationInput.
        if self.is_raw():
            return True

        # From here on, we assume both cables are non-raw.
        # (They must be, since both feed the same TI and self
        # is not raw.)
        if not self.source.get_cdt().is_restriction(
                other_cable.source.get_cdt()):
            return False

        # If there is non-trivial custom wiring on either, then
        # the wiring must match.
        if self.is_trivial() and other_cable.is_trivial():
            return True
        elif self.is_trivial() != other_cable.is_trivial():
            return False

        # Now we know that both have non-trivial wiring.  Check both
        # cables' wires and see if they connect corresponding pins.
        # (We already know they feed the same TransformationInput,
        # so we only have to check the indices.)
        for wire in self.custom_wires.all():
            corresp_wire = other_cable.custom_wires.get(
                dest_pin=wire.dest_pin)
            if (wire.source_pin.column_idx !=
                    corresp_wire.source_pin.column_idx):
                return False

        # Having reached this point, we know that the wiring matches.
        return True

    def is_compatible(self, other_cable, source_CDT):
        """
        Checks if a cable is compatible wrt specified CDT.
        
        Cables are compatible if:
         - Both can be fed by source_CDT
         - Both feed the same TransformationInput
         - Both are trivial, or the wiring matches
        
        For two cables' wires to match, any wire connecting column
        indices (source_idx, dest_idx) must appear in both cables.

        PRE: self, other_cable are clean.
        """
        # Both cables can be fed by source_CDT if source_CDT is a restriction of their CDTs
        other_CDT = other_cable.source.get_cdt()

        if not source_CDT.is_restriction(source_CDT) or not source_CDT.is_restriction(other_CDT):
            return False
        
        # After this point, all checks are the same as for is_compatible_given_input
        return self.is_compatible_given_input(other_cable)

    def is_compatible_given_input(self, other_cable):
        """
        Check compatibility of two cables having the same input.

        Given that both had the same input, they are compatible if:
         - both feed the same TransformationInput
         - both are trivial, or the wiring matches
        
        For two cables' wires to match, any wire connecting column
        indices (source_idx, dest_idx) must appear in both cables.

        PRE: self, other_cable are clean, and both can be fed the
        same input SymbolicDataset.
        """
        # Both cables can be fed by source_CDT if source_CDT is
        # a restriction of their sources' CDTs.
        if self.dest != other_cable.dest:
            return False

        if self.is_trivial() and other_cable.is_trivial():
            return True

        # We know they aren't trivial at this point, so check wiring.
        for wire in self.custom_wires.all():
            # Get the corresponding wire in other_cable.
            corresp_wire = other_cable.custom_wires.get(
                dest_pin=wire.dest_pin)

            if (wire.source_pin.column_idx !=
                    corresp_wire.source_pin.column_idx):
                return False

        # By the fact that self and other_cable are clean, we know
        # that we have checked all the wires.  Having made sure all of
        # the wiring matches, we can....
        return True

    def run_cable(self, source, output_path, cable_record):
        """
        Perform cable transformation on the input.
        Creates an ExecLog, associating it to cable_record.
        Source can either be a Dataset or a path to a file.

        INPUTS
        source          Either the Dataset to run through the cable, or a file path containing the data.
        output_path
        cable_record    RSIC/ROC for this step.

        OUTPUT
        curr_log        The exec log created while executing.
        """

        import inspect
        fn = "{}.{}()".format(self.__class__.__name__, inspect.stack()[0][3])

        # Create a new log with the current start_time and a null end_time
        self.logger.debug("Creating ExecLog and calling run_cable_h(source='{}', output_path='{}'".format(source,output_path))
        curr_log = archive.models.ExecLog(record=cable_record)
        curr_log.save()
        curr_log.start_time = timezone.now()

        run_cable_h(self, source, output_path)

        # Now give it the correct end_time
        curr_log.end_time = timezone.now()
        curr_log.complete_clean()
        curr_log.save()
        return curr_log

class CustomCableWire(models.Model):
    """
    Defines a customized connection within a pipeline.

    This allows us to filter/rearrange/repeat columns when handing
    data from a source TransformationXput to a destination Xput

    The analogue here is that we have customized a cable by rearranging
    the connections between the pins.
    """
    content_type = models.ForeignKey(
        ContentType,
        limit_choices_to = {"model__in": ("PipelineOutputCable", "PipelineStepInputCable")});
    object_id = models.PositiveIntegerField();
    cable = generic.GenericForeignKey("content_type", "object_id")

    # CDT member on the source output hole
    source_pin = models.ForeignKey(
        "metadata.CompoundDatatypeMember",
        related_name="source_pins")

    # CDT member on the destination input hole
    dest_pin = models.ForeignKey(
        "metadata.CompoundDatatypeMember",
        related_name="dest_pins")

    # A cable cannot have multiple wires leading to the same dest_pin
    class Meta:
        unique_together = ("content_type","object_id", "dest_pin")

    def clean(self):
        """
        Check the validity of this wire.

        The wire belongs to a cable which connects a source TransformationXput
        and a destination TransformationInput:
        - wires cannot connect a raw source or a raw destination
        - source_pin must be a member of the source CDT
        - dest_pin must be a member of the destination CDT
        - source_pin datatype matches the dest_pin datatype
        """

        # You cannot add a wire if the cable is raw
        if self.cable.is_raw():
            raise ValidationError(
                "Cable \"{}\" is raw and should not have wires defined" .
                format(self.cable))

        # Wires connect either PSIC or POCs, so these cases are separate
        source_CDT_members = self.cable.source.get_cdt().members.all() # Duck-typing
        dest_CDT = None
        dest_CDT_members = None
        if type(self.cable) == PipelineStepInputCable:
            dest_CDT = self.cable.dest.get_cdt()
            dest_CDT_members = dest_CDT.members.all()
        else:
            dest_CDT = self.cable.output_cdt
            dest_CDT_members = dest_CDT.members.all()

        if not source_CDT_members.filter(pk=self.source_pin.pk).exists():
            raise ValidationError(
                "Source pin \"{}\" does not come from compounddatatype \"{}\"".
                format(self.source_pin,
                       self.cable.source.get_cdt()))

        if not dest_CDT_members.filter(pk=self.dest_pin.pk).exists():
            raise ValidationError(
                "Destination pin \"{}\" does not come from compounddatatype \"{}\"".
                format(self.dest_pin,
                       dest_CDT))

        # Check that the datatypes on either side of this wire are
        # either the same, or restriction-compatible
        if not self.source_pin.datatype.is_restriction(self.dest_pin.datatype):
            raise ValidationError(
                "The datatype of the source pin \"{}\" is incompatible with the datatype of the destination pin \"{}\"".
                format(self.source_pin, self.dest_pin))

    def is_casting(self):
        """
        Tells whether the cable performs a casting on Datatypes.

        PRE: the wire must be clean (and therefore the source DT must
        at least be a restriction of the destination DT).
        """
        return self.source_pin.datatype != self.dest_pin.datatype
        
class PipelineOutputCable(models.Model):
    """
    Defines which outputs of internal PipelineSteps are mapped to
    end-point Pipeline outputs once internal execution is complete.

    Thus, a definition of cables leading to external pipeline outputs.

    Related to :model:`pipeline.models.Pipeline`
    Related to :model:`transformation.models.TransformationOutput`
    """
    pipeline = models.ForeignKey(
        Pipeline,
        related_name="outcables")

    output_name = models.CharField(
        "Output hole name",
        max_length=128,
        help_text="Pipeline output hole name")

    # We need to specify both the output name and the output index because
    # we are defining the outputs of the Pipeline indirectly through
    # this wiring information - name/index mapping is stored...?
    output_idx = models.PositiveIntegerField(
        "Output hole index",
        validators=[MinValueValidator(1)],
        help_text="Pipeline output hole index")

    # If null, the source must be raw
    output_cdt = models.ForeignKey(
        "metadata.CompoundDatatype",
        blank=True,
        null=True,
        related_name="cables_leading_to")

    # source_step refers to an actual step of the pipeline and
    # source actually refers to one of the outputs at that step.
    # This is enforced via clean()s.
    source_step = models.PositiveIntegerField(
        "Source pipeline step number",
        validators=[MinValueValidator(1)],
        help_text="Source step at which output comes from")

    source = models.ForeignKey(
        "transformation.TransformationOutput",
        help_text="Source output hole")

    custom_outwires = generic.GenericRelation("CustomCableWire")
    
    # Enforce uniqueness of output names and indices.
    # Note: in the pipeline, these will still need to be compared with the raw
    # output names and indices.
    class Meta:
        unique_together = (("pipeline", "output_name"),
                           ("pipeline", "output_idx"))

    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        super(self.__class__, self).__init__(*args, **kwargs)

    def __unicode__(self):
        """ Represent with the pipeline name, and TO output index + name """

        pipeline_name = "[no pipeline set]";
        if self.pipeline != None:
            pipeline_name = unicode(self.pipeline);

        return "POC feeding Output_idx [{}], Output_name [{}] for pipeline [{}]".format(
                self.output_idx,
                self.output_name,
                pipeline_name)

    def clean(self):
        """
        Checks coherence of this output cable.
        
        PipelineOutputCable must reference an existant, undeleted
        transformation output hole.  Also, if the cable is raw, there
        should be no custom wiring.  If the cable is not raw and there
        are custom wires, they should be clean.
        """
        # Step number must be valid for this pipeline
        if self.source_step > self.pipeline.steps.all().count():
            raise ValidationError(
                "Output requested from a non-existent step");
        
        source_ps = self.pipeline.steps.get(step_num=self.source_step);

        # Try to find a matching output hole
        if not source_ps.transformation.outputs.filter(pk=self.source.pk).exists():
            raise ValidationError(
                "Transformation at step {} does not produce output \"{}\"".
                format(self.source_step, self.source));

        outwires = self.custom_outwires.all()

        # The cable and destination must both be raw (or non-raw)
        if self.output_cdt == None and not self.is_raw():
            raise ValidationError(
                "Cable \"{}\" has a null output_cdt but its source is non-raw" .
                format(self))
        elif self.output_cdt != None and self.is_raw():
            raise ValidationError(
                "Cable \"{}\" has a non-null output_cdt but its source is raw" .
                format(self))

        # The cable has a raw source (and output_cdt is None).
        if self.is_raw():

            # Wires cannot exist.
            if outwires.exists():
                raise ValidationError(
                    "Cable \"{}\" is raw and should not have wires defined" .
                    format(self))

        # The cable has a nonraw source (and output_cdt is specified).
        else:
            if not self.source.get_cdt().is_restriction(
                    self.output_cdt) and not outwires.exists():
                raise ValidationError(
                    "Cable \"{}\" has a source CDT that is not a restriction of its target CDT, but no wires exist".
                    format(self))

            # Clean all wires.
            for outwire in outwires:
                outwire.clean()
                outwire.validate_unique()
                # It isn't enough that the outwires are clean: they
                # should do no casting.
                if outwire.is_casting():
                    raise ValidationError(
                        "Custom wire \"{}\" of PipelineOutputCable \"{}\" casts the Datatype of its source".
                        format(outwire, self))

    def complete_clean(self):
        """Checks completeness and coherence of this POC.
        
        Calls clean, and then checks that if this POC is not raw and there
        are any custom wires defined, then they must quench the output CDT.
        """
        self.clean()

        if not self.is_raw() and self.custom_outwires.all().exists():
            # Check that each CDT member has a wire leading to it
            for dest_member in self.output_cdt.members.all():
                if not self.custom_outwires.filter(dest_pin=dest_member).exists():
                    raise ValidationError(
                        "Destination member \"{}\" has no outwires leading to it".
                        format(dest_member))

    def is_raw(self):
        """True if this output cable is raw; False otherwise."""
        return self.source.is_raw()


    def is_trivial(self):
        """
        True if this output cable is trivial; False otherwise.
        
        This basically does exactly what the corresponding method for
        PipelineStepInputCable does, by calling cable_trivial_h.

        PRE: cable is clean.
        """
        return cable_trivial_h(self, self.custom_outwires.all())
    
    def is_restriction(self, other_outcable):
        """
        Returns whether this cable is a restriction of the specified.

        More specifically, this cable is a restriction of the
        parameter if they come from the same TransformationOutput and, if
        they are not raw:
         - destination CDT is a restriction of parameter's destination CDT
         - wiring matches

        PRE: both self and other_cable are clean.
        """
        # Trivial case.
        if self == other_outcable:
            return True

        if self.source != other_outcable.source:
            return False

        # Now we know that they are fed by the same TransformationOutput.
        if self.is_raw():
            return True

        # From here on, we assume both cables are non-raw.
        # (They must be, since both are fed by the same TO and self
        # is not raw.)
        if not self.output_cdt.is_restriction(other_outcable.output_cdt):
            return False

        # If there is non-trivial custom wiring on either, then
        # the wiring must match.
        if self.is_trivial() and other_outcable.is_trivial():
            return True
        elif self.is_trivial() != other_outcable.is_trivial():
            return False
        
        # Now we know that both have non-trivial wiring.  Check both
        # cables' wires and see if they connect corresponding pins.
        # (We already know they feed the same TransformationInput,
        # so we only have to check the indices.)
        for wire in self.custom_outwires.all():
            corresp_wire = other_outcable.custom_outwires.get(
                dest_pin=wire.dest_pin)
            if (wire.source_pin.column_idx !=
                    corresp_wire.source_pin.column_idx):
                return False

        # Having reached this point, we know that the wiring matches.
        return True

    def is_compatible(self, other_outcable):
        """
        Checks if other_outcable is compatible with this POC.
        
        Definition of compatible:
         - Both are fed by the same TransformationOutput
         - POCs transform the data in the same way (wires match)
        
        NOTES
        Although both restricted by the source CDT, destination
        CDTs of POCs are not necessarily the same: we cannot
        use destination CDTMs meaningfully.

        Furthermore, multiple wires can leave one source CDTM,
        while only one wire can lead to a destination: it makes
        more sense to query each wire on matching destination.

        ALGORITHM
        1) For each wire in cable 1, look at it's dest name/idx.
        2) Find a wire in cable 2 with the same dest name/idx.
        3A) If no such wire exists, cables are not compatible.
        3B) If a wire exists, they must have matching source CDTM:
        if not, cables are not compatible.
        4) If we reach the end, cables are compatible.

        PRE: Both cables are clean.
        """

        # TOs must be the same
        if self.source != other_outcable.source:
            return False

        # Trivial cables don't change column names or idx
        if self.is_trivial() and other_outcable.is_trivial():
            return True
        elif self.is_trivial() != other_outcable.is_trivial():
            return False

        for wire in self.custom_outwires.all():
            corresponding_wire = other_outcable.custom_outwires.filter(
                    dest_pin__column_name=wire.dest_pin.column_name,
                    dest_pin__column_idx=wire.dest_pin.column_idx)

            if not corresponding_wire.exists():
                return false

            if wire.source_pin != corresponding_wire.first().source_pin:
                return false

        return True
        
    def run_cable(self, source, output_path, cable_record):
        """
        Perform the cable-specified transformation on the input.

        This uses run_cable_h and creates an ExecLog, associating it
        to cable_record.
        """
        self.logger.debug("Creating ExecLog for {}".format(cable_record))
        curr_log = archive.models.ExecLog(record=cable_record)
        curr_log.save()
        curr_log.start_time = timezone.now()
        run_cable_h(self, source, output_path)
        curr_log.end_time = timezone.now()
        curr_log.clean()
        curr_log.save()
        curr_log.complete_clean()

        return curr_log
