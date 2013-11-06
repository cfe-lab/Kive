"""
transformation.models

Shipyard data models relating to the (abstract) definition of
Transformation.
"""

from django.db import models
from django.contrib.contenttypes import generic
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator
from django.db import transaction

import metadata.models
import metadata.models

class TransformationFamily(models.Model):
    """
    TransformationFamily is abstract and describes common
    parameters between MethodFamily and PipelineFamily.

    Extends :model:`method.MethodFamily`
    Extends :model:`pipeline.PipelineFamily`
    """

    name = models.CharField(
        "Transformation family name",
		max_length=128,
		help_text="The name given to a group of methods/pipelines");

    description = models.TextField(
        "Transformation family description",
		help_text="A description for this collection of methods/pipelines");

    def __unicode__(self):
        """ Describe transformation family by it's name """
        return self.name;

    class Meta:
        abstract = True;

class Transformation(models.Model):
    """
    Abstract class that defines common parameters
    across Method revisions and Pipeline revisions.

    Extends :model:`method.Method`
    Extends :model:`pipeline.Pipeline`
    Related to :model:`transformation.TransformationInput`
    Related to :model:`transformation.TransformationOutput`
    """

    revision_name = models.CharField(
		"Transformation revision name",
		max_length=128,
		help_text="The name of this transformation revision");

    revision_DateTime = models.DateTimeField(
		"Revision creation date",
		auto_now_add = True);

    revision_desc = models.TextField(
		"Transformation revision description",
		help_text="Description of this transformation revision");

    # inputs/outputs associated with transformations via GenericForeignKey
    # And can be accessed from within Transformations via GenericRelation
    inputs = generic.GenericRelation("transformation.TransformationInput");
    outputs = generic.GenericRelation("transformation.TransformationOutput");

    class Meta:
        abstract = True;

    def check_input_indices(self):
        """Check that input indices are numbered consecutively from 1."""
        # Append each input index (hole number) to a list
        input_nums = [];
        for curr_input in self.inputs.all():
            input_nums += [curr_input.dataset_idx];

        # Indices must be consecutively numbered from 1 to n
        if sorted(input_nums) != range(1, self.inputs.count()+1):
            raise ValidationError(
                "Inputs are not consecutively numbered starting from 1");
        
    def check_output_indices(self):
        """Check that output indices are numbered consecutively from 1."""
        # Append each output index (hole number) to a list
        output_nums = [];
        for curr_output in self.outputs.all():
            output_nums += [curr_output.dataset_idx];

        # Indices must be consecutively numbered from 1 to n
        if sorted(output_nums) != range(1, self.outputs.count()+1):
            raise ValidationError(
                "Outputs are not consecutively numbered starting from 1");

    def clean(self):
        """Validate transformation inputs and outputs."""
        self.check_input_indices();
        self.check_output_indices();

    # Helper to create inputs, which is now a 2-step operation if the input
    # is not raw.
    @transaction.commit_on_success
    def create_input(self, dataset_name, dataset_idx, compounddatatype=None,
                     min_row=None, max_row=None):
        """
        Create a TI for this transformation.

        Decides whether the created TI should have a structure or not based
        on the parameters given.

        If CDT is None but min_row or max_row is not None, then a ValueError
        is raised.
        """
        if compounddatatype == None and (min_row != None or max_row != None):
            raise ValueError("Row restrictions cannot be specified without a CDT")

        new_input = self.inputs.create(dataset_name=dataset_name,
                                       dataset_idx=dataset_idx)
        new_input.full_clean()

        if compounddatatype != None:
            new_input_structure = new_input.structure.create(
                compounddatatype=compounddatatype,
                min_row=min_row, max_row=max_row)
            # new_input_structure.full_clean()
            # FIXME August 22, 2013: for some reason full_clean() barfs
            # on clean_fields().  Seems like the problem is that
            # it can't find TransformationInput or TransformationOutput
            # in the ContentTypes table, which is dumb.
            new_input_structure.clean()
            new_input_structure.validate_unique()

        return new_input

    
    # Same thing to create outputs.
    @transaction.commit_on_success
    def create_output(self, dataset_name, dataset_idx, compounddatatype=None,
                     min_row=None, max_row=None):
        """
        Create a TO for this transformation.

        Decides whether the created TO should have a structure or not based
        on the parameters given.

        If CDT is None but min_row or max_row is not None, then a ValueError
        is raised.
        """
        if compounddatatype == None and (min_row != None or max_row != None):
            raise ValueError("Row restrictions cannot be specified without a CDT")

        new_output = self.outputs.create(dataset_name=dataset_name,
                                         dataset_idx=dataset_idx)
        new_output.full_clean()

        if compounddatatype != None:
            new_output_structure = new_output.structure.create(
                compounddatatype=compounddatatype,
                min_row=min_row, max_row=max_row)
            # new_output_structure.full_clean()
            # FIXME August 22, 2013: same as for create_input
            new_output_structure.clean()
            new_output_structure.validate_unique()


        return new_output

# August 20, 2013: changed the structure of our Xputs so that there is no distinction
# between raw and non-raw Xputs beyond the existence of an associated "structure"
class TransformationXput(models.Model):
    """
    Describes parameters common to all inputs and outputs
    of transformations - the "holes"

    Related to :models:`transformation.Transformation`
    """
    # TransformationXput describes the input/outputs of transformations,
    # so this class can only be associated with method and pipeline.
    content_type = models.ForeignKey(
        ContentType,
        limit_choices_to = {"model__in": ("method", "pipeline")})
    object_id = models.PositiveIntegerField()
    transformation = generic.GenericForeignKey("content_type", "object_id")

    # The name of the "input/output" hole.
    dataset_name = models.CharField(
        "Input/output name",
        max_length=128,
        help_text="Name for input/output as an alternative to index")

    # Input/output index on the transformation.
    ####### NOTE: ONLY METHODS NEED INDICES, NOT TRANSFORMATIONS....!!
    # If we differentiate between methods/pipelines... dataset_idx would only
    # belong to methods
    dataset_idx = models.PositiveIntegerField(
            "Input/output index",
            validators=[MinValueValidator(1)],
            help_text="Index defining the relative order of this input/output")

    structure = generic.GenericRelation("XputStructure")

    execrecordouts_referencing = generic.GenericRelation("librarian.ExecRecordOut")

    class Meta:
        abstract = True;

        # A transformation cannot have multiple definitions for column name or column index
        unique_together = (("content_type", "object_id", "dataset_name"),
                           ("content_type", "object_id", "dataset_idx"));

    def __unicode__(self):
        unicode_rep = u"";
        if self.is_raw():
            unicode_rep = u"[{}]:raw{} {}".format(self.transformation,
                                                  self.dataset_idx, self.dataset_name)
        else:
            unicode_rep = u"[{}]:{} {} {}".format(self.transformation,
                                                  self.dataset_idx,
                                                  self.get_cdt(),
                                                  self.dataset_name);
        return unicode_rep

    def is_raw(self):
        """True if this Xput is raw, false otherwise."""
        return not self.structure.all().exists()

    def get_cdt(self):
        """Accessor that returns the CDT of this xput (and None if it is raw)."""
        my_cdt = None
        if not self.is_raw():
            my_cdt = self.structure.all()[0].compounddatatype
        return my_cdt

    def get_min_row(self):
        """Accessor that returns min_row for this xput (and None if it is raw)."""
        my_min_row = None
        if not self.is_raw():
            my_min_row = self.structure.all()[0].min_row
        return my_min_row

    def get_max_row(self):
        """Accessor that returns max_row for this xput (and None if it is raw)."""
        my_max_row = None
        if not self.is_raw():
            my_max_row = self.structure.all()[0].max_row
        return my_max_row

class XputStructure(models.Model):
    """
    Describes the "holes" that are managed by Shipyard: i.e. the ones
    that correspond to well-understood CSV formatted data.

    Related to :model:`transformation.TransformationXput`
    """
    content_type = models.ForeignKey(
        ContentType,
        limit_choices_to = {"model__in": ("TransformationInput", "TransformationOutput")});
    object_id = models.PositiveIntegerField();
    transf_xput = generic.GenericForeignKey("content_type", "object_id")

    # The expected compounddatatype of the input/output
    compounddatatype = models.ForeignKey("metadata.CompoundDatatype");
    
    # Nullable fields indicating that this dataset has
    # restrictions on how many rows it can have
    min_row = models.PositiveIntegerField(
        "Minimum row",
        help_text="Minimum number of rows this input/output returns",
        null=True,
        blank=True);

    max_row = models.PositiveIntegerField(
        "Maximum row",
        help_text="Maximum number of rows this input/output returns",
        null=True,
        blank=True);

    class Meta:
        unique_together = ("content_type", "object_id")

class TransformationInput(TransformationXput):
    """
    Inherits from :model:`transformation.TransformationXput`
    """
    pass

class TransformationOutput(TransformationXput):
    """
    Inherits from :model:`transformation.TransformationXput`
    """
    pass
