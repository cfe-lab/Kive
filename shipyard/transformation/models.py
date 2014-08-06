"""
transformation.models

Shipyard data models relating to the (abstract) definition of
Transformation.
"""
from __future__ import unicode_literals

from django.db import models
from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator
from django.db import transaction
from django.utils.encoding import python_2_unicode_compatible

from constants import maxlengths

@python_2_unicode_compatible
class TransformationFamily(models.Model):
    """
    TransformationFamily is abstract and describes common
    parameters between MethodFamily and PipelineFamily.

    Extends :model:`method.MethodFamily`
    Extends :model:`pipeline.PipelineFamily`
    """
    name = models.CharField(
        "Transformation family name",
        max_length=maxlengths.MAX_NAME_LENGTH,
        help_text="The name given to a group of methods/pipelines",
        unique=True)

    description = models.TextField(
        "Transformation family description",
        help_text="A description for this collection of methods/pipelines",
        max_length=maxlengths.MAX_DESCRIPTION_LENGTH,
        blank=True)

    def __str__(self):
        """ Describe transformation family by it's name """
        return self.name

    class Meta:
        abstract = True


@python_2_unicode_compatible
class Transformation(models.Model):
    """
    Abstract class that defines common parameters
    across Method revisions and Pipeline revisions.

    Extends :model:`method.Method`
    Extends :model:`pipeline.Pipeline`
    Related to :model:`transformation.TransformationInput`
    Related to :model:`transformation.TransformationOutput`
    """
    revision_name = models.CharField("Transformation revision name", max_length=maxlengths.MAX_NAME_LENGTH,
                                     help_text="The name of this transformation revision",
                                     blank=True)

    revision_DateTime = models.DateTimeField("Revision creation date", auto_now_add = True)

    revision_desc = models.TextField(
        "Transformation revision description",
        help_text="Description of this transformation revision",
        max_length=maxlengths.MAX_DESCRIPTION_LENGTH,
        blank=True)

    # revision_number = models.IntegerField('Transformation revision number',
    #                                       help_text='Revision number of Transformation in its family')
    # Implicitly defined:
    # - inputs (via FK of TransformationInput)
    # - outputs (via FK of TransformationOutput)
    # - pipelinesteps (via FK of PipelineStep)

    # Note that we override these in both Pipeline and Method, in case
    # we try to invoke them directly on either.  (This code wouldn't work
    # in that case because a Method wouldn't have a field called "pipeline"
    # and vice versa.)
    @property
    def is_pipeline(self):
        """Is this a Pipeline, as opposed to a Method?"""
        try:
            self.pipeline
        except Transformation.DoesNotExist:
            return False
        return True

    @property
    def is_method(self):
        """Is this a method, as opposed to a Pipeline?"""
        try:
            self.method
        except Transformation.DoesNotExist:
            return False
        return True

    @property
    def definite(self):
        if self.is_pipeline:
            return self.pipeline
        else:
            return self.method

    def __str__(self):
        if self.revision_name:
            return "{}: {}".format(self.definite.revision_number, self.revision_name)
        return str(self.definite.revision_number)

    def check_input_indices(self):
        """Check that input indices are numbered consecutively from 1."""
        for i, curr_input in enumerate(self.inputs.order_by("dataset_idx"), start=1):
            if i != curr_input.dataset_idx:
                raise ValidationError("Inputs are not consecutively numbered starting from 1")
        
    def check_output_indices(self):
        """Check that output indices are numbered consecutively from 1."""
        # Append each output index (hole number) to a list
        output_nums = []
        for curr_output in self.outputs.all():
            output_nums += [curr_output.dataset_idx]

        # Indices must be consecutively numbered from 1 to n
        if sorted(output_nums) != range(1, self.outputs.count()+1):
            raise ValidationError(
                "Outputs are not consecutively numbered starting from 1")

    def clean(self):
        """Validate transformation inputs and outputs, and reject if it is neither Method nor Pipeline."""
        if not self.is_pipeline and not self.is_method:
            raise ValidationError("Transformation with pk={} is neither Method nor Pipeline".format(self.pk))
        self.check_input_indices()
        self.check_output_indices()

    @transaction.atomic
    def create_xput(self, dataset_name, dataset_idx=None, compounddatatype=None, row_limits=None, coords=None, 
                    input=True):
        """Create a TranformationXput for this Transformation.

        Decides whether the created TransformationXput should have a
        structure or not based on the parameters given. If
        compounddatatype os None but row limits are provided, then a
        ValueError is raised.

        PARAMETERS
        dataset_name        name for new xput
        dataset_idx         index for new output (defaults to number of
                            current in/outputs plus one)
        compounddatatype    CompoundDatatype for new xput
        row_limits          tuple (min_row, max_row), defaults to no limits
        coords              tuple (x, y), defaults to (0, 0)
        input               True to create a TransformationInput, False to
                            create a TransformationOutput
        """
        min_row, max_row = (None, None) if not row_limits else row_limits
        x, y = (0, 0) if not coords else coords

        if compounddatatype is None and (min_row is not None or max_row is not None):
            raise ValueError("Row restrictions cannot be specified without a CDT")

        xputs = self.inputs if input else self.outputs
        new_xput = xputs.create(dataset_name=dataset_name, dataset_idx=dataset_idx or xputs.count()+1, x=x, y=y)
        new_xput.full_clean()
        if compounddatatype:
            new_xput.add_structure(compounddatatype, min_row, max_row)
        return new_xput

    @transaction.atomic
    def create_input(self, dataset_name, dataset_idx=None, compounddatatype=None,
                     min_row=None, max_row=None, x=0, y=0):
        """Create a TransformationInput for this Transformation."""
        return self.create_xput(dataset_name, dataset_idx, compounddatatype, (min_row, max_row), (x, y), True)
    
    @transaction.atomic
    def create_output(self, dataset_name, dataset_idx, compounddatatype=None,
                     min_row=None, max_row=None, x=0, y=0):
        """Create a TransformationOutput for this Transformation."""
        return self.create_xput(dataset_name, dataset_idx, compounddatatype, (min_row, max_row), (x, y), False)

    # June 10, 2014: two helpers that we use in testing.  Maybe they'll be useful elsewhere?
    def delete_inputs(self):
        for curr_input in self.inputs.all():
            curr_input.delete()

    def delete_outputs(self):
        for curr_output in self.outputs.all():
            curr_output.delete()


@python_2_unicode_compatible
class TransformationXput(models.Model):
    """
    Describes parameters common to all inputs and outputs
    of transformations - the "holes"

    Related to :models:`transformation.Transformation`
    """
    # transformation, dataset_name, and dataset_idx have been moved to
    # the derived classes so they can have their own unique_together
    # constraints. structure is implicitly defined via a OneToOneField
    # on the XputStructure, as is execrecordouts_referencing (via FK
    # from librarian.ExecRecordOut)

    # UI information.
    x = models.IntegerField(default=0, validators=[MinValueValidator(0)])
    y = models.IntegerField(default=0, validators=[MinValueValidator(0)])

    @property
    def is_input(self):
        try:
            self.transformationinput
        except TransformationXput.DoesNotExist:
            return False
        return True

    @property
    def is_output(self):
        try:
            self.transformationoutput
        except TransformationXput.DoesNotExist:
            return False
        return True

    @property
    def definite(self):
        if self.is_input:
            return self.transformationinput
        else:
            return self.transformationoutput

    def clean(self):
        """Make sure this is either a TransformationInput or TransformationOutput."""
        if not self.is_input and not self.is_output:
            return ValidationError("TransformationXput with pk={} is neither an input nor an output".format(self.pk))

    def __str__(self):
        return "{}: {}".format(self.definite.dataset_idx, self.definite.dataset_name)

    @property
    def compounddatatype(self):
        return None if self.is_raw() else self.structure.compounddatatype

    def is_raw(self):
        """True if this Xput is raw, false otherwise."""
        return not hasattr(self, "structure")

    def get_cdt(self):
        """Accessor that returns the CDT of this xput (and None if it is raw)."""
        return None if self.is_raw() else self.structure.compounddatatype

    def get_min_row(self):
        """Accessor that returns min_row for this xput (and None if it is raw)."""
        return (None if self.is_raw() else self.structure.min_row)

    def get_max_row(self):
        """Accessor that returns max_row for this xput (and None if it is raw)."""
        return (None if self.is_raw() else self.structure.max_row)

    @property
    def has_structure(self):
        return hasattr(self, "structure")

    @transaction.atomic
    def add_structure(self, compounddatatype, min_row, max_row):
        """Add an XputStructure to this TransformationXput.

        ASSUMPTIONS
        This TransformationXput does not already have a structure.
        """
        assert not self.has_structure
        assert compounddatatype is not None

        new_structure = XputStructure(transf_xput=self,
                compounddatatype=compounddatatype,
                min_row=min_row, max_row=max_row)
        new_structure.full_clean()
        new_structure.save()

    def represent_as_dict(self):
        """
        Make a dict serialization of this TransformationXput.

        These will be dicts with the following fields:
         - CDT_pk: None if raw; PK of desired CDT otherwise
         - dataset_name: string
         - dataset_idx: 1-based index
         - x: int
         - y: int
         - min_row: None if no restriction; otherwise, int
         - max_row: None if no restriction; otherwise, int
        """
        input_CDT_pk = None if not self.has_structure else self.structure.compounddatatype.pk
        min_row = self.get_min_row()
        max_row = self.get_max_row()
        return {
            "CDT_pk": input_CDT_pk,
            "dataset_name": self.dataset_name,
            "dataset_idx": self.dataset_idx,
            "x": self.x,
            "y": self.y,
            "min_row": min_row,
            "max_row": max_row
        }


class XputStructure(models.Model):
    """
    Describes the "holes" that are managed by Shipyard: i.e. the ones
    that correspond to well-understood CSV formatted data.

    Related to :model:`transformation.TransformationXput`
    """
    # June 6, 2014: turned into regular FK.
    transf_xput = models.OneToOneField(TransformationXput, related_name="structure")

    # The expected compounddatatype of the input/output
    compounddatatype = models.ForeignKey("metadata.CompoundDatatype")
    
    # Nullable fields indicating that this dataset has
    # restrictions on how many rows it can have
    min_row = models.PositiveIntegerField(
        "Minimum row",
        help_text="Minimum number of rows this input/output returns",
        null=True,
        blank=True)

    max_row = models.PositiveIntegerField(
        "Maximum row",
        help_text="Maximum number of rows this input/output returns",
        null=True,
        blank=True)


class TransformationInput(TransformationXput):
    """
    Inherits from :model:`transformation.TransformationXput`
    """
    transformation = models.ForeignKey(Transformation, related_name="inputs")

    # The name of the input "hole".
    dataset_name = models.CharField(
        "input name",
        max_length=maxlengths.MAX_NAME_LENGTH,
        help_text="Name for input as an alternative to index")

    # Input index on the transformation.
    dataset_idx = models.PositiveIntegerField(
            "input index",
            validators=[MinValueValidator(1)],
            help_text="Index defining the relative order of this input")

    class Meta:
        # A transformation cannot have multiple definitions for column name or column index
        unique_together = (("transformation", "dataset_name"),
                           ("transformation", "dataset_idx"))


class TransformationOutput(TransformationXput):
    """
    Inherits from :model:`transformation.TransformationXput`
    """
    # Similarly to TransformationInput.
    # transformationxput = models.OneToOneField(TransformationXput, parent_link=True)
    transformation = models.ForeignKey(Transformation, related_name="outputs")

    dataset_name = models.CharField(
        "output name",
        max_length=maxlengths.MAX_NAME_LENGTH,
        help_text="Name for output as an alternative to index")

    dataset_idx = models.PositiveIntegerField(
            "output index",
            validators=[MinValueValidator(1)],
            help_text="Index defining the relative order of this output")

    class Meta:
        # A transformation cannot have multiple definitions for column name or column index
        unique_together = (("transformation", "dataset_name"),
                           ("transformation", "dataset_idx"))
