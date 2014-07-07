"""
transformation.models

Shipyard data models relating to the (abstract) definition of
Transformation.
"""

from django.db import models
from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator
from django.db import transaction


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
        help_text="The name given to a group of methods/pipelines",
        unique=True)

    description = models.TextField(
        "Transformation family description",
        help_text="A description for this collection of methods/pipelines")

    def __unicode__(self):
        """ Describe transformation family by it's name """
        return self.name

    class Meta:
        abstract = True


class Transformation(models.Model):
    """
    Abstract class that defines common parameters
    across Method revisions and Pipeline revisions.

    Extends :model:`method.Method`
    Extends :model:`pipeline.Pipeline`
    Related to :model:`transformation.TransformationInput`
    Related to :model:`transformation.TransformationOutput`
    """
    revision_name = models.CharField("Transformation revision name", max_length=128,
                                     help_text="The name of this transformation revision")

    revision_DateTime = models.DateTimeField("Revision creation date", auto_now_add = True)

    revision_desc = models.TextField(
        "Transformation revision description",
        help_text="Description of this transformation revision",
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

    # Helper to create inputs, which is now a 2-step operation if the input
    # is not raw.
    @transaction.atomic
    def create_input(self, dataset_name, dataset_idx, compounddatatype=None,
                     min_row=None, max_row=None, x=0, y=0):
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
                                       dataset_idx=dataset_idx,
                                       x=x, y=y)
        new_input.full_clean()

        if compounddatatype != None:
            new_input_structure = XputStructure(
                transf_xput=new_input,
                compounddatatype=compounddatatype,
                min_row=min_row, max_row=max_row)
            # June 6, 2014: now that we aren't using GFKs anymore we go back
            # to using full_clean() here.  Previously it was barfing on
            # clean_fields().
            new_input_structure.full_clean()
            # new_input_structure.clean()
            # new_input_structure.validate_unique()
            new_input_structure.save()

        return new_input

    # Same thing to create outputs.
    @transaction.atomic
    def create_output(self, dataset_name, dataset_idx, compounddatatype=None,
                     min_row=None, max_row=None, x=0, y=0):
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
                                         dataset_idx=dataset_idx,
                                         x=x, y=y)
        new_output.full_clean()

        if compounddatatype != None:
            new_output_structure = XputStructure(
                transf_xput=new_output,
                compounddatatype=compounddatatype,
                min_row=min_row, max_row=max_row)
            new_output_structure.full_clean()
            new_output_structure.save()

        return new_output

    # June 10, 2014: two helpers that we use in testing.  Maybe they'll be useful elsewhere?
    def delete_inputs(self):
        for curr_input in self.inputs.all():
            curr_input.delete()

    def delete_outputs(self):
        for curr_output in self.outputs.all():
            curr_output.delete()


# August 20, 2013: changed the structure of our Xputs so that there is no distinction
# between raw and non-raw Xputs beyond the existence of an associated "structure"
class TransformationXput(models.Model):
    """
    Describes parameters common to all inputs and outputs
    of transformations - the "holes"

    Related to :models:`transformation.Transformation`
    """
    # June 6, 2014: this is now a real thing, and transformation, dataset_name, and
    # dataset_idx have been moved to the derived classes so they can have their own
    # unique_together constraints.

    # June 6, 2014: structure is now simply be implicitly defined via a OneToOneField on the
    # XputStructure, as is execrecordouts_referencing (via FK from librarian.ExecRecordOut).

    # June 16, 2014: UI information.
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

    def __unicode__(self):
        unicode_rep = u"";
        definite_xput = self.definite
        if self.is_raw():
            unicode_rep = u"[{}]:raw{} {}".format(
                    definite_xput.transformation.definite,
                    definite_xput.dataset_idx,
                    definite_xput.dataset_name)
        else:
            unicode_rep = u"{} name:{} idx:{} cdt:{}".format(
                    definite_xput.transformation.definite,
                    definite_xput.dataset_name,
                    definite_xput.dataset_idx,
                    self.get_cdt())
        return unicode_rep

    @property
    def compounddatatype(self):
        if self.is_raw(): return None
        return self.structure.compounddatatype

    def is_raw(self):
        """True if this Xput is raw, false otherwise."""
        return not hasattr(self, "structure")

    def get_cdt(self):
        """Accessor that returns the CDT of this xput (and None if it is raw)."""
        my_cdt = None
        if not self.is_raw():
            my_cdt = self.structure.compounddatatype
        return my_cdt

    def get_min_row(self):
        """Accessor that returns min_row for this xput (and None if it is raw)."""
        return (None if self.is_raw() else self.structure.min_row)

    def get_max_row(self):
        """Accessor that returns max_row for this xput (and None if it is raw)."""
        return (None if self.is_raw() else self.structure.max_row)

    @property
    def has_structure(self):
        return hasattr(self, "structure")

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
    # # Specify an explicit parent link field so that we can go from here back up to
    # # the TransformationXput (e.g. so we can get at its structure).
    # transformationxput = models.OneToOneField(TransformationXput, parent_link=True)
    transformation = models.ForeignKey(Transformation, related_name="inputs")

    # The name of the input "hole".
    dataset_name = models.CharField(
        "input name",
        max_length=128,
        help_text="Name for input as an alternative to index")

    # Input index on the transformation.
    ####### NOTE: ONLY METHODS NEED INDICES, NOT TRANSFORMATIONS....!!
    # If we were to differentiate between methods/pipelines... dataset_idx would only
    # belong to methods
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
        max_length=128,
        help_text="Name for output as an alternative to index")

    dataset_idx = models.PositiveIntegerField(
            "output index",
            validators=[MinValueValidator(1)],
            help_text="Index defining the relative order of this output")

    class Meta:
        # A transformation cannot have multiple definitions for column name or column index
        unique_together = (("transformation", "dataset_name"),
                           ("transformation", "dataset_idx"))
