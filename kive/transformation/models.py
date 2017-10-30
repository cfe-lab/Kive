"""
transformation.models

Shipyard data models relating to the (abstract) definition of
Transformation.
"""
from __future__ import unicode_literals

from django.db import models
from django.core.exceptions import ValidationError, ObjectDoesNotExist
from django.core.validators import MinValueValidator, MaxValueValidator, validate_slug
from django.db import transaction
from django.utils.encoding import python_2_unicode_compatible

import metadata.models

from constants import maxlengths

import itertools


@python_2_unicode_compatible
class TransformationFamily(metadata.models.AccessControl):
    """
    TransformationFamily is abstract and describes common
    parameters between MethodFamily and PipelineFamily.

    Extends :model:`method.MethodFamily`
    Extends :model:`pipeline.PipelineFamily`
    """
    name = models.CharField(
        "Transformation family name",
        max_length=maxlengths.MAX_NAME_LENGTH,
        help_text="The name given to a group of methods/pipelines")

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
        ordering = ('name', )
        unique_together = ("name", "user")

    @classmethod
    @transaction.atomic
    def create(cls, *args, **kwargs):
        """Create a new TransformationFamily."""
        family = cls(*args, **kwargs)
        family.full_clean()
        family.save()
        return family


@python_2_unicode_compatible
class Transformation(metadata.models.AccessControl):
    """
    Abstract class that defines common parameters
    across Method revisions and Pipeline revisions.

    Inherited by :model:`method.Method`
    Inherited by :model:`pipeline.Pipeline`
    Related to :model:`transformation.TransformationInput`
    Related to :model:`transformation.TransformationOutput`
    """
    revision_name = models.CharField("Transformation revision name", max_length=maxlengths.MAX_NAME_LENGTH,
                                     help_text="The name of this transformation revision",
                                     blank=True)

    revision_DateTime = models.DateTimeField("Revision creation date", auto_now_add=True)

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
    def is_pipeline(self):
        """Is this a Pipeline, as opposed to a Method?"""
        try:
            self.pipeline
        except ObjectDoesNotExist:
            return False
        return True

    def is_method(self):
        """Is this a method, as opposed to a Pipeline?"""
        try:
            self.method
        except Transformation.DoesNotExist:
            return False
        return True

    @property
    def definite(self):
        if self.is_pipeline():
            return self.pipeline
        else:
            return self.method

    @property
    def display_name(self):
        return self.definite.display_name

    @property
    def sorted_inputs(self):
        """
        Return a sorted QuerySet of inputs to this Transformation.
        """
        return self.inputs.order_by("dataset_idx")

    @property
    def sorted_outputs(self):
        """
        Return a sorted QuerySet of outputs produced by this Transformation.
        """
        return self.outputs.order_by("dataset_idx")

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
        for i, curr_output in enumerate(self.outputs.order_by("dataset_idx"), start=1):
            if i != curr_output.dataset_idx:
                raise ValidationError("Outputs are not consecutively numbered starting from 1")

    def clean(self):
        """Validate transformation inputs and outputs, and reject if it is neither Method nor Pipeline."""
        if not self.is_pipeline() and not self.is_method():
            raise ValidationError("Transformation with pk={} is neither Method nor Pipeline".format(self.pk))

        for curr_input in self.inputs.all():
            curr_input.clean()
        for curr_output in self.outputs.all():
            curr_output.clean()
        self.check_input_indices()
        self.check_output_indices()

    def is_identical(self, other):
        """Is this Transformation identical to another?

        Ignores names (compares inputs and outputs only).
        """
        if self.user != other.user:
            return False

        my_xputs = itertools.chain(self.inputs.order_by("dataset_idx"), self.outputs.order_by("dataset_idx"))
        other_xputs = itertools.chain(other.inputs.order_by("dataset_idx"), other.outputs.order_by("dataset_idx"))
        for my_xput, other_xput in itertools.izip_longest(my_xputs, other_xputs, fillvalue=None):
            if my_xput is None or other_xput is None or not my_xput.is_identical(other_xput):
                return False
        return True

    @classmethod
    @transaction.atomic
    def create(cls, names, compounddatatypes=None, row_limits=None, coords=None, num_inputs=0, *args, **kwargs):
        """Create a new Transformation.

        names, compounddatatypes, row_limits, and coords are lists of
        items corresponding to the inputs and outputs for the new
        Transformation, ordered by index (inputs first). num_inputs
        controls how these are interpreted (eg. if num_inputs=2, then
        the first 2 items of names are for inputs, and the rest are for
        outputs).

        PARAMETERS
        names               names for inputs and outputs
        compounddatatyps    CompoundDatatypes for inputs and outputs
        row_limits          tuples (min_row, max_row)
        coords              tuples (x, y)
        num_inputs          number of inputs for the new Transformation
        *args, **kwargs     additional arguments for constructor
        """
        row_limits = row_limits or ([None] * len(names))
        coords = coords or ([None] * len(names))
        compounddatatypes = compounddatatypes or ([None] * len(names))

        transformation = cls(*args, **kwargs)
        transformation.save()
        for i in range(len(names)):
            transformation.create_xput(
                names[i],
                compounddatatype=compounddatatypes[i],
                row_limits=row_limits[i],
                coords=coords[i],
                input=i < num_inputs
            )

        # Hack: complete_clean() for Methods only (Pipelines can be
        # created without being complete).
        if transformation.is_method():
            transformation.complete_clean()
        else:
            transformation.full_clean()
        transformation.save()
        return transformation

    @transaction.atomic
    def create_xput(self,
                    dataset_name,
                    dataset_idx=None,
                    compounddatatype=None,
                    row_limits=None,
                    coords=None,
                    input=True,
                    clean=True):
        """Create a TransformationXput for this Transformation.

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
        if clean:
            new_xput.full_clean()
        if compounddatatype:
            new_xput.add_structure(compounddatatype, min_row, max_row, clean=clean)
        return new_xput

    def create_input(self, dataset_name, dataset_idx=None, compounddatatype=None,
                     min_row=None, max_row=None, x=0, y=0, clean=True):
        """Create a TransformationInput for this Transformation."""
        return self.create_xput(dataset_name, dataset_idx, compounddatatype, (min_row, max_row), (x, y), True,
                                clean=clean)

    def create_output(self,
                      dataset_name,
                      dataset_idx=None,
                      compounddatatype=None,
                      min_row=None,
                      max_row=None,
                      x=0,
                      y=0,
                      clean=True):
        """Create a TransformationOutput for this Transformation."""
        return self.create_xput(dataset_name, dataset_idx, compounddatatype, (min_row, max_row), (x, y), False,
                                clean=clean)

    def find_update(self):
        update = self.definite.family.members.latest('revision_number')
        members = list(self.definite.family.members.all())
        return update if update.id != self.id else None


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
    x = models.FloatField(default=0, validators=[MinValueValidator(0), MaxValueValidator(1)])
    y = models.FloatField(default=0, validators=[MinValueValidator(0), MaxValueValidator(1)])

    @property
    def is_input(self):
        return hasattr(self, 'transformationinput')

    @property
    def is_output(self):
        return hasattr(self, 'transformationoutput')

    @property
    def definite(self):
        if self.is_input:
            return self.transformationinput
        else:
            return self.transformationoutput

    def clean(self):
        """Make sure this is either a TransformationInput or TransformationOutput."""
        if not self.is_input and not self.is_output:
            raise ValidationError("TransformationXput with pk={} is neither an input nor an output".format(self.pk))
        if self.has_structure:
            self.structure.clean()

    def __str__(self):
        if self.is_input or self.is_output:
            return "{}: {}".format(self.definite.dataset_idx,
                                   self.definite.dataset_name)
        return 'TransformationXput(id={})'.format(self.id)

    @property
    def compounddatatype(self):
        return None if self.is_raw() else self.structure.compounddatatype

    def is_raw(self):
        """True if this Xput is raw, false otherwise."""
        return not self.has_structure

    def get_cdt(self):
        """Accessor that returns the CDT of this xput (and None if it is raw)."""
        return None if self.is_raw() else self.structure.compounddatatype

    def get_min_row(self):
        """Accessor that returns min_row for this xput (and None if it is raw)."""
        return (None if self.is_raw() else self.structure.min_row)

    def get_max_row(self):
        """Accessor that returns max_row for this xput (and None if it is raw)."""
        return (None if self.is_raw() else self.structure.max_row)

    def is_identical(self, other):
        """Is this TransformationXput the same as another?

        Ignores names and indices.
        """
        if self.is_input != other.is_input:
            return False

        if self.is_raw() and other.is_raw():
            return True
        if self.is_raw() or other.is_raw():
            return False
        return self.structure.is_identical(other.structure)

    @property
    def has_structure(self):
        # return hasattr(self, "structure")
        try:
            self.structure
        except ObjectDoesNotExist:
            return False
        return True

    @transaction.atomic
    def add_structure(self, compounddatatype, min_row, max_row, clean=True):
        """Add an XputStructure to this TransformationXput.

        ASSUMPTIONS
        This TransformationXput does not already have a structure.
        """
        assert not self.has_structure
        assert compounddatatype is not None

        new_structure = XputStructure(
                transf_xput=self,
                compounddatatype=compounddatatype,
                min_row=min_row, max_row=max_row)
        if clean:
            new_structure.full_clean()
        new_structure.save()


class XputStructure(models.Model):
    """
    Describes the "holes" that are managed by Shipyard: i.e. the ones
    that correspond to well-understood CSV formatted data.

    Related to :model:`transformation.TransformationXput`
    """
    transf_xput = models.OneToOneField(TransformationXput, related_name="structure")

    # The expected compounddatatype of the input/output
    compounddatatype = models.ForeignKey("metadata.CompoundDatatype", related_name="xput_structures")

    # Nullable fields indicating that this dataset has
    # restrictions on how many rows it can have
    min_row = models.PositiveIntegerField(
        "Minimum rows",
        help_text="Minimum number of rows this input/output returns",
        null=True,
        blank=True)

    max_row = models.PositiveIntegerField(
        "Maximum rows",
        help_text="Maximum number of rows this input/output returns",
        null=True,
        blank=True)

    def clean(self):
        tr = self.transf_xput.definite.transformation
        tr.validate_restrict_access([self.compounddatatype])

        if self.min_row is not None and self.max_row is not None:
            if self.min_row > self.max_row:
                raise ValidationError("Minimum row must not exceed maximum row",
                                      code="min_max")

    def is_identical(self, other):
        """Is this XputStructure identical to another one?"""
        return (self.compounddatatype == other.compounddatatype and
                self.min_row == other.min_row and
                self.max_row == other.max_row)


class TransformationInput(TransformationXput):
    """
    Inherits from :model:`transformation.TransformationXput`
    """
    transformation = models.ForeignKey(Transformation, related_name="inputs")

    # The name of the input "hole".
    dataset_name = models.CharField(
        "input name",
        max_length=maxlengths.MAX_NAME_LENGTH,
        help_text="Name for input as an alternative to index",
        validators=[validate_slug])

    # Input index on the transformation.
    dataset_idx = models.PositiveIntegerField(
        "input index",
        validators=[MinValueValidator(1)],
        help_text="Index defining the relative order of this input")

    class Meta:
        # A transformation cannot have multiple definitions for column name or column index
        unique_together = (("transformation", "dataset_name"),
                           ("transformation", "dataset_idx"))
        ordering = ('dataset_idx', )


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
        help_text="Name for output as an alternative to index",
        validators=[validate_slug])

    dataset_idx = models.PositiveIntegerField(
        "output index",
        validators=[MinValueValidator(1)],
        help_text="Index defining the relative order of this output")

    class Meta:
        # A transformation cannot have multiple definitions for column name or column index
        unique_together = (("transformation", "dataset_name"),
                           ("transformation", "dataset_idx"))
        ordering = ('dataset_idx', )
