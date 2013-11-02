"""
copperfish.models

Data model for the Shipyard (Copperfish) project - open source
software that performs revision control on datasets and bioinformatic
pipelines.
"""

from django.db import models
from django.contrib.auth.models import User
from django.contrib.contenttypes.models import ContentType
from django.contrib.contenttypes import generic
from django.db.models.signals import pre_save, post_save
from django.dispatch import receiver
from django.core.exceptions import ValidationError, ObjectDoesNotExist
from django.core.validators import MinValueValidator
from django.db import transaction

# Python math functions
import operator
# To calculate MD5 hash
import hashlib
# Regular expressions
import re
# Augments regular expressions
import string
# For checking file paths
import os.path
import os
import sys
import csv
import glob
import subprocess
import stat
import StringIO
import file_access_utils
import datetime

class Datatype(models.Model):
    """
    Abstract definition of a semantically atomic type of data.
    Related to :model:`copperfish.CompoundDatatype`
    """
    name = models.CharField(
        "Datatype name",
        max_length=64,
        help_text="The name for this Datatype");

    # auto_now_add: set to now on instantiation (editable=False)
    date_created = models.DateTimeField(
        'Date created',
        auto_now_add = True,
        help_text="Date Datatype was defined");

    description = models.TextField(
        "Datatype description",
        help_text="A description for this Datatype");

    # Admissible Python types.
    INT = "int"
    STR = "str"
    FLOAT = "float"
    BOOL = "bool"

    PYTHON_TYPE_CHOICES = (
        (INT, 'int'),
        (STR, 'str'),
        (FLOAT, 'float'),
        (BOOL, 'bool')
    )

    Python_type = models.CharField(
        'Python variable type',
        max_length=64,
        default = STR,
        choices=PYTHON_TYPE_CHOICES,
        help_text="Python type (int|str|float|bool|datetime)");

    restricts = models.ManyToManyField(
        'self',
        symmetrical=False,
        related_name="restricted_by",
        null=True,
        blank=True,
        help_text="Captures hierarchical is-a classifications among Datatypes");

    prototype = models.OneToOneField(
        'Dataset',
        null=True,
        blank=True,
        related_name="datatype_modelled")

    def is_restricted_by(self, possible_restrictor_datatype):
        """
        Determine if this datatype is ever *properly* restricted, directly or indirectly,
        by a given datatype.
        
        PRE: there is no circular restriction in the possible restrictor
        datatype (this would cause an infinite recursion).
        """
        # The default is that self is not restricted by
        # possible_restrictor_datatype; toggle to True if it turns out
        # that it is.
        is_restricted = False
        restrictions = possible_restrictor_datatype.restricts.all()

        for restrictedDataType in restrictions:

            # Case 1: If restrictions restrict self, return true
            if restrictedDataType == self:
                is_restricted = True

            # Case 2: Check if any restricted Datatypes themselves restrict self
            else:
                theValue = self.is_restricted_by(restrictedDataType)

                # If any restricted Datatypes themselves restrict self, propagate
                # this information to the parent Datatype as restricting self
                if theValue == True:
                    is_restricted = True

        # Return False if Case 1 is never encountered
        return is_restricted

    def is_restriction(self, possible_restrictor_datatype):
        """
        True if this Datatype restricts the parameter, directly or indirectly.

        This induces a partial ordering A <= B if A is a restriction of B.
        For example, a DNA sequence is a restriction of a string.
        """
        return (self == possible_restrictor_datatype or
                possible_restrictor_datatype.is_restricted_by(self))

    # Clean: If prototype is specified, it must have a CDT with
    # 2 columns: column 1 is a string "example" field,
    # column 2 is a bool "valid" field.  This CDT will be hard-coded
    # and loaded i

    # Clean: Check that the rows of prototype conform to the
    # constraints (if any) specified, including those
    # of the parent Datatypes.

    # FIXME: when we get execution working, we'll have to also
    # check that the first column of prototype yields the second
    # column of prototype after checking all constraints.

    # NOTE: we are going to assume that each Datatype has its own
    # well-defined constraints; we aren't going to check data
    # against all of its Datatype's parents.  But we *will* check
    # prototype against its parents' constraints.
    def clean(self):
        if hasattr(self, "restricts") and self.is_restricted_by(self):
            raise ValidationError(
                "Datatype \"{}\" has a circular restriction".
                format(self))

    def __unicode__(self):
        """Describe Datatype by name"""
        return self.name

    def has_custom_constraint(self):
        """Tells whether this Datatype has a CustomConstraint."""
        return hasattr(self, "custom_constraint")

    def check_basic_constraints(self, string_to_check):
        """
        Check the specified string against basic constraints.

        This includes both whether or not the string can be 
        interpreted as the appropriate Python type, but also
        whether it then checks out against all BasicConstraints.

        If it fails against even the simplest casting test (i.e.  the
        string could not be cast to the appropriate Python type),
        return a list containing a describing string.  If not, return
        a list of BasicConstraints that it failed (hopefully it's
        empty!).

        PRE: this Datatype and by extension all of its BasicConstraints
        are clean.  That means that only the appropriate BasicConstraints
        for this Datatype's Python_type are applied.
        """
        ####
        # CHECK PYTHON TYPE
        
        # First, try to cast it to the appropriate Python type.
        if self.Python_type == Datatype.STR:
            # string_to_check is, by definition, a string.
            pass
        elif self.Python_type == Datatype.INT:
            try:
                int(string_to_check)
            except ValueError:
                return ["Was not integer"]
        elif self.Python_type == Datatype.FLOAT:
            try:
                float(string_to_check)
            except ValueError:
                return ["Was not float"]
        elif self.Python_type == Datatype.BOOL:
            bool_RE = re.compile("^(True)|(False)|(true)|(false)|(TRUE)|(FALSE)|T|F|t|f|0|1$")
            if not bool_RE.match(string_to_check):
                return ["Was not boolean"]

        # FINISHED CHECKING PYTHON TYPE
        ####

        ####
        # CHECK BASIC CONSTRAINTS
        constraints_failed = []
        
        # Go through the BasicConstraints and check them all in turn.
        for basic_constraint in self.basic_constraints.all():
            if (basic_constraint.ruletype == BasicConstraint.MIN_LENGTH and
                    len(string_to_check) < int(basic_constraint.rule)):
                constraints_failed.append(basic_constraint)

            elif (basic_constraint.ruletype == BasicConstraint.MAX_LENGTH and
                    len(string_to_check) > int(basic_constraint.rule)):
                constraints_failed.append(basic_constraint)

            elif (basic_constraint.ruletype == BasicConstraint.MIN_VAL and
                    float(string_to_check) < float(basic_constraint.rule)):
                constraints_failed.append(basic_constraint)

            elif (basic_constraint.ruletype == BasicConstraint.MAX_VAL and
                    float(string_to_check) > float(basic_constraint.rule)):
                constraints_failed.append(basic_constraint)

            elif basic_constraint.ruletype == BasicConstraint.REGEXP:
                constraint_re = re.compile(basic_constraint.rule)
                if not constraint_re.match(string_to_check):
                    constraints_failed.append(basic_constraint)

            elif basic_constraint.ruletype == BasicConstraint.DATETIMEFORMAT:
                # Attempt to make a datetime object using this format
                # string.
                try:
                    datetime.datetime.strptime(string_to_check,
                                               basic_constraint.rule)
                except:
                    constraints_failed.append(basic_constraint)

        # FINISHED CHECKING BASIC CONSTRAINTS
        ####
                    
        return constraints_failed

    # Note that checking the CustomConstraint requires a place to run.
    # As such, it's debatable whether to put it here or as a Sandbox
    # method.  We'll probably put it here.

class BasicConstraint(models.Model):
    """
    Basic (level 1) constraint on a Datatype.

    The admissible constraints are:
     - (min|max)len (string)
     - (min|max)val (numeric)
     - (min|max)prec (float)
     - regexp (this will work on anything)
     - datetimeformat (string -- this is a special case)
    """
    datatype = models.ForeignKey(
        Datatype,
        related_name="basic_constraints")

    # Define the choices for rule type.
    MIN_LENGTH = "minlen"
    MAX_LENGTH = "maxlen"
    MIN_VAL = "minval"
    MAX_VAL = "maxval"
    # MIN_PREC = "minprec"
    # MAX_PREC = "maxprec"
    REGEXP = "regexp"
    DATETIMEFORMAT = "datetimeformat"
    
    CONSTRAINT_TYPES = (
        (MIN_LENGTH, "minimum string length"),
        (MAX_LENGTH, "maximum string length"),
        (MIN_VAL, "minimum numeric value"),
        (MAX_VAL, "maximum numeric value"),
        (REGEXP, "Perl regular expression"),
        (DATETIMEFORMAT, "date format string (1989 C standard)")
    )

    ruletype = models.CharField(
        "Type of rule",
        max_length=32,
        choices=CONSTRAINT_TYPES)

    rule = models.CharField(
        "Rule specification",
        max_length = 100)

    # TO DO: write a clean function handling the above.
    def clean(self):
        """
        Check coherence of the specified rule and rule type.

        The rule types must satisfy:
         - MIN_LENGTH: rule must be castable to a non-negative integer;
           parent DT must have Python type 'str'
         - MAX_LENGTH: rule must be castable to a positive integer;
           parent DT must have Python type 'str'
         - (MIN|MAX)_VAL: rule must be castable to a float; parent DT
           must have Python type 'float' or 'int'
         - REGEXP: rule must be a valid Perl-style RE
         - DATETIMEFORMAT: rule can be anything (note that it's up to you
           to define something *useful* here); parent DT must have Python 
           type 'str'
        """
        error_msg = ""
        is_error = False
        if self.ruletype == BasicConstraint.MIN_LENGTH:
            if self.datatype.Python_type != Datatype.STR:
                error_msg = ("Rule \"{}\" specifies a minimum string length but its parent Datatype \"{}\" is not a Python string".
                             format(self, self.datatype))
                is_error = True
            try:
                min_length = int(self.rule)
                if min_length < 0:
                    error_msg = ("Rule \"{}\" specifies a minimum string length but \"{}\" is negative".
                                 format(self, self.rule))
                    is_error = True
            except ValueError:
                error_msg = ("Rule \"{}\" specifies a minimum string length but \"{}\" does not specify an integer".
                             format(self, self.rule))
                is_error = True

        elif self.ruletype == BasicConstraint.MAX_LENGTH:
            if self.datatype.Python_type != Datatype.STR:
                error_msg = ("Rule \"{}\" specifies a maximum string length but its parent Datatype \"{}\" is not a Python string".
                             format(self, self.datatype))
                is_error = True
            try:
                max_length = int(self.rule)
                if max_length < 1:
                    error_msg = ("Rule \"{}\" specifies a maximum string length but \"{}\" is non-positive".
                                 format(self, self.rule))
                    is_error = True
            except ValueError:
                error_msg = ("Rule \"{}\" specifies a maximum string length but \"{}\" does not specify an integer".
                             format(self, self.rule))
                is_error = True

        elif self.ruletype in (BasicConstraint.MAX_VAL, 
                               BasicConstraint.MIN_VAL):
            if self.datatype.Python_type not in (Datatype.INT, Datatype.FLOAT):
                error_msg = ("Rule \"{}\" specifies a bound on a numeric value but its parent Datatype \"{}\" is not a number".
                             format(self, self.datatype))
                is_error = True
            try:
                val_bound = float(self.rule)
            except ValueError:
                error_msg = ("Rule \"{}\" specifies a bound on a numeric value but \"{}\" does not specify a numeric value".
                             format(self, self.rule))
                is_error = True
        
        elif self.ruletype == BasicConstraint.REGEXP:
            try:
                re.compile(self.rule)
            except re.error:
                error_msg = ("Rule \"{}\" specifies an invalid regular expression \"{}\"".
                             format(self, self.rule))
                is_error = True

        elif self.ruletype == BasicConstraint.DATETIMEFORMAT:
            if self.datatype.Python_type != Datatype.STR:
                error_msg = ("Rule \"{}\" specifies a date/time format but its parent Datatype \"{}\" is not a Python string".
                             format(self, self.datatype))
                is_error = True

        if is_error:
            raise ValidationError(error_msg)

class CustomConstraint(models.Model):
    """
    More complex (level 2) verification of Datatypes.

    These will be specified in the form of Methods that
    take a CSV of strings (which is the parent of all
    Datatypes) and return T/F for 
    """
    datatype = models.OneToOneField(
        Datatype,
        related_name="custom_constraint")

    verification_method = models.ForeignKey(
        "Method",
        related_name="custom_constraints")

    # Clean: Methods which function as CustomConstraints must take in
    # a column of strings named "to_test" and returns a column of
    # positive integers named "rownum".  We thus need to
    # hard-code in at least two Datatypes and two CDTs (string,
    # PositiveInteger (and probably int) so that PositiveInteger can
    # restrict it), and a CDT for each).  We'll probably need more
    # later anyway.  Such CDTs will be pre-loaded into the database.
    def clean(self):
        """
        Checks coherence of this CustomConstraint.

        The method used for verification must accept as input
        a CDT looking like (string to_test); it must return
        as output a CDT looking like (bool is_valid).
        """
        # Pre-defined CDTs that the verification method must use.
        VERIF_IN = CompoundDatatype.objects.get(pk=1)
        VERIF_OUT = CompoundDatatype.objects.get(pk=2)
        
        verif_method_in = self.verification_method.inputs.all()
        verif_method_out = self.verification_method.outputs.all()
        if verif_method_in.count() != 1 or verif_method_out.count() != 1:
            raise ValidationError("CustomConstraint \"{}\" verification method does not have exactly one input and one output".
                                  format(self))

        if not verif_method_in[0].get_cdt().is_identical(VERIF_IN):
            raise ValidationError(
                "CustomConstraint \"{}\" verification method does not have an input CDT identical to VERIF_IN".
                format(self))

        if not verif_method_out[0].get_cdt().is_identical(VERIF_OUT):
            raise ValidationError(
                "CustomConstraint \"{}\" verification method does not have an output CDT identical to VERIF_OUT".
                format(self))


class CompoundDatatypeMember(models.Model):
    """
    A data type member of a particular CompoundDatatype.
    Related to :model:`copperfish.Dataset`
    Related to :model:`copperfish.CompoundDatatype`
    """

    compounddatatype = models.ForeignKey(
        "CompoundDatatype",
        related_name="members",
        help_text="Links this DataType member to a particular CompoundDataType");

    datatype = models.ForeignKey(
        Datatype,
        help_text="Specifies which DataType this member is");

    column_name = models.CharField(
        "Column name",
        max_length=128,
        help_text="Gives datatype a 'column name' as an alternative to column index");

    # MinValueValidator(1) constrains column_idx to be >= 1
    column_idx = models.PositiveIntegerField(
        validators=[MinValueValidator(1)],
        help_text="The column number of this DataType");

    # Define database indexing rules to ensure tuple uniqueness
    # A compoundDataType cannot have 2 member definitions with the same column name or column number
    class Meta:
        unique_together = (("compounddatatype", "column_name"),
                           ("compounddatatype", "column_idx"));

    def __unicode__(self):
        """Describe a CompoundDatatypeMember with it's column number, datatype name, and column name"""

        returnString = u"{}: <{}> [{}]".format(self.column_idx,
                                               unicode(self.datatype),
                                               self.column_name);

        return returnString

class CompoundDatatype(models.Model):
    """
    A definition of a structured collection of datatypes,
    the resultant data structure serving as inputs or outputs
    for a Transformation.

    Related to :model:`copperfish.CompoundDatatypeMember`
    Related to :model:`copperfish.Dataset`
    """

    # Implicitly defined:
    #   members (CompoundDatatypeMember/ForeignKey)
    #   Conforming_datasets (Dataset/ForeignKey)

    def __unicode__(self):
        """ Represent CompoundDatatype with a list of it's members """

        string_rep = u"(";

        # Get the members for this compound data type
        all_members = self.members.all();

        # A) Get the column index for each member
        member_indices = [member.column_idx for member in all_members];

        # B) Get the column index of each Datatype member, along with the Datatype member itself
        members_with_indices = [ (member_indices[i], all_members[i]) for i in range(len(all_members))];
        # Can we do this?
        # members_with_indices = [ (all_members[i].column_idx, all_members[i])
        #                          for i in range(len(all_members))];

        # Sort members using column index as a basis (operator.itemgetter(0))
        members_with_indices = sorted(  members_with_indices,
                                        key=operator.itemgetter(0));

        # Add sorted Datatype members to the string representation
        for i, colIdx_and_member in enumerate(members_with_indices):
            colIdx, member = colIdx_and_member;
            string_rep += unicode(member);

            # Add comma if not at the end of member list
            if i != len(members_with_indices) - 1:
                string_rep += ", ";

        string_rep += ")";

        if string_rep == "()":
            string_rep = "[empty CompoundDatatype]";

        return string_rep;

    # clean() is executed prior to save() to perform model validation
    def clean(self):
        """Check if Datatype members have consecutive indices from 1 to n"""
        column_indices = [];

        # += is shorthand for extend() - concatenate a list with another list
        for member in self.members.all():
            column_indices += [member.column_idx];

        # Check if the sorted list is exactly a sequence from 1 to n
        if sorted(column_indices) != range(1, self.members.count()+1):
            raise ValidationError("Column indices are not consecutive starting from 1");

    def is_restriction(self, other_CDT):
        """
        True if this CDT is a column-wise restriction of its parameter.

        This is trivially true if they are the same CDT; otherwise
        the column names must be exactly the same and each column
        of this CDT is a restriction of the corresponding column
        of the parameter CDT.

        Note that this induces a partial order on CDTs.

        PRE: this CDT and other_CDT are clean.
        """
        if self == other_CDT:
            return True
        
        # Make sure they have the same number of columns.
        if self.members.count() != other_CDT.members.count():
            return False

        # Since they have the same number of columns at this point,
        # and we have enforced that the numbering of members is
        # consecutive starting from one, we can go through all of this
        # CDT's members and look for the matching one.
        for member in self.members.all():
            counterpart = other_CDT.members.get(
                column_idx=member.column_idx)
            if (member.column_name != counterpart.column_name or
                    not member.datatype.is_restriction(
                        counterpart.datatype)):
                return False
        
        # Having reached this point, this CDT must be a restriction
        # of other_CDT.
        return True

    def is_identical(self, other_CDT):
        """
        True if this CDT is identical with its parameter; False otherwise.
        
        This is trivially true if they are the same CDT; otherwise
        the column names and column types must be exactly the same.

        PRE: this CDT and other_CDT are clean.
        """
        return (self.is_restriction(other_CDT) and
                other_CDT.is_restriction(self))

 
class CodeResource(models.Model):
    """
    A CodeResource is any file tracked by ShipYard.
    Related to :model:`copperfish.CodeResourceRevision`
    """

    # Implicitly defined
    #   revisions (codeResourceRevision/ForeignKey)

    name = models.CharField(
            "Resource name",
            max_length=255,
            help_text="The name for this resource");

    filename = models.CharField(
            "Resource file name",
            max_length=255,
            help_text="The filename for this resource",
            blank=True);

    description = models.TextField("Resource description");

    def isValidFileName(self):

        # Code resources have no filenames if they are a meta-package of dependencies
        if self.filename == "":
            return True
    
        # File names cannot start with 1 or more spaces
        if re.search("^\s+", self.filename):
            return False

        # Names cannot end with 1 or more trailing spaces
        if re.search("\s+$", self.filename):
            return False

        # Names must be 1 or more of any from {alphanumeric, space, "-._()"}
        # This will prevent "../" as it contains a slash
        regex = "^[-_.() {}{}]+$".format(string.ascii_letters, string.digits)
        if re.search(regex, self.filename):
            pass
        else:
            return False

        return True

    def clean(self):
        """
        CodeResource name must be valid.

        It must not contain a leading space character or "..",
        must not end in space, and be composed of letters,
        numbers, dash, underscore, paranthesis, and space.
        """
        
        if self.isValidFileName():
            pass
        else:
            raise ValidationError("Invalid code resource filename");


    def __unicode__(self):
        return self.name;
    

class CodeResourceRevision(models.Model):
    """
    A particular revision of a code resource.

    Related to :model:`copperfish.CodeResource`
    Related to :model:`copperfish.CodeResourceDependency`
    Related to :model:`copperfish.Method`
    """

    # Implicitly defined
    #   descendents (self/ForeignKey)
    #   dependencies (CodeResourceDependency/ForeignKey)
    #   needed_by (CodeResourceDependency/ForeignKey)
    #   method_set (Method/ForeignKey)

    coderesource = models.ForeignKey(
            CodeResource,
            related_name="revisions");  
        
    revision_name = models.CharField(
            max_length=128,
            help_text="A name to differentiate revisions of a CodeResource");

    revision_DateTime = models.DateTimeField(
            auto_now_add=True,
            help_text="Date this resource revision was uploaded");

    revision_parent = models.ForeignKey(
            'self',
            related_name="descendants",
            null=True,
            blank=True);

    revision_desc = models.TextField(
            "Revision description",
            help_text="A description for this particular resource revision");

    content_file = models.FileField(
            "File contents",
            upload_to="CodeResources",
            null=True,
            blank=True,
            help_text="File contents of this code resource revision");

    MD5_checksum = models.CharField(
            max_length=64,
            blank=True,
            help_text="Used to validate file contents of this resource revision");

    def __unicode__(self):
        """Represent a resource revision by it's CodeResource name and revision name"""
        
        # Admin can create CR without save() and allow CRRev to be created in memory
        # So, in MEMORY, a revision can temporarily have no corresponding CodeResource
        if not hasattr(self, "coderesource"):
            returnCodeResource = u"[no code resource set]"
        else:
            returnCodeResource = unicode(self.coderesource)

        if self.revision_name == "":
            returnRevisionName = u"[no revision name]"
        else:
            returnRevisionName = unicode(self.revision_name)

        string_rep = unicode(returnCodeResource + ' ' + returnRevisionName)
        return string_rep

    # This CRR includes it's own filename at the root
    def list_all_filepaths(self):
        """Return all filepaths associated with this CodeResourceRevision.

        Filepaths are listed recursively following a root-first scheme,
        with the filepaths of the children listed in order.
        """
        return self.list_all_filepaths_h(self.coderesource.filename)

    # Self is be a dependency CRR, base_name is it's file name, specified either
    # by the parent dependency layer, or in the case of a top-level CR, just CRR.name
    def list_all_filepaths_h(self, base_name):

        # Filepath includes the original file which has dependencies.
        # If just a library of dependencies (IE, base_name=""), don't
        # add base_path.
        all_filepaths = []
        if base_name != "":
            all_filepaths = [unicode(base_name)]

        # For each dependency in this code resource revision
        for dep in self.dependencies.all():

            # Get all file paths of the CR of the child dependency
            # relative to itself
            dep_fn = dep.depFileName;
            # If depFileName is blank, check and see if the
            # corresponding CodeResource had a filename (i.e. if this
            # is a non-metapackage CRR and so there is an associated
            # file).
            if dep_fn == "":
                dep_fn = dep.requirement.coderesource.filename;
            
            inner_dep_paths = dep.requirement.list_all_filepaths_h(dep_fn)

            # Convert the paths from being relative to the child CRR to being
            # relative to the current parent CRR by appending pathing
            # information from the dependency layer
            for paths in inner_dep_paths:
                correctedPath = os.path.join(dep.depPath, paths)
                all_filepaths.append(unicode(correctedPath))

        return all_filepaths

    def has_circular_dependence(self):
        """Detect any circular dependences defined in this CodeResourceRevision."""
        return self.has_circular_dependence_h([]);

    def has_circular_dependence_h(self, dependants):
        """Helper for has_circular_dependence.

        dependants is an accumulator that tracks all of the all of the
        CRRs that have this one as a dependency.
        """
        # Base case: self is dependant on itself, in which case, return true.
        if self in dependants:
            return True;
        
        # Recursive case: go to all dependencies and check them.
        check_dep = False;
        for dep in self.dependencies.all():
            if dep.requirement.has_circular_dependence_h(dependants + [self]):
                check_dep = True;

        return check_dep;

    def clean(self):
        """Check coherence of this CodeResourceRevision.

        Tests for any circular dependency; does this CRR depend on
        itself at all?  Also, checks for conflicts in the
        dependencies.  Finally, if there is a file specified, fill in
        the MD5 checksum.
        """
        # CodeResource can be a collection of dependencies and not contain
        # a file - in this case, MD5 has no meaning and shouldn't exist
        try:
            md5gen = hashlib.md5();
            md5gen.update(self.content_file.read());
            self.MD5_checksum = md5gen.hexdigest();

        except ValueError as e:
            self.MD5_checksum = "";

        # Check for a circular dependency.
        if self.has_circular_dependence():
            raise ValidationError("Self-referential dependency"); 

        # Check if dependencies conflict with each other
        listOfDependencyPaths = self.list_all_filepaths()
        if len(set(listOfDependencyPaths)) != len(listOfDependencyPaths):
            raise ValidationError("Conflicting dependencies");

        # If content file exists, it must have a file name
        if self.content_file and self.coderesource.filename == "":
            raise ValidationError("If content file exists, it must have a file name")

        # If no content file exists, it must not have a file name
        if not self.content_file and self.coderesource.filename != "":
            raise ValidationError("Cannot have a filename specified in the absence of a content file")

    def install(self, install_path):
        """
        Install this CRR into the specified path.

        PRE: install_path exists and has all the sufficient permissions for us
        to write our files into.
        """
        self.install_h(install_path, coderesource.filename)
        
    def install_h(self, install_path, base_name):
        """Helper for install."""
        # Write content_file to [install_path]/base_name.  First we
        # get the file contents.
        curr_code = None
        try:
            self.content_file.open()
            curr_code = self.content_file.read()
        finally:
            self.content_file.close()
            
        with open(os.path.join(install_path, base_name), "wb") as f:
            f.write(curr_code)

        # Make sure this is written with read, write, and execute
        # permission.
        os.chmod(os.path.join(install_path, base_name),
                 stat.S_IRUSR | stat.S_IXUSR)

        for dep in self.dependencies.all():
            # Create any necessary sub-directory.  This should never
            # fail because we're in a nice clean working directory and
            # we already checked that this CRR doesn't have file
            # conflicts.  (Thus if an exception is raised, we want to
            # propagate it as that's a pretty deep problem.)
            path_for_deps = install_path
            if dep.depPath != "":
                path_for_deps = os.makedirs(
                    os.path.join(install_path, dep.depPath))            
            
            # Get the base name of this dependency.  If no special value
            # is specified in dep, then use the dependency's CRR name.
            dep_fn = dep.depFileName;
            if dep_fn == "":
                dep_fn = dep.requirement.coderesource.filename;
            
            dep.requirement.install_h(path_for_deps, dep_fn)

class CodeResourceDependency(models.Model):
    """
    Dependencies of a CodeResourceRevision - themselves also CodeResources.
    Related to :model:`copperfish.CodeResourceRevision`
    """

    coderesourcerevision = models.ForeignKey(CodeResourceRevision,
                         related_name="dependencies");

    # Dependency is a codeResourceRevision
    requirement = models.ForeignKey(CodeResourceRevision,
                                    related_name="needed_by");

    # Where to place it during runtime relative to the CodeResource
    # that relies on this CodeResourceDependency.
    depPath = models.CharField(
        "Dependency path",
        max_length=255,
        help_text="Where a code resource dependency must exist in the sandbox relative to it's parent");

    depFileName = models.CharField(
        "Dependency file name",
        max_length=255,
        help_text="The file name the dependency is given on the sandbox at execution",
        blank=True);

    def clean(self):
        """
        depPath cannot reference ".."
        """

        # Collapse down to a canonical path
        self.depPath = os.path.normpath(self.depPath)

        # Catch ".." on it's own
        if re.search("^\.\.$", self.depPath):
            raise ValidationError("depPath cannot reference ../");

        # Catch "../[whatever]"
        if re.search("^\.\./", self.depPath):
            raise ValidationError("depPath cannot reference ../");

        # This next case actually should never happen since we've collapsed down
        # to a canonical path.
        # Catch any occurrence of "/../" within a larger path (Ex: blah/../bar)
        if re.search("/\.\./", self.depPath):
            raise ValidationError("depPath cannot reference ../");

        # If the child CR is a meta-package (no filename), we cannot
        # have a depFileName as this makes no sense
        if self.requirement.coderesource.filename == "" and self.depFileName != "":
            raise ValidationError("Metapackage dependencies cannot have a depFileName");


    def __unicode__(self):
        """Represent as [codeResourceRevision] requires [dependency] as [dependencyLocation]."""
        return u"{} requires {} as {}".format(
                unicode(self.coderesourcerevision),
                unicode(self.requirement),
                os.path.join(self.depPath, self.depFileName));

class TransformationFamily(models.Model):
    """
    TransformationFamily is abstract and describes common
    parameters between MethodFamily and PipelineFamily.

    Extends :model:`copperfish.MethodFamily`
    Extends :model:`copperfish.PipelineFamily`
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

class MethodFamily(TransformationFamily):
    """
    MethodFamily groups revisions of Methods together.

    Inherits :model:`copperfish.TransformationFamily`
    Related to :model:`copperfish.Method`
    """

    # Implicitly defined:
    #   members (Method/ForeignKey)

    pass

class PipelineFamily(TransformationFamily):
    """
    PipelineFamily groups revisions of Pipelines together.

    Inherits :model:`copperfish.TransformationFamily`
    Related to :model:`copperfish.Pipeline`
    """

    # Implicitly defined:
    #   members (Pipeline/ForeignKey)

    pass


class Transformation(models.Model):
    """
    Abstract class that defines common parameters
    across Method revisions and Pipeline revisions.

    Extends :model:`copperfish.Method`
    Extends :model:`copperfish.Pipeline`
    Related to :model:`TransformationInput`
    Related to :model:`TransformationOutput`
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
    inputs = generic.GenericRelation("TransformationInput");
    outputs = generic.GenericRelation("TransformationOutput");

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

class Method(Transformation):
    """
    Methods are atomic transformations.

    Inherits from :model:`copperfish.Transformation`
    Related to :model:`copperfish.CodeResource`
    Related to :model:`copperfish.MethodFamily`
    """

    family = models.ForeignKey(MethodFamily,related_name="members")
    revision_parent = models.ForeignKey("self",related_name = "descendants",null=True,blank=True)

    # Code resource revisions are executable if they link to Method
    driver = models.ForeignKey(CodeResourceRevision);
    method = models.BooleanField(help_text="Is the output of this method nondeterministic")
    execrecords = generic.GenericRelation("ExecRecord")

    def __unicode__(self):
        """Represent a method by it's revision name and method family"""
        string_rep = u"Method {} {}".format("{}", self.revision_name)

        # MethodFamily may not be temporally saved in DB if created by admin
        if hasattr(self, "family"):
            string_rep = string_rep.format(unicode(self.family))
        else:
            string_rep = string_rep.format("[family unset]")

        return string_rep

    def save(self, *args, **kwargs):
        """
        Create or update a method revision.

        If a method revision being created is derived from a parental
        method revision, copy the parent input/outputs.
        """

        # Inputs/outputs cannot be stored in the database unless this
        # method revision has itself first been saved to the database
        super(Method, self).save(*args, **kwargs)

        # If no parent revision exists, there are no input/outputs to copy
        if self.revision_parent == None:
            return None

        # If parent revision exists, and inputs/outputs haven't been registered,
        # copy all inputs/outputs (Including raws) from parent revision to this revision
        if (self.inputs.count() + self.outputs.count() == 0):
            for parent_input in self.revision_parent.inputs.all():
                new_input = self.inputs.create(
                    dataset_name = parent_input.dataset_name,
                    dataset_idx = parent_input.dataset_idx)
                if not parent_input.is_raw():
                    new_input.structure.create(
                        compounddatatype = parent_input.get_cdt(),
                        min_row = parent_input.get_min_row(),
                        max_row = parent_input.get_max_row())

            for parent_output in self.revision_parent.outputs.all():
                new_output = self.outputs.create(
                    dataset_name = parent_output.dataset_name,
                    dataset_idx = parent_output.dataset_idx)
                if not parent_output.is_raw():
                    new_output.structure.create(
                        compounddatatype = parent_output.get_cdt(),
                        min_row = parent_output.get_min_row(),
                        max_row = parent_output.get_max_row())

    def run_code(self, run_path, input_paths, output_paths,
                 output_handle, error_handle):
        """
        Run the method using the given run path and input/outputs.
        
        This differs from 'execute' in that this is only responsible
        for running code; it does not handle any of the bookkeeping
        of creating ExecRecords and the like.
        
        run_path is the directory in which the code will be run;
        input_paths is a list of input files as expected by the code;
        output_paths is where the code will write the results.
        output_handle and error_handle are writable file handles that
        will capture the stdout and stderr of the code.  More
        specifically, the write mode string must start with "w".

        Returns a subprocess.Popen object which represents the running 
        process.

        Note: how this should work is that whatever calls this creates
        output_handle and error_handle, and monitors those alongside
        the returned subprocess.Popen object.  After the process is
        finished, the caller is responsible for whatever cleanup is
        required.

        PRE: the CRR of this Method is properly Shipyard-formatted, i.e.
        it has the right command-line interface:
        [script name] [input 1] ... [input n] [output 1] ... [output n]
        """
        # If there aren't the right number of inputs or outputs
        # specified, raise a ValueError.
        if (len(input_paths) != self.inputs.count() or 
                len(output_paths) != self.outputs.count()):
            raise ValueError(
                "Method \"{}\" expects {} inputs and {} outputs".
                format(self, self.inputs.count(), self.outputs.count()))

        if (not output_handle.mode.startswith("w") or 
              not error_handle.mode.startswith("w")):
            raise ValueError(
                "output_handle and error_handle must be writable")
        
        # First, check whether run_path exists and is
        # readable/writable/executable by us.
        file_access_utils.set_up_directory(run_path)

        # Now we know that run_path is a valid directory in which to work.

        # Check that all of the inputs exist and are readable by us.
        # We do this by attempting to open the file; we propagate any
        # errors back up.
        for input_path in input_paths:
            f = open(input_path, "rb")
            f.close()

        # Check that all of the outputs do *not* exist and we can
        # create them, i.e. we have write permission on their parent
        # directories.
        for output_path in output_paths:
            can_create, reason = file_access_utils.can_create_file(
                output_path)

            if not can_create:
                raise ValueError(reason)

        # Populate run_path with the CodeResourceRevision.
        driver.install(run_path)

        # At this point, run_path has all of the necessary stuff
        # written into place.  It remains to execute the code.
        # The code to be executed sits in 
        # [run_path]/[driver.coderesource.name],
        # and is executable.
        code_to_run = os.path.join(
            run_path, driver.coderesource.filename)
        code_popen = subprocess.Popen(
            [code_to_run].append(input_paths).append(output_paths), 
            shell=False,
            stdout=output_handle,
            stderr=error_handle)

        return code_popen
            

class Pipeline(Transformation):
    """
    A particular pipeline revision.

    Inherits from :model:`copperfish.Transformation`
    Related to :model:`copperfish.PipelineFamily`
    Related to :model:`copperfish.PipelineStep`
    Related to :model:`copperfish.PipelineOutputCable`
    """

    family = models.ForeignKey(
            PipelineFamily,
            related_name="members")

    revision_parent = models.ForeignKey(
            "self",
            related_name = "descendants",
            null=True,
            blank=True)

    execrecords = generic.GenericRelation("ExecRecord")

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
        super(Pipeline, self).clean();

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
            output_requested = outcable.provider_output

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
    @transaction.commit_on_success
    def create_raw_outcable(self, raw_output_name, raw_output_idx,
                            step_providing_raw_output, provider_raw_output):
        """Creates a raw outcable."""
        new_outcable = self.outcables.create(
            output_name=raw_output_name,
            output_idx=raw_output_idx,
            step_providing_output=step_providing_raw_output,
            provider_output=provider_raw_output)
        new_outcable.full_clean()

        return new_outcable

    # Helper to create non-raw outcables with a default output_cdt equalling
    # that of the providing TO.
    @transaction.commit_on_success
    def create_outcable(self, output_name, output_idx, step_providing_output,
                        provider_output):
        """Creates a non-raw outcable taking output_cdt from the providing TO."""
        new_outcable = self.outcables.create(
            output_name=output_name,
            output_idx=output_idx,
            step_providing_output=step_providing_output,
            provider_output=provider_output,
            output_cdt=provider_output.get_cdt())
        new_outcable.full_clean()

        return new_outcable

class PipelineStep(models.Model):
    """
    A step within a Pipeline representing a single transformation
    operating on inputs that are either pre-loaded (Pipeline inputs)
    or derived from previous pipeline steps within the same pipeline.

    Related to :mode;:`copperfish.Dataset`
    Related to :model:`copperfish.Pipeline`
    Related to :model:`copperfish.Transformation`
    Related to :model:`copperfish.PipelineStepInput`
    Related to :model:`copperfish.PipelineStepDelete`
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
        "TransformationOutput",
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
        if type(self.transformation) == Method:
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
            num_matches = self.cables_in.filter(transf_input=transformation_input).count()
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
            num_matches = self.cables_in.filter(transf_input=transformation_input).count()
            if num_matches == 0:
                raise ValidationError(
                    "Input \"{}\" to transformation at step {} is not cabled".
                    format(transformation_input.dataset_name, self.step_num))

    # Helper to create *raw* cables.  This is really just so that all our
    # unit tests can be easily amended; going forwards, there's no real reason
    # to use this.
    @transaction.commit_on_success
    def create_raw_cable(self, transf_raw_input, pipeline_raw_input):
        """
        Create a raw cable feeding this PipelineStep.
        """
        new_cable = self.cables_in.create(
            transf_input=transf_raw_input,
            step_providing_input=0,
            provider_output=pipeline_raw_input)
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
    @transaction.commit_on_success
    def add_deletion(self, dataset_to_delete):
        """
        Mark a TO for deletion.
        """
        self.outputs_to_delete.add(dataset_to_delete)


# A helper function that will be called both by PSICs and
# POCs to tell whether they are trivial.
def cable_trivial_h(cable, cable_wires):
    """
    Helper called by both PSICs and POCs to check triviality.
    
    If a cable is raw, it is trivial.  If it is not raw, then it
    is trivial if it either has no wiring, or if the wiring is
    trivial (i.e. mapping corresponding pin to corresponding pin
    without changing names or anything).
    
    PRE: cable is clean (and therefore so are its wires); cable_wires
    is a QuerySet containing cable's custom wires.
    """
    if cable.is_raw():
        return True
        
    if not cable_wires.exists():
        return True

    # At this point, we know there are wires.
    for wire in cable_wires:
        if (wire.source_pin.column_idx != wire.dest_pin.column_idx or
                wire.source_pin.column_name != wire.dest_pin.column_name):
            return False

    # All the wiring was trivial, so....
    return True


# Helper that will be called by both PSIC and POC.
def run_cable_h(wires, source, output_path):
    """
    Perform the cable-specified transformation on the input.

    wire_qs is the QuerySet containing the custom wires defined
    for this cable.
    """

    # Read/write binary files in chunks of 8 megabytes
    chunkSize = 1024*8
    
    if type(source) == str and self.is_trivial():
        # If trivial, make a link from source to output_path.
        # FIXME: for Windows we may not be able to use sym links
        os.link(source, output_path)
        return

    if type(source) == Dataset and self.is_trivial():
        # Write the dataset contents into the file output_path.
        try:
            source.dataset_file.open()
            with open(output_path,"wb") as outfile:
                chunk = source.dataset_file.read(chunkSize)
                while chunk != "":
                    outfile.write(chunk)
                    chunk = source.dataset_file.read(chunkSize)
        finally:
            source.dataset_file.close()
        return
        
    # The cable is not trivial.  Make a dict that encapsulates the
    # mapping required: keyed by the output column name, with value
    # being the input column name.
    source_of = {}
    column_names_by_idx = {}
    for wire in wires:
        source_of[wire.dest_pin.column_name] = (
            wire.source_pin.column_name)
        column_names_by_idx[wire.dest_pin.column_idx] = (
            wire.dest_pin.column_name)
        
    # Construct a list with the column names in the appropriate order.
    output_fields = [column_names_by_idx[i] 
                     for i in sorted(column_names_by_idx)]

    try:
        infile = None
        if type(source) == Dataset:
            infile = source.dataset_file
            infile.open()
        else:
            infile = open(source, "rb")
            
        input_csv = csv.DictReader(infile)

        with open(output_path, "wb") as outfile:
            output_csv = csv.DictWriter(outfile,
                                        fieldnames=output_fields)
            output_csv.writeheader()
            
            for source_row in input_csv:
                # row looks like {col1name: col1val, col2name:
                # col2val, ...}.
                dest_row = {}
                
                # source_of looks like:
                # {outcol1: sourcecol5, outcol2: sourcecol1, ...}
                for out_col_name in source_of:
                    dest_row[out_col_name] = source_row[source_of[out_col_name]]
                    output_csv.writerow(dest_row)

    finally:
        infile.close()
        

class PipelineStepInputCable(models.Model):
    """
    Represents the "cables" feeding into the transformation of a
    particular pipeline step, specifically:

    A) Destination of cable (transf_input_name) - step implicitly defined
    B) Source of the cable (step_providing_input, provider_output_name)

    Related to :model:`copperfish.PipelineStep`
    """
    # The step (Which has a transformation) where we define incoming cabling
    pipelinestep = models.ForeignKey(
        PipelineStep,
        related_name = "cables_in");
    
    # Input hole (TransformationInput) of the transformation
    # at this step to which the cable leads
    transf_input = models.ForeignKey(
        "TransformationInput",
        help_text="Wiring destination input hole");
    
    
    # (step_providing_input, provider_output) unambiguously defines
    # the source of the cable.  step_providing_input can't refer to a PipelineStep
    # as it might also refer to the pipeline's inputs (i.e. step 0).
    step_providing_input = models.PositiveIntegerField("Step providing the input source",
                                                       help_text="Cabling source step");

    content_type = models.ForeignKey(
            ContentType,
            limit_choices_to = {"model__in": ("TransformationOutput",
                                              "TransformationInput")});
    object_id = models.PositiveIntegerField();
    # Wiring source output hole.
    provider_output = generic.GenericForeignKey("content_type", "object_id");

    custom_wires = generic.GenericRelation("CustomCableWire")

    execrecords = generic.GenericRelation("ExecRecord")

    # October 15, 2013: allow the data coming out of a PSIC to be
    # saved.  Note that this is only relevant if the PSIC is not
    # trivial, and is false by default.
    keep_output = models.BooleanField(
        "Whether or not to retain the output of this PSIC",
        help_text="Keep or delete output",
        default=False)

    # step_providing_input must be PRIOR to this step (Time moves forward)

    # Coherence of data is already enforced by Pipeline

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
        return "{}:{}{}".format(step_str, self.transf_input.dataset_name, is_raw_str);

    
    def clean(self):
        """Check coherence of the cable.

        Check in all cases:
        - Are the input and output either both raw or both non-raw?

        If the cable is raw:
        - Does the input come from the Pipeline?
        - Are there any wires defined?  (There shouldn't be!)

        If the cable is not raw:
        - Does the input come from a prior step?
        - Does the cable map to an (existent) input of this step's transformation?
        - Does the requested output exist?
        - Do the input and output 'work together' (compatible min/max)?

        Whether the input and output have compatible CDTs or have valid custom
        wiring is checked via clean_and_completely_wired.
        """
        input_requested = self.provider_output;
        feed_to_input = self.transf_input;

        if input_requested.is_raw() != feed_to_input.is_raw():
            raise ValidationError(
                "Cable \"{}\" has mismatched source (\"{}\") and destination (\"{}\")".
                format(self, input_requested, feed_to_input))

        if self.is_raw():
            self.raw_clean()
        else:
            self.non_raw_clean()

    def raw_clean(self):
        """
        Helper function called by clean() to deal with raw cables.
        
        PRE: the pipeline step's transformation is not the parent pipeline (this should
        never happen anyway).
        PRE: cable is raw (i.e. the source and destination are both raw); this is enforced
        by clean().
        """
        input_requested = self.provider_output
        feed_to_input = self.transf_input
        step_trans = self.pipelinestep.transformation

        # If this cable is raw, does step_providing_input == 0?
        if self.is_raw() and self.step_providing_input != 0:
            raise ValidationError(
                "Cable \"{}\" must have step 0 for a source".
                format(self))

        # Does this input cable come from a raw input of the parent pipeline?
        # Note: this depends on the pipeline step's transformation not equalling
        # the parent pipeline (which shouldn't ever happen).
        if not self.pipelinestep.pipeline.inputs.filter(pk=input_requested.pk).exists():
            raise ValidationError(
                "Step {} requests raw input not coming from parent pipeline".
                format(self.pipelinestep.step_num))

        # Does the specified input defined for this transformation exist?
        if not step_trans.inputs.filter(pk=feed_to_input.pk).exists():
            raise ValidationError(
                "Transformation at step {} does not have raw input \"{}\"".
                format(self.pipelinestep.step_num, unicode(feed_to_input)))

        # Are there any wires defined?
        if self.custom_wires.all().exists():
            raise ValidationError(
                "Cable \"{}\" is raw and should not have custom wiring defined".
                format(self))

    def non_raw_clean(self):
        """Helper function called by clean() to deal with non-raw cables."""
        input_requested = self.provider_output;
        requested_from = self.step_providing_input;
        feed_to_input = self.transf_input;
        step_trans = self.pipelinestep.transformation

        # Does this input cable come from a step prior to this one?
        if requested_from >= self.pipelinestep.step_num:
            raise ValidationError(
                "Step {} requests input from a later step".
                format(self.pipelinestep.step_num));

        # Does the specified input defined for this transformation exist?
        if not step_trans.inputs.filter(pk=feed_to_input.pk).exists():
            raise ValidationError ("Transformation at step {} does not have input \"{}\"".
                                   format(self.pipelinestep.step_num, unicode(feed_to_input)));

        # Do the source and destination work together?
        # This checks:
        # - the source produces the requested data
        # - the source doesn't delete the requested data
        # - they have compatible min_row and max_row

        if requested_from == 0:
            # Get pipeline inputs of the cable's parent Pipeline,
            # and look for pipeline inputs that match the desired input.
            
            pipeline_inputs = self.pipelinestep.pipeline.inputs.all();
            if input_requested not in pipeline_inputs:
                raise ValidationError(
                    "Pipeline does not have input \"{}\"".
                    format(unicode(input_requested)));

        # If not from step 0, input derives from the output of a pipeline step
        else:
            # Look at the pipeline step referenced by the wiring parameter
            providing_step = self.pipelinestep.pipeline.steps.get(step_num=requested_from)

            # Does the source pipeline step produce the output requested?
            source_step_outputs = providing_step.transformation.outputs.all()
            if input_requested not in source_step_outputs:
                raise ValidationError(
                    "Transformation at step {} does not produce output \"{}\"".
                    format(requested_from, unicode(input_requested)))

        # Check that the input and output connected by the
        # cable are compatible re: number of rows.  Don't check for
        # ValidationError because this was checked in the
        # clean() of PipelineStep.

        provided_min_row = 0
        required_min_row = 0

        # Source output row constraint
        if input_requested.get_min_row() != None:
            provided_min_row = input_requested.get_min_row()

        # Destination input row constraint
        if feed_to_input.get_min_row() != None:
            required_min_row = feed_to_input.get_min_row()

        # Check for contradictory min row constraints
        if (provided_min_row < required_min_row):
            raise ValidationError(
                "Data fed to input \"{}\" of step {} may have too few rows".
                format(feed_to_input.dataset_name, self.pipelinestep.step_num))

        provided_max_row = float("inf")
        required_max_row = float("inf")

        if input_requested.get_max_row() != None:
            provided_max_row = input_requested.get_max_row()

        if feed_to_input.get_max_row() != None:
            required_max_row = feed_to_input.get_max_row()

        # Check for contradictory max row constraints
        if (provided_max_row > required_max_row):
            raise ValidationError(
                "Data fed to input \"{}\" of step {} may have too many rows".
                format(feed_to_input.dataset_name, self.pipelinestep.step_num))

        # Validate whatever wires there already are
        if self.custom_wires.all().exists():
            for wire in self.custom_wires.all():
                wire.clean()

        
    def clean_and_completely_wired(self):
        """
        Check coherence of the cable, and check that it is correctly wired (if it is non-raw).

        This will call clean() as well as checking whether the input
        and output 'work together'.  That is, either both are raw, or
        neither are non-raw and:
         - the source CDT is a restriction of the destination CDT; or
         - there is good wiring defined.
        """
        # Check coherence of this cable otherwise.
        self.clean();

        # There are no checks to be done on wiring if this is a raw cable.
        if self.is_raw():
            return
        
        input_requested = self.provider_output;
        feed_to_input = self.transf_input;
        
        # If source CDT cannot feed (i.e. is not a restriction of)
        # destination CDT, check presence of custom wiring
        if not input_requested.get_cdt().is_restriction(feed_to_input.get_cdt()):
            if not self.custom_wires.all().exists():
                raise ValidationError(
                    "Custom wiring required for cable \"{}\"".
                    format(unicode(self)));

        # Validate whatever wires there are.
        if self.custom_wires.all().exists():
            # Each destination CDT member of must be wired to exactly once.

            # Get the CDT members of transf_input
            dest_members = self.transf_input.get_cdt().members.all()

            # For each CDT member, check that there is exactly 1
            # custom_wire leading to it (IE, number of occurences of
            # CDT member = dest_pin)
            for dest_member in dest_members:
                numwires = self.custom_wires.filter(dest_pin=dest_member).count()

                if numwires == 0:
                    raise ValidationError(
                        "Destination member \"{}\" has no wires leading to it".
                        format(unicode(dest_member)));

                if numwires > 1:
                    raise ValidationError(
                        "Destination member \"{}\" has multiple wires leading to it".
                        format(unicode(dest_member)));

    def is_raw(self):
        """True if this cable maps raw data; false otherwise."""
        return self.transf_input.is_raw()

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

        if self.transf_input != other_cable.transf_input:
            return False

        # Now we know that they feed the same TransformationInput.
        if self.is_raw():
            return True

        # From here on, we assume both cables are non-raw.
        # (They must be, since both feed the same TI and self
        # is not raw.)
        if not self.provider_output.get_cdt().is_restriction(
                other_cable.provider_output.get_cdt()):
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

    # NOTE October 15, 2013: is this actually that useful?  I think
    # we're going to need is_compatible_given_input more.
    def is_compatible(self, other_cable, source_CDT):
        """
        Checks if a cable is compatible wrt specified CDT.
        
        The specified cable and this one are compatible if:
         - both can be fed by source_CDT
         - both feed the same TransformationInput
         - both are trivial, or the wiring matches
        
        For two cables' wires to match, any wire connecting column
        indices (source_idx, dest_idx) must appear in both cables.

        PRE: self, other_cable are clean.
        """
        # Both cables can be fed by source_CDT if source_CDT is
        # a restriction of their provider_outputs' CDTs.
        if (not source_CDT.is_restriction(self.provider_output.get_cdt()) or
                not source_CDT.is_restriction(
                    other_cable.provider_output.get_cdt())):
            return False
        
        # After this point, all of the checks are the same as for
        # is_compatible_given_input.
        return self.is_compatible_given_input(other_cable)

    def is_compatible_given_input(self, other_cable):
        """
9        Check compatibility of two cables having the same input.

        Given that both had the same input, they are compatible if:
         - both feed the same TransformationInput
         - both are trivial, or the wiring matches
        
        For two cables' wires to match, any wire connecting column
        indices (source_idx, dest_idx) must appear in both cables.

        PRE: self, other_cable are clean, and both can be fed the
        same input SymbolicDataset.
        """
        # Both cables can be fed by source_CDT if source_CDT is
        # a restriction of their provider_outputs' CDTs.
        if self.transf_input != other_cable.transf_input:
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

    def run_cable(self, source, output_path):
        """
        Perform the cable-specified transformation on the input.

        This uses run_cable_h.

        source can either be a Dataset or a path to a file.
        """
        run_cable_h(self.custom_wires.all(), source, output_path)

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
        CompoundDatatypeMember,
        related_name="source_pins")

    # CDT member on the destination input hole
    dest_pin = models.ForeignKey(
        CompoundDatatypeMember,
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
        source_CDT_members = self.cable.provider_output.get_cdt().members.all() # Duck-typing
        dest_CDT = None
        dest_CDT_members = None
        if type(self.cable) == PipelineStepInputCable:
            dest_CDT = self.cable.transf_input.get_cdt()
            dest_CDT_members = dest_CDT.members.all()
        else:
            dest_CDT = self.cable.output_cdt
            dest_CDT_members = dest_CDT.members.all()

        if not source_CDT_members.filter(pk=self.source_pin.pk).exists():
            raise ValidationError(
                "Source pin \"{}\" does not come from compounddatatype \"{}\"".
                format(self.source_pin,
                       self.cable.provider_output.get_cdt()))

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

    Related to :model:`copperfish.Pipeline`
    Related to :model:`copperfish.TransformationOutput` (Refactoring needed)
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
    output_cdt = models.ForeignKey(CompoundDatatype,
                                   blank=True,
                                   null=True,
                                   related_name="cables_leading_to")

    # PRE: step_providing_output refers to an actual step of the pipeline
    # and provider_output_name actually refers to one of the outputs
    # at that step
    # The coherence of the data here will be enforced at the Python level
    step_providing_output = models.PositiveIntegerField(
        "Source pipeline step number",
        validators=[MinValueValidator(1)],
        help_text="Source step at which output comes from")

    provider_output = models.ForeignKey(
        "TransformationOutput",
        help_text="Source output hole")

    custom_outwires = generic.GenericRelation("CustomCableWire")
    execrecords = generic.GenericRelation("ExecRecord")
    
    # Enforce uniqueness of output names and indices.
    # Note: in the pipeline, these will still need to be compared with the raw
    # output names and indices.
    class Meta:
        unique_together = (("pipeline", "output_name"),
                           ("pipeline", "output_idx"));

    def __unicode__(self):
        """ Represent with the pipeline name, output index, and output name (???) """
        pipeline_name = "[no pipeline set]";
        if self.pipeline != None:
            pipeline_name = unicode(self.pipeline);

        is_raw_str = ""
        if self.is_raw():
            is_raw_str = " (raw)"

        return "{}:{} ({}{})".format(pipeline_name, self.output_idx,
                                     self.output_name, is_raw_str);

    def clean(self):
        """
        Checks coherence of this output cable.
        
        PipelineOutputCable must reference an existant, undeleted
        transformation output hole.  Also, if the cable is raw, there
        should be no custom wiring.  If the cable is not raw and there
        are custom wires, they should be clean.
        """
        output_requested = self.provider_output;
        requested_from = self.step_providing_output;

        # Step number must be valid for this pipeline
        if requested_from > self.pipeline.steps.all().count():
            raise ValidationError(
                "Output requested from a non-existent step");
        
        providing_step = self.pipeline.steps.get(step_num=requested_from);

        # Try to find a matching output hole
        if not providing_step.transformation.outputs.filter(pk=output_requested.pk).exists():
            raise ValidationError(
                "Transformation at step {} does not produce output \"{}\"".
                format(requested_from, output_requested));

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


        # The cable has a raw source (and output_cdt is None)
        if self.is_raw():

            # Wires cannot exist
            if outwires.exists():
                raise ValidationError(
                    "Cable \"{}\" is raw and should not have wires defined" .
                    format(self))

        # The cable has a nonraw source (and output_cdt is specified)
        else:
            if not self.provider_output.get_cdt().is_restriction(
                    self.output_cdt) and not outwires.exists():
                raise ValidationError(
                    "Cable \"{}\" has a source CDT that is not a restriction of its target CDT, but no wires exist".
                    format(self))

            # Clean all wires
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
        if not self.is_raw() and self.custom_outwires.all().exists():
            # Check that each CDT member has a wire leading to it
            for dest_member in self.output_cdt.members.all():
                if not self.custom_outwires.filter(dest_pin=dest_member).exists():
                    raise ValidationError(
                        "Destination member \"{}\" has no outwires leading to it".
                        format(dest_member))

    def is_raw(self):
        """True if this output cable is raw; False otherwise."""
        return self.provider_output.is_raw()


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

        if self.provider_output != other_outcable.provider_output:
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
        Checks if an outcable is compatible with this one.
        
        The specified cable and this one are compatible if:
         - both are fed by the same TransformationOutput
         - both are trivial, or the wiring matches
        
        For two cables' wires to match, any wire connecting column
        indices (source_idx, dest_idx) must appear in both cables.

        PRE: self, other_outcable are clean.
        """
        if self.provider_output != other_outcable.provider_output:
            return False

        if self.is_trivial() and other_outcable.is_trivial():
            return True

        # We know they are fed by the same TransformationOutput
        # and are non-trivial.  As such, we have to check that
        # their wiring matches.
        for wire in self.custom_outwires.all():
            # Get the corresponding wire in other_outcable.
            corresp_wire = other_outcable.custom_outwires.get(
                dest_pin=wire.dest_pin)

            if (wire.source_pin.column_idx !=
                    corresp_wire.source_pin.column_idx):
                return False

        # By the fact that self and other_outcable are clean, we know
        # that we have checked all the wires.  Having made sure all of
        # the wiring matches, we can....
        return True
        
    def run_cable(self, source, output_path):
        """
        Perform the cable-specified transformation on the input.

        This uses run_cable_h.
        """
        run_cable_h(self.custom_outwires.all(), source, output_path)

# August 20, 2013: changed the structure of our Xputs so that there is no distinction
# between raw and non-raw Xputs beyond the existence of an associated "structure"
class TransformationXput(models.Model):
    """
    Describes parameters common to all inputs and outputs
    of transformations - the "holes"

    Related to :models:`copperfish.Transformation`
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

    execrecordouts_referencing = generic.GenericRelation("ExecRecordOut")

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

    Related to :model:`copperfish.TransformationXput`
    """
    content_type = models.ForeignKey(
        ContentType,
        limit_choices_to = {"model__in": ("TransformationInput", "TransformationOutput")});
    object_id = models.PositiveIntegerField();
    transf_xput = generic.GenericForeignKey("content_type", "object_id")

    # The expected compounddatatype of the input/output
    compounddatatype = models.ForeignKey(CompoundDatatype);
    
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
    Inherits from :model:`copperfish.TransformationXput`
    """
    pass

class TransformationOutput(TransformationXput):
    """
    Inherits from :model:`copperfish.TransformationXput`
    """
    pass

class Run(models.Model):
    """
    Stores data associated with an execution of a pipeline.

    Related to :model:`copperfish.Pipeline`
    Related to :model:`copperfish.RunStep`
    Related to :model:`copperfish.Dataset`
    """
    user = models.ForeignKey(User, help_text="User who performed this run")
    start_time = models.DateTimeField("start time", auto_now_add=True,
                                      help_text="Time at start of run")
    pipeline = models.ForeignKey(
        Pipeline,
        related_name="pipeline_instances",
        help_text="Pipeline used in this run")

    name = models.CharField("Run name", max_length=256)
    description = models.TextField("Run description", blank=True)
    
    # If run was spawned within another run, parent_runstep denotes
    # the run step that initiated it
    parent_runstep = models.OneToOneField(
        "RunStep",
        related_name="child_run",
        null=True,
        blank=True,
        help_text="Step of parent run initiating this one as a sub-run")

    execrecord = models.ForeignKey(
        "ExecRecord",
        null=True,
        blank=True,
        related_name="runs",
        help_text="Record of this run");

    reused = models.NullBooleanField(
        help_text="Indicates whether this run uses the record of a previous execution",
        default=None);

    def clean(self):
        """
        Checks coherence of the run (possibly in an incomplete state).

        The procedure:
         - if parent_runstep is not None, then pipeline should be consistent with it
         - if reused is None, then execrecord should not be set and there should be
           no RunSteps or RunOutputCables associated, and exit
        (from here on reused is assumed to be set)
         - if reused is True:
           - there should not be any RunSteps or RunOutputCables associated
         - else (reused is False):
           - check RSs; no RS should be associated without the previous ones being
             complete
           - if not all RSs are complete, no ROCs should be associated, ER should
             not be set
          (from here on all RSs are assumed to be complete)
           - clean all associated ROCs
           - if not all ROCs are complete, ER should not be set
          (from here on all ROCs are assumed to be complete)
        (from here on execrecord is assumed to be set)
         - check that execrecord is clean and complete
         - check that execrecord is consistent with pipeline
         - if this run did not reuse an ER, check that all EROs have a corresponding ROC
        """
        if (self.parent_runstep != None and
                self.pipeline != self.parent_runstep.pipelinestep.transformation):
            raise ValidationError(
                "Pipeline of Run \"{}\" is not consistent with its parent RunStep".
                format(self))
        
        if self.reused == None:
            if self.runsteps.all().exists():
                raise ValidationError(
                    "Run \"{}\" has not decided whether or not to reuse an ER yet, so there should be no associated RunSteps".
                    format(self))

            if self.runoutputcables.all().exists():
                raise ValidationError(
                    "Run \"{}\" has not decided whether or not to reuse an ER yet, so there should be no associated RunOutputCables".
                    format(self))

            if self.execrecord != None:
                raise ValidationError(
                    "Run \"{}\" has not decided whether or not to reuse an ER yet, so execrecord should not be set")
            
            return

        # From here on reused is assumed to be set.
        elif self.reused:
            if self.runsteps.all().exists():
                raise ValidationError(
                    "Run \"{}\" reuses an ER, so there should be no associated RunSteps".
                    format(self))

            if self.runoutputcables.all().exists():
                raise ValidationError(
                    "Run \"{}\" reuses an ER, so there should be no associated RunOutputCables".
                    format(self))

        else:
            # If no steps are registered yet, simply return.
            if not self.runsteps.all().exists():
                return
            
            # Check that steps are proceeding in order.  (Multiple quenching
            # of steps is taken care of already.)
            steps_associated = sorted(
                [rs.pipelinestep.step_num for rs in self.runsteps.all()])

            if steps_associated != range(1, len(steps_associated)+1):
                raise ValidationError(
                    "RunSteps of Run \"{}\" are not consecutively numbered starting from 1".
                    format(self))

            # All steps prior to the last registered one must be complete.
            for curr_step_num in steps_associated[:-1]:
                self.runsteps.get(pipelinestep__step_num=curr_step_num).complete_clean()

            # The most recent step should be clean.
            most_recent_step = self.runsteps.get(
                pipelinestep__step_num=steps_associated[-1])
            most_recent_step.clean()

            # If the last step is not complete, then no ROCs should be
            # associated.
            if (steps_associated[-1] < self.pipeline.steps.count() or 
                    not most_recent_step.is_complete()):
                if self.runoutputcables.all().exists():
                    raise ValidationError(
                        "Run \"{}\" has not completed all of its RunSteps, so there should be no associated RunOutputCables".
                        format(self))
                
                if self.execrecord != None:
                    raise ValidationError(
                        "Run \"{}\" has not completed all of its RunSteps, so execrecord should not be set".
                        format(self))
                return

            # From this point on, all RunSteps are assumed to be complete.

            # Run clean on all of its outcables.
            for run_outcable in self.runoutputcables.all():
                run_outcable.clean()

            # If not all ROCs are complete, there should be no ER set.
            all_outcables_complete = True
            for outcable in self.pipeline.outcables.all():
                corresp_roc = self.runoutputcables.filter(pipelineoutputcable=outcable)
                if not corresp_roc.exists() or not corresp_roc[0].is_complete():
                    all_outcables_complete = False
                    break

            if not all_outcables_complete and self.execrecord != None:
                raise ValidationError(
                    "Run \"{}\" has not completed all of its RunOutputCables, so execrecord should not be set".
                    format(self))

        if self.execrecord == None:
            return

        # From this point on, execrecord is assumed to be set.
        self.execrecord.complete_clean()

        # The ER must point to the same pipeline that this run points to
        if self.pipeline != self.execrecord.general_transf:
            raise ValidationError(
                "Run \"{}\" points to pipeline \"{}\" but corresponding ER does not".
                format(self, self.pipeline))

        # If this run did not reuse an ER, check that every ERO has a corresponding
        # RunOutputCable (we know it to be clean by checking above).
        if not self.reused:
            for ero in self.execrecord.execrecordouts.all():
                curr_output = ero.generic_output
                corresp_roc = self.runoutputcables.filter(
                    pipelineoutputcable__output_name=curr_output.dataset_name)

                # October 9, 2013: this actually cannot happen, because by this
                # point we have to have quenched all of the ROCs.
                # if not corresp_roc.exists():
                #     raise ValidationError(
                #         "ExecRecord of Run \"{}\" has an entry for output \"{}\" but no corresponding RunOutputCable exists".
                #         format(self, curr_output))

                # Now corresp_roc is assumed to exist: it should have the
                # same SymbolicDataset as the ERO.
                if (corresp_roc[0].execrecord.execrecordouts.all()[0].symbolicdataset !=
                        ero.symbolicdataset):
                    raise ValidationError(
                        "ExecRecordOut \"{}\" of Run \"{}\" does not match the corresponding RunOutputCable".
                        format(ero, self))
    
    def is_complete(self):
        """True if this run is complete; false otherwise."""
        return self.execrecord != None
            
    def complete_clean(self):
        """Checks completeness and coherence of a run."""
        self.clean()
        if not self.is_complete():
            raise ValidationError(
                "Run \"{}\" has no ExecRecord".format(self))

class RunOutputCable(models.Model):
    """
    Annotates the action of a PipelineOutputCable within a run.

    Related to :model:`copperfish.Run`
    Related to :model:`copperfish.ExecRecord`
    Related to :model:`copperfish.PipelineOutputCable`
    """
    run = models.ForeignKey(Run, related_name="runoutputcables")
    execrecord = models.ForeignKey(
        "ExecRecord",
        null=True, blank=True,
        related_name="runoutputcables")
    reused = models.NullBooleanField(
        help_text="Denotes whether this run reused the action of an output cable",
        default=None)
    pipelineoutputcable = models.ForeignKey(
        PipelineOutputCable,
        related_name="poc_instances")
    
    output = generic.GenericRelation("Dataset")

    class Meta:
        # Uniqueness constraint ensures that no POC is multiply-represented
        # within a run.
        unique_together = ("run", "pipelineoutputcable")

    def clean(self):
        """
        Check coherence of this RunOutputCable.

        In sequence, the checks we perform are:
         - pipelineoutputcable belongs to run.pipeline
         - if it has been decided not to reuse an ER:
           - if this cable is trivial, there should be no associated dataset
           - otherwise, clean any associated dataset
         - else if it has been decided to reuse an ER, check that there
           are no associated datasets
         - else if no decision has been made, check that no data has
           been associated, and that ER is unset
        (after this point it is assumed that ER is set)
         - check that it is complete and clean
         - check that it's coherent with pipelineoutputcable
         - if this ROC was not reused, any associated dataset should belong
           to the corresponding ERO
         - if this ROC's output was not marked for deletion, the corresponding
           ERO should have existent data associated
         - if the POC's output was not marked for deletion, the POC is not trivial,
           and this ROC did not reuse an ER, then this ROC should have existent
           data associated
        """
        if (not self.run.pipeline.outcables.
                filter(pk=self.pipelineoutputcable.pk).exists()):
            raise ValidationError(
                "POC \"{}\" does not belong to Pipeline \"{}\"".
                format(self.pipelineoutputcable, self.run.pipeline))

        if self.reused == None:
            if self.has_data():
                raise ValidationError(
                    "RunOutputCable \"{}\" has not decided whether or not to reuse an ExecRecord; no Datasets should be associated".
                    format(self))

            if self.execrecord != None:
                raise ValidationError(
                    "RunOutputCable \"{}\" has not decided whether or not to reuse an ExecRecord; execrecord should not be set yet".
                    format(self))

        elif self.reused:
            if self.has_data():
                raise ValidationError(
                    "RunOutputCable \"{}\" reused an ExecRecord and should not have generated any Datasets".
                    format(self))
        else:
            # If this cable is trivial, there should be no data associated.
            if self.pipelineoutputcable.is_trivial():
                if self.has_data():
                    raise ValidationError(
                        "RunOutputCable \"{}\" is trivial and should not have generated any Datasets".
                        format(self))

            # Otherwise, check that there is at most one Dataset attached, and
            # clean it.
            elif self.has_data():
                if self.output.count() > 1:
                    raise ValidationError(
                        "RunOutputCable \"{}\" should generate at most one Dataset".
                        format(self))
                self.output.all()[0].clean()

        if self.execrecord == None:
            return

        # self.execrecord is set, so complete_clean it.
        self.execrecord.complete_clean()

        # The ER must point to a cable that is compatible with the one
        # this RunOutputCable points to.
        if type(self.execrecord.general_transf) != PipelineOutputCable:
            raise ValidationError(
                "ExecRecord of RunOutputCable \"{}\" does not represent a POC".
                format(self))
        
        elif not self.pipelineoutputcable.is_compatible(self.execrecord.general_transf):
            raise ValidationError(
                "POC of RunOutputCable \"{}\" is incompatible with that of its ExecRecord".
                format(self))

        is_deleted = False
        if self.run.parent_runstep != None:
            is_deleted = self.run.parent_runstep.pipelinestep.outputs_to_delete.filter(
                dataset_name=self.pipelineoutputcable.output_name).exists()

        # If the output of this ROC is marked for deletion, there should be no data
        # associated.
        if is_deleted:
            if self.has_data():
                raise ValidationError(
                    "RunOutputCable \"{}\" is marked for deletion; no data should be produced".
                    format(self))
        # If it isn't marked for deletion:
        else:
            # The corresponding ERO should have existent data.
            corresp_ero = self.execrecord.execrecordouts.get(execrecord=self.execrecord)
            if not corresp_ero.has_data():
                raise ValidationError(
                    "RunOutputCable \"{}\" was not deleted; ExecRecordOut \"{}\" should reference existent data".
                    format(self, corresp_ero))

            # If the step was not reused and the cable was not
            # trivial, there should be data associated to this ROC.
            if not self.reused and not self.pipelineoutputcable.is_trivial():
                if not self.has_data():
                    raise ValidationError(
                        "RunOutputCable \"{}\" was not reused, trivial, or deleted; it should have produced data".
                        format(self))

                # The associated data should belong to the ERO of
                # self.execrecord (which has already been checked for
                # completeness and cleanliness).
                if not self.execrecord.execrecordouts.filter(
                        symbolicdataset=self.output.all()[0].symbolicdataset).exists():
                    raise ValidationError(
                        "Dataset \"{}\" was produced by RunOutputCable \"{}\" but is not in an ERO of ExecRecord \"{}\"".
                        format(self.output.all()[0], self, self.execrecord))

            

    def is_complete(self):
        """True if ROC is finished running; false otherwise."""
        return self.execrecord != None

    def complete_clean(self):
        """Check completeness and coherence of this RunOutputCable."""
        self.clean()
        if not self.is_complete():
            raise ValidationError(
                "RunOutputCable \"{}\" has no ExecRecord".format(self))

    def has_data(self):
        """True if associated output exists; False if not."""
        return self.output.all().exists()
        
class RunStep(models.Model):
    """
    Annotates the execution of a pipeline step within a run.

    Related to :model:`copperfish.Run`
    Related to :model:`copperfish.ExecRecord`
    Related to :model:`copperfish.PipelineStep`
    """
    run = models.ForeignKey(Run, related_name="runsteps")

    # If this RunStep has a child_run, then this execrecord may be null
    # (and you would look at child_run's execrecord).
    execrecord = models.ForeignKey(
        "ExecRecord",
        null=True, blank=True,
        related_name="runsteps")
    reused = models.NullBooleanField(
        default=None,
        help_text="Denotes whether this run step reuses a previous execution")
    pipelinestep = models.ForeignKey(
        PipelineStep,
        related_name="pipelinestep_instances")

    outputs = generic.GenericRelation("Dataset")

    class Meta:
        # Uniqueness constraint ensures you can't have multiple RunSteps for
        # a given PipelineStep within a Run.
        unique_together = ("run", "pipelinestep")

    def clean(self):
        """
        Check coherence of this RunStep.

        The checks we perform, in sequence:
         - pipelinestep is consistent with run
         - if pipelinestep is for a method, there should be no child_run
         - if any RSICs exist, check they are clean and complete.
         - if all RSICs are not quenched, reused, child_run, and execrecord should not be set, and no Datasets should be associated
        (from here on all RSICs are assumed to be quenched)
         - if we haven't decided whether or not to reuse an ER, child_run and execrecord should not be set, and no Datasets should be associated.
        (from here on, reused is assumed to be set)
         - if we are reusing an ER, check that:
           - there are no associated Datasets.
           - there is no child_run
         - else if we are not reusing an ER:
           - clean any associated Datasets
           - clean child_run if it exists
           - if child_run exists, execrecord should not be set
        (from here on, child_run is assumed to be appropriately set or blank)
        (from here on, execrecord or child_run.execrecord is assumed to be set)
         - check that it is complete and clean
         - check that it's coherent with pipelinestep
         - if an output is marked for deletion, there should be no associated Dataset
         - else:
           - the corresponding ERO should have an associated Dataset.
           - if this RunStep was not reused, that ERO's dataset should be associated
             to this RunStep.
         - any associated Dataset belongs to an ERO (this checks for Datasets that
           have been wrongly assigned to this RunStep)
        
        Note: don't need to check inputs for multiple quenching due to uniqueness.
        Quenching of outputs is checked by ExecRecord.
        """
        # Does pipelinestep belong to run.pipeline?
        if not self.run.pipeline.steps.filter(pk=self.pipelinestep.pk).exists():
            raise ValidationError(
                "PipelineStep \"{}\" of RunStep \"{}\" does not belong to Pipeline \"{}\"".
                format(self.pipelinestep, self, self.run.pipeline))

        # If the PS stores a method, it should have no child_run.
        # (Should not act as a parent runstep)
        if (type(self.pipelinestep.transformation) == Method and
                hasattr(self,"child_run") == True):
            raise ValidationError(
                "PipelineStep of RunStep \"{}\" is not a Pipeline but a child run exists".
                format(self))
        
        for rsic in self.RSICs.all():
            rsic.complete_clean()

        if (self.pipelinestep.cables_in.count() != self.RSICs.count()):
            if (self.reused != None or self.execrecord != None):
                raise ValidationError(
                    "RunStep \"{}\" inputs not quenched; reused and execrecord should not be set".
                    format(self))
            if (type(self.pipelinestep.transformation) == Pipeline and
                    hasattr(self, "child_run")):
                raise ValidationError(
                    "RunStep \"{}\" inputs not quenched; child_run should not be set".
                    format(self))
            if self.outputs.all().exists():
                raise ValidationError(
                    "RunStep \"{}\" inputs not quenched; no data should have been generated".
                    format(self))
            return

        # From here on, RSICs are assumed to be quenched.
        if self.reused == None:
            if self.outputs.all().exists():
                raise ValidationError(
                    "RunStep \"{}\" has not decided whether or not to reuse an ExecRecord; no data should have been generated".
                    format(self))
            if self.execrecord != None:
                raise ValidationError(
                    "RunStep \"{}\" has not decided whether or not to reuse an ExecRecord; execrecord should not be set".
                    format(self))
            if (type(self.pipelinestep.transformation) == Pipeline and
                    hasattr(self, "child_run")):
                raise ValidationError(
                    "RunStep \"{}\" has not decided whether or not to reuse an ExecRecord; child_run should not be set".
                    format(self))
            return

        # From here on, reused is assumed to be set.
        elif self.reused:
            if self.outputs.all().exists():
                raise ValidationError(
                    "RunStep \"{}\" reused an ExecRecord and should not have generated any data".
                    format(self))
            if hasattr(self, "child_run") == True:
                raise ValidationError(
                    "RunStep \"{}\" reused an ExecRecord and should not have a child run".
                    format(self))
        else:
            # If there is a child_run associated, clean it; if child_run is set,
            # there should be no associated output data, and ER should not be set.
            if hasattr(self, "child_run"):
                self.child_run.clean()

                if self.outputs.all().exists():
                    raise ValidationError(
                        "RunStep \"{}\" has a child run so should not have generated any data".
                        format(self))
                
                if self.execrecord != None:
                    raise ValidationError(
                        "RunStep \"{}\" has a child run so execrecord should not be set".
                        format(self))
            else:
                for out_data in self.outputs.all():
                    out_data.clean()

        # From here on, child_run is assumed to be appropriately set or blank.
        step_er = self.execrecord
        if hasattr(self, "child_run"):
            step_er = self.child_run.execrecord

        if step_er == None:
            return

        # From here on, the appropriate ER is assumed to be set.
        step_er.complete_clean()

        # ER must point to the same transformation that this runstep points to
        if self.pipelinestep.transformation != step_er.general_transf:
            raise ValidationError(
                "RunStep \"{}\" points to transformation \"{}\" but corresponding ER does not".
                format(self, self.pipelinestep))

        # Go through all of the outputs.
        to_type = ContentType.objects.get_for_model(TransformationOutput)

        # Track whether there are any outputs not deleted.
        any_outputs_kept = False
        
        for to in self.pipelinestep.transformation.outputs.all():
            if self.pipelinestep.outputs_to_delete.filter(
                    dataset_name=to.dataset_name).exists():
                # This output is deleted; there should be no associated Dataset.
                # Get the associated ERO.
                corresp_ero = step_er.execrecordouts.get(
                    content_type=to_type, object_id=to.id)
                if self.outputs.filter(symbolicdataset=corresp_ero.symbolicdataset).exists():
                    raise ValidationError(
                        "Output \"{}\" of RunStep \"{}\" is deleted; no data should be associated".
                        format(to, self))
            else:
                # The output is not deleted.
                any_outputs_kept = True

                # The corresponding ERO should have existent data.
                corresp_ero = step_er.execrecordouts.get(
                    content_type=to_type, object_id=to.id)
                if not corresp_ero.symbolicdataset.has_data():
                    raise ValidationError(
                        "ExecRecordOut \"{}\" of RunStep \"{}\" should reference existent data".
                        format(corresp_ero, self))

        # If there are any outputs not deleted, this RunStep did not
        # reuse an ER, and did not have a child run, then there should
        # be at least one corresponding real Dataset.
        associated_datasets = self.outputs.all():
        if (any_outputs_kept and not self.reused and
                not hasattr(self, "child_run") and
                not associated_datasets.exists()):
            raise ValidationError(
                "RunStep \"{}\" did not reuse an ExecRecord, had no child run, and did not delete all of its outputs; a corresponding Dataset should be associated".
                format(self, to))

        # Check that any associated data belongs to an ERO of this ER
        for out_data in associated_datasets:
            if not step_er.execrecordouts.filter(
                    symbolicdataset=out_data.symbolicdataset).exists():
                raise ValidationError(
                    "RunStep \"{}\" generated Dataset \"{}\" but it is not in its ExecRecord".
                    format(self, out_data))
            

    def is_complete(self):
        """True if RunStep is complete; false otherwise."""
        step_er = self.execrecord
        if hasattr(self, "child_run"):
            step_er = self.child_run.execrecord
        return step_er != None
    
    def complete_clean(self):
        """Checks coherence and completeness of this RunStep."""
        self.clean()
        if not self.is_complete():
            raise ValidationError(
                "RunStep \"{}\" has no ExecRecord".format(self))

class RunSIC(models.Model):
    """
    Annotates the action of a PipelineStepInputCable within a RunStep.

    Related to :model:`copperfish.RunStep`
    Related to :model:`copperfish.ExecRecord`
    Related to :model:`copperfish.PipelineStepInputCable`
    """
    runstep = models.ForeignKey(RunStep, related_name="RSICs")
    execrecord = models.ForeignKey(
        "ExecRecord",
        null=True,
        blank=True,
        related_name="RSICs")
    reused = models.NullBooleanField(
        help_text="Denotes whether this run reused the action of an output cable",
        default=None)
    PSIC = models.ForeignKey(
        PipelineStepInputCable,
        related_name="psic_instances")
    
    output = generic.GenericRelation("Dataset")

    class Meta:
        # Uniqueness constraint ensures that no POC is multiply-represented
        # within a run step.
        unique_together = ("runstep", "PSIC")

    def clean(self):
        """
        Check coherence of this RunSIC.

        In sequence, the checks we perform:
         - PSIC belongs to runstep.pipelinestep
         - if reused is None (no decision on reusing has been made), no data
           should be associated, and execrecord should not be set
         - else if reused is True, no data should be associated.
         - else if reused is False:
           - if the cable is trivial, there should be no associated Dataset
           - otherwise, make sure there is at most one Dataset, and clean it
             if it exists
        (from here on execrecord is assumed to be set)
         - it must be complete and clean
         - PSIC is the same as (or compatible to) self.execrecord.general_transf
         - if this RunSIC does not keep its output, there should be no existent
           data associated.
         - else if this RunSIC keeps its output:
           - the corresponding ERO should have existent data associated
           - if the PSIC is not trivial and this RunSIC does not reuse an ER,
             then there should be existent data associated and it should also
             be associated to the corresponding ERO.
        """
        if (not self.runstep.pipelinestep.cables_in.
                filter(pk=self.PSIC.pk).exists()):
            raise ValidationError(
                "PSIC \"{}\" does not belong to PipelineStep \"{}\"".
                format(self.PSIC, self.runstep.pipelinestep))

        if self.reused == None:
            if self.has_data():
                raise ValidationError(
                    "RunSIC \"{}\" has not decided whether or not to reuse an ExecRecord; no Datasets should be associated".
                    format(self))
            if self.execrecord != None:
                raise ValidationError(
                    "RunSIC \"{}\" has not decided whether or not to reuse an ExecRecord; execrecord should not be set yet".
                    format(self))

        elif self.reused:
            if self.has_data():
                raise ValidationError(
                    "RunSIC \"{}\" reused an ExecRecord and should not have generated any Datasets".
                    format(self))

        else:
            # If this cable is trivial, there should be no data
            # associated.
            if self.PSIC.is_trivial() and self.has_data():
                raise ValidationError(
                    "RunSIC \"{}\" is trivial and should not have generated any Datasets".
                    format(self))

            # Otherwise, check that there is at most one Dataset
            # attached, and clean it.
            elif self.has_data():
                if self.output.count() > 1:
                    raise ValidationError(
                        "RunSIC \"{}\" should generate at most one Dataset".
                        format(self))
                self.output.all()[0].clean()
        
        # If there is no execrecord defined, then exit.
        if self.execrecord == None:
            return
        
        # At this point there must be an associated ER; check that it is
        # clean and complete.
        self.execrecord.complete_clean()

        # Check that PSIC and execrecord.general_transf are compatible
        # given that the SymbolicDataset represented in the ERI is the
        # input to both.  (This must be true because our Pipeline was
        # well-defined.)
        if type(self.execrecord.general_transf) != PipelineStepInputCable:
            raise ValidationError(
                "ExecRecord of RunSIC \"{}\" does not represent a PSIC".
                format(self.PSIC))
        
        elif not self.PSIC.is_compatible_given_input(self.execrecord.general_transf):
            raise ValidationError(
                "PSIC of RunSIC \"{}\" is incompatible with that of its ExecRecord".
                format(self.PSIC))

        # If the output of this PSIC is not marked to keep, there should be
        # no data associated.
        if not self.PSIC.keep_output:
            if self.has_data():
                raise ValidationError(
                    "RunSIC \"{}\" does not keep its output; no data should be produced".
                    format(self))
        else:
            # The corresponding ERO should have existent data.
            corresp_ero = self.execrecord.execrecordouts.all()[0]
            if not corresp_ero.has_data():
                raise ValidationError(
                    "RunSIC \"{}\" keeps its output; ExecRecordOut \"{}\" should reference existent data".
                    format(self, corresp_ero))

            # If reused == False and the cable is not trivial,
            # there should be associated data, and it should match that
            # of corresp_ero.
            if not self.reused and not self.PSIC.is_trivial():
                if not self.has_data():
                    raise ValidationError(
                        "RunSIC \"{}\" was not reused, trivial, or deleted; it should have produced data".
                        format(self))

                if corresp_ero.symbolicdataset.dataset != self.output.all()[0]:
                    raise ValidationError(
                        "Dataset \"{}\" was produced by RunSIC \"{}\" but is not in an ERO of ExecRecord \"{}\"".
                        format(self.output.all()[0], self, self.execrecord))

    def is_complete(self):
        """True if RunSIC is complete; false otherwise."""
        return self.execrecord != None

    def complete_clean(self):
        """Check completeness and coherence of this RunSIC."""
        self.clean()
        if not self.is_complete():
            raise ValidationError(
                "RunSIC \"{}\" has no ExecRecord".format(self))

    def has_data(self):
        """True if associated output exists; False if not."""
        return self.output.all().exists()

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
    # For validation of Datasets when being reused, or when being regenerated.
    MD5_checksum = models.CharField(
        max_length=64,
        validators=[RegexValidator(
            regex=re.compile("^[1234567890AaBbCcDdEeFf]{32}$"),
            message="MD5 checksum is not 32 hex characters")],
        help_text="Validates file integrity")

    def __unicode__(self):
        """
        Unicode representation of a SymbolicDataset.

        This is simply the name.
        """
        return self.name

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
        return self.structure.num_rows()

    
class Dataset(models.Model):
    """
    Data files uploaded by users or created by transformations.

    Related to :model:`copperfish.RunStep`
    Related to :model:`copperfish.RunOutputCable`
    Related to :model:`copperfish.SymbolicDataset`
    Related to :model:`copperfish.DatasetStructure`

    The clean() function should be used when a pipeline is executed to
    confirm that the dataset structure is consistent with what's
    expected from the pipeline definition.
    
    Pipeline.clean() checks that the pipeline is well-defined in theory,
    while Dataset.clean() ensures the Pipeline produces what is expected.
    """
    user = models.ForeignKey(
        User,
        help_text="User that uploaded this Dataset.")
    
    name = models.CharField(
        max_length=128,
        help_text="Description of this Dataset.")
    
    description = models.TextField()
    
    date_created = models.DateTimeField(
        "Date created",
        auto_now_add=True,
        help_text="Date of Dataset creation.")

    # Four cases from which Datasets can originate:
    #
    # Case 1: uploaded
    # Case 2: from the transformation of a RunStep
    # Case 3: from the execution of a POC (i.e. from a ROC)
    # Case 4: from the execution of a PSIC (i.e. from a RunSIC)
    content_type = models.ForeignKey(
        ContentType,
        limit_choices_to = {
            "model__in": ("RunStep", "RunOutputCable",
                          "RunSIC")
        },
        null=True,
        blank=True)
    object_id = models.PositiveIntegerField(null=True, blank=True)
    created_by = generic.GenericForeignKey("content_type", "object_id")
    
    # Datasets are stored in the "Datasets" folder
    dataset_file = models.FileField(
        upload_to="Datasets",
        help_text="Physical path where datasets are stored",
        null=False)

    # Datasets always have a referring SymbolicDataset
    symbolicdataset = models.OneToOneField(
        SymbolicDataset,
        related_name="dataset")

    def __unicode__(self):
        """Unicode representation of this Dataset."""
        return "{} contents".format(self.symbolicdataset)


    def clean(self):
        """
        Check file integrity of this Dataset.
        """
        if not self.check_md5():
            raise ValidationError(
                "File integrity of \"{}\" lost.  Current checksum \"{}\" does not equal expected checksum \"{}\"".
                format(self, self.compute_md5(),
                       self.symbolicdataset.MD5_checksum))
            
    def compute_md5(self):
        """Computes the MD5 checksum of the Dataset."""
        md5gen = hashlib.md5()
        md5 = None
        try:
            self.dataset_file.open()
            md5 = file_access_utils.compute_md5(self.dataset_file.file)
        finally:
            self.dataset_file.close()
        
        return md5
            
    def check_md5(self):
        """
        Checks the MD5 checksum of the Dataset against its stored value.

        The stored value is in the Dataset's associated
        SymbolicDataset.  This will be used when regenerating data
        that once existed, as a coherence check.
        """
        # Recompute the MD5, see if it equals what is already stored
        return self.symbolicdataset.MD5_checksum == self.compute_md5()
    
class DatasetStructure(models.Model):
    """
    Data with a Shipyard-compliant structure: a CSV file with a header.
    Encodes the CDT, and the transformation output generating this data.

    Related to :model:`copperfish.SymbolicDataset`
    Related to :model:`copperfish.CompoundDatatype`
    """
    # Note: previously we were tracking the exact TransformationOutput
    # this came from (both for its Run and its RunStep) but this is
    # now done more cleanly using ExecRecord.

    symbolicdataset = models.OneToOneField(
        SymbolicDataset, related_name="structure")

    compounddatatype = models.ForeignKey(
        CompoundDatatype,
        related_name="conforming_datasets")
    num_rows = models.IntegerField(
        "number of rows",
        validators=[MinValueValidator(0)])

    # October 31, 2013: we now think that it's too onerous to have 
    # a clean() function here that opens up the CSV file and checks it.
    # Instead we will make it a precondition that any SymbolicDataset
    # that represents a CSV file has to have confirmed using
    # file_access_utils.summarize_CSV() that the CSV file is coherent.

    # At a later date, we might want to put in some kind of
    # "force_check()" which actually opens the file and makes sure its
    # contents are OK.

    def num_rows(self):
        """The number of rows in the CSV file (excluding header)."""
        return self.num_rows
    
class ExecRecord(models.Model):
    """
    Record of a previous execution of a Method/Pipeline/PipelineOutputCable/PSIC.

    This record is specific to using given inputs.
    """
    content_type = models.ForeignKey(
        ContentType,
        limit_choices_to = {
            "model__in": ("Method", "Pipeline", "PipelineOutputCable",
                          "PipelineStepInputCable")
        })
    object_id = models.PositiveIntegerField()
    general_transf = generic.GenericForeignKey("content_type", "object_id")

    # Output and error logs, i.e. the stdout and stderr produced by
    # running the code at this step.  These must be set if the ER
    # represents a Method, and are otherwise null.
    output_log = models.FileField(
        "output log", 
        null=True,
        blank=True,
        help_text="Terminal output of this ExecRecord, i.e. stdout.")
    
    error_log = models.FileField(
        "error log", 
        null=True,
        blank=True,
        help_text="Terminal error output of this ExecRecord, i.e. stderr.")

    # Has this record been called into question by a subsequent execution?
    tainted = models.BooleanField(
        default=False,
        help_text="Denotes whether this record's veracity is questionable")

    # FIXME: we probably need some kind of "are you done?" flag here
    # to act as a guide for concurrent running of code.  Our thinking
    # right now is that we want to make the creation of an ExecRecord
    # (during a Run/RS/RSIC/ROC) a three-step *transaction*:
    
    # 1) look for a suitable ER
    # 2) if one is not found, create it (and mark as incomplete)
    # 3) if one *is* found, wait for it to be complete and then reuse 
    #    it

    # Another (probably better) approach is that instead of this flag,
    # we have another table somewhere saying that this ER is in
    # progress.

    def __unicode__(self):
        """Unicode representation of this ExecRecord."""
        inputs_list = [unicode(eri) for eri in self.execrecordins.all()]
        outputs_list = [unicode(ero) for ero in self.execrecordouts.all()]

        string_rep = u""
        if type(self.general_transf) in ("Method", "Pipeline"):
            string_rep = u"{}({}) = ({})".format(self.general_transf,
                                                 u", ".join(inputs_list),
                                                 u", ".join(outputs_list))
        else:
            # Return a representation for a cable.
            string_rep = (u"{}".format(u", ".join(inputs_list)) +
                          " ={" + u"{}".format(self.general_transf) + "}=> " +
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

        output_log and error_log are null if this ER does not
        represent a Method.
        """
        eris = self.execrecordins.all()
        eros = self.execrecordouts.all()

        for eri in eris:
            eri.clean()
        for ero in eros:
            ero.clean()

        if type(self.general_transf) != Method:
            if self.output_log != None:
                raise ValidationError(
                    "ExecRecord \"{}\" does not represent a Method; no output log should exist".
                    format(self))

            if self.error_log != None:
                raise ValidationError(
                    "ExecRecord \"{}\" does not represent a Method; no error log should exist".
                    format(self))

        if type(self.general_transf) not in (Method, Pipeline):
            # If the cable is quenched:
            if eris.exists() and eros.exists():
                
                # If the cable is trivial, then the ERI and ERO should
                # have the same SymbolicDataset (if they both exist).
                if (self.general_transf.is_trivial() and
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
                elif (not self.general_transf.is_trivial() and
                         eris[0].symbolicdataset.has_data() and
                         eros[0].symbolicdataset.has_data()):
                    cable_wires = None
                    if type(self.general_transf) == PipelineStepInputCable:
                        cable_wires = self.general_transf.custom_wires.all()
                    else:
                        cable_wires = self.general_transf.custom_outwires.all()

                    source_CDT = (eris[0].symbolicdataset.dataset.structure.
                                  compounddatatype)
                    dest_CDT = (eros[0].symbolicdataset.dataset.structure.
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
        Method/Pipeline/POC/PSIC are quenched.
        
        output_log and error_log are *not* null if this ER
        represents a Method.
        """
        self.clean()

        if type(self.general_transf) == Method:
            if self.output_log == None:
                raise ValidationError(
                    "ExecRecord \"{}\" represents a Method but no output log exists".
                    format(self))
            
            if self.error_log == None:
                raise ValidationError(
                    "ExecRecord \"{}\" represents a Method but no error log exists".
                    format(self))

        # Because we know that each ERI is clean (and therefore each
        # one maps to a valid input of our Method/Pipeline/POC/PSIC), and
        # because there is no multiple quenching (due to a uniqueness
        # constraint), all we have to do is check the number of ERIs
        # to make sure everything is quenched.
        if type(self.general_transf) in (PipelineOutputCable, PipelineStepInputCable):
            # In this case we check that there is an input and an output.
            if not self.execrecordins.all().exists():
                raise ValidationError(
                    "Input to ExecRecord \"{}\" is not quenched".format(self))
            if not self.execrecordouts.all().exists():
                raise ValidationError(
                    "Output of ExecRecord \"{}\" is not quenched".format(self))

        else:
            if self.execrecordins.count() != self.general_transf.inputs.count():
                raise ValidationError(
                    "Input(s) to ExecRecord \"{}\" are not quenched".format(self));
        
            # Similar for EROs.
            if self.execrecordouts.count() != self.general_transf.outputs.count():
                raise ValidationError(
                    "Output(s) of ExecRecord \"{}\" are not quenched".format(self));
        
class ExecRecordIn(models.Model):
    """
    Denotes a symbolic input fed to the Method/Pipeline/POC/PSIC in the parent ExecRecord.

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
        transf_input_name = "";

        if type(self.execrecord.general_transf) in (PipelineOutputCable,
                PipelineStepInputCable):
            return unicode(self.symbolicdataset)
        else:
            transf_input_name = self.generic_input.dataset_name

        return "{}=>{}".format(self.symbolicdataset, transf_input_name)
            

    def clean(self):
        """
        Checks coherence of this ExecRecordIn.

        Checks that generic_input is appropriate for the parent
        ExecRecord's Method/Pipeline/POC/PSIC.
        - If execrecord is for a POC, then generic_input should be the TO that
          feeds it (i.e. the PipelineStep TO that is cabled to a Pipeline output).
        - If execrecord is for a PSIC, then generic_input should be the TO or TI
          that feeds it (TO if it's from a previous step; TI if it's from a Pipeline
          input).
        - If execrecord is for a Method/Pipeline, then generic_input is the TI
          that this ERI represents.
          
        Also, if symbolicdataset refers to existent data, check that it
        is compatible with the input represented.
        """
        parent_transf = self.execrecord.general_transf

        # If ER links to POC, ERI must link to TO which the outcable runs from.
        if type(parent_transf) == PipelineOutputCable:
            if self.generic_input != parent_transf.provider_output:
                raise ValidationError(
                    "ExecRecordIn \"{}\" does not denote the TO that feeds the parent ExecRecord POC".
                    format(self))
        # Similarly for a PSIC.
        elif type(parent_transf) == PipelineStepInputCable:
            if self.generic_input != parent_transf.provider_output:
                raise ValidationError(
                    "ExecRecordIn \"{}\" does not denote the TO/TI that feeds the parent ExecRecord PSIC".
                    format(self))

        else:
            # The ER represents a Method/Pipeline (not a cable).  Therefore
            # the ERI must refer to a TI of the parent ER's Method/Pipeline.
            if type(self.generic_input) == TransformationOutput:
                raise ValidationError(
                    "ExecRecordIn \"{}\" must refer to a TI of the Method/Pipeline of the parent ExecRecord".
                    format(self))

            transf_inputs = self.execrecord.general_transf.inputs
            if not transf_inputs.filter(pk=self.generic_input.pk).exists():
                raise ValidationError(
                    "Input \"{}\" does not belong to Method/Pipeline of ExecRecord \"{}\"".
                    format(self.generic_input, self.execrecord))


        # The ERI's SymbolicDataset raw/unraw state must match the
        # raw/unraw state of the generic_input that it feeds it (if ER is a cable)
        # or that it is fed into (if ER is a Method/Pipeline).
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
                if type(self.generic_input) == TransformationOutput:
                    error_str = "SymbolicDataset \"{}\" has too few rows to have come from TransformationOutput \"{}\""
                else:
                    error_str = "SymbolicDataset \"{}\" has too few rows for TransformationInput \"{}\""
                raise ValidationError(error_str.format(input_SD, transf_xput_used))
                    
            if (transf_xput_used.get_max_row() != None and
                input_SD.num_rows() > transf_xput_used.get_max_row()):
                error_str = ""
                if type(self.generic_input) == TransformationOutput:
                    error_str = "SymbolicDataset \"{}\" has too many rows to have come from TransformationOutput \"{}\""
                else:
                    error_str = "SymbolicDataset \"{}\" has too many rows for TransformationInput \"{}\""
                raise ValidationError(error_str.format(input_SD, transf_xput_used))

class ExecRecordOut(models.Model):
    """
    Denotes a symbolic output from the Method/Pipeline/POC in the parent ExecRecord.

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
        if type(self.execrecord.general_transf) in (PipelineOutputCable, PipelineStepInputCable):
            unicode_rep = unicode(self.symbolicdataset)
        else:
            unicode_rep = u"{}=>{}".format(self.generic_output.dataset_name,
                                           self.symbolicdataset)
        return unicode_rep


    def clean(self):
        """
        Checks coherence of this ExecRecordOut.

        If execrecord represents a POC, then check that output is the one defined
        by the POC.

        If execrecord represents a PSIC, then check that the output is the TI the
        cable feeds.
        
        If execrecord is not a cable, then check that output belongs to 
        execrecord.general_transf.

        The SymbolicDataset is compatible with generic_output).
        """

        # If the parent ER is linked with POC, the corresponding ERO TO must be coherent
        if type(self.execrecord.general_transf) == PipelineOutputCable:
            parent_er_outcable = self.execrecord.general_transf

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
        elif type (self.execrecord.general_transf) == PipelineStepInputCable:
            parent_er_psic = self.execrecord.general_transf

            # This ERO must point to a TI.
            if type(self.generic_output) != TransformationInput:
                raise ValidationError(
                    "Parent of ExecRecordOut \"{}\" represents a PSIC; ERO must be a TransformationInput".
                    format(self))

            # The TI this ERO points to must be the one fed by the PSIC.
            if parent_er_psic.transf_input != self.generic_output:
                raise ValidationError(
                    "Input \"{}\" is not the one fed by the PSIC of ExecRecord \"{}\"".
                    format(self.generic_output, self.execrecord))

        # Else the parent ER is linked with either a method or a pipeline
        else:
            query_for_outs = self.execrecord.general_transf.outputs

            # The ERO output TO must be a member of the ER's method/pipeline
            if not query_for_outs.filter(pk=self.generic_output.pk).exists():
                raise ValidationError(
                    "Output \"{}\" does not belong to Method/Pipeline of ExecRecord \"{}\"".
                    format(self.generic_output, self.execrecord))

        # Check that the SD is compatible with generic_output.

        # If SD is raw, the ERO output TO must also be raw
        if self.symbolicdataset.is_raw() != self.generic_output.is_raw():
            if type(self.generic_output) == PipelineStepInputCable:
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
            if type(self.execrecord.general_transf) == Method:
                if (input_SD.structure.compounddatatype !=
                        self.generic_output.get_cdt()):
                    raise ValidationError(
                        "CDT of SymbolicDataset \"{}\" is not the CDT of the TransformationOutput \"{}\" of the generating Method".
                        format(input_SD, self.generic_output))

            # If it refers to a POC, then SD CDT must be
            # identical to generic_output's CDT, because it was
            # generated either by this POC or by a compatible one,
            # and compatible ones must have a CDT identical to
            # this one.  This therefore is also the same for
            # Pipeline.
            elif (type(self.execrecord.general_transf) in
                      (Pipeline, PipelineOutputCable)):
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
                if type(self.execrecord.general_transf) == PipelineStepInputCable:
                    raise ValidationError(
                        "SymbolicDataset \"{}\" feeds TransformationInput \"{}\" but has too few rows".
                        format(input_SD, self.generic_output))
                else:
                    raise ValidationError(
                        "SymbolicDataset \"{}\" was produced by TransformationOutput \"{}\" but has too few rows".
                        format(input_SD, self.generic_output))

            if (self.generic_output.get_max_row() != None and 
                    input_SD.num_rows() > self.generic_output.get_max_row()):
                if type(self.execrecord.general_transf) == PipelineStepInputCable:
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

# Some stuff that was pre-loaded into the database, e.g. atomic
# Datatypes such as str, bool.  Also, CDTs for verification methods
# and for prototypes.  These must be loaded into the database right
# after the tables have been created, e.g. after calling
# "./manage.py syncdb"
    
# These are added using a fixture after this file is loaded; as such,
# we can't define these variables here.
    
# STR_DT = Datatype.objects.get(pk=1)
# BOOL_DT = Datatype.objects.get(pk=2)

# VERIF_IN = CompoundDatatype.objects.get(pk=1)
# VERIF_OUT = CompoundDatatype.objects.get(pk=2)
# PROTOTYPE_CDT = CompoundDatatype.objects.get(pk=3)
