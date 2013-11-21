"""
method.models

Shipyard data models relating to Methods: this includes everything to
do with CodeResources.

FIXME get all the models pointing at each other correctly!
"""

from django.db import models
from django.contrib.contenttypes import generic
from django.core.exceptions import ValidationError

import hashlib
import re
import string
import os.path
import os
import subprocess
import stat

import file_access_utils
import transformation.models

class CodeResource(models.Model):
    """
    A CodeResource is any file tracked by ShipYard.
    Related to :model:`method.CodeResourceRevision`
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

    Related to :model:`method.CodeResource`
    Related to :model:`method.CodeResourceDependency`
    Related to :model:`method.Method`
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
    Dependencies of a CodeResourceRevision - themselves also CRRs.
    
    Related to :model:`method.CodeResourceRevision`
    """

    coderesourcerevision = models.ForeignKey(
        CodeResourceRevision,
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



class Method(transformation.models.Transformation):
    """
    Methods are atomic transformations.

    Inherits from :model:`copperfish.Transformation`
    Related to :model:`copperfish.CodeResource`
    Related to :model:`copperfish.MethodFamily`
    """

    family = models.ForeignKey("MethodFamily",related_name="members")
    revision_parent = models.ForeignKey("self",related_name = "descendants",null=True,blank=True)

    # Code resource revisions are executable if they link to Method
    driver = models.ForeignKey(CodeResourceRevision);
    random = models.BooleanField(
        help_text="Is the output of this method nondeterministic?)

    tainted = models.BooleanField(
        default=False,
        help_text="Is this Method broken?")

    pipelinesteps = generic.GenericRelation("pipeline.PipelineStep")

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

    def find_compatible_ER(self, input_SDs):
        """
        Helper that finds an ER that we can reuse given these inputs.
    
        input_SDs is a list of inputs to this Method in the proper
        order.
        """
        # Look through all PipelineSteps that use this Method; then
        # look at all the RunSteps corresponding to it and their ERs.
        for possible_PS in self.pipelinesteps.all():
            for possible_RS in possible_PS.pipelinestep_instances.filter(
                    reused=False):
                candidate_ER = possible_RS.execrecord

                # Check if its outputs are OK; if not, move on.
                if not candidate_ER.outputs_OK():
                    continue

                # From here on we know the outputs are OK.  Check if
                # the inputs match.
                ER_matches = True
                for ERI in candidate_ER.execrecordins.all():
                    # Get the input index of this ERI.
                    input_idx = ERI.generic_input.dataset_idx
                    if ERI.symbolicdataset != input_SDs[input_idx-1]:
                        ER_matches = False
                        break
                        
                # At this point all the ERIs have matched the inputs.  So,
                # we have found our candidate.
                if ER_matches:
                    return candidate_ER
    
        # We didn't find anything.
        return None

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

class MethodFamily(transformation.models.TransformationFamily):
    """
    MethodFamily groups revisions of Methods together.

    Inherits :model:`transformation.TransformationFamily`
    Related to :model:`method.Method`
    """
    # Implicitly defined:
    #   members (Method/ForeignKey)
    pass
