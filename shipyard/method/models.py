"""
method.models

Shipyard data models relating to Methods: this includes everything to
do with CodeResources.

FIXME get all the models pointing at each other correctly!
"""

from django.db import models
from django.contrib.contenttypes import generic
from django.core.exceptions import ValidationError
import hashlib, os, re, string, stat, subprocess
import file_access_utils, transformation.models
from constants import error_messages

import traceback
import threading
import logging
import inspect

class CodeResource(models.Model):
    """
    A CodeResource is any file tracked by ShipYard.
    Related to :model:`method.CodeResourceRevision`
    """

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

    def count_revisions(self):
        """
        Number of revisions associated with this CodeResource.
        """
        return CodeResourceRevision.objects.filter(coderesource=self).count()

    num_revisions = property(count_revisions)

    def get_last_revision_date(self):
        """
        Date of most recent revision to this CodeResource.
        """
        revisions = CodeResourceRevision.objects.filter(coderesource=self)
        if len(revisions) == 0:
            return 'n/a'
        revision_dates = [revision.revision_DateTime for revision in revisions]
        revision_dates.sort() # ascending order
        return revision_dates[0]

    last_revision_date = property(get_last_revision_date)

    def get_absolute_url(self):
        return '/resources/%i' % self.id

    def clean(self):
        """
        CodeResource name must be valid.

        It must not contain a leading space character or "..",
        must not end in space, and be composed of letters,
        numbers, dash, underscore, paranthesis, and space.
        """
        
        if not self.isValidFileName():
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
        self.install_h(install_path, self.coderesource.filename)
        
    def install_h(self, install_path, base_name):
        """Helper for install."""
        import inspect, logging
        fn = "{}.{}()".format(self.__class__.__name__, inspect.stack()[0][3])

        logging.debug("{}: Writing code to {}".format(fn, install_path))
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
    random = models.BooleanField(default=False,
        help_text="Is the output of this method nondeterministic?")

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

    def clean(self):
        """
        Check coherence of this Method. The checks we perform are:

        - Method does not have a Metapackage as a driver.
        """
        super(Method, self).clean()
        if not self.driver.content_file:
            raise ValidationError(error_messages["driver_metapackage"].
                format(self, self.driver))

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
        Given a set of input SDs, find an ER that can be reused given these inputs.
        A compatible ER may have to be filled in.
        """
        import inspect, logging
        fn = "{}.{}()".format(self.__class__.__name__, inspect.stack()[0][3])

        logging.debug("{}: Considering all pipeline steps featuring this method...".format(fn))

        # For pipelinesteps featuring this method
        for possible_PS in self.pipelinesteps.all():
            logging.debug("{}: Considering pipeline step '{}'".format(fn, possible_PS))

            # For linked runsteps which did not *completely* reuse an ER
            for possible_RS in possible_PS.pipelinestep_instances.filter(reused=False):
                logging.debug("{}: Considering non-reused runstep '{}'".format(fn,possible_RS))

                candidate_ER = possible_RS.execrecord

                if not candidate_ER.outputs_OK():
                    logging.debug("{}: Rejecting runstep, outputs not OK".format(fn))
                    continue


                logging.debug("{}: Candidate ER is OK (no bad CCLs or ICLs): checking if inputs match".format(fn))

                ER_matches = True
                for ERI in candidate_ER.execrecordins.all():
                    input_idx = ERI.generic_input.dataset_idx
                    if ERI.symbolicdataset != input_SDs[input_idx-1]:
                        ER_matches = False
                        break
                        
                if ER_matches:
                    logging.debug("{}: All ERIs match input SDs - comitting to candidate ER {}".format(fn, candidate_ER))
                    return candidate_ER
    
        logging.debug("{}: No compatible ERs found".format(fn))
        return None

    def _poll_stream(self, proc, in_stream, out_streams):
        """
        Helper function for run_code_with_streams, which polls a Popen'ed procedure
        for output on stream until it terminates, and prints the output to all the
        out streams.
        """
        while True:
            line = in_stream.readline()
            if line:
                for stream in out_streams:
                    stream.write(line)
            if proc.poll() is not None:
                break

    def run_code_with_streams(self, run_path, input_paths, output_paths, output_streams, error_streams):
        """
        Run the method, passing each line in its stdout and stderr to any number
        of streams. Return the Method's return code, or -1 if the Method suffers
        an OS-level error (ie. is not executable).

        INPUTS
        run_path        see run_code
        input_paths     see run_code
        output_paths    see run_code
        output_streams  list of streams (eg. open file handles) to output stderr to
        error_streams   list of streams (eg. open file handles) to output stderr to

        OUTPUTS
        The return code of the Method's driver.
        """
        fn = "{}.{}()".format(self.__class__.__name__, inspect.stack()[0][3])
        trace = None # If the process caused a system level error, it will be stored here.
        try:
            method_popen = self.run_code(run_path, input_paths, output_paths)
        except OSError:
            trace = traceback.format_exc()

        # Succesful execution.
        if trace is None:
            logging.debug("{}: Polling Popen + displaying stdout/stderr to console".format(fn))

            out_thread = threading.Thread(target=self._poll_stream, 
                    args=(method_popen, method_popen.stdout, output_streams))
            err_thread = threading.Thread(target=self._poll_stream, 
                    args=(method_popen, method_popen.stdout, error_streams))
            out_thread.start()
            err_thread.start()
            out_thread.join()
            err_thread.join()

            returncode = method_popen.returncode

        # If the process bombed, store/write the traceback.
        else:
            for stream in error_streams:
                stream.write(trace)
            returncode = -1

        for stream in output_streams + error_streams:
            stream.flush()
        return returncode

    def run_code(self, run_path, input_paths, output_paths):
        """
        SYNOPSIS
        Runs a method using the run path and input/outputs.
        Leaves responsibility of DB annotation up to execute()

        INPUTS
        run_path        Directory where code will be run
        input_paths     List of input files expected by the code
        output_paths    List of where code will write results

        OUTPUTS
        A running subprocess.Popen object which is asynchronous

        ASSUMPTIONS
        1) The CRR of this Method can interface with Shipyard.
        Ie, it has positional inputs and outputs at command line:
        script_name.py [input 1] ... [input n] [output 1] ... [output n]

        2) The caller is responsible for cleaning up the stdout/err
        file handles after the Popen has finished processing.

        3) We don't handle exceptions of Popen here, the caller must do that.
        """
        import inspect, logging
        fn = "{}.{}()".format(self.__class__.__name__, inspect.stack()[0][3])

        if (len(input_paths) != self.inputs.count() or  len(output_paths) != self.outputs.count()):
            raise ValueError(
                error_messages["method_bad_inputcount"].
                format(self, self.inputs.count(), self.outputs.count(), len(input_paths), len(output_paths)))

        logging.debug("{}: Checking run_path exists: {}".format(fn, run_path))
        file_access_utils.set_up_directory(run_path, tolerate=True)

        for input_path in input_paths:
            logging.debug("{}: Confirming input file exists + readable: {}".format(fn, input_path))
            f = open(input_path, "rb")
            f.close()

        for output_path in output_paths:
            logging.debug("{}: Confirming output path doesn't exist: {}".format(fn,output_path))
            can_create, reason = file_access_utils.can_create_new_file(output_path)

            if not can_create:
                raise ValueError(reason)

        logging.debug("{}: Installing CRR driver to FS: {}".format(fn, self.driver))
        self.driver.install(run_path)

        # At this point, run_path has all of the necessary stuff
        # written into place.  It remains to execute the code.
        # The code to be executed sits in 
        # [run_path]/[driver.coderesource.name],
        # and is executable.
        code_to_run = os.path.join(run_path,
            self.driver.coderesource.filename)

        command = [code_to_run] + input_paths + output_paths
        logging.debug("{}: subprocess.Popen({})".format(fn, command))
        code_popen = subprocess.Popen(command, shell=False,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)

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
