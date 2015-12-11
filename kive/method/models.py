"""
method.models

Shipyard data models relating to Methods: this includes everything to
do with CodeResources.
"""

from __future__ import unicode_literals

from django.db import models, transaction
from django.db.models import Max
from django.db.models.signals import post_delete
from django.core.exceptions import ValidationError
from django.core.validators import RegexValidator, MinValueValidator
from django.core.files import File
from django.utils.encoding import python_2_unicode_compatible
from django.conf import settings

import metadata.models
import transformation.models
import file_access_utils
from constants import maxlengths
import method.signals
from metadata.models import empty_removal_plan, remove_helper, update_removal_plan
from fleet.exceptions import StopExecution

import os
import stat
import subprocess
import hashlib
import traceback
import threading
import logging
import shutil
import time


@python_2_unicode_compatible
class CodeResource(metadata.models.AccessControl):
    """
    A CodeResource is any file tracked by Shipyard.
    Related to :model:`method.CodeResourceRevision`
    """
    name = models.CharField("Resource name", max_length=maxlengths.MAX_NAME_LENGTH,
                            help_text="The name for this resource and all subsequent revisions.",
                            unique=True)  # to prevent confusion in drop-down menus

    # File names must either be empty, or be 1 or more of any from
    # {alphanumeric, space, "-._()"}. This will prevent "../" as it
    # contains a slash. They can't start or end with spaces.
    filename = models.CharField("Resource file name", max_length=maxlengths.MAX_FILENAME_LENGTH,
                                help_text="The filename for this resource",
                                blank=True, validators=[
                                    RegexValidator(regex="^(\b|([-_.()\w]+ *)*[-_.()\w]+)$",
                                                   message="Invalid code resource filename"),
                                ])
    description = models.TextField("Resource description", blank=True, max_length=maxlengths.MAX_DESCRIPTION_LENGTH)

    class Meta:
        ordering = ('name',)

    @property
    def num_revisions(self):
        """
        Number of revisions associated with this CodeResource.
        """
        return self.revisions.count()

    def max_revision(self):
        """
        Return the maximum revision number of all child revisions.
        """
        return self.revisions.aggregate(Max('revision_number'))['revision_number__max']

    def next_revision(self):
        """
        Return a number suitable for assigning to the next revision to be added.
        """
        max_rev = self.max_revision()
        return (max_rev if max_rev is not None else 0) + 1

    @property
    def last_revision_date(self):
        """
        Date of most recent revision to this CodeResource.
        """
        if self.revisions.count() == 0:
            return None
        return max([revision.revision_DateTime for revision in self.revisions.all()])

    def get_absolute_url(self):
        """
        A page that displays all revisions of this CodeResource
        """
        return '/resource_revisions/{}'.format(self.id)

    def __str__(self):
        return self.name

    @transaction.atomic()
    def remove(self):
        removal_plan = self.build_removal_plan()
        remove_helper(removal_plan)

    @transaction.atomic()
    def build_removal_plan(self):
        removal_plan = empty_removal_plan()
        removal_plan["CodeResources"].add(self)
        for revision in self.revisions.all():
            if revision not in removal_plan["CodeResourceRevisions"]:
                update_removal_plan(removal_plan, revision.build_removal_plan(removal_plan))

        return removal_plan


@python_2_unicode_compatible
class CodeResourceRevision(metadata.models.AccessControl):
    """
    A particular revision of a code resource.

    Related to :model:`method.CodeResource`
    Related to :model:`method.CodeResourceDependency`
    Related to :model:`method.Method`
    """
    UPLOAD_DIR = "CodeResources"

    # Implicitly defined
    #   descendents (self/ForeignKey)
    #   dependencies (CodeResourceDependency/ForeignKey)
    #   needed_by (CodeResourceDependency/ForeignKey)
    #   methods (Method/ForeignKey)

    coderesource = models.ForeignKey(CodeResource, related_name="revisions")

    # revision_number is allowed to be null because it's automatically set on save
    revision_number = models.PositiveIntegerField(
        'Revision number', help_text="Revision number of code resource",
        blank=True)

    revision_name = models.CharField(
        max_length=maxlengths.MAX_NAME_LENGTH,
        help_text="A name to differentiate revisions of a CodeResource",
        blank=True)

    revision_DateTime = models.DateTimeField(
        auto_now_add=True,
        help_text="Date this resource revision was uploaded")

    revision_parent = models.ForeignKey('self', related_name="descendants", null=True, blank=True,
                                        on_delete=models.SET_NULL)
    revision_desc = models.TextField(
        "Revision description",
        help_text="A description for this particular resource revision",
        max_length=maxlengths.MAX_DESCRIPTION_LENGTH,
        blank=True)

    content_file = models.FileField(
        "File contents",
        upload_to=UPLOAD_DIR,
        null=True,
        blank=True,
        help_text="File contents of this code resource revision")

    MD5_checksum = models.CharField(
        max_length=64,
        blank=True,
        help_text="Used to validate file contents of this resource revision")

    class Meta:
        unique_together = (("coderesource", "revision_number"))
        ordering = ["coderesource__name", "-revision_number"]

    @property
    def filename(self):
        """
        Return original file name (without path to CodeResources, timestamp).
        TODO: use os.path.split() instead of split("/")
        """
        return '_'.join(self.content_file.name.split('/')[-1].split('_')[:-1])

    @property
    def display_name(self):
        return self.revision_name or self.coderesource.name

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)

    def __str__(self):
        """Represent a resource revision by its revision name"""
        if self.revision_name == "":
            return "[no revision name]"
        elif not hasattr(self, "coderesource"):
            return self.revision_name
        else:
            return "{}:{} ({})".format(self.coderesource.name, self.revision_number, self.revision_name)

    def save(self, *args, **kwargs):
        """Save this CodeResourceRevision, incrementing the revision number."""
        if not self.revision_number:
            self.revision_number = self.coderesource.next_revision()

        super(CodeResourceRevision, self).save(*args, **kwargs)

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
            dep_fn = dep.depFileName
            # If depFileName is blank, check and see if the
            # corresponding CodeResource had a filename (i.e. if this
            # is a non-metapackage CRR and so there is an associated
            # file).
            if dep_fn == "":
                dep_fn = dep.requirement.coderesource.filename

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
        return self.has_circular_dependence_h([])

    def has_circular_dependence_h(self, dependants):
        """Helper for has_circular_dependence.

        dependants is an accumulator that tracks all of the all of the
        CRRs that have this one as a dependency.
        """
        # Base case: self is dependant on itself, in which case, return true.
        if self in dependants:
            return True

        # Recursive case: go to all dependencies and check them.
        check_dep = False
        for dep in self.dependencies.all():
            if dep.requirement.has_circular_dependence_h(dependants + [self]):
                check_dep = True

        return check_dep

    def compute_md5(self):
        """Computes the MD5 checksum of the CodeResourceRevision."""
        try:
            self.content_file.open()
            md5 = file_access_utils.compute_md5(self.content_file.file)
        finally:
            self.content_file.close()

        return md5

    def check_md5(self):
        """
        Checks the MD5s of the CodeResourceRevision and its dependencies against their stored values.
        """
        # Recompute the MD5, see if it equals what is already stored.
        new_md5 = self.compute_md5()
        if self.MD5_checksum != new_md5:
            self.logger.warn('MD5 mismatch for %s: expected %s, but was %s.',
                             self.content_file,
                             self.MD5_checksum,
                             new_md5)
            return False

        for dep in self.dependencies.all():
            if not dep.requirement.check_md5():
                return False

        return True

    def clean(self):
        """Check coherence of this CodeResourceRevision.

        Tests for any circular dependency; does this CRR depend on
        itself at all?  Also, checks for conflicts in the
        dependencies.  Finally, if there is a file specified, fill in
        the MD5 checksum.

        NOTE: originally we were going to disallow duplicates (by checking the MD5),
        but this will be too restrictive because:
        a) we now have multiple users
        b) multiple CodeResourceRevisions may have the same file but different
           dependencies
        """
        if self.pk is None or not CodeResourceRevision.objects.filter(pk=self.pk).exists():
            # Set the MD5 if it has never been set before, or leave it blank if there is no file
            # (i.e. if this is a metapackage).
            try:
                md5gen = hashlib.md5()
                # print("Before reading, self.content_file is open? {}".format(not self.content_file.closed))
                # print("Before reading, self.content_file.file is open? {}".format(not self.content_file.file.closed))
                # print("self.content_file.file is {}".format(self.content_file.file))
                # print("How about now, is self.content_file open? {}".format(not self.content_file.closed))
                # print("")

                # Get the initial state of content_file, so we can preserve it afterwards.
                initially_closed = self.content_file.closed
                md5gen.update(self.content_file.read())
                if initially_closed:
                    self.content_file.close()

                self.MD5_checksum = md5gen.hexdigest()

            except ValueError:
                self.MD5_checksum = ""
        else:
            # The CodeResourceRevision already existed, so we should check the MD5.
            curr_md5 = self.compute_md5()
            if curr_md5 != self.MD5_checksum:
                raise ValidationError(
                    "File has been corrupted: original MD5=%(orig_md5)s, current MD5=%(curr_md5)s",
                    params={
                        "orig_md5": self.MD5_checksum,
                        "curr_md5": curr_md5
                    }
                )

        # Check for a circular dependency.
        if self.has_circular_dependence():
            raise ValidationError("Self-referential dependency")

        for dep in self.dependencies.all():
            dep.clean()

        # Check if dependencies conflict with each other
        listOfDependencyPaths = self.list_all_filepaths()
        if len(set(listOfDependencyPaths)) != len(listOfDependencyPaths):
            raise ValidationError("Conflicting dependencies")

        # If content file exists, it must have a file name
        if self.content_file and self.coderesource.filename == "":
            raise ValidationError("If content file exists, it must have a file name")

        # If no content file exists, it must not have a file name
        if not self.content_file and self.coderesource.filename != "":
            raise ValidationError("Cannot have a filename specified in the absence of a content file")

        # Check that user/group access is coherent.
        self.validate_restrict_access([self.coderesource])
        if self.revision_parent is not None:
            self.validate_restrict_access([self.revision_parent])

    def install(self, install_path):
        """
        Install this CRR into the specified path.

        PRE: install_path exists and has all the sufficient permissions for us
        to write our files into.
        """
        self.install_h(install_path, self.coderesource.filename)

    def install_h(self, install_path, base_name):
        """Helper for install."""
        self.logger.debug("Writing code to {}".format(install_path))

        # Install if not a metapackage.
        if base_name != "":
            dest_path = os.path.join(install_path, base_name)
            with open(dest_path, "w") as f:
                self.content_file.open()
                with self.content_file:
                    shutil.copyfileobj(self.content_file, f)
            # Make sure this is written with read, write, and execute
            # permission.
            os.chmod(dest_path, stat.S_IRWXU)
            # This will tailor the permissions further if we are running
            # sandboxes with another user account via SSH.
            file_access_utils.configure_sandbox_permissions(dest_path)

        for dep in self.dependencies.all():
            # Create any necessary sub-directory.  This should never
            # fail because we're in a nice clean working directory and
            # we already checked that this CRR doesn't have file
            # conflicts.  (Thus if an exception is raised, we want to
            # propagate it as that's a pretty deep problem.)
            path_for_deps = os.path.normpath(os.path.join(install_path, dep.depPath))
            # the directory may already exist due to another dependency --
            # or if depPath is ".".
            try:
                os.makedirs(path_for_deps)
            except os.error:
                pass

            # Get the base name of this dependency.  If no special value
            # is specified in dep, then use the dependency's CRR name.
            dep_fn = dep.depFileName
            if dep_fn == "":
                dep_fn = dep.requirement.coderesource.filename

            dep.requirement.install_h(path_for_deps, dep_fn)

    def get_absolute_url(self):
        """
        A page that allows user to add a revision of the CodeResource
        with this CRRevision as its parent.
        """
        return '/resource_revision_add/%i' % self.id

    def get_view_url(self):
        """
        A page that displays the source of this CodeResourceRevision
        """
        return '/resource_revision_view/%i' % self.id

    @transaction.atomic
    def remove(self):
        removal_plan = self.build_removal_plan()
        remove_helper(removal_plan)

    @transaction.atomic
    def build_removal_plan(self, removal_accumulator=None):
        removal_plan = removal_accumulator or empty_removal_plan()
        assert self not in removal_plan["CodeResourceRevisions"]
        removal_plan["CodeResourceRevisions"].add(self)

        for dependant in self.needed_by.all().select_related("coderesourcerevision"):
            if dependant.coderesourcerevision not in removal_plan["CodeResourceRevisions"]:
                update_removal_plan(
                    removal_plan,
                    dependant.coderesourcerevision.build_removal_plan(removal_plan)
                )

        for method in self.methods.all():
            if method not in removal_plan["Methods"]:
                update_removal_plan(removal_plan, method.build_removal_plan(removal_plan))

        return removal_plan

    @transaction.atomic
    def remove_list(self):
        datasets_listed = set()
        ERs_listed = set()
        runs_listed = set()
        pipelines_listed = set()
        methods_listed = set()
        CRRs_listed = {self}

        for dependant in self.needed_by.all().select_related("coderesourcerevision"):
            stuff_removed = dependant.coderesourcerevision.remove_list()
            datasets_listed.update(stuff_removed[0])
            ERs_listed.update(stuff_removed[1])
            runs_listed.update(stuff_removed[2])
            pipelines_listed.update(stuff_removed[3])
            methods_listed.update(stuff_removed[4])
            CRRs_listed.update(stuff_removed[5])

        for method in self.members.all():
            stuff_removed = method.remove_list()
            datasets_listed.update(stuff_removed[0])
            ERs_listed.update(stuff_removed[1])
            runs_listed.update(stuff_removed[2])
            pipelines_listed.update(stuff_removed[3])

        return datasets_listed, ERs_listed, runs_listed, pipelines_listed, methods_listed, CRRs_listed

    def find_update(self):
        update = self.coderesource.revisions.latest('revision_number')
        return update if update != self else None


@python_2_unicode_compatible
class CodeResourceDependency(models.Model):
    """
    Dependencies of a CodeResourceRevision - themselves also CRRs.

    Related to :model:`method.CodeResourceRevision`
    """

    coderesourcerevision = models.ForeignKey(CodeResourceRevision, related_name="dependencies")

    # Dependency is a codeResourceRevision
    requirement = models.ForeignKey(CodeResourceRevision, related_name="needed_by")

    # Where to place it during runtime relative to the CodeResource
    # that relies on this CodeResourceDependency.
    depPath = models.CharField(
        "Dependency path",
        max_length=255,
        help_text="Where a code resource dependency must exist in the sandbox relative to it's parent",
        blank=True)

    depFileName = models.CharField(
        "Dependency file name",
        max_length=255,
        help_text="The file name the dependency is given on the sandbox at execution",
        blank=True)

    def clean(self):
        """
        depPath cannot reference ".."
        """
        # Collapse down to a canonical path
        self.depPath = os.path.normpath(self.depPath)
        if any(component == ".." for component in self.depPath.split(os.sep)):
            raise ValidationError("depPath cannot reference ../")

        # If the child CR is a meta-package (no filename), we cannot
        # have a depFileName as this makes no sense
        if self.requirement.coderesource.filename == "" and self.depFileName != "":
            raise ValidationError("Metapackage dependencies cannot have a depFileName")

        # Check that user/group access is coherent.
        self.coderesourcerevision.validate_restrict_access([self.requirement])

    def __str__(self):
        """Represent as [codeResourceRevision] requires [dependency] as [dependencyLocation]."""
        return "{} {} requires {} {} as {}".format(
                self.coderesourcerevision.coderesource,
                self.coderesourcerevision,
                self.requirement.coderesource,
                self.requirement,
                os.path.join(self.depPath, self.depFileName))


@python_2_unicode_compatible
class Method(transformation.models.Transformation):
    """
    Methods are atomic transformations.

    Inherits from :model:`copperfish.Transformation`
    Related to :model:`copperfish.CodeResource`
    Related to :model:`copperfish.MethodFamily`
    """

    DETERMINISTIC = 1
    REUSABLE = 2
    NON_REUSABLE = 3
    REUSABLE_CHOICES = (
        (DETERMINISTIC, "deterministic"),
        (REUSABLE, "reusable"),
        (NON_REUSABLE, "non-reusable")
    )

    family = models.ForeignKey("MethodFamily", related_name="members")
    revision_parent = models.ForeignKey("self", related_name="descendants", null=True, blank=True,
                                        on_delete=models.SET_NULL)

    # moved this here from Transformation so that it can be put into the
    # unique_together statement below. Allowed to be blank because it's
    # automatically set on save.
    revision_number = models.PositiveIntegerField(
        'Method revision number',
        help_text='Revision number of this Method in its family',
        blank=True
    )

    # Code resource revisions are executable if they link to Method
    driver = models.ForeignKey(CodeResourceRevision, related_name="methods")
    reusable = models.PositiveSmallIntegerField(
        choices=REUSABLE_CHOICES,
        default=DETERMINISTIC,
        help_text="""Is the output of this method the same if you run it again with the same inputs?

deterministic: always exactly the same

reusable: the same but with some insignificant differences (e.g., rows are shuffled)

non-reusable: no -- there may be meaningful differences each time (e.g., timestamp)
""")
    tainted = models.BooleanField(default=False, help_text="Is this Method broken?")

    threads = models.PositiveIntegerField(
        "Number of threads",
        help_text="How many threads does this Method use during execution?",
        default=1,
        validators=[MinValueValidator(1)]
    )

    # Implicitly defined:
    # - execrecords: from ExecRecord

    @property
    def display_name(self):
        return '{}: {}'.format(self.revision_number, self.revision_name)

    class Meta:
        unique_together = (("family", "revision_number"))
        ordering = ["family__name", "-revision_number"]

    def __init__(self, *args, **kwargs):
        super(Method, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)

    def __str__(self):
        """Represent a method by it's revision name and method family"""
        string_rep = "{}:{} ({})".format("{}", self.revision_number, self.revision_name)

        # MethodFamily may not be temporally saved in DB if created by the admin page.
        if hasattr(self, "family"):
            return string_rep.format(unicode(self.family))
        else:
            return string_rep.format("[family unset]")

    def save(self, *args, **kwargs):
        """Save a Method, automatically setting the revision number."""
        if not self.revision_number:
            self.revision_number = self.family.next_revision()

        super(Method, self).save(*args, **kwargs)

    @property
    def is_method(self):
        return True

    @property
    def is_pipeline(self):
        return False

    @property
    def is_cable(self):
        return False

    @property
    def is_incable(self):
        return False

    @property
    def is_outcable(self):
        return False

    @property
    def family_size(self):
        """Returns size of this Method's family"""
        return self.family.members.count()

    def clean(self):
        """
        Check coherence of this Method. The checks we perform are:

        - Method does not have a Metapackage as a driver.
        """
        super(Method, self).clean()
        if not self.driver.content_file:
            raise ValidationError('Method "{}" cannot have CodeResourceRevision "{}" as a driver, because it has no '
                                  'content file.'.format(self, self.driver))

        # Check that permissions are coherent.
        self.validate_restrict_access([self.family])
        self.validate_restrict_access([self.driver])

    def complete_clean(self):
        """Check coherence and completeness of this Method.

        Checks that the Method is clean, and that no identical
        Methods already exist in the database.
        """
        self.clean()
        # for other_method in Method.objects.filter(driver=self.driver).exclude(pk=self.pk):
        #     if self.is_identical(other_method):
        #         raise ValidationError("An identical method already exists")

    def copy_io_from_parent(self):
        """
        Copy inputs and outputs from parent revision.
        """
        # If no parent revision exists, there are no input/outputs to copy
        if self.revision_parent is None:
            return None

        # If inputs/outputs already exist, do nothing.
        if (self.inputs.count() + self.outputs.count() != 0):
            return None
        # Copy all inputs/outputs (Including raws) from parent revision to this revision
        else:
            for parent_input in self.revision_parent.inputs.all():
                new_input = self.inputs.create(
                    dataset_name=parent_input.dataset_name,
                    dataset_idx=parent_input.dataset_idx)
                if not parent_input.is_raw():
                    transformation.models.XputStructure(
                        transf_xput=new_input,
                        compounddatatype=parent_input.get_cdt(),
                        min_row=parent_input.get_min_row(),
                        max_row=parent_input.get_max_row()).save()

            for parent_output in self.revision_parent.outputs.all():
                new_output = self.outputs.create(
                    dataset_name=parent_output.dataset_name,
                    dataset_idx=parent_output.dataset_idx)
                if not parent_output.is_raw():
                    transformation.models.XputStructure(
                        transf_xput=new_output,
                        compounddatatype=parent_output.get_cdt(),
                        min_row=parent_output.get_min_row(),
                        max_row=parent_output.get_max_row()).save()

    def find_compatible_ERs(self, input_datasets, runstep):
        """
        Given a set of input datasets, find any ExecRecords that use these inputs that
        are accessible by the calling RunStep.

        Note that this ExecRecord may be a failure, which the calling function
        would then handle appropriately.
        """
        if self.reusable == Method.NON_REUSABLE:
            return []

        # For pipelinesteps featuring this method....
        candidates = []
        for possible_PS in self.pipelinesteps.all():

            # For linked runsteps which did not *completely* reuse an ER....
            for possible_RS in possible_PS.pipelinestep_instances.filter(
                    reused=False,
                    execrecord_id__isnull=False):
                candidate_ER = getattr(possible_RS, 'execrecord', None)
                if not candidate_ER or candidate_ER.is_redacted():
                    continue

                # Check that this ER is accessible by runstep.
                extra_users, extra_groups = runstep.top_level_run.extra_users_groups(
                    [candidate_ER.generating_run])
                if len(extra_users) > 0 or len(extra_groups) > 0:
                    continue

                # Check if inputs match.
                ER_matches = True

                for ERI in candidate_ER.execrecordins.all():
                    input_idx = ERI.generic_input.definite.dataset_idx
                    if ERI.dataset != input_datasets[input_idx-1]:
                        ER_matches = False
                        break

                if ER_matches:
                    # All ERIs match input datasets, so commit to candidate ER.
                    candidates.append(candidate_ER)

        return candidates

    def _poll_stream(self, source_stream, source_name, dest_streams):
        """ Redirect all input from source_stream to all the dest_streams

        This is a helper function for run_code, like the Unix tee command.
        @param source_stream: an input stream to redirect
        @param dest_streams: a sequence of streams to redirect output to
        """
        for line in source_stream:
            # drops \n
            self.logger.debug('%s: %s', source_name, line.rstrip().decode('utf-8'))

            for stream in dest_streams:
                stream.write(line)

    def _capture_stream(self, source_stream, dest_streams):
        """
        Read the source stream and multiplex its output to all destination streams.
        """
        source_contents = source_stream.read()
        # As in _poll_stream, this drops the trailing \n.
        for dest_stream in dest_streams:
            dest_stream.write(source_contents)

    def run_code(self,
                 run_path,
                 input_paths,
                 output_paths,
                 output_streams,
                 error_streams,
                 log=None,
                 details_to_fill=None,
                 stop_execution_callback=None):
        """
        Run this Method with the specified inputs and outputs.

        SYNOPSIS
        Run the method with the specified inputs and outputs, writing each
        line of its stdout/stderr to all of the specified streams.

        Return the Method's return code, or -1 if the Method suffers an
        OS-level error (ie. is not executable), or -2 if execution is
        stopped.

        If details_to_fill is not None, fill it in with the return code, and
        set its output and error logs to the provided handles (meaning
        these should be files, not standard streams, and they must be open
        for reading AND writing).

        If log is not None, set its start_time and end_time immediately
        before and after calling invoke_code.

        INPUTS
        run_path        see invoke_code
        input_paths     see invoke_code
        output_paths    see invoke_code
        output_streams  list of streams (eg. open file handles) to output stdout to
        error_streams   list of streams (eg. open file handles) to output stderr to
        log             object with start_time and end_time fields to fill in (either
                        VerificationLog, or ExecLog)
        details_to_fill object with return_code, output_log, and error_log to fill in
                        (either VerificationLog, or MethodOutput)

        ASSUMPTIONS
        1) if details_to_fill is provided, the first entry in output_streams and error_streams
        are handles to regular files, open for reading and writing.
        """
        if log:
            log.start(save=False)

        return_code = None
        try:
            method_popen = self.invoke_code(run_path, input_paths, output_paths)
        except OSError:
            for stream in error_streams:
                traceback.print_exc(file=stream)
            return_code = -1

        is_terminated = False
        # Successful execution.
        if return_code is None:
            if stop_execution_callback is None:
                self.logger.debug("Polling Popen + displaying stdout/stderr to console")

                err_thread = threading.Thread(
                    target=self._poll_stream,
                    args=(method_popen.stderr, 'stderr', error_streams))
                err_thread.start()
                self._poll_stream(method_popen.stdout, 'stdout', output_streams)
                err_thread.join()

                return_code = method_popen.wait()

            else:
                # While periodically checking for a STOP message, we
                # monitor the progress of method_popen and update the
                # streams.
                while method_popen.returncode is not None:
                    if stop_execution_callback() is not None:
                        # We have received a STOP message.  Terminate method_popen.
                        method_popen.terminate()
                        return_code = -2
                        is_terminated = True
                        break

                    time.sleep(settings.SLEEP_SECONDS)
                    method_popen.poll()

                # Having stopped one way or another, make sure we capture the rest of the output.
                self._capture_stream(method_popen.stderr, error_streams)
                self._capture_stream(method_popen.stdout, output_streams)
                if not is_terminated:
                    return_code = method_popen.returncode

        for stream in output_streams + error_streams:
            stream.flush()

        with transaction.atomic():
            if log:
                log.stop(save=True, clean=True)

            # TODO: I'm not sure how this is going to handle huge output,
            # it would be better to update the logs as we go.
            if details_to_fill:
                self.logger.debug('return code is %s for %r.',
                                  return_code,
                                  details_to_fill)
                details_to_fill.return_code = return_code
                outlog = output_streams[0]
                errlog = error_streams[0]
                outlog.seek(0)
                errlog.seek(0)

                details_to_fill.error_log.save(errlog.name, File(errlog))
                details_to_fill.output_log.save(outlog.name, File(outlog))
                details_to_fill.clean()
                details_to_fill.save()

        if is_terminated:
            raise StopExecution("Execution of method {} was stopped.".format(self))

    def invoke_code(self, run_path, input_paths, output_paths,
                    ssh_sandbox_worker_account=settings.KIVE_SANDBOX_WORKER_ACCOUNT):
        """
        SYNOPSIS
        Runs a method using the run path and input/outputs.
        Leaves responsibility of DB annotation up to execute().
        Leaves routing of output/error streams to run_code.

        INPUTS
        run_path        Directory where code will be run
        input_paths     List of input files expected by the code
        output_paths    List of where code will write results
        ssh_sandbox_worker_account
                        Name of the user account that the code will be invoked by
                        (see kive.settings for more details).  If blank, code will
                        be invoked directly by the current user

        OUTPUTS
        A running subprocess.Popen object which is asynchronous

        ASSUMPTIONS
        1) The CRR of this Method can interface with Shipyard.
        Ie, it has positional inputs and outputs at command line:
        script_name.py [input 1] ... [input n] [output 1] ... [output k]

        2) The caller is responsible for cleaning up the stdout/err
        file handles after the Popen has finished processing.

        3) We don't handle exceptions of Popen here, the caller must do that.
        """
        if len(input_paths) != self.inputs.count() or len(output_paths) != self.outputs.count():
            raise ValueError('Method "{}" expects {} inputs and {} outputs, but {} inputs and {} outputs were supplied'
                             .format(self, self.inputs.count(), self.outputs.count(), len(input_paths),
                                     len(output_paths)))

        self.logger.debug("Checking run_path exists: {}".format(run_path))
        file_access_utils.set_up_directory(run_path, tolerate=True)

        for input_path in input_paths:
            self.logger.debug("Confirming input file exists + readable: {}".format(input_path))
            f = open(input_path, "rb")
            f.close()

        for output_path in output_paths:
            self.logger.debug("Confirming output path doesn't exist: {}".format(output_path))
            can_create, reason = file_access_utils.can_create_new_file(output_path)

            if not can_create:
                raise ValueError(reason)

        self.logger.debug("Installing CodeResourceRevision driver to file system: {}".format(self.driver))
        self.driver.install(run_path)

        # At this point, run_path has all of the necessary stuff
        # written into place.  It remains to execute the code.
        # The code to be executed sits in
        # [run_path]/[driver.coderesource.name],
        # and is executable.
        code_to_run = os.path.join(
            run_path,
            self.driver.coderesource.filename
        )

        if ssh_sandbox_worker_account:
            # We have to first cd into the appropriate directory before executing the command.
            ins_and_outs = '"{}"'.format(input_paths[0]) if len(input_paths) > 0 else ""
            for input_path in input_paths[1:] + output_paths:
                ins_and_outs += ' "{}"'.format(input_path)
            full_command = '"{}" {}'.format(code_to_run, ins_and_outs)
            ssh_command = 'cd "{}" && {}'.format(run_path, full_command)

            kive_sandbox_worker_preamble = [
                "ssh",
                "{}@localhost".format(ssh_sandbox_worker_account)
            ]
            command = kive_sandbox_worker_preamble + [ssh_command]

        else:
            command = [code_to_run] + input_paths + output_paths

        self.logger.debug("subprocess.Popen({})".format(command))
        code_popen = subprocess.Popen(command, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                      cwd=run_path)

        return code_popen

    def is_identical(self, other):
        """Is this Method identical to another one?"""
        return self.driver == other.driver and super(Method, self).is_identical(super(Method, other))

    @transaction.atomic
    def remove(self):
        """
        Cleanly remove this Method from the database.
        """
        # Remove all Pipelines that use this Method.  This will eventually make its way over to
        # remove the ExecLogs and ExecRecords too.
        removal_plan = self.build_removal_plan()
        remove_helper(removal_plan)

    def build_removal_plan(self, removal_accumulator=None):
        removal_plan = removal_accumulator or empty_removal_plan()
        assert self not in removal_plan["Methods"]
        removal_plan["Methods"].add(self)

        pipelines_affected = set([ps.pipeline for ps in self.pipelinesteps.all()])
        for pipeline_affected in pipelines_affected:
            if pipeline_affected not in removal_plan["Pipelines"]:
                update_removal_plan(removal_plan, pipeline_affected.build_removal_plan(removal_plan))

        return removal_plan

    def remove_list(self):
        datasets_listed = set()
        ERs_listed = set()
        runs_listed = set()
        pipelines_listed = set()

        pipelines_affected = set([ps.pipeline for ps in self.pipelinesteps.all()])
        for pipeline_affected in pipelines_affected:
            curr_datasets_listed, curr_ERs_listed, curr_runs_listed, curr_pipelines_listed = pipeline_affected.remove_list()
            datasets_listed.update(curr_datasets_listed)
            ERs_listed.update(curr_ERs_listed)
            runs_listed.update(curr_runs_listed)
            pipelines_listed.update(curr_pipelines_listed)

        return datasets_listed, ERs_listed, runs_listed, pipelines_listed


@python_2_unicode_compatible
class MethodFamily(transformation.models.TransformationFamily):
    """
    MethodFamily groups revisions of Methods together.

    Inherits :model:`transformation.TransformationFamily`
    Related to :model:`method.Method`
    """
    # Implicitly defined:
    #   members (Method/ForeignKey)

    @property
    def num_revisions(self):
        """Number of revisions within this family."""
        return self.members.count()

    def max_revision(self):
        """
        Return the maximum revision number of all member Methods.
        """
        return self.members.aggregate(Max('revision_number'))['revision_number__max']

    def next_revision(self):
        """
        Return a number suitable for assigning to the next revision to be added.
        """
        max_rev = self.max_revision()
        return (max_rev if max_rev is not None else 0) + 1

    def __str__(self):
        return self.name

    @transaction.atomic
    def remove(self):
        removal_plan = self.build_removal_plan()
        remove_helper(removal_plan)

    def build_removal_plan(self):
        removal_plan = empty_removal_plan()
        removal_plan["MethodFamilies"].add(self)

        for method in self.members.all():
            if method not in removal_plan["Methods"]:
                update_removal_plan(removal_plan, method.build_removal_plan(removal_plan))

        return removal_plan

    def remove_list(self):
        datasets_listed = set()
        ERs_listed = set()
        runs_listed = set()
        pipelines_listed = set()
        methods_listed = set()

        for method in self.members.all():
            curr_datasets_lsited, curr_ERs_listed, curr_runs_listed, curr_pipelines_listed = method.remove_list()
            datasets_listed.update(curr_datasets_lsited)
            ERs_listed.update(curr_ERs_listed)
            runs_listed.update(curr_runs_listed)
            pipelines_listed.update(curr_pipelines_listed)
            methods_listed.add(method)

        return datasets_listed, ERs_listed, runs_listed, pipelines_listed, methods_listed


# Register signals.
post_delete.connect(method.signals.code_resource_revision_post_delete, sender=CodeResourceRevision)
