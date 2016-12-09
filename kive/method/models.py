"""
method.models

Shipyard data models relating to Methods: this includes everything to
do with CodeResources.
"""

from __future__ import unicode_literals
import pwd

from django.db import models, transaction
from django.db.models import Max
from django.db.models.signals import post_delete
from django.core.exceptions import ValidationError
from django.core.validators import RegexValidator, MinValueValidator
from django.utils.encoding import python_2_unicode_compatible
from django.core.urlresolvers import reverse
from django.conf import settings

import metadata.models
import transformation.models
import file_access_utils
from constants import maxlengths
import method.signals
from metadata.models import empty_removal_plan, remove_helper, update_removal_plan
from fleet.slurmlib import SlurmScheduler

import os
import stat
import hashlib
import logging
import shutil


@python_2_unicode_compatible
class CodeResource(metadata.models.AccessControl):
    """
    A CodeResource is any file tracked by Shipyard.
    Related to :model:`method.CodeResourceRevision`
    """
    name = models.CharField("Resource name", max_length=maxlengths.MAX_NAME_LENGTH,
                            help_text="The name for this resource and all subsequent revisions.",
                            unique=True)  # to prevent confusion in drop-down menus

    # File names must consist of alphanumerics, spaces, or "-._()".
    # This will prevent "../" as it contains a slash. They can't start or
    # end with spaces.
    filename = models.CharField(
        "Resource file name",
        max_length=maxlengths.MAX_FILENAME_LENGTH,
        help_text="The filename for this resource",
        validators=[
            RegexValidator(
                regex="^([-_.()\w]+ *)*[-_.()\w]+$",
                message='Filename must contain only: alphanumeric characters; spaces; and the characters -._(), '
                        'and cannot start with a space'
            )
        ]
    )
    description = models.TextField("Resource description", blank=True, max_length=maxlengths.MAX_DESCRIPTION_LENGTH)

    class Meta:
        ordering = ('name',)

    @property
    def absolute_url(self):
        """
        The URL for the page that displays all revisions of this CodeResource.
        """
        return reverse("resource_revisions", kwargs={"id": self.pk})

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
    Related to :model:`method.MethodDependency`
    Related to :model:`method.Method`
    """
    UPLOAD_DIR = "CodeResources"

    # Implicitly defined
    #   descendents (self/ForeignKey)
    #   used_by (MethodDependency/ForeignKey)
    #   methods (Method/ForeignKey)

    coderesource = models.ForeignKey(CodeResource, related_name="revisions")

    # revision_number is allowed to be null because it's automatically set on save
    revision_number = models.PositiveIntegerField(
        'Revision number', help_text="Revision number of code resource",
        blank=True
    )

    revision_name = models.CharField(
        max_length=maxlengths.MAX_NAME_LENGTH,
        help_text="A name to differentiate revisions of a CodeResource",
        blank=True
    )

    revision_DateTime = models.DateTimeField(
        auto_now_add=True,
        help_text="Date this resource revision was uploaded"
    )

    revision_parent = models.ForeignKey('self', related_name="descendants", null=True, blank=True,
                                        on_delete=models.SET_NULL)
    revision_desc = models.TextField(
        "Revision description",
        help_text="A description for this particular resource revision",
        max_length=maxlengths.MAX_DESCRIPTION_LENGTH,
        blank=True
    )

    content_file = models.FileField(
        "File contents",
        upload_to=UPLOAD_DIR,
        help_text="File contents of this code resource revision"
    )

    MD5_checksum = models.CharField(
        max_length=64,
        blank=True,
        help_text="Used to validate file contents of this resource revision"
    )

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

    @property
    def absolute_url(self):
        """
        A page that allows user to add a revision of the CodeResource
        with this CodeResourceRevision as its parent.
        """
        return reverse("resource_revision_add", kwargs={"id": self.pk})

    @property
    def view_url(self):
        """
        A page that displays this CodeResourceRevision.
        """
        return reverse("resource_revision_view", kwargs={"id": self.pk})

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
        Checks the MD5 of the CodeResourceRevision against its stored value.
        """
        # Recompute the MD5, see if it equals what is already stored.
        new_md5 = self.compute_md5()
        if self.MD5_checksum != new_md5:
            self.logger.warn('MD5 mismatch for %s: expected %s, but was %s.',
                             self.content_file,
                             self.MD5_checksum,
                             new_md5)
            return False
        #
        # for dep in self.dependencies.all():
        #     if not dep.requirement.check_md5():
        #         return False

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

        # Check that user/group access is coherent.
        self.validate_restrict_access([self.coderesource])
        if self.revision_parent is not None:
            self.validate_restrict_access([self.revision_parent])

    @transaction.atomic
    def remove(self):
        removal_plan = self.build_removal_plan()
        remove_helper(removal_plan)

    @transaction.atomic
    def build_removal_plan(self, removal_accumulator=None):
        removal_plan = removal_accumulator or empty_removal_plan()
        assert self not in removal_plan["CodeResourceRevisions"]
        removal_plan["CodeResourceRevisions"].add(self)

        for dependant in self.used_by.all().select_related("method"):
            if dependant.method not in removal_plan["Methods"]:
                update_removal_plan(
                    removal_plan,
                    dependant.method.build_removal_plan(removal_plan)
                )

        for meth in self.methods.all():
            if meth not in removal_plan["Methods"]:
                update_removal_plan(removal_plan, meth.build_removal_plan(removal_plan))

        return removal_plan

    def find_update(self):
        update = self.coderesource.revisions.latest('revision_number')
        return update if update != self else None


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

    # Moved this here from Transformation so that it can be put into the
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
    def display_name(self):
        return '{}: {}'.format(self.revision_number, self.revision_name)

    @property
    def absolute_url(self):
        return reverse("method_revise", kwargs={"id": self.pk})

    @property
    def view_url(self):
        return reverse("method_view", kwargs={"id": self.pk})

    def is_method(self):
        return True

    def is_pipeline(self):
        return False

    def is_cable(self):
        return False

    def is_incable(self):
        return False

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

        for dep in self.dependencies.all():
            dep.clean()

        # Check if dependencies conflict with each other.
        dependency_paths = self.list_all_filepaths()
        if len(set(dependency_paths)) != len(dependency_paths):
            dependency_paths.sort()
            raise ValidationError("Conflicting dependencies (full list: {})".format(dependency_paths))

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
        if self.inputs.count() + self.outputs.count() != 0:
            return None
        # Copy all inputs/outputs (Including raws) from parent revision to this revision
        else:
            for parent_input in self.revision_parent.inputs.all():
                new_input = self.inputs.create(
                    dataset_name=parent_input.dataset_name,
                    dataset_idx=parent_input.dataset_idx)
                if not parent_input.is_raw():
                    transformation.models.XputStructure.objects.create(
                        transf_xput=new_input,
                        compounddatatype=parent_input.get_cdt(),
                        min_row=parent_input.get_min_row(),
                        max_row=parent_input.get_max_row())

            for parent_output in self.revision_parent.outputs.all():
                new_output = self.outputs.create(
                    dataset_name=parent_output.dataset_name,
                    dataset_idx=parent_output.dataset_idx)
                if not parent_output.is_raw():
                    transformation.models.XputStructure.objects.create(
                        transf_xput=new_output,
                        compounddatatype=parent_output.get_cdt(),
                        min_row=parent_output.get_min_row(),
                        max_row=parent_output.get_max_row())

    def check_md5(self):
        """
        Checks the MD5 of the driver and its dependencies against their stored values.
        """
        if not self.driver.check_md5():
            return False

        for dep in self.dependencies.all():
            if not dep.requirement.check_md5():
                return False

        return True

    def list_all_filepaths(self):
        """
        Return all file paths associated with this Method, with the driver coming first.
        """
        file_paths = [os.path.normpath(self.driver.coderesource.filename)]
        file_paths.extend(
            [os.path.normpath(os.path.join(dep.path, dep.get_filename())) for dep in self.dependencies.all()]
        )
        return file_paths

    def install(self, install_path):
        """
        Install this Method's code into the specified path.

        RAISES: IOError, FileCreationError

        PRE: install_path exists and has all the sufficient permissions for us
        to write our files into.
        """
        base_name = self.driver.coderesource.filename
        self.logger.debug("Writing code to {}".format(install_path))

        destination_path = os.path.join(install_path, base_name)
        # This may raise an exception; we will propagate it up.
        file_access_utils.copy_and_confirm(self.driver.content_file.path, destination_path)

        # Make sure this is written with read, write, and execute
        # permission.
        os.chmod(destination_path, stat.S_IRWXU)
        # This will tailor the permissions further if we are running
        # sandboxes with another user account via SSH.
        file_access_utils.configure_sandbox_permissions(destination_path)

        for dep in self.dependencies.all():
            # Create any necessary sub-directory.  This directory may already exist due
            # to another dependency -- or if depPath is "." -- so we catch os.error.
            # We propagate any other errors.
            dep_dir = os.path.normpath(os.path.join(install_path, dep.path))
            try:
                os.makedirs(dep_dir)
            except os.error:
                pass

            # Write the dependency.
            dep_path = os.path.join(dep_dir, dep.get_filename())
            # This may raise an exception.
            file_access_utils.copy_and_confirm(dep.requirement.content_file.path, dep_path)

    def submit_code(self,
                    run_path,
                    input_paths,
                    output_paths,
                    stdout_path,
                    stderr_path,
                    after_okay=None,
                    uid=None,
                    gid=None,
                    priority=None,
                    job_name=None):
        """
        Submit this Method to Slurm for execution.

        The method runs with the specified inputs and outputs, writing its
        stdout/stderr to the specified streams.

        Return a SlurmJobHandle.

        INPUTS
        run_path        Directory where code will be run
        input_paths     List of input files expected by the code
        output_paths    List of where code will write results
        stdout_path     File path to write stdout to
        stderr_path     Path to write stderr to
        """
        if settings.KIVE_SANDBOX_WORKER_ACCOUNT:
            pwd_info = pwd.getpwnam(settings.KIVE_SANDBOX_WORKER_ACCOUNT)
            default_uid = pwd_info.pw_uid
            default_gid = pwd_info.pw_gid
        else:
            # Get our own current uid/gid.
            default_uid = os.getuid()
            default_gid = os.getgid()

        # Override the default UID and GID if possible.
        uid = uid or default_uid
        gid = gid or default_gid

        priority = priority or settings.DEFAULT_SLURM_PRIORITY
        job_name = job_name or self.driver.coderesource.filename

        job_handle = SlurmScheduler.submit_job(
            run_path,
            self.driver.coderesource.filename,
            input_paths + output_paths,
            uid,
            gid,
            priority,
            self.threads,
            stdout_path,
            stderr_path,
            after_okay,
            job_name=job_name
        )
        return job_handle

    def is_identical(self, other):
        """Is this Method identical to another one?"""
        return self.driver == other.driver and super(Method, self).is_identical(other)

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


@python_2_unicode_compatible
class MethodDependency(models.Model):
    """
    CodeResourceRevisions needed by a Method in support of its driver.

    Related to :model:`method.CodeResourceRevision`
    """
    method = models.ForeignKey(Method, related_name="dependencies")

    # Dependency is a codeResourceRevision
    requirement = models.ForeignKey(CodeResourceRevision, related_name="used_by")

    # Where to place it during runtime relative to the Method's sandbox directory.
    path = models.CharField(
        "Dependency path",
        max_length=255,
        help_text="Where a dependency must exist in the sandbox",
        blank=True
    )

    filename = models.CharField(
        "Dependency file name",
        max_length=255,
        help_text="The file name the dependency is given in the sandbox at execution",
        blank=True
    )

    def clean(self):
        """
        dep_path cannot reference ".."
        """
        # Collapse down to a canonical path
        self.path = os.path.normpath(self.path)
        if any(component == ".." for component in self.path.split(os.sep)):
            raise ValidationError("path cannot reference ../")

        # Check that user/group access is coherent.
        self.method.validate_restrict_access([self.requirement])

    def __str__(self):
        """Represent as [codeResourceRevision] requires [dependency] as [dependencyLocation]."""
        return "{} {} requires {} {} as {}".format(
            self.method.family,
            self.method,
            self.requirement.coderesource,
            self.requirement,
            os.path.join(self.path, self.get_filename())
        )

    def get_filename(self):
        return self.filename or self.requirement.coderesource.filename


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

    @property
    def absolute_url(self):
        """
        Gives the URL that lists all Methods under this family.
        """
        return reverse("methods", kwargs={"id": self.pk})

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

        for meth in self.members.all():
            if meth not in removal_plan["Methods"]:
                update_removal_plan(removal_plan, meth.build_removal_plan(removal_plan))

        return removal_plan


# Register signals.
post_delete.connect(method.signals.code_resource_revision_post_delete, sender=CodeResourceRevision)
