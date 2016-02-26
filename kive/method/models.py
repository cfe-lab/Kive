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
from django.core.urlresolvers import reverse

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
                                validators=[
                                    RegexValidator(regex="^(\b|([-_.()\w]+ *)*[-_.()\w]+)$",
                                                   message="Invalid code resource filename"),
                                ])
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

        for method in self.methods.all():
            if method not in removal_plan["Methods"]:
                update_removal_plan(removal_plan, method.build_removal_plan(removal_plan))

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

        for dep in self.dependencies.all():
            dep.clean()

        # Check if dependencies conflict with each other.
        dependency_paths = self.list_all_filepaths()
        if len(set(dependency_paths)) != len(dependency_paths):
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

        PRE: install_path exists and has all the sufficient permissions for us
        to write our files into.
        """
        base_name = self.driver.coderesource.filename
        self.logger.debug("Writing code to {}".format(install_path))

        destination_path = os.path.join(install_path, base_name)
        with open(destination_path, "w") as f:
            self.content_file.open()
            with self.content_file:
                shutil.copyfileobj(self.content_file, f)
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
            dep_path = os.path.join(dep_dir, dep.filename)
            with open(dep_path, "wb") as f:
                dep.requirement.content_file.open()
                with dep.requirement.content_file:
                    shutil.copyfileobj(dep.requirement.content_file, f)

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
                while method_popen.returncode is None:
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
        self.install(run_path)

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

        for method in self.members.all():
            if method not in removal_plan["Methods"]:
                update_removal_plan(removal_plan, method.build_removal_plan(removal_plan))

        return removal_plan


# Register signals.
post_delete.connect(method.signals.code_resource_revision_post_delete, sender=CodeResourceRevision)
