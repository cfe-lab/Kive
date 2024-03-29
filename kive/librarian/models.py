"""
librarian.models

Shipyard data models pertaining to the lookup of the past: ExecRecord,
Dataset, etc.
"""
import csv
import logging
import os
import os.path
import re
import time
import io

from django.contrib.humanize.templatetags.humanize import naturaltime
from django.db import models, transaction
from django.core.exceptions import ValidationError
from django.core.validators import RegexValidator
from django.core.files import File
from django.db.models.functions import Now
from django.utils import timezone
from django.conf import settings
from django.template.defaultfilters import filesizeformat, pluralize
from django.db.models.signals import post_delete
from django.urls import reverse

import metadata.models
import archive.models
import librarian.signals
from constants import maxlengths
from container.models import ContainerDataset
import six

import file_access_utils


LOGGER = logging.getLogger(__name__)


def get_upload_path(instance, filename):
    """
    Helper method for uploading dataset_files for Dataset.
    This is outside of the Dataset class, since @staticmethod and other method decorators were used instead of the
    method pointer when this method was inside Dataset class.

    :param instance:  Dataset instance
    :param filename: Dataset.dataset_file.name
    :return:  The upload directory for Dataset files.
    """
    # noinspection PyTypeChecker
    return instance.UPLOAD_DIR + os.sep + time.strftime('%Y_%m') + os.sep + filename


class ExternalFileDirectory(models.Model):
    """
    A database table storing directories whose contents we can make Datasets out of.
    """
    name = models.CharField(
        help_text="Human-readable name for this external file directory",
        unique=True,
        max_length=maxlengths.MAX_EXTERNAL_PATH_LENGTH
    )
    path = models.CharField(
        help_text="Absolute path",
        max_length=maxlengths.MAX_EXTERNAL_PATH_LENGTH
    )

    def __str__(self):
        return self.name

    def list_files(self):
        """
        Return a list of tuples representing files under this directory.

        The tuple looks like:
        ([absolute file path], [file path with external file directory name substituted])
        """
        path_with_slash = self.path if self.path.endswith("/") else "{}/".format(self.path)
        all_files = []
        for root, _dirs, files in sorted(os.walk(self.path)):
            for f in files:
                f = os.path.join(root, f)
                all_files.append((f, f.replace(path_with_slash, "[{}]/".format(self.name), 1)))
        return all_files

    def save(self, *args, **kwargs):
        """
        Normalize the path before saving.
        """
        self.path = os.path.normpath(self.path)
        super(ExternalFileDirectory, self).save(*args, **kwargs)


class Dataset(metadata.models.AccessControl):
    """
    A (possibly temporary) data file.

    That is to say, at some point, there was a data file uploaded to/
    generated by Shipyard, which was coherent with its
    specified/generating CDT and its producing
    TransformationOutput/cable (if it was generated), and this
    represents it, whether or not it was saved to the database.

    PRE: the actual file that the Dataset represents (whether
    it still exists or not) is/was coherent (e.g. checked using
    CDT.summarize_csv()).
    """
    UPLOAD_DIR = "Datasets"  # This is relative to kive.settings.MEDIA_ROOT

    name = models.CharField(max_length=maxlengths.MAX_FILENAME_LENGTH)
    description = models.TextField(help_text="Description of this Dataset.",
                                   max_length=maxlengths.MAX_DESCRIPTION_LENGTH,
                                   blank=True)
    date_created = models.DateTimeField(default=timezone.now,
                                        help_text="Date of Dataset creation.",
                                        db_index=True)

    # Datasets are stored in the "Datasets" folder
    dataset_file = models.FileField(upload_to=get_upload_path,
                                    help_text="Physical path where datasets are stored",
                                    blank=True,
                                    default='',
                                    db_index=True,
                                    max_length=maxlengths.MAX_FILENAME_LENGTH)

    externalfiledirectory = models.ForeignKey(
        ExternalFileDirectory,
        verbose_name="External file directory",
        help_text="External file directory containing the data file",
        null=True,
        blank=True,
        on_delete=models.CASCADE
    )
    external_path = models.CharField(
        help_text="Relative path of the file within the specified external file directory",
        blank=True,
        max_length=maxlengths.MAX_EXTERNAL_PATH_LENGTH
    )

    logger = logging.getLogger('librarian.Dataset')

    # For validation of Datasets when being reused, or when being
    # regenerated.  A blank MD5_checksum means that the file was
    # missing (not created when it was supposed to be created).
    MD5_checksum = models.CharField(
        max_length=64,
        validators=[RegexValidator(
            regex=re.compile("(^[0-9A-Fa-f]{32}$)|(^$)"),
            message="MD5 checksum is not either 32 hex characters or blank")],
        blank=True,
        default="",
        help_text="Validates file integrity")

    _redacted = models.BooleanField(default=False)

    # The last time a check was performed on this external file, to see whether
    # the external file referenced was still there.
    # See external_file_check() for details.
    last_time_checked = models.DateTimeField(default=timezone.now,
                                             help_text="Date-time of last (external) dataset existence check.",
                                             null=True)

    dataset_size = models.BigIntegerField(
        blank=True,
        null=True,
        help_text="Size of the dataset file in bytes.  If null, this has not been computed yet or there is no "
                  "internally stored file."
    )
    is_external_missing = models.BooleanField(
        default=False,
        help_text='True if the external file was missing when last checked.')
    is_uploaded = models.BooleanField(
        default=False,
        help_text='True if the file was uploaded, not an output.')

    class Meta:
        ordering = ["-date_created", "name"]

    def __init__(self, *args, **kwargs):
        super(Dataset, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)

    def __repr__(self):
        return 'Dataset(name={!r})'.format(self.name)

    def __str__(self):
        """
        Unicode representation of a Dataset.

        This is simply S[pk] if it has no data.
        """
        if not self.has_data():
            return "S{}".format(self.pk)

        display_name = self.name if self.name != "" else "[no name specified]"

        return "{} (created by {} on {})".format(display_name, self.user, self.date_created)

    @property
    def is_purged(self):
        return not (self.dataset_file or self.external_path)

    def external_absolute_path(self):
        if not self.external_path:
            return None
        return os.path.normpath(os.path.join(self.externalfiledirectory.path, self.external_path))

    def get_open_file_handle(self, mode="rb", raise_errors=False):
        """
        Retrieves an open Django file with which to access the data.

        This is self.dataset_file if possible, falls back to the external file if possible,
        and otherwise returns None.

        NOTE: for python3 there is a significant difference in opening a file in binary or
        text mode.
        Use binary when calculating hashes.
        Use text when read the CSV contents.
        """
        if self.dataset_file:
            try:
                self.dataset_file.open(mode)
            except IOError as e:
                if raise_errors:
                    raise
                self.logger.warning('error accessing dataset file: %s', e)
                return None
            return self.dataset_file
        elif self.external_path:
            abs_path = self.external_absolute_path()
            try:
                fhandle = open(abs_path, mode)
            except IOError as e:
                if raise_errors:
                    raise
                self.logger.warning('error accessing external file: %s', e)
                return None
            return File(fhandle, name=abs_path)
        if raise_errors:
            raise ValueError('Dataset has no dataset_file or external_path.')
        return None

    def all_rows(self, data_check=False, insert_at=None, limit=None, extra_errors=None):
        """ Returns an iterator over all rows of this Dataset.

        :param bool data_check: each field becomes a tuple: (value, [error])
        :param list insert_at: [column_index] add blank columns at each
        zero-based index
        :param int limit: maximum row number returned (header is not counted)
        :param list extra_errors: this will have extra rows added to it that
        contain the first error in each column, if they appear after the row
        limit. [(row_num, [(field_value, [error])])] row_num is 1-based.
        :return: an iterator over the rows, each row is either [field_value] or
        [(field_value, [error])], depending on data_check.
        """
        data_handle = self.get_open_file_handle("r")
        if data_handle is None:
            raise RuntimeError('Dataset file has been removed.')

        with data_handle:
            reader = csv.reader(data_handle)
            if data_check:
                row_errors = {}
            for row_num, row in enumerate(reader):
                if not data_check:
                    if limit is not None and row_num > limit:
                        break
                else:
                    row_error = row_errors.pop(row_num, {})
                    if limit is not None and row_num > limit and not row_error:
                        if row_errors:
                            # Still have errors on later rows
                            continue
                        # No more errors
                        break
                    new_row = []
                    for column_num, value in enumerate(row, 1):
                        failed_column = row_error.get(column_num)
                        if failed_column is None:
                            new_errors = []
                        else:
                            new_errors = failed_column.check_basic_constraints(value)
                        new_row.append((value, new_errors))

                    row = new_row
                if insert_at is not None:
                    dummy = ('', []) if data_check else ''
                    [row.insert(pos, dummy) for pos in insert_at]
                if limit is None or row_num <= limit:
                    yield row
                else:
                    extra_errors.append((row_num, row))

    def header(self):
        rows = self.all_rows()
        return next(rows)

    def rows(self, data_check=False, insert_at=None, limit=None, extra_errors=None):
        rows = self.all_rows(data_check,
                             insert_at,
                             limit=limit,
                             extra_errors=extra_errors)
        for i, row in enumerate(rows):
            if i == 0:
                pass  # skip header
            else:
                yield row

    def clean(self):
        """
        Checks coherence of this Dataset.

        If it has data (i.e. an associated Dataset), it cleans that
        Dataset.  Then, if there is an associated DatasetStructure,
        clean that.

        Note that the MD5 checksum is already checked via a validator.
        """
        if not (self.externalfiledirectory and self.external_path or
                not self.externalfiledirectory and not self.external_path):
            raise ValidationError(
                {
                    "external_path": "Both externalfiledirectory and external_path should be set or "
                                     "neither should be set"
                }
            )

        if self.has_data() and not self.check_md5():
            error_str = ('File integrity of "{}" lost. Current checksum "{}" does not equal expected checksum ' +
                         '"{}"').format(self, self.compute_md5(), self.MD5_checksum)
            raise ValidationError(
                {
                    "dataset_file": error_str
                }
            )

    # noinspection PyUnusedLocal
    def validate_uniqueness_on_upload(self, *args, **kwargs):
        """
        Validates that the name and MD5 of the Dataset are unique.

        This isn't at the model level because we do want to allow
        these duplicates (e.g. empty files generated by the same
        Pipeline), but we want to check files on upload.
        """
        query = Dataset.objects.filter(MD5_checksum=self.MD5_checksum,
                                       name=self.name)

        if query.exclude(pk=self.pk).exists():
            error_str = "A Dataset with that name and MD5 already exists."
            raise ValidationError(
                {
                    "dataset_file": error_str
                }
            )

    def get_access_limits(self, access_limits=None):
        if access_limits is None:
            access_limits = []

        # Is this an output from a container run?
        for container_dataset in self.containers.filter(argument__type='O'):
            access_limits.append(container_dataset.run)

        return access_limits

    @property
    def absolute_url(self):
        """
        :return str: URL to access the dataset_file
        """
        return reverse('dataset_download', kwargs={"dataset_id": self.id})

    def get_view_url(self):
        return reverse('dataset_view', kwargs={"dataset_id": self.id})

    def get_filesize(self):
        """
        :return int: size of dataset_file in bytes or None if the file handle
        cannot be accessed.
        """
        data_handle = None
        try:
            data_handle = self.get_open_file_handle("rb")
            if data_handle is None:
                return None
            return data_handle.size
        finally:
            if data_handle is not None:
                data_handle.close()

    def get_formatted_filesize(self):
        unformatted_size = self.get_filesize()
        if unformatted_size is None:
            return 'missing'
        return filesizeformat(unformatted_size)

    def compute_md5(self):
        """Computes the MD5 checksum of the Dataset.
        Return None if the file could not be accessed.
        """
        data_handle = self.get_open_file_handle("rb")
        if data_handle is None:
            self.logger.warning('cannot access file handle')
            return None
        with data_handle:
            return file_access_utils.compute_md5(data_handle.file)

    def check_md5(self):
        """
        Checks the MD5 checksum of the Dataset against its stored value.

        The stored value is used when regenerating data
        that once existed, as a coherence check.

        Return True if the check passed, otherwise False.
        """
        # Recompute the MD5, see if it equals what is already stored
        new_md5 = self.compute_md5()
        if self.MD5_checksum != new_md5:
            if self.dataset_file:
                filename = self.dataset_file.name
            else:
                filename = self.external_absolute_path()
            self.logger.warning('MD5 mismatch for %s: expected %s, but was %s.',
                                filename,
                                self.MD5_checksum,
                                new_md5)
            return False
        return True

    def has_data(self, raise_errors=False):
        try:
            data_handle = self.get_open_file_handle("rb", raise_errors=True)
            data_handle.close()
            return True
        except ValueError:
            # No file recorded for this dataset.
            return False
        except IOError:
            # Recorded file is not found or not readable.
            if raise_errors:
                raise
            return False

    def has_structure(self):
        """ Compound datatypes were removed, so all datasets are now raw. """
        return False

    def is_raw(self):
        """ Compound datatypes were removed, so all datasets are now raw. """
        return True

    def num_rows(self):
        """Returns number of rows in the associated Dataset.

        This returns None if the Dataset is raw.
        """
        return None if self.is_raw() else self.structure.num_rows

    def get_cdt(self):
        """ Compound datatypes were removed, so all datasets are now raw. """
        return None

    def set_md5(self, file_path=None, file_handle=None):
        """Set the MD5 hash from a file.

        Closes the file after the MD5 is computed.
        :param str file_path:  Path to file to calculate MD5 for.
            Defaults to dataset_file.path, and not used if file_handle supplied.
        :param file file_handle: file handle of file to calculate MD5.  File
            must be seeked to the beginning.
            If file_handle empty, then uses file_path.
        """
        opened_file_ourselves = False
        if file_handle is None:
            if file_path is None:
                file_path = self.dataset_file.path
            file_handle = io.open(file_path, "rb")
            opened_file_ourselves = True

        try:
            self.MD5_checksum = file_access_utils.compute_md5(file_handle)
        finally:
            if opened_file_ourselves:
                file_handle.close()

    @transaction.atomic
    def register_file(self, file_path, file_handle=None):
        """
        Save and register a new file for this Dataset.

        Compute and set the MD5.

        Closes the file afterwards if the file source is a string file path.
        Does not close the file afterwards if file source is a file handle.

        INPUTS
        file_path           file to upload as the new contents
        file_handle         file handle of the file to upload as the new contents.
                            If supplied, then does not reopen the file in file_path.
                            Moves handle to beginning of file before calculating MD5.
                            If None, then opens the file in file_path.

        PRE
        self must not have a file already associated
        """
        assert not bool(self.dataset_file)

        opened_file_ourselves = False
        if file_handle is None:
            file_handle = io.open(file_path, mode="rb")
            opened_file_ourselves = True

        try:
            full_name = file_path
            assert isinstance(full_name, six.string_types), "fname '{}' is not a string {}".format(
                file_handle.name,
                type(file_handle.name)
            )
            fname = os.path.basename(full_name)
            self.dataset_file.save(fname, File(file_handle))
        finally:
            if opened_file_ourselves:
                file_handle.close()

        self.clean()
        self.save()

    @classmethod
    def create_empty(cls, user=None, cdt=None, users_allowed=None, groups_allowed=None,
                     file_source=None, instance=None):
        """Create an empty Dataset.

        INPUTS
        cdt   CompoundDatatype for the new Dataset
                            (None indicates a raw Dataset)
        instance            None or a Dataset to fill in (e.g. if we get a dummy one from DatasetForm)

        OUTPUTS
        empty_dataset            Dataset with a blank MD5
        """
        users_allowed = users_allowed or []
        groups_allowed = groups_allowed or []

        if user is None:
            assert file_source is not None
            user = file_source.top_level_run.user
            users_allowed = file_source.top_level_run.users_allowed.all()
            groups_allowed = file_source.top_level_run.groups_allowed.all()
        elif file_source is not None:
            assert user == file_source.top_level_run.user
            assert set(users_allowed) == set(file_source.top_level_run.users_allowed.all())
            assert set(groups_allowed) == set(file_source.top_level_run.groups_allowed.all())

        with transaction.atomic():

            empty_dataset = instance or cls()
            empty_dataset.user = user
            empty_dataset.MD5_checksum = ""
            empty_dataset.dataset_file = None
            empty_dataset.file_source = file_source
            empty_dataset.last_time_checked = None
            # Save so we can add permissions.
            empty_dataset.save()

            if cdt:
                raise NotImplementedError(
                    'Compound data types are no longer supported.')

            for user in users_allowed:
                empty_dataset.users_allowed.add(user)
            for group in groups_allowed:
                empty_dataset.groups_allowed.add(group)
            empty_dataset.clean()

        return empty_dataset

    # noinspection PyUnusedLocal
    @classmethod
    def create_dataset(cls,
                       file_path,
                       user=None,
                       users_allowed=None,
                       groups_allowed=None,
                       cdt=None,
                       keep_file=True,
                       name=None,
                       description=None,
                       file_source=None,
                       check=True,
                       file_handle=None,
                       instance=None,
                       externalfiledirectory=None,
                       precomputed_md5=None,
                       is_uploaded=False):
        """
        Helper function to make defining SDs and Datasets faster.

        user and name must both be set if make_dataset=True.
        make_dataset creates a Dataset from the given file path to go
        with the SD. file_source can be a RunAtomic to register the
        Dataset with, or None if it was uploaded by the user (or if
        make_dataset=False). If check is True, do a ContentCheck on the
        file.  file_path is an absolute path; if externalfiledirectory
        is specified, file_path will be checked to ensure that it's
        inside the specified directory.

        Returns the Dataset created.
        """
        users_allowed = users_allowed or []
        groups_allowed = groups_allowed or []

        if user is None:
            assert file_source is not None
            user = file_source.top_level_run.user
            users_allowed = file_source.top_level_run.users_allowed.all()
            groups_allowed = file_source.top_level_run.groups_allowed.all()
        elif file_source is not None:
            assert user == file_source.top_level_run.user
            assert set(users_allowed) == set(file_source.top_level_run.users_allowed.all())
            assert set(groups_allowed) == set(file_source.top_level_run.groups_allowed.all())

        if file_path:
            LOGGER.debug("Creating Dataset from file {}".format(file_path))
            file_name = file_path
        elif file_handle:
            LOGGER.debug("Creating Dataset from file {}".format(file_handle.name))
            file_name = str(file_handle.name)
        else:
            raise ValueError("Must supply either the file path or file handle")

        if not isinstance(file_name, six.string_types):
            raise ValueError("file_name '{}' is not a string '{}'".format(file_name, type(file_name)))
        with transaction.atomic():
            external_path = ""
            # We do this in the transaction because we're accessing ExternalFileDirectory.
            if externalfiledirectory:
                # Check that file_path is in the specified ExternalFileDirectory.
                normalized_path = os.path.normpath(file_name)
                normalized_efd_with_slash = "{}/".format(os.path.normpath(externalfiledirectory.path))
                assert normalized_path.startswith(normalized_efd_with_slash)
                external_path = normalized_path.replace(normalized_efd_with_slash, "", 1)

            new_dataset = cls.create_empty(user, cdt=cdt,
                                           users_allowed=users_allowed, groups_allowed=groups_allowed,
                                           instance=instance, file_source=file_source)

            new_dataset.name = name or ""
            new_dataset.description = description or ""
            new_dataset.externalfiledirectory = externalfiledirectory
            new_dataset.external_path = external_path
            new_dataset.last_time_checked = timezone.now()
            new_dataset.is_uploaded = is_uploaded

            if precomputed_md5 is not None:
                new_dataset.MD5_checksum = precomputed_md5
            else:
                new_dataset.set_md5(file_name, file_handle)
            if file_handle is not None:
                file_handle.seek(0)

            if keep_file:
                new_dataset.register_file(file_path=file_name, file_handle=file_handle)

            new_dataset.clean()
            new_dataset.save()
        return new_dataset

    @transaction.atomic
    def build_redaction_plan(self, redaction_accumulator=None):
        """
        Create a list of what will be affected when redacting this Dataset.
        """
        redaction_plan = redaction_accumulator or archive.models.empty_redaction_plan()
        assert self not in redaction_plan["Datasets"]
        if self.is_redacted():
            return redaction_plan
        redaction_plan["Datasets"].add(self)

        # Make a special note if this Dataset is associated with an external file.
        if self.external_path:
            redaction_plan["ExternalFiles"].add(self)

        return redaction_plan

    @transaction.atomic
    def redact_this(self):
        """
        Helper function that only redacts this Dataset and does not handle any recursion.
        """
        if self.is_redacted():
            return

        self._redacted = True
        self.MD5_checksum = ""
        self.externalfiledirectory = None
        if self.external_path:
            self.external_path = ""
        self.save(update_fields=["_redacted", "MD5_checksum", "externalfiledirectory", "external_path"])

        if bool(self.dataset_file):
            self.dataset_file.delete(save=True)
        if self.has_structure():
            self.structure.delete()

    @transaction.atomic
    def redact(self):
        redaction_plan = self.build_redaction_plan()
        archive.models.redact_helper(redaction_plan)

    def is_redacted(self):
        return self._redacted

    @property
    def uploaded(self):
        """ Kept for backward compatibility in serializer. """
        return self.is_uploaded

    @transaction.atomic
    def build_removal_plan(self, removal_accumulator=None):
        """
        Make a manifest of objects to remove when removing this Dataset.
        """
        removal_plan = removal_accumulator or metadata.models.empty_removal_plan()
        assert self not in removal_plan["Datasets"]
        removal_plan["Datasets"].add(self)

        for run_dataset in self.containers.all():
            run = run_dataset.run
            if (run_dataset.argument.type == 'I' and
                    run not in removal_plan['ContainerRuns']):
                run.build_removal_plan(removal_plan)

        # Make a special note if this Dataset is associated with an external file.
        if self.external_path:
            removal_plan["ExternalFiles"].add(self)

        return removal_plan

    @transaction.atomic
    def remove(self):
        removal_plan = self.build_removal_plan()
        metadata.models.remove_helper(removal_plan)

    @classmethod
    def find_unneeded(cls):
        """ Finds datasets that could be purged.

        Excludes datasets that were uploaded or from an active run.
        :return: a QuerySet
        """
        # Exclude Datasets that are currently in use
        active_dataset_ids = ContainerDataset.objects.filter(
            run__end_time=None).values_list('dataset_id').order_by()

        unneeded = cls.objects.filter(
            is_uploaded=False).exclude(  # Only purge outputs.
            pk__in=active_dataset_ids).exclude(  # Don't purge while it's in use.
            dataset_file=None).exclude(  # External file.
            dataset_file='').exclude(  # Already purged.
            dataset_size=None)  # New dataset.
        return unneeded

    @classmethod
    def scan_file_names(cls):
        """ Yield all file names, relative to MEDIA_ROOT. """
        dataset_root = os.path.join(settings.MEDIA_ROOT, cls.UPLOAD_DIR)
        for dirpath, dirnames, filenames in os.walk(dataset_root):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                yield os.path.relpath(file_path, settings.MEDIA_ROOT)
            if not (dirnames or filenames):
                # Empty folder can be purged.
                yield os.path.relpath(dirpath, settings.MEDIA_ROOT)

    @classmethod
    def external_file_check(cls, batch_size=1000):
        """ Perform a consistency check of external files. """
        last_id = None
        missing_count = 0
        last_missing_date = None
        last_missing_path = None
        while True:
            batch = Dataset.objects.filter(
                externalfiledirectory__isnull=False).order_by(
                'id').prefetch_related('externalfiledirectory')
            if last_id is not None:
                batch = batch.filter(id__gt=last_id)
            batch = batch[:batch_size]
            batch_count = 0
            found_ids = []
            for dataset in batch:
                batch_count += 1
                last_id = dataset.id
                path_name = dataset.external_absolute_path()
                if path_name is None:
                    raise RuntimeError("Unexpected None for external dataset path!")
                if os.path.exists(path_name):
                    found_ids.append(dataset.id)
                else:
                    missing_count += 1
                    if not dataset.is_external_missing:
                        dataset.is_external_missing = True
                        if (last_missing_date is None or
                                dataset.last_time_checked > last_missing_date):
                            last_missing_date = dataset.last_time_checked
                            last_missing_path = path_name
                        dataset.save()
            Dataset.objects.filter(id__in=found_ids).update(
                last_time_checked=Now(),
                is_external_missing=False)
            if batch_count < batch_size:
                break
        if last_missing_date is not None:
            cls.logger.error(
                "Missing %d external dataset%s. Most recent from %s, last checked %s.",
                missing_count,
                pluralize(missing_count),
                last_missing_path,
                naturaltime(last_missing_date))

    def increase_permissions_from_json(self, permissions_json):
        """
        Grant permission to all users and groups specified in the parameter.

        The permissions_json parameter should be a JSON string formatted as it would
        be by the permissions widget used in the UI.
        """
        self.grant_from_json(permissions_json)

    def unique_filename(self) -> str:
        "Create a unique filename based on this dataset's name and ID."
        unique_id = self.id
        name, extension = os.path.splitext(
            self.name)  # Splitext retains a '.' if it's present
        return "{}_{}{}".format(name, unique_id, extension)


# Register signals.
post_delete.connect(librarian.signals.dataset_post_delete, sender=Dataset)
