"""
librarian.models

Shipyard data models pertaining to the lookup of the past: ExecRecord,
SymbolicDataset, etc.
"""

from django.db import models
from django.contrib.contenttypes.models import ContentType
from django.contrib.contenttypes import generic
from django.core.exceptions import ValidationError

import librarian.models
from constants import error_messages

class ContentCheckLog(models.Model):
    """
    Denotes a check performed on a SymbolicDataset's contents.

    One of these should be created basically every time the contents
    of the SymbolicDataset are verified, whether that be during the
    execution of a Pipeline (i.e. a Run), on the uploading of data,
    or on a manually-specified check.
    """
    symbolicdataset = models.ForeignKey(
        "librarian.SymbolicDataset",
        related_name="content_checks")

    # The execution during which this check occurred, if applicable.
    execlog = models.ForeignKey(
        "archive.ExecLog", null=True,
        related_name="content_checks")

    start_time = models.DateTimeField("start time",
                                      auto_now_add=True,
                                      help_text="Time at start of content check")

    end_time = models.DateTimeField("end time",
                                    null=True,
                                    blank=True,
                                    help_text="Time at end of content check")

    def clean(self):
        """
        Check coherence of this ContentCheckLog.

        First, this calls clean on any BadData associated to it.  Second,
        it checks that end_time is later than start_time.
        """
        if self.is_fail():
            self.baddata.clean()

        if self.end_time is not None and self.start_time > self.end_time:
            raise ValidationError(
                error_messages["ccl_swapped_times"].format(self))

    def is_complete(self):
        """
        Checks if this ContentCheckLog is finished; that is, if the end_time is set.
        """
        return self.end_time is not None

    def is_fail(self):
        """True if this content check is a failure."""
        return hasattr(self, "baddata")

class BadData(models.Model):
    """
    Denotes a failed result from a content check.
    """
    contentchecklog = models.OneToOneField(ContentCheckLog,
                                           related_name="baddata")

    # In decreasing order of severity....
    missing_output = models.BooleanField(default=False)
    bad_header = models.NullBooleanField()
    bad_num_rows = models.NullBooleanField()

    def clean(self):
        """
        Checks coherence of this BadData record.

        If the output is missing, then bad_num_rows and bad_header
        must be null, and there should be no associated CellErrors; if
        bad_header is set then bad_num_rows must be null and there
        should be no associated CellErrors.

        The existence of associated CellErrors will be tested by
        cleaning them.
        """
        if self.missing_output:
            if self.bad_header != None:
                raise ValidationError(
                    "BadData \"{}\" represents missing output; bad_header should not be set".
                    format(self))
            
            if self.bad_num_rows != None:
                raise ValidationError(
                    "BadData \"{}\" represents missing output; bad_num_rows should not be set".
                    format(self))

            return
                
        if self.bad_header != None:
            if self.bad_num_rows != None:
                raise ValidationError(
                    "BadData \"{}\" has a malformed header; bad_num_rows should not be set".
                    format(self))

        for cell_error in self.bad_cells.all():
            cell_error.clean()

class CellError(models.Model):
    """
    Represents a cell that fails validation within a BadData object.
    """
    baddata = models.ForeignKey(BadData, related_name="bad_cells")
    row_num = models.PositiveIntegerField()
    column = models.ForeignKey("metadata.CompoundDatatypeMember")

    content_type = models.ForeignKey(
        ContentType,
        limit_choices_to = {
            "model__in": ("BasicConstraint", "CustomConstraint")
        },
        null=True,
        blank=True)
    object_id = models.PositiveIntegerField(null=True)
    # This shows which constraint failed; if it's null that means that
    # the parent BadData object failed the basic Python type-based
    # check.
    constraint_failed = generic.GenericForeignKey("content_type", "object_id")

    def clean(self):
        """
        Checks coherence of this CellError.

        If the parent BadData object has missing output, is raw, or has
        a bad header, this should not exist.

        The column must be a CDTM belonging to the associated SD's
        CDT.

        If the associated SD has num_rows != -1, then row_num must be
        less than or equal to that.

        The constraint failed, if it is not null, must belong to
        that CDTM's Datatype.
        """
        bad_SD = self.baddata.contentchecklog.symbolicdataset

        if self.baddata.missing_output:
            raise ValidationError(
                "Parent of CellError \"{}\" has missing output, so it should not exist".
                format(self))

        if self.baddata.contentchecklog.symbolicdataset.is_raw():
            raise ValidationError(
                "Parent of CellError \"{}\" is raw, so it should not exist".
                format(self))

        if self.baddata.bad_header:
            raise ValidationError(
                "Parent of CellError \"{}\" has a malformed header, so it should not exist".
                format(self))

        if not bad_SD.structure.compounddatatype.members.filter(
                pk=self.column.pk).exists():
            raise ValidationError(
                "Column of CellError \"{}\" is not one of the columns of its associated SymbolicDataset".
                format(self))

        if bad_SD.structure.num_rows != -1:
            if self.row_num > bad_SD.structure.num_rows:
                raise ValidationError(
                    "CellError \"{}\" refers to a row that does not exist".
                    format(self))

        if type(self.constraint_failed) == metadata.models.BasicConstraint:
            if not self.column.datatype.basic_constraints.filter(
                    pk=self.constraint_failed.pk).exists():
                raise ValidationError(
                    "CellError \"{}\" refers to a BasicConstraint that does not belong to the associated column".
                    format(self))

        elif type(self.constraint_failed) == metadata.models.CustomConstraint:
            if self.column.datatype.custom_constraint != self.constraint_failed:
                raise ValidationError(
                    "CellError \"{}\" refers to a CustomConstraint that does not belong to the associated column".
                    format(self))



class IntegrityCheckLog(models.Model):
    """
    Denotes an integrity check performed on a SymbolicDataset.

    One of these should be created basically every time the MD5
    checksum of the SD is confirmed, be it during the execution
    of a Pipeline (i.e. a Run) or on a manual check.
    """
    symbolicdataset = models.ForeignKey(
        "librarian.SymbolicDataset",
        related_name="integrity_checks")

    # The execution during which this check occurred, if applicable.
    execlog = models.ForeignKey("archive.ExecLog", null=True,
                                related_name="integrity_checks")

    start_time = models.DateTimeField("start time",
                                      auto_now_add=True,
                                      help_text="Time at start of integrity check")

    end_time = models.DateTimeField("end time",
                                    null=True,
                                    blank=True,
                                    help_text="Time at end of integrity check")

    def clean(self):
        """
        Checks coherence of this IntegrityCheckLog.

        Calls clean on its child MD5Conflict, if it exists.  Checks if
        end_time is later than start_time.
        """
        if self.is_fail():
            self.usurper.clean()

        if self.end_time is not None and self.start_time > self.end_time:
            raise ValidationError(
                error_messages["ccl_swapped_times"].format(self))

    def is_complete(self):
        """
        Checks if this IntegrityCheckLog is finished; that is, if the end_time is set.
        """
        return self.end_time is not None

    def is_fail(self):
        """True if this integrity check is a failure."""
        return hasattr(self, "usurper")

class MD5Conflict(models.Model):
    """
    Denotes an MD5 conflict found during an integrity check.
    """
    integritychecklog = models.OneToOneField(
        IntegrityCheckLog,
        related_name="usurper")
    
    conflicting_SD = models.OneToOneField(
        "librarian.SymbolicDataset",
        related_name="usurps")
