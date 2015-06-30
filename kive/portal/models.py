"""
portal.models

Kive data models relating to general front-end functionality.
"""

from django.db import models
from django.contrib.auth.models import User
from django.db.models.signals import post_delete

import os.path

import portal.signals


class StagedFile(models.Model):
    UPLOAD_DIR = "StagedFiles"

    uploaded_file = models.FileField(
        "Uploaded file",
        upload_to=UPLOAD_DIR,
        help_text="Uploaded file held for further server-side processing")

    user = models.ForeignKey(
        User,
        help_text="User that uploaded this file")

    date_uploaded = models.DateTimeField(
        "Upload date",
        auto_now_add=True,
        help_text="Date and time of upload")

    def __unicode__(self):
        return "{}: {} ({} {})".format(
            self.pk,
            os.path.basename(self.uploaded_file.name),
            self.user,
            self.date_uploaded
        )


# Register a signal that ensures that files are removed when the StagedFile is deleted.
post_delete.connect(portal.signals.staged_file_post_delete, sender=StagedFile)