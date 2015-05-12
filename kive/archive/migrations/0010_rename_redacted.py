# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('archive', '0009_redacted_20150417_1128'),
    ]

    operations = [
        migrations.RenameField(
            model_name='methodoutput',
            old_name='_code_redacted',
            new_name='code_redacted',
        ),
        migrations.RenameField(
            model_name='methodoutput',
            old_name='_error_redacted',
            new_name='error_redacted',
        ),
        migrations.RenameField(
            model_name='methodoutput',
            old_name='_output_redacted',
            new_name='output_redacted',
        ),
    ]
