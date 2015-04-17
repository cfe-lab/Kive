# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('archive', '0008_runcomponent_complete_n_success'),
    ]

    operations = [
        migrations.AddField(
            model_name='methodoutput',
            name='_code_redacted',
            field=models.BooleanField(default=False),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='methodoutput',
            name='_error_redacted',
            field=models.BooleanField(default=False),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='methodoutput',
            name='_output_redacted',
            field=models.BooleanField(default=False),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='runcomponent',
            name='_redacted',
            field=models.BooleanField(default=False, help_text='Denotes whether this has been redacted. Private use only!'),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='methodoutput',
            name='error_log',
            field=models.FileField(help_text='Terminal error output of the RunStep Method, i.e. stderr.', upload_to='Logs', null=True, verbose_name='error log', blank=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='methodoutput',
            name='output_log',
            field=models.FileField(help_text='Terminal output of the RunStep Method, i.e. stdout.', upload_to='Logs', null=True, verbose_name='output log', blank=True),
            preserve_default=True,
        ),
    ]
