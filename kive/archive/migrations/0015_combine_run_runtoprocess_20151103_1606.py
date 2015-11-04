# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django.utils.timezone
from django.conf import settings
import django.core.validators


class Migration(migrations.Migration):

    dependencies = [
        ('librarian', '0004_redacted_20150417_1128'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('archive', '0014_dataset_name_length'),
    ]

    operations = [
        migrations.CreateModel(
            name='ExceedsSystemCapabilities',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('threads_requested', models.PositiveIntegerField(validators=[django.core.validators.MinValueValidator(1)])),
                ('max_available', models.PositiveIntegerField(validators=[django.core.validators.MinValueValidator(1)])),
                ('run', models.OneToOneField(related_name='not_enough_CPUs', to='archive.Run')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='RunInput',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('index', models.PositiveIntegerField()),
                ('run', models.ForeignKey(related_name='inputs', to='archive.Run')),
                ('symbolicdataset', models.ForeignKey(related_name='runinputs', to='librarian.SymbolicDataset')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.AddField(
            model_name='run',
            name='paused_by',
            field=models.ForeignKey(related_name='pauser', to=settings.AUTH_USER_MODEL, help_text='User that paused this Run', null=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='run',
            name='purged',
            field=models.BooleanField(default=False),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='run',
            name='sandbox_path',
            field=models.CharField(default='', max_length=256, blank=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='run',
            name='stopped_by',
            field=models.ForeignKey(related_name='stopper', to=settings.AUTH_USER_MODEL, help_text='User that stopped this Run', null=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='run',
            name='time_queued',
            field=models.DateTimeField(default=django.utils.timezone.now, null=True),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='run',
            name='name',
            field=models.CharField(max_length=60, verbose_name='Run name', blank=True),
            preserve_default=True,
        ),
    ]
