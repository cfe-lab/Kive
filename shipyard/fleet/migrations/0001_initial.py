# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
from django.conf import settings
import django.core.validators


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('archive', '0002_auto_20150128_0950'),
        ('pipeline', '0001_initial'),
        ('librarian', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='ExceedsSystemCapabilities',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('threads_requested', models.PositiveIntegerField(validators=[django.core.validators.MinValueValidator(1)])),
                ('max_available', models.PositiveIntegerField(validators=[django.core.validators.MinValueValidator(1)])),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='RunToProcess',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('sandbox_path', models.CharField(default=b'', max_length=256, blank=True)),
                ('time_queued', models.DateTimeField(auto_now_add=True)),
                ('pipeline', models.ForeignKey(to='pipeline.Pipeline')),
                ('run', models.ForeignKey(to='archive.Run', null=True)),
                ('user', models.ForeignKey(to=settings.AUTH_USER_MODEL)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='RunToProcessInput',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('index', models.PositiveIntegerField()),
                ('runtoprocess', models.ForeignKey(related_name='inputs', to='fleet.RunToProcess')),
                ('symbolicdataset', models.ForeignKey(to='librarian.SymbolicDataset')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.AddField(
            model_name='exceedssystemcapabilities',
            name='runtoprocess',
            field=models.OneToOneField(related_name='not_enough_CPUs', to='fleet.RunToProcess'),
            preserve_default=True,
        ),
    ]
