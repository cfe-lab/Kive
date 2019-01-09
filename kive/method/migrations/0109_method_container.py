# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2018-08-07 18:07
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('container', '0002_ordering'),
        ('method', '0108_docker_image_tag_help'),
    ]

    operations = [
        migrations.AddField(
            model_name='method',
            name='container',
            field=models.ForeignKey(blank=True, help_text='The method will run inside this Singularity container.', null=True, on_delete=django.db.models.deletion.CASCADE, related_name='methods', to='container.Container'),
        ),
        migrations.AlterField(
            model_name='method',
            name='docker_image',
            field=models.ForeignKey(blank=True, help_text='The method will run inside this docker image (deprecated).', null=True, on_delete=django.db.models.deletion.CASCADE, related_name='methods', to='method.DockerImage'),
        ),
    ]