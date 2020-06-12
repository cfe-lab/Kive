# Generated by Django 2.2.10 on 2020-06-12 22:57

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('container', '0020_containerrun_is_warned'),
    ]

    operations = [
        migrations.AddField(
            model_name='containerdataset',
            name='multi_position',
            field=models.PositiveIntegerField(default=None, help_text='Position in a multi-valued argument (None for single-value arguments).', null=True),
        ),
    ]
