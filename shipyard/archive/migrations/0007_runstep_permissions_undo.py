from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('archive', '0006_runstep_permissions'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='runstep',
            name='groups_allowed',
        ),
        migrations.RemoveField(
            model_name='runstep',
            name='user',
        ),
        migrations.RemoveField(
            model_name='runstep',
            name='users_allowed',
        ),
    ]
