from __future__ import unicode_literals

from django.db import models, migrations
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        ('auth', '0001_initial'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('archive', '0005_auto_20150213_1703'),
    ]

    operations = [
        migrations.AddField(
            model_name='runstep',
            name='groups_allowed',
            field=models.ManyToManyField(help_text='What groups have access?', related_name='archive_runstep_has_access_to', null=True, to='auth.Group', blank=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='runstep',
            name='user',
            field=models.ForeignKey(default=1, to=settings.AUTH_USER_MODEL),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='runstep',
            name='users_allowed',
            field=models.ManyToManyField(help_text='Which users have access?', related_name='archive_runstep_has_access_to', null=True, to=settings.AUTH_USER_MODEL, blank=True),
            preserve_default=True,
        ),
    ]
