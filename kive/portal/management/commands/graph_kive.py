import itertools
import os

from django.conf import settings
from django.core.management import call_command
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'Generates class diagrams.'

    def handle(self, *args, **options):
        if 'django_extensions' not in settings.INSTALLED_APPS:
            exit('django_extensions not found, try using --setting kive.UML_settings')

        docs_path = os.path.join(os.path.pardir, 'docs', 'models')
        apps = [app for app in settings.INSTALLED_APPS
                if not (app.startswith('django') or app == 'rest_framework')]
        apps.sort()
        for app in apps:
            print(app)
            exclude_models = ['User', 'Group']
            if app != 'metadata':
                exclude_models.append('AccessControl')
            call_command("graph_models",
                         app,
                         pygraphviz=True,
                         group_models=True,
                         outputfile=os.path.join(docs_path, app+'.png'),
                         exclude_models=','.join(exclude_models))

        readme_path = os.path.join(docs_path, 'README.md')
        with open(readme_path, 'rU+') as f:
            models_section = '### Models ###\n'
            header = itertools.takewhile(lambda line: line != models_section,
                                         f.readlines())
            f.seek(0)
            for line in header:
                f.write(line)
            f.write(models_section)
            for app in apps:
                f.write('#### {} ####\n'.format(app))
                f.write('![{} classes]({}.png)\n\n'.format(app, app))
