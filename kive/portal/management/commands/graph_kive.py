from django.conf import settings
from django.core.management.base import BaseCommand
from django.core.management import call_command
import os

class Command(BaseCommand):
    help = 'Resets the database and loads sample data.'
    
    def handle(self, *args, **options):
        if 'django_extensions' not in settings.INSTALLED_APPS:
            exit('django_extensions not found, try using --setting kive.UML_settings')
            
        apps = [app for app in settings.INSTALLED_APPS
                if not (app.startswith('django') or app == 'rest_framework')]
        apps.sort()
        for app in apps:
            print app
            call_command("graph_models",
                         app,
                         pygraphviz=True,
                         group_models=True,
                         outputfile=os.path.join(os.path.pardir,
                                                 'doc',
                                                 'models',
                                                 app+'.png'),
                         exclude_models="User,Group")
