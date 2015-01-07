from optparse import make_option
import os
import shutil
import subprocess
import sys

from django.core.management.base import BaseCommand

import shipyard.settings  # @UnresolvedImport


class Command(BaseCommand):
    help = 'Resets the database and loads sample data.'
    
    option_list = BaseCommand.option_list + (
        make_option('--load', '-l', help="fixture name to load"), )
    
    def handle(self, *args, **options):
        python = sys.executable
        manage_script = sys.argv[0]
        fixture = options['load']
        
        targets = ["CodeResources",
                   "Datasets",
                   "Logs",
                   "Sandboxes",
                   "VerificationLogs",
                   "VerificationScripts"]
        for target in targets:
            target_path = os.path.join(shipyard.settings.MEDIA_ROOT, target)
            if os.path.isdir(target_path):
                shutil.rmtree(target_path)
                
        subprocess.check_call([python, manage_script, "flush", "--noinput"])
        subprocess.check_call([python, manage_script, "loaddata", "initial_user"])
        os.mkdir(os.path.join(shipyard.settings.MEDIA_ROOT, "Sandboxes"))
        #TODO: Convert the data from setup_test_pipeline to fixture files
        #TODO: Should we still import the data from load_default_objects.py?
        if fixture:
            subprocess.check_call([python, manage_script, "loaddata", fixture])
            fixture_folder = os.path.join("FixtureFiles", fixture)
            if os.path.isdir(fixture_folder):
                for child in os.listdir(fixture_folder):
                    if os.path.isdir(child):
                        source = os.path.join(fixture_folder, child)
                        destination = os.path.join(shipyard.settings.MEDIA_ROOT,
                                                   child)
                        shutil.copytree(source, destination)
