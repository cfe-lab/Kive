from optparse import make_option
import os
import shutil

from django.core.management.base import BaseCommand
from django.core.management import call_command

import kive.settings  # @UnresolvedImport
import file_access_utils


class Command(BaseCommand):
    help = 'Resets the database and loads sample data.'
    
    option_list = BaseCommand.option_list + (
        make_option('--load', '-l', help="fixture name to load"), )
    
    def handle(self, *args, **options):
        fixture = options['load']
        
        targets = ["CodeResources",
                   "Datasets",
                   "Logs",
                   "Sandboxes",
                   "VerificationLogs",
                   "VerificationScripts",
                   "StagedFiles"]
        for target in targets:
            target_path = os.path.join(kive.settings.MEDIA_ROOT, target)
            if os.path.isdir(target_path):
                shutil.rmtree(target_path)
        
        call_command("flush", interactive=False)
        call_command("migrate")
        # flush truncates all tables, so we need to re-load this stuff.
        call_command("loaddata", "initial_groups")
        call_command("loaddata", "initial_user")
        call_command("loaddata", "initial_data")

        # Create the Sandboxes directory specially because it has to have
        # special permissions added to it.
        sandboxes_path = os.path.join(kive.settings.MEDIA_ROOT, "Sandboxes")
        os.mkdir(sandboxes_path)
        file_access_utils.configure_sandbox_permissions(sandboxes_path)

        if fixture:
            call_command("loaddata", fixture)
            fixture_folder = os.path.join("FixtureFiles", fixture)
            if os.path.isdir(fixture_folder):
                for child in os.listdir(fixture_folder):
                    source = os.path.join(fixture_folder, child)
                    if os.path.isdir(source):
                        destination = os.path.join(kive.settings.MEDIA_ROOT,
                                                   child)
                        shutil.copytree(source, destination)
