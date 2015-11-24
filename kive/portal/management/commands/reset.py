from optparse import make_option
import os
import shutil

from django.core.management.base import BaseCommand
from django.core.management import call_command
from django.conf import settings

import file_access_utils
import method.models
import archive.models
import datachecking.models
import portal.models


class Command(BaseCommand):
    help = 'Resets the database and loads sample data.'
    
    option_list = BaseCommand.option_list + (
        make_option('--load', '-l', help="fixture name to load"), )
    
    def handle(self, *args, **options):
        fixture = options['load']

        targets = [
            method.models.CodeResourceRevision.UPLOAD_DIR,
            archive.models.Dataset.UPLOAD_DIR,
            archive.models.MethodOutput.UPLOAD_DIR,
            datachecking.models.VerificationLog.UPLOAD_DIR,
            portal.models.StagedFile.UPLOAD_DIR,
            settings.SANDBOX_PATH
        ]
        for target in targets:
            target_path = os.path.join(settings.MEDIA_ROOT, target)
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
        sandboxes_path = os.path.join(settings.MEDIA_ROOT, settings.SANDBOX_PATH)
        os.mkdir(sandboxes_path)
        file_access_utils.configure_sandbox_permissions(sandboxes_path)

        if fixture:
            call_command("loaddata", fixture)
            fixture_folder = os.path.join("FixtureFiles", fixture)
            if os.path.isdir(fixture_folder):
                for child in os.listdir(fixture_folder):
                    source = os.path.join(fixture_folder, child)
                    if os.path.isdir(source):
                        destination = os.path.join(settings.MEDIA_ROOT, child)
                        if not os.path.exists(destination):
                            os.mkdir(destination)
                        for grandchild in os.listdir(source):
                            source_child = os.path.join(source, grandchild)
                            destination_child = os.path.join(destination,
                                                             grandchild)
                            if os.path.isdir(source_child):
                                shutil.copytree(source_child, destination_child)
                            else:
                                shutil.copy(source_child, destination_child)
