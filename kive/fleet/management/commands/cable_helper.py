from django.core.management.base import BaseCommand
from sandbox.execute import Sandbox
import json
import file_access_utils


class Command(BaseCommand):
    help = 'Executes a cable based on the specified details.'

    def add_arguments(self, parser):
        parser.add_argument(
            "cable_execution_info_json",
            help="JSON file containing cable execution information"
        )

    def handle(self, *args, **options):
        file_access_utils.confirm_file_created(options["cable_execution_info_json"])
        with open(options["cable_execution_info_json"], "rb") as f:
            cable_execute_dict = json.loads(f.read())

        Sandbox.finish_cable(cable_execute_dict)