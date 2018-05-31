from django.core.management.base import BaseCommand

from sandbox.execute import Sandbox
import file_access_utils

import json
import logging
import time

worker_logger = logging.getLogger("fleet.Worker")


class Command(BaseCommand):
    help = 'Executes a cable based on the specified details.'

    def add_arguments(self, parser):
        parser.add_argument(
            "cable_execution_info_json",
            help="JSON file containing cable execution information"
        )

    def handle(self, *args, **options):
        worker_logger.debug("start time: %f" % time.time())
        file_access_utils.confirm_file_created(options["cable_execution_info_json"])
        with open(options["cable_execution_info_json"], "r") as f:
            cable_execute_dict = json.loads(f.read())

        Sandbox.finish_cable(cable_execute_dict)
        worker_logger.debug("stop time: %f" % time.time())
