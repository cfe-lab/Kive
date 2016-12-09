from django.core.management.base import BaseCommand
from sandbox.execute import Sandbox
from fleet.exceptions import StopExecution
import json
import sys
import logging

worker_logger = logging.getLogger("fleet.Worker")


class Command(BaseCommand):
    help = 'Performs setup or bookkeeping for step execution.'

    def add_arguments(self, parser):
        parser.add_argument(
            "step_execution_info_json",
            help="JSON file containing step execution information"
        )

        parser.add_argument(
            "--bookkeeping",
            action="store_true",
            help="Whether to perform bookkeeping "
                 "(default is False, meaning this is setup rather than bookkeeping)"
        )

    def handle(self, *args, **options):
        with open(options["step_execution_info_json"], "rb") as f:
            step_execute_dict = json.loads(f.read())

        if not options["bookkeeping"]:
            try:
                curr_RS = Sandbox.step_execution_setup(step_execute_dict)
            except StopExecution:
                worker_logger.exception("Execution was stopped during setup.")
                sys.exit(103)

            if curr_RS.is_failed():
                sys.exit(101)
            elif curr_RS.is_cancelled():
                sys.exit(102)
        else:
            Sandbox.step_execution_bookkeeping(step_execute_dict)