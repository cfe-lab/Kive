from django.core.management.base import BaseCommand
from sandbox.execute import Sandbox
import json


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
            Sandbox.step_execution_setup(step_execute_dict)
        else:
            Sandbox.step_execution_bookkeeping(step_execute_dict)