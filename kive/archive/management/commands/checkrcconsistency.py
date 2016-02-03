from django.core.management.base import BaseCommand
from archive.models import RunStep, RunCable
from optparse import make_option
from itertools import chain


class Command(BaseCommand):
    help = " Checks for and optionally fixes inconsistencies between the normalized fields " \
           "_successful and _complete on RunSteps and RunCables. "

    def add_arguments(self, parser):
        parser.add_argument(
            "-f",
            "--fix-inconsistencies",
            dest="fix_inconsistencies",
            choices=['0', '1'],
            default='1'
        )

    def handle(self, *args, **options):
        fix_inconsistencies = options['fix_inconsistencies']

        # We only use the _completed/_successful flags on runsteps and runcables.
        # TODO: Filter out redacted steps once that field has been added
        for rc in chain(RunStep.objects.all(), RunCable.objects.all()):
            is_mcompl, is_compl = (rc.is_complete(use_cache=True, dont_save=True),
                                   rc.is_complete(dont_save=not fix_inconsistencies))
            is_msuccs, is_succs = (rc.is_successful(use_cache=True, dont_save=True),
                                   rc.is_successful(dont_save=not fix_inconsistencies))

            if is_compl != is_mcompl:
                self.stdout.write('%s %s (id: %d) is_complete has mark(%s) but was computed to be (%s) ' % (
                    type(rc).__name__, rc, rc.id, is_compl, is_mcompl))

            if is_succs != is_msuccs:
                self.stdout.write('%s %s (id: %d) is_successful has mark(%s) but was computed to be (%s) ' % (
                    type(rc).__name__, rc, rc.id, is_succs, is_msuccs))

        self.stdout.write('Completed check!')
