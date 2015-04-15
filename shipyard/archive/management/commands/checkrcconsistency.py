from django.core.management.base import BaseCommand
from archive.models import RunStep, RunCable
from optparse import make_option
from itertools import chain


class Command(BaseCommand):
    help = " Checks for and optionally fixes inconsistencies between the normalized fields " \
           "_successful and _complete on RunSteps and RunCables. "

    option_list = BaseCommand.option_list + (
        make_option('--fix-inconsistencies', '-f', type='choice', choices=['0', '1'], default='1'), )

    def handle(self, *args, **options):
        fix_inconsistencies = options['fix_inconsistencies']

        # We only use the _completed/_successful flags on
        # runsteps and runcables.
        # TODO: Filter out redacted steps once that field has been added
        for rc in chain(RunStep.objects.all(), RunCable.objects.all()):
            inconsistent = False

            is_compl, is_mcompl = rc.is_complete(), rc.is_marked_complete()
            is_succs, is_msuccs = rc.is_successful(), rc.is_marked_successful()

            if is_compl != is_mcompl:
                self.stdout.write('%s %s (id: %d) is_complete has mark(%s) but was computed to be (%s) ' % (
                    type(rc).__name__, rc, rc.id, is_compl, is_mcompl))
                inconsistent = True

            if is_succs != is_msuccs:
                self.stdout.write('%s %s (id: %d) is_successful has mark(%s) but was computed to be (%s) ' % (
                    type(rc).__name__, rc, rc.id, is_succs, is_msuccs))
                inconsistent = True

            # Saving will recompute these flags if inconsistent
            if fix_inconsistencies and inconsistent:
                rc.save()

        self.stdout.write('Completed check!')
