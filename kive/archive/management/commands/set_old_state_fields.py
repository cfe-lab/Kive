from django.core.management.base import BaseCommand

from archive.models import Run, RunStep, RunOutputCable, RunSIC


class Command(BaseCommand):
    help = "Sets the _complete and _successful fields for all Runs and RunComponents " \
           "(for migrations beyond v0.7)"

    def handle(self, *args, **options):

        # Ideally, this handles everything through propagating downward to
        # its components.
        for run in Run.objects.filter(_complete__isnull=True, parent_runstep__isnull=True):
            run.is_complete(use_cache=True)
        for run in Run.objects.filter(_successful__isnull=True, parent_runstep__isnull=True):
            run.is_successful(use_cache=True)

        # Now check any runs that we missed in case their parent run somehow had
        # their _complete and _successful flags set.
        for run in Run.objects.filter(_complete__isnull=True, parent_runstep__isnull=False):
            run.is_complete(use_cache=True)
        for run in Run.objects.filter(_successful__isnull=True, parent_runstep__isnull=False):
            run.is_successful(use_cache=True)

        # Any steps missed by the above are handled here.
        for rs in RunStep.objects.filter(_complete__isnull=True):
            rs.is_complete(use_cache=True)
        for rs in RunStep.objects.filter(_successful__isnull=True):
            rs.is_successful(use_cache=True)

        # Any output cables missed by the above are handled here.
        for roc in RunOutputCable.objects.filter(_complete__isnull=True):
            roc.is_complete(use_cache=True)
        for roc in RunOutputCable.objects.filter(_successful__isnull=True):
            roc.is_successful(use_cache=True)

        # Finally, handle any input cables missed by the above.
        for rsic in RunSIC.objects.filter(_complete__isnull=True):
            rsic.is_complete(use_cache=True)
        for rsic in RunSIC.objects.filter(_successful__isnull=True):
            rsic.is_successful(use_cache=True)