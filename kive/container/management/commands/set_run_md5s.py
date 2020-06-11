from django.core.management.base import BaseCommand

from container.models import ContainerRun


class Command(BaseCommand):
    help = "Set MD5 on any runs that still have a blank."

    def handle(self, **kwargs):
        batch_size = 100
        run_count = 0
        print('Starting.')
        while True:
            runs = ContainerRun.objects.filter(md5='')[:batch_size]
            batch_count = len(runs)
            if not batch_count:
                break
            for run in runs:
                run.set_md5()
                run.save()
            run_count += batch_count
            print('Set MD5 on {} runs.'.format(run_count))
        print('Done.')
