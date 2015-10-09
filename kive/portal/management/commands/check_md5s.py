from django.core.management.base import BaseCommand
from method.models import CodeResourceRevision


class Command(BaseCommand):
    help = 'Checks MD5 checksums for all code resources.'

    def handle(self, *args, **options):
        failures = 0
        for r in CodeResourceRevision.objects.all():
            if not r.check_md5():
                failures += 1

        print('{} failures, see log for details.'.format(failures))
