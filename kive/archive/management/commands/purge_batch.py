from django.core.management.base import BaseCommand

from archive.models import RunBatch


class Command(BaseCommand):
    help = "Purges output datasets from a run batch."

    def add_arguments(self, parser):
        parser.add_argument(
            "batch_id",
            type=int)
        parser.add_argument(
            "--outputs",
            default="",
            help='comma-separated list of output names to purge')

    def handle(self, *args, **options):
        batch = RunBatch.objects.get(id=options['batch_id'])
        targets = filter(None, options['outputs'].split(','))
        for run in batch.runs.all():
            print(run)
            for step in run.runsteps_in_order:
                print("  {}".format(step))
                execrecord = step.execrecord
                for output in execrecord.execrecordouts_in_order:
                    output_name = output.generic_output.definite.dataset_name
                    dataset = output.dataset
                    if not targets or output_name in targets:
                        dataset.dataset_file.delete()
                        print("    {}".format(output_name))
