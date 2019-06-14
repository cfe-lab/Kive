
from librarian.models import Dataset
from datachecking.models import VerificationLog


def clean_up_all_files():
    """
    Delete all files that have been put into the database as FileFields.
    """

    # Also clear all datasets.  This was previously in librarian.tests
    # but we move it here.
    for dataset in Dataset.objects.all():
        dataset.dataset_file.close()
        dataset.dataset_file.delete()
        dataset.delete()

    for vl in VerificationLog.objects.all():
        vl.output_log.close()
        vl.output_log.delete()
        vl.error_log.close()
        vl.error_log.delete()
        vl.delete()
