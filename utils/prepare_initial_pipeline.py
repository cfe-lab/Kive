#! /usr/bin/env python

# Prepare a test Pipeline and a dataset to run it with for the purpose of generating a fixture.
# Make sure DJANGO_SETTINGS_MODULE is set appropriately in the shell.
# Dump data using
# ./manage.py dumpdata --indent=4 librarian method pipeline transformation archive > [filename]

from django.core.files import File
from django.contrib.auth.models import User

import metadata.models
from librarian.models import Dataset
import method.models
import kive.testing_utils as tools

# This comes from the initial_user fixture.
kive_user = User.objects.get(pk=1)

test_fasta = Dataset.create_dataset("../samplecode/step_0_raw.fasta", file_path=None, user=kive_user, cdt=None,
                                       keep_file=True, name="TestFASTA",
                                       description="Toy FASTA file for testing pipelines")

# Set up a test Pipeline.
resource = method.models.CodeResource(name="Fasta2CSV", description="FASTA converter script", filename="Fasta2CSV.py")
resource.clean()
resource.save()
with open("../samplecode/fasta2csv.py", "rb") as f:
    revision = method.models.CodeResourceRevision(
        coderesource=resource,
        revision_name="v1",
        revision_desc="First version",
        content_file=File(f))
    revision.clean()
    revision.save()
resource.clean()

# The CDT to use is defined in the initial_data fixture.
fasta_CSV_CDT = metadata.models.CompoundDatatype.objects.get(pk=4)
fasta_to_CSV = tools.make_first_method("Fasta2CSV", "Converts FASTA to CSV", revision)
fasta_to_CSV.create_input(compounddatatype=None, dataset_name="FASTA", dataset_idx=1)
fasta_to_CSV.create_output(compounddatatype=fasta_CSV_CDT, dataset_name="CSV", dataset_idx=1)

test_pipeline = tools.make_first_pipeline("Fasta2CSV", "One-step pipeline wrapper for Fasta2CSV Method")
tools.create_linear_pipeline(test_pipeline, [fasta_to_CSV], "pipeline_input", "pipeline_output")

# Set the positions so this looks OK in the interface.  These numbers were taken from actually
# laying this out in the interface previously.
pi = test_pipeline.inputs.first()
pi.x = 0.07
pi.y = 0.194
pi.save()

po = test_pipeline.outputs.first()
po.x = 0.85
po.y = 0.194
po.save()

step1 = test_pipeline.steps.get(step_num=1)
step1.x = 0.15
step1.y = 0.21
step1.save()
