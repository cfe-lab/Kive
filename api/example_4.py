from kiveapi import KiveAPI, KiveMalformedDataException

# This assumes you have a Kive instance listening on port 8000, running
# the demo fixture.  In production, you wouldn't put your authentication
# information in source code.
KiveAPI.SERVER_URL = 'http://localhost:8000'
kive = KiveAPI()
kive.login('kive', 'kive')

# Upload data
try:
    fastq1 = kive.add_dataset('New fastq file 1', 'None', open('exfastq1.fastq', 'r'), None, None, ["Everyone"])
except KiveMalformedDataException:
    fastq1 = kive.find_datasets(name='New fastq file 1')[0]

try:
    fastq2 = kive.add_dataset('New fastq file 2', 'None', open('exfastq2.fastq', 'r'), None, None, ["Everyone"])
except KiveMalformedDataException:
    fastq2 = kive.find_datasets(name='New fastq file 2')[0]

# Get the pipeline by family ID
pipeline_family = kive.get_pipeline_family(2)

print 'Using data:'
print fastq1, fastq2

print 'With pipeline:'
print pipeline_family.published_or_latest()

# Create a RunBatch.
rb = kive.create_run_batch(
    name="TestRunBatch",
    description="Created by Python API",
    users=["kive"],
    groups=["Everyone"]
)

# Run the pipeline.
status = kive.run_pipeline(
    pipeline_family.published_or_latest(),
    [fastq1, fastq2],
    name="run1",
    runbatch=rb
)

# Run another one, using only the RunBatch ID.
status2 = kive.run_pipeline(
    pipeline_family.published_or_latest(),
    [fastq2, fastq1],
    name="run2",
    runbatch=rb.id
)