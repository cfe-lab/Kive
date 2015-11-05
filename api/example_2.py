import os
import sched
import time

from kiveapi import KiveAPI, KiveMalformedDataException

# This is how I would recommend authenticating to Kive
KiveAPI.SERVER_URL = 'http://127.0.0.1:8000/'
kive = KiveAPI('kive', 'kive')

# Upload data
try:
    fastq1 = kive.add_dataset('New fastq file 1', 'None', open('exfastq1.fastq', 'r'), None, None, ["Everyone"])
except KiveMalformedDataException:
    fastq1 = kive.find_datasets(dataset_name='New fastq file 1')[0]

try:
    fastq2 = kive.add_dataset('New fastq file 2', 'None', open('exfastq2.fastq', 'r'), None, None, ["Everyone"])
except KiveMalformedDataException:
    fastq2 = kive.find_datasets(dataset_name='New fastq file 2')[0]

# Get the pipeline by family ID
pipeline_family = kive.get_pipeline_family(2)

print 'Using data:'
print fastq1, fastq2

print 'With pipeline:'
print pipeline_family.published_or_latest()

# Run the pipeline
status = kive.run_pipeline(
    pipeline_family.published_or_latest(),
    [fastq1, fastq2]
)

# Start polling Kive
s = sched.scheduler(time.time, time.sleep)


def check_run(sc, run):
    print run.get_status()

    if run.is_running() or run.is_complete():
        print run.get_progress(), run.get_progress_percent(), '%'

    if not run.is_complete():
        sc.enter(5, 1, check_run, (sc, run,))

s.enter(5, 1, check_run, (s, status,))
s.run()

print 'Finished Run, nabbing files'
for dataset in status.get_results():
    with open(os.path.join('results', dataset.filename), 'wb') as file_handle:
        dataset.download(file_handle)
