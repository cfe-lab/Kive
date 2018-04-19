import re
from argparse import ArgumentTypeError
from collections import namedtuple
from csv import DictReader
from datetime import datetime, timedelta
from itertools import chain
from subprocess import check_call

SlurmJob = namedtuple('SlurmJob', 'job_type run_id start duration memory node')


def parse_date(text):
    try:
        return datetime.strptime(text, '%d %b %Y %H:%M')
    except ValueError:
        raise ArgumentTypeError("Date did not fit 'DD Mon YYYY HH:MM' format: {!r}.".format(text))


def fetch_slurm(args):
    columns = 'jobname,jobid,state,exitcode,start,elapsed,maxrss,nodelist'
    command = ['sacct',
               '-u', args.slurm_user,
               '-P',  # Parsable output
               '--delimiter', ',',
               '-o', columns,
               '-S', args.start_date.strftime('%Y-%m-%dT%H:%M'),
               '-E', args.end_date.strftime('%Y-%m-%dT%H:%M')]
    print(' '.join(command))
    with open(args.slurm_data, 'w') as f:
        check_call(command, stdout=f)


def parse_memory(text):
    if not text:
        return None
    suffix = text[-1]
    multiplier = {'M': 1, 'K': 1/1024.0, 'G': 1024}[suffix]
    return int(0.5 + multiplier*float(text[:-1]))


def read_slurm(f, args, job_type_filter=None):
    reader = DictReader(f)
    current_job_id = job_type = run_id = duration = node_list = memory = None
    start = None
    for row in chain(reader, [{'JobName': None, 'JobID': None}]):
        job_name = row['JobName']
        new_job_id = row['JobID']
        if job_name == 'batch':
            assert new_job_id == current_job_id + '.batch'
            new_job_id = current_job_id
            memory = parse_memory(row['MaxRSS'])
        if new_job_id != current_job_id and current_job_id is not None:
            if job_type_filter is None or job_type == job_type_filter:
                job = SlurmJob(job_type=job_type,
                               run_id=run_id,
                               start=start,
                               duration=duration,
                               memory=memory,
                               node=node_list)
                yield job
        if new_job_id is None:
            break
        if job_name == 'batch':
            continue
        current_job_id = new_job_id
        memory = parse_memory(row['MaxRSS'])
        node_list = row['NodeList']
        match = re.match(r'^r(?:un)?(\d+)(?:s\d+)?_?(.*?)\d*$', job_name)
        run_id = int(match.group(1))
        job_type = match.group(2)
        start = datetime.strptime(row['Start'], '%Y-%m-%dT%H:%M:%S')
        duration_parts = [int(part)
                          for part in row['Elapsed'].split(':')]
        assert len(duration_parts) == 3
        duration = timedelta(hours=duration_parts[0],
                             minutes=duration_parts[1],
                             seconds=duration_parts[2])


def read_prelim_map_jobs(f, args):
    for job in read_slurm(f, args):
        if job.job_type == 'driver[prelim_map.py]':
            yield job
