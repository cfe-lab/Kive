from argparse import ArgumentTypeError
import errno
from collections import namedtuple, Counter
from csv import DictReader, DictWriter
from datetime import datetime, timedelta
from functools import partial
from itertools import chain
# noinspection PyProtectedMember
from multiprocessing.pool import ThreadPool
import os
import re
from subprocess import check_call

from kiveapi import KiveAPI, KiveServerException

SlurmJob = namedtuple('SlurmJob', 'job_type run_id start duration memory node')


def parse_date(text):
    try:
        return datetime.strptime(text, '%d %b %Y %H:%M')
    except ValueError:
        raise ArgumentTypeError("Date did not fit 'DD Mon YYYY HH:MM' format: {!r}.".format(text))


def fetch_slurm(args):
    if not os.path.isdir(args.cache_folder):
        os.mkdir(args.cache_folder)
    params_path = os.path.join(args.cache_folder, 'params.csv')
    try:
        with open(params_path) as params_csv:
            reader = DictReader(params_csv)
            cached_params = {row['name']: row['value'] for row in reader}
    except IOError as ex:
        if ex.errno != errno.ENOENT:
            raise
        cached_params = {}

    fetch_slurm_accounting(args, cached_params)

    with open(params_path, 'w') as params_csv:
        writer = DictWriter(params_csv, ['name', 'value'])
        writer.writeheader()
        for name, value in cached_params.items():
            writer.writerow(dict(name=name, value=value))


def fetch_slurm_accounting(args, cached_params):
    columns = 'JobName,JobID,State,ExitCode,Start,Elapsed,MaxRSS,NodeList'
    start_date = args.start_date.strftime('%Y-%m-%dT%H:%M')
    end_date = args.end_date.strftime('%Y-%m-%dT%H:%M')
    cached_start_date = cached_params.get('accounting_start_date')
    cached_end_date = cached_params.get('accounting_end_date')
    cache_has_dump = cached_params.get('has_dump')
    has_dump = args.slurm_job_dump and 'Yes'
    if (cached_start_date and cached_start_date <= start_date and
            cached_end_date and end_date <= cached_end_date and
            cache_has_dump == has_dump and
            not args.refresh):
        if args.slurm_job_dump:
            args.slurm_job_dump.close()
            args.slurm_step_dump.close()
        return
    cached_params['accounting_start_date'] = start_date
    cached_params['accounting_end_date'] = end_date
    cached_params['has_dump'] = has_dump
    command = ['sacct',
               '-u', args.slurm_user,
               '-P',  # Parsable output
               '--delimiter', ',',
               '-o', columns,
               '-S', start_date,
               '-E', end_date]
    print(' '.join(command) + ' > slurm_accounting.csv')
    accounting_path = os.path.join(args.cache_folder, 'slurm_accounting.csv')
    with open(accounting_path, 'w') as f:
        check_call(command, stdout=f)
    dump_path = os.path.join(args.cache_folder, 'slurm_dump.csv')
    if not args.slurm_job_dump:
        os.remove(dump_path)
        return
    with open(dump_path, 'w') as slurm_dump_csv, \
            args.slurm_job_dump, \
            args.slurm_step_dump:
        max_rss_by_job = Counter()
        for line in args.slurm_step_dump:
            fields = line.split('\t')
            job_id = fields[0]
            max_rss = int(fields[23])
            max_rss_by_job[job_id] = max(max_rss_by_job[job_id], max_rss)
        writer = DictWriter(slurm_dump_csv, columns.split(','))
        writer.writeheader()
        for line in args.slurm_job_dump:
            fields = line.split('\t')
            start = datetime.fromtimestamp(int(fields[37]))
            if not (args.start_date <= start <= args.end_date):
                continue
            end = datetime.fromtimestamp(int(fields[38]))
            duration = end-start
            elapsed = '0:0:{}'.format(int(duration.total_seconds()))
            job_id = fields[0]
            max_rss = '{}K'.format(max_rss_by_job[job_id])
            writer.writerow({'JobID': job_id,
                             'JobName': fields[12],
                             'MaxRSS': max_rss,
                             'NodeList': fields[28],
                             'Start': start.strftime('%Y-%m-%dT%H:%M:%S'),
                             'Elapsed': elapsed})
    args.slurm_job_dump.close()
    args.slurm_step_dump.close()


def parse_memory(text):
    if not text:
        return None
    suffix = text[-1]
    multiplier = {'M': 1, 'K': 1/1024.0, 'G': 1024}[suffix]
    return int(0.5 + multiplier*float(text[:-1]))


def iterate_file_lines(filename):
    with open(filename) as f:
        for line in f:
            yield line


def read_slurm(args):
    try:
        slurm_dump_path = os.path.join(args.cache_folder, 'slurm_dump.csv')
        job_rows = DictReader(iterate_file_lines(slurm_dump_path))
    except IOError as ex:
        if ex.errno != errno.ENOENT:
            raise
        job_rows = []
    slurm_accounting_path = os.path.join(args.cache_folder,
                                         'slurm_accounting.csv')
    job_rows = chain(job_rows,
                     DictReader(iterate_file_lines(slurm_accounting_path)),
                     [{'JobName': None, 'JobID': None}])  # Sentry value at end.
    current_job_id = job_type = run_id = duration = node_list = memory = None
    start = None
    for row in job_rows:
        job_name = row['JobName']
        new_job_id = row['JobID']
        if job_name == 'batch':
            assert new_job_id == current_job_id + '.batch'
            new_job_id = current_job_id
            memory = parse_memory(row['MaxRSS'])
        if new_job_id != current_job_id and current_job_id is not None:
            if node_list == 'None assigned':
                pass
            elif not (args.start_date <= start <= args.end_date):
                pass
            elif run_id == 0:
                pass
            else:
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
        match = re.match(r'^r(?:un)?(\d+)(s\d+)?_?(.*?)\d*$', job_name)
        if match:
            run_id = int(match.group(1))
            job_type = match.group(3)
            match2 = re.match(r'^driver\[(.*)\]$', job_type)
            if match2:
                job_type = match2.group(1)
                if match.group(2):
                    job_type += '_' + match.group(2)
        else:
            run_id = 0
            job_type = 'unknown'
        start = datetime.strptime(row['Start'], '%Y-%m-%dT%H:%M:%S')
        duration_parts = row['Elapsed'].split('-')
        duration_parts[-1:] = duration_parts[-1].split(':')
        if len(duration_parts) == 3:
            duration_parts.insert(0, '0')
        duration_parts = [int(part) for part in duration_parts]
        assert len(duration_parts) == 4
        duration = timedelta(days=duration_parts[0],
                             hours=duration_parts[1],
                             minutes=duration_parts[2],
                             seconds=duration_parts[3])


def fetch_input_size(slurm_job, cache, kive_session):
    run_id = slurm_job.run_id
    input_size = cache.get(run_id)
    is_cached = input_size is not None
    error_message = None
    if not is_cached:
        try:
            run = kive_session.get_run(run_id)
            dataset = kive_session.get_dataset(run.raw['inputs'][0]['dataset'])
            input_size = dataset.raw['filesize'] / 1024 / 1024  # in MB
        except KiveServerException as ex:
            error_message = str(ex)
    return run_id, input_size, is_cached, error_message


def fetch_input_sizes(args, slurm_jobs):
    data_path = os.path.join(args.cache_folder, 'speed_data_sizes.csv')
    try:
        with open(data_path) as f:
            reader = DictReader(f)
            cache = {int(row['run_id']): float(row['MB'])
                     for row in reader}
    except OSError as ex:
        if ex.errno != errno.ENOENT:
            raise
        cache = {}
    session = KiveAPI(args.kive_server)
    session.login(args.kive_user, args.kive_password)
    fetcher = partial(fetch_input_size, cache=cache, kive_session=session)
    pool = ThreadPool()
    job_count = len(slurm_jobs)
    fetch_count = 0
    failed_run_ids = set()
    last_error = None
    data_file = None
    data_writer = None
    input_sizes = {}
    try:
        for i, (run_id, input_size, is_cached, error_message) in enumerate(
                pool.imap_unordered(fetcher, slurm_jobs, chunksize=10)):
            if error_message is not None:
                last_error = error_message
                failed_run_ids.add(run_id)
            if not is_cached:
                if data_file is None:
                    data_file = open(data_path, 'w')
                    data_writer = DictWriter(data_file, ['run_id', 'MB'])
                    data_writer.writeheader()
                    for old_run_id, old_input_size in input_sizes.items():
                        data_writer.writerow({'run_id': old_run_id,
                                              'MB': old_input_size})
                if fetch_count % 10000 == 0:
                    print('Fetched {} runs after scanning {} of {} at {}.'.format(
                        fetch_count,
                        i,
                        job_count,
                        datetime.now()))
                fetch_count += 1
            input_sizes[run_id] = input_size
            if data_writer:
                data_writer.writerow({'run_id': run_id, 'MB': input_size})
    finally:
        if data_file is not None:
            data_file.close()

    if failed_run_ids:
        message = 'Failed to fetch run ids: {}\n  Caused by {}'.format(
            ', '.join(sorted(failed_run_ids)),
            last_error)
        raise RuntimeError(message)
    return input_sizes
