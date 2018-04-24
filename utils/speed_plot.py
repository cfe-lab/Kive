import os
import re
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from collections import defaultdict
from csv import DictReader, DictWriter
from datetime import timedelta
from itertools import islice, chain

import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

from kiveapi import KiveAPI
from utils.speed_plot_loader import fetch_slurm, read_slurm, parse_date, read_optional_file


def parse_args():
    parser = ArgumentParser(description='Plot pipeline speeds.',
                            formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('start_date',
                        help='earliest run date to fetch (DD Mon YYYY HH:MM)',
                        type=parse_date)
    parser.add_argument('change_date',
                        help='date to compare before and after (DD Mon YYYY HH:MM)',
                        type=parse_date)
    parser.add_argument('end_date',
                        help='latest run date to fetch (DD Mon YYYY HH:MM)',
                        type=parse_date)
    parser.add_argument('--kive_server',
                        default='http://localhost:8000',
                        help='Kive server to fetch runs from')
    parser.add_argument('--kive_user',
                        default='kive',
                        help='Kive user to connect with')
    parser.add_argument('--kive_password',
                        default='kive',
                        help='Kive password to connect with')
    parser.add_argument('--slurm_user',
                        default='kivefleet',
                        help='Kive user to connect with')
    parser.add_argument('--slurm_data',
                        default='speed_data_slurm.csv',
                        help='Slurm accounting data file to write or read')
    parser.add_argument('--kive_data',
                        default='speed_data_kive.csv',
                        help='Kive runs data file to write or read')
    parser.add_argument('--refresh',
                        action='store_true',
                        help='Refresh the data files, even if they exist?')
    return parser.parse_args()


def fetch_input_sizes(args, slurm_jobs):
    data_path = 'speed_data_sizes.csv'
    try:
        with open(data_path) as f:
            reader = DictReader(f)
            return {int(row['run_id']): float(row['MB'])
                    for row in reader}
    except OSError:
        pass
    session = KiveAPI(args.kive_server)
    session.login(args.kive_user, args.kive_password)
    with open(data_path, 'w') as f:
        writer = DictWriter(f, ['run_id', 'MB'])
        writer.writeheader()
        input_sizes = {}
        for job in slurm_jobs:
            run = session.get_run(job.run_id)
            dataset = session.get_dataset(run.raw['inputs'][0]['dataset'])
            dataset_size = dataset.raw['filesize'] / 1024 / 1024  # in MB
            input_sizes[job.run_id] = dataset_size
            writer.writerow({'run_id': job.run_id, 'MB': dataset_size})
    return input_sizes


def all_jobs(categorized_jobs):
    for category_jobs in categorized_jobs.values():
        for job in category_jobs:
            yield job


def plot_durations(slurm_jobs):
    categorized_jobs = defaultdict(list)
    for job in slurm_jobs:
        category = job.node
        categorized_jobs[category].append(job)
    min_start = min(job.start for job in all_jobs(categorized_jobs))
    max_start = max(job.start for job in all_jobs(categorized_jobs))
    categories = sorted(categorized_jobs)
    interval_size = timedelta(days=7)
    # min_start += interval_size  # First week only had 1 run.
    interval_count = int((max_start - min_start)/interval_size) + 1
    fig, axes_list = plt.subplots(interval_count, 1, sharex='col')
    fig.suptitle('Driver duration for prelim_map step')
    for interval_num in range(interval_count):
        interval_start = interval_size * interval_num + min_start
        interval_end = interval_start + interval_size
        interval_jobs = {category: [job.duration.total_seconds()/3600
                                    for job in jobs
                                    if interval_start <= job.start < interval_end]
                         for category, jobs in categorized_jobs.items()}

        axes = axes_list[interval_num]
        duration_count = 0
        for category in categories:
            durations = interval_jobs[category]
            duration_count += len(durations)
            label = '{} ({})'.format(category, len(durations))
            if len(durations) > 2:
                sns.kdeplot(durations,
                            label=label,
                            ax=axes,
                            cut=0)
            else:
                axes.plot([0], label=label)
        axes.set_title('{} ({} jobs)'.format(interval_start.date(), duration_count))
        axes.legend(ncol=3)
    axes_list[-1].set_xlabel('Duration (hours)')
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.show()


def plot_size_and_memory(slurm_jobs, args):
    df = pd.DataFrame([
        {'job': job,
         'memory (MB)': job.memory,
         'job type': job.job_type,
         'node': job.node,
         'start': job.start,
         'duration (hours)': job.duration.total_seconds()/3600.0}
        for job in slurm_jobs])
    input_sizes = fetch_input_sizes(args, df['job'])
    df['size (MB)'] = [input_sizes[job.run_id] for job in df['job']]
    limits = df.agg({'start': ['min', 'max']})
    min_start = limits['start']['min']
    interval_size = timedelta(days=7)
    # min_start += interval_size  # First week only had 1 run.
    df['interval'] = [int((job.start - min_start) / interval_size)
                      for job in df['job']]
    df = df.dropna(subset=['memory (MB)'])
    df = df[df['duration (hours)'] > 31/3600.0]  # Checks memory every 30s
    # df = df[df['job type'] == 'driver[prelim_map.py]']
    df = df[~(df['job type'].isin(('bookkeeping', 'Xsetup', 'cable',
                                   'Xdriver[trim_fastqs.py]',  #
                                   'Xdriver[sam2aln.py]',
                                   'Xdriver[remap.py]',  #
                                   'Xdriver[prelim_map.py]',  #
                                   'driver[filter_quality.py]',
                                   'Xdriver[fastq_g2p.py]',  #
                                   'driver[coverage_plots.py]',
                                   'driver[cascade_report.py]',
                                   'driver[aln2counts.py]')))]
    grouped = df.groupby('job type')
    groups = grouped.groups
    column_count = 3
    row_count = (len(groups) + column_count-1) // column_count
    # noinspection PyTypeChecker
    fig, subplot_axes = plt.subplots(row_count,
                                     column_count,
                                     squeeze=False,
                                     sharex=True,
                                     sharey=True)

    for i, (group, group_jobs) in enumerate(groups.items()):
        row = i // column_count
        column = i % column_count
        ax = subplot_axes[row][column]
        group_size = len(group_jobs)
        match = re.search(r'\[(.*)\]', group)
        group_name = match.group(1) if match else group
        ax.set_title('{} ({})'.format(group_name, group_size))
        if group_size <= 2:
            continue
        x = df['size (MB)'][group_jobs]
        y = df['memory (MB)'][group_jobs]
        sns.kdeplot(x, y, ax=ax)
        if row != row_count - 1 or column != 0:
            ax.set_xlabel('')
            ax.set_ylabel('')
    plt.suptitle('Memory use by input sizes in Kive v0.11 (job count)')
    plt.show()


def main(job_limit=None):
    args = parse_args()
    sns.set(color_codes=True)
    if args.refresh or not os.path.exists(args.slurm_data):
        fetch_slurm(args)
    old_slurm_file = read_optional_file('linux0_cluster_job_table.txt')
    with open(args.slurm_data, 'r') as f:
        job_type_filter = None
        plot_size_and_memory(
            islice(chain(read_slurm(old_slurm_file,
                                    args,
                                    job_type_filter=job_type_filter,
                                    is_dump=True),
                         read_slurm(f, args, job_type_filter=job_type_filter)),
                   job_limit),
            args)
    print('Done.')


if __name__ == '__main__':
    main()
elif __name__ == '__live_coding__':
    # noinspection PyUnresolvedReferences
    __live_coding_context__.message_limit = 100000
    main(job_limit=None)
