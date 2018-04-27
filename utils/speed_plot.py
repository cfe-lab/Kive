import os
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, FileType, SUPPRESS
from collections import defaultdict
from datetime import timedelta
from itertools import islice

import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

from utils.speed_plot_loader import fetch_slurm, read_slurm, parse_date, \
    fetch_input_sizes


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
                        default=SUPPRESS,
                        help='Kive password to connect with (default from '
                             'KIVE_PASSWORD environment variable)')
    parser.add_argument('--slurm_user',
                        default='kivefleet',
                        help='Kive user to connect with')
    parser.add_argument('--slurm_job_dump',
                        type=FileType(),
                        help='Slurm accounting job dump: typically '
                             'linux0_cluster_job_table.txt. '
                             'Requires --slurm_step_dump.')
    parser.add_argument('--slurm_step_dump',
                        type=FileType(),
                        help='Slurm accounting step dump: typically '
                             'linux0_cluster_step_table.txt. '
                             'Requires --slurm_job_dump.')
    parser.add_argument('--cache_folder',
                        default='speed_plot_cache',
                        help='Folder to cache fetched data')
    parser.add_argument('--refresh',
                        action='store_true',
                        help='Refresh the cache files, even if they exist?')

    args = parser.parse_args()
    if args.slurm_job_dump and not args.slurm_step_dump:
        parser.error('Option --slurm_step_dump is required with --slurm_job_dump.')
    if args.slurm_step_dump and not args.slurm_job_dump:
        parser.error('Option --slurm_job_dump is required with --slurm_step_dump.')
    if not hasattr(args, 'kive_password'):
        args.kive_password = os.environ.get('KIVE_PASSWORD', 'kive')

    return args


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
    df['node type'] = ['head' if job.node == 'octomore' else 'compute'
                       for job in df['job']]
    limits = df.agg({'start': ['min', 'max']})
    min_start = limits['start']['min']
    interval_size = timedelta(days=7)
    # min_start += interval_size  # First week only had 1 run.
    df['interval'] = [int((job.start - min_start) / interval_size)
                      for job in df['job']]
    df['is_new'] = df['start'] > args.change_date
    df = df.dropna(subset=['memory (MB)'])
    # df = df[df['duration (hours)'] > 31/3600.0]  # Checks memory every 30s
    # df = df[df['job type'] == 'driver[prelim_map.py]']
    # df = df[df['node type'] == 'compute']
    df = df[~(df['job type'].isin(('bookkeeping', 'setup', 'cable',
                                   'trim_fastqs.py_s2',  #
                                   'Xsam2aln.py_s6',
                                   'sam2aln.py_s2',  # Mixed-HCV pipeline
                                   'Xremap.py_s5',  #
                                   'random-primer-hcv.py_s1',
                                   'Xprelim_map.py_s4',  #
                                   'merge_by_ref_gene.py_s4',
                                   'filter_quality.py_s1',
                                   'fastq_g2p.py_s3',  #
                                   'coverage_plots.py_s9',
                                   'cascade_report.py_s7',
                                   'aln2counts.py_s8',
                                   'aln2aafreq.py_s3')))]
    grouped = df.groupby(['job type', 'is_new'])
    groups = grouped.groups
    column_count = 2
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
        job_type, is_new = group
        ax.set_title('{} {} ({})'.format('new' if is_new else 'old',
                                         job_type,
                                         group_size))
        if group_size <= 2:
            continue
        x = df['size (MB)'][group_jobs]
        y = df['duration (hours)'][group_jobs]
        clip = ((-50, 200), (-50, 200))
        sns.kdeplot(x, y, ax=ax, clip=clip)
        if row != row_count - 1 or column != 0:
            ax.set_xlabel('')
            ax.set_ylabel('')
    plt.suptitle('Processing time in Kive v0.10 and v0.11 (job count)')
    plt.show()


def main(job_limit=None):
    args = parse_args()
    sns.set(color_codes=True)
    fetch_slurm(args)
    plot_size_and_memory(islice(read_slurm(args), job_limit), args)
    print('Done.')


if __name__ == '__main__':
    main()
elif __name__ == '__live_coding__':
    # noinspection PyUnresolvedReferences
    __live_coding_context__.message_limit = 100000
    main(job_limit=None)
