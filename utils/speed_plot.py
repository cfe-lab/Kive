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
    df['start day'] = (
            (df['start'] - (df['interval']*interval_size + min_start)) /
            timedelta(days=1))
    # df['is_new'] = df['start'] > args.change_date
    # df = df.dropna(subset=['memory (MB)'])
    # df = df[df['duration (hours)'] > 31/3600.0]  # Checks memory every 30s
    # df = df[df['job type'] == 'remap.py_s5']
    # df = df[df['node type'] == 'compute']
    df = df[~(df['job type'].isin(('bookkeeping', 'setup', 'cable',
                                   'Xtrim_fastqs.py_s2',  #
                                   'sam2aln.py_s6',
                                   'sam2aln.py_s2',  # Mixed-HCV pipeline
                                   'Xremap.py_s5',  #
                                   'random-primer-hcv.py_s1',
                                   'prelim_map.py_s4',  #
                                   'merge_by_ref_gene.py_s4',
                                   'filter_quality.py_s1',
                                   'fastq_g2p.py_s3',  #
                                   'coverage_plots.py_s9',
                                   'Xcascade_report.py_s7',
                                   'aln2counts.py_s8',
                                   'aln2aafreq.py_s3')))]
    grouped = df.groupby(['interval'])
    groups = grouped.groups

    plot_data = []  # [(title1, start_days, title2, sizes, durations)]
    for group, group_jobs in groups.items():
        group_size = len(group_jobs)
        if group_size <= 100:
            continue
        group_rows = df.reindex(index=group_jobs)
        start_date = group*interval_size + min_start
        title1 = '{} ({})'.format(start_date.strftime('%Y-%m-%d'), group_size)
        compute_node_remaps = group_rows[
            (group_rows['job type'] == 'remap.py_s5') &
            (group_rows['node type'] == 'compute')]
        title2 = '{} ({})'.format(start_date.strftime('%Y-%m-%d'),
                                  len(compute_node_remaps))
        sizes = compute_node_remaps['size (MB)']
        durations = compute_node_remaps['duration (hours)']
        start_days = group_rows['start day']
        plot_data.append((title1, start_days, title2, sizes, durations))
    column_count = 3
    row_count = (len(plot_data) + column_count-1) // column_count
    fig1, subplot_axes1 = plt.subplots(row_count,
                                       column_count,
                                       squeeze=False,
                                       sharex='all')
    fig2, subplot_axes2 = plt.subplots(row_count,
                                       column_count,
                                       squeeze=False,
                                       sharex='all',
                                       sharey='all')
    clip = ((-50, 200), (-50, 200))
    for i, (title1, start_days, title2, sizes, durations) in enumerate(plot_data):
        row = i // column_count
        column = i % column_count
        ax1 = subplot_axes1[row][column]
        ax2 = subplot_axes2[row][column]
        ax1.set_title(title1)
        sns.kdeplot(start_days, ax=ax1, legend=False)
        ax2.set_title(title2)
        sns.kdeplot(sizes, durations, ax=ax2, clip=clip)
        if row != row_count - 1 or column != 0:
            ax2.set_xlabel('')
            ax2.set_ylabel('')
        else:
            ax1.set_xlabel('days after Wednesday')
            ax1.set_ylabel('distribution')
    fig1.suptitle('Weekly work load (job count)')
    fig1.tight_layout(rect=[0, 0, 1, 0.95])
    fig2.suptitle('Remap durations on compute nodes (job count)')
    fig2.tight_layout(rect=[0, 0, 1, 0.95])
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
