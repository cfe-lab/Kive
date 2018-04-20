import os
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from collections import defaultdict
from datetime import timedelta
from itertools import islice, chain

import seaborn as sns
import matplotlib.pyplot as plt

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


def plot_durations(slurm_jobs):
    categorized_jobs = defaultdict(list)
    for job in slurm_jobs:
        category = job.node
        categorized_jobs[category].append(job)
    min_start = min(job.start
                    for category_jobs in categorized_jobs.values()
                    for job in category_jobs)
    max_start = max(job.start
                    for category_jobs in categorized_jobs.values()
                    for job in category_jobs)
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


def main(job_limit=None):
    args = parse_args()
    sns.set(color_codes=True)
    if args.refresh or not os.path.exists(args.slurm_data):
        fetch_slurm(args)
    old_slurm_file = read_optional_file('linux0_cluster_job_table.txt')
    with open(args.slurm_data, 'r') as f:
        job_type_filter = 'driver[prelim_map.py]'
        plot_durations(islice(
            chain(read_slurm(old_slurm_file,
                             args,
                             job_type_filter=job_type_filter,
                             is_dump=True),
                  read_slurm(f, args, job_type_filter=job_type_filter)),
            job_limit))
    print('Done.')


if __name__ == '__main__':
    main()
elif __name__ == '__live_coding__':
    # noinspection PyUnresolvedReferences
    __live_coding_context__.message_limit = 100000
    main(job_limit=1000)
