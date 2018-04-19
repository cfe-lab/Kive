import os
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from collections import defaultdict
from itertools import islice

import seaborn as sns
import matplotlib.pyplot as plt

from utils.speed_plot_loader import fetch_slurm, read_slurm, parse_date


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
    job_durations = defaultdict(list)
    for job in slurm_jobs:
        category = job.node
        # category = '{}/{}'.format(job.node, job.job_type)
        job_durations[category].append(job.duration.total_seconds()/3600)

    categories = sorted(job_durations)
    for category in categories:
        durations = job_durations[category]
        if len(durations) > 2:
            sns.kdeplot(durations,
                        label='{} ({})'.format(category, len(durations)),
                        cut=0)

    axes = plt.gca()
    skip_count = 0
    mid_height = axes.get_ylim()[1]/2
    line_count = len(axes.get_lines())
    for i, category in enumerate(categories):
        durations = job_durations[category]
        if len(durations) <= 2:
            skip_count += 1
        else:
            line_index = i - skip_count
            dot_heights = [(line_count - line_index)*mid_height/line_count] * len(durations)
            kde_line = axes.get_lines()[line_index]
            plt.plot(durations,
                     dot_heights,
                     'o',
                     alpha=0.2,
                     markerfacecolor=kde_line.get_color())
    axes.set_title('Durations of prelim_map driver')
    axes.set_xlabel('Duration (hours)')
    plt.legend()
    plt.show()


def main(job_limit=None):
    args = parse_args()
    sns.set(color_codes=True)
    if args.refresh or not os.path.exists(args.slurm_data):
        fetch_slurm(args)
    with open(args.slurm_data, 'r') as f:
        plot_durations(islice(
            read_slurm(f, args, job_type_filter='driver[prelim_map.py]'),
            job_limit))
    print('Done.')


if __name__ == '__main__':
    main()
elif __name__ == '__live_coding__':
    # noinspection PyUnresolvedReferences
    __live_coding_context__.message_limit = 100000
    main(job_limit=1000)
