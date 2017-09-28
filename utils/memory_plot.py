from argparse import ArgumentParser, FileType
from collections import Counter
from csv import DictReader
from datetime import datetime
from itertools import groupby
import math

from matplotlib import pyplot as plt


def parse_args():
    parser = ArgumentParser(description='Plot memory usage over time.')
    parser.add_argument('--file',
                        '-f',
                        default='memory_watch.log',
                        type=FileType(),
                        help='CSV file to load data from')
    parser.add_argument('--plot',
                        '-p',
                        default='memory_plot.png',
                        help='file name for plot')
    parser.add_argument('--start',
                        '-s',
                        help='start date in YYYY-MM-DD HH:MM format')
    parser.add_argument('--end',
                        '-e',
                        help='end date in YYYY-MM-DD HH:MM format')
    parser.add_argument('--nodes',
                        '-n',
                        help='comma-separated list of node names to plot')
    parser.add_argument('--mem',
                        '-m',
                        type=float,
                        help='Maximum memory in GB to plot')
    return parser.parse_args()


def parse_date(text):
    formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d']
    for i, date_format in enumerate(formats):
        try:
            return datetime.strptime(text, date_format)
        except ValueError:
            if i == len(formats) - 1:
                # Last format, so give up
                raise


def main():
    args = parse_args()
    with args.file:
        reader = DictReader(args.file)
        all_free_columns = [name for name in reader.fieldnames
                            if name.endswith('_free')]
        if args.nodes is None:
            free_columns = all_free_columns
        else:
            node_names = args.nodes.split(',')
            free_columns = [name for name in all_free_columns
                            if name.split('_')[0] in node_names]
        free_labels = sorted({name[:-7] for name in free_columns},
                             key=lambda s: ((1 if s.startswith('head')
                                             else 2),
                                            s))
        free = []
        times = []
        for i, row in enumerate(reader):
            row_totals = Counter()
            for column in free_columns:
                if row[column]:
                    row_totals[column[:-7]] += float(row[column])
            times.append(parse_date(row['time']))
            free.append([row_totals[name] if name in row_totals else None
                         for name in free_labels])
    fig, ax = plt.subplots()
    ax.set_title('Memory')
    ax.set_ylabel('Free GB')
    lines = ax.plot(times, free)
    cmap = plt.get_cmap('jet')
    node_count = len(list(groupby(free_labels,
                                  lambda label: label.split('_')[0])))
    node_num = -1
    prev_node = None
    line_num = 0
    for i, line in enumerate(lines):
        node_name = free_labels[i].split('_')[0]
        if node_name != prev_node:
            node_num += 1
            prev_node = node_name
            line_num = 0
        line.set_color(cmap(float(node_num) / node_count))
        line.set_linewidth(line_num + 1)
        line_num += 1
    ax.legend(free_labels,
              loc='best',
              ncol=4)
    xlim = list(ax.get_xlim())
    if args.start is not None:
        xlim[0] = parse_date(args.start)
    if args.end is not None:
        xlim[1] = parse_date(args.end)
    ax.set_xlim(xlim)
    ylim = list(ax.get_ylim())
    ylim[0] = 0
    if args.mem is None:
        ylim[1] *= 1 + 0.1 * (math.ceil(len(free_labels)*0.25))
    else:
        ylim[1] = args.mem
    ax.set_ylim(ylim)
    fig.autofmt_xdate()
    plt.savefig(args.plot)


main()
