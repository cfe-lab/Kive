from argparse import ArgumentParser, FileType
from csv import DictReader

from datetime import datetime
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
        columns = sorted(reader.fieldnames,
                         key=lambda name: ((1 if name.startswith('head')
                                            else 2),
                                           name))
        free_columns = [name for name in columns
                        if name.endswith('_free')]
        free_labels = [name[:-5] for name in free_columns]
        free = []
        times = []
        for i, row in enumerate(reader):
            times.append(parse_date(row['time']))
            free.append([float(row[name]) for name in free_columns])
    fig, ax = plt.subplots()
    ax.set_title('Memory')
    ax.set_ylabel('Free GB')
    ax.plot(times, free)
    ax.legend(free_labels,
              loc='best',
              ncol=3)
    xlim = list(ax.get_xlim())
    if args.start is not None:
        xlim[0] = parse_date(args.start)
    if args.end is not None:
        xlim[1] = parse_date(args.end)
    ax.set_xlim(xlim)
    ylim = list(ax.get_ylim())
    ylim[0] = 0
    ylim[1] *= 1.25
    ax.set_ylim(ylim)
    fig.autofmt_xdate()
    plt.savefig(args.plot)

if __name__ == '__main__':
    main()
