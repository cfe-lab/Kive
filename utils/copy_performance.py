from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, FileType
from csv import DictWriter
from datetime import datetime
import os
import shutil
from socket import gethostname
import sys


def parse_args():
    parser = ArgumentParser(
        description='Test copy speed with different chunk sizes',
        formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('data',
                        help='data file to copy')
    parser.add_argument('--times',
                        type=FileType('a'),
                        help='CSV file to append copy times to')
    parser.add_argument('--plot',
                        nargs='*',
                        help='CSV files to plot times from')
    args = parser.parse_args()
    if args.times is None:
        args.times = sys.stdout
    return args


def plot_results(args):
    from matplotlib import pyplot as plt
    import pandas as pd

    plot_count = len(args.plot)
    # noinspection PyTypeChecker
    f, axes_list = plt.subplots(plot_count, 1, sharex=True, sharey=True)
    for i, data_file in enumerate(args.plot):
        ax = axes_list[i]
        df = pd.read_csv(data_file)
        df.set_index('chunk', inplace=True)
        groups = df.groupby(['host'])
        groups['time'].plot(legend=True, logx=True, title=data_file, ax=ax)
        ax.set_ylabel('Copy time (s)')
    plt.tight_layout()
    plt.show()


def main():
    args = parse_args()
    if args.plot:
        plot_results(args)
        return
    has_header = args.times is not sys.stdout and args.times.tell() != 0
    data_size = os.lstat(args.data).st_size
    target_name = args.data + '.copy'
    host_name = gethostname()
    writer = DictWriter(args.times,
                        ['host', 'chunk', 'time'],
                        lineterminator=os.linesep)
    if not has_header:
        writer.writeheader()
    chunk_size = 16*1024  # Start with default.
    while chunk_size < data_size:
        # Do the copy an extra time to avoid cache effects?
        with open(args.data, 'rb') as src, open(target_name, 'wb') as dst:
            shutil.copyfileobj(src, dst, length=chunk_size)

        # Now do the copy for real.
        start_time = datetime.now()
        with open(args.data, 'rb') as src, open(target_name, 'wb') as dst:
            shutil.copyfileobj(src, dst, length=chunk_size)
        duration = datetime.now() - start_time
        writer.writerow(dict(host=host_name,
                             chunk=chunk_size,
                             time=duration.total_seconds()))
        chunk_size *= 2


if __name__ == '__main__':
    main()
elif __name__ == '__live_coding__':
    main()
