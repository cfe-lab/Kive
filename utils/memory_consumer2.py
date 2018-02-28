from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, FileType

from datetime import datetime, timedelta


def parse_args():
    parser = ArgumentParser(description='Consume memory, and keep it active.',
                            formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('-t',
                        '--time',
                        type=int,
                        help='time in seconds to run')
    parser.add_argument('--source',
                        type=FileType('rb'),
                        help='source file to read data from')
    parser.add_argument('size',
                        type=int,
                        default=1000,
                        nargs='?',
                        help='size of memory to hold, in MB')
    return parser.parse_args()


def read_chunk(source):
    read_size = 1024*1024
    chunk = ''
    while read_size:
        piece = source.read(read_size)
        if not piece:
            source.seek(0)
            piece = '!'
        chunk += piece
        read_size -= len(piece)
    return chunk


def generate_chunk(i):
    letter = chr(ord('A') + i % 26)
    chunk = (1024*1024) * letter
    return chunk


def main():
    args = parse_args()
    if args.time is None:
        end_time = None
    else:
        end_time = datetime.now() + timedelta(seconds=args.time)
    chunks = []
    allocated_count = 0
    while end_time is None or datetime.now() < end_time:
        while len(chunks) > args.size:
            chunks.pop(0)
        if args.source is None:
            chunk = generate_chunk(allocated_count)
        else:
            chunk = read_chunk(args.source)
        chunks.append(chunk)
        allocated_count += 1
    print('Allocated {}; now holding {}.'.format(allocated_count, len(chunks)))


main()
