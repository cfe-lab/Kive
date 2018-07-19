#!/usr/bin/env python

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from random import randrange
from time import sleep


def parse_args():
    parser = ArgumentParser(description='Consume memory and hold it.',
                            formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('--repeat',
                        '-r',
                        type=int,
                        default=1,
                        help='number of times to repeat the cycle')
    parser.add_argument('--delete',
                        '-d',
                        action='store_true',
                        help='delete some entries while growing')
    parser.add_argument('size',
                        type=int,
                        nargs='?',
                        default=100,
                        help='size of memory to hold in MB')
    parser.add_argument('grow',
                        type=int,
                        nargs='?',
                        default=60,
                        help='time to grow memory usage in seconds')
    parser.add_argument('hold',
                        type=int,
                        nargs='?',
                        default=0,
                        help='time to hold memory in seconds')
    parser.add_argument('release',
                        type=int,
                        nargs='?',
                        default=0,
                        help='time to wait in seconds after memory released')
    return parser.parse_args()


class Consumer(object):
    def __init__(self,
                 size=1,
                 grow=1,
                 hold=0,
                 chunk_size=1,
                 repeat=1,
                 release=0,
                 delete=False):
        self.size = size
        self.grow = grow
        self.hold = hold
        self.chunk_size = chunk_size
        self.repeat = repeat
        self.release = release
        self.delete = delete
        self.data = []

    def __iter__(self):
        for _ in range(self.repeat):
            for i in range(self.grow):
                chunk_count = (self.size * (i+1)//self.grow) - len(self.data)
                if self.delete:
                    delete_count = min(chunk_count, len(self.data))
                    for j in range(delete_count):
                        target = randrange(len(self.data))
                        self.data = self.data[:target] + self.data[target+1:]
                    chunk_count += delete_count
                self.data += ['*' * self.chunk_size for _ in range(chunk_count)]
                yield
            for i in range(self.hold):
                yield
            self.data = []
            for i in range(self.release):
                yield


def main():
    args = parse_args()
    consumer = Consumer(args.size,
                        args.grow,
                        args.hold,
                        chunk_size=1024*1024,
                        repeat=args.repeat,
                        release=args.release,
                        delete=args.delete)
    for _ in consumer:
        sleep(1)


if __name__ == '__main__':
    main()
