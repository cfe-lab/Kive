#!/usr/bin/env python

from argparse import ArgumentParser
from time import sleep


def parse_args():
    parser = ArgumentParser(description='Consume memory and hold it.')
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
    return parser.parse_args()


class Consumer(object):
    def __init__(self, size=1, grow=1, hold=0, chunk_size=1):
        self.size = size
        self.grow = grow
        self.hold = hold
        self.chunk_size = chunk_size
        self.data = []

    def __iter__(self):
        for i in range(self.grow):
            chunk_count = (self.size * (i+1)//self.grow) - len(self.data)
            self.data += ['*' * self.chunk_size for _ in range(chunk_count)]
            yield
        for _ in range(self.hold):
            yield


def main():
    args = parse_args()
    consumer = Consumer(args.size, args.grow, args.hold, chunk_size=1024*1024)
    for _ in consumer:
        sleep(1)

if __name__ == '__main__':
    main()
