#!/usr/bin/env python3

""" Generates a 1GB dataset.

Configure this as a method that is not reusable, then use
dataset_generator_client.py to submit a bunch of runs that will generate lots
of big datasets and trigger dataset purges.
"""

import shutil
from argparse import ArgumentParser, FileType
from datetime import datetime
from subprocess import check_call


def parse_args():
    parser = ArgumentParser(
        description='Generates a large dataset to test purging.')

    parser.add_argument(
        'header_txt',
        type=FileType('rb'),
        help='Text to write at the top of the output.')
    parser.add_argument(
        'zeros',
        type=FileType('wb'),
        help='A data file that will get a bunch of zeros bytes to it.')
    return parser.parse_args()


def main():
    print('Starting at {}.'.format(datetime.now()))
    args = parse_args()
    shutil.copyfileobj(args.header_txt, args.zeros)
    args.zeros.flush()
    check_call(['head', '-c', '1000000000', '/dev/zero'], stdout=args.zeros)
    print('Finished at {}.'.format(datetime.now()))


main()
