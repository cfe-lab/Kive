#! /usr/bin/env python

import os
import json
from argparse import ArgumentParser, FileType


def parse_args():
    parser = ArgumentParser(
        description="Counts the number of lines in the input file, reports the contents of /mnt/input and /mnt/output."
    )
    parser.add_argument(
        'input_text',
        type=FileType("rt"),
        help='A text file, anything goes'
    )
    parser.add_argument('summary_json', type=FileType('w'))

    return parser.parse_args()


def main():
    args = parse_args()
    summary = {
        "lines": sum(1 for _ in args.input_text),
        "mnt_input_contents": os.listdir("/mnt/input"),
        "mnt_output_contents": os.listdir("/mnt/output")
    }
    args.summary_json.write(json.dumps(summary))


if __name__ == "__main__":
    main()
