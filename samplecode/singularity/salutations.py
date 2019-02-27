#! /usr/bin/env python

import os
from argparse import ArgumentParser, FileType
from csv import DictReader, DictWriter


def parse_args():
    parser = ArgumentParser(
        description="Takes a list of names and a list of salutations and puts them together"
    )
    parser.add_argument(
        'names_csv',
        type=FileType(),
        help='CSV with one column called "name"'
    )
    parser.add_argument(
        'regularized_salutations_csv',
        type=FileType(),
        help='CSV with one column called "regularized_salutation"'
    )
    parser.add_argument('salutations_csv', type=FileType('w'))

    return parser.parse_args()


def main():
    args = parse_args()
    names_reader = DictReader(args.names_csv)
    regularized_salutation_reader = DictReader(args.regularized_salutations_csv)

    writer = DictWriter(args.salutations_csv,
                        ['salutation'],
                        lineterminator=os.linesep)
    writer.writeheader()
    names = [row["name"] for row in names_reader]
    salutations = [row["regularized_salutation"] for row in regularized_salutation_reader]

    for i in range(min(len(names), len(salutations))):
        writer.writerow(dict(salutation="{}, {}".format(salutations[i], names[i])))


if __name__ == "__main__":
    main()
