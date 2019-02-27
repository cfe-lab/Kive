#! /usr/bin/env python

from argparse import FileType, ArgumentParser
import csv
import os

# In order to work with kive, scripts that have a inputs
# and b outputs must have a+b command line arguments, the first a
# arguments specifying paths of input files, the subsequent b
# arguments specifying the paths where outputs are written.

# ArgumentParser facilitates parsing inputs from sys.argv, and
# generates help messages based on the expected input specification
parser = ArgumentParser(
    description='Takes CSV with one column ("raw_salutation"), '
                'outputs two CSVs with one column ("regularized_salutation")')
parser.add_argument(
    "input_csv",
    type=FileType('rU'),
    help="CSV containing (hi|hello|hola|bye|goodbye|adios|[other]) rows")
parser.add_argument(
    "output_csv",
    type=FileType('wb'),
    help="CSV containing (hello|goodbye|huh) rows"
)
parser.add_argument(
    "opposite_output_csv",
    type=FileType('wb'),
    help="CSV containing (hello|goodbye|huh) rows that are opposite to the main output"
)
args = parser.parse_args()

reader = csv.DictReader(args.input_csv)
writer = csv.DictWriter(args.output_csv,
                        ['regularized_salutation'],
                        lineterminator=os.linesep)
writer.writeheader()
opposite_writer = csv.DictWriter(args.opposite_output_csv,
                                 ["regularized_salutation"],
                                 lineterminator=os.linesep)
opposite_writer.writeheader()

hellos = {"hi", "hello", "hola"}
goodbyes = {"bye", "goodbye", "adios"}
for row in reader:
    regularized = "huh"
    opposite = "huh"
    if row["raw_salutation"].lower() in hellos:
        regularized = "hello"
        opposite = "goodbye"
    elif row["raw_salutation"].lower() in goodbyes:
        regularized = "goodbye"
        opposite = "hello"

    writer.writerow(dict(regularized_salutation=regularized))
    opposite_writer.writerow(dict(regularized_salutation=opposite))
