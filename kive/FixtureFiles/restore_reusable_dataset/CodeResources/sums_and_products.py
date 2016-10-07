#! /usr/bin/env python

from argparse import FileType, ArgumentParser
import csv
import os
from random import shuffle

parser = ArgumentParser(
    description="Takes CSV with (x,y), outputs CSV with (x+y),(x*y)");
parser.add_argument("input_csv",
                    type=FileType('rU'),
                    help="CSV containing (x,y) pairs");
parser.add_argument("output_csv",
                    type=FileType('wb'),
                    help="CSV containing (x+y,xy) pairs");
args = parser.parse_args();

reader = csv.DictReader(args.input_csv);
writer = csv.DictWriter(args.output_csv,
                        ['sum', 'product'],
                        lineterminator=os.linesep)
writer.writeheader()

rows = list(reader)
shuffle(rows) # Makes this version reusable, but not deterministic
for row in rows:
    x = int(row['x'])
    y = int(row['y'])
    writer.writerow(dict(sum=x+y, product=x*y))
