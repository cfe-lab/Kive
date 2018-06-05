#!/usr/bin/env python

from argparse import FileType, ArgumentParser
import csv
from operator import itemgetter
import os

parser = ArgumentParser(description='Calculate the total of a column.');
parser.add_argument("input_csv",
                    type=FileType('rU'),
                    help="CSV containing (sum,product) pairs");
parser.add_argument("output_csv",
                    type=FileType('w'),
                    help="CSV containing one (sum,product) pair");
args = parser.parse_args();

reader = csv.DictReader(args.input_csv);
writer = csv.DictWriter(args.output_csv,
                        ['sum', 'product'],
                        lineterminator=os.linesep)
writer.writeheader()

# Copy first row unchanged
for row in reader:
    writer.writerow(row)
    break

sum_total = 0
product_total = sum(map(int, map(itemgetter('product'), reader)))
writer.writerow(dict(sum=sum_total, product=product_total))
