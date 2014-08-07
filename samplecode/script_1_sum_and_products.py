#! /usr/bin/env python

"""
Takes as input a 2-column (x,y) CSV file and outputs
a single 2-column (x+y,x*y) output CSV file.
"""

import csv;
import argparse;
import sys;

# In order to work with shipyard, scripts which having a inputs
# and b outputs must have a+b command line arguments, the first a
# arguments specifying paths of input files, the subsequent b
# arguments specifying the paths of where outputs are written]


# ArgumentParser facilitates parsing inputs from sys.argv, and
# generates help messages based on the expected input specification

# Only allow .stringUT.py -h or ./stringUT.py --help by default
scriptDescription = "Takes CSV containing (x,y), \
outputs CSV containing (x+y),(x*y)";

parser = argparse.ArgumentParser(scriptDescription);
parser.add_argument("input_csv",help="CSV containing (x,y) pairs");
parser.add_argument("output_csv",help="CSV containing (x+y,xy) pairs");
args = parser.parse_args();

with open(args.input_csv, "rU") as f, open(args.output_csv, "w") as output:

    # csv.reader() returns list inside an iterable
    # Iterables can be used in for/in blocks
    reader = csv.DictReader(f);

    output.write('sum,product\n')
    try:
        for row in reader:
            x = int(row['x'])
            y = int(row['y'])
            output.write('{},{}\n'.format(x+y, x*y))

    # If csv iterable method __next__() throws error, exit
    except csv.Error as e:
        sys.exit("Error at line {}: {}".format(reader.line_num, e));


