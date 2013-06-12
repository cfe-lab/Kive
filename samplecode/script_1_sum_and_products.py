#! /usr/bin/env python

"""
Takes as input a 2-column (x,y) CSV file and outputs
a single 2-column (x+y,x*y) output CSV file.
"""

import csv;
import argparse;
import sys;

# In order to work with shipyard, scripts which having a inputs
# and b inputs must have a+b command line arguments, the first a
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

try:
	with open(args.input_csv, "rb") as f:
		output = open(args.output_csv, "wb");

		# csv.reader() returns list inside an iterable
		# Iterables can be used in for/in blocks
		string_csv = csv.reader(f, delimiter=',');

		try:
			for row in string_csv:
				x = int(row[0])
				y = int(row[1])
				output.write(str(x+y) + "," + str(x*y) + "\n")

		# If csv iterable method __next__() throws error, exit
		except csv.Error as e:
			print("Error at line {}: {}".format(reader.line_num, e));
			sys.exit(1);

		output.close;

	# If no errors, return with code 0 (success)
	sys.exit(0);

# Return error code 2 if file cannot be opened
except IOError as e:
	print(e);
	sys.exit(2);
