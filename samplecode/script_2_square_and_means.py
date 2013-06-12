#! /usr/bin/env python

"""
Takes as input one 3-column (a,b,c) input CSV and outputs
a 3-column (a',b',c') output CSV with the square of each
term, and also a 1-column (u) CSV with the mean of (a,b,c)
"""

import csv
import argparse
import sys

# ArgumentParser facilitates parsing inputs from sys.argv, and
# generates help messages based on the expected input specification

# Only allow .stringUT.py -h or ./stringUT.py --help by default
scriptDescription =  "Takes 3-column CSV containing (a,b,c), \
outputs a 3-column CSV (a^2,b^2,c^2), \
and a 1-column CSV (u) containing the mean \
of (a,b,c)"

parser = argparse.ArgumentParser(scriptDescription)

parser.add_argument("input_1",help="CSV containing (a,b,c) tuples")
parser.add_argument("output_1",help="CSV containing (a^2,b^2,c^2)")
parser.add_argument("output_2",help="CSV containing mean(a,b,c)")

# Assign parsed input to args
args = parser.parse_args()

try:
	# with does not create new scope
	with open(args.input_1, "rb") as f:

		output1 = open(args.output_1, "wb")
		output2 = open(args.output_2, "wb")

		# csv.reader() returns a list inside an iterable
		string_csv = csv.reader(f, delimiter=',')

		# Iterables can be used in for/in blocks
		try:
			for row in string_csv:
				a = float(row[0])
				b = float(row[1])
				c = float(row[2])
				output1.write(str(a*a) + "," + str(b*b) + "," + str(c*c) + "\n")
				output2.write(str((a+b+c)/3) + "\n")

		# If csv iterable method __next__() throws error exot
		except csv.Error as e:
			print("Error at line {}: {}".format(reader.line_num, e))
			sys.exit(1)

		output1.close
		output2.close

	# If no errors, return with code 0 (success)
	sys.exit(0)

# Return error code 2 if file cannot be opened
except IOError as e:
	print(e)
	sys.exit(2)
