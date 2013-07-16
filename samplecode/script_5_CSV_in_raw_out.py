#! /usr/bin/env python

"""
CSV INPUT: 3-column CSV (a,b,c)
RAW OUTPUT: 3-column (a^2,b^2,c^2) with non-standard delimiter
"""

import csv
import argparse
import sys

# ArgumentParser facilitates parsing inputs from sys.argv, and
# generates help messages based on the expected input specification

# Only allow .stringUT.py -h or ./stringUT.py --help by default
scriptDescription =  "Takes raw underscore delimited input containing (a,b,c) and outputs CSV with (a^2,b^2,c^2)"
parser = argparse.ArgumentParser(scriptDescription)
parser.add_argument("input_1",help="CSV containing (a,b,c) tuples")
parser.add_argument("output_1",help="RAW CSV containing (a^2,b^2,c^2)")

# Assign parsed input to args
args = parser.parse_args()

try:
	# with does not create new scope
	with open(args.input_1, "rb") as f:

		output1 = open(args.output_1, "wb")

		# csv.reader() returns a list inside an iterable
		string_csv = csv.reader(f, delimiter=',')

		# Iterables can be used in for/in blocks
		try:
			for row in string_csv:
				a = float(row[0])
				b = float(row[1])
				c = float(row[2])
				output1.write(str(a*a) + "_" + str(b*b) + "_" + str(c*c) + "\n")

		# If csv iterable method __next__() throws error exot
		except csv.Error as e:
			print("Error at line {}: {}".format(reader.line_num, e))
			sys.exit(1)

		output1.close

	# If no errors, return with code 0 (success)
	sys.exit(0)

# Return error code 2 if file cannot be opened
except IOError as e:
	print(e)
	sys.exit(2)
