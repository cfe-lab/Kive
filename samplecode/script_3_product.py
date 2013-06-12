#! /usr/bin/env python

"""
Takes two input CSV files. First, a one-column (k) input
CSV, and second, a single-row 1-column (r) CSV.

Returns a 1-column output CSV containing k*r
"""

import csv
import argparse
import sys

# ArgumentParser facilitates parsing inputs from sys.argv, and
# generates help messages based on the expected input specification

scriptDescription =  "Takes 2 single-column CSV files:\n\
1) A list of numbers (k)\n\
2) A single-row 'scalar' (r)\n\
Output: a 1-column CSV containing the product k*r"

# Add positional arguments
parser = argparse.ArgumentParser(scriptDescription)
parser.add_argument("input_csv_1", help="CSV containing k")
parser.add_argument("input_csv_2", help="1-row CSV containing r")
parser.add_argument("output_csv_1", help="Output CSV containing r*k")
args = parser.parse_args()

try:
	r = None

	with open(args.input_csv_2, "rb") as f1:
		r_csv = csv.reader(f1)
		try:
			for row in r_csv:
				r = float(row[0])
		except csv.Error as e:
			print("Error at line {}: {}".format(reader.line_num, e))
			sys.exit(1)

	with open(args.input_csv_1, "rb") as f2:
		output1 = open(args.output_csv_1, "wb")
		string_csv = csv.reader(f2, delimiter=',')

		try:
			for row in string_csv:
				k = float(row[0])
				output1.write(str(k*r) + "\n")
		except csv.Error as e:
			print("Error at line {}: {}".format(reader.line_num, e))
			sys.exit(1)

		output1.close
	sys.exit(0)

# Return error code 2 if file cannot be opened
except IOError as e:
	print(e)
	sys.exit(2)
