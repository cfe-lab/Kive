#! /usr/bin/env python

"""
CSV INPUT_1: 3-column CSV (a,b,c)
RAW INPUT_2: 3-column (d,e,f) with non-standard '_' delimiter (single line)

CSV OUTPUT_1: 3-column (a+d,b+e,c+f)
RAW OUTPUT_2: 3-column (a+d,b+e,c+f) with non-standard '_' delimiter
"""

import csv
import argparse
import sys

# ArgumentParser facilitates parsing inputs from sys.argv, and
# generates help messages based on the expected input specification

scriptDescription =  "Takes CSV (a,b,c) and RAW (d,e,f) and outputs (a+d,b+e,c+f) in CSV and RAW"
parser = argparse.ArgumentParser(scriptDescription)
parser.add_argument("input_1",help="CSV containing (a,b,c) tuples")
parser.add_argument("input_2",help="Single-row RAW containing a single line of (d,e,f) separated by '_'")
parser.add_argument("output_1",help="CSV containing (a+d,b+e,c+f)")
parser.add_argument("output_2",help="RAW containing (a+d,b+e,c+f) separated by '_'")

# Assign parsed input to args
args = parser.parse_args()

try:

	d = -999
	e = -999
	f = -999

	with open(args.input_2, "rb") as myFile:
		string_csv = csv.reader(myFile, delimiter="_")

		for row in string_csv:
			d = float(row[0])
			e = float(row[1])
			f = float(row[2])


	# with does not create new scope
	with open(args.input_1, "rb") as myFile:

		output1 = open(args.output_1, "wb")
		output2 = open(args.output_2, "wb")
		string_csv = csv.reader(myFile, delimiter=',')

		try:
			for row in string_csv:
				a = float(row[0])
				b = float(row[1])
				c = float(row[2])
				output1.write(str(a+d) + "," + str(b+e) + "," + str(c+f) + "\n")
				output2.write(str(a+d) + "_" + str(b+e) + "_" + str(c+f) + "\n")

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
