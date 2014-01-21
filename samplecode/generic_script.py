#! /usr/bin/env python

import argparse, csv, time, sys

# In order to work with shipyard, scripts which having a inputs
# and b inputs must have a+b command line arguments, the first a
# arguments specifying paths of input files, the subsequent b
# arguments specifying the paths of where outputs are written]

scriptDescription = "Takes CSV containing (str a, int b), outputs CSV containing (str c, int d)"
parser = argparse.ArgumentParser(scriptDescription);

parser.add_argument("input_csv",help="CSV containing (str a, int b) doublets")
parser.add_argument("output_csv",help="CSV containing (int c, str d) doublets")
args = parser.parse_args()

try:
	with open(args.input_csv, "rb") as f:
		output = open(args.output_csv, "wb");

		# csv.reader() returns list inside an iterable
		# Iterables can be used in for/in blocks
		string_csv = csv.reader(f, delimiter=',');

		try:
			for i, row in enumerate(string_csv):
				time.sleep(0.5)
				if i == 0:
					# Output column names must be registered in shipyard
					output.write("c,d\n")
					continue
				print "Processing row {}".format(i)
				a = str(row[0])
				b = int(row[1])
				c = 2*b
				d = a
				output.write(str(c) + "," + str(d) + "\n")

		# If csv iterable method __next__() throws error, exit
		except csv.Error as e:
			print("Error at line {}: {}".format(reader.line_num, e))
			sys.exit(1)

		output.close()

	# If no errors, return with code 0 (success)
	sys.exit(0)

# Return error code 2 if file cannot be opened
except IOError as e:
	print(e)
	sys.exit(2)
