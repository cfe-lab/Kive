#! /usr/bin/env python

"""
Verification script for whether a CSV file contains strings.

Determines whether the CSV file can be read, as the CSV module
will automatically turn everything into a string.

Exit code = 0 for success
Exic code = 1 for any error
Exic code = 2 if file cannot be opened
"""

import csv
import argparse
import sys

# ArgumentParser takes a specification of inputs for the script from
# sys.argv, generates help messages, and performs argv parsing

# argparse now intercepts script inputs and only allow
# the input ./stringUT.py -h or ./stringUT.py --help
scriptDescription = "Verifies a CSV file contains strings."
parser = argparse.ArgumentParser(scriptDescription)

# Add a positional argument file_containing_strings_to_check
parser.add_argument(
        "strings_to_validate",
        help="One-column CSV (no header) containing strings to validate")

# Assign parsed input to args
args = parser.parse_args()

# Attempt to open the file
try:

        # 'with' allows automization of try/finally where the invoked function
        # __enter__() and __exit__() defined (IE, is a ContextManager)

        # When the function is invoked, __enter__() returns the value to
        # the variable referenced by 'as' (Ex: setup and return a db handle)

        # When the with block is exited, __exit__() is invoked to perform
        # any necessary code tear-down

        # with DOES NOT CREATE NEW SCOPE
        with open(args.strings_to_validate, "rb") as f:
                # csv.reader() returns a list inside an iterable container
                string_csv = csv.reader(f, delimiter=',')

                # Iterables can be used in for/in blocks: first, __iter__()
                # is called, then repeated calls to __next__() - this is
                # why csv.reader() is not placed inside the try block
                try:
                        for row in string_csv:
                                print(row)
                                pass

	        # If csv __next__() throws an error, return error code 1
		except csv.Error as e:
			print("Error at line {}: {}".format(reader.line_num, e))
			sys.exit(1)

	# If no errors, return with code 0 (success)
	sys.exit(0)

# Return error code 2 if file cannot be opened
except IOError as e:
	print(e)
	sys.exit(2)
