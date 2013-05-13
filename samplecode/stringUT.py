#! /usr/bin/env python

"""Verification script for whether a CSV file contains strings.

This is actually pretty much a question of whether the CSV file can be read,
as the CSV module will automatically turn everything into a string.

The exit code will be 0 if everything is fine and 1 if there are any errors,
along with an accompanying message telling you the first line at which
an error occurs.  If the file fails to open outright, then the exit code
is 2."""

import csv;
import argparse;
import sys;

parser = argparse.ArgumentParser("Verification script for whether a CSV file contains strings.");
parser.add_argument("strings_to_check",
                    help="One-column CSV file (no header) consisting of strings to check");
args = parser.parse_args();

try:
    with open(args.strings_to_check, "rb") as f:
        string_csv = csv.reader(f);
        
        # Check for errors
        try:
            for row in string_csv:
                pass
        except csv.Error as e:
            print("Error at line {}: {}".format(reader.line_num, e));
            sys.exit(1);
        
        # Okay, if we got here, everything is fine.
        sys.exit(0);
except IOError as e:
    print(e);
    sys.exit(2);

