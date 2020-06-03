#! /usr/bin/env python
"""Tests whether the given file contains valid RNA nucleotide sequences.

Exits with exit code 0 if file checks out; exit code 1 if not."""

import re;
import csv;
import sys;

# Regular expression that matches a nucleotide sequence
nuc_re = re.compile("^[aAcCgGuU]*$");
def is_nucleotide_sequence(possible_seq):
    """Tests argument to see if it is a valid RNA nucleotide sequence.

    Returns T/F based on whether possible_seq is a valid RNA nucleotide sequence;
    i.e. contains only A, C, G, and/or U (or a/c/g/u).
    Arguments:
    possible_seq -- string to be tested
    """
    result = False;
    if nuc_re.match(possible_seq):
        result = True;

    return(result);


# Driver code
if __name__ == "__main__":
    import argparse;
    import csv;

    parser = argparse.ArgumentParser();
    parser.add_argument("inputCSV",
                        help="One-column CSV file whose entries are to be tested to see whether they are valid RNA nucleotide sequences or not (without header entry)");
    args = parser.parse_args();

    entries_pass = True;
    with open(args.inputCSV, "rb") as f:
        csv_reader = csv.reader(f);

        row_num = 1;
        for row in csv_reader:
            if not is_nucleotide_sequence(row[0]):
                print("Entry {} ({}) is not a RNA nucleotide sequence.".format(row_num, row[0]));
                entries_pass = False;
            row_num += 1;

    # If everything checked out, exit with code 0; otherwise, with code 1.
    if entries_pass:
        sys.exit(0);
    else:
        sys.exit(1);


