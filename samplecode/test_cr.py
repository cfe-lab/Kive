#! /usr/bin/env python

"""Complements the given RNA/DNA sequences.

The specified input CSV file should have one column (with header) consisting
of valid RNA/DNA sequences."""

# We'll use Biopython for the actual substance of the code
from Bio.Seq import Seq;

import argparse;
import csv;
import sys;

parser = argparse.ArgumentParser("Complements the given RNA/DNA sequences");
parser.add_argument("seqs_to_complement",
                    help = "One-column CSV (with header) consisting of RNA/DNA sequences to complement");
args = parser.parse_args();

# Skip the first row.
header = "";
# Complemented sequences
complemented = [];

with open(args.seqs_to_complement, "rb") as f:
    seq_reader = csv.reader(f);
    header = seq_reader.next();
    for row in seq_reader:
        complemented += [(Seq(row[0]).complement().tostring(),)];

# DEBUGGING
#print(complemented);

with open("ComplementedSeqs.csv", "wb") as f:
    seq_writer = csv.writer(f);
    seq_writer.writerow(("ComplementedSeq",));

    seq_writer.writerows(complemented);

sys.exit(0);
