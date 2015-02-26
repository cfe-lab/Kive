#! /usr/bin/env python

"""Complements the given RNA/DNA sequences.

The specified input CSV file should have one column (with header) consisting
of valid RNA/DNA sequences."""

from Bio.Seq import Seq
import argparse
import csv
import sys

parser = argparse.ArgumentParser("Complements the given RNA/DNA sequences")
parser.add_argument("seqs_to_complement",
                    help="One-column CSV of RNA/DNA sequences")
parser.add_arguments("output_csv_1", help="Complemented CSV")
args = parser.parse_args()

header = ""
complemented = []

with open(args.seqs_to_complement, "rb") as f:
    seq_reader = csv.reader(f)
    header = seq_reader.next()
    for row in seq_reader:
        complemented += [(Seq(row[0]).complement().tostring(),)]

with open(args.output_csv_1, "wb") as f:
    seq_writer = csv.writer(f)
    seq_writer.writerow(("ComplementedSeq",))
    seq_writer.writerows(complemented)

sys.exit(0)
