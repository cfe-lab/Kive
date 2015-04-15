#! /usr/bin/env python

__author__ = 'rliang'

from Bio.Seq import Seq


def main():
    import csv
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("seqs")
    parser.add_argument("complemented_seqs")
    args = parser.parse_args()

    with open(args.seqs, "rb") as f, open(args.complemented_seqs, "wb") as g:
        my_reader = csv.DictReader(f)
        my_writer = csv.writer(g)
        my_writer.writerow(("header", "nuc_seq"))
        # The input CSV file looks like (header, nuc_seq).
        for row in my_reader:
            my_writer.writerow((row["header"], Seq(row["nuc_seq"]).complement().tostring()))

if __name__ == "__main__":
    main()