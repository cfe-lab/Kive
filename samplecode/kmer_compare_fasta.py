#! /usr/bin/env python

import argparse
import sys
import re

scriptDescription = 'Count all shared k-mers between every pair of sequences ' 
    'between two FASTAs using a very simple direct lookup of Python dicts.' 

parser = argparse.ArgumentParser(scriptDescription);

parser.add_argument("input_ref",help="CSV containing (str, nucleotide sequence) doublets")
parser.add_argument("input_query",help="CSV containing (str, nucleotide sequence) doublets")
parser.add_argument("output_csv",help="CSV containing (str a, str b, int c) doublets")
args = parser.parse_args()

k = 4  # just for demonstration, set fixed k-mer length

## read in reference sequences and generate k-mer distribution
dref = {}
try:
    with open(args.input_ref, "rb") as f:
        for line in f:
            h, s = line.strip('\n').split(',')
            kmers = set([s[i:(i+k)] for i in range(0, len(s)-k)])
            counts = [len(re.findall('(?=%s)' % kmer, s)) for kmer in kmers]
            dref.update({h: dict(zip(kmers, counts))})

    # If no errors, return with code 0 (success)

# Return error code 2 if file cannot be opened
except IOError as e:
    print(e)
    sys.exit(2)


## read in query sequences and compute k-mer inner products
try:
    with open(args.input_query, "rb") as f:
        outfile = open(args.output_csv, 'w')
        
        for line in f:
            h, s = line.strip('\n').split(',')
            # exhaustive search through reference dictionaries
            best_score = 0
            best_ref = None
            for ref, kdict in dref.iteritems():
                counts = [len(re.findall('(?=%s)' % kmer, s)) for kmer in kmers]
                score = sum(counts)
                if score > best_score:
                    best_score = score
                    best_ref = ref
            
            outfile.write('%s,%s,%d' % ())
            
        outfile.close()


# Return error code 2 if file cannot be opened
except IOError as e:
    print(e)
    sys.exit(2)

# If no errors, return with code 0 (success)
sys.exit(0)