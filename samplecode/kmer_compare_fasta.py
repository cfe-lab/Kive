#! /usr/bin/env python
import argparse
import sys
import re
import csv

scriptDescription = 'Count all shared k-mers between every pair of sequences \
between two FASTAs using a very simple direct lookup of Python dicts.' 

parser = argparse.ArgumentParser(scriptDescription);

parser.add_argument("input_ref",help="CSV containing (str, nucleotide sequence) doublets")
parser.add_argument("input_query",help="CSV containing (str, nucleotide sequence) doublets")
parser.add_argument("output_csv",help="CSV containing (str a, str b, int c) doublets")
args = parser.parse_args()

k = 3  # just for demonstration, set fixed k-mer length

## read in reference sequences and generate k-mer distribution

try:
    f = open(args.input_ref, "rb")
except IOError as e:
    print(e)
    sys.exit(2)

try:
    dref = {}
    rows = csv.reader(f, delimiter=',')
    for h, s in rows:
        kmers = set([s[i:(i+k)] for i in range(0, len(s)-k)])
        counts = []
        for kmer in kmers:
            counts.append(len(re.findall('(?=%s)' % kmer.replace('?', '\?'), s)))
        dref.update({h: dict(zip(kmers, counts))})
except:
    print('Error parsing reference sequences')
    print()
    raise

f.close()


## read in query sequences and compute k-mer inner products
try:
    f = open(args.input_query, 'rb')
    f2 = open(args.output_csv, 'w')
    outfile = csv.writer(f2)
    outfile.writerow(['header', 'best_match', 'score'])
except IOError as e:
    print(e)
    sys.exit(2)

rows = csv.reader(f, delimiter = ',')
header = rows.next()

for h, s in rows:
    # exhaustive search through reference dictionaries
    best_score = 0.
    best_ref = None
    for ref, kdict in dref.iteritems():
        numer = 0
        denom = 0
        for kmer, ref_count in kdict.iteritems():        
            query_count = len(re.findall('(?=%s)' % kmer.replace('?', '\?'), s))
            numer += query_count * ref_count
            denom += ref_count * ref_count
        
        score = float(numer) / denom
        if score > best_score:
            best_score = score
            best_ref = ref
    outfile.writerow([h, best_ref, best_score])
    
f2.close()
f.close()

sys.exit(0) # return with no errors
