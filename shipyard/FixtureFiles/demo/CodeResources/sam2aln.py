#! /usr/bin/env python

"""
Shipyard-style MiSeq pipeline, step 3
Takes SAM in CSV format as input.  Assumes that these data have been
through remap.py (but not strictly necessary!).  Merges paired-end reads
and outputs aligned sequences (with insertions and deletions, minus soft
clips).

Dependencies:
    bowtie2-build
    bowtie2-align
    samtools
    settings.py
"""

import argparse
import sys
import itertools
import re

from settings import max_prop_N, read_mapping_cutoff, sam2aln_q_cutoffs
import csv

def parseArgs():
    parser = argparse.ArgumentParser(
        description='Conversion of SAM data into aligned format.')
    parser.add_argument('remap_csv',
                        type=argparse.FileType('rU'),
                        help='<input> SAM output of bowtie2 in CSV format')
    parser.add_argument('aligned_csv',
                        type=argparse.FileType('w'),
                        help='<output> CSV containing cleaned and merged reads')
    parser.add_argument('insert_csv',
                        type=argparse.FileType('w'),
                        help='<output> CSV containing insertions relative to sample consensus')
    parser.add_argument('failed_csv',
                        type=argparse.FileType('w'),
                        help='<output> CSV containing reads that failed to merge')
    
    return parser.parse_args()


cigar_re = re.compile('[0-9]+[MIDNSHPX=]')  # CIGAR token
gpfx = re.compile('^[-]+')  # length of gap prefix


def apply_cigar (cigar, seq, qual):
    """
    Use CIGAR string (Compact Idiosyncratic Gapped Alignment Report) in SAM data
    to apply soft clips, insertions, and deletions to the read sequence.
    Any insertions relative to the sample consensus sequence are discarded to
    enforce a strict pairwise alignment, and returned separately in a
    dict object.
    """
    newseq = ''
    newqual = ''
    insertions = {}
    tokens = cigar_re.findall(cigar)
    if len(tokens) == 0:
        return None, None, None
    # Account for removing soft clipped bases on left
    shift = 0
    if tokens[0].endswith('S'):
        shift = int(tokens[0][:-1])
    left = 0
    for token in tokens:
        length = int(token[:-1])
        # Matching sequence: carry it over
        if token[-1] == 'M':
            newseq += seq[left:(left+length)]
            newqual += qual[left:(left+length)]
            left += length
        # Deletion relative to reference: pad with gaps
        elif token[-1] == 'D':
            newseq += '-'*length
            newqual += ' '*length  # Assign fake placeholder score (Q=-1)
        # Insertion relative to reference: skip it (excise it)
        elif token[-1] == 'I':
            insertions.update({left: (seq[left:(left+length)], qual[left:(left+length)])})
            left += length
            continue
        # Soft clipping leaves the sequence in the SAM - so we should skip it
        elif token[-1] == 'S':
            left += length
            continue
        else:
            print "Unable to handle CIGAR token: {} - quitting".format(token)
            sys.exit()

    return shift, newseq, newqual, insertions


def merge_pairs (seq1, seq2, qual1, qual2, q_cutoff=10, minimum_q_delta=5):
    """
    Combine paired-end reads into a single sequence by managing discordant
    base calls on the basis of quality scores.
    """
    mseq = ''
    # force second read to be longest of the two
    if len(seq1) > len(seq2):
        seq1, seq2 = seq2, seq1
        qual1, qual2 = qual2, qual1

    for i, c2 in enumerate(seq2):
        q2 = ord(qual2[i])-33
        if i < len(seq1):
            c1 = seq1[i]
            q1 = ord(qual1[i])-33
            if c1 == '-' and c2 == '-':
                mseq += '-'
                continue
            if c1 == c2:  # Reads agree and at least one has sufficient confidence
                if q1 > q_cutoff or q2 > q_cutoff:
                    mseq += c1
                else:
                    mseq += 'N'  # neither base is confident
            else:
                if abs(q2 - q1) >= minimum_q_delta:
                    if q1 > max(q2, q_cutoff):
                        mseq += c1
                    elif q2 > max(q1, q_cutoff):
                        mseq += c2
                    else:
                        mseq += 'N'
                else:
                    mseq += 'N'  # cannot resolve between discordant bases
        else:
            # past end of read 1
            if c2 == '-' and q2 == 0:
                mseq += 'n'  # interval between reads
                continue
            mseq += c2 if (q2 > q_cutoff) else 'N'
    return mseq


def len_gap_prefix(s):
    hits = gpfx.findall(s)
    if hits:
        return len(hits[0])
    return 0

class RemapReader(object):
    
    def __init__(self, remap_file):
        self.remap_reader = csv.DictReader(remap_file)
        
    def _get_key(self, row):
        return (row['sample_name'], row['rname'])
    
    def read_groups(self):
        for key, group in itertools.groupby(self.remap_reader, self._get_key):
            sample_name, refname = key
            yield sample_name, refname, group
        
def is_first_read(flag):
    """
    Interpret bitwise flag from SAM field.
    Returns True or False indicating whether the read is the first read in a pair.
    """
    IS_FIRST_SEGMENT = 0x40
    return (int(flag) & IS_FIRST_SEGMENT) != 0

def main():
    args = parseArgs()
    
    reader = RemapReader(args.remap_csv)
    args.aligned_csv.write('sample,refname,qcut,rank,count,offset,seq\n')
    insert_writer = csv.DictWriter(args.insert_csv,
                                   ['sample',
                                    'qname',
                                    'fwd_rev',
                                    'refname',
                                    'pos',
                                    'insert',
                                    'qual'])
    insert_writer.writeheader()
    failed_writer = csv.DictWriter(args.failed_csv,
                                   ['sample',
                                    'qname',
                                    'qcut',
                                    'seq1',
                                    'qual1',
                                    'seq2',
                                    'qual2',
                                    'prop_N',
                                    'mseq'])
    failed_writer.writeheader()

    for sample_name, refname, group in reader.read_groups():
        aligned = dict([(qcut, {}) for qcut in sam2aln_q_cutoffs])
        cached_reads = {}  # for mate pairing
        for row in group:
            if refname == '0':
                print row
                sys.exit()

            cigar = row['cigar']
            qname = row['qname']
            if cigar == '*' or int(row['mapq']) < read_mapping_cutoff:
                continue

            pos1 = int(row['pos'])-1  # convert 1-index to 0-index

            # report insertions relative to sample consensus
            _, seq1, qual1, inserts = apply_cigar(cigar, row['seq'], row['qual'])
            for left, (iseq, iqual) in inserts.iteritems():
                insert_writer.writerow({'sample': sample_name,
                                        'qname': qname,
                                        'fwd_rev': 'F' if is_first_read(row['flag']) else 'R',
                                        'refname': refname,
                                        'pos': pos1+left,
                                        'insert': iseq,
                                        'qual': iqual})

            seq2 = '-'*pos1 + seq1  # pad sequence on left
            qual2 = '!'*pos1 + qual1  # assign lowest quality to gap prefix so it does not override mate

            if qname not in cached_reads:
                cached_reads.update({qname: (seq2, qual2)})
                continue

            # otherwise we have a matched pair
            seq1, qual1 = cached_reads.pop(qname)
            for qcut in sam2aln_q_cutoffs:
                mseq = merge_pairs(seq1, seq2, qual1, qual2, qcut)
                prop_N = mseq.count('N') / float(len(mseq.strip('-')))
                if prop_N > max_prop_N:
                    # merged read is too messy
                    failed_writer.writerow({'sample': sample_name,
                                            'qname': qname,
                                            'seq1': seq1,
                                            'qual1': qual1,
                                            'seq2': seq2,
                                            'qual2': qual2,
                                            'prop_N': prop_N,
                                            'mseq': mseq})
                    continue

                if mseq in aligned[qcut]:
                    aligned[qcut][mseq] += 1  # compress identical sequences
                else:
                    aligned[qcut].update({mseq: 1})

        # output compressed data
        for qcut, data in aligned.iteritems():
            # sort variants by count
            intermed = [(count, len_gap_prefix(s), s) for s, count in data.iteritems()]
            intermed.sort(reverse=True)
            for rank, (count, offset, seq) in enumerate(intermed):
                args.aligned_csv.write(','.join(map(str, [sample_name,
                                                 refname,
                                                 qcut,
                                                 rank,
                                                 count,
                                                 offset,
                                                 seq.strip('-')])))
                args.aligned_csv.write('\n')


if __name__ == '__main__':
    main()
