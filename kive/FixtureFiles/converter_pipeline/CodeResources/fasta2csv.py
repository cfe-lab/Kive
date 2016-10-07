#! /usr/bin/env python

import argparse, sys, csv

# In order to work with kive, scripts which having a inputs
# and b inputs must have a+b command line arguments, the first a
# arguments specifying paths of input files, the subsequent b
# arguments specifying the paths of where outputs are written]

scriptDescription = "Takes FASTA, outputs CSV containing (str header, str sequence)"
parser = argparse.ArgumentParser(scriptDescription)

parser.add_argument("input_fasta", help="FASTA-formatted text file")
parser.add_argument("output_csv", help="CSV containing (str header, str sequence) doublets")
args = parser.parse_args()

try:
    csvfile = open(args.output_csv, 'wb')
    output = csv.writer(csvfile)
    output.writerow(['FASTA header', 'FASTA sequence'])
    header = ''
    
    with open(args.input_fasta, "rb") as f:
        try:            
            sequence = ''
            for line in f:
                if line.startswith('>'):
                    if len(sequence) > 0:
                        output.writerow([header, sequence])
                        sequence = ''
                    header = line.lstrip('>').rstrip('\n')
                else:
                    sequence += line.strip('\n').upper()
            
            output.writerow([header, sequence])
                
        except:
            print('Error parsing FASTA file')
            raise
            sys.exit(1)
    
    csvfile.close()
    # If no errors, return with code 0 (success)
    sys.exit(0)

# Return error code 2 if file cannot be opened
except IOError as e:
    print(e)
    sys.exit(2)
