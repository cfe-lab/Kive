#! /usr/bin/env python

import argparse, sys

# In order to work with shipyard, scripts which having a inputs
# and b inputs must have a+b command line arguments, the first a
# arguments specifying paths of input files, the subsequent b
# arguments specifying the paths of where outputs are written]

scriptDescription = "Takes FASTA, outputs CSV containing (str header, str sequence)"
parser = argparse.ArgumentParser(scriptDescription);

parser.add_argument("input_fasta",help="FASTA-formatted text file")
parser.add_argument("output_csv",help="CSV containing (str header, str sequence) doublets")
args = parser.parse_args()

try:
    with open(args.input_fasta, "rb") as f:
        output = open(args.output_csv, "wb");
        output.write('string,DNA/RNA\n')
        
        try:            
            sequence = ''
            for line in f:
                if line.startswith('>'):
                    if len(sequence) > 0:
                        output.write('%s,%s\n' % (header, sequence))
                        sequence = ''
                    header = line.lstrip('>').rstrip('\n')
                else:
                    sequence += line.strip('\n').upper()
            
            output.write('%s,%s\n' % (header, sequence))
                
        except:
            print('Error parsing FASTA file')
            sys.exit(1)
            
        output.close()

    # If no errors, return with code 0 (success)
    sys.exit(0)

# Return error code 2 if file cannot be opened
except IOError as e:
    print(e)
    sys.exit(2)
