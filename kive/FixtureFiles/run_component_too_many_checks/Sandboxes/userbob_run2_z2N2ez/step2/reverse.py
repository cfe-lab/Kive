#!/usr/bin/env python
import sys
import csv
with open(sys.argv[1]) as infile, open(sys.argv[2], 'w') as outfile:
  reader = csv.reader(infile)
  writer = csv.writer(outfile)
  for row in reader:
      writer.writerow([row[1][::-1], row[0][::-1]])
