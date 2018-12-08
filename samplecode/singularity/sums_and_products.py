from argparse import FileType, ArgumentParser
import csv
import os

# In order to work with kive, scripts that have a inputs
# and b outputs must have a+b command line arguments, the first a
# arguments specifying paths of input files, the subsequent b
# arguments specifying the paths where outputs are written.


# ArgumentParser facilitates parsing inputs from sys.argv, and
# generates help messages based on the expected input specification
parser = ArgumentParser(
    description="Takes CSV with (x,y), outputs CSV with (x+y),(x*y)")
parser.add_argument("input_csv",
                    type=FileType('rU'),
                    help="CSV containing (x,y) pairs")
parser.add_argument("output_csv",
                    type=FileType('wb'),
                    help="CSV containing (x+y,xy) pairs")
args = parser.parse_args()

reader = csv.DictReader(args.input_csv)
writer = csv.DictWriter(args.output_csv,
                        ['sum', 'product'],
                        lineterminator=os.linesep)
writer.writeheader()

for row in reader:
    x = int(row['x'])
    y = int(row['y'])
    writer.writerow(dict(sum=x+y, product=x*y))
