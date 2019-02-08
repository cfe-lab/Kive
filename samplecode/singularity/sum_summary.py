from argparse import FileType, ArgumentParser
import csv
import os

parser = ArgumentParser(
    description="Checks which is bigger: sum or product.")
parser.add_argument("sums_csv",
                    type=FileType('rU'),
                    help="CSV containing (sum,product) pairs")
parser.add_argument("summary_csv",
                    type=FileType('wb'),
                    help="CSV containing (sum,product,bigger) rows")
args = parser.parse_args()

reader = csv.DictReader(args.sums_csv)
writer = csv.DictWriter(args.summary_csv,
                        ['sum', 'product', 'bigger'],
                        lineterminator=os.linesep)
writer.writeheader()

for row in reader:
    row['bigger'] = 'sum' if row['product'] < row['sum'] else 'product'
    writer.writerow(row)
