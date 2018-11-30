from argparse import ArgumentParser, FileType
from csv import DictReader, DictWriter


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('names_csv', type=FileType())
    parser.add_argument('greetings_csv', type=FileType('wb'))

    return parser.parse_args()


def main():
    args = parse_args()
    reader = DictReader(args.names_csv)
    writer = DictWriter(args.greetings_csv, ['greeting'])
    writer.writeheader()
    for row in reader:
        writer.writerow(dict(greeting='Hello, ' + row['name']))


main()
