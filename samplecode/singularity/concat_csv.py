import argparse
import csv
import pathlib
import typing as ty
import unittest

PARSER = argparse.ArgumentParser()
PARSER.add_argument("--input_csvs", nargs="*")
PARSER.add_argument("outputfile")


def write_to_outfile(lines: ty.Iterable[ty.List[str]], outpath: pathlib.Path) -> None:
    with outpath.open("w") as outf:
        writer = csv.writer(outf)
        writer.writerows(lines)


def fileinputrows(inputpath: pathlib.Path) -> ty.Iterable[ty.List[str]]:
    inputname = inputpath.name
    with inputpath.open() as inputfile:
        reader = csv.reader(inputfile)
        for row in reader:
            yield [inputname] + list(row)


def parse_inputrows(inputpaths: ty.List[pathlib.Path]) -> ty.Iterable[ty.List[str]]:
    for inputpath in inputpaths:
        yield from fileinputrows(inputpath)


def main() -> None:
    args = PARSER.parse_args()
    input_csv = [pathlib.Path(inf) for inf in args.input_csvs]
    inputrows = parse_inputrows(input_csv)
    write_to_outfile(inputrows, pathlib.Path(args.outputfile))


class TestConcatCsv(unittest.TestCase):
    def test_parse_args(self):
        raw_args = ["--input_csvs", "first.csv", "second.csv", "--", "output.csv"]
        args = PARSER.parse_args(raw_args)
        self.assertEqual(args.input_csvs, ["first.csv", "second.csv"])
        self.assertEqual(args.outputfile, "output.csv")


if __name__ == "__main__":
    main()
