"""Given a file of names and a file of salutations to give each
name, make an output file with greetings for each name in the input file.

This script demonstrates the use of single-valued keyword-style parameters
to Kive pipelines.
"""

import argparse
import csv
import typing as ty

PARSER = argparse.ArgumentParser()
PARSER.add_argument("--names", type=argparse.FileType())
PARSER.add_argument("--salutations", type=argparse.FileType())
PARSER.add_argument("outputfile", type=argparse.FileType("w"))


def greet(name: str, salutation: str = "Hello") -> str:
    return f"{salutation} {name}!"


def get_salutations(rdr: csv.DictReader) -> ty.Dict[str, str]:
    return {row["name"]: row["salutation"] for row in rdr}


def main() -> None:
    args = PARSER.parse_args()

    names_reader = csv.DictReader(args.names)
    salutations = get_salutations(csv.DictReader(args.salutations))

    output_writer = csv.DictWriter(args.outputfile, fieldnames=["greeting"])
    output_writer.writeheader()
    for name in (r["name"] for r in names_reader):
        output_writer.writerow(
            {"greeting": greet(name, salutations.get(name, "Hello"))}
        )


if __name__ == "__main__":
    main()
