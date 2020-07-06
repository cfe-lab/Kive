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


def get_salutations(inputfile: ty.Optional[ty.TextIO]) -> ty.Dict[str, str]:
    if inputfile is not None:
        rdr = csv.DictReader(inputfile)
        return {row["name"]: row["salutation"] for row in rdr}
    else:
        return {
            "Grace Hopper": "Oh my goodness, it's Admiral",
            "Radia Perlman": "Introducing the inventor of the spanning-tree protocol,",
        }


def get_names(inputfile: ty.Optional[ty.TextIO]) -> ty.Iterable[str]:
    if inputfile is not None:
        rdr = csv.DictReader(inputfile)
        yield from (r["name"] for r in rdr)
    else:
        yield from iter(["Abraham", "Bud", "Charlize", "Radia Perlman"])


def main() -> None:
    args = PARSER.parse_args()

    names = get_names(args.names)
    salutations = get_salutations(args.salutations)

    output_writer = csv.DictWriter(args.outputfile, fieldnames=["greeting"])
    output_writer.writeheader()
    for name in names:
        output_writer.writerow(
            {"greeting": greet(name, salutations.get(name, "Hello"))}
        )


if __name__ == "__main__":
    main()
