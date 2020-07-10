"""Given a CSV file of URLs and filenames, download each URL to the given filename.

Output files and a log are written to an output directory.
"""
import argparse
import concurrent.futures
import csv
import logging
import pathlib
import typing as ty
from urllib.request import urlopen

PARSER = argparse.ArgumentParser("scraper.py")
PARSER.add_argument("inputfile", type=argparse.FileType("r"))
PARSER.add_argument("output_directory", type=pathlib.Path)


def main():
    args = PARSER.parse_args()

    output_directory: patlib.Path = args.output_directory

    if not (output_directory.exists() and output_directory.is_dir()):
        output_directory.mkdir()

    logging.basicConfig(
        filename=(output_directory / "scraper.log"), level=logging.INFO,
    )
    log = logging.getLogger("scraper.py")

    def download(url: str, filename: str) -> None:
        outpath = output_directory / filename
        try:
            response = urlopen(url)
            body = response.read()
            with outpath.open("wb") as outf:
                outf.write(body)
            log.info(f"Fetched {url}")
        except Exception as exc:
            log.error(f"Error fetching {url}: {exc}")

    log.info("Starting...")
    targets = csv.DictReader(args.inputfile)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        tasks = [executor.submit(download, **kwargs) for kwargs in targets]
        concurrent.futures.wait(tasks)
    log.info("Done")


if __name__ == "__main__":
    main()
