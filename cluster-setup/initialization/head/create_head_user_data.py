#! /usr/bin/env python

import argparse
import textwrap

import yaml


def main():
    parser = argparse.ArgumentParser("Create cloud-init user-data for the head node")
    parser.add_argument(
        "--template",
        help="Template file to insert the host mappings into",
        default="user-data.template",
    )
    parser.add_argument(
        "--output",
        help="File to write the resulting user-data file to",
        default="user-data",
    )
    parser.add_argument(
        "host_mapping_yaml",
        help="YAML file containing the compute node details in `compute_nodes`",
    )
    args = parser.parse_args()

    with open(args.host_mapping_yaml, "r") as f:
        host_mappings = yaml.safe_load(f)["compute_nodes"]

    host_mapping_str: str = "\n".join(
        [f'{hm["name"]}\t{hm["ip"]}' for hm in host_mappings]
    )
    host_mapping_str = textwrap.indent(host_mapping_str, "      ")
    with open(args.template, "r") as template:
        with open(args.output, "w") as output:
            output.write(template.read().format(host_mappings=host_mapping_str))


if __name__ == "__main__":
    main()
