#! /usr/bin/env python

import argparse


def main():
    parser = argparse.ArgumentParser("Create cloud-init user-data.template for the worker nodes")
    parser.add_argument(
        "--template",
        help="Template file to insert the root SSH public key into",
        default="user-data.template",
    )
    parser.add_argument(
        "--output",
        help="File to write the resulting user-data.template file to",
        default="user-data",
    )
    parser.add_argument(
        "ssh_public_key",
        help="SSH public key file to insert into the template"
    )
    args = parser.parse_args()

    with open(args.ssh_public_key, "r") as f:
        ssh_key: str = f.read().strip()

    with open(args.template, "r") as template:
        with open(args.output, "w") as output:
            output.write(template.read().format(root_ssh_public_key=ssh_key))


if __name__ == "__main__":
    main()
