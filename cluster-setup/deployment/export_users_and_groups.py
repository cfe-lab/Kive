#! /usr/bin/env python

import csv
from typing import Optional, Iterable, Mapping
from collections.abc import Container
from dataclasses import dataclass, field, asdict
from io import TextIOBase

import argparse
import yaml


@dataclass
class ShadowEntry:
    name: str
    hashed_password: str
    last_changed: Optional[int]
    min: Optional[int]
    max: Optional[int]
    warn: Optional[int]
    inactive: Optional[int]
    expire: Optional[int]

def int_or_none(possible_int_string: str) -> Optional[int]:
    try:
        return int(possible_int_string)
    except ValueError:
        return None

def parse_shadow(shadow_file: TextIOBase) -> dict[str, ShadowEntry]:
    shadow_csv = csv.reader(shadow_file, delimiter=":")
    shadow_entries: dict[str, ShadowEntry] = {}
    for row in shadow_csv:
        name: str = row[0]
        shadow_entries[name] = ShadowEntry(
            name=name,
            hashed_password=row[1],
            last_changed=int_or_none(row[2]),
            min=int_or_none(row[3]),
            max=int_or_none(row[4]),
            warn=int_or_none(row[5]),
            inactive=int_or_none(row[6]),
            expire=int_or_none(row[7]),
        )
    return shadow_entries

@dataclass
class PasswdEntry:
    name: str
    passwdx: str
    uid: int
    gid: int
    info: str
    home: str
    shell: str

def parse_passwd(passwd_file: TextIOBase) -> dict[str, PasswdEntry]:
    passwd_csv = csv.reader(passwd_file, delimiter=":")
    passwd_entries: dict[str, PasswdEntry] = {}
    for row in passwd_csv:
        name: str = row[0]
        passwd_entries[name] = PasswdEntry(
            name=name,
            passwdx=row[1],
            uid=int(row[2]),
            gid=int(row[3]),
            info=row[4],
            home=row[5],
            shell=row[6],
        )
    return passwd_entries

@dataclass
class GroupEntry:
    name: str
    passwdx: str
    gid: int
    users: list[str]

def parse_group(
    group_file: TextIOBase,
    sudo_original_name: str,
    sudo_new_gid: int,
    sudo_new_name: str,
) -> dict[int, GroupEntry]:
    group_csv = csv.reader(group_file, delimiter=":")
    group_entries: dict[GroupEntry] = {}
    for row in group_csv:
        gid: int = int(row[2])
        name: str = row[0]
        if name == sudo_original_name:
            gid = sudo_new_gid
            name = sudo_new_name
        group_entries[gid] = GroupEntry(
            name=name,
            passwdx=row[1],
            gid=gid,
            users=row[3].split(","),
        )
    return group_entries

def get_other_groups_by_user(
    users_to_export: Iterable[str],
    groups_to_export: Container[str],
    passwd_entries: dict[int, PasswdEntry],
    group_entries: dict[int, GroupEntry],
) -> dict[str, list[str]]:
    """
    Assemble a mapping of username to the (non-primary) groups that this user belongs to.
    """
    groups_by_user: dict[str, list[str]] = {}
    for user in users_to_export:
        groups_by_user[user] = []

    for gid, group_entry in group_entries.items():
        group_name: str = group_entry.name
        if group_name not in groups_to_export:
            continue
        for user in group_entry.users:
            if user in groups_by_user:
                passwd_entry: PasswdEntry = passwd_entries[user]
                if gid != passwd_entry.gid:  # check if this is the primary group
                    groups_by_user[user].append(group_name)

    return groups_by_user


@dataclass
class User:
    name: str
    hashed_password: str
    uid: int
    home: str
    primary_group: str
    groups: list[str] = field(default_factory=list)

def create_user(
    name: str,
    passwd_entries: dict[str, PasswdEntry],
    shadow_entries: dict[str, ShadowEntry],
    group_entries: dict[str, GroupEntry],
    groups_by_user: dict[str, list[str]],
) -> User:
    passwd_entry: PasswdEntry = passwd_entries[name]
    return User(
        name=name,
        hashed_password=shadow_entries[name].hashed_password,
        uid=passwd_entry.uid,
        home=passwd_entry.home,
        primary_group=group_entries[passwd_entry.gid].name,
        groups=groups_by_user[name]
    )


def get_user_primary_groups(
    users: Iterable[User],
    group_entries: dict[int, GroupEntry],
) -> dict[str, GroupEntry]:
    primary_groups: dict[str, GroupEntry] = {}
    groups_by_name: dict[str, GroupEntry] = {}
    for group_entry in group_entries.values():
        groups_by_name[group_entry.name] = group_entry
    for user in users:
        primary_groups[user.name] = groups_by_name[user.primary_group]
    return primary_groups


@dataclass
class ExportedUsersAndGroups:
    users: list[User]
    primary_groups: list[GroupEntry]
    other_groups: list[GroupEntry]


def exported_users_and_groups(
    users_to_export: Iterable[str],
    groups_to_export: Container[str],
    passwd_entries: Mapping[str, PasswdEntry],
    shadow_entries: Mapping[str, ShadowEntry],
    group_entries: Mapping[int, GroupEntry],
) -> ExportedUsersAndGroups:

    other_groups_by_user: dict[str, list[str]] = get_other_groups_by_user(
        users_to_export,
        groups_to_export,
        passwd_entries,
        group_entries,
    )

    users: dict[str, User] = {}
    for username in users_to_export:
        users[username] = create_user(
            username,
            passwd_entries,
            shadow_entries,
            group_entries,
            other_groups_by_user,
        )

    primary_groups: dict[int, GroupEntry] = get_user_primary_groups(
        users.values(),
        group_entries,
    )
    other_groups: dict[int, GroupEntry] = {}
    for gid, group_entry in group_entries.items():
        if gid in primary_groups or group_entry.name not in groups_to_export:
            continue
        other_groups[gid] = group_entry

    return ExportedUsersAndGroups(
        list(users.values()),
        list(primary_groups.values()),
        list(other_groups.values()),
    )


def main():
    parser = argparse.ArgumentParser(
        "Collate user and group information for recreating them on a new server"
    )
    parser.add_argument(
        "--passwd",
        help="The passwd file (as it appears in /etc/passwd on the original server)",
        default="/etc/passwd",
    )
    parser.add_argument(
        "--shadow",
        help="The shadow file (as it appears in /etc/shadow on the original server)",
        default="/etc/shadow",
    )
    parser.add_argument(
        "--group",
        help="The group file (as it appears in /etc/group on the original server)",
        default="/etc/group",
    )
    parser.add_argument(
        "--out",
        help="File to write the output YAML to (default out.yaml)",
        default="out.yaml",
    )
    parser.add_argument(
        "users_and_groups",
        help="YAML file with `users` (list of usernames to export) and `groups` (list of group names to export)",
    )
    parser.add_argument(
        "sudo_original_name",
        help="Name of the wheel/sudo group on the exporting machine",
    )
    parser.add_argument(
        "sudo_new_gid",
        help="GID of the wheel/sudo group on the importing machine",
    )
    parser.add_argument(
        "sudo_new_name",
        help="Name of the wheel/sudo group on the importing machine",
    )
    args = parser.parse_args()

    with open(args.users_and_groups, "r") as f:
        users_and_groups = yaml.safe_load(f)

    users_to_export: list[str] = users_and_groups["users"]
    groups_to_export: list[str] = users_and_groups["groups"]

    with open(args.passwd, "r") as f:
        passwd_entries: dict[str, PasswdEntry] = parse_passwd(f)

    with open(args.shadow, "r") as f:
        shadow_entries: dict[str, ShadowEntry] = parse_shadow(f)

    with open(args.group, "r") as f:
        group_entries: dict[str, GroupEntry] = parse_group(
            f,
            args.sudo_original_name,
            int(args.sudo_new_gid),
            args.sudo_new_name,
        )

    for_export: ExportedUsersAndGroups = exported_users_and_groups(
        users_to_export,
        groups_to_export,
        passwd_entries=passwd_entries,
        shadow_entries=shadow_entries,
        group_entries=group_entries,
    )
    serialized = {
        "users": [asdict(x) for x in for_export.users],
        "primary_groups": [asdict(x) for x in for_export.primary_groups],
        "other_groups": [asdict(x) for x in for_export.other_groups],
    }
    with open(args.out, "w") as f:
        yaml.dump(serialized, f)


if __name__ == "__main__":
    main()
