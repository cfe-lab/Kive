from __future__ import unicode_literals

from argparse import ArgumentParser, FileType
from csv import DictWriter
import os
import re

from subprocess import check_output

from datetime import datetime
from time import sleep


def parse_args():
    parser = ArgumentParser(description='Log free memory.')
    parser.add_argument('-d', '--delay', type=int, default=60)
    parser.add_argument('log', type=FileType('w'))

    return parser.parse_args()


class ZoneInfoScanner(object):
    def __init__(self, zone_info):
        self.zone_info = zone_info

    def __iter__(self):
        i = None
        entry = None
        for line in self.zone_info:
            fields = re.split(r'[ :,]+', line.strip())
            if entry is None:
                if (len(fields) == 5 and
                        fields[1] == 'Node' and
                        fields[3:5] == ['zone', 'Normal']):
                    i = 0
                    entry = dict(node=fields[0], mem_node=fields[2])
            elif i == 1:
                if len(fields) == 4 and fields[1:3] == ['pages', 'free']:
                    entry['free_mem'] = int(fields[3])
                else:
                    entry['unexpected'] = line
            elif i == 2:
                if len(fields) == 3 and fields[1] == 'min':
                    entry['min_mem'] = int(fields[2])
                else:
                    unexpected = entry.get('unexpected', '')
                    entry['unexpected'] = unexpected + line
                yield entry
                entry = None
            if i is not None:
                i += 1


class LogWriter(object):
    def __init__(self, log):
        self.log = log
        self.writer = None

    def write(self, time, entries):
        row = dict(time=time.strftime('%Y-%m-%d %H:%M:%S'))
        prefixes = []
        for entry in entries:
            prefix = entry['node'] + '_' + entry['mem_node'] + '_'
            prefixes.append(prefix)
            row[prefix + 'free'] = self.format(entry.get('free_mem'))
            row[prefix + 'min'] = self.format(entry.get('min_mem'))
            row[prefix + 'unexpected'] = entry.get('unexpected')
        if self.writer is None:
            prefixes.sort()
            columns = ['time']
            columns += [prefix + suffix
                        for prefix in prefixes
                        for suffix in ('min', 'free')]
            columns += [prefix + 'unexpected' for prefix in prefixes]
            self.writer = DictWriter(self.log, columns, lineterminator=os.linesep)
            self.writer.writeheader()
        self.writer.writerow(row)
        self.log.flush()

    def format(self, pages):
        if pages is None:
            return None
        return '{:.02f}'.format(pages*4.0/1024/1024)


def main():
    args = parse_args()
    writer = LogWriter(args.log)
    while True:
        zone_info = check_output(['bpsh', '-1', 'cat', '/proc/zoneinfo'])
        lines = ['head: ' + line for line in zone_info.splitlines()]
        zone_info = check_output(['bpsh', '-sap', 'cat', '/proc/zoneinfo'])
        lines += zone_info.splitlines()
        entries = ZoneInfoScanner(lines)
        writer.write(datetime.now(), entries)

        sleep(args.delay)

if __name__ == '__main__':
    main()
