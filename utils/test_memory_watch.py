from __future__ import unicode_literals
from io import StringIO, BytesIO
from unittest import TestCase

from datetime import datetime

from memory_watch import ZoneInfoScanner, LogWriter


class ZoneInfoScannerTest(TestCase):
    def test_simple(self):
        zone_info = StringIO("""\
  2: Node 0, zone   Normal
  2:   pages free     1363343
  2:         min      9891
""")
        expected_entries = [dict(node='2',
                                 mem_node='0',
                                 free_mem=1363343,
                                 min_mem=9891)]
        scanner = ZoneInfoScanner(zone_info)

        entries = list(scanner)

        self.assertEqual(expected_entries, entries)

    def test_two_nodes(self):
        zone_info = StringIO("""\
  2: Node 0, zone   Normal
  2:   pages free     1363343
  2:         min      9891
  3: Node 0, zone   Normal
  3:   pages free     2000000
  3:         min      9999
""")
        expected_entries = [dict(node='2',
                                 mem_node='0',
                                 free_mem=1363343,
                                 min_mem=9891),
                            dict(node='3',
                                 mem_node='0',
                                 free_mem=2000000,
                                 min_mem=9999)]
        scanner = ZoneInfoScanner(zone_info)

        entries = list(scanner)

        self.assertEqual(expected_entries, entries)

    def test_skipping_lines(self):
        zone_info = StringIO("""\
  2: some garbage
  2: Zode 0, zone   Normal
  2: Node 0, zone   Zormal
  2: Node 0, zone   Normal
  2:   pages free     1363343
  2:         min      9891
  2: more garbage
  3: Node 0, zone   Normal
  3:   pages free     2000000
  3:         min      9999
""")
        expected_entries = [dict(node='2',
                                 mem_node='0',
                                 free_mem=1363343,
                                 min_mem=9891),
                            dict(node='3',
                                 mem_node='0',
                                 free_mem=2000000,
                                 min_mem=9999)]
        scanner = ZoneInfoScanner(zone_info)

        entries = list(scanner)

        self.assertEqual(expected_entries, entries)

    def test_unexpected_lines(self):
        zone_info = StringIO("""\
  2: Node 0, zone   Normal
  2:  xpages free     1363343
  2:        ymin      9891
""")
        bad_lines = "  2:  xpages free     1363343\n  2:        ymin      9891\n"
        expected_entries = [dict(node='2',
                                 mem_node='0',
                                 unexpected=bad_lines)]
        scanner = ZoneInfoScanner(zone_info)

        entries = list(scanner)

        self.assertEqual(expected_entries, entries)


class LogWriterTest(TestCase):
    def test_simple(self):
        entries = [dict(node='2',
                        mem_node='0',
                        free_mem=1363343,
                        min_mem=9891),
                   dict(node='3',
                        mem_node='0',
                        free_mem=2000000,
                        min_mem=9999)]
        time = datetime(2017, 10, 15, 19, 30, 1)
        report = BytesIO()
        expected_report = """\
time,2_0_min,2_0_free,3_0_min,3_0_free,2_0_unexpected,3_0_unexpected
2017-10-15 19:30:01,9891,1363343,9999,2000000,,
"""
        writer = LogWriter(report)

        writer.write(time, entries)

        self.assertEqual(expected_report, report.getvalue().decode('UTF-8'))

    def test_multiple_times(self):
        entries1 = [dict(node='2',
                         mem_node='0',
                         free_mem=1363343,
                         min_mem=9891),
                    dict(node='3',
                         mem_node='0',
                         free_mem=2000000,
                         min_mem=9999)]
        entries2 = [dict(node='2',
                         mem_node='0',
                         free_mem=1363342,
                         min_mem=9891),
                    dict(node='3',
                         mem_node='0',
                         free_mem=1999999,
                         min_mem=9999)]
        time1 = datetime(2017, 10, 15, 19, 30, 1)
        time2 = datetime(2017, 10, 15, 19, 31, 1)
        report = BytesIO()
        expected_report = """\
time,2_0_min,2_0_free,3_0_min,3_0_free,2_0_unexpected,3_0_unexpected
2017-10-15 19:30:01,9891,1363343,9999,2000000,,
2017-10-15 19:31:01,9891,1363342,9999,1999999,,
"""
        writer = LogWriter(report)

        writer.write(time1, entries1)
        writer.write(time2, entries2)

        self.assertEqual(expected_report, report.getvalue().decode('UTF-8'))

    def test_node_missing(self):
        entries1 = [dict(node='2',
                         mem_node='0',
                         free_mem=1363343,
                         min_mem=9891),
                    dict(node='3',
                         mem_node='0',
                         free_mem=2000000,
                         min_mem=9999)]
        entries2 = [dict(node='2',
                         mem_node='0',
                         free_mem=1363342,
                         min_mem=9891)]
        time1 = datetime(2017, 10, 15, 19, 30, 1)
        time2 = datetime(2017, 10, 15, 19, 31, 1)
        report = BytesIO()
        expected_report = """\
time,2_0_min,2_0_free,3_0_min,3_0_free,2_0_unexpected,3_0_unexpected
2017-10-15 19:30:01,9891,1363343,9999,2000000,,
2017-10-15 19:31:01,9891,1363342,,,,
"""
        writer = LogWriter(report)

        writer.write(time1, entries1)
        writer.write(time2, entries2)

        self.assertEqual(expected_report, report.getvalue().decode('UTF-8'))

    def test_unexpected(self):
        entries = [dict(node='2',
                        mem_node='0',
                        free_mem=1363343,
                        unexpected='bogus line\n'),
                   dict(node='3',
                        mem_node='0',
                        unexpected='bogus line 1\nbogus line 2\n')]
        time = datetime(2017, 10, 15, 19, 30, 1)
        report = BytesIO()
        expected_report = """\
time,2_0_min,2_0_free,3_0_min,3_0_free,2_0_unexpected,3_0_unexpected
2017-10-15 19:30:01,,1363343,,,"bogus line
","bogus line 1
bogus line 2
"
"""
        writer = LogWriter(report)

        writer.write(time, entries)

        self.assertEqual(expected_report, report.getvalue().decode('UTF-8'))
