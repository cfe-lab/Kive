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
                                 zone='Normal',
                                 free_mem=1363343,
                                 min_mem=9891)]
        scanner = ZoneInfoScanner(zone_info)

        entries = list(scanner)

        self.assertEqual(expected_entries, entries)

    def test_available(self):
        zone_info = StringIO("""\
  2: Node 0, zone   Normal
  2:   pages free     1363343
  2:         min      9891
  2: MemAvailable:  8000000 kB
""")
        expected_entries = [dict(node='2',
                                 mem_node='0',
                                 zone='Normal',
                                 free_mem=1363343,
                                 min_mem=9891),
                            dict(node='2',
                                 mem_node='avail',
                                 free_mem=2000000)]
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
                                 zone='Normal',
                                 free_mem=1363343,
                                 min_mem=9891),
                            dict(node='3',
                                 mem_node='0',
                                 zone='Normal',
                                 free_mem=2000000,
                                 min_mem=9999)]
        scanner = ZoneInfoScanner(zone_info)

        entries = list(scanner)

        self.assertEqual(expected_entries, entries)

    def test_skipping_lines(self):
        zone_info = StringIO("""\
  2: some garbage
  2: Zode 0, zone   Normal
  2: Node 0, bone   Normal
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
                                 zone='Normal',
                                 free_mem=1363343,
                                 min_mem=9891),
                            dict(node='3',
                                 mem_node='0',
                                 zone='Normal',
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
                                 zone='Normal',
                                 unexpected=bad_lines)]
        scanner = ZoneInfoScanner(zone_info)

        entries = list(scanner)

        self.assertEqual(expected_entries, entries)


class LogWriterTest(TestCase):
    def test_simple(self):
        entries = [dict(node='2',
                        mem_node='0',
                        zone='Normal',
                        free_mem=1024*1024,
                        min_mem=10*1024),
                   dict(node='3',
                        mem_node='0',
                        zone='Normal',
                        free_mem=2*1024*1024,
                        min_mem=128*1024)]
        time = datetime(2017, 10, 15, 19, 30, 1)
        report = BytesIO()
        expected_report = """\
time,2_0_0_min,2_0_0_free,3_0_0_min,3_0_0_free,2_0_0_unexpected,3_0_0_unexpected
2017-10-15 19:30:01,0.04,4.00,0.50,8.00,,
"""
        writer = LogWriter(report)

        writer.write(time, entries)

        self.assertEqual(expected_report, report.getvalue().decode('UTF-8'))

    def test_available(self):
        entries = [dict(node='2',
                        mem_node='0',
                        zone='Normal',
                        free_mem=1024*1024,
                        min_mem=10*1024),
                   dict(node='2',
                        mem_node='avail',
                        free_mem=2*1024*1024)]
        time = datetime(2017, 10, 15, 19, 30, 1)
        report = BytesIO()
        expected_report = """\
time,2_0_0_min,2_0_0_free,2_avail_1_min,2_avail_1_free,2_0_0_unexpected,2_avail_1_unexpected
2017-10-15 19:30:01,0.04,4.00,,8.00,,
"""
        writer = LogWriter(report)

        writer.write(time, entries)

        self.assertEqual(expected_report, report.getvalue().decode('UTF-8'))

    def test_multiple_times(self):
        entries1 = [dict(node='2',
                         mem_node='0',
                         zone='Normal',
                         free_mem=262144,  # 1.00GB
                         min_mem=2621),    # 0.01GB
                    dict(node='3',
                         mem_node='0',
                         zone='Normal',
                         free_mem=262144,  # 1.00GB
                         min_mem=2621)]    # 0.01GB
        entries2 = [dict(node='2',
                         mem_node='0',
                         zone='Normal',
                         free_mem=267387,  # 1.02GB
                         min_mem=5242),    # 0.02GB
                    dict(node='3',
                         mem_node='0',
                         zone='Normal',
                         free_mem=270008,  # 1.03GB
                         min_mem=7864)]    # 0.03GB
        time1 = datetime(2017, 10, 15, 19, 30, 1)
        time2 = datetime(2017, 10, 15, 19, 31, 1)
        report = BytesIO()
        expected_report = """\
time,2_0_0_min,2_0_0_free,3_0_0_min,3_0_0_free,2_0_0_unexpected,3_0_0_unexpected
2017-10-15 19:30:01,0.01,1.00,0.01,1.00,,
2017-10-15 19:31:01,0.02,1.02,0.03,1.03,,
"""
        writer = LogWriter(report)

        writer.write(time1, entries1)
        writer.write(time2, entries2)

        self.assertEqual(expected_report, report.getvalue().decode('UTF-8'))

    def test_multiple_zones(self):
        entries1 = [dict(node='2',
                         mem_node='0',
                         zone='DMA32',
                         free_mem=262144,  # 1.00GB
                         min_mem=2621),  # 0.01GB
                    dict(node='2',
                         mem_node='0',
                         zone='Normal',
                         free_mem=262144,  # 1.00GB
                         min_mem=2621)]  # 0.01GB
        time1 = datetime(2017, 10, 15, 19, 30, 1)
        report = BytesIO()
        expected_report = """\
time,2_0_0_min,2_0_0_free,2_0_1_min,2_0_1_free,2_0_0_unexpected,2_0_1_unexpected
2017-10-15 19:30:01,0.01,1.00,0.01,1.00,,
"""
        writer = LogWriter(report)

        writer.write(time1, entries1)

        self.assertEqual(expected_report, report.getvalue().decode('UTF-8'))

    def test_node_missing(self):
        entries1 = [dict(node='2',
                         mem_node='0',
                         zone='Normal',
                         free_mem=262144,
                         min_mem=2621),
                    dict(node='3',
                         mem_node='0',
                         zone='Normal',
                         free_mem=262144,
                         min_mem=2621)]
        entries2 = [dict(node='2',
                         mem_node='0',
                         zone='Normal',
                         free_mem=262144,
                         min_mem=2621)]
        time1 = datetime(2017, 10, 15, 19, 30, 1)
        time2 = datetime(2017, 10, 15, 19, 31, 1)
        report = BytesIO()
        expected_report = """\
time,2_0_0_min,2_0_0_free,3_0_0_min,3_0_0_free,2_0_0_unexpected,3_0_0_unexpected
2017-10-15 19:30:01,0.01,1.00,0.01,1.00,,
2017-10-15 19:31:01,0.01,1.00,,,,
"""
        writer = LogWriter(report)

        writer.write(time1, entries1)
        writer.write(time2, entries2)

        self.assertEqual(expected_report, report.getvalue().decode('UTF-8'))

    def test_unexpected(self):
        entries = [dict(node='2',
                        mem_node='0',
                        zone='Normal',
                        free_mem=262144,
                        unexpected='bogus line\n'),
                   dict(node='3',
                        mem_node='0',
                        zone='Normal',
                        unexpected='bogus line 1\nbogus line 2\n')]
        time = datetime(2017, 10, 15, 19, 30, 1)
        report = BytesIO()
        expected_report = """\
time,2_0_0_min,2_0_0_free,3_0_0_min,3_0_0_free,2_0_0_unexpected,3_0_0_unexpected
2017-10-15 19:30:01,,1.00,,,"bogus line
","bogus line 1
bogus line 2
"
"""
        writer = LogWriter(report)

        writer.write(time, entries)

        self.assertEqual(expected_report, report.getvalue().decode('UTF-8'))
