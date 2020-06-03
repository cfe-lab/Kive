import StringIO
import unittest

from slow_test_report import SlowTestReport

class SlowTestReportTest(unittest.TestCase):
    def setUp(self):
        self.report_file = StringIO.StringIO("""\
<testsuite
    errors="0"
    failures="0"
    name="Django Project Tests"
    skips="0"
    tests="3"
    time="10.042">

    <properties/>
    <testcase
        classname="app1.tests.SomeTest"
        name="test_some_feature"
        time="6.04192" />
    <testcase
        classname="app1.tests.SomeOtherTest"
        name="test_other_feature"
        time="1" />
    <testcase
        classname="app2.tests.AnotherTest"
        name="test_stuff"
        time="3" />
</testsuite>
""")
        self.other_report_file = StringIO.StringIO("""\
<testsuite
    errors="0"
    failures="0"
    name="Django Project Other Tests"
    skips="0"
    tests="1"
    time="5">

    <testcase
        classname="app3.tests.WholeDifferentTest"
        name="test_crazy_feature"
        time="5" />
</testsuite>
""")

    def test_summary(self):
        expected_description = '2 out of 3 tests took 9s out of 10s'

        report = SlowTestReport().load([self.report_file], 2)

        self.assertSequenceEqual(expected_description, report.description)

    def test_slowest(self):
        expected_description = '6s for app1.SomeTest.test_some_feature'

        report = SlowTestReport().load([self.report_file], 2)
        test = report.tests[0]

        self.assertSequenceEqual(expected_description, test.description)

    def test_multiple_files(self):
        expected_description = '2 out of 4 tests took 11s out of 15s'

        report = SlowTestReport().load([self.report_file,
                                        self.other_report_file],
                                       2)

        self.assertSequenceEqual(expected_description, report.description)
