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
    
    <testcase
        classname="some.package.SomeTest"
        name="test_some_feature"
        time="6.04192" />
    <testcase
        classname="some.package.SomeOtherTest"
        name="test_other_feature"
        time="1" />
    <testcase
        classname="other.package.AnotherTest"
        name="test_stuff"
        time="3" />
</testsuite>
""")
        
    def test_summary(self):
        expected_description = '2 out of 3 tests took 9s out of 10s'
        
        report = SlowTestReport().load(self.report_file, 2)
        
        self.assertSequenceEqual(expected_description, report.description)

    def test_slowest(self):
        expected_description = '6s for some.package.SomeTest.test_some_feature'
        
        report = SlowTestReport().load(self.report_file, 2)
        test = report.tests[0]
        
        self.assertSequenceEqual(expected_description, test.description)
