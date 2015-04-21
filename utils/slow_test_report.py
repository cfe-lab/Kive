import argparse
from xml.etree import ElementTree
from operator import attrgetter
import bisect
import glob

# To track progress, here is how we started on Dec 31, against SQLite:
# 10 out of 704 tests took 136s out of 499s
# 17s for archive.GetCoordinatesTests.test_get_coordinates_nested_roc
# 17s for archive.GetCoordinatesTests.test_get_coordinates_nested_rsic
# 17s for archive.GetCoordinatesTests.test_get_coordinates_nested_runstep
# 17s for archive.GetCoordinatesTests.test_get_coordinates_nested_runs
# 17s for archive.TopLevelRunTests.test_deep_nested_run
# 15s for sandbox.SandboxTests.test_pipeline_execute_C_twostep_pipeline_with_subpipeline
# 14s for sandbox.ExecuteTests.test_pipeline_execute_C_twostep_pipeline_with_subpipeline
# 8s for sandbox.SandboxTests.test_pipeline_execute_B_twostep_pipeline_with_recycling
# 7s for sandbox.ExecuteTests.test_pipeline_execute_B_twostep_pipeline_with_recycling
# 7s for sandbox.ExecuteTests.test_pipeline_all_inputs_OK_nonraw

def parseOptions():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f',
                        '--folder',
                        help='output folder with test report XML files to load',
                        default='.')
    parser.add_argument('-n',
                        '--count',
                        help='number of tests to report as the slowest ones',
                        type=int,
                        default=10)
    
    return parser.parse_args()

class SlowTestReport(object):
    def load(self, report_files, count):
        self.tests = []
        total_time = 0
        total_tests = 0

        for report_file in report_files:
            doc = ElementTree.parse(report_file)
            suite = doc.getroot()
            total_time += float(suite.attrib['time'])
            total_tests += int(suite.attrib['tests'])
            for testcase in suite.findall('testcase'):
                bisect.insort(self.tests, Test(testcase))
                if len(self.tests) > count:
                    self.tests.pop()
        time = sum(map(attrgetter('time'), self.tests))
        self.description = '{} out of {} tests took {:.0f}s out of {:.0f}s'.format(
            count,
            total_tests,
            time,
            total_time)
        return self

class Test(object):
    def __init__(self, testcase):
        self.time = float(testcase.attrib['time'])
        path = testcase.attrib['classname'].split('.')
        path.pop(1) # remove the 'tests' module name
        path.append(testcase.attrib['name'])
        self.description = '{:.0f}s for {}'.format(self.time, '.'.join(path))
    
    def __cmp__(self, other):
        return -cmp(self.time, other.time) # longer time sorts earlier

def main():
    args = parseOptions()
    report_files = glob.glob1(args.folder, 'TEST-*.xml')
    summary = SlowTestReport().load(report_files, args.count)
    
    print summary.description
    for test in summary.tests:
        print test.description
        
if __name__ == '__main__':
    main()
