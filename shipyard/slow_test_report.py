import argparse
from xml.etree import ElementTree
from operator import attrgetter
import bisect

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
                        '--file',
                        help='test report XML file to load',
                        type=argparse.FileType('rU'),
                        default='testreport.xml')
    parser.add_argument('-n',
                        '--count',
                        help='number of tests to report as the slowest ones',
                        type=int,
                        default=10)
    
    return parser.parse_args()

class SlowTestReport(object):
    def load(self, report_file, count):
        self.tests = []
        doc = ElementTree.parse(report_file)
        suite = doc.getroot()
        for testcase in suite:
            bisect.insort(self.tests, Test(testcase))
            if len(self.tests) > count:
                self.tests.pop()
        time = sum(map(attrgetter('time'), self.tests))
        self.description = '{} out of {} tests took {:.0f}s out of {:.0f}s'.format(
            count,
            suite.attrib['tests'],
            time,
            float(suite.attrib['time']))
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
    summary = SlowTestReport().load(args.file, args.count)
    
    print summary.description
    for test in summary.tests:
        print test.description
        
if __name__ == '__main__':
    main()
