import argparse
from xml.etree import ElementTree
from operator import attrgetter
import bisect
import glob

# To track progress, here is how we started on 9 Oct 2015, against SQLite:
# 10 out of 1020 tests took 68s out of 657s
# 15s for sandbox.ExecuteTests.test_pipeline_execute_C_twostep_pipeline_with_subpipeline
# 8s for sandbox.ExecuteTests.test_pipeline_execute_B_twostep_pipeline_with_recycling
# 8s for sandbox.FindSDTests.test_find_symds_subpipeline_input_and_intermediate
# 7s for sandbox.ExecuteTests.test_pipeline_all_inputs_OK_nonraw
# 7s for sandbox.ExecuteTests.test_pipeline_all_inputs_OK_raw
# 7s for sandbox.ExecuteTests.test_pipeline_execute_A_simple_onestep_pipeline
# 4s for archive.IsCompleteSuccessfulExecutionTests.test_runcomponent_unsuccessful_failed_integrity_check_during_recovery
# 4s for sandbox.FindSDTests.test_find_symds_pipeline_input_and_intermediate_custom_wire
# 4s for method.NonReusableMethodTests.test_execute_does_not_reuse
# 4s for archive.RunTests.test_Run_clean_all_complete_RunOutputCables

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
        self.tests = []  # [Test()]
        self.files = []  # [(-time, file_name)]
        total_time = 0
        total_tests = 0

        for report_file in report_files:
            doc = ElementTree.parse(report_file)
            suite = doc.getroot()
            file_time = float(suite.attrib['time'])
            total_time += file_time
            total_tests += int(suite.attrib['tests'])
            bisect.insort(self.files, (-file_time, report_file))
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
    
    print 'Slowest test classes:'
    for time, file in summary.files[0:10]:
        print -time, file
        
if __name__ == '__main__':
    main()
