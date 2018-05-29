import argparse
from xml.etree import ElementTree
from operator import attrgetter
import bisect
import glob

# To track progress, here is how we started on 25 May 2016, against SQLite:
# 10 out of 1042 tests took 125s out of 692s
# 15s for sandbox.BadRunTests.test_code_bad_execution
# 15s for method.NonReusableMethodTests.test_execute_does_not_reuse
# 14s for archive.RunStepReuseFailedExecRecordTests.test_reuse_failed_ER_can_have_missing_outputs
# 14s for archive.StateMachineActualExecutionTests.test_runcomponent_unsuccessful_failed_integrity_check_during_recovery
# 13s for sandbox.RawTests.test_execute_pipeline_raw_twice
# 13s for archive.StateMachineActualExecutionTests.test_runcomponent_unsuccessful_failed_invoked_log
# 12s for sandbox.ExecuteTests.test_filling_in_execrecord_with_incomplete_content_check
# 11s for sandbox.FindDatasetTests.test_find_dataset_subpipeline_input_and_intermediate
# 9s for sandbox.ExecuteTests.test_pipeline_execute_C_twostep_pipeline_with_subpipeline
# 9s for sandbox.BadRunTests.test_method_fails
# Slowest test classes:
# 58.146 TEST-librarian.tests.ExecRecordTests-20160525154303.xml
# 53.895 TEST-sandbox.tests.ExecuteTests-20160525154303.xml
# 45.881 TEST-method.tests.MethodTests-20160525154303.xml
# 34.804 TEST-archive.tests.StateMachineActualExecutionTests-20160525154303.xml
# 30.56 TEST-metadata.tests_CustomConstraint.CustomConstraintTestsWithExecution-20160525154303.xml
# 26.195 TEST-metadata.tests.DatatypeTests-20160525154303.xml
# 24.387 TEST-sandbox.tests_rm.FindDatasetTests-20160525154303.xml
# 23.7 TEST-sandbox.tests_rm.BadRunTests-20160525154303.xml
# 22.201 TEST-method.tests.NonReusableMethodTests-20160525154303.xml
# 20.715 TEST-sandbox.tests_rm.RawTests-20160525154303.xml


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
        path.pop(1)  # remove the 'tests' module name
        path.append(testcase.attrib['name'])
        self.description = '{:.0f}s for {}'.format(self.time, '.'.join(path))

    def __cmp__(self, other):
        # return -cmp(self.time, other.time)  # longer time sorts earlier
        return -self.time.__cmp__(other.time)  # longer time sorts earlier


def main():
    args = parseOptions()
    report_files = glob.glob1(args.folder, 'TEST-*.xml')
    summary = SlowTestReport().load(report_files, args.count)

    print(summary.description)
    for test in summary.tests:
        print(test.description)

    print('Slowest test classes:')
    for time, file in summary.files[0:10]:
        print(-time, file)


if __name__ == '__main__':
    main()
