import os
from unittest.case import TestCase

from django.utils import timezone

from mock import PropertyMock, Mock, patch

from kive.mock_setup import mock_relations, mocked_relations  # Import before any Django models
from constants import datatypes, runcomponentstates
from datachecking.models import BadData, CellError, ContentCheckLog
from kive.tests import dummy_file
from librarian.models import Dataset, ExecRecord, ExecRecordOut
from metadata.models import Datatype, CompoundDatatypeMember
from archive.models import RunStep, ExecLog


class DatasetMockTests(TestCase):
    def test_rows_with_no_errors(self):
        data_file = dummy_file("""\
name,count
Bob,20
Dave,40
""")
        expected_rows = [[('Bob', []), ('20', [])],
                         [('Dave', []), ('40', [])]]

        with mock_relations(Dataset, ContentCheckLog):
            dataset = Dataset()
            dataset.get_open_file_handle = lambda: data_file
            expected_check = dataset.content_checks.create()
            type(expected_check).baddata = PropertyMock(side_effect=BadData.DoesNotExist)

            rows = list(dataset.rows(data_check=True))

            self.assertEqual(expected_rows, rows)

    def test_rows_insert_at(self):
        data_file = dummy_file("""\
name,count
Bob,20
Dave,40
""")
        expected_rows = [[('', []), ('', []), ('Bob', []), ('20', [])],
                         [('', []), ('', []), ('Dave', []), ('40', [])]]

        with mock_relations(Dataset, ContentCheckLog):
            dataset = Dataset()
            dataset.get_open_file_handle = lambda: data_file
            expected_check = dataset.content_checks.create()
            type(expected_check).baddata = PropertyMock(side_effect=BadData.DoesNotExist)

            rows = list(dataset.rows(data_check=True, insert_at=[0, 1]))

            self.assertEqual(expected_rows, rows)

    def test_rows_with_errors(self):
        data_file = dummy_file("""\
name,count
Bob,tw3nty
Dave,40
Tom,15
""")
        bad_row, bad_column = 1, 2
        expected_rows = [[('Bob', []), ('tw3nty', [u'Was not integer'])],
                         [('Dave', []), ('40', [])],
                         [('Tom', []), ('15', [])]]

        with mock_relations(Dataset, ContentCheckLog):
            int_datatype = Datatype(id=datatypes.INT_PK)
            count_column = CompoundDatatypeMember(column_idx=bad_column,
                                                  datatype=int_datatype)
            cell_error = CellError(column=count_column, row_num=bad_row)
            dataset = Dataset()
            dataset.get_open_file_handle = lambda: data_file
            expected_check = dataset.content_checks.create()
            ContentCheckLog.baddata = PropertyMock()
            expected_check.baddata.cell_errors.order_by.return_value = [cell_error]

            rows = list(dataset.rows(data_check=True))

            self.assertEqual(expected_rows, rows)

    def test_rows_with_limit(self):
        data_file = dummy_file("""\
name,count
Bob,tw3nty
Dave,40
Tom,15
""")
        bad_row, bad_column = 1, 2
        expected_rows = [[('Bob', []), ('tw3nty', [u'Was not integer'])],
                         [('Dave', []), ('40', [])]]

        with mock_relations(Dataset, ContentCheckLog):
            int_datatype = Datatype(id=datatypes.INT_PK)
            count_column = CompoundDatatypeMember(column_idx=bad_column,
                                                  datatype=int_datatype)
            cell_error = CellError(column=count_column, row_num=bad_row)
            dataset = Dataset()
            dataset.get_open_file_handle = lambda: data_file
            expected_check = dataset.content_checks.create()
            ContentCheckLog.baddata = PropertyMock()
            expected_check.baddata.cell_errors.order_by.return_value.filter.return_value = [cell_error]

            rows = list(dataset.rows(data_check=True, limit=2))

            self.assertEqual(expected_rows, rows)

    def test_rows_with_error_after_limit(self):
        data_file = dummy_file("""\
name,count
Bob,20
Dave,40
Tom,15
Jim,th1rty
""")
        bad_row, bad_column = 4, 2
        count_column_id = 42
        name_column_id = 99
        expected_rows = [[('Bob', []), ('20', [])],
                         [('Dave', []), ('40', [])]]
        expected_extra_errors = [
            (bad_row, [('Jim', []), ('th1rty', [u'Was not integer'])])]

        with mock_relations(Dataset, ContentCheckLog):
            mock_structure = Mock(name='Dataset.structure')
            Dataset.structure = mock_structure
            int_datatype = Datatype(id=datatypes.INT_PK)
            count_column = CompoundDatatypeMember(id=count_column_id,
                                                  column_idx=bad_column,
                                                  datatype=int_datatype)
            str_datatype = Datatype(id=datatypes.STR_PK)
            name_column = CompoundDatatypeMember(id=name_column_id,
                                                 datatype=str_datatype)
            compound_datatype = mock_structure.compounddatatype
            compound_datatype.members.all.return_value = [count_column, name_column]
            extra_cell_errors = [{'column_id': count_column_id,
                                  'row_num__min': bad_row}]
            dataset = Dataset()
            dataset.get_open_file_handle = lambda: data_file
            expected_check = dataset.content_checks.create()
            ContentCheckLog.baddata = PropertyMock()
            expected_check.baddata.cell_errors.order_by.return_value.filter.return_value = []
            expected_check.baddata.cell_errors.values.return_value.\
                annotate.return_value.order_by.return_value = extra_cell_errors

            extra_errors = []
            rows = list(dataset.rows(data_check=True,
                                     limit=2,
                                     extra_errors=extra_errors))

            self.assertEqual(expected_rows, rows)
            self.assertEqual(expected_extra_errors, extra_errors)

    def test_rows_with_no_data_check(self):
        data_file = dummy_file("""\
name,count
Bob,tw3nty
Dave,40
""")
        expected_rows = [['Bob', 'tw3nty'],
                         ['Dave', '40']]

        with mock_relations(Dataset):
            dataset = Dataset()
            dataset.get_open_file_handle = lambda: data_file

            rows = list(dataset.rows(data_check=False))

            self.assertEqual(expected_rows, rows)

    def test_rows_with_no_content_check(self):
        data_file = dummy_file("""\
name,count
Bob,tw3nty
Dave,40
""")
        expected_rows = [[('Bob', []), ('tw3nty', [])],
                         [('Dave', []), ('40', [])]]

        with mock_relations(Dataset):
            dataset = Dataset()
            dataset.get_open_file_handle = lambda: data_file
            Dataset.content_checks.first.return_value = None

            rows = list(dataset.rows(data_check=True))

            self.assertEqual(expected_rows, rows)

    def test_check_file_contents(self):
        file_path = os.devnull
        with mock_relations(Dataset, BadData):
            mock_structure = Mock(name='Dataset.structure')
            Dataset.structure = mock_structure
            expected_bad_data = BadData.objects.create.return_value  # @UndefinedVariable
            Dataset.content_checks = Mock(name='Dataset.content_checks')
            expected_check = Dataset.content_checks.create.return_value  # @UndefinedVariable
            expected_bad_row = 42
            expected_bad_column = 2
            count_column = CompoundDatatypeMember()
            compound_datatype = mock_structure.compounddatatype
            compound_datatype.members.get.return_value = count_column
            compound_datatype.summarize_CSV.return_value = {
                u'num_rows': expected_bad_row * 2,
                u'header': ['name', 'count'],
                u'failing_cells': {(expected_bad_row,
                                    expected_bad_column): [u'Was not integer']}
            }
            dataset = Dataset()

            check = dataset.check_file_contents(file_path_to_check=file_path,
                                                summary_path=None,
                                                min_row=None,
                                                max_row=None,
                                                execlog=None,
                                                checking_user=None,
                                                notify_all=False)

            self.assertIs(expected_check, check)
            compound_datatype.members.get.assert_called_once_with(
                column_idx=expected_bad_column)
            expected_bad_data.cell_errors.create.assert_called_once_with(
                column=count_column,
                row_num=expected_bad_row)


@mocked_relations(ExecRecord)
class ExecRecordQuarantineDecontaminateMockTests(TestCase):
    """
    Tests of the quarantine/decontamination functionality of ExecRecord.
    """
    def test_quarantine_runcomponents(self):
        """
        Quarantines all Successful RunComponents using this ExecRecord.
        """
        generating_el = ExecLog()
        er = ExecRecord(generator=generating_el)
        rs1 = RunStep(execrecord=er, _runcomponentstate_id=runcomponentstates.SUCCESSFUL_PK)
        rs2 = RunStep(execrecord=er, _runcomponentstate_id=runcomponentstates.RUNNING_PK)
        rs3 = RunStep(execrecord=er, _runcomponentstate_id=runcomponentstates.SUCCESSFUL_PK)

        er.used_by_components.add(rs1, rs2, rs3)

        rs1.quarantine = Mock()
        rs2.quarantine = Mock()
        rs3.quarantine = Mock()

        er.quarantine_runcomponents()
        rs1.quarantine.assert_called_once_with(save=True, recurse_upward=True)
        rs2.quarantine.assert_not_called()
        rs3.quarantine.assert_called_once_with(save=True, recurse_upward=True)

    def test_decontaminate_runcomponents(self):
        """
        Decontaminates all Quarantined RunComponents using this ExecRecord.
        """
        generating_el = ExecLog()
        er = ExecRecord(generator=generating_el)
        rs1 = RunStep(execrecord=er, _runcomponentstate_id=runcomponentstates.QUARANTINED_PK)
        rs2 = RunStep(execrecord=er, _runcomponentstate_id=runcomponentstates.RUNNING_PK)
        rs3 = RunStep(execrecord=er, _runcomponentstate_id=runcomponentstates.QUARANTINED_PK)

        er.used_by_components.add(rs1, rs2, rs3)

        rs1.decontaminate = Mock()
        rs2.decontaminate = Mock()
        rs3.decontaminate = Mock()

        er.decontaminate_runcomponents()
        rs1.decontaminate.assert_called_once_with(save=True, recurse_upward=True)
        rs2.decontaminate.assert_not_called()
        rs3.decontaminate.assert_called_once_with(save=True, recurse_upward=True)

    @patch('archive.models.ExecLog.is_successful')
    @patch('librarian.models.ExecRecord.decontaminate_runcomponents')
    def test_attempt_decontamination(self, mock_decontaminate, mock_is_successful):
        """
        ExecRecord correctly decontaminates all RunComponents using it.
        """
        generating_el = ExecLog()
        er = ExecRecord(generator=generating_el)

        ds1 = Dataset()
        ds2 = Dataset()
        ds3 = Dataset()
        ero1 = ExecRecordOut(execrecord=er, dataset=ds1)
        ero2 = ExecRecordOut(execrecord=er, dataset=ds2)
        ero3 = ExecRecordOut(execrecord=er, dataset=ds3)
        ero1.is_OK = Mock(return_value=True)
        ero2.is_OK = Mock(return_value=True)
        ero3.is_OK = Mock(return_value=True)
        er.execrecordouts.add(ero1, ero2, ero3)

        rs1 = RunStep(execrecord=er, _runcomponentstate_id=runcomponentstates.QUARANTINED_PK,
                      end_time=timezone.now())
        rs1.log = ExecLog(record=rs1)
        rs2 = RunStep(execrecord=er, _runcomponentstate_id=runcomponentstates.QUARANTINED_PK,
                      end_time=timezone.now())
        rs2.log = ExecLog(record=rs2)
        rs3 = RunStep(execrecord=er, _runcomponentstate_id=runcomponentstates.SUCCESSFUL_PK,
                      end_time=timezone.now())
        rs3.log = ExecLog(record=rs3)
        mock_is_successful.return_value = True
        er.used_by_components.add(rs1, rs2, rs3)

        er.attempt_decontamination(ds1)
        ero1.is_OK.assert_not_called()
        ero2.is_OK.assert_called_once_with()
        ero3.is_OK.assert_called_once_with()
        mock_is_successful.assert_called_once_with()
        mock_decontaminate.assert_called_once_with()

    @patch('archive.models.ExecLog.is_successful')
    @patch('librarian.models.ExecRecord.decontaminate_runcomponents')
    def test_attempt_decontamination_still_has_bad_outputs(self, mock_decontaminate, mock_is_successful):
        """
        Attempt bails if another output is still bad.
        """
        generating_el = ExecLog()
        er = ExecRecord(generator=generating_el)

        ds1 = Dataset()
        ds2 = Dataset()
        ds3 = Dataset()
        ero1 = ExecRecordOut(execrecord=er, dataset=ds1)
        ero2 = ExecRecordOut(execrecord=er, dataset=ds2)
        ero3 = ExecRecordOut(execrecord=er, dataset=ds3)
        ero1.is_OK = Mock(return_value=True)
        ero2.is_OK = Mock(return_value=False)
        ero3.is_OK = Mock(return_value=True)
        er.execrecordouts.add(ero1, ero2, ero3)

        rs1 = RunStep(execrecord=er, _runcomponentstate_id=runcomponentstates.QUARANTINED_PK,
                      end_time=timezone.now())
        rs1.log = ExecLog(record=rs1)
        rs2 = RunStep(execrecord=er, _runcomponentstate_id=runcomponentstates.QUARANTINED_PK,
                      end_time=timezone.now())
        rs2.log = ExecLog(record=rs2)
        rs3 = RunStep(execrecord=er, _runcomponentstate_id=runcomponentstates.SUCCESSFUL_PK,
                      end_time=timezone.now())
        rs3.log = ExecLog(record=rs3)
        er.used_by_components.add(rs1, rs2, rs3)

        er.attempt_decontamination(ds1)
        mock_is_successful.assert_not_called()
        mock_decontaminate.assert_not_called()

    def test_attempt_decontamination_last_log_unsuccessful(self):
        """
        Attempt bails if the last using component is not successful.
        """
        generating_el = ExecLog()
        er = ExecRecord(generator=generating_el)

        ds1 = Dataset()
        ds2 = Dataset()
        ds3 = Dataset()
        ero1 = ExecRecordOut(execrecord=er, dataset=ds1)
        ero2 = ExecRecordOut(execrecord=er, dataset=ds2)
        ero3 = ExecRecordOut(execrecord=er, dataset=ds3)
        ero1.is_OK = Mock(return_value=True)
        ero2.is_OK = Mock(return_value=True)
        ero3.is_OK = Mock(return_value=True)
        er.execrecordouts.add(ero1, ero2, ero3)

        rs1 = RunStep(execrecord=er, _runcomponentstate_id=runcomponentstates.QUARANTINED_PK,
                      end_time=timezone.now())
        rs1.log = ExecLog()
        rs2 = RunStep(execrecord=er, _runcomponentstate_id=runcomponentstates.QUARANTINED_PK,
                      end_time=timezone.now())
        rs2.log = ExecLog()
        rs3 = RunStep(execrecord=er, _runcomponentstate_id=runcomponentstates.SUCCESSFUL_PK,
                      end_time=timezone.now())
        rs3.log = ExecLog()
        rs3.log.is_successful = Mock(return_value=False)
        er.used_by_components.add(rs1, rs2, rs3)

        er.decontaminate_runcomponents = Mock()

        er.attempt_decontamination(ds1)
        rs3.log.is_successful.assert_not_called()
        er.decontaminate_runcomponents.assert_not_called()
