import os
from unittest.case import TestCase

from mock import PropertyMock, Mock

from kive.mock_setup import mock_relations  # Import before any Django models
from constants import datatypes
from datachecking.models import BadData, CellError
from kive.tests import dummy_file
from librarian.models import Dataset
from metadata.models import Datatype, CompoundDatatypeMember


class DatasetMockTests(TestCase):
    def test_rows_with_no_errors(self):
        data_file = dummy_file("""\
name,count
Bob,20
Dave,40
""")
        expected_rows = [[('Bob', []), ('20', [])],
                         [('Dave', []), ('40', [])]]

        with mock_relations(Dataset):
            dataset = Dataset()
            dataset.get_open_file_handle = lambda: data_file
            expected_check = Dataset.content_checks.first.return_value  # @UndefinedVariable
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

        with mock_relations(Dataset):
            dataset = Dataset()
            dataset.get_open_file_handle = lambda: data_file
            expected_check = Dataset.content_checks.first.return_value  # @UndefinedVariable
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

        with mock_relations(Dataset):
            int_datatype = Datatype(id=datatypes.INT_PK)
            count_column = CompoundDatatypeMember(column_idx=bad_column,
                                                  datatype=int_datatype)
            cell_error = CellError(column=count_column, row_num=bad_row)
            dataset = Dataset()
            dataset.get_open_file_handle = lambda: data_file
            expected_check = Dataset.content_checks.first.return_value  # @UndefinedVariable
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

        with mock_relations(Dataset):
            int_datatype = Datatype(id=datatypes.INT_PK)
            count_column = CompoundDatatypeMember(column_idx=bad_column,
                                                  datatype=int_datatype)
            cell_error = CellError(column=count_column, row_num=bad_row)
            dataset = Dataset()
            dataset.get_open_file_handle = lambda: data_file
            expected_check = Dataset.content_checks.first.return_value  # @UndefinedVariable
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

        with mock_relations(Dataset):
            int_datatype = Datatype(id=datatypes.INT_PK)
            count_column = CompoundDatatypeMember(id=count_column_id,
                                                  column_idx=bad_column,
                                                  datatype=int_datatype)
            str_datatype = Datatype(id=datatypes.STR_PK)
            name_column = CompoundDatatypeMember(id=name_column_id,
                                                 datatype=str_datatype)
            compound_datatype = Dataset.structure.compounddatatype  # @UndefinedVariable
            compound_datatype.members.all.return_value = [count_column, name_column]
            extra_cell_errors = [{'column_id': count_column_id,
                                  'row_num__min': bad_row}]
            dataset = Dataset()
            dataset.get_open_file_handle = lambda: data_file
            expected_check = Dataset.content_checks.first.return_value  # @UndefinedVariable
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
            expected_bad_data = BadData.objects.create.return_value  # @UndefinedVariable
            Dataset.content_checks = Mock(name='Dataset.content_checks')
            expected_check = Dataset.content_checks.create.return_value  # @UndefinedVariable
            expected_bad_row = 42
            expected_bad_column = 2
            count_column = CompoundDatatypeMember()
            compound_datatype = Dataset.structure.compounddatatype  # @UndefinedVariable
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
                                                checking_user=None)

            self.assertIs(expected_check, check)
            compound_datatype.members.get.assert_called_once_with(
                column_idx=expected_bad_column)
            expected_bad_data.cell_errors.create.assert_called_once_with(
                column=count_column,
                row_num=expected_bad_row)
