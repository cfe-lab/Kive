from datetime import datetime
import os
from unittest.case import TestCase

from django.core.exceptions import ValidationError
from mock import PropertyMock, Mock

from kive.mock_setup import mocked_relations  # Import before any Django models
from django_mock_queries.query import MockSet
from constants import datatypes, runcomponentstates
from datachecking.models import BadData, CellError, ContentCheckLog
from kive.tests import dummy_file
from librarian.models import Dataset, ExecRecord, ExecRecordOut, ExecRecordIn, DatasetStructure
from metadata.models import Datatype, CompoundDatatypeMember, CompoundDatatype
from archive.models import RunStep, ExecLog, Run, RunOutputCable, RunSIC
from method.models import Method
from pipeline.models import PipelineOutputCable, PipelineStepInputCable, PipelineStep, Pipeline, PipelineCable, \
    CustomCableWire
from transformation.models import TransformationOutput, XputStructure, TransformationXput, TransformationInput, \
    Transformation


@mocked_relations(Dataset, ContentCheckLog, BadData)
class DatasetMockTests(TestCase):
    def test_rows_with_no_errors(self):
        data_file = dummy_file("""\
name,count
Bob,20
Dave,40
""")
        expected_rows = [[('Bob', []), ('20', [])],
                         [('Dave', []), ('40', [])]]

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

        dataset = Dataset()
        dataset.get_open_file_handle = lambda: data_file

        rows = list(dataset.rows(data_check=True))

        self.assertEqual(expected_rows, rows)

    def test_check_file_contents(self):
        file_path = os.devnull
        BadData.objects = Mock(name='BadData.objects')
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


# noinspection PyUnresolvedReferences
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

    def test_attempt_decontamination(self):
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
                      end_time=datetime(2000, 2, 14))
        rs1.log = ExecLog(record=rs1)
        rs2 = RunStep(execrecord=er, _runcomponentstate_id=runcomponentstates.QUARANTINED_PK,
                      end_time=datetime(2000, 2, 15))
        rs2.log = ExecLog(record=rs2)
        rs3 = RunStep(execrecord=er, _runcomponentstate_id=runcomponentstates.SUCCESSFUL_PK,
                      end_time=datetime(2000, 2, 16))
        rs3.log = ExecLog(record=rs3)
        rs3.log.is_successful = Mock(return_value=True)
        er.used_by_components.add(rs1, rs2, rs3)

        er.decontaminate_runcomponents = Mock()

        er.attempt_decontamination(ds1)
        ero1.is_OK.assert_not_called()
        ero2.is_OK.assert_called_once_with()
        ero3.is_OK.assert_called_once_with()
        rs3.log.is_successful.assert_called_once_with()
        er.decontaminate_runcomponents.assert_called_once_with()

    def test_attempt_decontamination_still_has_bad_outputs(self):
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
                      end_time=datetime(2000, 2, 14))
        rs1.log = ExecLog(record=rs1)
        rs2 = RunStep(execrecord=er, _runcomponentstate_id=runcomponentstates.QUARANTINED_PK,
                      end_time=datetime(2000, 2, 15))
        rs2.log = ExecLog(record=rs2)
        rs3 = RunStep(execrecord=er, _runcomponentstate_id=runcomponentstates.SUCCESSFUL_PK,
                      end_time=datetime(2000, 2, 16))
        rs3.log = ExecLog(record=rs3)
        rs3.log.is_successful = Mock(return_value=True)
        er.used_by_components.add(rs1, rs2, rs3)

        er.decontaminate_runcomponents = Mock()

        er.attempt_decontamination(ds1)
        rs3.log.is_successful.assert_not_called()
        er.decontaminate_runcomponents.assert_not_called()

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
                      end_time=datetime(2000, 2, 14))
        rs1.log = ExecLog(record=rs1)
        rs2 = RunStep(execrecord=er, _runcomponentstate_id=runcomponentstates.QUARANTINED_PK,
                      end_time=datetime(2000, 2, 15))
        rs2.log = ExecLog(record=rs2)
        rs3 = RunStep(execrecord=er, _runcomponentstate_id=runcomponentstates.FAILED_PK,
                      end_time=datetime(2000, 2, 16))
        rs3.log = ExecLog()
        rs3.log.is_successful = Mock(return_value=False)
        er.used_by_components.add(rs1, rs2, rs3)

        er.decontaminate_runcomponents = Mock()

        er.attempt_decontamination(ds1)
        rs3.log.is_successful.assert_called_once_with()
        er.decontaminate_runcomponents.assert_not_called()


class ExecRecordMockTests(TestCase):
    def test_execrecord_input_matches_output_cable_source(self):
        trx_out1 = TransformationOutput()
        trx_out1.transformationoutput = trx_out1
        execrecord = ExecRecord(generator=ExecLog(record=RunOutputCable(
            run=Run(),
            pipelineoutputcable=PipelineOutputCable(source=trx_out1))))

        trx_out2 = TransformationOutput()
        trx_out2.transformationoutput = trx_out2
        execrecordin = ExecRecordIn(execrecord=execrecord, dataset=Dataset(id=99), generic_input=trx_out2)

        self.assertRaisesRegexp(
            ValidationError,
            'ExecRecordIn "S99" does not denote the TO that feeds the parent ExecRecord POC',
            execrecordin.clean)

    def test_execrecord_input_matches_input_cable_source(self):
        trx_out1 = TransformationOutput()
        trx_out1.transformationoutput = trx_out1
        execrecord = ExecRecord(generator=ExecLog(record=RunSIC(
            dest_runstep=RunStep(run=Run()),
            PSIC=PipelineStepInputCable(source=trx_out1))))

        trx_out2 = TransformationOutput()
        trx_out2.transformationoutput = trx_out2
        execrecordin = ExecRecordIn(execrecord=execrecord, dataset=Dataset(id=99), generic_input=trx_out2)

        self.assertRaisesRegexp(
            ValidationError,
            'ExecRecordIn "S99" does not denote the TO/TI that feeds the parent ExecRecord PSIC',
            execrecordin.clean)

    def test_ER_doesnt_link_cable_so_ERI_mustnt_link_TO(self):
        method = Method()
        method.method = method
        execrecord = ExecRecord(generator=ExecLog(record=RunStep(run=Run(),
                                                                 pipelinestep=PipelineStep(transformation=method))))

        trx_out = TransformationOutput(dataset_name='ages')
        trx_out.transformationoutput = trx_out
        execrecordin = ExecRecordIn(execrecord=execrecord, dataset=Dataset(id=99), generic_input=trx_out)

        self.assertRaisesRegexp(
            ValidationError,
            'ExecRecordIn "S99=>ages" must refer to a TI of the Method of the parent ExecRecord',
            execrecordin.clean)

    def test_general_transf_returns_correct_method(self):
        """
        Test if ExecRecord.general_transf() returns the method of the PipelineStep
        it was defined with.
        """
        method = Method()
        method.method = method
        execrecord = ExecRecord(generator=ExecLog(record=RunStep(run=Run(),
                                                                 pipelinestep=PipelineStep(transformation=method))))

        self.assertEqual(execrecord.general_transf(), method)

    def test_execrecordin_raw_raw(self):
        trx_out = TransformationOutput()
        trx_out.transformationoutput = trx_out
        execrecord = ExecRecord(generator=ExecLog(record=RunOutputCable(
            run=Run(),
            pipelineoutputcable=PipelineOutputCable(source=trx_out))))

        execrecordin = ExecRecordIn(execrecord=execrecord, dataset=Dataset(), generic_input=trx_out)

        execrecordin.clean()

    @mocked_relations(Dataset)
    def test_execrecordin_raw_cdt(self):
        trx_out = TransformationOutput(dataset_idx=3, dataset_name='ages')
        trx_out.transformationoutput = trx_out
        trx_out.structure = XputStructure()
        execrecord = ExecRecord(generator=ExecLog(record=RunOutputCable(
            run=Run(),
            pipelineoutputcable=PipelineOutputCable(source=trx_out))))

        execrecordin = ExecRecordIn(execrecord=execrecord, dataset=Dataset(id=99), generic_input=trx_out)

        self.assertRaisesRegexp(
            ValidationError,
            'Dataset "S99" \(raw\) cannot feed source "3: ages" \(non-raw\)',
            execrecordin.clean)

    def test_execrecordin_cdt_raw(self):
        trx_out = TransformationOutput(dataset_idx=3, dataset_name='ages')
        trx_out.transformationoutput = trx_out
        execrecord = ExecRecord(generator=ExecLog(record=RunOutputCable(
            run=Run(),
            pipelineoutputcable=PipelineOutputCable(source=trx_out))))

        dataset = Dataset(id=99)
        dataset.structure = DatasetStructure()
        execrecordin = ExecRecordIn(execrecord=execrecord, dataset=dataset, generic_input=trx_out)

        self.assertRaisesRegexp(
            ValidationError,
            'Dataset "S99" \(non-raw\) cannot feed source "3: ages" \(raw\)',
            execrecordin.clean)

    def test_execrecordin_cdt_cdt(self):
        trx_out = TransformationOutput(dataset_idx=3, dataset_name='ages')
        trx_out.transformationoutput = trx_out
        cdt = CompoundDatatype()
        trx_out.structure = XputStructure(compounddatatype=cdt)
        execrecord = ExecRecord(generator=ExecLog(record=RunOutputCable(
            run=Run(),
            pipelineoutputcable=PipelineOutputCable(source=trx_out))))

        dataset = Dataset(id=99)
        dataset.structure = DatasetStructure(compounddatatype=cdt)
        execrecordin = ExecRecordIn(execrecord=execrecord, dataset=dataset, generic_input=trx_out)

        execrecordin.clean()

    @mocked_relations(CompoundDatatype)
    def test_execrecordin_cdts_differ(self):
        trx_out = TransformationOutput(dataset_idx=3, dataset_name='ages')
        trx_out.transformationoutput = trx_out
        cdt1 = CompoundDatatype()
        cdt1.members = MockSet(CompoundDatatypeMember(datatype=Datatype()), CompoundDatatypeMember(datatype=Datatype()))
        trx_out.structure = XputStructure(compounddatatype=cdt1)
        execrecord = ExecRecord(generator=ExecLog(record=RunOutputCable(
            run=Run(),
            pipelineoutputcable=PipelineOutputCable(source=trx_out))))

        dataset = Dataset(id=99)
        cdt2 = CompoundDatatype()
        cdt2.members = MockSet(CompoundDatatypeMember(datatype=Datatype()))
        dataset.structure = DatasetStructure(compounddatatype=cdt2)
        execrecordin = ExecRecordIn(execrecord=execrecord, dataset=dataset, generic_input=trx_out)

        self.assertRaisesRegexp(
            ValidationError,
            'CDT of Dataset "S99" is not a restriction of the required CDT',
            execrecordin.clean)

    def test_execrecordin_max_row(self):
        trx_out = TransformationOutput(dataset_idx=3, dataset_name='ages')
        trx_out.transformationoutput = trx_out
        cdt = CompoundDatatype()
        trx_out.structure = XputStructure(compounddatatype=cdt, max_row=10)
        execrecord = ExecRecord(generator=ExecLog(record=RunOutputCable(
            run=Run(),
            pipelineoutputcable=PipelineOutputCable(source=trx_out))))

        dataset = Dataset(id=99)
        dataset.structure = DatasetStructure(compounddatatype=cdt, num_rows=10)
        execrecordin = ExecRecordIn(execrecord=execrecord, dataset=dataset, generic_input=trx_out)

        execrecordin.clean()

    def test_execrecordin_too_many_rows(self):
        trx_out = TransformationOutput(dataset_idx=3, dataset_name='ages')
        trx_out.transformationoutput = trx_out
        cdt = CompoundDatatype()
        trx_out.structure = XputStructure(compounddatatype=cdt, max_row=10)
        execrecord = ExecRecord(generator=ExecLog(record=RunOutputCable(
            run=Run(),
            pipelineoutputcable=PipelineOutputCable(source=trx_out))))

        dataset = Dataset(id=99)
        dataset.structure = DatasetStructure(compounddatatype=cdt, num_rows=11)
        execrecordin = ExecRecordIn(execrecord=execrecord, dataset=dataset, generic_input=trx_out)

        self.assertRaisesRegexp(
            ValidationError,
            'Dataset "S99" has too many rows to have come from TransformationOutput "3: ages"',
            execrecordin.clean)

    def create_pipeline_execrecordout(self):
        pipeline = Pipeline()
        pipeline.pipeline = pipeline
        method = Method()
        method.method = method
        method_out = TransformationOutput(pk=42, transformation=method)
        method_out.transformationoutput = method_out
        execrecord = ExecRecord(generator=ExecLog(record=RunOutputCable(
            run=Run(),
            pipelineoutputcable=PipelineOutputCable(source=method_out, pipeline=pipeline, output_name='ages'))))
        pipeline_out = TransformationOutput(pk=43, transformation=pipeline, dataset_name='ages', dataset_idx=3)
        pipeline_out.transformationoutput = pipeline_out
        dataset = Dataset(id=99)
        Dataset.objects = MockSet(dataset)
        TransformationXput.objects = MockSet(method_out, pipeline_out)
        execrecordout = ExecRecordOut(execrecord=execrecord, dataset=dataset, generic_output=pipeline_out)
        return execrecordout

    @mocked_relations(Dataset, TransformationXput)
    def test_pipeline_execrecordout_different_pipelines(self):
        """ If the parent ER is linked with a POC, the ERO TO must belong to that pipeline """

        execrecordout = self.create_pipeline_execrecordout()
        pipeline2 = Pipeline()
        pipeline2.pipeline = pipeline2
        execrecordout.generic_output.transformation = pipeline2

        self.assertRaisesRegexp(
            ValidationError,
            'ExecRecordOut "S99" does not belong to the same pipeline as its parent ExecRecord POC',
            execrecordout.clean)

    @mocked_relations(Dataset, TransformationXput)
    def test_pipeline_execrecordout_same_pipeline(self):
        """ If the parent ER is linked with a POC, the ERO TO must belong to that pipeline """

        execrecordout = self.create_pipeline_execrecordout()

        execrecordout.clean()

    @mocked_relations(Dataset, TransformationXput)
    def test_ER_links_with_POC_and_POC_output_name_must_match_pipeline_TO_name(self):
        # The TO must have the same name as the POC which supposedly created it
        execrecordout = self.create_pipeline_execrecordout()
        execrecordout.execrecord.generator.record.pipelineoutputcable.output_name = 'foo'

        self.assertRaisesRegexp(
            ValidationError,
            'ExecRecordOut "S99" does not represent the same output as its parent ExecRecord POC',
            execrecordout.clean)

    @mocked_relations(Dataset, TransformationInput, TransformationOutput, TransformationXput)
    def test_pipeline_execrecordout_raw_cdt(self):
        execrecordout = self.create_pipeline_execrecordout()
        execrecordout.generic_output.structure = XputStructure()
        self.assertRaisesRegexp(
            ValidationError,
            r'Dataset "S99" \(raw\) cannot have come from output "3: ages" \(non-raw\)',
            execrecordout.clean)

    @mocked_relations(Dataset, TransformationXput)
    def test_pipeline_execrecordout_cdt_raw(self):
        execrecordout = self.create_pipeline_execrecordout()
        execrecordout.dataset.structure = DatasetStructure()
        self.assertRaisesRegexp(
            ValidationError,
            r'Dataset "S99" \(non-raw\) cannot have come from output "3: ages" \(raw\)',
            execrecordout.clean)

    @mocked_relations(Dataset, TransformationXput, CompoundDatatype)
    def test_pipeline_execrecordout_cdt_cdt(self):
        cdt = CompoundDatatype()

        execrecordout = self.create_pipeline_execrecordout()
        execrecordout.generic_output.structure = XputStructure(compounddatatype=cdt)
        execrecordout.dataset.structure = DatasetStructure(compounddatatype=cdt)
        execrecordout.clean()

    @mocked_relations(Dataset, TransformationInput, TransformationOutput, TransformationXput, CompoundDatatype)
    def test_pipeline_execrecordout_cdt1_cdt2(self):
        execrecordout = self.create_pipeline_execrecordout()
        cdt1 = CompoundDatatype()
        cdt1.members = MockSet(CompoundDatatypeMember(datatype=Datatype()))
        execrecordout.generic_output.structure = XputStructure(compounddatatype=cdt1)
        cdt2 = CompoundDatatype()
        cdt2.members = MockSet(CompoundDatatypeMember(datatype=Datatype()),
                               CompoundDatatypeMember(datatype=Datatype()))
        execrecordout.dataset.structure = DatasetStructure(compounddatatype=cdt2)
        self.assertRaisesRegexp(
            ValidationError,
            'CDT of Dataset "S99" is not identical to the CDT of the '
            'TransformationOutput "3: ages" of the generating Pipeline',
            execrecordout.clean)

    @mocked_relations(Dataset, TransformationInput, TransformationOutput, TransformationXput, CompoundDatatype)
    def test_pipeline_execrecordout_cdt_cdt(self):
        cdt = CompoundDatatype()

        execrecordout = self.create_pipeline_execrecordout()
        execrecordout.generic_output.structure = XputStructure(compounddatatype=cdt, max_row=10)
        execrecordout.dataset.structure = DatasetStructure(compounddatatype=cdt, num_rows=11)

        self.assertRaisesRegexp(
            ValidationError,
            'Dataset "S99" was produced by TransformationOutput "3: ages" but has too many rows',
            execrecordout.clean)

    def create_method_execrecordout(self):
        method = Method()
        method.method = method
        execrecord = ExecRecord(generator=ExecLog(record=RunStep(run=Run(),
                                                                 pipelinestep=PipelineStep(transformation=method))))

        trx_out = TransformationOutput(pk=43, dataset_idx=3, dataset_name='ages')
        trx_out.transformationoutput = trx_out
        method.outputs = MockSet(trx_out)
        dataset = Dataset(id=99)
        Dataset.objects = MockSet(dataset)
        TransformationXput.objects = MockSet(trx_out)
        execrecordout = ExecRecordOut(execrecord=execrecord, dataset=dataset, generic_output=trx_out)
        return execrecordout

    @mocked_relations(Dataset, TransformationXput, Transformation)
    def test_method_execrecordout_raw_raw(self):
        execrecordout = self.create_method_execrecordout()

        execrecordout.clean()

    @mocked_relations(Dataset,
                      Transformation,
                      TransformationInput,
                      TransformationOutput,
                      TransformationXput,
                      CompoundDatatype)
    def test_method_execrecordout_cdt1_cdt2(self):
        execrecordout = self.create_method_execrecordout()
        cdt1 = CompoundDatatype()
        cdt1.members = MockSet(CompoundDatatypeMember(datatype=Datatype()))
        execrecordout.generic_output.structure = XputStructure(compounddatatype=cdt1)
        cdt2 = CompoundDatatype()
        cdt2.members = MockSet(CompoundDatatypeMember(datatype=Datatype()),
                               CompoundDatatypeMember(datatype=Datatype()))
        execrecordout.dataset.structure = DatasetStructure(compounddatatype=cdt2)

        self.assertRaisesRegexp(
            ValidationError,
            'CDT of Dataset "S99" is not the CDT of the TransformationOutput "3: ages" of the generating Method',
            execrecordout.clean)

    @mocked_relations(ExecRecord, Dataset, TransformationXput)
    def test_cable_execrecordout_raw_raw(self):
        execrecordout = self.create_cable_execrecordout()

        execrecordout.clean()

    @mocked_relations(ExecRecord, Dataset, TransformationXput, CompoundDatatype)
    def test_cable_execrecordout_cdt1_cdt2(self):

        execrecordout = self.create_cable_execrecordout()
        cdt1 = CompoundDatatype()
        cdt1.members = MockSet(CompoundDatatypeMember(datatype=Datatype()))
        execrecordout.generic_output.structure = XputStructure(compounddatatype=cdt1)
        cdt2 = CompoundDatatype()
        cdt2.members = MockSet(CompoundDatatypeMember(datatype=Datatype()),
                               CompoundDatatypeMember(datatype=Datatype()))
        execrecordout.dataset.structure = DatasetStructure(compounddatatype=cdt2)

        self.assertRaisesRegexp(
            ValidationError,
            'CDT of Dataset "S99" is not a restriction of the CDT of the fed TransformationInput "3: ages"',
            execrecordout.clean)

    @mocked_relations(ExecRecord, TransformationInput, TransformationOutput, TransformationXput, Dataset)
    def test_trivial_execrecord_matches(self):
        """ERs representing trivial PSICs must have the same Dataset on both sides."""

        execrecord = self.create_cable_execrecordout().execrecord

        execrecord.clean()

    @mocked_relations(ExecRecord, TransformationInput, TransformationOutput, TransformationXput, Dataset)
    def test_trivial_execrecord_no_match(self):
        """ERs representing trivial PSICs must have the same Dataset on both sides."""

        execrecord = self.create_cable_execrecordout().execrecord
        execrecord.execrecordins.first().dataset = Dataset(id=100)

        self.assertTrue(execrecord.execrecordouts.first().generic_output.is_raw())
        self.assertTrue(execrecord.execrecordins.first().generic_input.is_raw())
        self.assertRaisesRegexp(
            ValidationError,
            r'ExecRecord "S100 ={2: history:ages\(raw\)}=> S99" represents a trivial cable'
            r' but its input and output do not match',
            execrecord.clean)

    def create_cable_execrecordout(self):
        prev_out = TransformationOutput(pk=42, dataset_idx=1, dataset_name='eras')
        prev_out.transformationoutput = prev_out
        next_in = TransformationInput(pk=43, dataset_idx=3, dataset_name='ages')
        next_in.transformationinput = next_in
        execrecord = ExecRecord(generator=ExecLog(record=RunSIC(
            dest_runstep=RunStep(run=Run()),
            PSIC=PipelineStepInputCable(source=prev_out, dest=next_in, pipelinestep=PipelineStep(name='history',
                                                                                                 step_num=2)))))
        dataset = Dataset(id=99)
        Dataset.objects = MockSet(dataset)
        execrecordin = ExecRecordIn(execrecord=execrecord, dataset=dataset, generic_input=prev_out)
        TransformationXput.objects = MockSet(next_in)
        execrecordout = ExecRecordOut(execrecord=execrecord, dataset=dataset, generic_output=next_in)
        execrecord.execrecordins = MockSet(execrecordin)
        execrecord.execrecordouts = MockSet(execrecordout)
        return execrecordout

    def create_custom_cable_execrecordout(self):
        execrecordout = self.create_cable_execrecordout()
        execrecord = execrecordout.execrecord
        execrecordin = execrecord.execrecordins.first()
        string_type = Datatype(name='string')
        cdt1 = CompoundDatatype()
        field1_in = CompoundDatatypeMember(datatype=string_type, column_idx=1, column_name='age')
        field2_in = CompoundDatatypeMember(datatype=string_type, column_idx=2, column_name='start')
        cdt1.members = MockSet(field1_in,
                               field2_in)
        execrecordin.generic_input.structure = XputStructure(compounddatatype=cdt1)
        execrecordin.dataset.structure = DatasetStructure(compounddatatype=cdt1)
        cdt2 = CompoundDatatype()
        field1_out = CompoundDatatypeMember(datatype=string_type, column_idx=1, column_name='era')
        field2_out = CompoundDatatypeMember(datatype=string_type, column_idx=2, column_name='start')
        cdt2.members = MockSet(field1_out,
                               field2_out)
        execrecordout.generic_output.structure = XputStructure(compounddatatype=cdt2)
        execrecordout.dataset = Dataset(id=100)
        execrecordout.dataset.structure = DatasetStructure(compounddatatype=cdt2)
        Dataset.objects = MockSet(execrecordin.dataset, execrecordout.dataset)
        cable = execrecord.generator.record.PSIC
        cable.custom_wires = MockSet(CustomCableWire(source_pin=field1_in, dest_pin=field1_out),
                                     CustomCableWire(source_pin=field2_in, dest_pin=field2_out))
        return execrecordout

    @mocked_relations(ExecRecord, TransformationXput, Dataset, CompoundDatatype, PipelineCable)
    def test_custom_cable(self):
        """Test that the Datatypes of Datasets passing through PSICs are properly preserved."""
        execrecordout = self.create_custom_cable_execrecordout()
        execrecord = execrecordout.execrecord
        execrecord.clean()

    @mocked_relations(ExecRecord, TransformationXput, Dataset, CompoundDatatype, PipelineCable)
    def test_custom_cable_mismatch(self):
        """Test that the Datatypes of Datasets passing through PSICs are properly preserved."""
        execrecordout = self.create_custom_cable_execrecordout()
        execrecord = execrecordout.execrecord
        int_type = Datatype(name='int')
        execrecordout.dataset.structure.compounddatatype.members.last().datatype = int_type
        self.assertRaisesRegexp(
            ValidationError,
            'ExecRecord "S99 ={2: history:ages}=> S100" represents a cable, but the Datatype of its destination '
            'column, "int", does not match the Datatype of its source column, "string"',
            execrecord.clean)

    def create_execrecord_with_runstep_states(self, *state_ids):
        execrecord = ExecRecord()
        for state_id in state_ids:
            run_step = RunStep(_runcomponentstate_id=state_id)
            run_step.runstep = run_step
            execrecord.used_by_components.add(run_step)
        return execrecord

    @mocked_relations(ExecRecord)
    def test_execrecord_new_never_failed(self):
        """An ExecRecord with no RunSteps has never failed."""
        execrecord = self.create_execrecord_with_runstep_states()

        self.assertFalse(execrecord.has_ever_failed())

    @mocked_relations(ExecRecord)
    def test_execrecord_one_good_step(self):
        """An ExecRecord with one good RunStep has never failed."""
        execrecord = self.create_execrecord_with_runstep_states(runcomponentstates.SUCCESSFUL_PK)

        self.assertFalse(execrecord.has_ever_failed())

    @mocked_relations(ExecRecord)
    def test_execrecord_two_good_steps(self):
        """An ExecRecord with two good RunSteps has never failed."""
        execrecord = self.create_execrecord_with_runstep_states(runcomponentstates.SUCCESSFUL_PK,
                                                                runcomponentstates.SUCCESSFUL_PK)

        self.assertFalse(execrecord.has_ever_failed())

    @mocked_relations(ExecRecord)
    def test_execrecord_mixed_steps(self):
        """An ExecRecord with two good RunSteps has never failed."""
        execrecord = self.create_execrecord_with_runstep_states(runcomponentstates.SUCCESSFUL_PK,
                                                                runcomponentstates.FAILED_PK)

        self.assertTrue(execrecord.has_ever_failed())
