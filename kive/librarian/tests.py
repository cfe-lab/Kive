"""
Shipyard models pertaining to the librarian app.
"""

from datetime import datetime, timedelta
import os
import random
import re
import tempfile
import time
import logging
import json
import shutil
import stat
import django.utils.six as dsix

from django.core.exceptions import ValidationError
from django.contrib.auth.models import User, Group
from django.test import TestCase, skipIfDBFeature, Client
from django.core.urlresolvers import reverse, resolve
from django.core.files import File
from django.core.files.base import ContentFile
# from django.utils.timezone import get_default_timezone, get_current_timezone
from django.utils import timezone
from django_mock_queries.mocks import mocked_relations
from mock import patch

from rest_framework.test import force_authenticate, APIRequestFactory
from rest_framework import status

from archive.models import ExecLog, MethodOutput, Run, RunStep, RunComponentState
from constants import datatypes, groups, runcomponentstates
from container.models import ContainerFamily
from datachecking.models import MD5Conflict
from librarian.ajax import ExternalFileDirectoryViewSet, DatasetViewSet
from librarian.models import Dataset, ExecRecord, ExternalFileDirectory, DatasetStructure
from librarian.serializers import DatasetSerializer
from metadata.models import Datatype, CompoundDatatype, kive_user, everyone_group
from method.models import CodeResource, CodeResourceRevision, Method, \
    MethodFamily
from pipeline.models import Pipeline, PipelineFamily

import file_access_utils
import kive.testing_utils as tools
from kive.tests import BaseTestCases, DuckContext, install_fixture_files, remove_fixture_files, capture_log_stream

FROM_FILE_END = 2


def er_from_record(record):
    """
    Helper function to create an ExecRecord from an Run, RunStep, or
    RunOutputCable (record), by creating a throwaway ExecLog.
    """
    exec_log = ExecLog(record=record, invoking_record=record)
    exec_log.start_time = timezone.now()
    time.sleep(1)
    exec_log.end_time = timezone.now()
    exec_log.save()
    if record.__class__.__name__ == "RunStep":
        output = MethodOutput(execlog=exec_log, return_code=0)
        output.save()
        exec_log.methodoutput = output
        exec_log.save()
    exec_record = ExecRecord(generator=exec_log)
    exec_record.save()
    return exec_record


@skipIfDBFeature('is_mocked')
class LibrarianTestCase(TestCase, object):
    """
    Set up a database state for unit testing the librarian app.

    This extends PipelineTestCase, which itself extended
    other stuff (follow the chain).
    """
    def setUp(self):
        """Set up default database state for librarian unit testing."""
        tools.create_librarian_test_environment(self)

    def tearDown(self):
        tools.clean_up_all_files()


class DatasetTests(LibrarianTestCase):

    def setUp(self):
        super(DatasetTests, self).setUp()

        # Turn off logging, so the test output isn't polluted.
        logging.getLogger('Dataset').setLevel(logging.CRITICAL)
        logging.getLogger('CompoundDatatype').setLevel(logging.CRITICAL)

        rows = 10
        seqlen = 10

        self.data = ""
        for i in range(rows):
            seq = "".join([random.choice("ATCG") for _ in range(seqlen)])
            self.data += "patient{},{}\n".format(i, seq)
        self.header = "header,sequence"

        self.datatype_str = Datatype.objects.get(pk=datatypes.STR_PK)
        self.datatype_dna = Datatype(name="DNA", description="sequences of ATCG",
                                     user=self.myUser)
        # self.datatype_dna.clean()
        self.datatype_dna.save()
        self.datatype_dna.restricts.add(self.datatype_str)
        self.datatype_dna.complete_clean()
        self.cdt_record = CompoundDatatype(user=self.myUser)
        self.cdt_record.save()
        self.cdt_record.members.create(datatype=self.datatype_str,
                                       column_name="header",
                                       column_idx=1)
        self.cdt_record.members.create(datatype=self.datatype_dna,
                                       column_name="sequence",
                                       column_idx=2)
        self.cdt_record.clean()

        self.data_file = tempfile.NamedTemporaryFile(delete=False)
        data_str = self.header + "\n" + self.data
        self.data_file.write(data_str.encode())
        self.file_path = self.data_file.name
        self.data_file.close()

        self.dsname = "good data"
        self.dsdesc = "some headers and sequences"
        self.dataset = Dataset.create_dataset(
            file_path=self.file_path,
            user=self.myUser,
            cdt=self.cdt_record,
            keep_file=True,
            name=self.dsname,
            description=self.dsdesc
        )

    def tearDown(self):
        super(DatasetTests, self).tearDown()
        os.remove(self.file_path)

    def test_filehandle(self):
        """
        Test that you can pass a filehandle to create_dataset() to make a dataset.
        """
        import datetime
        dt = datetime.datetime.now()
        # Turn off logging, so the test output isn't polluted.
        logging.getLogger('Dataset').setLevel(logging.CRITICAL)
        logging.getLogger('CompoundDatatype').setLevel(logging.CRITICAL)

        with tempfile.NamedTemporaryFile(delete=True) as tmpfile:
            tmpfile.file.write("Random stuff".encode())
            tmpfile.file.flush()  # flush python buffer to os buffer
            os.fsync(tmpfile.file.fileno())  # flush os buffer to disk
            tmpfile.file.seek(0)  # go to beginning of file before calculating expected md5

            expected_md5 = file_access_utils.compute_md5(tmpfile)
            tmpfile.file.seek(0)  # return to beginning before creating a Dataset

            raw_datatype = None  # raw compound datatype
            name = "Test file handle" + str(dt.microsecond)
            desc = "Test create dataset with file handle"
            dataset = Dataset.create_dataset(
                file_path=None,
                user=self.myUser,
                cdt=raw_datatype,
                keep_file=True,
                name=name,
                description=desc,
                check=True,
                file_handle=tmpfile
            )

        self.assertIsNotNone(Dataset.objects.filter(name=name).get(),
                             msg="Can't find Dataset in DB for name=" + name)

        actual_md5 = Dataset.objects.filter(id=dataset.id).get().MD5_checksum
        self.assertEqual(actual_md5, expected_md5,
                         msg="Checksum for Dataset ({}) file does not match expected ({})".format(
                             actual_md5,
                             expected_md5
                         ))

    def test_is_raw(self):
        self.assertEqual(self.triplet_dataset.is_raw(), False)
        self.assertEqual(self.raw_dataset.is_raw(), True)

    def test_forgot_header(self):
        """
        Dataset creation with a CDT fails when the header is left off
        the data file.
        """
        # Write the data with no header.
        data_file = tempfile.NamedTemporaryFile()
        data_file.write(self.data.encode())

        # Try to create a dataset.
        self.assertRaisesRegexp(ValueError,
                                re.escape('The header of file "{}" does not match the CompoundDatatype "{}"'
                                          .format(data_file.name, self.cdt_record)),
                                lambda: Dataset.create_dataset(file_path=data_file.name,
                                                               user=self.myUser, cdt=self.cdt_record,
                                                               name="lab data", description="patient sequences"))
        data_file.close()

    def test_empty_file(self):
        """
        Dataset creation fails if the file passed is empty.
        """
        data_file = tempfile.NamedTemporaryFile()
        file_path = data_file.name

        self.assertRaisesRegexp(
            ValueError,
            re.escape('The header of file "{}" does not match the CompoundDatatype "{}"'
                      .format(file_path, self.cdt_record)),
            lambda: Dataset.create_dataset(
                file_path=data_file.name,
                user=self.myUser,
                cdt=self.cdt_record,
                name="missing data",
                description="oops!"
            )
        )
        data_file.close()

    def test_too_many_columns(self):
        """
        Dataset creation fails if the data file has too many
        columns.
        """
        with tempfile.NamedTemporaryFile() as data_file:
            data_file.write("""\
header,sequence,extra
foo,bar,baz
            """.encode())
            data_file.flush()
            file_path = data_file.name

            self.assertRaisesRegexp(
                ValueError,
                re.escape('The header of file "{}" does not match the CompoundDatatype "{}"'
                          .format(file_path, self.cdt_record)),
                lambda: Dataset.create_dataset(file_path=file_path, user=self.myUser,
                                               cdt=self.cdt_record, name="bad data",
                                               description="too many columns")
            )

    def test_right_columns(self):
        """
        Dataset creation fails if the data file has too many
        columns.
        """
        with tempfile.NamedTemporaryFile() as data_file:
            data_file.write("""\
header,sequence
foo,bar
""".encode())
            data_file.flush()
            file_path = data_file.name

            Dataset.create_dataset(file_path=file_path, user=self.myUser, cdt=self.cdt_record,
                                   description="right columns", name="good data")

    def test_invalid_integer_field(self):
        compound_datatype = CompoundDatatype(user=self.myUser)
        compound_datatype.save()
        compound_datatype.members.create(datatype=self.STR,
                                         column_name="name",
                                         column_idx=1)
        compound_datatype.members.create(datatype=self.INT,
                                         column_name="count",
                                         column_idx=2)
        compound_datatype.clean()

        data_file = dsix.StringIO("""\
name,count
Bob,tw3nty
""")
        data_file.name = 'test_file.csv'

        self.assertRaisesRegexp(
            ValueError,
            re.escape('The entry at row 1, column 2 of file "{}" did not pass the constraints of Datatype "integer"'
                      .format(data_file.name)),
            lambda: Dataset.create_dataset(file_path=None,
                                           file_handle=data_file,
                                           user=self.myUser,
                                           cdt=compound_datatype,
                                           name="bad data",
                                           description="bad integer field"))

    def test_dataset_creation(self):
        """
        Test coherence of a freshly created Dataset.
        """
        self.assertEqual(self.dataset.clean(), None)
        self.assertEqual(self.dataset.has_data(), True)
        self.assertEqual(self.dataset.is_raw(), False)

        self.assertEqual(self.dataset.user, self.myUser)
        self.assertEqual(self.dataset.name, self.dsname)
        self.assertEqual(self.dataset.description, self.dsdesc)
        self.assertEqual(self.dataset.date_created.date(), timezone.now().date())
        self.assertEqual(self.dataset.date_created < timezone.now(), True)
        self.assertEqual(self.dataset.file_source, None)
        self.assertEqual(os.path.basename(self.dataset.dataset_file.path), os.path.basename(self.file_path))
        self.data_file.close()

    def test_dataset_increase_permissions_from_json(self):
        """
        Test increase_permissions_from_json reaches any usurping Datasets.
        """
        # First, we revoke Everyone permissions on a Dataset.
        self.singlet_dataset.groups_allowed.remove(everyone_group())

        # We store the original contents of a Dataset...
        self.singlet_dataset.dataset_file.open()
        orig_contents = self.singlet_dataset.dataset_file.read()
        self.singlet_dataset.dataset_file.close()
        orig_md5 = self.singlet_dataset.MD5_checksum

        # ... and then we corrupt it.
        self.singlet_dataset.MD5_checksum = "corruptedmd5"
        self.singlet_dataset.save()

        usurping_ds = Dataset(
            name="Usurping DS",
            description="Usurps self.singlet_dataset",
            user=self.myUser,
            dataset_file=ContentFile(orig_contents),
            MD5_checksum=orig_md5
        )
        usurping_ds.save()

        ic = self.singlet_dataset.integrity_checks.create(user=self.myUser)
        ic.start()

        new_conflict = MD5Conflict(integritychecklog=ic, conflicting_dataset=usurping_ds)
        new_conflict.save()

        ic.stop()

        # Now, let's try to grant some permissions on self.singlet_dataset.
        new_perms_json = json.dumps(
            [
                [self.ringoUser.username],
                [Group.objects.get(pk=groups.DEVELOPERS_PK).name]
            ]
        )
        self.singlet_dataset.increase_permissions_from_json(new_perms_json)

        self.assertTrue(self.singlet_dataset.users_allowed.filter(pk=self.ringoUser.pk).exists())
        self.assertTrue(usurping_ds.users_allowed.filter(pk=self.ringoUser.pk).exists())

        self.assertTrue(self.singlet_dataset.groups_allowed.filter(pk=groups.DEVELOPERS_PK).exists())
        self.assertTrue(usurping_ds.groups_allowed.filter(pk=groups.DEVELOPERS_PK).exists())

    def test_update_name(self):
        dataset = self.singlet_dataset
        self.assertEqual('singlet', dataset.name)

        user = dataset.user
        client = Client()
        client.force_login(user)
        expected_name = 'Changed to Synglet'

        response = client.post(reverse('dataset_view',
                                       kwargs=dict(dataset_id=dataset.id)),
                               dict(name=expected_name))

        if response.status_code != 302:
            self.assertEqual({}, response.context['form'].errors)
        dataset.refresh_from_db()
        self.assertEqual(expected_name, dataset.name)

    def test_increase_permissions(self):
        dataset = self.singlet_dataset
        dataset.groups_allowed.clear()
        self.assertFalse(dataset.shared_with_everyone)

        user = dataset.user
        client = Client()
        client.force_login(user)

        response = client.post(reverse('dataset_view',
                                       kwargs=dict(dataset_id=dataset.id)),
                               dict(name='synglet',
                                    permissions_1='Everyone'))

        if response.status_code != 302:
            self.assertEqual({}, response.context['form'].errors)
        dataset.refresh_from_db()
        self.assertTrue(dataset.shared_with_everyone)

    def test_source_run_permissions(self):
        """ Dataset not allowed to have more permissions than source run. """
        run_step = RunStep.objects.first()
        run_step.run.groups_allowed.clear()

        dataset = self.singlet_dataset
        dataset.groups_allowed.clear()
        dataset.file_source = run_step
        dataset.save()
        self.assertFalse(dataset.shared_with_everyone)
        expected_errors = {
            '__all__': ['Group(s) Everyone cannot be granted access']}

        user = dataset.user
        client = Client()
        client.force_login(user)

        response = client.post(reverse('dataset_view',
                                       kwargs=dict(dataset_id=dataset.id)),
                               dict(name='synglet',
                                    permissions_1='Everyone'))

        self.assertEqual(200, response.status_code)  # Form error, not redirect
        self.assertEqual(expected_errors,
                         response.context['dataset_form'].errors)
        dataset.refresh_from_db()
        self.assertFalse(dataset.shared_with_everyone)

    def test_source_container_run_permissions(self):
        """ Dataset can't have more permissions than source container run. """
        user = self.singlet_dataset.user
        family = ContainerFamily.objects.create(user=user)
        container = family.containers.create(user=user)
        app = container.apps.create()
        argument = app.arguments.create(type='O')
        run = app.runs.create(user=user)

        dataset = self.singlet_dataset
        dataset.groups_allowed.clear()
        run.datasets.create(dataset=dataset,
                            argument=argument)
        self.assertFalse(dataset.shared_with_everyone)
        expected_errors = {
            '__all__': ['Group(s) Everyone cannot be granted access']}

        user = dataset.user
        client = Client()
        client.force_login(user)

        response = client.post(reverse('dataset_view',
                                       kwargs=dict(dataset_id=dataset.id)),
                               dict(name='synglet',
                                    permissions_1='Everyone'))

        self.assertEqual(200, response.status_code)  # Form error, not redirect
        self.assertEqual(expected_errors,
                         response.context['dataset_form'].errors)
        dataset.refresh_from_db()
        self.assertFalse(dataset.shared_with_everyone)


class DatasetStructureTests(LibrarianTestCase):

    def test_num_rows(self):
        self.assertEqual(self.triplet_3_rows_dataset.num_rows(), 3)
        self.assertEqual(self.triplet_3_rows_dataset.structure.num_rows, 3)


@skipIfDBFeature('is_mocked')
class FindCompatibleERTests(TestCase):
    fixtures = ['simple_run']

    def find_run_step(self):
        for e in ExecRecord.objects.all():
            if e.has_ever_failed():
                continue
            is_running = False
            runstep = None
            for runcomponent in e.used_by_components.all():
                if type(runcomponent.definite) is RunStep:
                    runstep = runcomponent.definite
                if not runcomponent.top_level_run.is_complete():
                    is_running = True
                    break
            if not is_running and runstep:
                return runstep

    def test_find_compatible_ER_never_failed(self):
        """Should be able to find a compatible ExecRecord which never failed."""
        runstep = self.find_run_step()
        execrecord = runstep.execrecord
        self.assertIsNotNone(execrecord)
        input_datasets_decorated = [(eri.generic_input.definite.dataset_idx, eri.dataset)
                                    for eri in execrecord.execrecordins.all()]
        input_datasets_decorated.sort()
        input_datasets = [entry[1] for entry in input_datasets_decorated]
        runstep.reused = False
        runstep.save()
        self.assertFalse(execrecord.has_ever_failed())
        self.assertIn(execrecord, runstep.find_compatible_ERs(input_datasets))

    def test_find_compatible_ER_redacted(self):
        """Should not be able to find a redacted ExecRecord."""
        runstep = self.find_run_step()
        execrecord = runstep.execrecord
        self.assertIsNotNone(execrecord)
        execrecord.execrecordins.first().dataset.redact()
        input_datasets_decorated = [(eri.generic_input.definite.dataset_idx, eri.dataset)
                                    for eri in execrecord.execrecordins.all()]
        input_datasets_decorated.sort()
        input_datasets = [entry[1] for entry in input_datasets_decorated]
        runstep.reused = False
        runstep.save()
        self.assertTrue(execrecord.is_redacted())
        self.assertNotIn(execrecord, runstep.find_compatible_ERs(input_datasets))

    def test_find_compatible_ER_failed(self):
        """Should also find a compatible ExecRecord which failed."""
        runstep = self.find_run_step()
        execrecord = runstep.execrecord
        self.assertIsNotNone(execrecord)
        methodoutput = runstep.log.methodoutput
        methodoutput.return_code = 1  # make this a failure
        methodoutput.save()
        # noinspection PyUnresolvedReferences
        runstep._runcomponentstate = RunComponentState.objects.get(pk=runcomponentstates.FAILED_PK)
        runstep.save()

        input_datasets_decorated = [(eri.generic_input.definite.dataset_idx, eri.dataset)
                                    for eri in execrecord.execrecordins.all()]
        input_datasets_decorated.sort()
        input_datasets = [entry[1] for entry in input_datasets_decorated]
        runstep = execrecord.used_by_components.first().definite
        runstep.reused = False
        runstep.save()
        self.assertTrue(execrecord.has_ever_failed())
        self.assertIn(execrecord, runstep.find_compatible_ERs(input_datasets))

    def test_find_compatible_ER_skips_nulls(self):
        """
        Incomplete run steps don't break search for compatible ExecRecords.
        """
        # Find an ExecRecord that has never failed
        runstep = self.find_run_step()
        execrecord = runstep.execrecord
        input_datasets_decorated = [(eri.generic_input.definite.dataset_idx, eri.dataset)
                                    for eri in execrecord.execrecordins.all()]
        input_datasets_decorated.sort()
        input_datasets = [entry[1] for entry in input_datasets_decorated]

        method = execrecord.general_transf()
        pipeline = execrecord.generating_run.pipeline
        ps = pipeline.steps.filter(transformation=method).first()

        # Create two RunSteps using this method.  First, an incomplete one.
        run1 = Run(user=pipeline.user, pipeline=pipeline, name="First incomplete run",
                   description="Be patient!")
        run1.save()
        run1.start()
        run1.runsteps.create(pipelinestep=ps)

        # Second, one that is looking for an ExecRecord.
        run2 = Run(user=pipeline.user, pipeline=pipeline, name="Second run in progress",
                   description="Impatient!")
        run2.save()
        run2.start()
        rs2 = run2.runsteps.create(pipelinestep=ps)

        self.assertIn(execrecord, rs2.find_compatible_ERs(input_datasets))


@skipIfDBFeature('is_mocked')
class RemovalTests(TestCase):
    fixtures = ["removal"]

    def setUp(self):
        install_fixture_files("removal")

        self.remover = User.objects.get(username="RemOver")
        self.noop_plf = PipelineFamily.objects.get(name="Nucleotide Sequence Noop")
        self.noop_pl = self.noop_plf.members.get(revision_name="v1")
        self.first_run = self.noop_pl.pipeline_instances.order_by("start_time").first()
        self.second_run = self.noop_pl.pipeline_instances.order_by("start_time").last()
        self.input_DS = Dataset.objects.get(name="Removal test data")
        self.nuc_seq_noop_mf = MethodFamily.objects.get(name="Noop (nucleotide sequence)")
        self.nuc_seq_noop = self.nuc_seq_noop_mf.members.get(revision_name="v1")
        self.p_nested_plf = PipelineFamily.objects.get(name="Nested pipeline")
        self.p_nested = self.p_nested_plf.members.get(revision_name="v1")
        self.noop_cr = CodeResource.objects.get(name="Noop")
        self.noop_crr = self.noop_cr.revisions.get(revision_name="1")
        self.pass_through_cr = CodeResource.objects.get(name="Pass Through")
        self.pass_through_crr = self.pass_through_cr.revisions.get(revision_name="1")
        self.raw_pass_through_mf = MethodFamily.objects.get(name="Pass-through (raw)")
        self.raw_pass_through = self.raw_pass_through_mf.members.get(revision_name="v1")
        self.nuc_seq = Datatype.objects.get(name="Nucleotide sequence")
        self.one_col_nuc_seq = self.nuc_seq.CDTMs.get(column_name="sequence", column_idx=1).compounddatatype

        self.two_step_noop_plf = PipelineFamily.objects.get(name="Nucleotide Sequence two-step Noop")
        self.two_step_noop_pl = self.two_step_noop_plf.members.get(revision_name="v1")
        self.two_step_input_dataset = Dataset.objects.get(name="Removal test data for a two-step Pipeline")

        # Datasets and ExecRecords produced by the first run.
        self.produced_data = set()
        self.execrecords = set()
        for runstep in self.first_run.runsteps.all():
            self.produced_data.update(runstep.outputs.all())
            self.execrecords.add(runstep.execrecord)
            for rsic in runstep.RSICs.all():
                self.produced_data.update(rsic.outputs.all())
                self.execrecords.add(rsic.execrecord)
        for roc in self.first_run.runoutputcables.all():
            self.produced_data.update(roc.outputs.all())
            self.execrecords.add(roc.execrecord)

        self.step_log = self.first_run.runsteps.first().log

        self.two_step_run = self.two_step_noop_pl.pipeline_instances.first()
        self.two_step_intermediate_data = self.two_step_run.runsteps.get(
            pipelinestep__step_num=1).outputs.first()
        self.two_step_output_data = self.two_step_run.runsteps.get(
            pipelinestep__step_num=2).outputs.first()
        self.two_step_execrecords = set()
        for runstep in self.two_step_run.runsteps.all():
            self.two_step_execrecords.add(runstep.execrecord)
            for rsic in runstep.RSICs.all():
                self.two_step_execrecords.add(rsic.execrecord)
        for roc in self.two_step_run.runoutputcables.all():
            self.two_step_execrecords.add(roc.execrecord)

    def tearDown(self):
        tools.clean_up_all_files()
        remove_fixture_files()

    def removal_plan_tester(self, obj_to_remove, datasets=None, ers=None, runs=None, pipelines=None, pfs=None,
                            methods=None, mfs=None, cdts=None, dts=None, crrs=None, crs=None,
                            external_files=None):
        removal_plan = obj_to_remove.build_removal_plan()
        self.assertSetEqual(removal_plan["Datasets"], set(datasets) if datasets is not None else set())
        self.assertSetEqual(removal_plan["ExecRecords"], set(ers) if ers is not None else set())
        self.assertSetEqual(removal_plan["Runs"], set(runs) if runs is not None else set())
        self.assertSetEqual(removal_plan["Pipelines"], set(pipelines) if pipelines is not None else set())
        self.assertSetEqual(removal_plan["PipelineFamilies"], set(pfs) if pfs is not None else set())
        self.assertSetEqual(removal_plan["Methods"], set(methods) if methods is not None else set())
        self.assertSetEqual(removal_plan["MethodFamilies"], set(mfs) if mfs is not None else set())
        self.assertSetEqual(removal_plan["CompoundDatatypes"], set(cdts) if cdts is not None else set())
        self.assertSetEqual(removal_plan["Datatypes"], set(dts) if dts is not None else set())
        self.assertSetEqual(removal_plan["CodeResourceRevisions"], set(crrs) if crrs is not None else set())
        self.assertSetEqual(removal_plan["CodeResources"], set(crs) if crs is not None else set())
        self.assertSetEqual(removal_plan["ExternalFiles"], set(external_files) if external_files is not None else set())

    def test_run_build_removal_plan(self):
        """Removing a Run should remove all intermediate/output data and ExecRecords, and all Runs that reused it."""
        self.removal_plan_tester(self.first_run, datasets=self.produced_data, ers=self.execrecords,
                                 runs={self.first_run, self.second_run})

    def test_reused_run_build_removal_plan(self):
        """Removing a reused Run should leave reused data/ExecRecords alone."""
        self.removal_plan_tester(self.second_run, runs={self.second_run})

    def test_input_data_build_removal_plan(self):
        """Removing input data to a Run should remove any Run started from it."""
        self.removal_plan_tester(
            self.input_DS,
            datasets=self.produced_data.union({self.input_DS}),
            ers=self.execrecords,
            runs={self.first_run, self.second_run}
        )

    def test_external_input_build_removal_plan(self):
        """Removing an input dataset that is externally-backed."""
        working_dir = tempfile.mkdtemp()
        efd = ExternalFileDirectory(
            name="TestBuildRemovalPlanEFD",
            path=working_dir
        )
        efd.save()

        ext_path = "ext.txt"
        self.input_DS.dataset_file.open()
        with self.input_DS.dataset_file:
            with open(os.path.join(working_dir, ext_path), "wb") as f:
                f.write(self.input_DS.dataset_file.read())

        # Mark the input dataset as externally-backed.
        self.input_DS.externalfiledirectory = efd
        self.input_DS.external_path = ext_path
        self.input_DS.save()

        all_data = self.produced_data
        all_data.add(self.input_DS)

        self.removal_plan_tester(
            self.input_DS,
            datasets=self.produced_data.union({self.input_DS}),
            ers=self.execrecords,
            runs={self.first_run, self.second_run},
            external_files={self.input_DS}
        )

    def test_produced_data_build_removal_plan(self):
        """Removing data produced by the Run should have the same effect as removing the Run itself."""
        produced_dataset = list(self.produced_data)[0]

        self.removal_plan_tester(produced_dataset, datasets=self.produced_data, ers=self.execrecords,
                                 runs={self.first_run, self.second_run})

    def test_step_ER_build_removal_plan(self):
        """Removing the ExecRecord of the first RunStep should be like removing the whole Run."""
        first_step_er = self.first_run.runsteps.get(pipelinestep__step_num=1).execrecord

        self.removal_plan_tester(first_step_er, datasets=self.produced_data, ers=self.execrecords,
                                 runs={self.first_run, self.second_run})

    def test_rsic_ER_build_removal_plan(self):
        """Removing the ExecRecord of a RunSIC should be like removing the whole Run."""
        first_rsic_er = self.first_run.runsteps.get(pipelinestep__step_num=1).RSICs.first().execrecord

        self.removal_plan_tester(first_rsic_er, datasets=self.produced_data, ers=self.execrecords,
                                 runs={self.first_run, self.second_run})

    def test_roc_ER_build_removal_plan(self):
        """Removing the ExecRecord of a RunOutputCable should be like removing the whole Run."""
        first_roc_er = self.first_run.runoutputcables.first().execrecord

        self.removal_plan_tester(first_roc_er, datasets=self.produced_data, ers=self.execrecords,
                                 runs={self.first_run, self.second_run})

    def test_pipeline_build_removal_plan(self):
        """Removing a Pipeline."""
        self.removal_plan_tester(self.noop_pl, datasets=self.produced_data, ers=self.execrecords,
                                 runs={self.first_run, self.second_run}, pipelines={self.noop_pl, self.p_nested})

    def test_nested_pipeline_build_removal_plan(self):
        """Removing a nested Pipeline."""
        self.removal_plan_tester(self.p_nested, pipelines={self.p_nested})

    def test_pipelinefamily_build_removal_plan(self):
        """Removing a PipelineFamily removes everything that goes along with it."""
        self.removal_plan_tester(self.noop_plf, datasets=self.produced_data, ers=self.execrecords,
                                 runs={self.first_run, self.second_run}, pipelines={self.noop_pl, self.p_nested},
                                 pfs={self.noop_plf})

    def test_method_build_removal_plan(self):
        """Removing a Method removes all Pipelines containing it and all of the associated stuff."""
        self.removal_plan_tester(
            self.nuc_seq_noop,
            datasets=self.produced_data.union({self.two_step_intermediate_data, self.two_step_output_data}),
            ers=self.execrecords.union(self.two_step_execrecords),
            runs={self.first_run, self.second_run, self.two_step_run},
            pipelines={self.noop_pl, self.p_nested, self.two_step_noop_pl},
            methods={self.nuc_seq_noop}
        )

    def test_methodfamily_build_removal_plan(self):
        """Removing a MethodFamily."""
        self.removal_plan_tester(
            self.nuc_seq_noop_mf,
            datasets=self.produced_data.union(
                {self.two_step_intermediate_data, self.two_step_output_data}
            ),
            ers=self.execrecords.union(self.two_step_execrecords),
            runs={self.first_run, self.second_run, self.two_step_run},
            pipelines={self.noop_pl, self.p_nested, self.two_step_noop_pl},
            methods={self.nuc_seq_noop},
            mfs={self.nuc_seq_noop_mf}
        )

    def test_crr_build_removal_plan(self):
        """Removing a CodeResourceRevision."""
        self.removal_plan_tester(
            self.noop_crr,
            datasets=self.produced_data.union({self.two_step_intermediate_data, self.two_step_output_data}),
            ers=self.execrecords.union(self.two_step_execrecords),
            runs={self.first_run, self.second_run, self.two_step_run},
            pipelines={self.noop_pl, self.p_nested, self.two_step_noop_pl},
            methods={self.nuc_seq_noop, self.raw_pass_through},
            crrs={self.noop_crr}
        )

    def test_method_nodep_build_removal_plan(self):
        """Removing a Method that has CodeResourceDependencies leaves it alone."""
        self.removal_plan_tester(self.raw_pass_through, methods={self.raw_pass_through})

    def test_cr_build_removal_plan(self):
        """Removing a CodeResource removes its revisions."""
        self.removal_plan_tester(
            self.noop_cr,
            datasets=self.produced_data.union({self.two_step_intermediate_data, self.two_step_output_data}),
            ers=self.execrecords.union(self.two_step_execrecords),
            runs={self.first_run, self.second_run, self.two_step_run},
            pipelines={self.noop_pl, self.p_nested, self.two_step_noop_pl},
            methods={self.nuc_seq_noop, self.raw_pass_through},
            crrs={self.noop_crr},
            crs={self.noop_cr}
        )

    def test_cdt_build_removal_plan(self):
        """Removing a CompoundDatatype."""
        all_data = self.produced_data.union(
            {
                self.input_DS,
                self.two_step_input_dataset,
                self.two_step_intermediate_data,
                self.two_step_output_data
            }
        )
        self.removal_plan_tester(
            self.one_col_nuc_seq,
            datasets=all_data,
            ers=self.execrecords.union(self.two_step_execrecords),
            runs={self.first_run, self.second_run, self.two_step_run},
            pipelines={self.noop_pl, self.p_nested, self.two_step_noop_pl},
            methods={self.nuc_seq_noop},
            cdts={self.one_col_nuc_seq}
        )

    def test_dt_build_removal_plan(self):
        """Removing a Datatype."""
        all_data = self.produced_data.union(
            {
                self.input_DS,
                self.two_step_input_dataset,
                self.two_step_intermediate_data,
                self.two_step_output_data
            }
        )
        self.removal_plan_tester(
            self.nuc_seq,
            datasets=all_data,
            ers=self.execrecords.union(self.two_step_execrecords),
            runs={self.first_run, self.second_run, self.two_step_run},
            pipelines={self.noop_pl, self.p_nested, self.two_step_noop_pl},
            methods={self.nuc_seq_noop},
            cdts={self.one_col_nuc_seq},
            dts={self.nuc_seq}
        )

    def remove_tester(self, obj_to_remove):
        removal_plan = obj_to_remove.build_removal_plan()

        dataset_pks = [x.pk for x in removal_plan["Datasets"]]
        er_pks = [x.pk for x in removal_plan["ExecRecords"]]
        run_pks = [x.pk for x in removal_plan["Runs"]]
        pipeline_pks = [x.pk for x in removal_plan["Pipelines"]]
        pf_pks = [x.pk for x in removal_plan["PipelineFamilies"]]
        method_pks = [x.pk for x in removal_plan["Methods"]]
        mf_pks = [x.pk for x in removal_plan["MethodFamilies"]]
        cdt_pks = [x.pk for x in removal_plan["CompoundDatatypes"]]
        dt_pks = [x.pk for x in removal_plan["Datatypes"]]
        crr_pks = [x.pk for x in removal_plan["CodeResourceRevisions"]]
        cr_pks = [x.pk for x in removal_plan["CodeResources"]]

        obj_to_remove.remove()
        self.assertFalse(Dataset.objects.filter(pk__in=dataset_pks).exists())
        self.assertFalse(ExecRecord.objects.filter(pk__in=er_pks).exists())
        self.assertFalse(Run.objects.filter(pk__in=run_pks).exists())
        self.assertFalse(Pipeline.objects.filter(pk__in=pipeline_pks).exists())
        self.assertFalse(PipelineFamily.objects.filter(pk__in=pf_pks).exists())
        self.assertFalse(Method.objects.filter(pk__in=method_pks).exists())
        self.assertFalse(MethodFamily.objects.filter(pk__in=mf_pks).exists())
        self.assertFalse(CompoundDatatype.objects.filter(pk__in=cdt_pks).exists())
        self.assertFalse(Datatype.objects.filter(pk__in=dt_pks).exists())
        self.assertFalse(CodeResourceRevision.objects.filter(pk__in=crr_pks).exists())
        self.assertFalse(CodeResource.objects.filter(pk__in=cr_pks).exists())

    def test_pipeline_remove(self):
        """
        Removing a Pipeline should remove all Runs created from it.
        """
        self.remove_tester(self.noop_pl)

    def test_nested_pipeline_remove(self):
        """Removing a nested Pipeline."""
        self.remove_tester(self.p_nested)

    def test_pipelinefamily_remove(self):
        """Removing a PipelineFamily should remove all Pipelines in it."""
        self.remove_tester(self.noop_plf)

    def test_method_remove(self):
        """Removing a Method should remove the Pipelines containing it."""

        self.remove_tester(self.nuc_seq_noop)

    def test_methodfamily_remove(self):
        """Removing a MethodFamily should remove the Methods in it."""
        self.remove_tester(self.nuc_seq_noop_mf)

    def test_crr_remove(self):
        """Removing a CodeResourceRevision should remove the Methods using it, and its dependencies."""
        self.remove_tester(self.noop_crr)

    def test_method_nodep_remove(self):
        """Removing a Method that has dependencies leaves the dependencies alone."""
        self.remove_tester(self.raw_pass_through)

    def test_cr_remove(self):
        """Removing a CodeResource should remove the CodeResourceRevisions using it."""
        self.remove_tester(self.noop_cr)

    def test_cdt_remove(self):
        """Removing a CDT should remove the Methods/Pipelines/Datasets using it."""
        self.remove_tester(self.one_col_nuc_seq)

    def test_datatype_remove(self):
        """Removing a Datatype should remove the CDTs that use it."""
        self.remove_tester(self.nuc_seq)

    def test_dataset_remove(self):
        """Removing a Dataset should remove anything that touches it."""
        self.remove_tester(self.input_DS)

    def test_run_remove(self):
        """Removing a Run."""
        self.remove_tester(self.first_run)

    def test_reused_run_remove(self):
        """Removing a reused Run."""
        self.remove_tester(self.second_run)

    def test_produced_data_remove(self):
        """Removing data produced by the Run should have the same effect as removing the Run itself."""
        produced_dataset = list(self.produced_data)[0]
        self.remove_tester(produced_dataset)

    def test_step_ER_remove(self):
        """Removing the ExecRecord of the first RunStep should be like removing the whole Run."""
        first_step_er = self.first_run.runsteps.get(pipelinestep__step_num=1).execrecord
        self.remove_tester(first_step_er)

    def test_rsic_ER_remove(self):
        """Removing the ExecRecord of a RunSIC should be like removing the whole Run."""
        first_rsic_er = self.first_run.runsteps.get(pipelinestep__step_num=1).RSICs.first().execrecord
        self.remove_tester(first_rsic_er)

    def test_roc_ER_remove(self):
        """Removing the ExecRecord of a RunOutputCable should be like removing the whole Run."""
        first_roc_er = self.first_run.runoutputcables.first().execrecord
        self.remove_tester(first_roc_er)

    def dataset_redaction_plan_tester(self, dataset_to_redact, datasets=None, output_logs=None, error_logs=None,
                                      return_codes=None, external_files=None):
        redaction_plan = dataset_to_redact.build_redaction_plan()

        # The following ExecRecords should also be in the redaction plan.
        redaction_plan_execrecords = set()
        dataset_set = datasets or set()
        for dataset in dataset_set:
            for eri in dataset.execrecordins.all():
                redaction_plan_execrecords.add(eri.execrecord)

        self.assertSetEqual(redaction_plan["Datasets"], set(datasets) if datasets is not None else set())
        self.assertSetEqual(redaction_plan["OutputLogs"], set(output_logs) if output_logs is not None else set())
        self.assertSetEqual(redaction_plan["ErrorLogs"], set(error_logs) if error_logs is not None else set())
        self.assertSetEqual(redaction_plan["ReturnCodes"], set(return_codes) if return_codes is not None else set())
        self.assertSetEqual(redaction_plan["ExecRecords"], redaction_plan_execrecords)
        self.assertSetEqual(redaction_plan["ExternalFiles"],
                            set(external_files) if external_files is not None else set())

    def dataset_redaction_tester(self, dataset_to_redact):
        redaction_plan = dataset_to_redact.build_redaction_plan()
        dataset_to_redact.redact()
        self.redaction_tester_helper(redaction_plan)

    def redaction_tester_helper(self, redaction_plan):
        # Check that all of the objects in the plan, and the RunComponents/ExecRecords that
        # reference them, got redacted.
        for dataset in redaction_plan["Datasets"]:
            reloaded_dataset = Dataset.objects.get(pk=dataset.pk)
            self.assertTrue(reloaded_dataset.is_redacted())

        execlogs_affected = redaction_plan["OutputLogs"].union(
            redaction_plan["ErrorLogs"]).union(redaction_plan["ReturnCodes"])
        for log in execlogs_affected:
            # noinspection PyUnresolvedReferences
            reloaded_log = ExecLog.objects.get(pk=log.pk)
            if log in redaction_plan["OutputLogs"]:
                self.assertTrue(reloaded_log.methodoutput.is_output_redacted())
            if log in redaction_plan["ErrorLogs"]:
                self.assertTrue(reloaded_log.methodoutput.is_error_redacted())
            if log in redaction_plan["ReturnCodes"]:
                self.assertTrue(reloaded_log.methodoutput.is_code_redacted())

            self.assertTrue(reloaded_log.is_redacted())
            self.assertTrue(reloaded_log.record.is_redacted())
            if reloaded_log.generated_execrecord():
                self.assertTrue(reloaded_log.execrecord.is_redacted())

        for er in redaction_plan["ExecRecords"]:
            self.assertTrue(er.is_redacted())
            for rc in er.used_by_components.all():
                self.assertTrue(rc.is_redacted())

    def log_redaction_plan_tester(self, log_to_redact, output_log=True, error_log=True, return_code=True):
        output_already_redacted = log_to_redact.methodoutput.is_output_redacted()
        error_already_redacted = log_to_redact.methodoutput.is_error_redacted()
        code_already_redacted = log_to_redact.methodoutput.is_code_redacted()

        redaction_plan = log_to_redact.build_redaction_plan(output_log=output_log, error_log=error_log,
                                                            return_code=return_code)

        self.assertSetEqual(redaction_plan["Datasets"], set())
        self.assertSetEqual(redaction_plan["ExecRecords"], set())
        self.assertSetEqual(redaction_plan["OutputLogs"],
                            {log_to_redact} if output_log and not output_already_redacted else set())
        self.assertSetEqual(redaction_plan["ErrorLogs"],
                            {log_to_redact} if error_log and not error_already_redacted else set())
        self.assertSetEqual(redaction_plan["ReturnCodes"],
                            {log_to_redact} if return_code and not code_already_redacted else set())

    def log_redaction_tester(self, log_to_redact, output_log=True, error_log=True, return_code=True):
        redaction_plan = log_to_redact.build_redaction_plan(output_log, error_log, return_code)

        if output_log:
            log_to_redact.methodoutput.redact_output_log()
        if error_log:
            log_to_redact.methodoutput.redact_error_log()
        if return_code:
            log_to_redact.methodoutput.redact_return_code()

        self.redaction_tester_helper(redaction_plan)

    def test_input_dataset_build_redaction_plan(self):
        """Test redaction of the input dataset to a Run."""
        logs_to_redact = {self.step_log}

        self.dataset_redaction_plan_tester(
            self.input_DS,
            datasets=self.produced_data.union({self.input_DS}),
            output_logs=logs_to_redact,
            error_logs=logs_to_redact,
            return_codes=logs_to_redact
        )

    def test_external_input_build_redaction_plan(self):
        """Redacting an input dataset that is externally-backed."""
        working_dir = tempfile.mkdtemp()
        efd = ExternalFileDirectory(
            name="TestBuildRemovalPlanEFD",
            path=working_dir
        )
        efd.save()

        ext_path = "ext.txt"
        self.input_DS.dataset_file.open()
        with self.input_DS.dataset_file:
            with open(os.path.join(working_dir, ext_path), "wb") as f:
                f.write(self.input_DS.dataset_file.read())

        # Mark the input dataset as externally-backed.
        self.input_DS.externalfiledirectory = efd
        self.input_DS.external_path = ext_path
        self.input_DS.save()

        logs_to_redact = {self.step_log}

        self.dataset_redaction_plan_tester(
            self.input_DS,
            datasets=self.produced_data.union({self.input_DS}),
            output_logs=logs_to_redact,
            error_logs=logs_to_redact,
            return_codes=logs_to_redact,
            external_files={self.input_DS}
        )

    def test_input_dataset_redact(self):
        self.dataset_redaction_tester(self.input_DS)

    def test_dataset_redact_idempotent(self):
        """Redacting an already-redacted Dataset should give an empty redaction plan."""
        self.input_DS.redact()
        # All of the parameters to this function are None, indicating nothing gets redacted.
        self.dataset_redaction_plan_tester(self.input_DS)

    def test_produced_dataset_build_redaction_plan(self):
        """Redacting produced data."""
        # The run we're dealing with has a single step, and that's the only produced data.
        produced_dataset = list(self.produced_data)[0]

        self.dataset_redaction_plan_tester(
            produced_dataset,
            datasets=self.produced_data
        )

    def test_produced_dataset_redact(self):
        produced_dataset = list(self.produced_data)[0]
        self.dataset_redaction_tester(produced_dataset)

    def test_intermediate_dataset_build_redaction_plan(self):
        """Redacting a Dataset from the middle of a Run only redacts the stuff following it."""
        logs_to_redact = {self.two_step_run.runsteps.get(pipelinestep__step_num=2).log}

        self.dataset_redaction_plan_tester(
            self.two_step_intermediate_data,
            datasets={self.two_step_intermediate_data, self.two_step_output_data},
            output_logs=logs_to_redact,
            error_logs=logs_to_redact,
            return_codes=logs_to_redact
        )

    def test_intermediate_dataset_redact(self):
        self.dataset_redaction_tester(self.two_step_intermediate_data)

    def test_step_log_build_redaction_plan_remove_all(self):
        # There's only one step in self.first_run.
        self.log_redaction_plan_tester(
            self.step_log, True, True, True
        )

    def test_step_log_redact_all(self):
        self.log_redaction_tester(
            self.step_log, True, True, True
        )

    def test_step_log_build_redaction_plan_redact_output_log(self):
        self.log_redaction_plan_tester(
            self.step_log, output_log=True, error_log=False, return_code=False
        )

    def test_step_log_redact_output_log(self):
        self.log_redaction_tester(
            self.step_log, output_log=True, error_log=False, return_code=False
        )

    def test_step_log_build_redaction_plan_redact_error_log(self):
        self.log_redaction_plan_tester(
            self.step_log, output_log=False, error_log=True, return_code=False
        )

    def test_step_log_redact_error_log(self):
        self.log_redaction_tester(
            self.step_log, output_log=False, error_log=True, return_code=False
        )

    def test_step_log_build_redaction_plan_redact_return_code(self):
        self.log_redaction_plan_tester(
            self.step_log, output_log=False, error_log=False, return_code=True
        )

    def test_step_log_redact_return_code(self):
        self.log_redaction_tester(
            self.step_log, output_log=False, error_log=False, return_code=True
        )

    def test_step_log_build_redaction_plan_redact_partially_redacted(self):
        """Redacting something that's been partially redacted should take that into account."""
        self.step_log.methodoutput.redact_output_log()
        self.log_redaction_plan_tester(
            self.step_log, output_log=True, error_log=True, return_code=True
        )


@skipIfDBFeature('is_mocked')
class DatasetWithFileTests(TestCase):

    def setUp(self):
        tools.create_librarian_test_environment(self)

    def tearDown(self):
        tools.clean_up_all_files()

    def test_Dataset_check_MD5(self):
        old_md5 = "7dc85e11b5c02e434af5bd3b3da9938e"
        new_md5 = "d41d8cd98f00b204e9800998ecf8427e"

        self.assertEqual(self.raw_dataset.compute_md5(), old_md5)

        # Initially, no change to the raw dataset has occured, so the md5 check will pass
        self.assertEqual(self.raw_dataset.clean(), None)

        # The contents of the file are changed, disrupting file integrity
        self.raw_dataset.dataset_file.close()
        self.raw_dataset.dataset_file.open(mode='w')
        self.raw_dataset.dataset_file.close()
        self.assertRaisesRegexp(ValidationError,
                                re.escape('File integrity of "{}" lost. Current checksum "{}" does not equal expected '
                                          'checksum "{}"'.format(self.raw_dataset, new_md5, old_md5)),
                                self.raw_dataset.clean)

    def test_Dataset_filename_MD5_clash(self):
        ds1, ds2 = Dataset.objects.all()[:2]
        ds1.name = ds2.name
        ds1.MD5_checksum = ds2.MD5_checksum
        ds1.save()
        msg = "A Dataset with that name and MD5 already exists"
        self.assertRaisesRegexp(ValidationError, msg, ds1.validate_uniqueness_on_upload)


class DatasetApiMockTests(BaseTestCases.ApiTestCase):

    def setUp(self):
        self.mock_viewset(DatasetViewSet)
        super(DatasetApiMockTests, self).setUp()
        # num_cols = 12

        patcher = mocked_relations(CompoundDatatype)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.list_path = reverse("dataset-list")
        self.list_view, _, _ = resolve(self.list_path)

        self.detail_pk = 43
        self.detail_path = reverse("dataset-detail",
                                   kwargs={'pk': self.detail_pk})
        self.redaction_path = reverse("dataset-redaction-plan",
                                      kwargs={'pk': self.detail_pk})
        self.removal_path = reverse("dataset-removal-plan",
                                    kwargs={'pk': self.detail_pk})

        self.detail_view, _, _ = resolve(self.detail_path)
        self.redaction_view, _, _ = resolve(self.redaction_path)
        self.removal_view, _, _ = resolve(self.removal_path)

        tz = timezone.get_current_timezone()
        apples_date = timezone.make_aware(datetime(2017, 1, 1), tz)
        apples = Dataset(pk=42,
                         name='apples',
                         description='chosen',
                         date_created=apples_date,
                         user=self.kive_kive_user)
        cherries_date = timezone.make_aware(datetime(2017, 1, 2), tz)
        cherries = Dataset(pk=43,
                           name='cherries',
                           date_created=cherries_date,
                           MD5_checksum='1234',
                           user=self.kive_kive_user)
        bananas_date = timezone.make_aware(datetime(2017, 1, 3), tz)
        bananas = Dataset(pk=44,
                          name='bananas',
                          date_created=bananas_date,
                          file_source=RunStep(),
                          user=self.kive_kive_user)
        Dataset.objects.add(apples,
                            cherries,
                            bananas)
        apples.structure = DatasetStructure(compounddatatype_id=5)
        bananas.structure = DatasetStructure(compounddatatype_id=6)

    def test_list(self):
        """
        Test the API list view.
        """
        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 3)
        self.assertEquals(response.data[2]['name'], 'bananas')

    def test_filter_smart(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=smart&filters[0][val]=ch")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 2)
        self.assertEquals(response.data[0]['name'], 'cherries')
        self.assertEquals(response.data[1]['description'], 'chosen')

    def test_filter_name(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=name&filters[0][val]=ch")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['name'], 'cherries')

    def test_filter_description(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=description&filters[0][val]=ch")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['description'], 'chosen')

    def test_filter_user(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=user&filters[0][val]=kive")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 3)

    def test_filter_uploaded(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=uploaded")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 2)

    def test_filter_raw(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=cdt")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['name'], 'cherries')

    def test_filter_cdt(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=cdt&filters[0][val]=5")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['name'], 'apples')

    def test_filter_md5(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=md5&filters[0][val]=1234")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['name'], 'cherries')

    def test_filter_date(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=createdafter&filters[0][val]=02 Jan 2017 0:00" +
            "&filters[1][key]=createdbefore&filters[1][val]=02 Jan 2017 0:00")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['name'], 'cherries')

    def test_filter_unknown(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=bogus&filters[0][val]=kive")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals({u'detail': u'Unknown filter key: bogus'},
                          response.data)


@skipIfDBFeature('is_mocked')
class DatasetApiTests(BaseTestCases.ApiTestCase):

    def setUp(self):
        super(DatasetApiTests, self).setUp()
        num_cols = 12

        self.list_path = reverse("dataset-list")
        # This should equal librarian.ajax.DatasetViewSet.as_view({"get": "list"}).
        self.list_view, _, _ = resolve(self.list_path)

        with tempfile.NamedTemporaryFile() as f:
            data = ','.join(map(str, range(num_cols)))
            f.write(data.encode())
            f.seek(0)
            self.test_dataset = Dataset.create_dataset(
                file_path=None,
                user=self.kive_user,
                users_allowed=None,
                groups_allowed=None,
                cdt=None,
                keep_file=True,
                name="Test dataset",
                description="Test data for a test that tests test data",
                file_source=None,
                check=True,
                file_handle=f,
            )
            self.test_dataset_path = "{}{}/".format(self.list_path,
                                                    self.test_dataset.pk)
            self.n_preexisting_datasets = 1

        self.detail_pk = self.test_dataset.pk
        self.detail_path = reverse("dataset-detail",
                                   kwargs={'pk': self.detail_pk})
        self.redaction_path = reverse("dataset-redaction-plan",
                                      kwargs={'pk': self.detail_pk})
        self.removal_path = reverse("dataset-removal-plan",
                                    kwargs={'pk': self.detail_pk})

        self.detail_view, _, _ = resolve(self.detail_path)
        self.redaction_view, _, _ = resolve(self.redaction_path)
        self.removal_view, _, _ = resolve(self.removal_path)

    def tearDown(self):
        for d in Dataset.objects.all():
            d.dataset_file.delete()

    def test_dataset_add(self):
        """
        Test adding a Dataset via the API.

        Each dataset must have unique content.
        """
        num_cols = 12
        num_files = 2

        with tempfile.TemporaryFile() as f:
            data = ','.join(map(str, range(num_cols)))
            f.write(data.encode())
            for i in range(num_files):
                f.seek(0, FROM_FILE_END)
                f.write('data file {}\n'.format(i).encode())
                f.seek(0)
                request = self.factory.post(
                    self.list_path,
                    {
                        'name': "My cool file %d" % i,
                        'description': 'A really cool file',
                        # No CompoundDatatype -- this is raw.
                        'dataset_file': f
                    }
                )

                force_authenticate(request, user=self.kive_user)
                resp = self.list_view(request).render().data

                self.assertIsNone(resp.get('errors'))
                self.assertEquals(resp['name'], "My cool file %d" % i)

        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.kive_user)
        resp = self.list_view(request).data

        self.assertEquals(len(resp), num_files + self.n_preexisting_datasets)
        self.assertEquals(resp[-1]['description'],
                          "Test data for a test that tests test data")

    def test_dataset_add_duplicate(self):
        """
        Test adding a duplicate Dataset via the API.

        Each dataset must have unique content.
        """
        num_cols = 12

        with tempfile.TemporaryFile() as f:
            data = ','.join(map(str, range(num_cols)))
            f.write(data.encode())
            f.seek(0)

            # First, we add this file and it works.
            request = self.factory.post(
                self.list_path,
                {
                    'name': "Original",
                    'description': 'Totes unique',
                    # No CompoundDatatype -- this is raw.
                    'dataset_file': f
                }
            )
            force_authenticate(request, user=self.kive_user)
            self.list_view(request).render()

            # Now we add the same file again.
            request = self.factory.post(
                self.list_path,
                {
                    'name': "CarbonCopy",
                    'description': "Maybe not so unique",
                    'dataset_file': f
                }
            )
            force_authenticate(request, user=self.kive_user)
            resp = self.list_view(request).render().data

        self.assertEqual({'dataset_file': [u'The submitted file is empty.']},
                         resp)

    def test_dataset_removal_plan(self):
        request = self.factory.get(self.removal_path)
        force_authenticate(request, user=self.kive_user)
        response = self.removal_view(request, pk=self.detail_pk)

        self.assertEquals(response.data['Datasets'], 1)
        self.assertEquals(response.data['CompoundDatatypes'], 0)

    def test_dataset_removal(self):
        start_count = Dataset.objects.all().count()

        request = self.factory.delete(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.status_code, status.HTTP_204_NO_CONTENT)

        end_count = Dataset.objects.all().count()
        self.assertEquals(end_count, start_count - 1)

    def test_dataset_redaction_plan(self):
        request = self.factory.get(self.redaction_path)
        force_authenticate(request, user=self.kive_user)
        response = self.redaction_view(request, pk=self.detail_pk)
        self.assertEquals(response.data['Datasets'], 1)
        self.assertEquals(response.data['OutputLogs'], 0)

    def test_dataset_redaction(self):

        request = self.factory.patch(self.detail_path,
                                     {'is_redacted': "true"})
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.status_code, status.HTTP_200_OK)

        dataset = Dataset.objects.get(pk=self.detail_pk)
        self.assertTrue(dataset.is_redacted())

    def test_dataset_purge(self):
        request = self.factory.patch(self.detail_path,
                                     json.dumps({'dataset_file': None}),
                                     content_type='application/json')
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.status_code, status.HTTP_200_OK)

        dataset = Dataset.objects.get(pk=self.detail_pk)
        self.assertFalse(dataset.has_data())

    def test_dataset_purge_again(self):
        # Purge the dataset file.
        Dataset.objects.get(pk=self.detail_pk).dataset_file.delete(save=True)

        # Now send a request to purge it again. Should do nothing.
        request = self.factory.patch(self.detail_path,
                                     json.dumps({'dataset_file': None}),
                                     content_type='application/json')
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.status_code, status.HTTP_200_OK)

        dataset = Dataset.objects.get(pk=self.detail_pk)
        self.assertFalse(dataset.has_data())

    def test_dataset_view_purged(self):
        dataset = Dataset.objects.get(id=self.detail_pk)
        dataset.dataset_file.delete(save=True)

        request = self.factory.get(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(
            response.data['description'],
            "Test data for a test that tests test data")
        self.assertFalse(response.data['has_data'])
        self.assertFalse(response.data['is_redacted'])


@skipIfDBFeature('is_mocked')
class DatasetSerializerTests(TestCase):
    """
    Tests of DatasetSerializer.
    """
    def setUp(self):
        self.factory = APIRequestFactory()
        self.list_path = reverse("dataset-list")

        # This defines a user named "john" which is now accessible as self.myUser.
        tools.create_metadata_test_environment(self)
        self.kive_user = kive_user()
        self.duck_context = DuckContext()

        num_cols = 12
        self.raw_file_contents = ','.join(map(str, range(num_cols))).encode()

        # A CompoundDatatype that belongs to the Kive user.
        self.kive_CDT = CompoundDatatype(user=self.kive_user)
        self.kive_CDT.save()
        self.kive_CDT.members.create(
            datatype=self.string_dt,
            column_name="col1",
            column_idx=1
        )
        self.kive_CDT.full_clean()

        self.kive_file_contents = """col1
foo
bar
baz
"""

        self.data_to_serialize = {
            "name": "SerializedData",
            "description": "Dataset for testing deserialization",
            "users_allowed": [],
            "groups_allowed": []
        }

        # An external file directory.
        self.working_dir = tempfile.mkdtemp()
        self.efd = ExternalFileDirectory(
            name="WorkingDirectory",
            path=self.working_dir
        )
        self.efd.save()

        # An external file.
        _, self.ext_fn = tempfile.mkstemp(dir=self.working_dir)
        with open(self.ext_fn, "wb") as f:
            f.write(self.raw_file_contents)

        self.csv_file_temp_open_mode = "w+t"
        if dsix.PY2:
            self.csv_file_temp_open_mode = "w+b"

    def tearDown(self):
        shutil.rmtree(self.working_dir)

    def test_validate(self):
        """
        Test validating a new Dataset.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.raw_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f, name="bla")

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            self.assertTrue(ds.is_valid())

    def test_validate_with_users_allowed(self):
        """
        Test validating a new Dataset with users allowed.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.raw_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f, name="bla")
            self.data_to_serialize["users_allowed"].append(self.myUser.username)

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            self.assertTrue(ds.is_valid())

    def test_validate_with_groups_allowed(self):
        """
        Test validating a new Dataset with groups allowed.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.raw_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f, name="bla")
            self.data_to_serialize["groups_allowed"].append(everyone_group().name)

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            self.assertTrue(ds.is_valid())

    def test_validate_with_CDT(self):
        """
        Test validating a Dataset with a CDT.
        """
        with tempfile.TemporaryFile(self.csv_file_temp_open_mode) as f:
            f.write(self.kive_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f, name="bla")
            self.data_to_serialize["compounddatatype"] = self.kive_CDT.pk

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            self.assertTrue(ds.is_valid())

    def test_validate_ineligible_CDT(self):
        """
        Test validating a Dataset with a CDT that the user doesn't have access to.
        """
        with tempfile.TemporaryFile(self.csv_file_temp_open_mode) as f:
            f.write(self.kive_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f, name="bla")
            self.data_to_serialize["compounddatatype"] = self.kive_CDT.pk

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=DuckContext(self.myUser)
            )
            self.assertFalse(ds.is_valid())
            self.assertEquals(len(ds.errors["compounddatatype"]), 1)

    def test_validate_externally_backed(self):
        """
        Test validating a new Dataset with external backing.
        """
        self.data_to_serialize["externalfiledirectory"] = self.efd.name
        self.data_to_serialize["external_path"] = self.ext_fn
        ds = DatasetSerializer(
            data=self.data_to_serialize,
            context=self.duck_context
        )
        self.assertTrue(ds.is_valid())

    def test_validate_externally_backed_no_efd(self):
        """
        If external_path is present, externalfiledirectory should be also.
        """
        self.data_to_serialize["external_path"] = self.ext_fn
        ds = DatasetSerializer(
            data=self.data_to_serialize,
            context=self.duck_context
        )
        self.assertFalse(ds.is_valid())
        self.assertListEqual(ds.errors["non_field_errors"],
                             ["externalfiledirectory must be specified"])

    def test_validate_externally_backed_no_external_path(self):
        """
        If externalfiledirectory is present, external_path should be also.
        """
        self.data_to_serialize["externalfiledirectory"] = self.efd.name
        ds = DatasetSerializer(
            data=self.data_to_serialize,
            context=self.duck_context
        )
        self.assertFalse(ds.is_valid())
        self.assertListEqual(ds.errors["non_field_errors"],
                             ["external_path must be specified"])

    def test_validate_dataset_file_specified(self):
        """
        If dataset_file is specified, external_path and externalfiledirectory should not be.
        """
        self.data_to_serialize["externalfiledirectory"] = self.efd.name
        self.data_to_serialize["external_path"] = self.ext_fn

        with tempfile.TemporaryFile() as f:
            f.write(self.raw_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f, name="bla")

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            self.assertFalse(ds.is_valid())
            self.assertSetEqual(
                set([str(e) for e in ds.errors["non_field_errors"]]),
                {
                    "external_path should not be specified if dataset_file is",
                    "externalfiledirectory should not be specified if dataset_file is"
                }
            )

    def test_create(self):
        """
        Test creating a Dataset.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.raw_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f, name="bla")

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            ds.is_valid()
            dataset = ds.save()

            # Probe the Dataset to make sure everything looks fine.
            self.assertEquals(dataset.name, self.data_to_serialize["name"])
            self.assertEquals(dataset.description, self.data_to_serialize["description"])
            self.assertIsNone(dataset.compounddatatype)
            self.assertEquals(dataset.user, self.kive_user)
            self.assertTrue(bool(dataset.dataset_file))

    def test_create_do_not_retain(self):
        """
        Test creating a Dataset but without retaining a file in the DB.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.raw_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f, name="bla")
            self.data_to_serialize["save_in_db"] = False

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            ds.is_valid()
            dataset = ds.save()

            # Probe the Dataset to make sure everything looks fine.
            self.assertEquals(dataset.name, self.data_to_serialize["name"])
            self.assertEquals(dataset.description, self.data_to_serialize["description"])
            self.assertIsNone(dataset.compounddatatype)
            self.assertEquals(dataset.user, self.kive_user)
            self.assertFalse(bool(dataset.dataset_file))

    def test_create_with_CDT(self):
        """
        Test creating a Dataset with a CDT.
        """
        with tempfile.TemporaryFile(mode="w+t") as f:
            f.write(self.kive_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f, name="bla")
            self.data_to_serialize["compounddatatype"] = self.kive_CDT.pk

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            ds.is_valid()
            dataset = ds.save()

            # Probe to make sure the CDT got set correctly.
            self.assertEquals(dataset.compounddatatype, self.kive_CDT)

    def test_create_with_users_allowed(self):
        """
        Test validating a new Dataset with users allowed.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.raw_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f, name="bla")
            self.data_to_serialize["users_allowed"].append(self.myUser.username)

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            ds.is_valid()
            dataset = ds.save()

            self.assertListEqual(list(dataset.users_allowed.all()),
                                 [self.myUser])

    def test_create_with_groups_allowed(self):
        """
        Test validating a new Dataset with groups allowed.
        """
        with tempfile.TemporaryFile() as f:
            f.write(self.raw_file_contents)
            f.seek(0)

            self.data_to_serialize["dataset_file"] = File(f, name="bla")
            self.data_to_serialize["groups_allowed"].append(everyone_group().name)

            ds = DatasetSerializer(
                data=self.data_to_serialize,
                context=self.duck_context
            )
            ds.is_valid()
            dataset = ds.save()

            self.assertListEqual(list(dataset.groups_allowed.all()),
                                 [everyone_group()])

    def test_create_externally_backed(self):
        """
        Test creating a Dataset from external data.
        """
        self.data_to_serialize["externalfiledirectory"] = self.efd.name
        self.data_to_serialize["external_path"] = os.path.basename(self.ext_fn)

        ds = DatasetSerializer(
            data=self.data_to_serialize,
            context=self.duck_context
        )
        ds.is_valid()
        dataset = ds.save()

        # Probe the Dataset to make sure everything looks fine.
        self.assertEquals(dataset.name, self.data_to_serialize["name"])
        self.assertEquals(dataset.description, self.data_to_serialize["description"])
        self.assertIsNone(dataset.compounddatatype)
        self.assertEquals(dataset.user, self.kive_user)
        self.assertEquals(dataset.external_path, os.path.basename(self.ext_fn))
        self.assertEquals(dataset.externalfiledirectory, self.efd)
        self.assertFalse(bool(dataset.dataset_file))

    def test_create_externally_backed_internal_copy(self):
        """
        Test creating a Dataset from external data and keeping an internal copy.
        """

        self.data_to_serialize["externalfiledirectory"] = self.efd.name
        self.data_to_serialize["external_path"] = os.path.basename(self.ext_fn)
        self.data_to_serialize["save_in_db"] = True

        ds = DatasetSerializer(
            data=self.data_to_serialize,
            context=self.duck_context
        )
        ds.is_valid()
        dataset = ds.save()

        # Probe the Dataset to make sure everything looks fine.
        self.assertEquals(dataset.name, self.data_to_serialize["name"])
        self.assertEquals(dataset.description, self.data_to_serialize["description"])
        self.assertIsNone(dataset.compounddatatype)
        self.assertEquals(dataset.user, self.kive_user)
        self.assertEquals(dataset.external_path, os.path.basename(self.ext_fn))
        self.assertEquals(dataset.externalfiledirectory, self.efd)
        self.assertTrue(bool(dataset.dataset_file))
        dataset.dataset_file.open("rb")
        with dataset.dataset_file:
            self.assertEquals(dataset.dataset_file.read(), self.raw_file_contents)


class ExternalFileDirectoryApiMockTests(BaseTestCases.ApiTestCase):
    def setUp(self):
        self.mock_viewset(ExternalFileDirectoryViewSet)
        super(ExternalFileDirectoryApiMockTests, self).setUp()

        self.list_path = reverse("externalfiledirectory-list")
        self.detail_pk = 43
        self.detail_path = reverse("externalfiledirectory-detail",
                                   kwargs={'pk': self.detail_pk})

        self.list_view, _, _ = resolve(self.list_path)
        self.detail_view, _, _ = resolve(self.detail_path)

        ExternalFileDirectory.objects.add(ExternalFileDirectory(id=42,
                                                                name="apples",
                                                                path="/bank/apples"),
                                          ExternalFileDirectory(id=43,
                                                                name="cherries",
                                                                path="/dock/cherries"),
                                          ExternalFileDirectory(id=44,
                                                                name="bananas",
                                                                path="/dock/bananas"))

    def test_list(self):
        """
        Test the API list view.
        """
        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 3)
        self.assertEquals(response.data[2]['name'], 'bananas')

    def test_filter_smart(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=smart&filters[0][val]=ban")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 2)
        self.assertEquals(response.data[0]['name'], 'bananas')
        self.assertEquals(response.data[1]['path'], '/bank/apples')

    def test_filter_name(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=name&filters[0][val]=ban")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['name'], 'bananas')

    def test_filter_path(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=path&filters[0][val]=bank")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['path'], '/bank/apples')

    def test_filter_unknown(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=bogus&filters[0][val]=kive")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals({u'detail': u'Unknown filter key: bogus'},
                          response.data)

    def test_detail(self):
        request = self.factory.get(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.data['name'], 'cherries')

    @patch('os.walk')
    def test_list_files(self, mock_walk):
        mock_walk.return_value = [('/dock/cherries', [], ['foo.txt', 'bar.txt'])]
        expected_data = {
            'url': u'http://testserver/api/externalfiledirectories/43/',
            'pk': 43,
            'list_files': [('/dock/cherries/foo.txt', '[cherries]/foo.txt'),
                           ('/dock/cherries/bar.txt', '[cherries]/bar.txt')],
            'name': u'cherries',
            'path': u'/dock/cherries'
        }
        path = reverse("externalfiledirectory-list-files",
                       kwargs={'pk': self.detail_pk})

        view, _, _ = resolve(path)
        request = self.factory.get(path)
        force_authenticate(request, user=self.kive_user)
        response = view(request, pk=self.detail_pk)
        self.assertDictEqual(expected_data, response.data)


@skipIfDBFeature('is_mocked')
class ExternalFileTests(TestCase):

    def setUp(self):
        tools.create_metadata_test_environment(self)

        self.working_dir = tempfile.mkdtemp()
        self.efd = ExternalFileDirectory(
            name="WorkingDirectory",
            path=self.working_dir
        )
        self.efd.save()

        self.ext1_path = "ext1.txt"
        self.ext1_contents = "First test file"
        with open(os.path.join(self.working_dir, self.ext1_path), "wb") as f:
            f.write(self.ext1_contents.encode())

        self.ext2_path = "ext2.txt"
        self.ext2_contents = "Second test file"
        with open(os.path.join(self.working_dir, self.ext2_path), "wb") as f:
            f.write(self.ext2_contents.encode())

        os.makedirs(os.path.join(self.working_dir, "ext_subdir"))
        os.makedirs(os.path.join(self.working_dir, "ext_subdir2"))

        self.ext_sub1_path = os.path.join("ext_subdir", "ext_sub1.txt")
        self.ext_sub1_contents = "Test file in subdirectory"
        with open(os.path.join(self.working_dir, self.ext_sub1_path), "wb") as f:
            f.write(self.ext_sub1_contents.encode())

        self.external_file_ds = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext1_path),
            user=self.myUser,
            externalfiledirectory=self.efd
        )
        self.external_file_ds_no_internal = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext1_path),
            user=self.myUser,
            keep_file=False,
            externalfiledirectory=self.efd
        )
        self.external_file_ds_subdir = Dataset.create_dataset(
            os.path.join(self.working_dir, "ext_subdir", "ext_sub1.txt"),
            user=self.myUser,
            externalfiledirectory=self.efd
        )
        self.non_external_dataset = Dataset(
            user=self.myUser,
            name="foo",
            description="Foo",
            dataset_file=ContentFile("Foo")
        )
        self.non_external_dataset.save()

    def tearDown(self):
        shutil.rmtree(self.working_dir)

    def test_save(self):
        """Calling save() normalizes the path."""
        new_working_dir = tempfile.mkdtemp()
        unnamed_efd = ExternalFileDirectory(name="TestSaveDir", path="{}/./".format(new_working_dir))
        unnamed_efd.save()
        self.assertEquals(unnamed_efd.path, os.path.normpath(new_working_dir))
        shutil.rmtree(new_working_dir)

    def test_list_files(self):
        expected_list = [
            (os.path.join(self.working_dir, self.ext1_path), "[WorkingDirectory]/{}".format(self.ext1_path)),
            (os.path.join(self.working_dir, "ext2.txt"), "[WorkingDirectory]/ext2.txt"),
            (os.path.join(self.working_dir, "ext_subdir", "ext_sub1.txt"),
             "[WorkingDirectory]/ext_subdir/ext_sub1.txt")
        ]
        self.assertSetEqual(set(expected_list), set(self.efd.list_files()))

    def test_create_dataset_external_file(self):
        """
        Create a Dataset from an external file, making a copy in the database.
        """
        external_file_ds = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext1_path),
            user=self.myUser,
            externalfiledirectory=self.efd
        )

        self.assertEquals(external_file_ds.external_path, self.ext1_path)

        external_file_ds.dataset_file.open("r")
        with external_file_ds.dataset_file:
            self.assertEquals(external_file_ds.dataset_file.read(), self.ext1_contents)

        with open(os.path.join(self.working_dir, self.ext1_path), "rb") as f:
            self.assertEquals(file_access_utils.compute_md5(f), external_file_ds.MD5_checksum)

    def test_create_dataset_external_file_no_internal_copy(self):
        """
        Create a Dataset from an external file without making a copy in the database.
        """
        external_file_ds = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext1_path),
            user=self.myUser,
            keep_file=False,
            externalfiledirectory=self.efd
        )

        self.assertEquals(external_file_ds.external_path, self.ext1_path)
        self.assertFalse(bool(external_file_ds.dataset_file))

        with open(os.path.join(self.working_dir, self.ext1_path), "rb") as f:
            self.assertEquals(file_access_utils.compute_md5(f), external_file_ds.MD5_checksum)

    def test_create_dataset_external_file_subdirectory(self):
        """
        Create a Dataset from an external file in a subdirectory of the external file directory.
        """
        external_file_ds = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext_sub1_path),
            user=self.myUser,
            externalfiledirectory=self.efd
        )

        self.assertEquals(external_file_ds.externalfiledirectory, self.efd)
        self.assertEquals(external_file_ds.external_path, self.ext_sub1_path)

        external_file_ds.dataset_file.open("r")
        with external_file_ds.dataset_file:
            self.assertEquals(external_file_ds.dataset_file.read(), self.ext_sub1_contents)

        with open(os.path.join(self.working_dir, self.ext_sub1_path), "rb") as f:
            self.assertEquals(file_access_utils.compute_md5(f), external_file_ds.MD5_checksum)

    def test_get_file_handle(self):
        """
        Test retrieving a file handle.
        """
        ext_sub1_path = os.path.join(self.working_dir, "ext_subdir", "ext_sub1.txt")
        external_file_ds = Dataset.create_dataset(
            ext_sub1_path,
            user=self.myUser,
            externalfiledirectory=self.efd
        )

        # Where possible get_file_handle uses the internal copy.
        with external_file_ds.get_open_file_handle("r") as data_handle:
            self.assertEquals(data_handle, external_file_ds.dataset_file)

        # It falls back on the external copy.
        external_file_ds.dataset_file.delete()
        with external_file_ds.get_open_file_handle('r') as external_file_handle:
            self.assertEquals(os.path.abspath(external_file_handle.name), ext_sub1_path)

    def test_get_file_handle_subdirectory(self):
        """
        Test retrieving a file handle on a Dataset with a file in a subdirectory.
        """
        # Where possible get_file_handle uses the internal copy.
        with self.external_file_ds.get_open_file_handle('r') as data_handle:
            self.assertEquals(data_handle, self.external_file_ds.dataset_file)

        # It falls back on the external copy.
        with self.external_file_ds_no_internal.get_open_file_handle('r') as external_file_handle:
            self.assertEquals(
                os.path.abspath(external_file_handle.name),
                os.path.abspath(os.path.join(self.working_dir, self.ext1_path))
            )

    def test_external_absolute_path(self):
        """
        Retrieve the external absolute path of an externally-backed Dataset.
        """
        ext1_path = os.path.join(self.working_dir, self.ext1_path)
        ext_sub1_path = os.path.join(self.working_dir, self.ext_sub1_path)

        self.assertEquals(self.external_file_ds.external_absolute_path(), ext1_path)
        self.assertEquals(self.external_file_ds_no_internal.external_absolute_path(), ext1_path)
        self.assertEquals(self.external_file_ds_subdir.external_absolute_path(), ext_sub1_path)
        self.assertIsNone(self.non_external_dataset.external_absolute_path())

    def test_has_data(self):
        """
        Dataset factors in presence/absence of external files when checking for data.
        """
        self.assertTrue(self.external_file_ds.has_data())
        self.assertTrue(self.external_file_ds_no_internal.has_data())
        self.assertTrue(self.external_file_ds_subdir.has_data())

        # We make an externally-backed Dataset to mess with.
        ext_path = "ext_test_has_data.txt"
        ext_contents = "File has data"
        with open(os.path.join(self.working_dir, ext_path), "wb") as f:
            f.write(ext_contents.encode())

        external_path = os.path.join(self.working_dir, ext_path)
        external_file_ds_no_internal = Dataset.create_dataset(
            external_path,
            user=self.myUser,
            keep_file=False,
            externalfiledirectory=self.efd
        )
        # Delete this file.
        os.remove(external_path)
        self.assertFalse(external_file_ds_no_internal.has_data())

        # Now test when the file exists but is unreadable.
        with open(os.path.join(self.working_dir, ext_path), "wb") as f:
            f.write(ext_contents.encode())
        self.assertTrue(external_file_ds_no_internal.has_data())
        os.chmod(external_path, stat.S_IWUSR | stat.S_IXUSR)
        self.assertFalse(external_file_ds_no_internal.has_data())

    def test_clean_efd_external_path_both_set(self):
        """
        Both or neither of externalfiledirectory and external_path are set.
        """
        self.external_file_ds.clean()

        self.external_file_ds.externalfiledirectory = None
        self.assertRaisesRegexp(
            ValidationError,
            "Both externalfiledirectory and external_path should be set or neither should be set",
            self.external_file_ds.clean
        )

        self.external_file_ds.externalfiledirectory = self.efd
        self.external_file_ds.external_path = ""
        self.assertRaisesRegexp(
            ValidationError,
            "Both externalfiledirectory and external_path should be set or neither should be set",
            self.external_file_ds.clean
        )

        # Reduce this to a purely internal Dataset.
        self.external_file_ds.externalfiledirectory = None
        self.external_file_ds.clean()

    def test_external_file_redact_this(self):
        """
        Externally-backed Datasets should have external_path and externalfiledirectory cleared on redaction.
        """
        self.external_file_ds.redact_this()
        self.external_file_ds.refresh_from_db()
        self.assertEquals(self.external_file_ds.external_path, "")
        self.assertIsNone(self.external_file_ds.externalfiledirectory)

    def test_file_check_passes(self):
        external_file_ds = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext1_path),
            user=self.myUser,
            keep_file=False,
            externalfiledirectory=self.efd)
        expected_log_messages = ''
        start_time = timezone.now()

        with capture_log_stream(logging.ERROR,
                                'librarian.Dataset') as mocked_stderr:
            Dataset.external_file_check()
            log_messages = mocked_stderr.getvalue()

        end_time = timezone.now()

        external_file_ds.refresh_from_db()
        self.assertGreaterEqual(external_file_ds.last_time_checked, start_time)
        self.assertLessEqual(external_file_ds.last_time_checked, end_time)
        self.assertFalse(external_file_ds.is_external_missing)
        self.assertMultiLineEqual(expected_log_messages, log_messages)

    def test_file_check_missing_one(self):
        Dataset.objects.all().delete()  # Remove existing datasets.

        external_file_ds = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext1_path),
            user=self.myUser,
            keep_file=False,
            externalfiledirectory=self.efd)
        external_file_ds.last_time_checked = timezone.now() - timedelta(minutes=1)
        external_file_ds.save()
        os.remove(external_file_ds.external_absolute_path())
        expected_log_messages = """\
Missing 1 external dataset. Most recent from {}, last checked a minute ago.
""".format(external_file_ds.external_absolute_path())
        start_time = timezone.now()

        with capture_log_stream(logging.ERROR,
                                'librarian.Dataset') as mocked_stderr:
            Dataset.external_file_check()
            log_messages = mocked_stderr.getvalue()

        external_file_ds.refresh_from_db()
        self.assertLess(external_file_ds.last_time_checked, start_time)
        self.assertTrue(external_file_ds.is_external_missing)
        self.assertMultiLineEqual(expected_log_messages, log_messages)

    def test_file_check_missing_two(self):
        Dataset.objects.all().delete()  # Remove existing datasets.

        external_file_ds = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext1_path),
            user=self.myUser,
            keep_file=False,
            externalfiledirectory=self.efd)
        external_file_ds.last_time_checked = timezone.now() - timedelta(minutes=5)
        external_file_ds.save()
        os.remove(external_file_ds.external_absolute_path())

        external_file_ds = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext2_path),
            user=self.myUser,
            keep_file=False,
            externalfiledirectory=self.efd)
        external_file_ds.last_time_checked = timezone.now() - timedelta(minutes=4)
        external_file_ds.save()
        os.remove(external_file_ds.external_absolute_path())
        expected_log_messages = """\
Missing 2 external datasets. Most recent from {}, last checked 4 minutes ago.
""".format(external_file_ds.external_absolute_path())

        with capture_log_stream(logging.ERROR,
                                'librarian.Dataset') as mocked_stderr:
            Dataset.external_file_check()
            log_messages = mocked_stderr.getvalue().replace(u'\xa0', ' ')

        self.assertMultiLineEqual(expected_log_messages, log_messages)

    def test_file_check_batches(self):
        Dataset.objects.all().delete()  # Remove existing datasets.

        for _ in range(10):
            Dataset.create_dataset(
                os.path.join(self.working_dir, self.ext1_path),
                user=self.myUser,
                keep_file=False,
                externalfiledirectory=self.efd)

        external_file_ds = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext2_path),
            user=self.myUser,
            keep_file=False,
            externalfiledirectory=self.efd)
        external_file_ds.last_time_checked = timezone.now() - timedelta(minutes=4)
        external_file_ds.save()
        os.remove(external_file_ds.external_absolute_path())
        expected_log_messages = """\
Missing 1 external dataset. Most recent from {}, last checked 4 minutes ago.
""".format(external_file_ds.external_absolute_path())

        with capture_log_stream(logging.ERROR,
                                'librarian.Dataset') as mocked_stderr:
            Dataset.external_file_check(batch_size=10)
            log_messages = mocked_stderr.getvalue().replace(u'\xa0', ' ')

        self.assertMultiLineEqual(expected_log_messages, log_messages)

    def test_file_check_file_restored(self):
        external_file_ds = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext1_path),
            user=self.myUser,
            keep_file=False,
            externalfiledirectory=self.efd)
        external_file_ds.is_external_missing = True
        external_file_ds.save()
        expected_log_messages = ''
        start_time = timezone.now()

        with capture_log_stream(logging.ERROR,
                                'librarian.Dataset') as mocked_stderr:
            Dataset.external_file_check()
            log_messages = mocked_stderr.getvalue()

        end_time = timezone.now()

        external_file_ds.refresh_from_db()
        self.assertGreaterEqual(external_file_ds.last_time_checked, start_time)
        self.assertLessEqual(external_file_ds.last_time_checked, end_time)
        self.assertFalse(external_file_ds.is_external_missing)
        self.assertMultiLineEqual(expected_log_messages, log_messages)

    def test_file_check_still_missing(self):
        external_file_ds = Dataset.create_dataset(
            os.path.join(self.working_dir, self.ext2_path),
            user=self.myUser,
            keep_file=False,
            externalfiledirectory=self.efd)
        external_file_ds.is_external_missing = True
        external_file_ds.save()
        os.remove(external_file_ds.external_absolute_path())
        expected_log_messages = ''
        start_time = timezone.now()

        with capture_log_stream(logging.ERROR,
                                'librarian.Dataset') as mocked_stderr:
            Dataset.external_file_check()
            log_messages = mocked_stderr.getvalue()

        external_file_ds.refresh_from_db()
        self.assertLess(external_file_ds.last_time_checked, start_time)
        self.assertTrue(external_file_ds.is_external_missing)
        self.assertMultiLineEqual(expected_log_messages, log_messages)
