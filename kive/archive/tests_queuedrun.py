from unittest import skipIf

import os
import re
import tempfile
import itertools
import copy

from mock import patch

from django.conf import settings
from django.contrib.auth.models import User
from django.test import TestCase, skipIfDBFeature
from django.core.urlresolvers import reverse, resolve
from django.utils import timezone
from django.contrib.auth.models import Group

from rest_framework.test import APIRequestFactory, force_authenticate
from rest_framework import status

from archive.models import Run, ExecLog
from archive.serializers import RunSerializer, RunBatchSerializer, grplst2str, usrlst2str
from archive.exceptions import SandboxActiveException, RunNotFinished
from constants import runstates
from file_access_utils import create_sandbox_base_path
from fleet.dockerlib import SingularityDockerHandler
from librarian.models import ExecRecord, Dataset
from pipeline.models import Pipeline, PipelineFamily
from metadata.models import kive_user, everyone_group
from kive.testing_utils import clean_up_all_files
from kive.tests import install_fixture_files, remove_fixture_files, DuckContext,\
    BaseTestCases
from fleet.workers import Manager


@skipIfDBFeature('is_mocked')
class RemoveRedactRunInProgress(TestCase):
    fixtures = ["em_sandbox_test_environment"]

    def setUp(self):
        # Clear out all the Runs and ExecRecords in the environment.
        # Run.objects.all().delete()
        # ExecRecord.objects.all().delete()

        self.pf = PipelineFamily.objects.get(name="Pipeline_family")
        self.myUser = self.pf.user
        self.pE = self.pf.members.get(revision_name="pE_name")
        self.triplet_dataset = Dataset.objects.filter(name="triplet").first()
        self.doublet_dataset = Dataset.objects.get(name="doublet")
        self.singlet_dataset = Dataset.objects.filter(name="singlet").first()
        self.raw_dataset = Dataset.objects.get(name="raw_DS")
        self.step_E1 = self.pE.steps.get(step_num=1)
        self.mA = self.step_E1.transformation.definite

        # A run that's mid-progress.
        self.run = Run(pipeline=self.pE, user=self.myUser)
        self.run.save()
        self.run.inputs.create(
            index=1,
            dataset=self.triplet_dataset
        )
        self.run.inputs.create(
            index=2,
            dataset=self.singlet_dataset
        )
        self.run.inputs.create(
            index=3,
            dataset=self.raw_dataset
        )

        self.rs_1 = self.run.runsteps.create(
            pipelinestep=self.step_E1,
            reused=False,
        )
        self.rsic = self.rs_1.RSICs.create(
            PSIC=self.step_E1.cables_in.first()
        )
        self.rsic.log = ExecLog.create(self.rsic, self.rsic)
        rsic_er = ExecRecord(generator=self.rsic.log)
        rsic_er.save()
        self.rsic.execrecord = rsic_er
        self.rsic.save()
        self.rsic.execrecord.execrecordins.create(
            generic_input=self.pE.inputs.get(dataset_idx=3),
            dataset=self.raw_dataset
        )
        self.rsic.execrecord.execrecordouts.create(
            generic_output=self.step_E1.transformation.definite.inputs.first(),
            dataset=self.raw_dataset
        )

        self.rs_1_log = ExecLog.create(self.rs_1, self.rs_1)
        self.rs_1_log.methodoutput.return_code = 0
        self.rs_1_log.methodoutput.save()
        rs_1_er = ExecRecord(generator=self.rs_1_log)
        rs_1_er.save()
        self.rs_1.execrecord = rs_1_er
        self.rs_1.save()
        self.rs_1.execrecord.execrecordins.create(
            generic_input=self.mA.inputs.first(),
            dataset=self.raw_dataset
        )
        self.rs_1.execrecord.execrecordouts.create(
            generic_output=self.mA.outputs.first(),
            dataset=self.doublet_dataset
        )
        self.run.start(save=True)

    def tearDown(self):
        clean_up_all_files()

    def test_remove_pipeline_fails(self):
        """
        Removing the Pipeline of a Run that's in progress should fail.
        """
        self.assertRaisesRegexp(
            RunNotFinished,
            "Cannot remove: an affected run is still in progress",
            lambda: self.pE.remove()
        )

    def test_remove_dataset_fails(self):
        """
        Removing a Dataset of a Run that's in progress should fail.
        """
        self.assertRaisesRegexp(
            RunNotFinished,
            "Cannot remove: an affected run is still in progress",
            lambda: self.triplet_dataset.remove()
        )

    def test_redact_dataset_fails(self):
        """
        Redacting a Dataset of a Run that's in progress should fail.
        """
        self.assertRaisesRegexp(
            RunNotFinished,
            "Cannot redact: an affected run is still in progress",
            lambda: self.triplet_dataset.redact()
        )

    def test_remove_execrecord_fails(self):
        """
        Removing an ExecRecord of a Run that's in progress should fail.
        """
        self.assertRaisesRegexp(
            RunNotFinished,
            "Cannot remove: an affected run is still in progress",
            lambda: self.rs_1.execrecord.remove()
        )

    def test_redact_execrecord_fails(self):
        """
        Redacting an ExecRecord of a Run that's in progress should fail.
        """
        self.assertRaisesRegexp(
            RunNotFinished,
            "Cannot redact: an affected run is still in progress",
            lambda: self.rs_1.execrecord.redact()
        )


@skipIfDBFeature('is_mocked')
class RemoveRedactRunJustStarting(TestCase):
    """
    Removal/redaction of stuff used in an unstarted run should be allowed.
    """
    fixtures = ["em_sandbox_test_environment"]

    def setUp(self):
        self.pf = PipelineFamily.objects.get(name="Pipeline_family")
        self.myUser = self.pf.user
        self.pE = self.pf.members.get(revision_name="pE_name")
        self.triplet_dataset = Dataset.objects.filter(name="triplet").first()
        self.doublet_dataset = Dataset.objects.get(name="doublet")
        self.singlet_dataset = Dataset.objects.filter(name="singlet").first()
        self.raw_dataset = Dataset.objects.get(name="raw_DS")

        # A run that's just starting, to the point that no Run exists yet.
        self.run_just_starting = Run(user=self.myUser, pipeline=self.pE)
        self.run_just_starting.save()
        self.run_just_starting.inputs.create(
            index=1,
            dataset=self.triplet_dataset
        )
        self.run_just_starting.inputs.create(
            index=2,
            dataset=self.singlet_dataset
        )
        self.run_just_starting.inputs.create(
            index=3,
            dataset=self.raw_dataset
        )

    def tearDown(self):
        clean_up_all_files()

    def test_remove_pipeline(self):
        """
        Removing the Pipeline of an unstarted Run should work.
        """
        self.pE.remove()
        # self.assertRaisesRegexp(
        #     RunNotFinished,
        #     "Cannot remove: an affected run is still in progress",
        #     lambda: self.pE.remove()
        # )

    def test_remove_dataset(self):
        """
        Removing a Dataset of an unstarted Run should work.
        """
        self.triplet_dataset.remove()
        # self.assertRaisesRegexp(
        #     RunNotFinished,
        #     "Cannot remove: an affected run is still in progress",
        #     lambda: self.triplet_dataset.remove()
        # )

    def test_redact_dataset(self):
        """
        Redacting a Dataset of a Run should work.
        """
        self.triplet_dataset.redact()
        # self.assertRaisesRegexp(
        #     RunNotFinished,
        #     "Cannot redact: an affected run is still in progress",
        #     lambda: self.triplet_dataset.redact()
        # )


@skipIfDBFeature('is_mocked')
class RestoreReusableDatasetTest(BaseTestCases.SlurmExecutionTestCase):
    """
    Scenario where an output is marked as reusable, and it needs to be restored.

    There are three methods:
    * sums_and_products - take each row of two integers, calculate sum and
    product, then shuffle all the result rows. This makes it reusable, but not
    deterministic.
    * total_sums - copy the first row, then one more row with the sum of all
    the sums from the remaining rows.
    * total_products - copy the first row, then one more row with the sum of all
    the products from the remaining rows.
    """
    fixtures = ["restore_reusable_dataset"]

    def setUp(self):
        install_fixture_files("restore_reusable_dataset")

    def tearDown(self):
        # remove_fixture_files()
        pass

    def execute_pipeline(self, pipeline):
        dataset = Dataset.objects.get(name="pairs")
        mgr = Manager.execute_pipeline(pipeline.user, pipeline, [dataset])
        return mgr.get_last_run()

    def test_run_new_pipeline(self):
        pipeline = Pipeline.objects.get(revision_name='sums and products')
        run_to_process = self.execute_pipeline(pipeline)
        self.assertIsNotNone(run_to_process)
        if not run_to_process.is_successful():
            state_name = run_to_process.get_state_name()
            print("unexpected run state name: '{}'".format(state_name))
            self.fail("run is not successful")

    @skipIf(not settings.RUN_SINGULARITY_TESTS, "Singularity tests disabled.")
    def test_run_new_pipeline_with_singularity(self):
        pipeline = Pipeline.objects.get(revision_name='sums and products')
        dataset = Dataset.objects.get(name="pairs")
        mgr = Manager.execute_pipeline(
            pipeline.user,
            pipeline,
            [dataset],
            singularity_handler_class=SingularityDockerHandler)
        run_to_process = mgr.get_last_run()
        self.assertIsNotNone(run_to_process)
        if not run_to_process.is_successful():
            state_name = run_to_process.get_state_name()
            print("unexpected run state name: '{}'".format(state_name))
            self.fail("run is not successful")

    def test_rerun_old_pipeline(self):
        pipeline = Pipeline.objects.get(revision_name='sums only')
        expected_execrecord_count = ExecRecord.objects.count()

        run_to_process = self.execute_pipeline(pipeline)

        self.assertTrue(run_to_process.is_successful())
        self.assertEqual(expected_execrecord_count, ExecRecord.objects.count())


@skipIfDBFeature('is_mocked')
class GarbageCollectionTest(TestCase):
    """
    Tests of sandbox garbage collection.
    """
    fixtures = ["removal"]

    def setUp(self):
        self.noop_pl = Pipeline.objects.filter(
            family__name="Nucleotide Sequence Noop"
        ).order_by(
            "revision_number"
        ).first()

        self.noop_run = Run.objects.filter(
            pipeline=self.noop_pl
        ).order_by(
            "end_time"
        ).first()

        # A phony directory that we mock-run a Pipeline in.
        self.mock_sandbox_path = tempfile.mkdtemp(
            prefix="user{}_run{}_".format(self.noop_run.user, self.noop_run.pk),
            dir=create_sandbox_base_path())

    def test_reap_nonexistent_sandbox_path(self):
        """ A Run that has no sandbox path should do nothing.

        This used to raise an exception, but it's a valid scenario. When a run
        is cancelled before it starts, it never gets a sandbox path.
        """
        now = timezone.now()
        run = Run(pipeline=self.noop_pl,
                  user=self.noop_pl.user,
                  start_time=now,
                  end_time=now,
                  _runstate_id=runstates.SUCCESSFUL_PK)
        run.save()

        run.collect_garbage()

        reloaded_run = Run.objects.get(id=run.id)
        self.assertTrue(reloaded_run.purged)

    def test_reap_unfinished_run(self):
        """
        A Run that is not marked as finished should raise an exception.
        """
        run = Run(pipeline=self.noop_pl, user=self.noop_pl.user, sandbox_path=self.mock_sandbox_path)
        run.save()

        self.assertRaisesRegexp(
            SandboxActiveException,
            re.escape("Run (pk={}, Pipeline={}, queued {}, User=RemOver) is not finished".format(
                run.id,
                self.noop_pl,
                run.time_queued)),
            run.collect_garbage
        )

    def test_reap_finished_run(self):
        """
        A Run that is finished should be reaped without issue.
        """
        self.noop_run.sandbox_path = self.mock_sandbox_path
        self.noop_run.save()
        self.noop_run.collect_garbage()

        self.assertFalse(os.path.exists(self.mock_sandbox_path))
        self.assertTrue(self.noop_run.purged)

    def test_reap_on_removal(self):
        """
        Removing a Run should reap the sandbox.
        """
        self.noop_run.sandbox_path = self.mock_sandbox_path
        self.noop_run.save()

        self.noop_run.remove()
        self.assertFalse(os.path.exists(self.mock_sandbox_path))

    def test_reap_on_redaction(self):
        """
        Redacting part of a Run should reap the sandbox.
        """
        self.noop_run.sandbox_path = self.mock_sandbox_path
        self.noop_run.save()

        self.noop_run.runsteps.first().execrecord.redact()
        self.assertFalse(os.path.exists(self.mock_sandbox_path))


@skipIfDBFeature('is_mocked')
class RunApiTests(TestCase):
    # This fixture has the result of sandbox.tests.execute_tests_environment_setup,
    # as well of setting up another Pipeline; this other Pipeline and the resulting
    # run is used in this test case.
    fixtures = ["run_api_tests"]

    def setUp(self):
        install_fixture_files("run_api_tests")

        self.kive_user = kive_user()
        self.myUser = User.objects.get(username="john")

        self.pipeline_to_run = Pipeline.objects.get(
            family__name="self.pf",
            revision_name="pX_revision_2"
        )

        self.dataset = Dataset.objects.get(
            name="pX_in_dataset",
            structure__isnull=False,
            user=self.myUser
        )

        self.factory = APIRequestFactory()
        self.run_list_path = reverse('run-list')
        self.run_list_view, _, _ = resolve(self.run_list_path)
        self.run_status_path = reverse('run-status')
        self.run_status_view, _, _ = resolve(self.run_status_path)

    def tearDown(self):
        clean_up_all_files()
        remove_fixture_files()

    def test_run_index(self, expected_runs=1):
        request = self.factory.get(self.run_list_path)
        response = self.run_list_view(request).render()
        data = response.render().data

        self.assertEquals(
            data['detail'],
            "Authentication credentials were not provided.")

        force_authenticate(request, user=self.myUser)
        response = self.run_list_view(request).render()
        data = response.render().data

        self.assertEquals(len(data), expected_runs)
        for run in data:
            self.assertIn('id', run)
            self.assertIn('removal_plan', run)
            self.assertIn('run_status', run)

    def test_run_status(self):
        expected_runs = 1
        request = self.factory.get(self.run_status_path)
        force_authenticate(request, user=self.myUser)
        response = self.run_status_view(request).render()
        data = response.render().data

        self.assertEquals(len(data), expected_runs)
        self.assertEqual('**-**', data[0]['run_progress']['status'])

    def test_pipeline_execute_plus_details_and_run_remove(self):
        # TODO: This should be split into one test to test the pipeline execution
        # Plus many tests to test details (which needs a proper fixture)
        # Kick off the run
        request = self.factory.post(
            self.run_list_path,
            {
                "pipeline": self.pipeline_to_run.pk,
                "inputs": [
                    {
                        "index": 1,
                        "dataset": self.dataset.pk
                    }
                ]
            },
            format="json"
        )
        force_authenticate(request, user=self.myUser)
        response = self.run_list_view(request).render()
        data = response.render().data

        # Check that the run created something sensible.
        self.assertIn("id", data)
        self.assertIn('run_outputs', data)

        # There should be two runs now: the one we just started (and hasn't run yet)
        # and the one we already ran that's in the fixture.
        self.test_run_index(2)

        # Let's examine a real execution.
        real_run_pk = self.pipeline_to_run.pipeline_instances.first().pk

        # Touch the record detail page.
        path = self.run_list_path + "{}/".format(real_run_pk)
        request = self.factory.get(path)
        force_authenticate(request, user=self.myUser)
        view, args, kwargs = resolve(path)
        response = view(request, *args, **kwargs)
        data = response.render().data

        # Touch the run status page.
        path = self.run_list_path + "{}/run_status/".format(real_run_pk)
        request = self.factory.get(path)
        force_authenticate(request, user=self.myUser)
        view, args, kwargs = resolve(path)
        response = view(request, *args, **kwargs)
        data = response.render().data
        self.assertEquals(data['status'], '**-**')
        self.assertIn('step_progress', data)

        # Touch the outputs.
        path = self.run_list_path + "{}/run_outputs/".format(real_run_pk)
        request = self.factory.get(path)
        force_authenticate(request, user=self.myUser)
        view, args, kwargs = resolve(path)
        response = view(request, *args, **kwargs)
        data = response.render().data
        self.assertEquals(data['id'], real_run_pk)
        self.assertEquals(len(data['output_summary']), 8)

        for output in data['output_summary']:
            self.assertEquals(output['is_ok'], True)
            self.assertEquals(output['is_invalid'], False)

        # Touch the removal plan.
        path = self.run_list_path + "{}/removal_plan/".format(real_run_pk)
        request = self.factory.get(path)
        force_authenticate(request, user=self.myUser)
        view, args, kwargs = resolve(path)
        response = view(request, *args, **kwargs)
        data = response.render().data

        # 4 Datasets created:
        #  - 1 by the custom input cable to step 1 (and this is reused by the input cable to step 2)
        #  - 1 by step 1
        #  - 1 by step 2
        #  - 1 by the custom output cable
        self.assertEquals(data['Datasets'], 4)
        self.assertEquals(data['Runs'], 1)
        self.assertEquals(data['Datatypes'], 0)

        # Delete the record.
        path = self.run_list_path + "{}/".format(real_run_pk)
        request = self.factory.delete(path)
        force_authenticate(request, user=self.kive_user)
        view, args, kwargs = resolve(path)
        response = view(request, *args, **kwargs)
        self.assertEquals(response.render().data, None)
        self.test_run_index(1)  # The run in the fixture should still be there.

    def test_stop_run(self):
        """
        Test PATCHing a run to stop.
        """
        request = self.factory.post(
            self.run_list_path,
            {
                "pipeline": self.pipeline_to_run.pk,
                "inputs": [
                    {
                        "index": 1,
                        "dataset": self.dataset.pk
                    }
                ]
            },
            format="json"
        )
        force_authenticate(request, user=self.myUser)
        response = self.run_list_view(request).render()
        data = response.render().data

        detail_path = reverse("run-detail", kwargs={'pk': data["id"]})
        request = self.factory.patch(detail_path, {'is_stop_requested': "true"})
        force_authenticate(request, user=self.myUser)

        detail_view, _, _ = resolve(detail_path)
        response = detail_view(request, pk=data["id"])
        self.assertEquals(response.status_code, status.HTTP_200_OK)

        stopped_run = Run.objects.get(pk=data["id"])
        self.assertEquals(stopped_run.stopped_by, self.myUser)

    def test_stop_run_administrator(self):
        """
        An administrator should be allowed to stop a run.
        """
        request = self.factory.post(
            self.run_list_path,
            {
                "pipeline": self.pipeline_to_run.pk,
                "inputs": [
                    {
                        "index": 1,
                        "dataset": self.dataset.pk
                    }
                ]
            },
            format="json"
        )
        force_authenticate(request, user=self.myUser)
        response = self.run_list_view(request).render()
        data = response.render().data

        detail_path = reverse("run-detail", kwargs={'pk': data["id"]})
        request = self.factory.patch(detail_path, {'is_stop_requested': "true"})
        force_authenticate(request, user=self.kive_user)

        detail_view, _, _ = resolve(detail_path)
        response = detail_view(request, pk=data["id"])
        self.assertEquals(response.status_code, status.HTTP_200_OK)

        stopped_run = Run.objects.get(pk=data["id"])
        self.assertEquals(stopped_run.stopped_by, self.kive_user)

    def test_stop_run_non_owner(self):
        """
        A user who does not own the run should not be allowed to stop it.
        """
        # First, we have to give other people access to the data and Pipeline.
        self.dataset.grant_everyone_access()

        # This is only one layer deep so we don't have to recurse.
        for xput in itertools.chain(self.pipeline_to_run.inputs.all(), self.pipeline_to_run.outputs.all()):
            if xput.has_structure:
                xput.structure.compounddatatype.grant_everyone_access()

        for step in self.pipeline_to_run.steps.all():
            curr_method = step.transformation.definite

            for xput in itertools.chain(curr_method.inputs.all(), curr_method.outputs.all()):
                if xput.has_structure:
                    xput.structure.compounddatatype.grant_everyone_access()

            # Fortunately this has no dependencies.
            curr_method.driver.coderesource.grant_everyone_access()
            curr_method.driver.grant_everyone_access()
            step.transformation.definite.family.grant_everyone_access()
            step.transformation.grant_everyone_access()

        self.pipeline_to_run.family.grant_everyone_access()
        self.pipeline_to_run.grant_everyone_access()

        request = self.factory.post(
            self.run_list_path,
            {
                "pipeline": self.pipeline_to_run.pk,
                "inputs": [
                    {
                        "index": 1,
                        "dataset": self.dataset.pk
                    }
                ],
                "groups_allowed": ["Everyone"]
            },
            format="json"
        )
        force_authenticate(request, user=self.kive_user)
        response = self.run_list_view(request).render()
        data = response.render().data

        detail_path = reverse("run-detail", kwargs={'pk': data["id"]})
        request = self.factory.patch(detail_path, {'is_stop_requested': "true"})
        force_authenticate(request, user=self.myUser)

        detail_view, _, _ = resolve(detail_path)
        response = detail_view(request, pk=data["id"])
        self.assertEquals(response.status_code, status.HTTP_403_FORBIDDEN)


@skipIfDBFeature('is_mocked')
class RunSerializerTestBase(TestCase):
    fixtures = ["em_sandbox_test_environment"]

    def setUp(self):
        install_fixture_files("em_sandbox_test_environment")
        self.kive_user = kive_user()
        self.myUser = User.objects.get(username="john")

        self.duck_context = DuckContext()
        self.john_context = DuckContext(user=self.myUser)

        self.em_pf = PipelineFamily.objects.get(name="Pipeline_family")
        self.em_pipeline = self.em_pf.members.get(revision_name="pE_name")

        # The inputs to this pipeline are (triplet_cdt, singlet_cdt, raw).
        # The second one has min_row=10.
        self.triplet_cdt = self.em_pipeline.inputs.get(dataset_idx=1).get_cdt()
        self.singlet_cdt = self.em_pipeline.inputs.get(dataset_idx=2).get_cdt()

        # Datasets to feed the pipeline that are defined in the fixture.
        self.triplet_SD = Dataset.objects.get(
            name="triplet",
            description="lol",
            dataset_file__endswith="step_0_triplet.csv",
            user=self.myUser,
            structure__isnull=False,
            structure__compounddatatype=self.triplet_cdt
        )
        self.singlet_SD = Dataset.objects.get(
            name="singlet",
            description="lol",
            dataset_file__endswith="singlet_cdt_large.csv",
            user=self.myUser,
            structure__isnull=False,
            structure__compounddatatype=self.singlet_cdt
        )
        self.raw_SD = Dataset.objects.get(
            name="raw_DS",
            description="lol",
            user=self.myUser
        )

    def tearDown(self):
        remove_fixture_files()


class RunSerializerTests(RunSerializerTestBase):
    def test_validate(self):
        """
        Validating a well-specified Run to process.
        """
        serialized_rtp = {
            "pipeline": self.em_pipeline.pk,
            "inputs": [
                {
                    "dataset": self.triplet_SD.pk,
                    "index": 1
                },
                {
                    "dataset": self.singlet_SD.pk,
                    "index": 2
                },
                {
                    "dataset": self.raw_SD.pk,
                    "index": 3
                }
            ],
            "users_allowed": [],
            "groups_allowed": []
        }
        rtp_serializer = RunSerializer(data=serialized_rtp, context=self.john_context)

        self.assertTrue(rtp_serializer.is_valid())

    def test_validate_wrong_number_inputs(self):
        """
        Validation fails if the number of inputs is wrong.
        """
        serialized_rtp = {
            "pipeline": self.em_pipeline.pk,
            "inputs": [
                {
                    "dataset": self.triplet_SD.pk,
                    "index": 1
                },
                {
                    "dataset": self.singlet_SD.pk,
                    "index": 2
                }
            ],
            "users_allowed": [],
            "groups_allowed": []
        }
        rtp_serializer = RunSerializer(data=serialized_rtp, context=self.john_context)

        self.assertFalse(rtp_serializer.is_valid())
        self.assertEquals([str(e) for e in rtp_serializer.errors["non_field_errors"]],
                          [u"Pipeline has 3 inputs, but only received 2."])

    def test_validate_inputs_oversated(self):
        """
        Validation fails if an input has more than one input defined.
        """
        serialized_rtp = {
            "pipeline": self.em_pipeline.pk,
            "inputs": [
                {
                    "dataset": self.triplet_SD.pk,
                    "index": 1
                },
                {
                    "dataset": self.singlet_SD.pk,
                    "index": 2
                },
                {
                    "dataset": self.triplet_SD.pk,
                    "index": 1
                },
            ],
            "users_allowed": [],
            "groups_allowed": []
        }
        rtp_serializer = RunSerializer(data=serialized_rtp, context=self.john_context)

        self.assertFalse(rtp_serializer.is_valid())
        self.assertEquals([str(e) for e in rtp_serializer.errors["non_field_errors"]],
                          [u"Pipeline inputs must be uniquely specified"])

    def test_validate_input_index_dne(self):
        """
        Validation fails if an input index doesn't exist.
        """
        serialized_rtp = {
            "pipeline": self.em_pipeline.pk,
            "inputs": [
                {
                    "dataset": self.triplet_SD.pk,
                    "index": 1
                },
                {
                    "dataset": self.singlet_SD.pk,
                    "index": 2
                },
                {
                    "dataset": self.triplet_SD.pk,
                    "index": 4
                },
            ],
            "users_allowed": [],
            "groups_allowed": []
        }
        rtp_serializer = RunSerializer(data=serialized_rtp, context=self.john_context)

        self.assertFalse(rtp_serializer.is_valid())
        self.assertEquals([str(e) for e in rtp_serializer.errors["non_field_errors"]],
                          [u"Pipeline {} has no input with index {}".format(
                              self.em_pipeline, 4
                          )])

    def test_validate_illegal_priority(self):
        """
        Validation fails if the job's priority is out of bounds (normally 0..2)
        """
        serialized_rtp = {
            "pipeline": self.em_pipeline.pk,
            "inputs": [
                {
                    "dataset": self.triplet_SD.pk,
                    "index": 1
                },
                {
                    "dataset": self.singlet_SD.pk,
                    "index": 2
                },
                {
                    "dataset": self.triplet_SD.pk,
                    "index": 4
                },
            ],
            "users_allowed": [],
            "groups_allowed": [],
            "priority": 1001
        }
        rtp_serializer = RunSerializer(data=serialized_rtp, context=self.john_context)

        self.assertFalse(rtp_serializer.is_valid())
        self.assertEquals([str(e) for e in rtp_serializer.errors["non_field_errors"]],
                          [u"Illegal priority level"])

    def test_validate_input_CDT_incompatible(self):
        """
        Validation fails if an input Dataset is incompatible with the Pipeline input.
        """
        serialized_rtp = {
            "pipeline": self.em_pipeline.pk,
            "inputs": [
                {
                    "dataset": self.triplet_SD.pk,
                    "index": 1
                },
                {
                    "dataset": self.singlet_SD.pk,
                    "index": 2
                },
                {
                    "dataset": self.triplet_SD.pk,
                    "index": 3
                },
            ],
            "users_allowed": [],
            "groups_allowed": []
        }
        rtp_serializer = RunSerializer(data=serialized_rtp, context=self.john_context)

        self.assertFalse(rtp_serializer.is_valid())
        self.assertEquals(rtp_serializer.errors["non_field_errors"],
                          [u"Input {} is incompatible with Dataset {}".format(
                              self.em_pipeline.inputs.get(dataset_idx=3), self.triplet_SD
                          )])

    def test_validate_overextending_permissions(self):
        """
        Validation fails if users_allowed and groups_allowed exceed those on the inputs and the Pipeline.
        """
        self.em_pipeline.groups_allowed.remove(everyone_group())
        serialized_rtp = {
            "pipeline": self.em_pipeline.pk,
            "inputs": [
                {
                    "dataset": self.triplet_SD.pk,
                    "index": 1
                },
                {
                    "dataset": self.singlet_SD.pk,
                    "index": 2
                },
                {
                    "dataset": self.raw_SD.pk,
                    "index": 3
                }
            ],
            "users_allowed": [self.kive_user],
            "groups_allowed": [everyone_group()]
        }
        rtp_serializer = RunSerializer(data=serialized_rtp, context=self.john_context)

        self.assertFalse(rtp_serializer.is_valid())
        self.assertEqual(
            set([str(e) for e in rtp_serializer.errors["non_field_errors"]]),
            set([
                u"User(s) {} may not be granted access".format(usrlst2str([self.kive_user])),
                u"Group(s) {} may not be granted access".format(grplst2str([everyone_group()]))
            ])
        )

    def test_create(self):
        """
        Creating a Run to process, i.e. adding a job to the queue.
        """
        serialized_rtp = {
            "pipeline": self.em_pipeline.pk,
            "inputs": [
                {
                    "dataset": self.triplet_SD.pk,
                    "index": 1
                },
                {
                    "dataset": self.singlet_SD.pk,
                    "index": 2
                },
                {
                    "dataset": self.raw_SD.pk,
                    "index": 3
                }
            ],
            "users_allowed": [kive_user()],
            "groups_allowed": [everyone_group()]
        }

        rtp_serializer = RunSerializer(data=serialized_rtp, context=self.john_context)
        self.assertTrue(rtp_serializer.is_valid())

        before = timezone.now()
        rtp = rtp_serializer.save()
        after = timezone.now()

        # Probe the RunToProcess to check that it was correctly created.
        self.assertEqual(rtp.pipeline, self.em_pipeline)
        self.assertEqual(rtp.user, self.myUser)
        self.assertEqual(set(rtp.users_allowed.all()), set([kive_user()]))
        self.assertEqual(set(rtp.groups_allowed.all()), set([everyone_group()]))
        self.assertTrue(before <= rtp.time_queued)
        self.assertTrue(after >= rtp.time_queued)

        self.assertEqual(rtp.inputs.count(), 3)
        self.assertEqual(rtp.inputs.get(index=1).dataset, self.triplet_SD)
        self.assertEqual(rtp.inputs.get(index=2).dataset, self.singlet_SD)
        self.assertEqual(rtp.inputs.get(index=3).dataset, self.raw_SD)


class RunBatchSerializerTests(RunSerializerTestBase):
    def setUp(self):
        super(RunBatchSerializerTests, self).setUp()
        self.serialized_run = {
            "pipeline": self.em_pipeline.pk,
            "inputs": [
                {
                    "dataset": self.triplet_SD.pk,
                    "index": 1
                },
                {
                    "dataset": self.singlet_SD.pk,
                    "index": 2
                },
                {
                    "dataset": self.raw_SD.pk,
                    "index": 3
                }
            ],
        }

    def test_validate(self):
        """
        Validating a well-formed RunBatch.
        """
        serialized_rb = {
            "name": "My RunBatch",
            "description": "foo",
            "runs": [
                self.serialized_run,
                self.serialized_run,
                self.serialized_run
            ]
        }
        rb_serializer = RunBatchSerializer(data=serialized_rb, context=self.john_context)
        self.assertTrue(rb_serializer.is_valid())

    def test_validate_bad_permissions(self):
        """
        Validation fails if the Runs' permissions exceed those on the RunBatch.
        """
        second_serialized_run = copy.deepcopy(self.serialized_run)

        self.serialized_run["users_allowed"] = [self.kive_user.username]
        self.serialized_run["groups_allowed"] = [everyone_group().name]
        self.serialized_run["name"] = "OverlyPermissiveRun"

        second_serialized_run["users_allowed"] = [self.kive_user.username]
        second_serialized_run["groups_allowed"] = []
        second_serialized_run["name"] = "LessPermissiveButStillTooMuchRun"

        serialized_rb = {
            "name": "RunBatch with permissions issues",
            "description": "foo",
            "runs": [
                self.serialized_run,
                second_serialized_run
            ]
        }

        rb_serializer = RunBatchSerializer(data=serialized_rb, context=self.john_context)
        self.assertFalse(rb_serializer.is_valid())
        # NOTE: use sets here, as the order of the error messages is unimportant
        expected_set = set([
            "Group(s) {} may not be granted access to run {} (index {})".format(
                grplst2str([everyone_group()]),
                self.serialized_run["name"],
                1
            ),
            "User(s) {} may not be granted access to run {} (index {})".format(
                usrlst2str([self.kive_user]),
                self.serialized_run["name"],
                1
            ),
            "User(s) {} may not be granted access to run {} (index {})".format(
                usrlst2str([self.kive_user]),
                second_serialized_run["name"],
                2
            ),
        ])
        # we must convert the list of ErrorDetail into strings first...
        got_set = set([str(e) for e in rb_serializer.errors["non_field_errors"]])
        self.assertSetEqual(expected_set, got_set)
        # assert False, "force fail"

    def test_validate_coherent_permissions(self):
        """
        Validating a well-formed RunBatch with coherent permissions.
        """
        second_serialized_run = copy.deepcopy(self.serialized_run)

        self.serialized_run["users_allowed"] = [self.kive_user.username]
        self.serialized_run["name"] = "LetKiveSee"

        second_serialized_run["users_allowed"] = [self.kive_user.username]
        second_serialized_run["groups_allowed"] = []
        second_serialized_run["name"] = "LetKiveSeeToo"

        serialized_rb = {
            "name": "RunBatch that Kive sees",
            "description": "foo",
            "runs": [
                self.serialized_run,
                second_serialized_run
            ],
            "users_allowed": [self.kive_user.username]
        }

        rb_serializer = RunBatchSerializer(data=serialized_rb, context=self.john_context)
        self.assertTrue(rb_serializer.is_valid())

    def test_validate_batch_permissions_not_allowed(self):
        """
        Validating a RunBatch whose permissions exceed those allowed by its Runs.
        """
        self.em_pipeline.groups_allowed.remove(everyone_group())

        self.serialized_run["name"] = "Run whose pipeline doesn't allow everyone to use it"

        serialized_rb = {
            "name": "RunBatch that Kive and everyone sees",
            "description": "foo",
            "runs": [
                self.serialized_run,
            ],
            "users_allowed": [self.kive_user.username],
            "groups_allowed": [everyone_group().name]
        }

        rb_serializer = RunBatchSerializer(data=serialized_rb, context=self.john_context)
        self.assertFalse(rb_serializer.is_valid())
        expected_set = set([
            "User(s) {} may not be granted access to run {} (index {})".format(
                usrlst2str([self.kive_user]),
                self.serialized_run["name"],
                1
            ),
            "Group(s) {} may not be granted access to run {} (index {})".format(
                grplst2str([everyone_group()]),
                self.serialized_run["name"],
                1
            )
        ])
        got_set = set([str(e) for e in rb_serializer.errors["non_field_errors"]])
        self.assertSetEqual(expected_set, got_set)

    def test_validate_everyone_has_access(self):
        """
        Validating a well-formed RunBatch.
        """
        second_serialized_run = copy.deepcopy(self.serialized_run)
        third_serialized_run = copy.deepcopy(self.serialized_run)

        self.serialized_run["users_allowed"] = [self.kive_user.username]
        self.serialized_run["name"] = "LetKiveSee"

        second_serialized_run["users_allowed"] = [self.kive_user.username]
        second_serialized_run["groups_allowed"] = [everyone_group().name]
        second_serialized_run["name"] = "EveryoneSees"

        third_serialized_run["name"] = "OwnerOnly"

        serialized_rb = {
            "name": "RunBatch that everyone sees",
            "description": "foo",
            "runs": [
                self.serialized_run,
                second_serialized_run
            ],
            "groups_allowed": [everyone_group().name]
        }

        rb_serializer = RunBatchSerializer(data=serialized_rb, context=self.john_context)
        self.assertTrue(rb_serializer.is_valid())

    def test_create(self):
        """
        Create a well-formed RunBatch.
        """
        second_serialized_run = copy.deepcopy(self.serialized_run)
        third_serialized_run = copy.deepcopy(self.serialized_run)

        self.serialized_run["name"] = "One"

        second_serialized_run["name"] = "Two"
        second_serialized_run["users_allowed"] = [self.kive_user.username]

        third_serialized_run["name"] = "Three"
        third_serialized_run["groups_allowed"] = [everyone_group().name]

        serialized_rb = {
            "name": "My RunBatch",
            "description": "foo",
            "runs": [
                self.serialized_run,
                second_serialized_run,
                third_serialized_run
            ],
            "users_allowed": [self.kive_user.username],
            "groups_allowed": [everyone_group().name]
        }
        rb_serializer = RunBatchSerializer(data=serialized_rb, context=self.john_context)
        self.assertTrue(rb_serializer.is_valid())
        rb = rb_serializer.save()

        # Probe the RunBatch to check that it was correctly created.
        self.assertEqual(rb.user, self.myUser)
        self.assertEqual(set(rb.users_allowed.all()), set([self.kive_user]))
        self.assertEqual(set(rb.groups_allowed.all()), set([everyone_group()]))

        self.assertEqual(rb.runs.count(), 3)

        # Run One inherits its permissions.
        run1 = rb.runs.get(name="One")
        self.assertEqual(run1.users_allowed.count(), 1)
        self.assertEqual(run1.users_allowed.first(), self.kive_user)
        self.assertEqual(run1.groups_allowed.count(), 1)
        self.assertEqual(run1.groups_allowed.first(), everyone_group())

        # Runs Two and Three had their own permissions defined.
        run2 = rb.runs.get(name="Two")
        self.assertEqual(run2.users_allowed.count(), 1)
        self.assertEqual(run2.users_allowed.first(), self.kive_user)
        self.assertEqual(run2.groups_allowed.count(), 0)

        run3 = rb.runs.get(name="Three")
        self.assertEqual(run3.users_allowed.count(), 0)
        self.assertEqual(run3.groups_allowed.count(), 1)
        self.assertEqual(run3.groups_allowed.first(), everyone_group())

    def test_create_no_permissions_copied(self):
        """
        Create a well-formed RunBatch without copying permissions over.
        """
        second_serialized_run = copy.deepcopy(self.serialized_run)
        third_serialized_run = copy.deepcopy(self.serialized_run)

        self.serialized_run["name"] = "One"

        second_serialized_run["name"] = "Two"
        second_serialized_run["users_allowed"] = [self.kive_user.username]

        third_serialized_run["name"] = "Three"
        third_serialized_run["groups_allowed"] = [everyone_group().name]

        serialized_rb = {
            "name": "My RunBatch",
            "description": "foo",
            "runs": [
                self.serialized_run,
                second_serialized_run,
                third_serialized_run
            ],
            "users_allowed": [self.kive_user.username],
            "groups_allowed": [everyone_group().name],
            "copy_permissions_to_runs": False
        }
        rb_serializer = RunBatchSerializer(data=serialized_rb, context=self.john_context)
        self.assertTrue(rb_serializer.is_valid())
        rb = rb_serializer.save()

        # Probe the RunBatch to check that it was correctly created.
        self.assertEqual(rb.user, self.myUser)
        self.assertEqual(set(rb.users_allowed.all()), set([self.kive_user]))
        self.assertEqual(set(rb.groups_allowed.all()), set([everyone_group()]))

        self.assertEqual(rb.runs.count(), 3)

        # Run One does not inherit any permissions.
        run1 = rb.runs.get(name="One")
        self.assertEqual(run1.users_allowed.count(), 0)
        self.assertEqual(run1.groups_allowed.count(), 0)

        # Runs Two and Three had their own permissions defined.
        run2 = rb.runs.get(name="Two")
        self.assertEqual(run2.users_allowed.count(), 1)
        self.assertEqual(run2.users_allowed.first(), self.kive_user)
        self.assertEqual(run2.groups_allowed.count(), 0)

        run3 = rb.runs.get(name="Three")
        self.assertEqual(run3.users_allowed.count(), 0)
        self.assertEqual(run3.groups_allowed.count(), 1)
        self.assertEqual(run3.groups_allowed.first(), everyone_group())

    def test_validate_update_good(self):
        """
        Validating a good update to a RunBatch.
        """
        # First, we create a RunBatch as we did in the above.
        second_serialized_run = copy.deepcopy(self.serialized_run)
        third_serialized_run = copy.deepcopy(self.serialized_run)

        self.serialized_run["name"] = "One"

        second_serialized_run["name"] = "Two"
        second_serialized_run["users_allowed"] = [self.kive_user.username]

        third_serialized_run["name"] = "Three"
        third_serialized_run["groups_allowed"] = [everyone_group().name]

        serialized_rb = {
            "name": "My RunBatch",
            "description": "foo",
            "runs": [
                self.serialized_run,
                second_serialized_run,
                third_serialized_run
            ],
            "users_allowed": [self.kive_user.username],
            "groups_allowed": [everyone_group().name],
            "copy_permissions_to_runs": False
        }
        rb_serializer = RunBatchSerializer(data=serialized_rb, context=self.john_context)
        self.assertTrue(rb_serializer.is_valid())

    @patch.object(Run, "is_complete", return_value=True)
    def test_validate_update_copy_permissions_invalid(self, _):
        """
        Validating an update to a RunBatch which attempts to copy permissions but the permissions are not coherent.
        """
        self.em_pipeline.groups_allowed.remove(everyone_group())
        # Create a RunBatch with appropriate permissions.
        second_serialized_run = copy.deepcopy(self.serialized_run)
        third_serialized_run = copy.deepcopy(self.serialized_run)

        self.serialized_run["name"] = "One"

        second_serialized_run["name"] = "Two"

        third_serialized_run["name"] = "Three"

        serialized_rb = {
            "name": "My RunBatch",
            "description": "foo",
            "runs": [
                self.serialized_run,
                second_serialized_run,
                third_serialized_run
            ],
            "copy_permissions_to_runs": False
        }
        rb_serializer = RunBatchSerializer(data=serialized_rb, context=self.john_context)
        self.assertTrue(rb_serializer.is_valid())
        rb = rb_serializer.save()

        run1 = rb.runs.get(name="One")
        run2 = rb.runs.get(name="Two")
        run3 = rb.runs.get(name="Three")

        # Now we attempt to update it, adding a permission that won't work.
        new_group = Group(name="Interlopers")
        new_group.save()
        update_dict = {
            "name": "My updated RunBatch",
            "groups_allowed": [new_group.name]
        }
        update_serializer = RunBatchSerializer(rb, data=update_dict, context=self.john_context)
        self.assertFalse(update_serializer.is_valid())  # note that we patched Run.is_complete so this would work

        self.assertSetEqual(
            set([str(e) for e in update_serializer.errors["non_field_errors"]]),
            {
                "Group(s) {} may not be granted access to run {}".format(
                    grplst2str([new_group]),
                    run1
                ),
                "Group(s) {} may not be granted access to run {}".format(
                    grplst2str([new_group]),
                    run2,
                ),
                "Group(s) {} may not be granted access to run {}".format(
                    grplst2str([new_group]),
                    run3
                )
            }
        )

    @patch.object(Run, "is_complete", return_value=True)
    def test_validate_update_no_copy_permissions(self, _):
        """
        Validating an update to a RunBatch which does not copy permissions.
        """
        self.em_pipeline.groups_allowed.remove(everyone_group())
        # Create a RunBatch with appropriate permissions.
        second_serialized_run = copy.deepcopy(self.serialized_run)
        third_serialized_run = copy.deepcopy(self.serialized_run)

        self.serialized_run["name"] = "One"

        second_serialized_run["name"] = "Two"

        third_serialized_run["name"] = "Three"

        serialized_rb = {
            "name": "My RunBatch",
            "description": "foo",
            "runs": [
                self.serialized_run,
                second_serialized_run,
                third_serialized_run
            ],
            "copy_permissions_to_runs": False
        }
        rb_serializer = RunBatchSerializer(data=serialized_rb, context=self.john_context)
        self.assertTrue(rb_serializer.is_valid())
        rb = rb_serializer.save()

        # Now we attempt to update it, adding a permission that won't work for the runs,
        # but it doesn't matter because we aren't changing anything.
        new_group = Group(name="Interlopers")
        new_group.save()
        update_dict = {
            "name": "My updated RunBatch",
            "groups_allowed": [new_group.name],
            "copy_permissions_to_runs": False
        }
        update_serializer = RunBatchSerializer(rb, data=update_dict, context=self.john_context)
        self.assertTrue(update_serializer.is_valid())  # note that we patched Run.is_complete so this would work

    @patch.object(Run, "is_complete", return_value=True)
    def test_update_copy_permissions(self, _):
        """
        Update a RunBatch, copying permissions.
        """
        # First, we create a RunBatch as we did in the above.
        second_serialized_run = copy.deepcopy(self.serialized_run)
        third_serialized_run = copy.deepcopy(self.serialized_run)

        self.serialized_run["name"] = "One"

        second_serialized_run["name"] = "Two"
        second_serialized_run["users_allowed"] = [self.kive_user.username]

        third_serialized_run["name"] = "Three"
        third_serialized_run["groups_allowed"] = [everyone_group().name]

        serialized_rb = {
            "name": "My RunBatch",
            "description": "foo",
            "runs": [
                self.serialized_run,
                second_serialized_run,
                third_serialized_run
            ],
            "users_allowed": [self.kive_user.username],
            "groups_allowed": [everyone_group().name],
            "copy_permissions_to_runs": False
        }
        rb_serializer = RunBatchSerializer(data=serialized_rb, context=self.john_context)
        self.assertTrue(rb_serializer.is_valid())
        rb = rb_serializer.save()

        # Now we update the RunBatch.
        new_group = Group(name="Interlopers")
        new_group.save()
        update_dict = {
            "name": "My updated RunBatch",
            "groups_allowed": [new_group.name]
        }
        update_serializer = RunBatchSerializer(rb, data=update_dict, context=self.john_context)
        update_serializer.is_valid()  # note that we patched Run.is_complete
        update_serializer.save()

        # Probe the RunBatch to check that it was correctly updated.
        self.assertEqual(rb.user, self.myUser)
        self.assertEqual(rb.name, update_dict["name"])
        self.assertSetEqual(set(rb.users_allowed.all()), {self.kive_user})
        self.assertEqual(set(rb.groups_allowed.all()), set([everyone_group(), new_group]))

        self.assertEqual(rb.runs.count(), 3)

        # All runs had the Everyone group added.
        run1 = rb.runs.get(name="One")
        self.assertFalse(run1.users_allowed.exists())
        self.assertSetEqual(set(run1.groups_allowed.all()), {new_group})

        run2 = rb.runs.get(name="Two")
        self.assertSetEqual(set(run2.users_allowed.all()), {self.kive_user})
        self.assertSetEqual(set(run2.groups_allowed.all()), {new_group})

        run3 = rb.runs.get(name="Three")
        self.assertFalse(run3.users_allowed.exists())
        self.assertSetEqual(set(run3.groups_allowed.all()), {everyone_group(), new_group})

    @patch.object(Run, "is_complete", return_value=True)
    def test_update_no_copy_permissions(self, _):
        """
        Update a RunBatch, not copying permissions.
        """
        # First, we create a RunBatch as we did in the above.
        second_serialized_run = copy.deepcopy(self.serialized_run)
        third_serialized_run = copy.deepcopy(self.serialized_run)

        self.serialized_run["name"] = "One"

        second_serialized_run["name"] = "Two"
        second_serialized_run["users_allowed"] = [self.kive_user.username]

        third_serialized_run["name"] = "Three"

        serialized_rb = {
            "name": "My RunBatch",
            "description": "foo",
            "runs": [
                self.serialized_run,
                second_serialized_run,
                third_serialized_run
            ],
            "users_allowed": [self.kive_user.username],
            "copy_permissions_to_runs": False
        }
        rb_serializer = RunBatchSerializer(data=serialized_rb, context=self.john_context)
        self.assertTrue(rb_serializer.is_valid())
        rb = rb_serializer.save()

        # Now we update the RunBatch.  This newly-added group won't be propagated to the runs.
        new_group = Group(name="Interlopers")
        new_group.save()
        update_dict = {
            "name": "My updated RunBatch",
            "groups_allowed": [new_group.name],
            "copy_permissions_to_runs": False
        }
        update_serializer = RunBatchSerializer(rb, data=update_dict, context=self.john_context)
        update_serializer.is_valid()  # note that we patched Run.is_complete
        update_serializer.save()

        # Probe the RunBatch to check that it was correctly updated.
        self.assertEqual(rb.user, self.myUser)
        self.assertEqual(rb.name, update_dict["name"])
        self.assertSetEqual(set(rb.users_allowed.all()), {self.kive_user})
        self.assertEqual(set(rb.groups_allowed.all()), {new_group})

        self.assertEqual(rb.runs.count(), 3)

        # All runs had the Everyone group added.
        run1 = rb.runs.get(name="One")
        self.assertFalse(run1.users_allowed.exists())
        self.assertFalse(run1.groups_allowed.exists())

        run2 = rb.runs.get(name="Two")
        self.assertSetEqual(set(run2.users_allowed.all()), {self.kive_user})
        self.assertFalse(run2.groups_allowed.exists())

        run3 = rb.runs.get(name="Three")
        self.assertFalse(run3.users_allowed.exists())
        self.assertFalse(run3.groups_allowed.exists())
