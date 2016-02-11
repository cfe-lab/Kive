import os.path
import re
import tempfile
import itertools

from django.contrib.auth.models import User
from django.test import TestCase
from django.utils.dateparse import parse_datetime
from django.core.urlresolvers import reverse, resolve
from django.utils import timezone

from rest_framework.test import APIRequestFactory, force_authenticate
from rest_framework import status

from archive.models import Run, RunStep, RunSIC, ExecLog, RunOutputCable,\
    RunInput
from archive.serializers import RunSerializer
from archive.exceptions import SandboxActiveException, RunNotFinished
from librarian.models import ExecRecord, Dataset
from pipeline.models import Pipeline, PipelineFamily
from metadata.models import kive_user, everyone_group
from kive.testing_utils import clean_up_all_files
from kive import settings
from kive.tests import install_fixture_files, restore_production_files, DuckContext
from fleet.workers import Manager
from sandbox.execute import Sandbox


class QueuedRunTest(TestCase):
    """ Check various status reports. Status symbols are:
    ? - requested
    . - waiting
    : - ready
    + - running
    * - complete
    Overall format is steps-outcables-displayname
    """
    fixtures = ['initial_data', "initial_groups", 'initial_user', 'converter_pipeline']

    def setUp(self):
        self.converter_pf = PipelineFamily.objects.get(name="Fasta2CSV")
        self.converter_pl = self.converter_pf.members.first()

    def test_owner(self):
        expected_username = 'dave'
        run = Run(user=User(username=expected_username), pipeline=self.converter_pl)

        progress = run.get_run_progress()

        self.assertSequenceEqual(expected_username, progress['user'])

    def create_with_empty_pipeline(self):
        pipeline = Pipeline(family=self.converter_pf, user=kive_user())
        pipeline.save()

        run = Run(pipeline=pipeline, user=kive_user())
        run.save()
        run.start()
        return run

    def test_run_progress_empty_pipeline(self):
        run = self.create_with_empty_pipeline()

        progress = run.get_run_progress()

        self.assertSequenceEqual('-', progress['status'])

    def create_with_pipeline_step(self):
        pipeline = Pipeline.objects.get(pk=2)
        user = User.objects.first()
        run = Run(pipeline=pipeline, user=user)
        run.save()
        run.start()
        return run

    def test_run_progress_starting(self):
        run = self.create_with_pipeline_step()

        progress = run.get_run_progress()

        self.assertSequenceEqual('.-.', progress['status'])
        self.assertSequenceEqual('Fasta2CSV at {}'.format(run.time_queued), progress['name'])

    def add_exec_log(self, run_component):
        ExecLog.create(record=run_component,
                       invoking_record=run_component)

    def add_exec_record(self, run_component):
        generator = run_component.log
        execrecord = ExecRecord(generator=generator)
        execrecord.save()
        run_component.execrecord = execrecord
        run_component.save()
        return execrecord

    def create_with_run_step(self):
        run = self.create_with_pipeline_step()
        pipeline_step = run.pipeline.steps.first()
        run_step = RunStep(run=run,
                           pipelinestep=pipeline_step,
                           start_time=timezone.now())
        run_step.save()
        run_step_input_cable = RunSIC(PSIC=pipeline_step.cables_in.first(),
                                      dest_runstep=run_step).save()

        return run

    def test_run_progress_ready(self):
        run = self.create_with_run_step()

        progress = run.get_run_progress()

        self.assertSequenceEqual(':-.', progress['status'])

    def create_with_started_run_step(self):
        run = self.create_with_run_step()
        run_step = run.runsteps.first()
        run_step_input_cable = run_step.RSICs.first()
        self.add_exec_log(run_step_input_cable)
        self.add_exec_record(run_step_input_cable)
        self.add_exec_log(run_step)
        return run

    def create_with_completed_run_step(self):
        run = self.create_with_started_run_step()
        run_step = run.runsteps.first()
        exec_record = self.add_exec_record(run_step)
        exec_record.generator.methodoutput.return_code = 0
        exec_record.generator.methodoutput.save()
        return run

    def test_run_progress_started_steps(self):
        run = self.create_with_started_run_step()

        progress = run.get_run_progress()

        self.assertSequenceEqual('+-.', progress['status'])

    def test_run_progress_completed_steps(self):
        run = self.create_with_completed_run_step()

        progress = run.get_run_progress()

        self.assertSequenceEqual('*-.', progress['status'])

    def test_run_progress_failed_steps(self):
        run = self.create_with_completed_run_step()
        run_step = run.runsteps.first()
        exec_log = run_step.invoked_logs.first()
        exec_log.methodoutput.return_code = 5
        exec_log.methodoutput.save()
        run_step.save()

        progress = run.get_run_progress()

        self.assertSequenceEqual('!-.', progress['status'])

    def test_run_progress_output_ready(self):
        run = self.create_with_completed_run_step()
        pipeline_output_cable = run.pipeline.outcables.first()
        roc = RunOutputCable(pipelineoutputcable=pipeline_output_cable, run=run)
        roc.save()

        progress = run.get_run_progress()

        self.assertSequenceEqual('*-:', progress['status'])

    def test_run_progress_output_running(self):
        run = self.create_with_completed_run_step()
        pipeline_output_cable = run.pipeline.outcables.first()
        run_output_cable = RunOutputCable(
            pipelineoutputcable=pipeline_output_cable,
            run=run
        )
        run_output_cable.save()
        self.add_exec_log(run_output_cable)

        progress = run.get_run_progress()

        self.assertSequenceEqual('*-+', progress['status'])

    def test_run_progress_complete(self):
        run = self.create_with_completed_run_step()
        pipeline_output_cable = run.pipeline.outcables.first()
        run_output_cable = RunOutputCable(
            pipelineoutputcable=pipeline_output_cable,
            run=run
        )
        run_output_cable.save()
        self.add_exec_log(run_output_cable)
        self.add_exec_record(run_output_cable)

        progress = run.get_run_progress()

        self.assertSequenceEqual('*-*', progress['status'])

    def add_input(self, run):
        run.save()
        dataset = Dataset.objects.get(pk=1)
        run_input = RunInput(run=run,
                             dataset=dataset,
                             index=1)
        run_input.save()

    def test_run_progress_display_name(self):
        run = self.create_with_pipeline_step()
        self.add_input(run)

        progress = run.get_run_progress()

        self.assertSequenceEqual('Fasta2CSV on TestFASTA', progress['name'])

    def test_run_progress_display_name_but_no_run(self):
        pipeline = Pipeline.objects.get(pk=2)
        user = User.objects.first()
        run = Run(user=user, pipeline=pipeline)
        self.add_input(run)

        progress = run.get_run_progress()

        self.assertSequenceEqual('Fasta2CSV on TestFASTA', progress['name'])

    def test_display_name(self):
        pipeline = Pipeline.objects.get(pk=2)
        user = User.objects.first()
        run = Run(user=user, pipeline=pipeline)
        self.add_input(run)

        display_name = run.display_name

        self.assertSequenceEqual(u'Fasta2CSV on TestFASTA', display_name)

    def test_display_name_no_input(self):
        pipeline = Pipeline.objects.get(pk=2)
        user = User.objects.first()
        run = Run(user=user, pipeline=pipeline, time_queued=parse_datetime('2015-01-13 00:00:00Z'))
        run.save()

        display_name = run.display_name

        self.assertSequenceEqual('Fasta2CSV at 2015-01-13 00:00:00+00:00',
                                 display_name)

    def test_display_name_run_name_set(self):
        pipeline = Pipeline.objects.get(pk=2)
        user = User.objects.first()

        run_name = "Test Run name"
        run = Run(user=user, pipeline=pipeline, name=run_name, time_queued=parse_datetime('2015-01-13 00:00:00Z'))
        run.save()

        display_name = run.display_name

        self.assertEqual(run_name, display_name)


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


class RestoreReusableDatasetTest(TestCase):
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
        restore_production_files()

    def create_run_to_process(self, pipeline):
        dataset = Dataset.objects.get(name='pairs')
        run_to_process = Run(pipeline=pipeline, user=pipeline.user)
        run_to_process.save()
        run_to_process.clean()
        run_to_process.inputs.create(dataset=dataset, index=1)
        return run_to_process

    def execute_pipeline(self, pipeline):
        run_to_process = self.create_run_to_process(pipeline)

        manager = Manager(0)
        manager.max_host_cpus = 1
        manager.worker_status = {}
        manager.find_new_runs()
        while manager.task_queue:
            tasks = manager.task_queue
            manager.task_queue = []
            for sandbox, task in tasks:
                task_info = sandbox.get_task_info(task)
                task_info_dict = task_info.dict_repr()
                worker_rank = 1
                manager.tasks_in_progress[worker_rank] = {"task": task,
                                                          "vassals": []}
                if type(task) == RunStep:
                    sandbox_result = Sandbox.finish_step(task_info_dict, worker_rank)
                else:
                    sandbox_result = Sandbox.finish_cable(task_info_dict, worker_rank)
                manager.note_progress(worker_rank, sandbox_result)

        return Run.objects.get(id=run_to_process.id)

    def test_run_new_pipeline(self):
        pipeline = Pipeline.objects.get(revision_name='sums and products')

        run_to_process = self.execute_pipeline(pipeline)

        self.assertTrue(run_to_process.is_successful())

    def test_rerun_old_pipeline(self):
        pipeline = Pipeline.objects.get(revision_name='sums only')
        expected_execrecord_count = ExecRecord.objects.count()

        run_to_process = self.execute_pipeline(pipeline)

        self.assertTrue(run_to_process.is_successful())
        self.assertEqual(expected_execrecord_count, ExecRecord.objects.count())


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
            dir=os.path.join(settings.MEDIA_ROOT, settings.SANDBOX_PATH))

    def test_reap_nonexistent_sandbox_path(self):
        """
        A Run that has no sandbox path should raise an exception.
        """
        run = Run(pipeline=self.noop_pl, user=self.noop_pl.user)
        run.save()

        self.assertRaisesRegexp(
            SandboxActiveException,
            re.escape("Run (pk={}, Pipeline={}, queued {}, User=Rem Over) has no sandbox path".format(
                run.id,
                self.noop_pl,
                run.time_queued)),
            run.collect_garbage
        )

    def test_reap_unfinished_run(self):
        """
        A Run that is not marked as finished should raise an exception.
        """
        run = Run(pipeline=self.noop_pl, user=self.noop_pl.user, sandbox_path=self.mock_sandbox_path)
        run.save()

        self.assertRaisesRegexp(
            SandboxActiveException,
            re.escape("Run (pk={}, Pipeline={}, queued {}, User=Rem Over) is not finished".format(
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
        restore_production_files()

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


class RunSerializerTests(TestCase):
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
        restore_production_files()

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
        self.assertEquals(rtp_serializer.errors["non_field_errors"],
                          [u"Number of inputs must equal the number of Pipeline inputs"])

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
        self.assertEquals(rtp_serializer.errors["non_field_errors"],
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
        self.assertEquals(rtp_serializer.errors["non_field_errors"],
                          [u"Pipeline {} has no input with index {}".format(
                              self.em_pipeline, 4
                          )])

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
            set(rtp_serializer.errors["non_field_errors"]),
            set([
                u"User(s) {} may not be granted access".format([self.kive_user]),
                u"Group(s) {} may not be granted access".format([everyone_group()])
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
