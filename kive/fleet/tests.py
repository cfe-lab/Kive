import re
import os.path
import tempfile

from django.contrib.auth.models import User
from django.test import TestCase
from django.utils.dateparse import parse_datetime
from django.core.urlresolvers import reverse, resolve

from rest_framework.test import APIRequestFactory, force_authenticate

from archive.models import Run, RunStep, RunSIC, ExecLog, RunOutputCable
from fleet.models import RunToProcess, RunToProcessInput
from fleet.exceptions import SandboxActiveException
from librarian.models import ExecRecord, SymbolicDataset
from pipeline.models import Pipeline, PipelineFamily
from metadata.models import kive_user, RTPNotFinished
from kive.testing_utils import clean_up_all_files
from kive import settings
from kive.tests import install_fixture_files, restore_production_files


class RunToProcessTest(TestCase):
    """ Check various status reports. Status symbols are:
    ? - requested
    . - waiting
    : - ready
    + - running
    * - complete
    Overall format is steps-outcables-displayname
    """
    fixtures = ['initial_data', "initial_groups", 'initial_user', 'converter_pipeline']

    def test_run_progress_no_run(self):
        run_tracker = RunToProcess()
        
        progress = run_tracker.get_run_progress()
        
        self.assertSequenceEqual('?', progress['status'])
        self.assertSequenceEqual('Run', progress['name'])
        
    def test_owner(self):
        expected_username = 'dave'
        run_tracker = RunToProcess(user=User(username=expected_username))
        
        progress = run_tracker.get_run_progress()
        
        self.assertSequenceEqual(expected_username, progress['user'])

    def create_with_empty_pipeline(self):
        pipeline = Pipeline()
        run_tracker = RunToProcess(run=Run(pipeline=pipeline))
        return run_tracker

    def test_run_progress_empty_pipeline(self):
        run_tracker = self.create_with_empty_pipeline()
        
        progress = run_tracker.get_run_progress()
        
        self.assertSequenceEqual('-', progress['status'])

    def create_with_pipeline_step(self):
        pipeline = Pipeline.objects.get(pk=2)
        user = User.objects.first()
        run = Run(pipeline=pipeline, user=user)
        run.save()
        run_tracker = RunToProcess(run=run, user=user, pipeline=pipeline)
        return run_tracker

    def test_run_progress_starting(self):
        run_tracker = self.create_with_pipeline_step()
        
        progress = run_tracker.get_run_progress()
        
        self.assertSequenceEqual('.-.', progress['status'])
        self.assertSequenceEqual('Fasta2CSV', progress['name'])

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
        run_tracker = self.create_with_pipeline_step()
        run = run_tracker.run
        pipeline_step = run.pipeline.steps.first()
        run_step = RunStep(run=run, pipelinestep=pipeline_step)
        run_step.save()
        run_step_input_cable = RunSIC(PSIC=pipeline_step.cables_in.first())
        run_step.RSICs.add(run_step_input_cable)
        
        return run_tracker

    def test_run_progress_ready(self):
        run_tracker = self.create_with_run_step()
        
        progress = run_tracker.get_run_progress()
        
        self.assertSequenceEqual(':-.', progress['status'])

    def create_with_started_run_step(self):
        run_tracker = self.create_with_run_step()
        run_step = run_tracker.run.runsteps.first()
        run_step_input_cable = run_step.RSICs.first()
        self.add_exec_log(run_step_input_cable)
        self.add_exec_record(run_step_input_cable)
        self.add_exec_log(run_step)
        return run_tracker

    def create_with_completed_run_step(self):
        run_tracker = self.create_with_started_run_step()
        run_step = run_tracker.run.runsteps.first()
        exec_record = self.add_exec_record(run_step)
        exec_record.generator.methodoutput.return_code = 0
        exec_record.generator.methodoutput.save()
        return run_tracker

    def test_run_progress_started_steps(self):
        run_tracker = self.create_with_started_run_step()
        
        progress = run_tracker.get_run_progress()
        
        self.assertSequenceEqual('+-.', progress['status'])

    def test_run_progress_completed_steps(self):
        run_tracker = self.create_with_completed_run_step()
        
        progress = run_tracker.get_run_progress()
        
        self.assertSequenceEqual('*-.', progress['status'])

    def test_run_progress_failed_steps(self):
        run_tracker = self.create_with_completed_run_step()
        run_step = run_tracker.run.runsteps.first()
        exec_log = run_step.invoked_logs.first()
        exec_log.methodoutput.return_code = 5
        exec_log.methodoutput.save()
        run_step.save()
        
        progress = run_tracker.get_run_progress()
        
        self.assertSequenceEqual('!-.', progress['status'])

    def test_run_progress_output_ready(self):
        run_tracker = self.create_with_completed_run_step()
        run = run_tracker.run
        pipeline_output_cable = run.pipeline.outcables.first()
        run.runoutputcables.add(RunOutputCable(
            pipelineoutputcable=pipeline_output_cable))
        
        progress = run_tracker.get_run_progress()
        
        self.assertSequenceEqual('*-:', progress['status'])

    def test_run_progress_output_running(self):
        run_tracker = self.create_with_completed_run_step()
        run = run_tracker.run
        pipeline_output_cable = run.pipeline.outcables.first()
        run_output_cable = RunOutputCable(
            pipelineoutputcable=pipeline_output_cable)
        run.runoutputcables.add(run_output_cable)
        self.add_exec_log(run_output_cable)
        
        progress = run_tracker.get_run_progress()
        
        self.assertSequenceEqual('*-+', progress['status'])

    def test_run_progress_complete(self):
        run_tracker = self.create_with_completed_run_step()
        run = run_tracker.run
        pipeline_output_cable = run.pipeline.outcables.first()
        run_output_cable = RunOutputCable(
            pipelineoutputcable=pipeline_output_cable)
        run.runoutputcables.add(run_output_cable)
        self.add_exec_log(run_output_cable)
        self.add_exec_record(run_output_cable)
        
        progress = run_tracker.get_run_progress()
        
        self.assertSequenceEqual('*-*', progress['status'])

    def add_input(self, run_tracker):
        run_tracker.save()
        symbolicdataset = SymbolicDataset.objects.get(pk=1)
        run_input = RunToProcessInput(runtoprocess=run_tracker,
                                      symbolicdataset=symbolicdataset,
                                      index=1)
        run_input.save()
        
    def test_run_progress_display_name(self):
        run_tracker = self.create_with_pipeline_step()
        self.add_input(run_tracker)
        
        progress = run_tracker.get_run_progress()
        
        self.assertSequenceEqual('Fasta2CSV on TestFASTA', progress['name'])

    def test_run_progress_display_name_but_no_run(self):
        pipeline=Pipeline.objects.get(pk=2)
        user = User.objects.first()
        run_tracker = RunToProcess(user=user, pipeline=pipeline)
        self.add_input(run_tracker)
        
        progress = run_tracker.get_run_progress()
        
        self.assertSequenceEqual('Fasta2CSV on TestFASTA', progress['name'])

    def test_display_name(self):
        pipeline=Pipeline.objects.get(pk=2)
        user = User.objects.first()
        run_tracker = RunToProcess(user=user, pipeline=pipeline)
        self.add_input(run_tracker)
        
        display_name = run_tracker.display_name

        self.assertSequenceEqual(u'Fasta2CSV on TestFASTA', display_name)

    def test_display_name_no_input(self):
        pipeline=Pipeline.objects.get(pk=2)
        user = User.objects.first()
        run_tracker = RunToProcess(user=user, pipeline=pipeline)
        run_tracker.save()
        run_tracker.time_queued = parse_datetime('2015-01-13 00:00:00Z')
        run_tracker.save()
        
        display_name = run_tracker.display_name

        self.assertSequenceEqual('Fasta2CSV at 2015-01-13 00:00:00+00:00',
                                 display_name)


class RemoveRedactRunInProgress(TestCase):
    fixtures = ["em_sandbox_test_environment"]

    def setUp(self):
        # Clear out all the Runs and ExecRecords in the environment.
        # Run.objects.all().delete()
        # ExecRecord.objects.all().delete()

        self.pf = PipelineFamily.objects.get(name="Pipeline_family")
        self.myUser = self.pf.user
        self.pE = self.pf.members.get(revision_name="pE_name")
        self.triplet_symDS = SymbolicDataset.objects.filter(dataset__name="triplet").first()
        self.doublet_symDS = SymbolicDataset.objects.get(dataset__name="doublet")
        self.singlet_symDS = SymbolicDataset.objects.filter(dataset__name="singlet").first()
        self.raw_symDS = SymbolicDataset.objects.get(dataset__name="raw_DS")
        self.step_E1 = self.pE.steps.get(step_num=1)
        self.mA = self.step_E1.transformation.definite

        # A run that's mid-progress.
        self.run = Run(pipeline=self.pE, user=self.myUser)
        self.run.save()
        self.run_tracker = RunToProcess(run=self.run, user=self.myUser, pipeline=self.pE)
        self.run_tracker.save()
        self.run_tracker.inputs.create(
            index=1,
            symbolicdataset=self.triplet_symDS
        )
        self.run_tracker.inputs.create(
            index=2,
            symbolicdataset=self.singlet_symDS
        )
        self.run_tracker.inputs.create(
            index=3,
            symbolicdataset=self.raw_symDS
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
            symbolicdataset=self.raw_symDS
        )
        self.rsic.execrecord.execrecordouts.create(
            generic_output=self.step_E1.transformation.definite.inputs.first(),
            symbolicdataset=self.raw_symDS
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
            symbolicdataset=self.raw_symDS
        )
        self.rs_1.execrecord.execrecordouts.create(
            generic_output=self.mA.outputs.first(),
            symbolicdataset=self.doublet_symDS
        )

    def tearDown(self):
        clean_up_all_files()

    def test_remove_pipeline_fails(self):
        """
        Removing the Pipeline of a Run that's in progress should fail.
        """
        self.assertRaisesRegexp(
            RTPNotFinished,
            "Cannot remove: an affected run is still in progress",
            lambda: self.pE.remove()
        )

    def test_remove_dataset_fails(self):
        """
        Removing a Dataset of a Run that's in progress should fail.
        """
        self.assertRaisesRegexp(
            RTPNotFinished,
            "Cannot remove: an affected run is still in progress",
            lambda: self.triplet_symDS.remove()
        )

    def test_redact_dataset_fails(self):
        """
        Redacting a Dataset of a Run that's in progress should fail.
        """
        self.assertRaisesRegexp(
            RTPNotFinished,
            "Cannot redact: an affected run is still in progress",
            lambda: self.triplet_symDS.redact()
        )

    def test_remove_execrecord_fails(self):
        """
        Removing an ExecRecord of a Run that's in progress should fail.
        """
        self.assertRaisesRegexp(
            RTPNotFinished,
            "Cannot remove: an affected run is still in progress",
            lambda: self.rs_1.execrecord.remove()
        )

    def test_redact_execrecord_fails(self):
        """
        Redacting an ExecRecord of a Run that's in progress should fail.
        """
        self.assertRaisesRegexp(
            RTPNotFinished,
            "Cannot redact: an affected run is still in progress",
            lambda: self.rs_1.execrecord.redact()
        )


class RemoveRedactRunJustStarting(TestCase):
    """
    Testing of removal/redaction of stuff used in a run that's just starting.

    FIXME this might be eliminated when we merge Run and RunToProcess.
    """
    fixtures = ["em_sandbox_test_environment"]

    def setUp(self):
        self.pf = PipelineFamily.objects.get(name="Pipeline_family")
        self.myUser = self.pf.user
        self.pE = self.pf.members.get(revision_name="pE_name")
        self.triplet_symDS = SymbolicDataset.objects.filter(dataset__name="triplet").first()
        self.doublet_symDS = SymbolicDataset.objects.get(dataset__name="doublet")
        self.singlet_symDS = SymbolicDataset.objects.filter(dataset__name="singlet").first()
        self.raw_symDS = SymbolicDataset.objects.get(dataset__name="raw_DS")

        # A run that's just starting, to the point that no Run exists yet.
        self.rtp_just_starting = RunToProcess(user=self.myUser, pipeline=self.pE)
        self.rtp_just_starting.save()
        self.rtp_just_starting.inputs.create(
            index=1,
            symbolicdataset=self.triplet_symDS
        )
        self.rtp_just_starting.inputs.create(
            index=2,
            symbolicdataset=self.singlet_symDS
        )
        self.rtp_just_starting.inputs.create(
            index=3,
            symbolicdataset=self.raw_symDS
        )

    def tearDown(self):
        clean_up_all_files()

    def test_remove_pipeline_fails(self):
        """
        Removing the Pipeline of a Run should fail.
        """
        self.assertRaisesRegexp(
            RTPNotFinished,
            "Cannot remove: an affected run is still in progress",
            lambda: self.pE.remove()
        )

    def test_remove_dataset_fails(self):
        """
        Removing a Dataset of a Run should fail.
        """
        self.assertRaisesRegexp(
            RTPNotFinished,
            "Cannot remove: an affected run is still in progress",
            lambda: self.triplet_symDS.remove()
        )

    def test_redact_dataset_fails(self):
        """
        Redacting a Dataset of a Run should fail.
        """
        self.assertRaisesRegexp(
            RTPNotFinished,
            "Cannot redact: an affected run is still in progress",
            lambda: self.triplet_symDS.redact()
        )


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
        A RunToProcess that has no sandbox path should raise an exception.
        """
        rtp = RunToProcess(pipeline=self.noop_pl, user=self.noop_pl.user)
        rtp.save()

        self.assertRaisesRegexp(
            SandboxActiveException,
            re.escape("Run (RunToProcess={}, Pipeline={}, queued {}, User=Rem Over) has not yet started".format(
                rtp.id,
                self.noop_pl,
                rtp.time_queued)),
            rtp.collect_garbage
        )

    def test_reap_unfinished_run(self):
        """
        A RunToProcess that is not marked as finished should raise an exception.
        """
        rtp = RunToProcess(pipeline=self.noop_pl, user=self.noop_pl.user)
        rtp.save()
        run = Run(pipeline=self.noop_pl, user=self.noop_pl.user)
        run.save()
        rtp.sandbox_path = self.mock_sandbox_path
        rtp.save()

        self.assertRaisesRegexp(
            SandboxActiveException,
            re.escape("Run (RunToProcess={}, Pipeline={}, queued {}, User=Rem Over) is not finished".format(
                rtp.id,
                self.noop_pl,
                rtp.time_queued)),
            rtp.collect_garbage
        )

    def test_reap_finished_run(self):
        """
        A RunToProcess that is not marked as finished should raise an exception.
        """
        rtp = RunToProcess(
            pipeline=self.noop_pl, run=self.noop_run,
            sandbox_path=self.mock_sandbox_path,
            user=self.noop_run.user)
        rtp.save()
        rtp.collect_garbage()

        self.assertFalse(os.path.exists(self.mock_sandbox_path))
        self.assertTrue(rtp.purged)

    def test_reap_on_removal(self):
        """
        Removing a Run should reap the sandbox.
        """
        rtp = RunToProcess(
            pipeline=self.noop_pl, run=self.noop_run,
            sandbox_path=self.mock_sandbox_path,
            user=self.noop_run.user)
        rtp.save()

        self.noop_run.remove()
        self.assertFalse(os.path.exists(self.mock_sandbox_path))

    def test_reap_on_redaction(self):
        """
        Redacting part of a Run should reap the sandbox.
        """
        rtp = RunToProcess(
            pipeline=self.noop_pl, run=self.noop_run,
            sandbox_path=self.mock_sandbox_path,
            user=self.noop_run.user)
        rtp.save()

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

        self.factory = APIRequestFactory()
        self.run_list_path = reverse('runtoprocess-list')
        self.run_list_view, _, _ = resolve(self.run_list_path)

    def tearDown(self):
        clean_up_all_files()
        restore_production_files()

    def test_run_index(self, expected_runs=0):
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

    def test_pipeline_execute_plus_details_and_run_remove(self):
        # TODO: This should be split into one test to test the pipeline execution
        # Plus many tests to test details (which needs a proper fixture)
        pipeline_to_run = Pipeline.objects.get(
            family__name="self.pf",
            revision_name="pX_revision_2"
        )
        symDS = SymbolicDataset.objects.get(
            dataset__name="pX_in_symDS",
            structure__isnull=False,
            user=self.myUser
        )

        # Kick off the run
        request = self.factory.post(self.run_list_path, {'pipeline': pipeline_to_run.pk, 'input_1': symDS.pk})
        force_authenticate(request, user=self.myUser)
        response = self.run_list_view(request).render()
        data = response.render().data

        # Check that the run created something sensible
        self.assertIn("id", data)
        rtp_pk = data["id"]
        self.assertIn('run_outputs', data)

        # Faux-execute the Pipeline.
        rtp = RunToProcess.objects.get(pk=rtp_pk)
        rtp.run = pipeline_to_run.pipeline_instances.first()
        rtp.save()

        # Test and make sure we have a Run now
        self.test_run_index(1)

        # Touch the record detail page
        path = self.run_list_path + "{}/".format(rtp_pk)
        request = self.factory.get(path)
        force_authenticate(request, user=self.myUser)
        view, args, kwargs = resolve(path)
        response = view(request, *args, **kwargs)
        data = response.render().data

        # Touch the run status page
        path = self.run_list_path + "{}/run_status/".format(rtp_pk)
        request = self.factory.get(path)
        force_authenticate(request, user=self.myUser)
        view, args, kwargs = resolve(path)
        response = view(request, *args, **kwargs)
        data = response.render().data
        self.assertEquals(data['status'], '**-**')
        self.assertIn('step_progress', data)

        # Touch the outputs
        path = self.run_list_path + "{}/run_outputs/".format(rtp_pk)
        request = self.factory.get(path)
        force_authenticate(request, user=self.myUser)
        view, args, kwargs = resolve(path)
        response = view(request, *args, **kwargs)
        data = response.render().data
        self.assertEquals(data['id'], rtp_pk)
        self.assertEquals(len(data['run']['output_summary']), 8)

        for output in data['run']['output_summary']:
            self.assertEquals(output['is_ok'], True)
            self.assertEquals(output['is_invalid'], False)

        # Touch the removal plan
        path = self.run_list_path + "{}/removal_plan/".format(rtp_pk)
        request = self.factory.get(path)
        force_authenticate(request, user=self.myUser)
        view, args, kwargs = resolve(path)
        response = view(request, *args, **kwargs)
        data = response.render().data

        self.assertEquals(data['SymbolicDatasets'], 3)
        self.assertEquals(data['Runs'], 1)
        self.assertEquals(data['Datatypes'], 0)

        # Delete the record
        path = self.run_list_path + "{}/".format(rtp_pk)
        request = self.factory.delete(path)
        force_authenticate(request, user=self.kive_user)
        view, args, kwargs = resolve(path)
        response = view(request, *args, **kwargs)
        self.assertEquals(response.render().data, None)
        self.test_run_index(0)

