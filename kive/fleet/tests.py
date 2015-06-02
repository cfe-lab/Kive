import re
import os.path
import tempfile

from django.contrib.auth.models import User
from django.test import TestCase
from django.utils.dateparse import parse_datetime
from django.core.urlresolvers import reverse, resolve

from rest_framework.test import APIRequestFactory, force_authenticate

from sandbox.tests import ExecuteTestsBase
from sandbox.execute import Sandbox

from archive.models import Run, RunStep, RunSIC, ExecLog, RunOutputCable
from fleet.models import RunToProcess, RunToProcessInput, SandboxActiveException
from librarian.models import ExecRecord, SymbolicDataset
from pipeline.models import Pipeline
from metadata.models import CompoundDatatype, kive_user
from archive.models import Dataset
from kive import settings


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
        pipeline=Pipeline.objects.get(pk=2)
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
            re.escape("Run (Pipeline={}, queued {}) has not yet started".format(self.noop_pl, rtp.time_queued)),
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
            re.escape("Run (Pipeline={}, queued {}) is not finished".format(self.noop_pl, rtp.time_queued)),
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


class RunApiTests(ExecuteTestsBase):
    def setUp(self):
        super(RunApiTests, self).setUp()

        self.kive_user = kive_user()

        self.factory = APIRequestFactory()
        self.run_list_path = reverse('runtoprocess-list')
        self.run_list_view, _, _ = resolve(self.run_list_path)

    def tearDown(self):
        for d in Dataset.objects.all():
            d.dataset_file.delete()
        super(RunApiTests, self).tearDown()

    def setup_pipeline(self):
        # Define pipeline containing two steps with the same method + pipeline input
        self.pX = Pipeline(family=self.pf, revision_name="pX_revision", revision_desc="X", user=self.myUser)
        self.pX.save()
        self.X1_in = self.pX.create_input(compounddatatype=self.pX_in_cdt, dataset_name="pX_in", dataset_idx=1)
        self.step_X1 = self.pX.steps.create(transformation=self.mA, step_num=1)
        self.step_X2 = self.pX.steps.create(transformation=self.mA, step_num=2)

        # Use the SAME custom cable from pipeline input to steps 1 and 2
        self.cable_X1_A1 = self.step_X1.cables_in.create(dest=self.mA_in, source_step=0, source=self.X1_in)
        self.wire1 = self.cable_X1_A1.custom_wires.create(source_pin=self.pX_in_cdtm_2, dest_pin=self.mA_in_cdtm_2)
        self.wire2 = self.cable_X1_A1.custom_wires.create(source_pin=self.pX_in_cdtm_3, dest_pin=self.mA_in_cdtm_1)
        self.cable_X1_A2 = self.step_X2.cables_in.create(dest=self.mA_in, source_step=0, source=self.X1_in)
        self.wire3 = self.cable_X1_A2.custom_wires.create(source_pin=self.pX_in_cdtm_2, dest_pin=self.mA_in_cdtm_2)
        self.wire4 = self.cable_X1_A2.custom_wires.create(source_pin=self.pX_in_cdtm_3, dest_pin=self.mA_in_cdtm_1)

        # POCs: one is trivial, the second uses custom outwires
        # Note: by default, create_outcables assumes the POC has the CDT of the source (IE, this is a TRIVIAL cable)
        self.outcable_1 = self.pX.create_outcable(output_name="pX_out_1",output_idx=1,source_step=1,source=self.mA_out)
        self.outcable_2 = self.pX.create_outcable(output_name="pX_out_2",output_idx=2,source_step=2,source=self.mA_out)

        # Define CDT for the second output (first output is defined by a trivial cable)
        self.pipeline_out2_cdt = CompoundDatatype(user=self.myUser)
        self.pipeline_out2_cdt.save()
        self.out2_cdtm_1 = self.pipeline_out2_cdt.members.create(column_name="c",column_idx=1,datatype=self.int_dt)
        self.out2_cdtm_2 = self.pipeline_out2_cdt.members.create(column_name="d",column_idx=2,datatype=self.string_dt)
        self.out2_cdtm_3 = self.pipeline_out2_cdt.members.create(column_name="e",column_idx=3,datatype=self.string_dt)

        # Second cable is not a trivial - we assign the new CDT to it
        self.outcable_2.output_cdt = self.pipeline_out2_cdt
        self.outcable_2.save()

        # Define custom outwires to the second output (Wire twice from cdtm 2)
        self.outwire1 = self.outcable_2.custom_wires.create(source_pin=self.mA_out_cdtm_1, dest_pin=self.out2_cdtm_1)
        self.outwire2 = self.outcable_2.custom_wires.create(source_pin=self.mA_out_cdtm_2, dest_pin=self.out2_cdtm_2)
        self.outwire3 = self.outcable_2.custom_wires.create(source_pin=self.mA_out_cdtm_2, dest_pin=self.out2_cdtm_3)

        # Have the cables define the TOs of the pipeline
        self.pX.create_outputs()

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
        self.setup_pipeline()

        # Kick off the run
        request = self.factory.post(self.run_list_path, {'pipeline': self.pX.pk, 'input_1': self.symDS.pk})
        force_authenticate(request, user=self.myUser)
        response = self.run_list_view(request).render()
        data = response.render().data

        # Check that the run created something sensible
        self.assertEquals(data['id'], 1)
        self.assertIn('run_outputs', data)

        # Execute the pipeline
        rtp = RunToProcess.objects.all()[0]
        sbox = Sandbox(rtp.user, rtp.pipeline, [x.symbolicdataset for x in rtp.inputs.order_by("index")])
        rtp.run = sbox.run
        rtp.save()
        sbox.execute_pipeline()

        # Test and make sure we have a dataset now
        self.test_run_index(1)

        # Touch the record detail page
        path = self.run_list_path + "1/"
        request = self.factory.get(path)
        force_authenticate(request, user=self.myUser)
        view, args, kwargs = resolve(path)
        response = view(request, *args, **kwargs)
        data = response.render().data

        # Touch the run status page
        path = self.run_list_path + "1/run_status/"
        request = self.factory.get(path)
        force_authenticate(request, user=self.myUser)
        view, args, kwargs = resolve(path)
        response = view(request, *args, **kwargs)
        data = response.render().data
        self.assertEquals(data['status'], '**-**')
        self.assertIn('step_progress', data)

        # Touch the outputs
        path = self.run_list_path + "1/run_outputs/"
        request = self.factory.get(path)
        force_authenticate(request, user=self.myUser)
        view, args, kwargs = resolve(path)
        response = view(request, *args, **kwargs)
        data = response.render().data
        self.assertEquals(data['id'], 1)
        self.assertEquals(len(data['run']['output_summary']), 8)

        for output in data['run']['output_summary']:
            self.assertEquals(output['is_ok'], True)
            self.assertEquals(output['is_invalid'], False)

        # Touch the removal plan
        path = self.run_list_path + "1/removal_plan/"
        request = self.factory.get(path)
        force_authenticate(request, user=self.myUser)
        view, args, kwargs = resolve(path)
        response = view(request, *args, **kwargs)
        data = response.render().data

        self.assertEquals(data['SymbolicDatasets'], 3)
        self.assertEquals(data['Runs'], 1)
        self.assertEquals(data['Datatypes'], 0)

        # Delete the record
        path = self.run_list_path + "1/"
        request = self.factory.delete(path)
        force_authenticate(request, user=self.kive_user)
        view, args, kwargs = resolve(path)
        response = view(request, *args, **kwargs)
        self.assertEquals(response.render().data, None)
        self.test_run_index(0)

