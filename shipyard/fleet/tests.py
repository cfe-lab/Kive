from django.test import TestCase
from fleet.models import RunToProcess
from archive.models import Run, RunStep, RunSIC, ExecLog, RunOutputCable
from pipeline.models import Pipeline
from django.contrib.auth.models import User
from librarian.models import ExecRecord

class RunToProcessTest(TestCase):
    fixtures = ['initial_user', 'converter_pipeline']
    def test_run_progress_no_run(self):
        run_tracker = RunToProcess()
        
        progress = run_tracker.get_run_progress()
        
        self.assertSequenceEqual('Waiting', progress)

    def create_with_empty_pipeline(self):
        pipeline = Pipeline()
        run_tracker = RunToProcess(run=Run(pipeline=pipeline))
        return run_tracker

    def test_run_progress_empty_pipeline(self):
        run_tracker = self.create_with_empty_pipeline()
        
        progress = run_tracker.get_run_progress()
        
        self.assertSequenceEqual('Complete', progress)

    def create_with_pipeline_step(self):
        pipeline=Pipeline.objects.get(pk=2)
        run = Run(pipeline=pipeline, user=User.objects.first())
        run.save()
        run_tracker = RunToProcess(run=run)
        return run_tracker

    def test_run_progress_starting(self):
        run_tracker = self.create_with_pipeline_step()
        
        progress = run_tracker.get_run_progress()
        
        self.assertSequenceEqual('Starting step 1 of 1', progress)

    def add_exec_record(self, run_component):
        generator = ExecLog.create(record=run_component,
                                   invoking_record=run_component)
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

    def test_run_progress_running(self):
        run_tracker = self.create_with_run_step()
        
        progress = run_tracker.get_run_progress()
        
        self.assertSequenceEqual('Running step 1 of 1', progress)

    def create_with_completed_run_step(self):
        run_tracker = self.create_with_run_step()
        run_step = run_tracker.run.runsteps.first()
        run_step_input_cable = run_step.RSICs.first()
        self.add_exec_record(run_step_input_cable)
        exec_record = self.add_exec_record(run_step)
        exec_record.generator.methodoutput.return_code = 0
        exec_record.generator.methodoutput.save()
        return run_tracker

    def test_run_progress_completed_steps(self):
        run_tracker = self.create_with_completed_run_step()
        
        progress = run_tracker.get_run_progress()
        
        self.assertSequenceEqual('Starting output 1 of 1', progress)

    def test_run_progress_failed_steps(self):
        run_tracker = self.create_with_completed_run_step()
        run_step = run_tracker.run.runsteps.first()
        exec_log = run_step.invoked_logs.first()
        exec_log.methodoutput.return_code = 5
        exec_log.methodoutput.save()
        
        progress = run_tracker.get_run_progress()
        
        self.assertSequenceEqual('Step 1 of 1 failed (Return code 5)', progress)

    def test_run_progress_creating_output(self):
        run_tracker = self.create_with_completed_run_step()
        run = run_tracker.run
        pipeline_output_cable = run.pipeline.outcables.first()
        run.runoutputcables.add(RunOutputCable(
            pipelineoutputcable=pipeline_output_cable))
        
        progress = run_tracker.get_run_progress()
        
        self.assertSequenceEqual('Creating output 1 of 1', progress)

    def test_run_progress_complete(self):
        run_tracker = self.create_with_completed_run_step()
        run = run_tracker.run
        pipeline_output_cable = run.pipeline.outcables.first()
        run_output_cable = RunOutputCable(
            pipelineoutputcable=pipeline_output_cable)
        run.runoutputcables.add(run_output_cable)
        self.add_exec_record(run_output_cable)
        
        progress = run_tracker.get_run_progress()
        
        self.assertSequenceEqual('Complete', progress)
