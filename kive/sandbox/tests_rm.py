from django.test import TestCase, TransactionTestCase
from django.utils import timezone

from librarian.models import SymbolicDataset
from sandbox.execute import Sandbox
import sandbox.testing_utils as tools


# def rmf(path):
#     try:
#         os.remove(path)
#     except OSError:
#         pass


class SandboxRMTestCase(TestCase):
    fixtures = ["initial_data", "initial_groups", "initial_user"]

    def setUp(self):
        tools.create_sandbox_testing_tools_environment(self)

    def tearDown(self):
        tools.destroy_sandbox_testing_tools_environment(self)


class SandboxRMTransactionTestCase(TransactionTestCase):
    fixtures = ["initial_data", "initial_groups", "initial_user"]

    def setUp(self):
        tools.create_sandbox_testing_tools_environment(self)

    def tearDown(self):
        tools.destroy_sandbox_testing_tools_environment(self)


class ExecuteTestsRM(TestCase):
    fixtures = ["initial_data", "initial_groups", "initial_user"]

    def setUp(self):
        tools.create_sequence_manipulation_environment(self)

    def tearDown(self):
        tools.destroy_sequence_manipulation_environment(self)

    def test_execute_pipeline_run(self):
        """
        Check the coherence of Runs created when a pipeline is executed the first time.
        """
        run = self.sandbox_complement.execute_pipeline()
        self.assertEqual(run.user, self.user_alice)
        self.assertEqual(run.start_time.date(), timezone.now().date())
        self.assertEqual(run.start_time < timezone.now(), True)
        self.assertEqual(run.is_complete(), True)
        self.assertEqual(run.parent_runstep, None)
        self.assertEqual(run.complete_clean(), None)

    def test_execute_pipeline_runstep(self):
        """
        Check the coherence of a RunStep created when a Pipeline is executed the first time.
        """
        run = self.sandbox_complement.execute_pipeline()
        # sandbox_complement has only one step, so this is OK.
        runstep = run.runsteps.first()

        self.assertEqual(runstep.run, run)
        self.assertEqual(runstep.start_time.date(), timezone.now().date())
        self.assertEqual(runstep.start_time < timezone.now(), True)
        self.assertEqual(runstep.reused, False)
        self.assertEqual(runstep.is_complete(), True)
        self.assertEqual(runstep.complete_clean(), None)
        self.assertEqual(hasattr(runstep, "child_run"), False)
        self.assertEqual(runstep.successful_execution(), True)
        self.assertEqual(runstep.outputs.count(), 1)

    def test_execute_pipeline_symds_contents(self):
        """
        Test that the content checks, which take place as part of Pipeline
        execution, pass in the ordinary Pipeline execution case.
        """
        run = self.sandbox_complement.execute_pipeline() # 1 step
        runstep = run.runsteps.first()
        execrecord = runstep.execrecord
        symds = execrecord.execrecordouts.first().symbolicdataset
        check = symds.content_checks.first()

        self.assertEqual(symds.content_checks.count(), 1) # should have been checked once
        self.assertEqual(check.symbolicdataset, symds)
        self.assertEqual(check.end_time is None, False)
        self.assertEqual(check.start_time <= check.end_time, True)
        self.assertEqual(check.start_time.date(), check.end_time.date())
        self.assertEqual(check.is_fail(), False)

    def test_execute_pipeline_symbolicdataset(self):
        """
        Test the integrity of a SymbolicDataset output by a PipelineStep in
        the middle of a Pipeline.
        """
        run = self.sandbox_complement.execute_pipeline() # 1 step
        runstep = run.runsteps.first()
        execrecord = runstep.execrecord
        symds = execrecord.execrecordouts.first().symbolicdataset
        ds = runstep.outputs.first()

        self.assertEqual(symds.MD5_checksum, self.labdata_compd_md5)
        self.assertEqual(symds.dataset, ds)
        self.assertEqual(hasattr(symds, "usurps"), False)
        self.assertEqual(symds.has_data(), True)
        self.assertEqual(symds.num_rows(), 10)
        self.assertEqual(symds.is_raw(), False)
        self.assertEqual(symds.get_cdt(), self.cdt_record)
        self.assertEqual(symds.structure.compounddatatype, self.cdt_record)
        self.assertEqual(symds.structure.num_rows, 10)
        self.assertEqual(symds.is_OK(), True)

    def test_execute_pipeline_runstep_execrecordout(self):
        """
        Check the coherence of a RunStep's ExecRecord's ExecRecordOut, created
        when a Pipeline is executed the first time.
        """
        pipelinestep = self.pipeline_complement.steps.first() # 1 step
        run = self.sandbox_complement.execute_pipeline()
        runstep = run.runsteps.first()
        symds_out = runstep.outputs.first().symbolicdataset
        execlog = runstep.log
        execrecord = runstep.execrecord
        execrecordout = execrecord.execrecordouts.first()

        self.assertEqual(execrecordout is None, False)
        self.assertEqual(execrecordout.execrecord, execrecord)
        self.assertEqual(execrecordout.symbolicdataset, symds_out)
        self.assertEqual(execrecordout.generic_output.definite, pipelinestep.transformation.outputs.first())
        self.assertEqual(execrecordout.has_data(), True)
        self.assertEqual(execrecordout.is_OK(), True)
        self.assertNotEqual(None, execlog)

    def test_execute_pipeline_runstep_execrecord(self):
        """
        Check the coherence of a RunStep's ExecRecord, created when a Pipeline
        is executed the first time.
        """
        run = self.sandbox_complement.execute_pipeline() # 1 step
        runstep = run.runsteps.first()
        execlog = runstep.log
        execrecord = runstep.execrecord
        outputs = self.method_complement.outputs.all()

        self.assertEqual(execrecord.generator, execlog)
        #self.assertEqual(execrecord.runsteps.first(), runstep)
        #self.assertEqual(execrecord.runs.first(), run)
        self.assertEqual(execrecord.complete_clean(), None)
        self.assertEqual(execrecord.general_transf(), runstep.pipelinestep.transformation.method)
        self.assertEqual(execrecord.provides_outputs(outputs), True)
        self.assertEqual(execrecord.outputs_OK(), True)

    def test_execute_pipeline_twice(self):
        """
        You can't execute a pipeline twice in the same Sandbox.
        """
        run1 = self.sandbox_complement.execute_pipeline()
        run2 = self.sandbox_complement.execute_pipeline()
        self.assertEqual(run1 is run2, True) 

    def test_execute_pipeline_reuse(self):
        """
        An identical pipeline, run in a different sandbox, should reuse an ExecRecord
        and not create an ExecLog.
        """
        self.sandbox_complement.execute_pipeline()
        sandbox2 = Sandbox(self.user_alice, self.pipeline_complement, [self.symds_labdata])
        sandbox2.execute_pipeline()

        step1 = self.sandbox_complement.run.runsteps.first()
        step2 = sandbox2.run.runsteps.first()

        self.assertEqual(step1.reused, False)
        self.assertEqual(step2.reused, True)
        self.assertFalse(step2.has_log)
        self.assertEqual(step1.execrecord, step2.execrecord)

    def test_execute_pipeline_fill_in_ER(self):
        """
        Running an identical Pipeline where we did not keep the data around the first time
        should fill in an existing ExecRecord, but also create a new ExecLog.
        """

        sandbox = Sandbox(self.user_alice, self.pipeline_complement, [self.symds_labdata])
        sandbox.execute_pipeline()
        self.sandbox_complement.execute_pipeline()

        step1 = sandbox.run.runsteps.first()
        step2 = self.sandbox_complement.run.runsteps.first()

        self.assertEqual(step1.reused, False)
        self.assertEqual(step2.reused, True)
        self.assertTrue(step1.has_log)
        self.assertFalse(step2.has_log)
        self.assertEqual(step1.execrecord, step2.execrecord)

    def test_execute_pipeline_reuse_within_different_pipeline(self):
        """
        Running the same dataset through the same Method, in two different 
        pipelines, should reuse an ExecRecord.
        """
        sandbox_reverse = Sandbox(self.user_alice, self.pipeline_reverse, [self.symds_labdata])
        sandbox_revcomp = Sandbox(self.user_alice, self.pipeline_revcomp, [self.symds_labdata])
        sandbox_reverse.execute_pipeline()
        sandbox_revcomp.execute_pipeline()

        step1 = sandbox_reverse.run.runsteps.first() # 1 step
        step2 = sandbox_revcomp.run.runsteps.get(pipelinestep__step_num=1)

        self.assertEqual(step1.reused, False)
        self.assertEqual(step2.reused, True)
        self.assertFalse(step2.has_log)
        self.assertEqual(step1.execrecord, step2.execrecord)

    def test_execute_pipeline_output_symds(self):
        """
        A Pipeline with no deleted outputs should have a SymbolicDataset as an output.
        """
        self.sandbox_complement.execute_pipeline()
        output = self.sandbox_complement.run.runoutputcables.first()
        output_symds = output.execrecord.execrecordouts.first().symbolicdataset
        self.assertEqual(output_symds is not None, True)

    def test_pipeline_trivial_cable(self):
        """
        A trivial cable should have is_trivial() = True.
        """
        outcable = self.pipeline_complement.outcables.first()
        self.assertEqual(outcable.is_trivial(), True)

    def test_trivial_cable_num_rows(self):
        """
        A trivial cable should have the same dataset all the way through.
        """
        self.sandbox_complement.execute_pipeline()

        step = self.sandbox_complement.run.runsteps.first()
        step_output_SD = step.execrecord.execrecordouts.first().symbolicdataset

        outcable = self.sandbox_complement.run.runoutputcables.first()
        outcable_input_SD = outcable.execrecord.execrecordins.first().symbolicdataset
        outcable_output_SD = outcable.execrecord.execrecordouts.first().symbolicdataset

        self.assertEqual(step_output_SD, outcable_input_SD)
        self.assertEqual(outcable_input_SD, outcable_output_SD)
        self.assertEqual(step_output_SD.num_rows(), outcable_input_SD.num_rows())
        self.assertEqual(outcable_input_SD.num_rows(), outcable_output_SD.num_rows())

    def test_execute_pipeline_num_rows(self):
        """
        A pipeline which does not change the number of rows in a dataset,
        should have the same number of rows in all SD's along the way.
        """
        self.sandbox_complement.execute_pipeline()

        incable = self.sandbox_complement.run.runsteps.first().RSICs.first()
        incable_input_SD = incable.execrecord.execrecordins.first().symbolicdataset
        incable_output_SD = incable.execrecord.execrecordins.first().symbolicdataset

        step = self.sandbox_complement.run.runsteps.first()
        step_input_SD = step.execrecord.execrecordins.first().symbolicdataset
        step_output_SD = step.execrecord.execrecordouts.first().symbolicdataset

        outcable = self.sandbox_complement.run.runoutputcables.first()
        outcable_input_SD = outcable.execrecord.execrecordins.first().symbolicdataset
        outcable_output_SD = outcable.execrecord.execrecordouts.first().symbolicdataset

        self.assertEqual(incable_input_SD.num_rows(), self.symds_labdata.num_rows())
        self.assertEqual(incable_input_SD.num_rows(), incable_output_SD.num_rows())
        self.assertEqual(incable_output_SD.num_rows(), step_input_SD.num_rows())
        self.assertEqual(step_input_SD.num_rows(), step_output_SD.num_rows())
        self.assertEqual(step_output_SD.num_rows(), outcable_input_SD.num_rows())
        self.assertEqual(outcable_input_SD.num_rows(), outcable_output_SD.num_rows())

    def test_discard_intermediate_file(self):
        """
        A Pipeline which indicates one of its intermediate outputs should not be kept,
        should not create any datasets for that output.
        """
        step = self.pipeline_revcomp_v2.steps.get(step_num=1)
        sandbox = Sandbox(self.user_alice, self.pipeline_revcomp_v2, [self.symds_labdata])
        sandbox.execute_pipeline()
        runstep = sandbox.run.runsteps.get(pipelinestep__step_num=1)
        output = runstep.execrecord.execrecordouts.first().symbolicdataset
        self.assertEqual(runstep.pipelinestep.outputs_to_retain(), [])
        self.assertEqual(output.has_data(), False)
        self.assertNotEqual(None, step)

    def test_recover_intermediate_dataset(self):
        """
        Test recovery of an intermediate dataset.
        """
        # Don't keep the intermediate or final output.
        sandbox = Sandbox(self.user_alice, self.pipeline_revcomp_v2, [self.symds_labdata])
        sandbox.execute_pipeline()
        # steps = sandbox.run.runsteps.all()
        # steps = sorted(steps, key = lambda step: step.pipelinestep.step_num)

        # This time we need the final output - that means we have to recover the intermediate
        # output.
        sandbox2 = Sandbox(self.user_alice, self.pipeline_revcomp_v3, [self.symds_labdata])
        sandbox2.execute_pipeline()


class BadRunTests(TestCase):
    """
    Tests for when things go wrong during Pipeline execution.
    """
    fixtures = ["initial_data", "initial_groups", "initial_user"]

    def setUp(self):
        tools.create_grandpa_sandbox_environment(self)

    def tearDown(self):
        tools.destroy_grandpa_sandbox_environment(self)

    def test_code_bad_execution(self):
        """
        If the user's code bombs, we should get an ExecLog with a -1 return code.
        """
        sandbox = Sandbox(self.user_grandpa, self.pipeline_faulty, [self.symds_grandpa])
        sandbox.execute_pipeline()
        runstep1 = sandbox.run.runsteps.first()
        log = runstep1.log
        interm_SD = runstep1.execrecord.execrecordouts.first().symbolicdataset

        print("RETURN CODE: {}".format(log.methodoutput.return_code))

        self.assertEqual(log.is_successful(), False)
        self.assertEqual(log.methodoutput.return_code, -1)
        self.assertEqual(log.missing_outputs(), [interm_SD])

    def test_method_fails(self):
        """Properly handle a failed method in a pipeline."""
        sandbox = Sandbox(self.user_grandpa, self.pipeline_fubar, [self.symds_grandpa])
        sandbox.execute_pipeline()
        self.assertIsNone(sandbox.run.complete_clean())
        self.assertFalse(sandbox.run.successful_execution())

        runstep1 = sandbox.run.runsteps.get(pipelinestep__step_num=1)
        self.assertIsNone(runstep1.complete_clean())
        self.assertTrue(runstep1.successful_execution())

        runstep2 = sandbox.run.runsteps.get(pipelinestep__step_num=2)
        self.assertIsNone(runstep2.complete_clean())
        self.assertFalse(runstep2.successful_execution())

        log = runstep2.log

        self.assertFalse(log.is_successful())
        self.assertEqual(log.methodoutput.return_code, 1)
        self.assertEqual(log.missing_outputs(), [runstep2.execrecord.execrecordouts.first().symbolicdataset])


class FindSDTests(TestCase):
    """
    Tests for first_generator_of_SD.
    """
    fixtures = ["initial_data", "initial_groups", "initial_user"]

    def setUp(self):
        tools.create_word_reversal_environment(self)

        self.setup_simple_pipeline()
        self.setup_twostep_pipeline()
        self.setup_nested_pipeline()

    def tearDown(self):
        tools.destroy_word_reversal_environment(self)

    def setup_nested_pipeline(self):
        # A two-step pipeline with custom cable wires at each step.
        self.pipeline_nested = tools.make_first_pipeline(
            "nested pipeline",
            "a pipeline with a sub-pipeline",
            self.user_bob)

        transforms = [self.method_noop_backwords, self.pipeline_twostep, self.method_noop_backwords]
        tools.create_linear_pipeline(self.pipeline_nested,
            transforms, "data", "unchanged data")
        cable = self.pipeline_nested.steps.get(step_num=3).cables_in.first()
        tools.make_crisscross_cable(cable)
        self.pipeline_nested.create_outputs()
        self.pipeline_nested.complete_clean()
    
    def setup_twostep_pipeline(self):
        """
        (drow,word) (word,drow) (word,drow)    (drow,word)  (drow,word)    (drow,word)
                         _____________              ______________
           [o]====<>====|o           o|=====<>=====|o            o|============[o]
                        |   reverse   |            |     noop     |
                        |_____________|            |______________|
        """
        # A two-step pipeline with custom cable wires at each step.
        self.pipeline_twostep = tools.make_first_pipeline(
            "two-step pipeline",
            "a two-step pipeline with custom cable wires at each step",
            self.user_bob)
        self.pipeline_twostep.create_input(compounddatatype=self.cdt_backwords, dataset_name="words_to_reverse",
                                           dataset_idx=1)

        methods = [self.method_reverse, self.method_noop_backwords]
        for i, _method in enumerate(methods):
            step = self.pipeline_twostep.steps.create(transformation=methods[i], step_num=i+1)
            if i == 0:
                source = self.pipeline_twostep.inputs.first()
            else:
                source = methods[i-1].outputs.first()
            cable = step.cables_in.create(source_step = i, 
                source = source,
                dest = methods[i].inputs.first())
            tools.make_crisscross_cable(cable)

        cable = self.pipeline_twostep.create_outcable(output_name = "reversed_words",
            output_idx = 1,
            source_step = 2,
            source = methods[-1].outputs.first())

        self.pipeline_twostep.create_outputs()
        self.pipeline_twostep.complete_clean()
    
    def setup_simple_pipeline(self):
        # A simple, one-step pipeline, which does nothing.
        self.pipeline_noop = tools.make_first_pipeline("simple pipeline", "a simple, one-step pipeline",
                                                       self.user_bob)
        tools.create_linear_pipeline(
            self.pipeline_noop,
            [self.method_noop],
            "lab_data", "complemented_lab_data")
        self.pipeline_noop.create_outputs()

    def test_find_symds_pipeline_input(self):
        """
        Finding a SymbolicDataset which was input to a Pipeline should return None
        as the generator, and the top-level run as the run.
        """
        sandbox = Sandbox(self.user_bob, self.pipeline_noop, [self.symds_words])
        sandbox.execute_pipeline()
        self.assertIsNone(sandbox.run.complete_clean())
        self.assertTrue(sandbox.run.successful_execution())

        run, gen = sandbox.first_generator_of_SD(self.symds_words)
        self.assertEqual(run, sandbox.run)
        self.assertEqual(gen, None)

    def test_find_symds_step_output(self):
        """
        Finding a SymbolicDataset which was output from a step, and also input
        to a cable, should return the step (and in particular, not the cable).
        """
        sandbox = Sandbox(self.user_bob, self.pipeline_noop, [self.symds_words])
        sandbox.execute_pipeline()
        self.assertIsNone(sandbox.run.complete_clean())
        self.assertTrue(sandbox.run.successful_execution())

        symds_out = sandbox.run.runsteps.first().execrecord.execrecordouts.first().symbolicdataset
        run, gen = sandbox.first_generator_of_SD(symds_out)
        self.assertEqual(run, sandbox.run)
        self.assertEqual(gen, self.pipeline_noop.steps.first())

    def test_find_symds_pipeline_input_custom_wire(self):
        """
        Finding a SymbolicDataset which was passed through a custom wire to a
        Pipeline should return the cable as the generator, and the top-level
        run as the run.
        """
        sandbox = Sandbox(self.user_bob, self.pipeline_twostep, [self.symds_backwords])
        sandbox.execute_pipeline()
        self.assertIsNone(sandbox.run.complete_clean())
        self.assertTrue(sandbox.run.successful_execution())

        runcable = sandbox.run.runsteps.first().RSICs.first()
        symds_to_find = runcable.execrecord.execrecordouts.first().symbolicdataset

        run, gen = sandbox.first_generator_of_SD(symds_to_find)
        self.assertEqual(run, sandbox.run)
        self.assertEqual(gen, runcable.PSIC)

    def test_find_symds_custom_wire(self):
        """
        Finding a SymbolicDataset which was produced by a custom wire as an 
        intermediate step should return the cable as the generator, and the
        top-level run as the run.
        """
        sandbox = Sandbox(self.user_bob, self.pipeline_twostep, [self.symds_backwords])
        sandbox.execute_pipeline()
        self.assertIsNone(sandbox.run.complete_clean())
        self.assertTrue(sandbox.run.successful_execution())

        runcable = sandbox.run.runsteps.get(pipelinestep__step_num=2).RSICs.first()
        symds_to_find = runcable.execrecord.execrecordouts.first().symbolicdataset

        run, gen = sandbox.first_generator_of_SD(symds_to_find)
        self.assertEqual(run, sandbox.run)
        self.assertEqual(gen, runcable.PSIC)

    def test_find_symds_subpipeline(self):
        """
        Find a symbolic dataset in a sub-pipeline, which is output from a step.
        """
        sandbox = Sandbox(self.user_bob, self.pipeline_nested, [self.symds_backwords])
        sandbox.execute_pipeline()
        self.assertIsNone(sandbox.run.complete_clean())
        self.assertTrue(sandbox.run.successful_execution())

        for step in sandbox.run.runsteps.all():
            if step.pipelinestep.step_num == 2:
                subrun = step.child_run
                runstep = subrun.runsteps.first()
                outrecord = runstep.execrecord.execrecordouts.first()
                symds_to_find = outrecord.symbolicdataset
                break

        run, gen = sandbox.first_generator_of_SD(symds_to_find)
        self.assertEqual(run, subrun)
        self.assertEqual(gen, runstep.pipelinestep)

    def test_find_symds_subpipeline_input(self):
        """
        Find a symbolic dataset in a sub-pipeline, which is input to the sub-pipeline
        on a custom cable.
        """
        sandbox = Sandbox(self.user_bob, self.pipeline_nested, [self.symds_backwords])
        sandbox.execute_pipeline()
        self.assertIsNone(sandbox.run.complete_clean())
        self.assertTrue(sandbox.run.successful_execution())

        for step in sandbox.run.runsteps.all():
            if step.pipelinestep.step_num == 2:
                subrun = step.child_run
                runstep = subrun.runsteps.first()
                cable = runstep.RSICs.first()
                symds_to_find = runstep.execrecord.execrecordins.first().symbolicdataset

        run, gen = sandbox.first_generator_of_SD(symds_to_find)
        self.assertEqual(run, subrun)
        self.assertEqual(gen, cable.PSIC)


class RawTests(SandboxRMTestCase):

    def setUp(self):
        super(RawTests, self).setUp()

        self.pipeline_raw = tools.make_first_pipeline(
            "raw noop", "a pipeline to do nothing to raw data",
            self.user_bob)
        tools.create_linear_pipeline(self.pipeline_raw, [self.method_noop_raw], "raw_in", "raw_out")
        self.pipeline_raw.create_outputs()

        self.symds_raw = SymbolicDataset.create_SD(
            "/usr/share/dict/words", user=self.user_bob,
            cdt=None, make_dataset=True, name="raw", description="some raw data")

    def test_execute_pipeline_raw(self):
        """Execute a raw Pipeline."""
        sandbox = Sandbox(self.user_bob, self.pipeline_raw, [self.symds_raw])
        sandbox.execute_pipeline()

    def test_execute_pipeline_raw_twice(self):
        """Execute a raw Pipeline and reuse an ExecRecord."""
        Sandbox(self.user_bob, self.pipeline_raw, [self.symds_raw]).execute_pipeline()
        Sandbox(self.user_bob, self.pipeline_raw, [self.symds_raw]).execute_pipeline()

    def tearDown(self):
        super(RawTests, self).tearDown()

