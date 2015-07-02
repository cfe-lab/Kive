from django.test import TestCase, TransactionTestCase
from django.utils import timezone
from django.contrib.auth.models import User

import unittest
import tempfile
import shutil
import os.path

from librarian.models import SymbolicDataset
from sandbox.execute import Sandbox
import sandbox.testing_utils as tools
import kive.settings
from pipeline.models import Pipeline, PipelineFamily
from metadata.tests import clean_up_all_files
from kive.tests import install_fixture_files, restore_production_files
from method.models import Method
import file_access_utils


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


class ExecuteResultTestsRM(TestCase):
    """
    Tests on the results of executing Pipelines.
    """
    fixtures = ["execute_result_tests_rm.json"]

    def setUp(self):
        install_fixture_files("execute_result_tests_rm.json")
        self.method_complement = Method.objects.get(
            family__name="DNA complement",
            revision_name="v1"
        )

        self.pipeline_complement = Pipeline.objects.get(
            family__name="DNA complement",
            revision_name="v1"
        )
        self.pipeline_reverse = Pipeline.objects.get(
            family__name="DNA reverse",
            revision_name="v1"
        )
        self.pipeline_revcomp = Pipeline.objects.get(
            family__name="DNA revcomp",
            revision_name="v1"
        )

        self.user_alice = User.objects.get(username="alice")

        self.comp_run = self.pipeline_complement.pipeline_instances.order_by("start_time").first()
        self.comp_run_2 = self.pipeline_complement.pipeline_instances.order_by("start_time").last()
        self.reverse_run = self.pipeline_reverse.pipeline_instances.first()
        self.revcomp_run = self.pipeline_revcomp.pipeline_instances.first()

        self.symds_labdata = SymbolicDataset.objects.get(
            dataset__name="lab data",
            user=self.user_alice
        )

        # Tracking down CDTs is a pain....
        self.cdt_record = self.method_complement.inputs.first().structure.compounddatatype

    def tearDown(self):
        clean_up_all_files()
        restore_production_files()

    def test_execute_pipeline_run(self):
        """
        Check the coherence of Runs created when a pipeline is executed the first time.
        """
        run = self.comp_run
        self.assertEqual(run.user, self.user_alice)
        self.assertEqual(run.start_time < timezone.now(), True)
        self.assertEqual(run.is_complete(), True)
        self.assertEqual(run.parent_runstep, None)
        self.assertEqual(run.complete_clean(), None)

    def test_execute_pipeline_runstep(self):
        """
        Check the coherence of a RunStep created when a Pipeline is executed the first time.
        """
        run = self.comp_run
        # sandbox_complement has only one step, so this is OK.
        runstep = run.runsteps.first()

        self.assertEqual(runstep.run, run)
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
        run = self.comp_run
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
        # Figure out the MD5 of the output file created when the complement method
        # is run on Alice's data to check against the result of the run.
        tmpdir = tempfile.mkdtemp(dir=file_access_utils.sandbox_base_path())
        file_access_utils.configure_sandbox_permissions(tmpdir)
        outfile = os.path.join(tmpdir, "output")
        complement_popen = self.method_complement.invoke_code(
            tmpdir,
            [self.symds_labdata.dataset.dataset_file.file.name],
            [outfile]
        )
        complement_popen.wait()
        labdata_compd_md5 = file_access_utils.compute_md5(open(outfile))
        shutil.rmtree(tmpdir)

        run = self.comp_run
        runstep = run.runsteps.first()
        execrecord = runstep.execrecord
        symds = execrecord.execrecordouts.first().symbolicdataset
        ds = runstep.outputs.first()

        self.assertEqual(symds.MD5_checksum, labdata_compd_md5)
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
        run = self.comp_run

        pipelinestep = self.pipeline_complement.steps.first() # 1 step
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
        run = self.comp_run
        runstep = run.runsteps.first()
        execlog = runstep.log
        execrecord = runstep.execrecord
        outputs = self.method_complement.outputs.all()

        self.assertEqual(execrecord.generator, execlog)
        self.assertEqual(execrecord.complete_clean(), None)
        self.assertEqual(execrecord.general_transf(), runstep.pipelinestep.transformation.method)
        self.assertEqual(execrecord.provides_outputs(outputs), True)
        self.assertEqual(execrecord.outputs_OK(), True)

    def test_execute_pipeline_reuse(self):
        """
        An identical pipeline, run in a different sandbox, should reuse an ExecRecord
        and not create an ExecLog.
        """
        step1 = self.comp_run.runsteps.first()
        step2 = self.comp_run_2.runsteps.first()

        self.assertEqual(step1.reused, False)
        self.assertEqual(step2.reused, True)
        self.assertFalse(step2.has_log)
        self.assertEqual(step1.execrecord, step2.execrecord)

    def test_execute_pipeline_fill_in_ER(self):
        """
        Running an identical Pipeline where we did not keep the data around the first time
        should fill in an existing ExecRecord, but also create a new ExecLog.
        """
        step1 = self.comp_run.runsteps.first()
        step2 = self.comp_run_2.runsteps.first()

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
        step1 = self.reverse_run.runsteps.first()  # 1 step
        step2 = self.revcomp_run.runsteps.get(pipelinestep__step_num=1)

        self.assertEqual(step1.reused, False)
        self.assertEqual(step2.reused, True)
        self.assertFalse(step2.has_log)
        self.assertEqual(step1.execrecord, step2.execrecord)

    def test_execute_pipeline_output_symds(self):
        """
        A Pipeline with no deleted outputs should have a SymbolicDataset as an output.
        """
        output = self.comp_run.runoutputcables.first()
        output_symds = output.execrecord.execrecordouts.first().symbolicdataset
        self.assertEqual(output_symds is not None, True)

    def test_trivial_cable_num_rows(self):
        """
        A trivial cable should have the same dataset all the way through.
        """
        step = self.comp_run.runsteps.first()
        step_output_SD = step.execrecord.execrecordouts.first().symbolicdataset

        outcable = self.comp_run.runoutputcables.first()
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
        incable = self.comp_run.runsteps.first().RSICs.first()
        incable_input_SD = incable.execrecord.execrecordins.first().symbolicdataset
        incable_output_SD = incable.execrecord.execrecordins.first().symbolicdataset

        step = self.comp_run.runsteps.first()
        step_input_SD = step.execrecord.execrecordins.first().symbolicdataset
        step_output_SD = step.execrecord.execrecordouts.first().symbolicdataset

        outcable = self.comp_run.runoutputcables.first()
        outcable_input_SD = outcable.execrecord.execrecordins.first().symbolicdataset
        outcable_output_SD = outcable.execrecord.execrecordouts.first().symbolicdataset

        self.assertEqual(incable_input_SD.num_rows(), self.symds_labdata.num_rows())
        self.assertEqual(incable_input_SD.num_rows(), incable_output_SD.num_rows())
        self.assertEqual(incable_output_SD.num_rows(), step_input_SD.num_rows())
        self.assertEqual(step_input_SD.num_rows(), step_output_SD.num_rows())
        self.assertEqual(step_output_SD.num_rows(), outcable_input_SD.num_rows())
        self.assertEqual(outcable_input_SD.num_rows(), outcable_output_SD.num_rows())


class ExecuteDiscardedIntermediateTests(TestCase):
    fixtures = ["execute_discarded_intermediate_tests_rm.json"]

    def setUp(self):
        install_fixture_files("execute_discarded_intermediate_tests_rm.json")
        self.revcomp_pf = PipelineFamily.objects.get(name="DNA revcomp")
        self.pipeline_revcomp_v2 = self.revcomp_pf.members.get(revision_name="2")
        self.pipeline_revcomp_v3 = self.revcomp_pf.members.get(revision_name="3")

        self.user_alice = User.objects.get(username="alice")

        self.revcomp_v2_run = self.pipeline_revcomp_v2.pipeline_instances.first()  # only one exists

        self.symds_labdata = SymbolicDataset.objects.get(
            dataset__name="lab data",
            user=self.user_alice
        )

    def tearDown(self):
        clean_up_all_files()
        restore_production_files()

    def test_discard_intermediate_file(self):
        """
        A Pipeline which indicates one of its intermediate outputs should not be kept,
        should not create any datasets for that output.
        """
        runstep = self.revcomp_v2_run.runsteps.get(pipelinestep__step_num=1)
        output = runstep.execrecord.execrecordouts.first().symbolicdataset
        step = self.pipeline_revcomp_v2.steps.get(step_num=1)
        self.assertEqual(runstep.pipelinestep.outputs_to_retain(), [])
        self.assertEqual(output.has_data(), False)
        self.assertNotEqual(None, step)

    def test_recover_intermediate_dataset(self):
        """
        Test recovery of an intermediate dataset.
        """
        # In the fixture, we already ran self.pipeline_revcomp_v2, which discards the intermediate
        # output.  We now run v3, which will recover it.
        sandbox = Sandbox(self.user_alice, self.pipeline_revcomp_v3, [self.symds_labdata])
        sandbox.execute_pipeline()


class ExecuteTestsRM(TestCase):
    """
    Tests of actually executing Pipelines, and of the Sandboxes.
    """
    def setUp(self):
        tools.create_sequence_manipulation_environment(self)

    def tearDown(self):
        tools.destroy_sequence_manipulation_environment(self)

    def test_execute_pipeline_twice(self):
        """
        You can't execute a pipeline twice in the same Sandbox.
        """
        run1 = self.sandbox_complement.execute_pipeline()
        run2 = self.sandbox_complement.execute_pipeline()
        self.assertEqual(run1 is run2, True)


class BadRunTests(TestCase):
    """
    Tests for when things go wrong during Pipeline execution.
    """
    fixtures = ["initial_data", "initial_groups", "initial_user"]

    def setUp(self):
        tools.create_grandpa_sandbox_environment(self)

    def tearDown(self):
        tools.destroy_grandpa_sandbox_environment(self)

    @unittest.skipIf(
        kive.settings.KIVE_SANDBOX_WORKER_ACCOUNT,
        "OSError will not be thrown when using SSH to the Kive sandbox worker account"
    )
    def test_code_bad_execution(self):
        """
        If the user's code causes subprocess to throw an OSError, the ExecLog should have a -1 return code.

        Note that this doesn't occur if using ssh to an unprivileged account for execution.
        """
        sandbox = Sandbox(self.user_grandpa, self.pipeline_faulty, [self.symds_grandpa])
        sandbox.execute_pipeline()
        runstep1 = sandbox.run.runsteps.first()
        log = runstep1.log
        interm_SD = runstep1.execrecord.execrecordouts.first().symbolicdataset

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
            transforms, "data", "unchanged_data")
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

    def test_find_symds_pipeline_input_and_step_output(self):
        """
        Finding a SymbolicDataset which was input to a Pipeline should return None
        as the generator, and the top-level run as the run.

        Finding a SymbolicDataset which was output from a step, and also input
        to a cable, should return the step (and in particular, not the cable).
        """
        sandbox = Sandbox(self.user_bob, self.pipeline_noop, [self.symds_words])
        sandbox.execute_pipeline()
        self.assertIsNone(sandbox.run.complete_clean())
        self.assertTrue(sandbox.run.successful_execution())

        run, gen = sandbox.first_generator_of_SD(self.symds_words)
        self.assertEqual(run, sandbox.run)
        self.assertEqual(gen, None)

        symds_out_intermediate = sandbox.run.runsteps.first().execrecord.execrecordouts.first().symbolicdataset
        run_2, gen_2 = sandbox.first_generator_of_SD(symds_out_intermediate)
        self.assertEqual(run_2, sandbox.run)
        self.assertEqual(gen_2, self.pipeline_noop.steps.first())

    def test_find_symds_pipeline_input_and_intermediate_custom_wire(self):
        """
        Finding a SymbolicDataset which was passed through a custom wire to a
        Pipeline should return the cable as the generator, and the top-level
        run as the run.

        Finding a SymbolicDataset which was produced by a custom wire as an
        intermediate step should return the cable as the generator, and the
        top-level run as the run.
        """
        sandbox = Sandbox(self.user_bob, self.pipeline_twostep, [self.symds_backwords])
        sandbox.execute_pipeline()
        self.assertIsNone(sandbox.run.complete_clean())
        self.assertTrue(sandbox.run.successful_execution())

        runcable = sandbox.run.runsteps.get(pipelinestep__step_num=1).RSICs.first()
        symds_to_find = runcable.execrecord.execrecordouts.first().symbolicdataset

        run, gen = sandbox.first_generator_of_SD(symds_to_find)
        self.assertEqual(run, sandbox.run)
        self.assertEqual(gen, runcable.PSIC)

        # Testing on an intermediate SymbolicDataset.
        runcable_2 = sandbox.run.runsteps.get(pipelinestep__step_num=2).RSICs.first()
        symds_to_find_2 = runcable_2.execrecord.execrecordouts.first().symbolicdataset

        run_2, gen_2 = sandbox.first_generator_of_SD(symds_to_find_2)
        self.assertEqual(run_2, sandbox.run)
        self.assertEqual(gen_2, runcable_2.PSIC)

    def test_find_symds_subpipeline_input_and_intermediate(self):
        """
        Find a symbolic dataset in a sub-pipeline, which is output from a step.

        Find a symbolic dataset in a sub-pipeline, which is input to the sub-pipeline
        on a custom cable.
        """
        sandbox = Sandbox(self.user_bob, self.pipeline_nested, [self.symds_backwords])
        sandbox.execute_pipeline()
        self.assertIsNone(sandbox.run.complete_clean())
        self.assertTrue(sandbox.run.successful_execution())

        subpipeline_step = sandbox.run.runsteps.get(pipelinestep__step_num=2)
        subrun = subpipeline_step.child_run
        runstep = subrun.runsteps.first()
        outrecord = runstep.execrecord.execrecordouts.first()
        symds_to_find = outrecord.symbolicdataset

        run, gen = sandbox.first_generator_of_SD(symds_to_find)
        self.assertEqual(run, subrun)
        self.assertEqual(gen, runstep.pipelinestep)

        cable = runstep.RSICs.first()
        symds_to_find_2 = runstep.execrecord.execrecordins.first().symbolicdataset

        run_2, gen_2 = sandbox.first_generator_of_SD(symds_to_find_2)
        self.assertEqual(run_2, subrun)
        self.assertEqual(gen_2, cable.PSIC)


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

