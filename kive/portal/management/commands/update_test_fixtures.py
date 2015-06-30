from django.contrib.auth.models import User
from django.core.management import call_command
from django.core.management.base import BaseCommand

import sys
import shutil
import os
import json

from metadata.models import CompoundDatatype, everyone_group
from sandbox.execute import Sandbox
import sandbox.testing_utils as tools
from archive.tests import create_archive_test_environment
from librarian.tests import create_removal_test_environment
import sandbox.tests
import pipeline.models
import kive.settings
import method.models
import archive.models
import datachecking.models
import portal.models


class FixtureBuilder(object):
    def get_name(self):
        """ Return the fixture file's name. """
        raise NotImplementedError()
    
    def build(self):
        """ Build all the records that should be in the fixture. """
        raise NotImplementedError()
    
    def run(self):
        call_command('reset')
        
        before_filename = 'test_fixture_before.json'
        self.dump_all_data(before_filename)
        
        self.build()

        after_filename = 'test_fixture_after.json'
        self.dump_all_data(after_filename)
        
        with open(before_filename, 'rU') as jsonfile:
            before_objects = json.load(jsonfile)
        
        with open(after_filename, 'rU') as jsonfile:
            after_objects = json.load(jsonfile)

        dump_objects = []
        before_index = 0
        for after_object in after_objects:
            before_object = (before_index < len(before_objects)
                             and before_objects[before_index])
            if after_object == before_object:
                before_index += 1
            else:
                dump_objects.append(after_object)
        
        dump_filename = os.path.join('portal', 'fixtures', self.get_name())
        with open(dump_filename, 'w') as dump_file:
            json.dump(dump_objects, dump_file, indent=4)
            
        os.remove(before_filename)
        os.remove(after_filename)

        # If any files were created at this time, we have to stash them in the appropriate place.
        targets = [
            method.models.CodeResourceRevision.UPLOAD_DIR,
            archive.models.Dataset.UPLOAD_DIR,
            archive.models.MethodOutput.UPLOAD_DIR,
            datachecking.models.VerificationLog.UPLOAD_DIR,
            portal.models.StagedFile.UPLOAD_DIR,
            kive.settings.SANDBOX_PATH
        ]

        for target in targets:
            target_path = os.path.join(kive.settings.MEDIA_ROOT, target)
            fixture_files_path = os.path.join("FixtureFiles", self.get_name(), target)

            # Out with the old...
            if os.path.isdir(fixture_files_path):
                shutil.rmtree(fixture_files_path)

            # ... in with the new.
            if os.path.isdir(target_path):
                shutil.copytree(target_path, fixture_files_path)
    
    def dump_all_data(self, filename):
        with open(filename, "w") as fixture_file:
            old_stdout = sys.stdout
            sys.stdout = fixture_file
            try:
                call_command("dumpdata", indent=4)
            finally:
                sys.stdout = old_stdout


class ArchiveTestEnvironmentBuilder(FixtureBuilder):
    def get_name(self):
        return 'archive_test_environment.json'
    
    def build(self):
        create_archive_test_environment(self)


class SimpleRunBuilder(FixtureBuilder):
    def get_name(self):
        return 'simple_run.json'
    
    def build(self):
        create_archive_test_environment(self)
        user = User.objects.get(username='john')
        # Everything in this pipeline will be a no-op, so all can be linked together
        # without remorse.
        p_basic = tools.make_first_pipeline("P_basic", "Innermost pipeline", user)
        tools.create_linear_pipeline(p_basic, [self.method_noop, self.method_noop], "basic_in", "basic_out")
        p_basic.family.grant_everyone_access()
        p_basic.grant_everyone_access()
        p_basic.create_outputs()
        p_basic.save()

        # Set up a dataset with words in it called self.symds_words.
        tools.make_words_symDS(self)

        run_sandbox = Sandbox(self.user_bob,
                              p_basic,
                              [self.symds_words],
                              groups_allowed=[everyone_group()])
        run_sandbox.execute_pipeline()


class DeepNestedRunBuilder(FixtureBuilder):
    def get_name(self):
        return 'deep_nested_run.json'
    
    def build(self):
        create_archive_test_environment(self)
        user = User.objects.get(username='john')
        # Everything in this pipeline will be a no-op, so all can be linked together
        # without remorse.
        p_basic = tools.make_first_pipeline("p_basic", "innermost pipeline", user)
        tools.create_linear_pipeline(p_basic, [self.method_noop, self.method_noop], "basic_in", "basic_out")
        p_basic.family.grant_everyone_access()
        p_basic.grant_everyone_access()
        p_basic.create_outputs()
        p_basic.save()

        p_sub = tools.make_first_pipeline("p_sub", "second-level pipeline", user)
        tools.create_linear_pipeline(p_sub, [p_basic, p_basic], "sub_in", "sub_out")
        p_sub.family.grant_everyone_access()
        p_sub.grant_everyone_access()
        p_sub.create_outputs()
        p_sub.save()

        p_top = tools.make_first_pipeline("p_top", "top-level pipeline", user)
        tools.create_linear_pipeline(p_top, [p_sub, p_sub, p_sub], "top_in", "top_out")
        p_top.family.grant_everyone_access()
        p_top.grant_everyone_access()
        p_top.create_outputs()
        p_top.save()

        # Set up a dataset with words in it called self.symds_words.
        tools.make_words_symDS(self)

        run_sandbox = Sandbox(self.user_bob, p_top, [self.symds_words],
                              groups_allowed=[everyone_group()])
        run_sandbox.execute_pipeline()


class RemovalTestEnvironmentBuilder(FixtureBuilder):
    def get_name(self):
        return 'removal.json'

    def build(self):
        create_removal_test_environment()


class RunApiTestsEnvironmentBuilder(FixtureBuilder):
    def get_name(self):
        return "run_api_tests.json"

    def build(self):
        sandbox.tests.execute_tests_environment_setup(self)

        # Define pipeline containing two steps with the same method + pipeline input
        self.pX_2 = pipeline.models.Pipeline(family=self.pf, revision_name="pX_revision_2",
                                             revision_desc="X2", user=self.myUser)
        self.pX_2.save()
        self.X1_in = self.pX_2.create_input(compounddatatype=self.pX_in_cdt, dataset_name="pX_in", dataset_idx=1)
        self.step_X1 = self.pX_2.steps.create(transformation=self.mA, step_num=1)
        self.step_X2 = self.pX_2.steps.create(transformation=self.mA, step_num=2)

        # Use the SAME custom cable from pipeline input to steps 1 and 2
        self.cable_X1_A1 = self.step_X1.cables_in.create(dest=self.mA_in, source_step=0, source=self.X1_in)
        self.wire1 = self.cable_X1_A1.custom_wires.create(source_pin=self.pX_in_cdtm_2, dest_pin=self.mA_in_cdtm_2)
        self.wire2 = self.cable_X1_A1.custom_wires.create(source_pin=self.pX_in_cdtm_3, dest_pin=self.mA_in_cdtm_1)
        self.cable_X1_A2 = self.step_X2.cables_in.create(dest=self.mA_in, source_step=0, source=self.X1_in)
        self.wire3 = self.cable_X1_A2.custom_wires.create(source_pin=self.pX_in_cdtm_2, dest_pin=self.mA_in_cdtm_2)
        self.wire4 = self.cable_X1_A2.custom_wires.create(source_pin=self.pX_in_cdtm_3, dest_pin=self.mA_in_cdtm_1)

        # POCs: one is trivial, the second uses custom outwires
        # Note: by default, create_outcables assumes the POC has the CDT of the source (IE, this is a TRIVIAL cable)
        self.outcable_1 = self.pX_2.create_outcable(output_name="pX_out_1",output_idx=1,source_step=1,source=self.mA_out)
        self.outcable_2 = self.pX_2.create_outcable(output_name="pX_out_2",output_idx=2,source_step=2,source=self.mA_out)

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
        self.pX_2.create_outputs()

        # Run this pipeline.
        sbox = Sandbox(self.myUser, self.pX_2, [self.symDS])
        sbox.execute_pipeline()


class RunComponentTooManyChecksEnvironmentBuilder(FixtureBuilder):
    def get_name(self):
        return "run_component_too_many_checks.json"

    def build(self):
        tools.create_word_reversal_environment(self)

        # Set up and run a Pipeline that throws away its intermediate data.
        self.two_step_pl = tools.make_first_pipeline("Two-step pipeline",
                                                     "Toy pipeline for testing data check cleaning of RunSteps.",
                                                     self.user_bob)
        tools.create_linear_pipeline(self.two_step_pl, [self.method_noop_wordbacks, self.method_noop_wordbacks],
                                     "data", "samedata")
        first_step = self.two_step_pl.steps.get(step_num=1)
        first_step.add_deletion(self.method_noop_wordbacks.outputs.first())
        first_step.save()

        self.two_step_sdbx = Sandbox(self.user_bob, self.two_step_pl, [self.symds_wordbacks],
                                     groups_allowed=[everyone_group()])
        self.two_step_sdbx.execute_pipeline()

        # The second one's second step will have to recover its first step.  (Its input cable is trivial
        # and is able to reuse the input cable from the first Pipeline's second step.)
        self.following_pl = tools.make_first_pipeline(
            "Pipeline that will follow the first",
            "Toy pipeline that will need to recover its first step when following the above.",
            self.user_bob
        )
        tools.create_linear_pipeline(self.following_pl, [self.method_noop_wordbacks, self.method_reverse],
                                     "data", "reversed_data")
        first_step = self.following_pl.steps.get(step_num=1)
        first_step.add_deletion(self.method_noop_wordbacks.outputs.first())
        first_step.save()

        self.following_sdbx = Sandbox(self.user_bob, self.following_pl, [self.symds_wordbacks],
                                      groups_allowed=[everyone_group()])
        self.following_sdbx.execute_pipeline()
        second_step = self.following_sdbx.run.runsteps.get(pipelinestep__step_num=2)
        assert(second_step.invoked_logs.count() == 2)

        # FIXME are there other files that aren't properly being removed?
        if hasattr(self, "words_datafile"):
            os.remove(self.words_datafile.name)


class RunPipelinesRecoveringReusedStepEnvironmentBuilder(FixtureBuilder):
    """
    Setting up and running two pipelines, where the second one reuses and then recovers a step from the first.
    """

    def get_name(self):
        return "run_pipelines_recovering_reused_step.json"

    def build(self):
        create_archive_test_environment(self)

        p_one = tools.make_first_pipeline("p_one", "two no-ops", self.myUser)
        p_one.family.grant_everyone_access()
        p_one.grant_everyone_access()
        tools.create_linear_pipeline(p_one, [self.method_noop, self.method_noop], "p_one_in", "p_one_out")
        p_one.create_outputs()
        p_one.save()
        # Mark the output of step 1 as not retained.
        p_one.steps.get(step_num=1).add_deletion(self.method_noop.outputs.first())

        p_two = tools.make_first_pipeline("p_two", "one no-op then one trivial", self.myUser)
        p_two.family.grant_everyone_access()
        p_two.grant_everyone_access()
        tools.create_linear_pipeline(p_two, [self.method_noop, self.method_trivial], "p_two_in", "p_two_out")
        p_two.create_outputs()
        p_two.save()
        # We also delete the output of step 1 so that it reuses the existing ER we'll have
        # create for p_one.
        p_two.steps.get(step_num=1).add_deletion(self.method_noop.outputs.first())

        # Set up a words dataset.
        tools.make_words_symDS(self)

        sandbox_one = Sandbox(self.user_bob, p_one, [self.symds_words],
                              groups_allowed=[everyone_group()])
        sandbox_one.execute_pipeline()

        sandbox_two = Sandbox(self.user_bob, p_two, [self.symds_words],
                              groups_allowed=[everyone_group()])
        sandbox_two.execute_pipeline()


class ExecuteResultTestsRMEnvironmentBuilder(FixtureBuilder):
    """
    Execution tests using tools.create_sequence_manipulation_environment.
    """

    def get_name(self):
        return "execute_result_tests_rm.json"

    def build(self):
        tools.create_sequence_manipulation_environment(self)

        # Many tests use this run.
        self.sandbox_complement.execute_pipeline()

        # An identically-specified second run that reuses the stuff from the first.
        sandbox2 = Sandbox(self.user_alice, self.pipeline_complement, [self.symds_labdata])
        sandbox2.execute_pipeline()

        # A couple more runs, used by another test.
        sandbox_reverse = Sandbox(self.user_alice, self.pipeline_reverse, [self.symds_labdata])
        sandbox_reverse.execute_pipeline()
        self.sandbox_revcomp.execute_pipeline()

        # This is usually done in tools.destroy_sequence_manipulation_environment.
        if os.path.exists(self.datafile.name):
            os.remove(self.datafile.name)


class ExecuteDiscardedIntermediateTestsRMEnvironmentBuilder(FixtureBuilder):
    """
    Creates a fixture used by execution tests involving discarded intermediate data.
    """

    def get_name(self):
        return "execute_discarded_intermediate_tests_rm.json"

    def build(self):
        tools.create_sequence_manipulation_environment(self)

        # This run will discard intermediate data.
        sandbox = Sandbox(self.user_alice, self.pipeline_revcomp_v2, [self.symds_labdata])
        sandbox.execute_pipeline()

        # This is usually done in tools.destroy_sequence_manipulation_environment.
        if os.path.exists(self.datafile.name):
            os.remove(self.datafile.name)


class Command(BaseCommand):
    help = "Update test fixtures by running scripts and dumping test data."

    def handle(self, *args, **options):
        ArchiveTestEnvironmentBuilder().run()
        DeepNestedRunBuilder().run()
        SimpleRunBuilder().run()
        RemovalTestEnvironmentBuilder().run()
        RunApiTestsEnvironmentBuilder().run()
        RunComponentTooManyChecksEnvironmentBuilder().run()
        RunPipelinesRecoveringReusedStepEnvironmentBuilder().run()
        ExecuteResultTestsRMEnvironmentBuilder().run()
        ExecuteDiscardedIntermediateTestsRMEnvironmentBuilder().run()
        
        self.stdout.write('Done.')
