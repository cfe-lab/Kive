import sys

from django.contrib.auth.models import User
from django.core.management import call_command
from django.core.management.base import BaseCommand

from metadata.models import everyone_group
import os
import sandbox
import sandbox.testing_utils as tools
import json
from archive.tests import create_archive_test_environment
from librarian.tests import create_removal_test_environment


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

        run_sandbox = sandbox.execute.Sandbox(self.user_bob,
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

        run_sandbox = sandbox.execute.Sandbox(self.user_bob, p_top, [self.symds_words],
                                              groups_allowed=[everyone_group()])
        run_sandbox.execute_pipeline()


class RemovalTestEnvironmentBuilder(FixtureBuilder):
    def get_name(self):
        return 'removal.json'

    def build(self):
        create_removal_test_environment()


class Command(BaseCommand):
    help = "Update test fixtures by running scripts and dumping test data."

    def handle(self, *args, **options):
        ArchiveTestEnvironmentBuilder().run()
        DeepNestedRunBuilder().run()
        SimpleRunBuilder().run()
        RemovalTestEnvironmentBuilder().run()
        
        self.stdout.write('Done.')
