from datetime import datetime
import json
import os
import shutil
import sys

from django.contrib.auth.models import User
from django.core.files.base import ContentFile
from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils.dateparse import parse_datetime
from django.utils.timezone import utc
from django.conf import settings

import archive.models
import datachecking.models
from file_access_utils import compute_md5
import kive.testing_utils as tools
from librarian.models import Dataset
from metadata.models import CompoundDatatype, everyone_group
import method.models
from method.models import CodeResource, MethodFamily, Method,\
    CodeResourceRevision
import pipeline.models
from pipeline.models import PipelineFamily
import portal.models
import sandbox.tests
from fleet.workers import Manager


class FixtureBuilder(object):
    def get_name(self):
        """ Return the fixture file's name. """
        raise NotImplementedError()

    def build(self):
        """ Build all the records that should be in the fixture. """
        raise NotImplementedError()

    def run(self):
        print "--------"
        print self.get_name()
        print "--------"
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
            before_object = (before_index < len(before_objects) and
                             before_objects[before_index])
            if after_object == before_object:
                before_index += 1
            else:
                dump_objects.append(after_object)

        self.replace_timestamps(dump_objects)
        self.rename_dataset_files(os.path.join(settings.MEDIA_ROOT,
                                               Dataset.UPLOAD_DIR),
                                  dump_objects)

        dump_filename = os.path.join('portal', 'fixtures', self.get_name())
        with open(dump_filename, 'w') as dump_file:
            json.dump(dump_objects, dump_file, indent=4, sort_keys=True)

        os.remove(before_filename)
        os.remove(after_filename)

        # If any files were created at this time, we have to stash them in the appropriate place.
        targets = [
            method.models.CodeResourceRevision.UPLOAD_DIR,
            Dataset.UPLOAD_DIR,
            archive.models.MethodOutput.UPLOAD_DIR,
            datachecking.models.VerificationLog.UPLOAD_DIR,
            portal.models.StagedFile.UPLOAD_DIR,
            settings.SANDBOX_PATH
        ]

        for target in targets:
            target_path = os.path.join(settings.MEDIA_ROOT, target)
            fixture_name, _extension = os.path.splitext(self.get_name())
            fixture_files_path = os.path.join("FixtureFiles", fixture_name, target)

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

    def rename_dataset_files(self, dataset_path, dump_objects):
        source_root, source_folder = os.path.split(dataset_path)
        fixtures_path = os.path.join(dataset_path, 'fixtures')
        if not os.path.isdir(fixtures_path):
            os.makedirs(fixtures_path)
        for dump_object in dump_objects:
            if dump_object['model'] == 'archive.dataset':
                file_path = dump_object['fields']['dataset_file']
                source_path = os.path.join(source_root, file_path)
                file_name = os.path.basename(file_path)
                target_path = os.path.join(fixtures_path, file_name)
                os.rename(source_path, target_path)
                new_file_path = os.path.join(source_folder, 'fixtures', file_name)
                dump_object['fields']['dataset_file'] = new_file_path

    def replace_timestamps(self, dump_objects):
        date_map = {}  # {old_date: new_date}
        field_names = set()
        for dump_object in dump_objects:
            for field, value in dump_object['fields'].iteritems():
                if value is not None and (field.endswith('time') or
                                          field.startswith('date') or
                                          field.endswith('DateTime') or
                                          field == 'last_login'):

                    field_names.add(field)
                    date_map[value] = None
        old_dates = date_map.keys()
        old_dates.sort()
        offset = None
        for old_date in old_dates:
            old_datetime = parse_datetime(old_date)
            if offset is None:
                offset = datetime(2000, 1, 1, tzinfo=utc) - old_datetime
            rounded = (old_datetime + offset).replace(microsecond=0, tzinfo=None)
            date_map[old_date] = rounded.isoformat() + 'Z'

        for dump_object in dump_objects:
            for field, value in dump_object['fields'].iteritems():
                if value is not None and field in field_names:
                    dump_object['fields'][field] = date_map[value]


class EMSandboxTestEnvironmentBuilder(FixtureBuilder):
    def get_name(self):
        return 'em_sandbox_test_environment.json'

    def build(self):
        tools.create_eric_martin_test_environment(self)
        tools.create_sandbox_testing_tools_environment(self)

        pE_run = self.pE.pipeline_instances.create(user=self.myUser, name='pE_run')
        pE_run.grant_everyone_access()
        pE_run.inputs.create(dataset=self.triplet_dataset, index=1)
        pE_run.inputs.create(dataset=self.singlet_dataset, index=2)
        pE_run.inputs.create(dataset=self.raw_dataset, index=3)


class ArchiveTestEnvironmentBuilder(FixtureBuilder):
    def get_name(self):
        return 'archive_test_environment.json'

    def build(self):
        tools.create_archive_test_environment(self)


class ArchiveNoRunsTestEnvironmentBuilder(FixtureBuilder):
    def get_name(self):
        return 'archive_no_runs_test_environment.json'

    def build(self):
        tools.create_archive_no_runs_test_environment(self)


class SimpleRunBuilder(FixtureBuilder):
    def get_name(self):
        return 'simple_run.json'

    def build(self):
        tools.create_eric_martin_test_environment(self)
        tools.create_sandbox_testing_tools_environment(self)

        user = User.objects.get(username='john')
        # Everything in this pipeline will be a no-op, so all can be linked together
        # without remorse.
        p_basic = tools.make_first_pipeline("P_basic", "Innermost pipeline", user)
        tools.create_linear_pipeline(p_basic, [self.method_noop, self.method_noop], "basic_in", "basic_out")
        p_basic.family.grant_everyone_access()
        p_basic.grant_everyone_access()
        p_basic.create_outputs()
        p_basic.save()

        # Set up a dataset with words in it called self.dataset_words.
        tools.make_words_dataset(self)

        Manager.execute_pipeline(self.user_bob, p_basic, [self.dataset_words], groups_allowed=[everyone_group()])


class DeepNestedRunBuilder(FixtureBuilder):
    def get_name(self):
        return 'deep_nested_run.json'

    def build(self):
        tools.create_eric_martin_test_environment(self)
        tools.create_sandbox_testing_tools_environment(self)
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

        # Set up a dataset with words in it called self.dataset_words.
        tools.make_words_dataset(self)

        Manager.execute_pipeline(self.user_bob, p_top, [self.dataset_words], groups_allowed=[everyone_group()])


class RestoreReusableDatasetBuilder(FixtureBuilder):
    def get_name(self):
        return 'restore_reusable_dataset.json'

    def build(self):
        user = User.objects.first()
        pipeline1, _pipeline2 = self.create_pipelines(user)

        DATASET_CONTENT = """\
x,y
0,1
2,3
4,5
6,7
8,9
"""
        dataset_file = ContentFile(DATASET_CONTENT)
        dataset = Dataset(user=user,
                          name="pairs",
                          MD5_checksum=compute_md5(dataset_file))
        dataset.dataset_file.save(name='pairs.csv', content=dataset_file)
        dataset.save()
        dataset.clean()
        dataset.grant_everyone_access()

        run = Manager.execute_pipeline(user=user, pipeline=pipeline1, inputs=[dataset]).get_last_run()
        run.collect_garbage()  # Delete sandbox directories

    def create_pipelines(self, user):
        """ Create two pipelines: sums_only and sums_and_products.

        @return: (pipeline1, pipeline2)
        """
        SHUFFLED_SUMS_AND_PRODUCTS_SOURCE = """\
#! /usr/bin/env python

from argparse import FileType, ArgumentParser
import csv
import os
from random import shuffle

parser = ArgumentParser(
    description="Takes CSV with (x,y), outputs CSV with (x+y),(x*y)");
parser.add_argument("input_csv",
                    type=FileType('rU'),
                    help="CSV containing (x,y) pairs");
parser.add_argument("output_csv",
                    type=FileType('wb'),
                    help="CSV containing (x+y,xy) pairs");
args = parser.parse_args();

reader = csv.DictReader(args.input_csv);
writer = csv.DictWriter(args.output_csv,
                        ['sum', 'product'],
                        lineterminator=os.linesep)
writer.writeheader()

rows = list(reader)
shuffle(rows) # Makes this version reusable, but not deterministic
for row in rows:
    x = int(row['x'])
    y = int(row['y'])
    writer.writerow(dict(sum=x+y, product=x*y))
"""
        TOTAL_SOURCE_TEMPLATE = """\
#!/usr/bin/env python

from argparse import FileType, ArgumentParser
import csv
from operator import itemgetter
import os

parser = ArgumentParser(description='Calculate the total of a column.');
parser.add_argument("input_csv",
                    type=FileType('rU'),
                    help="CSV containing (sum,product) pairs");
parser.add_argument("output_csv",
                    type=FileType('wb'),
                    help="CSV containing one (sum,product) pair");
args = parser.parse_args();

reader = csv.DictReader(args.input_csv);
writer = csv.DictWriter(args.output_csv,
                        ['sum', 'product'],
                        lineterminator=os.linesep)
writer.writeheader()

# Copy first row unchanged
for row in reader:
    writer.writerow(row)
    break

sum_total = 0
product_total = 0
writer.writerow(dict(sum=sum_total, product=product_total))
"""
        total_sums_source = TOTAL_SOURCE_TEMPLATE.replace(
            "sum_total = 0",
            "sum_total = sum(map(int, map(itemgetter('sum'), reader)))")
        total_products_source = TOTAL_SOURCE_TEMPLATE.replace(
            "product_total = 0",
            "product_total = sum(map(int, map(itemgetter('product'), reader)))")

        sums_and_products = self.create_method(
            'sums_and_products',
            SHUFFLED_SUMS_AND_PRODUCTS_SOURCE,
            user,
            ['pairs'],
            ['sums_and_products'])
        sums_and_products.reusable = Method.REUSABLE
        sums_and_products.save()
        sums_and_products.clean()
        total_sums = self.create_method('total_sums',
                                        total_sums_source,
                                        user,
                                        ['sums_and_products'],
                                        ['total_sums'])
        total_products = self.create_method('total_products',
                                            total_products_source,
                                            user,
                                            ['sums_and_products'],
                                            ['total_products'])
        with transaction.atomic():
            family = PipelineFamily(name='sums and products', user=user)
            family.save()
            family.clean()
            family.grant_everyone_access()

            pipeline1 = family.members.create(revision_name='sums only',
                                              user=user)
            pipeline1.clean()
            pipeline1.grant_everyone_access()

            self.next_step_num = 1
            self.next_output_num = 1
            input1 = pipeline1.inputs.create(dataset_name='pairs',
                                             dataset_idx=1)
            step1_1 = self.create_step(pipeline1, sums_and_products, input1)
            step1_1.outputs_to_delete.add(sums_and_products.outputs.first())
            step1_2 = self.create_step(pipeline1, total_sums, step1_1)
            self.create_cable(step1_2, pipeline1)
            pipeline1.create_outputs()
            self.set_position([input1,
                               step1_1,
                               step1_2,
                               pipeline1.outputs.first()])
            pipeline1.complete_clean()

            pipeline2 = family.members.create(revision_name='sums and products',
                                              revision_parent=pipeline1,
                                              user=user)
            pipeline2.clean()
            pipeline2.grant_everyone_access()
            self.next_step_num = 1
            self.next_output_num = 1
            input2 = pipeline2.inputs.create(dataset_name='pairs',
                                             dataset_idx=1)
            step2_1 = self.create_step(pipeline2, sums_and_products, input2)
            step2_1.outputs_to_delete.add(sums_and_products.outputs.first())
            step2_2 = self.create_step(pipeline2, total_sums, step2_1)
            step2_3 = self.create_step(pipeline2, total_products, step2_1)
            self.create_cable(step2_2, pipeline2)
            self.create_cable(step2_3, pipeline2)
            pipeline2.create_outputs()
            self.set_position([input2,
                               step2_1,
                               step2_2,
                               pipeline2.outputs.first(),
                               step2_3,
                               pipeline2.outputs.last()])
            pipeline2.complete_clean()
        return pipeline1, pipeline2

    def set_position(self, objects):
        n = len(objects)
        for i, object in enumerate(objects, 1):
            object.x = object.y = float(i)/(n+1)
            object.save()

    def create_cable(self, source, dest):
        """ Create a cable between to pipeline objects.

        @param source: either a PipelineStep or one of the pipeline's
        TransformationInput objects for the cable to use as a source.
        @param dest: either a PipelineStep or the Pipeline for the cable to use
        as a destination.
        """
        try:
            source_output = source.transformation.outputs.first()
            source_step_num = source.step_num
        except AttributeError:
            # must be a pipeline input
            source_output = source
            source_step_num = 0

        try:
            cable = dest.cables_in.create(dest=dest.transformation.inputs.first(),
                                          source=source_output,
                                          source_step=source_step_num)
        except AttributeError:
            # must be a pipeline output
            cable = dest.create_raw_outcable(source.name,
                                             self.next_output_num,
                                             source.step_num,
                                             source_output)
            self.next_output_num += 1
        return cable

    def create_step(self, pipeline, method, input_source):
        """ Create a pipeline step.

        @param method: the method for the step to run
        @param input_source: either a pipeline input or another step that this
        step will use for its input.
        """
        step = pipeline.steps.create(transformation=method,
                                     name=method.family.name,
                                     step_num=self.next_step_num)
        self.create_cable(input_source, step)
        step.clean()
        self.next_step_num += 1
        return step

    def create_method(self, name, source, user, input_names, output_names):
        """ Create a method.

        @param source: source code
        @param input_names: list of strings to name raw inputs
        @param output_names: list of strings to name raw outputs
        @return: a new Method object that has been saved
        """
        with transaction.atomic():
            code_resource_revision = self.create_code_resource(name, source, user)
            family = MethodFamily(name=name, user=user)
            family.save()
            family.clean()
            family.grant_everyone_access()

            method = family.members.create(revision_name='first',
                                           driver=code_resource_revision,
                                           user=user)
            method.save()
            for i, input_name in enumerate(input_names, 1):
                method.inputs.create(dataset_name=input_name, dataset_idx=i)
            for i, output_name in enumerate(output_names, 1):
                method.outputs.create(dataset_name=output_name, dataset_idx=i)
            method.clean()
            method.grant_everyone_access()
            return method

    def create_code_resource(self, name, source, user):
        """ Create a new code resource.

        @param source: source code
        @return: a new CodeResourceRevision object that has been saved
        """
        with transaction.atomic():
            filename = name+'.py'
            resource = CodeResource(name=name, filename=filename, user=user)
            resource.save()
            resource.clean()
            resource.grant_everyone_access()

            revision = CodeResourceRevision(coderesource=resource, user=user)
            revision.content_file.save(filename, ContentFile(source), save=False)
            revision.clean()  # calculates md5
            revision.save()
            revision.grant_everyone_access()
        resource.clean()
        return revision


class RemovalTestEnvironmentBuilder(FixtureBuilder):
    def get_name(self):
        return 'removal.json'

    def build(self):
        tools.create_removal_test_environment()


class ExecuteTestsBuilder(FixtureBuilder):
    def get_name(self):
        return "execute_tests.json"

    def build(self):
        sandbox.tests.execute_tests_environment_setup(self)


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
        self.outcable_1 = self.pX_2.create_outcable(output_name="pX_out_1",
                                                    output_idx=1,
                                                    source_step=1,
                                                    source=self.mA_out)
        self.outcable_2 = self.pX_2.create_outcable(output_name="pX_out_2",
                                                    output_idx=2,
                                                    source_step=2,
                                                    source=self.mA_out)

        # Define CDT for the second output (first output is defined by a trivial cable)
        self.pipeline_out2_cdt = CompoundDatatype(user=self.myUser)
        self.pipeline_out2_cdt.save()
        self.out2_cdtm_1 = self.pipeline_out2_cdt.members.create(
            column_name="c",
            column_idx=1,
            datatype=self.int_dt)
        self.out2_cdtm_2 = self.pipeline_out2_cdt.members.create(
            column_name="d",
            column_idx=2,
            datatype=self.string_dt)
        self.out2_cdtm_3 = self.pipeline_out2_cdt.members.create(
            column_name="e",
            column_idx=3,
            datatype=self.string_dt)

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
        Manager.execute_pipeline(self.myUser, self.pX_2, [self.dataset])


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

        Manager.execute_pipeline(self.user_bob, self.two_step_pl, [self.dataset_wordbacks],
                                 groups_allowed=[everyone_group()])

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

        following_run = Manager.execute_pipeline(self.user_bob, self.following_pl, [self.dataset_wordbacks],
                                                 groups_allowed=[everyone_group()]).get_last_run()
        second_step = following_run.runsteps.get(pipelinestep__step_num=2)
        assert(second_step.invoked_logs.count() == 3)

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
        tools.create_eric_martin_test_environment(self)
        tools.create_sandbox_testing_tools_environment(self)

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
        tools.make_words_dataset(self)

        Manager.execute_pipeline(user=self.user_bob, pipeline=p_one, inputs=[self.dataset_words],
                                 groups_allowed=[everyone_group()],
                                 name="RunOne", description="Bob runs p_one")

        Manager.execute_pipeline(
            user=self.user_bob,
            pipeline=p_two,
            inputs=[self.dataset_words],
            groups_allowed=[everyone_group()],
            name="RunOne",
            description="Bob runs p_two, which tries to reuse part of run_one but ultimately needs to recover"
        )


class ExecuteResultTestsRMEnvironmentBuilder(FixtureBuilder):
    """
    Execution tests using tools.create_sequence_manipulation_environment.
    """

    def get_name(self):
        return "execute_result_tests_rm.json"

    def build(self):
        tools.create_sequence_manipulation_environment(self)

        # Many tests use this run.
        Manager.execute_pipeline(self.user_alice, self.pipeline_complement, [self.dataset_labdata])

        # An identically-specified second run that reuses the stuff from the first.
        Manager.execute_pipeline(self.user_alice, self.pipeline_complement, [self.dataset_labdata])

        # A couple more runs, used by another test.
        Manager.execute_pipeline(self.user_alice, self.pipeline_reverse, [self.dataset_labdata])

        Manager.execute_pipeline(self.user_alice, self.pipeline_revcomp, [self.dataset_labdata])

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
        Manager.execute_pipeline(self.user_alice, self.pipeline_revcomp_v2, [self.dataset_labdata])

        # This is usually done in tools.destroy_sequence_manipulation_environment.
        if os.path.exists(self.datafile.name):
            os.remove(self.datafile.name)


class FindDatasetsBuilder(FixtureBuilder):
    """For testing the tools that find datasets in a sandbox."""

    def get_name(self):
        return "find_datasets.json"

    def build(self):
        tools.create_word_reversal_environment(self)

        self.setup_simple_pipeline()
        self.setup_twostep_pipeline()
        self.setup_nested_pipeline()

    def setup_nested_pipeline(self):
        # A two-step pipeline with custom cable wires at each step.
        self.pipeline_nested = tools.make_first_pipeline(
            "nested pipeline",
            "a pipeline with a sub-pipeline",
            self.user_bob)

        transforms = [self.method_noop_backwords, self.pipeline_twostep, self.method_noop_backwords]
        tools.create_linear_pipeline(self.pipeline_nested,
                                     transforms,
                                     "data",
                                     "unchanged_data")
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
            cable = step.cables_in.create(source_step=i,
                                          source=source,
                                          dest=methods[i].inputs.first())
            tools.make_crisscross_cable(cable)

        cable = self.pipeline_twostep.create_outcable(
            output_name="reversed_words",
            output_idx=1,
            source_step=2,
            source=methods[-1].outputs.first())

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


class Command(BaseCommand):
    help = "Update test fixtures by running scripts and dumping test data."

    def handle(self, *args, **options):
        EMSandboxTestEnvironmentBuilder().run()
        ArchiveTestEnvironmentBuilder().run()
        ArchiveNoRunsTestEnvironmentBuilder().run()
        DeepNestedRunBuilder().run()
        SimpleRunBuilder().run()
        RemovalTestEnvironmentBuilder().run()
        RunApiTestsEnvironmentBuilder().run()
        RunComponentTooManyChecksEnvironmentBuilder().run()
        RunPipelinesRecoveringReusedStepEnvironmentBuilder().run()
        ExecuteResultTestsRMEnvironmentBuilder().run()
        ExecuteDiscardedIntermediateTestsRMEnvironmentBuilder().run()
        RestoreReusableDatasetBuilder().run()
        ExecuteTestsBuilder().run()
        FindDatasetsBuilder().run()

        self.stdout.write('Done.')
