from datetime import datetime
import json
import os
import shutil
import sys

from django.contrib.auth.models import User
from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.utils.dateparse import parse_datetime
from django.utils.timezone import utc
from django.conf import settings

from container.models import ContainerFamily, ContainerArgument, Container
from file_access_utils import compute_md5
from librarian.models import Dataset
from portal.management.commands.reset import Command as ResetCommand


class FixtureBuilder:
    def __init__(self):
        self.next_output_num = self.next_step_num = 1

    def get_name(self):
        """ Return the fixture file's name. """
        raise NotImplementedError()

    def build(self):
        """ Build all the records that should be in the fixture. """
        raise NotImplementedError()

    def run(self):
        print("--------")
        print(self.get_name())
        print("--------")
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

        for target in ResetCommand.TARGETS:
            target_path = os.path.join(settings.MEDIA_ROOT, target)
            fixture_name, _extension = os.path.splitext(self.get_name())
            fixture_files_path = os.path.join("FixtureFiles", fixture_name, target)

            # Out with the old...
            if os.path.isdir(fixture_files_path):
                shutil.rmtree(fixture_files_path)

            # ... in with the new.
            if os.path.isdir(target_path):
                shutil.copytree(target_path, fixture_files_path)
                self.remove_empty_folders(fixture_files_path)

    def remove_empty_folders(self, path):
        is_empty = True
        for name in os.listdir(path):
            child_path = os.path.join(path, name)
            if os.path.isdir(child_path):
                self.remove_empty_folders(child_path)
            if os.path.exists(child_path):
                is_empty = False
        if is_empty:
            os.rmdir(path)

    def fillpathset(self, orgset):
        """ Given a set of directory name strings, create a new set of strings that contains
        the intermediate directory names as well as the original strings.
        E.g.
        input: ( 'micall/core', micall/bla/goo', 'micall/utils' )
        output:
        ( 'micall/core', micall/bla/goo', 'micall/utils', 'micall', 'micall/bla' )
        """
        newset = set()
        for pathname in orgset:
            clst = pathname.split(os.sep)
            for n in range(len(clst)):
                newset.add(os.path.join(*clst[:n+1]))
        return newset

    def _rdfilelst(self, fnamelst):
        """Given a list of file names, return a list of strings, where
        each string is the contents of that file.
        """
        rlst = []
        for fn in fnamelst:
            with open(fn, "r") as f:
                rlst.append(f.read())
        return rlst

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
            for field, value in dump_object['fields'].items():
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
            for field, value in dump_object['fields'].items():
                if value is not None and field in field_names:
                    dump_object['fields'][field] = date_map[value]

    def create_cable(self, source, dest):
        """ Create a cable between two pipeline objects.

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

        @param pipeline: the pipeline that will contain the step
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

    def set_position(self, objlst):
        """Set the x, y screen coordinates of the objects in the list
        along a diagonal line (top left to bottom right)
        """
        n = len(objlst)
        for i, obj in enumerate(objlst, 1):
            obj.x = obj.y = float(i)/(n+1)
            obj.save()


class ContainerRunBuilder(FixtureBuilder):
    """For testing the tools that find datasets in a sandbox."""

    def get_name(self):
        return "container_run.json"

    def build(self):
        user = User.objects.first()
        assert user is not None
        input_path = os.path.abspath(os.path.join(
            __file__,
            '../../../../../samplecode/singularity/host_input/example_names.csv'))
        family = ContainerFamily.objects.create(name='fixture family', user=user)
        container_path = os.path.abspath(os.path.join(
            __file__,
            '../../../../../samplecode/singularity/python2-alpine-trimmed.simg'))
        with open(container_path, "rb") as f:
            container_md5 = compute_md5(f)
        container = family.containers.create(
            tag='vFixture',
            user=user,
            file='Containers/kive-default.simg',
            md5=container_md5
        )
        app = container.apps.create()
        arg1 = app.arguments.create(type=ContainerArgument.INPUT,
                                    name='names_csv',
                                    position=1)
        app.arguments.create(type=ContainerArgument.OUTPUT,
                             name='greetings_csv',
                             position=2)
        dataset = Dataset.create_dataset(input_path,
                                         name='names.csv',
                                         user=user)
        run = app.runs.create(name='fixture run', user=user)
        run.sandbox_path = ""  # blank this out as it won't be accessible in testing anyway
        run.slurm_job_id = None  # this also would cause tests to fail on a fresh system
        run.save(schedule=False)  # scheduling would overwrite sandbox_path
        run.datasets.create(argument=arg1, dataset=dataset)

        upload_path = os.path.join(settings.MEDIA_ROOT, Container.UPLOAD_DIR)
        readme_path = os.path.join(upload_path, 'README.md')
        os.makedirs(upload_path)
        with open(readme_path, 'w') as f:
            f.write('Just a placeholder to create the folder for containers.')


class Command(BaseCommand):
    help = "Update test fixtures by running scripts and dumping test data."

    def handle(self, *args, **options):
        ContainerRunBuilder().run()

        self.stdout.write('Done.')
