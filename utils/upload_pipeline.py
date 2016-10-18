# Instructions:
# Run dump_pipeline.py, and it will create a configuration file in your home
# folder. Edit that file to point at the server you want to upload to,
# then run this script. It will ask you which pipeline you want to upload.

import json
import logging
import os

from requests.adapters import HTTPAdapter

from kiveapi import KiveAPI
from urlparse import urlparse
from glob import glob
from operator import attrgetter
from kiveapi.pipeline import PipelineFamily, Pipeline
from itertools import count, chain
from constants import maxlengths


class PipelineStepRequest(object):
    def __init__(self, json_config):
        self.config = json_config
        self.name = None
        self.code_resource = self.code_resource_revision = None
        self.method_family = self.method = self.old_method = None

    def get_code_resource_revision(self, kive, id):
        response = kive.get('/api/coderesourcerevisions/{}'.format(id))
        return response.json()

    def has_xput_changes(self, old_xputs, new_xputs, kive):
        if len(old_xputs) != len(new_xputs):
            return True
        for old_xput, new_xput in zip(old_xputs, new_xputs):
            if (old_xput['dataset_idx'] != new_xput['dataset_idx'] or
                    old_xput['dataset_name'] != new_xput['dataset_name']):
                return True
            old_structure = old_xput['structure']
            new_structure = new_xput['structure']
            if (old_structure is None) != (new_structure is None):
                return True
            if old_structure is not None:
                if (old_structure['min_row'] != new_structure['min_row'] or
                        old_structure['max_row'] != new_structure['max_row']):
                    return True
                old_compound_datatype_id = old_structure['compounddatatype']
                old_compound_datatype = kive.get_cdt(old_compound_datatype_id)
                if old_compound_datatype.name != new_structure['compounddatatype']:
                    return True
        return False

    def get_display(self):
        suffix = '' if self.method is None else ' (existing method)'
        return self.driver.get_display() + suffix

    def load(self, kive, old_step):
        response = kive.get('/api/methodfamilies/{}'.format(
            old_step['transformation_family']))
        self.method_family = response.json()
        response = kive.get('/api/methods/{}'.format(
            old_step['transformation']))
        self.old_method = response.json()
        self.old_method['driver'] = self.get_code_resource_revision(
            kive,
            self.old_method['driver'])
        has_changes = self.driver.check_for_changes(self.old_method['driver'])
        response = kive.get('/api/coderesources/')
        code_resource_filenames = {r['name']: r['filename']
                                   for r in response.json()}
        old_dependencies = {}  # {filename: dependency}
        for old_dependency in self.old_method['dependencies']:
            old_revision_id = old_dependency['requirement']
            old_revision = self.get_code_resource_revision(kive, old_revision_id)
            resource_name = old_revision['coderesource']
            resource_filename = code_resource_filenames[resource_name]
            old_dependency['requirement'] = old_revision
            old_dependencies[resource_filename] = old_dependency
        for new_dependency in self.dependencies:
            old_dependency = old_dependencies.get(
                new_dependency['requirement'].name)
            has_changes |= (old_dependency is None or
                            new_dependency['requirement'].check_for_changes(
                                old_dependency['requirement']))
        checked_attributes = 'reusable threads users_allowed groups_allowed'.split()
        new_method = self.config['transformation']
        for attribute in checked_attributes:
            has_changes = (has_changes or
                           new_method[attribute] != self.old_method[attribute])
        has_changes = has_changes or self.has_xput_changes(self.old_method['inputs'],
                                                           self.config['inputs'],
                                                           kive)
        has_changes = has_changes or self.has_xput_changes(self.old_method['outputs'],
                                                           self.config['outputs'],
                                                           kive)
        if not has_changes:
            self.method = self.old_method


class CodeResourceRequest(object):
    existing = {}  # {name: code_resource_revision}

    def __init__(self, kive, folder, config):
        self.kive = kive
        self.config = config
        self.name = self.config['coderesource']['filename']
        self.path = os.path.join(folder, self.name)
        self.code_resource = self.code_resource_revision = None
        assert os.path.isfile(self.path), self.path

    def get_display(self):
        display = self.name
        if self.code_resource_revision is not None:
            display += ' (unchanged)'
        return display

    def check_for_changes(self, old_revision):
        """ Check if the code resource revision is different from the old one.

        :param old_revision: old one to compare to
        :return: True if there are changes.
        """
        self.code_resource = dict(name=old_revision['coderesource'])
        existing_config = CodeResourceRequest.existing.get(self.name, None)
        if existing_config is not None:
            self.code_resource_revision = existing_config
            return False
        old_md5 = old_revision['MD5_checksum']
        new_md5 = self.config['MD5_checksum']
        if new_md5 == old_md5:
            self.code_resource_revision = old_revision
            CodeResourceRequest.existing[self.name] = old_revision
            return False
        return True

    def upload(self, revision_name):
        """ Upload a code resource and code resource revision, if needed.

        Sets self.code_resource_revision to hold the JSON configuration, and
        adds it to CodeResourceRequest.existing.
        """
        existing_config = CodeResourceRequest.existing.get(self.name, None)
        if existing_config is not None:
            self.code_resource_revision = existing_config
            return
        code_resource_config = self.config['coderesource']
        for copy_num in count(start=1):
            if self.code_resource is not None:
                break
            name = self.name
            if copy_num > 1:
                suffix = ' ({})'.format(copy_num)
                name = name[:maxlengths.MAX_NAME_LENGTH-len(suffix)]
                name += suffix
            response = self.kive.get(
                '/api/coderesources/?filters[0][key]=name&filters[0][val]=' +
                name)
            if not any(match['name'] == name for match in response.json()):
                response = self.kive.post(
                    '/api/coderesources/',
                    json=dict(name=name,
                              filename=self.name,
                              users_allowed=code_resource_config['users_allowed'],
                              groups_allowed=code_resource_config['groups_allowed']))
                self.code_resource = response.json()
        if self.code_resource_revision is None:
            with open(self.path, 'rb') as f:
                response = self.kive.post('/api/stagedfiles/',
                                          files=dict(uploaded_file=f))
            staged_file = response.json()
            response = self.kive.post(
                '/api/coderesourcerevisions/',
                json=dict(coderesource=self.code_resource['name'],
                          revision_name=revision_name,
                          staged_file=staged_file['pk'],
                          users_allowed=self.config['users_allowed'],
                          groups_allowed=self.config['groups_allowed']))
            self.code_resource_revision = response.json()
        CodeResourceRequest.existing[self.name] = self.code_resource_revision


class CompoundDatatypeRequest(object):
    datatypes = {}  # {name: id}
    existing = {}  # {representation: request}
    new_requests = set()

    @classmethod
    def load_existing(cls, kive):
        """ Load all the existing compound datatypes. """
        response = kive.get('/api/datatypes')
        for datatype in response.json():
            cls.datatypes[datatype['name']] = datatype['id']
        for compound_datatype in kive.get_cdts():
            representation = compound_datatype.raw['representation']
            request = cls(representation)
            request.compound_datatype = compound_datatype.raw
            cls.existing[representation] = request

    @classmethod
    def load(cls, kive, representation):
        request = cls.existing.get(representation, None)
        if request is None:
            request = cls(representation)
            cls.existing[representation] = request
            cls.new_requests.add(request)
        return request

    def __init__(self, representation):
        self.representation = representation
        self.members = []
        stripped = representation.strip('()')
        for i, member_representation in enumerate(stripped.split(', '), start=1):
            name, type_name = member_representation.split(': ')
            is_blankable = type_name[-1] == '?'
            if is_blankable:
                type_name = type_name[:-1]
            datatype_id = CompoundDatatypeRequest.datatypes[type_name]
            self.members.append(dict(datatype=datatype_id,
                                     column_name=name,
                                     column_idx=i,
                                     blankable=is_blankable))
        self.compound_datatype = None

    def create(self, kive, groups):
        if self.compound_datatype is None:
            self.compound_datatype = kive.create_cdt(name=self.representation,
                                                     members=self.members,
                                                     users=[],
                                                     groups=groups).raw


def choose_folder():
    dump_folders = sorted(os.path.dirname(folder)
                          for folder in glob('dump/*/pipeline.json'))
    print('Choose a pipeline to upload:')
    for i, folder in enumerate(dump_folders, start=1):
        print('  {}: {}'.format(i, folder))

    folder = None
    while folder is None:
        try:
            choice = int(raw_input('Enter the number: '))
            folder = dump_folders[choice - 1]
        except ValueError:
            pass
        except IndexError:
            pass

    return folder


def choose_family(kive):
    """ Choose a pipeline family from a Kive server.

    :return: Either a PipelineFamily object from the Kive API, or a string
    with the name of a new pipeline family to create.
    """
    pipeline_families = kive.get_pipeline_families()
    pipeline_families.sort(key=attrgetter('name'))
    hostname = urlparse(kive.server_url).hostname
    print('Choose a pipeline family from {}:'.format(hostname))
    for i, family in enumerate(pipeline_families, start=1):
        print('  {}: {}'.format(i, family.name))
    family_name = ''
    while not family_name:
        family_name = raw_input('Enter the number or a new family name:')
    pipeline_family = None
    try:
        pipeline_family = pipeline_families[int(family_name)-1]
        return pipeline_family
    except ValueError:
        pass
    except IndexError:
        pass
    return family_name


def create_pipeline_family(kive, family_name, groups):
    response = kive.post('@api_pipeline_families',
                         json=dict(name=family_name,
                                   groups_allowed=groups))
    pipeline_family = PipelineFamily(response.json())
    return pipeline_family


def load_pipeline(kive, pipeline_config):
    for config in chain(pipeline_config['inputs'], pipeline_config['outputs']):
        structure = config['structure']
        if structure is not None:
            CompoundDatatypeRequest.load(kive, structure['compounddatatype'])


def create_pipeline(kive, pipeline_family, revision_name, pipeline_config, steps):
    groups = pipeline_family.details['groups_allowed']
    inputs = []
    for old_input in pipeline_config['inputs']:
        new_input = dict(old_input)
        structure = new_input['structure']
        if structure is not None:
            request = CompoundDatatypeRequest.load(kive,
                                                   structure['compounddatatype'])
            structure['compounddatatype'] = request.compound_datatype['id']
        inputs.append(new_input)
    step_data = []
    for step in steps:
        step_data.append(dict(transformation=step.method['id'],
                              name=step.config['name'],
                              cables_in=step.config['cables_in'],
                              step_num=step.config['step_num']))
    outcables = []
    for i, old_cable in enumerate(pipeline_config['outcables']):
        new_cable = dict(old_cable)
        compound_datatype = new_cable['output_cdt']
        if compound_datatype is not None:
            request = CompoundDatatypeRequest.load(kive, compound_datatype)
            new_cable['output_cdt'] = request.compound_datatype['id']
        new_cable['x'] = new_cable['y'] = i
        outcables.append(new_cable)
    outputs = []
    for old_output in pipeline_config['outputs']:
        new_output = dict(old_output)
        structure = new_output['structure']
        if structure is not None:
            request = CompoundDatatypeRequest.load(kive,
                                                   structure['compounddatatype'])
            structure['compounddatatype'] = request.compound_datatype['id']
        outputs.append(new_output)
    response = kive.post('@api_pipelines',
                         json=dict(family=pipeline_family.name,
                                   revision_name=revision_name,
                                   inputs=inputs,
                                   steps=step_data,
                                   outputs=outputs,
                                   outcables=outcables,
                                   users_allowed=[],
                                   groups_allowed=groups))
    pipeline = Pipeline(response.json())
    return pipeline


def find_dependencies(kive, folder, revision_config):
    for dependency in revision_config['dependencies']:
        old_requirement = dependency['requirement']
        new_dependency = dict(dependency)
        new_dependency['requirement'] = CodeResourceRequest(kive,
                                                            folder,
                                                            old_requirement)
        yield new_dependency


def load_steps(kive, folder, pipeline_family, groups):
    with open(os.path.join(folder, 'pipeline.json'), 'rU') as pipeline_file:
        pipeline_config = json.load(pipeline_file)
    try:
        old_pipeline_id = pipeline_family.latest().pipeline_id
        old_pipeline = kive.get_pipeline(old_pipeline_id)
    except AttributeError:
        old_pipeline = None
    steps = []
    for step_config in pipeline_config['steps']:
        step = PipelineStepRequest(step_config)
        step.users_allowed = []
        step.groups_allowed = groups
        transformation = step_config['transformation']
        step.driver = CodeResourceRequest(kive, folder, transformation['driver'])
        step.name = step_config['name']
        step.dependencies = list(
            find_dependencies(kive, folder, transformation))
        step.inputs = []
        for input_config in step_config['inputs']:
            if input_config['structure'] is None:
                request = None
            else:
                representation = input_config['structure']['compounddatatype']
                request = CompoundDatatypeRequest.load(kive, representation)
            step.inputs.append((input_config, request))
        step.outputs = []
        for output_config in step_config['outputs']:
            if output_config['structure'] is None:
                request = None
            else:
                representation = output_config['structure']['compounddatatype']
                request = CompoundDatatypeRequest.load(kive, representation)
            step.outputs.append((output_config, request))
        if old_pipeline:
            for old_step in old_pipeline.details['steps']:
                if old_step['name'] == step.name:
                    step.load(kive, old_step)
                    break
        steps.append(step)
    return steps, pipeline_config


def create_code_resources(kive, steps, revision_name):
    for step in steps:
        step.driver.upload(revision_name)
        for dependency in step.dependencies:
            dependency['requirement'].upload(revision_name)


def create_methods(kive, steps, revision_name):
    for step in steps:
        for copy_num in count(start=1):
            if step.method_family is not None:
                break
            name = step.name
            if copy_num > 1:
                suffix = ' ({})'.format(copy_num)
                name = name[:maxlengths.MAX_NAME_LENGTH-len(suffix)]
                name += suffix
            response = kive.get(
                '/api/methodfamilies/?filters[0][key]=name&filters[0][val]=' +
                name)
            if not any(match['name'] == name for match in response.json()):
                response = kive.post('/api/methodfamilies/',
                                     json=dict(name=name,
                                               users_allowed=step.users_allowed,
                                               groups_allowed=step.groups_allowed))
                step.method_family = response.json()
        if step.method is None:
            transformation = step.config['transformation']
            inputs = []
            for input_config, request in step.inputs:
                if request is not None:
                    new_id = request.compound_datatype['id']
                    input_config['structure']['compounddatatype'] = new_id
                inputs.append(input_config)
            outputs = []
            for output_config, request in step.outputs:
                if request is not None:
                    new_id = request.compound_datatype['id']
                    output_config['structure']['compounddatatype'] = new_id
                outputs.append(output_config)
            dependencies = []
            dependency_paths = set()
            for dependency in step.dependencies:
                new_revision = dependency['requirement'].code_resource_revision

                # Skip duplicates caused by migrations.
                filename = dependency['filename']
                if not filename:
                    filename = dependency['requirement'].config['coderesource']['filename']
                dependency_path = os.path.normpath(
                    os.path.join(dependency['path'], filename))
                if dependency_path not in dependency_paths:
                    dependencies.append(dict(requirement=new_revision['id'],
                                             path=dependency['path'],
                                             filename=dependency['filename']))
                    dependency_paths.add(dependency_path)
            response = kive.post('/api/methods/',
                                 json=dict(revision_name=revision_name,
                                           family=step.method_family['name'],
                                           driver=step.driver.code_resource_revision['id'],
                                           reusable=transformation['reusable'],
                                           threads=transformation['threads'],
                                           inputs=inputs,
                                           outputs=outputs,
                                           dependencies=dependencies,
                                           users_allowed=step.users_allowed,
                                           groups_allowed=step.groups_allowed))
            step.method = response.json()


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(
        logging.WARN)
    CONFIG_FILE = os.path.expanduser("~/.dump_pipeline.config")

    with open(CONFIG_FILE, 'rU') as f:
        config = json.load(f)

    kive = KiveAPI(config['server'])
    kive.mount('https://', HTTPAdapter(max_retries=20))
    kive.login(config['username'], config['password'])

    folder = choose_folder()
    pipeline_family = choose_family(kive)
    groups = raw_input('Groups allowed? [Everyone] ') or 'Everyone'
    groups = groups.split(',')

    CompoundDatatypeRequest.load_existing(kive)
    steps, pipeline_config = load_steps(kive, folder, pipeline_family, groups)
    load_pipeline(kive, pipeline_config)
    print('Uploading {!r} to {} for {}.'.format(folder, pipeline_family, groups))
    for i, step in enumerate(steps, start=1):
        print '  {}: {}'.format(i, step.get_display())
        for dependency in step.dependencies:
            print '     ' + dependency['requirement'].get_display()
    new_compound_datatypes = [request.representation
                              for request in CompoundDatatypeRequest.new_requests]
    new_compound_datatypes.sort()
    print('New compound datatypes:')
    print('\n'.join(new_compound_datatypes))
    revision_name = raw_input('Enter a revision name, or leave blank to abort: ')
    if not revision_name:
        return

    for request in CompoundDatatypeRequest.new_requests:
        request.create(kive, groups)
    create_code_resources(kive, steps, revision_name)
    create_methods(kive, steps, revision_name)
    if not isinstance(pipeline_family, PipelineFamily):
        pipeline_family = create_pipeline_family(kive, pipeline_family, groups)
    create_pipeline(kive, pipeline_family, revision_name, pipeline_config, steps)
    print('Done.')

if __name__ == '__main__':
    main()
elif __name__ == '__live_coding__':
    CompoundDatatypeRequest.datatypes = dict(integer=1,
                                             float=2)
    request = CompoundDatatypeRequest(
        "(count: integer, fwdq: float?, revq: float?)")
