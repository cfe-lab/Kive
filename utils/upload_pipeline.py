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
from itertools import count
from constants import maxlengths


class PipelineStepRequest(object):
    def __init__(self, json_config):
        self.config = json_config
        self.name = None
        self.code_resource = self.code_resource_revision = None
        self.method_family = self.method = None

    def load(self, kive, old_step):
        response = kive.get('/api/methodfamilies/{}'.format(
            old_step['transformation_family']))
        self.method_family = response.json()
        response = kive.get('/api/methods/{}'.format(
            old_step['transformation']))
        self.method = response.json()
        response = kive.get('/api/coderesourcerevisions/{}'.format(
            self.method['driver']))
        self.code_resource_revision = response.json()
        response = kive.get(
            '/api/coderesources/?filters[0][key]=name&filters[0][val]={}'.format(
                self.code_resource_revision['coderesource']))
        self.code_resource = response.json()[0]


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
        return self.name

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


def create_pipeline(kive, pipeline_family, pipeline_config, steps):
    groups = pipeline_family.details['groups_allowed']
    inputs = []
    for old_input in pipeline_config['inputs']:
        new_input = dict(old_input)
        new_input['structure'] = None  # TODO: Add structure
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
        new_cable['output_cdt'] = None  # TODO: Add structure
        new_cable['x'] = new_cable['y'] = i
        outcables.append(new_cable)
    response = kive.post('@api_pipelines',
                         json=dict(family=pipeline_family.name,
                                   inputs=inputs,
                                   steps=step_data,
                                   # outputs=outputs,
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
        for child in find_dependencies(kive, folder, old_requirement):
            yield child


def load_steps(kive, folder, pipeline_family, groups):
    with open(os.path.join(folder, 'pipeline.json'), 'rU') as pipeline_file:
        pipeline_config = json.load(pipeline_file)
    try:
        old_pipeline = pipeline_family.latest()
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
            find_dependencies(kive, folder, transformation['driver']))
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
            inputs = step.config['inputs']
            for inp in inputs:
                inp['structure'] = None  # TODO: load structures
            outputs = step.config['outputs']
            for out in outputs:
                out['structure'] = None  # TODO: load structures
            dependencies = []
            for dependency in step.dependencies:
                new_revision = dependency['requirement'].code_resource_revision
                dependencies.append(dict(requirement=new_revision['id'],
                                         path=dependency['depPath'],
                                         filename=dependency['depFileName']))
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

    steps, pipeline_config = load_steps(kive, folder, pipeline_family, groups)
    print('Uploading {!r} to {} for {}.'.format(folder, pipeline_family, groups))
    for i, step in enumerate(steps, start=1):
        print '  {}: {}'.format(i, step.driver.get_display())
        for dependency in step.dependencies:
            print '     ' + dependency['requirement'].get_display()
    revision_name = raw_input('Enter a revision name, or leave blank to abort: ')
    if not revision_name:
        return

    create_code_resources(kive, steps, revision_name)
    create_methods(kive, steps, revision_name)
    pipeline_family = create_pipeline_family(kive, pipeline_family, groups)
    create_pipeline(kive, pipeline_family, pipeline_config, steps)
    print('Done.')

main()
