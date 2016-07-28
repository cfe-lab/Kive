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


class PipelineStep(object):
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
#     outputs = []
#     for old_output in pipeline_config['outputs']:
#         new_output = dict(old_output)
#         new_output['structure'] = None  # TODO: Add structure
#         outputs.append(new_output)
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


def load_steps(kive, folder, pipeline_family, groups):
    with open(os.path.join(folder, 'pipeline.json'), 'rU') as pipeline_file:
        pipeline_config = json.load(pipeline_file)
    try:
        old_pipeline = pipeline_family.latest()
    except AttributeError:
        old_pipeline = None
    steps = []
    for step_config in pipeline_config['steps']:
        step = PipelineStep(step_config)
        step.users_allowed = []
        step.groups_allowed = groups
        transformation = step_config['transformation']
        step.filename = transformation['driver']['coderesource']['filename']
        step.path = os.path.join(folder, step.filename)
        step.name = step_config['name']
        if old_pipeline:
            for old_step in old_pipeline.details['steps']:
                if old_step['name'] == step.name:
                    step.load(kive, old_step)
                    break
        assert os.path.isfile(step.path), step.path
        steps.append(step)
    return steps, pipeline_config


def create_code_resources(kive, steps, revision_name):
    for step in steps:
        for copy_num in count(start=1):
            if step.code_resource is not None:
                break
            name = step.name
            if copy_num > 1:
                suffix = ' ({})'.format(copy_num)
                name = name[:maxlengths.MAX_NAME_LENGTH-len(suffix)]
                name += suffix
            response = kive.get(
                '/api/coderesources/?filters[0][key]=name&filters[0][val]=' +
                name)
            if not any(match['name'] == name for match in response.json()):
                response = kive.post('/api/coderesources/',
                                     json=dict(name=name,
                                               filename=step.filename,
                                               users_allowed=step.users_allowed,
                                               groups_allowed=step.groups_allowed))
                step.code_resource = response.json()
        if step.code_resource_revision is None:
            with open(step.path, 'rb') as f:
                response = kive.post('/api/stagedfiles/',
                                     files=dict(uploaded_file=f))
            staged_file = response.json()
            response = kive.post('/api/coderesourcerevisions/',
                                 json=dict(coderesource=step.code_resource['name'],
                                           revision_name=revision_name,
                                           staged_file=staged_file['pk'],
                                           users_allowed=step.users_allowed,
                                           groups_allowed=step.groups_allowed))
            step.code_resource_revision = response.json()


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
            response = kive.post('/api/methods/',
                                 json=dict(revision_name=revision_name,
                                           family=step.method_family['name'],
                                           driver=step.code_resource_revision['id'],
                                           reusable=transformation['reusable'],
                                           threads=transformation['threads'],
                                           inputs=inputs,
                                           outputs=outputs,
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
    print('\n'.join('  {}: {}'.format(i, step.name)
                    for i, step in enumerate(steps, start=1)))
    revision_name = raw_input('Enter a revision name, or leave blank to abort: ')
    if not revision_name:
        return

    create_code_resources(kive, steps, revision_name)
    create_methods(kive, steps, revision_name)
    pipeline_family = create_pipeline_family(kive, pipeline_family, groups)
    create_pipeline(kive, pipeline_family, pipeline_config, steps)
    print('Done.')
#     for pipeline in pipelines[:5]:
#         print '{}, id {}'.format(pipeline, pipeline.pipeline_id)
#     pipeline_request = raw_input("Enter pipeline id to dump, or 'm' for more:")
#     if pipeline_request == 'm':
#         for pipeline in pipelines[5:]:
#             print '{}, id {}'.format(pipeline, pipeline.pipeline_id)
#         pipeline_request = raw_input("Enter pipeline id to dump:")
#     pipeline_id = int(pipeline_request)
#     dump_folder = 'utils/dump/{}_pipeline{}'.format(hostname, pipeline_id)
#
#     if not os.path.isdir(dump_folder):
#         os.makedirs(dump_folder)
#
#     compound_datatypes = {}  # {id: columns}
#     for compound_datatype in kive.get_cdts():
#         columns = compound_datatype.name
#         compound_datatypes[compound_datatype.cdt_id] = columns
#     code_resources = {}  # {id: {'filename': filename}}
#     for code_resource in kive.get('/api/coderesources/').json():
#         dump = {}
#         for field in ('groups_allowed', 'users_allowed', 'filename'):
#             dump[field] = code_resource[field]
#         code_resources[code_resource['name']] = dump
#     code_resource_revisions = {}  # {id: revision}
#     for revision in kive.get('/api/coderesourcerevisions/').json():
#         code_resource_revisions[revision['id']] = CodeResourceRevision(
#             revision,
#             code_resources)
#     for revision in code_resource_revisions.itervalues():
#         for dependency in revision['dependencies']:
#             requirement = code_resource_revisions[dependency['requirement']]
#             dependency['requirement'] = requirement
#         revision['dependencies'].sort(
#             key=lambda dep: (dep['depPath'],
#                              dep['depFileName'],
#                              dep['requirement']['coderesource']['filename']))
#     methods = {}  # {id: method}
#     for method in kive.get('/api/methods/').json():
#         dump = {'driver': code_resource_revisions[method['driver']]}
#         for field in ('groups_allowed',
#                       'users_allowed',
#                       'reusable',
#                       'threads'):
#             dump[field] = method[field]
#         methods[method['id']] = dump
#
#     used_revisions = set()
#     pipeline_wrapper = kive.get_pipeline(pipeline_id)
#     pipeline = pipeline_wrapper.details
#     print 'Dumping {}.'.format(pipeline_wrapper)
#     dump = {}
#     for input_item in pipeline['inputs']:
#         del input_item['x']
#         del input_item['y']
#         replace_structure(input_item, compound_datatypes)
#     dump['inputs'] = pipeline['inputs']
#     for output_item in pipeline['outputs']:
#         del output_item['x']
#         del output_item['y']
#         del output_item['dataset_idx']
#         replace_structure(output_item, compound_datatypes)
#     pipeline['outputs'].sort()
#     dump['outputs'] = pipeline['outputs']
#     for step in pipeline['steps']:
#         del step['x']
#         del step['y']
#         for cable in step['cables_in']:
#             del cable['dest']
#             del cable['source']
#         for input_item in step['inputs']:
#             replace_structure(input_item, compound_datatypes)
#         for output_item in step['outputs']:
#             replace_structure(output_item, compound_datatypes)
#         del step['transformation_family']
#         step['transformation'] = methods[step['transformation']]
#         step['transformation']['driver'].collect_revisions(used_revisions)
#     dump['steps'] = pipeline['steps']
#
#     pipeline_filename = 'pipeline.json'
#     with open(os.path.join(dump_folder, pipeline_filename), 'w') as f:
#         json.dump(dump, f, indent=4, sort_keys=True)
#
#     filename_counts = Counter()
#     for revision in used_revisions:
#         filename = revision['coderesource']['filename']
#         filename_counts[filename] += 1
#         response = kive.get(revision.url, is_json=False, stream=True)
#         deadline = datetime.now() + timedelta(seconds=10)
#         is_complete = True
#         with open(os.path.join(dump_folder, filename), 'w') as f:
#             for block in response.iter_content():
#                 f.write(block)
#                 if datetime.now() > deadline:
#                     is_complete = False
#                     break
#         if not is_complete:
#             os.remove(os.path.join(dump_folder, filename))
#             with open(os.path.join(dump_folder, filename + '_timed_out'), 'w'):
#                 pass
#     duplicate_filenames = [filename
#                            for filename, count in filename_counts.iteritems()
#                            if count > 1]
#     if duplicate_filenames:
#         raise RuntimeError('Multiple versions found: ' +
#                            ', '.join(duplicate_filenames))
#
#     print 'Dumped {}.'.format(pipeline_wrapper)
#
#
# class CodeResourceRevision(dict):
#     def __init__(self, data, code_resources):
#         for field in ('groups_allowed',
#                       'users_allowed',
#                       'MD5_checksum',
#                       'dependencies'):
#             self[field] = data[field]
#         self['coderesource'] = code_resources[data['coderesource']]
#         self.id = data['id']
#         self.url = data['download_url']
#
#     def __hash__(self):
#         return hash(self.id)
#
#     def __eq__(self, other):
#         return self.id == other.id
#
#     def collect_revisions(self, used_revisions):
#         """ Collect all the related code resource revisions, including self.
#
#         @param used_revisions: a set of resource revision ids
#         """
#         used_revisions.add(self)
#         for dependency in self['dependencies']:
#             dependency['requirement'].collect_revisions(used_revisions)
#
#
# def replace_structure(item, compound_datatypes):
#     structure = item['structure']
#     if structure:
#         columns = compound_datatypes[structure['compounddatatype']]
#         structure['compounddatatype'] = columns

main()
