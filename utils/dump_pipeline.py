# Instructions:
# Run the script once, and it will create a configuration file in your home
# folder. Edit that file to point at the server you want to dump from,
# then run the script again. It will ask you which pipeline you want to dump.
from argparse import ArgumentParser
from collections import Counter
from datetime import datetime, timedelta
import json
import logging
from itertools import groupby
from operator import itemgetter, attrgetter
import os
from six.moves import input
from six.moves.urllib.parse import urlparse

from requests.adapters import HTTPAdapter

from kiveapi import KiveAPI


def parse_args():
    parser = ArgumentParser(description='Dump a pipeline into a JSON file.')
    parser.add_argument('--server',
                        '-s',
                        default=os.getenv('KIVE_SERVER',
                                          'http://localhost:8000'),
                        help='Kive server to upload to.')
    parser.add_argument('--username',
                        '-u',
                        default=os.getenv('KIVE_USER',
                                          'kive'),
                        help='Kive user to connect with. Set the password '
                             'in environment variable KIVE_PASSWORD.')
    args = parser.parse_args()
    args.password = os.getenv('KIVE_PASSWORD', 'kive')
    return args


def recent_pipelines(all_pipelines):
    for family, pipelines in groupby(all_pipelines, attrgetter('family')):
        for i, pipeline in enumerate(pipelines):
            if i < 3:
                yield pipeline


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(
        logging.WARN)
    args = parse_args()
    print('Starting.')

    kive = KiveAPI(args.server)
    kive.mount('https://', HTTPAdapter(max_retries=20))
    kive.login(args.username, args.password)

    all_pipelines = kive.get_pipelines()
    pipelines = list(recent_pipelines(all_pipelines))
    hostname = urlparse(kive.server_url).hostname
    print('Recent pipelines from {}:'.format(hostname))
    for pipeline in pipelines:
        # noinspection PyUnresolvedReferences
        print('{} - {}, id {}'.format(pipeline.family,
                                      pipeline,
                                      pipeline.pipeline_id))
    # noinspection PyCompatibility
    pipeline_request = input("Enter pipeline id to dump, or 'm' for more:")
    if pipeline_request == 'm':
        for pipeline in all_pipelines:
            print('{} - {}, id {}'.format(pipeline.family,
                                          pipeline,
                                          pipeline.pipeline_id))
        # noinspection PyCompatibility
        pipeline_request = input("Enter pipeline id to dump:")
    pipeline_id = int(pipeline_request)
    dump_folder = os.path.abspath(
        'dump/{}_pipeline{}'.format(hostname, pipeline_id))

    if not os.path.isdir(dump_folder):
        os.makedirs(dump_folder)

    compound_datatypes = {}  # {id: columns}
    for compound_datatype in kive.get_cdts():
        columns = compound_datatype.name
        compound_datatypes[compound_datatype.cdt_id] = columns
    code_resources = {}  # {id: {'filename': filename}}
    for code_resource in kive.get('/api/coderesources/').json():
        dump = {}
        for field in ('groups_allowed', 'users_allowed', 'filename'):
            dump[field] = code_resource[field]
        code_resources[code_resource['name']] = dump
    code_resource_revisions = {}  # {id: revision}
    for revision in kive.get('/api/coderesourcerevisions/').json():
        code_resource_revisions[revision['id']] = CodeResourceRevision(
            revision,
            code_resources)
    code_resource_revisions[None] = None
    api_end_points = kive.get('/api/').json()
    if 'dockerimages' in api_end_points:
        docker_images = {img['url']: img['full_name']
                         for img in kive.get('/api/dockerimages/').json()}
    else:
        # Old server doesn't have docker image support.
        docker_images = {}
    docker_images[None] = None
    methods = {}  # {id: method}
    for method in kive.get('/api/methods/').json():
        for dep in method['dependencies']:
            dep['requirement'] = code_resource_revisions[dep['requirement']]
            if dep['path'] == '././':
                dep['path'] = '.'
        method['dependencies'].sort(
            key=lambda x: (x['path'],
                           x['filename'],
                           x['requirement']['coderesource']['filename']))
        dump = {'driver': code_resource_revisions[method['driver']],
                'docker_image': docker_images[method.get('docker_image')]}
        for field in ('groups_allowed',
                      'users_allowed',
                      'reusable',
                      'threads',
                      'memory',
                      'dependencies'):
            dump[field] = method[field]
        methods[method['id']] = dump

    used_revisions = set()
    pipeline_wrapper = kive.get_pipeline(pipeline_id)
    pipeline = pipeline_wrapper.details
    print('Dumping {} in {}.'.format(pipeline_wrapper, dump_folder))
    dump = dict(positions=dict(inputs={},
                               outputs={},
                               steps={}))
    for input_item in pipeline['inputs']:
        input_name = input_item['dataset_name']
        dump['positions']['inputs'][input_name] = dict(x=input_item['x'],
                                                       y=input_item['y'])
        del input_item['x']
        del input_item['y']
        replace_structure(input_item, compound_datatypes)
    dump['inputs'] = pipeline['inputs']
    for output_item in pipeline['outputs']:
        output_name = output_item['dataset_name']
        dump['positions']['outputs'][output_name] = dict(x=output_item['x'],
                                                         y=output_item['y'])
        del output_item['x']
        del output_item['y']
        del output_item['dataset_idx']
        replace_structure(output_item, compound_datatypes)
    pipeline['outputs'].sort()
    dump['outputs'] = pipeline['outputs']
    for outcable in pipeline['outcables']:
        del outcable['pk']
        del outcable['source']
        if outcable['output_cdt']:
            columns = compound_datatypes[outcable['output_cdt']]
            outcable['output_cdt'] = columns
    pipeline['outcables'].sort(key=itemgetter('output_idx'))
    dump['outcables'] = pipeline['outcables']
    for step in pipeline['steps']:
        step_name = step['name']
        dump['positions']['steps'][step_name] = dict(x=step['x'], y=step['y'])
        del step['x']
        del step['y']
        step['cables_in'].sort(key=itemgetter('dest_dataset_name'))
        for cable in step['cables_in']:
            del cable['dest']
            del cable['source']
        for input_item in step['inputs']:
            replace_structure(input_item, compound_datatypes)
        for output_item in step['outputs']:
            replace_structure(output_item, compound_datatypes)
        del step['transformation_family']
        step['transformation'] = methods[step['transformation']]
        driver = step['transformation']['driver']
        if driver is not None:
            used_revisions.add(driver)
        used_revisions.update(map(itemgetter('requirement'),
                                  step['transformation']['dependencies']))
    dump['steps'] = pipeline['steps']

    pipeline_filename = 'pipeline.json'
    with open(os.path.join(dump_folder, pipeline_filename), 'w') as f:
        json.dump(dump, f, indent=4, sort_keys=True)

    pipeline_deadline = datetime.now() + timedelta(seconds=90)
    filename_counts = Counter()
    for revision in used_revisions:
        filename = revision['coderesource']['filename']
        filename_counts[filename] += 1
        response = kive.get(revision.url, is_json=False, stream=True)
        deadline = max(pipeline_deadline,
                       datetime.now() + timedelta(seconds=10))
        is_complete = True
        with open(os.path.join(dump_folder, filename), 'w') as f:
            for block in response.iter_content():
                f.write(block)
                if datetime.now() > deadline:
                    is_complete = False
                    break
        if not is_complete:
            os.remove(os.path.join(dump_folder, filename))
            with open(os.path.join(dump_folder, filename + '_timed_out'), 'w'):
                pass
    duplicate_filenames = [filename
                           for filename, count in filename_counts.items()
                           if count > 1]
    if duplicate_filenames:
        raise RuntimeError('Multiple versions found: ' +
                           ', '.join(duplicate_filenames))

    print('Dumped {}.'.format(pipeline_wrapper))


class CodeResourceRevision(dict):
    def __init__(self, data, code_resources):
        super(CodeResourceRevision, self).__init__()
        for field in ('groups_allowed',
                      'users_allowed',
                      'MD5_checksum'):
            self[field] = data[field]
        self['coderesource'] = code_resources[data['coderesource']]
        self.id = data['id']
        self.url = data['download_url']

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return self.id == other.id


def replace_structure(item, compound_datatypes):
    structure = item['structure']
    if structure:
        columns = compound_datatypes[structure['compounddatatype']]
        structure['compounddatatype'] = columns


main()
