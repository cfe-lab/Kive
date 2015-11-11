from argparse import ArgumentParser
from collections import Counter
import errno
import json
import logging
import os

from requests.adapters import HTTPAdapter

from kiveapi import KiveAPI


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARN)
    CONFIG_FILE = os.path.expanduser("~/.dump_pipeline.config")
    UNSET = '***'

    parser = ArgumentParser(description='Dump a Kive pipeline')
    parser.add_argument('pipeline_id', type=int)
    parser.add_argument('--folder', '-f', default='dump')
    args = parser.parse_args()

    try:
        with open(CONFIG_FILE, 'rU') as f:
            config = json.load(f)
    except IOError as e:
        if e.errno != errno.ENOENT:
            raise
        with open(CONFIG_FILE, 'w') as f:
            config = dict(username=UNSET,
                          password=UNSET,
                          server='http://localhost:8000')
            json.dump(config, f, indent=4)

    if config['username'] == UNSET:
        exit('Set up your configuration in ' + CONFIG_FILE)
    kive = KiveAPI(config['server'])
    kive.mount('https://', HTTPAdapter(max_retries=20))
    kive.login(config['username'], config['password'])
    if not os.path.isdir(args.folder):
        os.mkdir(args.folder)

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
    for revision in code_resource_revisions.itervalues():
        for dependency in revision['dependencies']:
            requirement = code_resource_revisions[dependency['requirement']]
            dependency['requirement'] = requirement
    methods = {}  # {id: method}
    for method in kive.get('/api/methods/').json():
        dump = {'driver': code_resource_revisions[method['driver']]}
        for field in ('groups_allowed',
                      'users_allowed',
                      'reusable',
                      'threads'):
            dump[field] = method[field]
        methods[method['id']] = dump

    used_revisions = set()
    pipeline = kive.get_pipeline(args.pipeline_id).details
    dump = {}
    for input_item in pipeline['inputs']:
        del input_item['x']
        del input_item['y']
        replace_structure(input_item, compound_datatypes)
    dump['inputs'] = pipeline['inputs']
    for output_item in pipeline['outputs']:
        del output_item['x']
        del output_item['y']
        del output_item['dataset_idx']
        replace_structure(output_item, compound_datatypes)
    pipeline['outputs'].sort()
    dump['outputs'] = pipeline['outputs']
    for step in pipeline['steps']:
        del step['x']
        del step['y']
        for cable in step['cables_in']:
            del cable['dest']
            del cable['source']
        for input_item in step['inputs']:
            replace_structure(input_item, compound_datatypes)
        for output_item in step['outputs']:
            replace_structure(output_item, compound_datatypes)
        del step['transformation_family']
        step['transformation'] = methods[step['transformation']]
        step['transformation']['driver'].collect_revisions(used_revisions)
    dump['steps'] = pipeline['steps']

    pipeline_filename = 'pipeline.json'
    with open(os.path.join(args.folder, pipeline_filename), 'w') as f:
        json.dump(dump, f, indent=4, sort_keys=True)

    filename_counts = Counter()
    for revision in used_revisions:
        filename = revision['coderesource']['filename']
        filename_counts[filename] += 1
        response = kive.get(revision.url, is_json=False, stream=True)
        with open(os.path.join(args.folder, filename), 'w') as f:
            for block in response.iter_content():
                f.write(block)
    duplicate_filenames = [filename
                           for filename, count in filename_counts.iteritems()
                           if count > 1]
    if duplicate_filenames:
        raise RuntimeError('Multiple versions found: ' +
                           ', '.join(duplicate_filenames))

    print 'Done.'


class CodeResourceRevision(dict):
    def __init__(self, data, code_resources):
        for field in ('groups_allowed',
                      'users_allowed',
                      'MD5_checksum',
                      'dependencies'):
            self[field] = data[field]
        self['coderesource'] = code_resources[data['coderesource']]
        self.id = data['id']
        self.url = data['download_url']

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return self.id == other.id

    def collect_revisions(self, used_revisions):
        """ Collect all the related code resource revisions, including self.

        @param used_revisions: a set of resource revision ids
        """
        used_revisions.add(self)
        for dependency in self['dependencies']:
            dependency['requirement'].collect_revisions(used_revisions)


def replace_structure(item, compound_datatypes):
    structure = item['structure']
    if structure:
        columns = compound_datatypes[structure['compounddatatype']]
        structure['compounddatatype'] = columns

main()
