from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from itertools import count
from logging import basicConfig, getLogger, DEBUG, WARN

import os

from collections import defaultdict
from time import sleep

from kiveapi import KiveAPI
from requests import HTTPError

basicConfig(level=DEBUG,
            format="%(asctime)s[%(levelname)s]%(name)s:%(message)s")
getLogger('requests').level = WARN
logger = getLogger(__name__)


def parse_args():
    parser = ArgumentParser(
        description='Purge datasets through the API, and launch reruns.',
        formatter_class=ArgumentDefaultsHelpFormatter,
        epilog='See also: kive/archive/management/commands/force_reruns.py')
    parser.add_argument(
        "--run_ids",
        type=int_list,
        help='comma-separated list of runs to purge and rerun')
    parser.add_argument(
        "--batch_ids",
        type=int_list,
        help='comma-separated list of run batches to purge and rerun')
    parser.add_argument(
        "--pipeline_ids",
        type=int_list,
        default=[],
        help='comma-separated list of pipeline ids to search for runs')
    parser.add_argument(
        "--skip_pipeline_ids",
        type=int_list,
        default=[],
        help='comma-separated list of pipeline ids to avoid')
    parser.add_argument(
        "--runs_per_pipeline",
        type=int,
        default=10,
        help='number of runs to purge for each pipeline, chosen randomly')
    parser.add_argument(
        "--active_count",
        type=int,
        default=50,
        help='number of runs active at one time')
    parser.add_argument(
        "--outputs",
        type=str_list,
        default=[],
        help='comma-separated list of output names to purge')
    parser.add_argument(
        "--no_launch",
        action='store_true',
        help='do not launch new runs')
    return parser.parse_args()


def str_list(arg):
    return arg.split(',')


def int_list(arg):
    return [int(s) for s in str_list(arg)]


def find_pipelines(kive, skip_pipeline_ids):
    skip_pipeline_ids = set(skip_pipeline_ids)
    for page_num in count(1):
        page = kive.get('@api_pipelines', params=dict(page_size=25,
                                                      page=page_num)).json()
        for pipeline in page['results']:
            pipeline_id = pipeline['id']
            if pipeline_id not in skip_pipeline_ids:
                yield pipeline_id
        if not page['next']:
            break


def main():
    args = parse_args()
    logger.info('Starting.')
    kive_server = os.environ.get('KIVE_SERVER', 'http://localhost:8000')
    kive_user = os.environ.get('KIVE_USER', 'kive')
    kive_password = os.environ.get('KIVE_PASSWORD', 'kive')
    kive = KiveAPI(server=kive_server)
    kive.login(kive_user, kive_password)
    purged_dataset_ids = set()
    launched_run_ids = set()
    if args.run_ids:
        runs = find_runs_by_id(kive, args.run_ids)
    elif args.batch_ids:
        runs = find_runs_by_batch(kive, args.batch_ids)
    else:
        if args.pipeline_ids:
            pipeline_ids = args.pipeline_ids
        else:
            pipeline_ids = find_pipelines(kive, args.skip_pipeline_ids)
        runs = find_runs_by_pipeline(kive, pipeline_ids, args.runs_per_pipeline)
    batch = None
    current_pipeline_id = None
    for run in runs:
        monitor_progress(kive, args.active_count, launched_run_ids)

        run_outputs = kive.get(run['run_outputs']).json()
        logger.debug('Rerunning %d: %s', run['id'], run['display_name'])
        for run_output in run_outputs['output_summary']:
            if run_output['id'] in purged_dataset_ids:
                # Already purged it for a previous run, don't break that run!
                continue
            if args.outputs and run_output['name'] not in args.outputs:
                # Haven't been asked to purge this output.
                continue
            if run_output['type'] != 'dataset':
                continue
            if not run_output['url']:
                # This output has already been purged.
                continue
            purged_dataset_ids.add(run_output['id'])

            patch_result = kive.patch(run_output['url'],
                                      json=dict(dataset_file=None),
                                      headers={'Content-Type': 'application/json',
                                               'X-CSRFToken': kive.csrf_token})
            raise_for_status_from_json(patch_result)

        if args.no_launch:
            continue

        run_inputs = [dict(dataset=run_input['id'],
                           index=i)
                      for i, run_input in enumerate(run_outputs['input_summary'],
                                                    1)]
        pipeline_id = run['pipeline']
        if pipeline_id != current_pipeline_id:
            pipeline = kive.get_pipeline(pipeline_id)
            batch = kive.create_run_batch(name=str(pipeline),
                                          description='Rerun',
                                          users=[],
                                          groups=['Everyone'])
            current_pipeline_id = pipeline_id
        params = dict(
            pipeline=pipeline_id,
            inputs=run_inputs,
            name=run['name'],
            users_allowed=run['users_allowed'],
            groups_allowed=run['groups_allowed'],
            runbatch=batch.id)
        # noinspection PyBroadException
        try:
            response = kive.post('@api_runs', json=params, is_json=True)
            response_json = response.json()
            launched_run_ids.add(response_json['id'])
        except Exception:
            logger.error('Failed to rerun run id %d: %r.',
                         run['id'],
                         run,
                         exc_info=True)
    logger.info('Done.')


def monitor_progress(kive, active_count, launched_run_ids):
    while True:
        active_runs = kive.get(
            '/api/runs/status/?filters[0][key]=active').json()
        unfinished_run_ids = {run['id']
                              for run in active_runs
                              if not run['end_time']}
        if len(unfinished_run_ids) < active_count:
            break
        sleep(10)
    finished_run_ids = launched_run_ids - unfinished_run_ids
    for run_id in finished_run_ids:
        if not is_run_successful(kive, run_id):
            logger.error('Run id %d failed.', run_id)
        launched_run_ids.remove(run_id)


def raise_for_status_from_json(response):
    try:
        response.raise_for_status()
    except HTTPError as ex:
        response_json = response.json()
        message = response_json.get('detail')
        errors = response_json.get('errors')
        if errors:
            errors = '; '.join(errors)
            if message:
                message += errors
            else:
                message = errors
        if message:
            message = ex.args[0] + ' - ' + message
            ex.args = (message,)
        raise


def find_runs_by_id(kive, run_ids):
    for run_id in run_ids:
        run = kive.get_run(run_id)
        yield run.raw


def are_inputs_public(kive, input_ids):
    for input_id in input_ids:
        input_dataset = kive.get_dataset(input_id)
        if 'Everyone' not in input_dataset.groups_allowed:
            return False
    return True


def is_run_successful(kive, run_id):
    response = kive.get('/api/runs/{}/run_status/'.format(run_id))
    raise_for_status_from_json(response)
    run_status = response.json()
    status_display = run_status['status']
    return '!' not in status_display and status_display != 'CANCELLED'


def find_runs_by_pipeline(kive, pipeline_ids, run_count):
    launched_input_ids = defaultdict(set)  # {family_url: {input_id}}
    for pipeline_id in pipeline_ids:
        # We run into trouble when two versions of a pipeline run at the
        # same time and try to restore the same dataset at the same time.
        pipeline = kive.get_pipeline(pipeline_id)
        family_input_ids = launched_input_ids[pipeline.details['family_url']]
        instances_url = pipeline.details['pipeline_instances']
        pipeline_run_count = 0
        for page_num in count(1):
            page = kive.get(instances_url,
                            params=dict(page_size=25, page=page_num)).json()
            for run in page['results']:
                input_ids = {run_input['dataset']
                             for run_input in run['inputs']}
                if input_ids - family_input_ids:
                    if (are_inputs_public(kive, input_ids) and
                            is_run_successful(kive, run['id'])):
                        yield run
                        pipeline_run_count += 1
                    family_input_ids |= input_ids
                if pipeline_run_count >= run_count:
                    break
            if not page['next'] or pipeline_run_count >= run_count:
                break
        if pipeline_run_count < run_count:
            logger.warn('Only found %d runs for pipeline %s.',
                        pipeline_run_count,
                        pipeline)


def find_runs_by_batch(kive, batch_ids):
    for batch_id in batch_ids:
        for run in kive.find_runs(batch_pk=batch_id):
            yield run.raw


main()
