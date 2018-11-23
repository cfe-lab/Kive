from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, SUPPRESS
import logging
import os
from time import sleep

from kiveapi import KiveAPI
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)


def parse_args():
    parser = ArgumentParser(
        description="Launch runs of the dataset_generator pipeline.",
        formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--kive_server',
        default=os.environ.get('KIVE_SERVER', 'http://localhost:8000'),
        help='server to send runs to')
    parser.add_argument(
        '--kive_user',
        default=os.environ.get('KIVE_USER', 'kive'),
        help='user name for Kive server')
    parser.add_argument(
        '--kive_password',
        default=SUPPRESS,
        help='password for Kive server (default not shown)')
    # noinspection PyTypeChecker
    parser.add_argument(
        '--max_active',
        default=os.environ.get('KIVE_MAX_ACTIVE', '4'),
        type=int,
        help='number of runs active at once')

    args = parser.parse_args()
    if not hasattr(args, 'kive_password'):
        args.kive_password = os.environ.get('KIVE_PASSWORD', 'kive')
    return args


def launch_if_needed(session, args, pipeline, input_dataset):
    runs = session.find_runs(active=True)
    active_count = 0
    for run in runs:
        if run.pipeline_id != pipeline.pipeline_id:
            continue
        if run.raw['end_time'] is not None:
            continue
        active_count += 1
    while active_count < args.max_active:
        run = session.run_pipeline(pipeline,
                                   [input_dataset],
                                   'dataset_generator test')
        logger.info('Started run %d.', run.run_id)
        active_count += 1


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s[%(levelname)s]%(name)s:%(message)s")
    logging.getLogger(
        "requests.packages.urllib3.connectionpool").setLevel(logging.WARNING)
    logging.info('Starting.')

    args = parse_args()
    session = KiveAPI(args.kive_server)
    session.mount('https://', HTTPAdapter(max_retries=20))
    session.login(args.kive_user, args.kive_password)

    runs = session.find_runs(active=True)
    pipeline_id = input_id = None
    for run in runs:
        if 'dataset_generator' in run.raw['display_name']:
            pipeline_id = run.pipeline_id
            input_id = run.raw['inputs'][0]['dataset']
            break
    if pipeline_id is None:
        raise RuntimeError(
            'No active runs found with "dataset_generator" in the name.')
    pipeline = session.get_pipeline(pipeline_id)
    input_dataset = session.get_dataset(input_id)

    while True:
        launch_if_needed(session, args, pipeline, input_dataset)
        sleep(1)


main()
