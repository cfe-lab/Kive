import logging
import os
from argparse import ArgumentParser
from csv import DictWriter
from io import BytesIO
from random import randint
from time import sleep

from kiveapi import KiveAPI


def create_dataset(session, filename, cdt):
    f = BytesIO()
    writer = DictWriter(f, ['x', 'y'], lineterminator=os.linesep)
    writer.writeheader()
    for _ in range(10):
        writer.writerow(dict(x=randint(0, 100), y=randint(0, 100)))
    f.seek(0)
    dataset = session.add_dataset(filename,
                                  "generated during stress test",
                                  f,
                                  cdt=cdt)
    return dataset


def parse_args():
    parser = ArgumentParser(description='Generate many runs.')
    parser.add_argument('--user', '-u', help='user to connect with Kive')
    parser.add_argument('--password', '-p', help='password to connect with Kive')
    parser.add_argument('--pipeline', '-l', type=int, help='pipeline id')
    parser.add_argument('--cdt', '-c', type=int, help='compound datatype id')
    parser.add_argument('--max_active',
                        '-x',
                        type=int,
                        default=100,
                        help='maximum active runs')
    parser.add_argument('--datasets',
                        '-d',
                        type=int,
                        default=1000,
                        help='target number of datasets')
    parser.add_argument('--runs',
                        '-r',
                        type=int,
                        default=1000,
                        help='target number of runs')
    return parser.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s:%(levelname)s:%(name)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARN)
    logging.info('Starting.')
    session = KiveAPI("http://localhost:8000")
    session.login("kive", "kive")
    cdt = session.get_cdt(args.cdt)
    pipeline = session.get_pipeline(args.pipeline)
    response = session.get('/api/datasets/?filters[0][key]=uploaded&page_size=1', is_json=True)
    dataset_count = response.json()['count']
    response = session.get('/api/runs/?page_size=1', is_json=True)
    run_count = response.json()['count']
    while dataset_count < args.datasets or run_count < args.runs:
        dataset_count += 1
        filename = 'pairs_{}.csv'.format(dataset_count)
        dataset = create_dataset(session, filename, cdt)
        session.run_pipeline(pipeline, [dataset])
        run_count += 1
        while True:
            response = session.get('/api/runs/status/?filters[0][key]=active&page_size=1')
            active_count = response.json()['count']
            if active_count < args.max_active:
                break
            sleep(5)
        logging.info('%d datasets, %d runs', dataset_count, run_count)


main()
