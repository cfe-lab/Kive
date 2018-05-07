#!/usr/bin/env python

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import errno
from collections import namedtuple
from glob import glob
from gzip import GzipFile
from logging import basicConfig, getLogger, DEBUG, ERROR
import os
from random import shuffle
from subprocess import check_output, STDOUT
from time import sleep

from itertools import islice

STDOUT_SUFFIX = '_stdout.txt'
basicConfig(level=DEBUG,
            format="%(asctime)s[%(levelname)s]%(name)s:%(message)s")
logger = getLogger(__name__)


def parse_args():
    parser = ArgumentParser(
        description='Try reading large files and waiting for completion.',
        formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('source_pattern',
                        help='source files to copy (wildcards allowed)')
    parser.add_argument('target_dir',
                        help='Folder to hold slurm output files')
    # noinspection PyTypeChecker
    parser.add_argument('--min_size',
                        type=int,
                        default=0,
                        help='Minimum size in MB')
    # noinspection PyTypeChecker
    parser.add_argument('--max_size',
                        type=int,
                        default=1000000,
                        help='Maximum size in MB')
    # noinspection PyTypeChecker
    parser.add_argument('-n',
                        '--num_files',
                        type=int,
                        default=100,
                        help='Number of files to copy')
    # noinspection PyTypeChecker
    parser.add_argument('-p',
                        '--processes',
                        type=int,
                        default=16,
                        help='Number processes to run at the same time')
    parser.add_argument(
        '--read',
        action='store_true',
        help='Read a single gzipped file that matches source_pattern')
    return parser.parse_args()


SourceFile = namedtuple('SourceFile', 'path size is_link')


def find_files(source_pattern,
               min_size,
               max_size):
    file_names = glob(source_pattern)
    shuffle(file_names)
    logger.info('Found %d source files.', len(file_names))
    for i, file_name in enumerate(file_names):
        if i % 1000 == 0:
            logger.debug('Scanned %d files.', i)
        is_link = os.path.islink(file_name)
        file_size = os.path.getsize(file_name)
        if min_size <= file_size <= max_size:
            yield SourceFile(file_name, file_size, is_link)
    logger.debug('Finished scanning.')


def read_file(source_files):
    source_file_list = list(source_files)
    assert len(source_file_list) == 1, source_file_list
    source_file = source_file_list[0]
    with GzipFile(source_file.path) as f:
        i = 0
        for i, line in enumerate(f):
            pass
    print('{} lines'.format(i))


def submit_files(source_files, args):
    input_files = {}  # {job_id: input_file}
    for i, source_file in enumerate(islice(source_files, args.num_files)):
        session_name = 'sandbox{}'.format(i)
        while True:
            job_ids = get_slurm_jobs()
            if len(job_ids) < args.processes:
                break
            sleep(1)
        for job_id in set(input_files) - job_ids:
            old_file = input_files.pop(job_id)
            tear_down(old_file)
        logger.debug('%s, %s', source_file.is_link, source_file.path)
        file_name = os.path.basename(source_file.path)
        target_file = os.path.join(args.target_dir,
                                   session_name + '_' + file_name)
        output_file = target_file + STDOUT_SUFFIX
        response = check_output(["sbatch",
                                 "--mem", "100",
                                 "--output", output_file,
                                 __file__,
                                 source_file.path,
                                 args.target_dir,
                                 "--read"],
                                stderr=STDOUT)
        job_id = int(response.split()[-1])
        input_files[job_id] = target_file
    logger.info('Waiting for jobs to finish.')
    while get_slurm_jobs():
        sleep(1)
    for old_file in input_files.values():
        tear_down(old_file)


def main():
    args = parse_args()
    if args.read:
        logger.setLevel(ERROR)
    logger.info('Scanning %r.', args.source_pattern)
    # noinspection PyBroadException
    try:
        try:
            os.makedirs(args.target_dir)
        except OSError as ex:
            if ex.errno != errno.EEXIST:
                raise
        megabyte_size = 1024*1024
        source_files = find_files(args.source_pattern,
                                  args.min_size * megabyte_size,
                                  args.max_size * megabyte_size)
        if args.read:
            read_file(source_files)
        else:
            submit_files(source_files, args)
        logger.info('Done.')
    except Exception:
        logger.error('Failed.', exc_info=True)


def get_slurm_jobs():
    slurm_queue = check_output(['squeue',
                                '--noheader',
                                '-o',
                                '%i'],
                               stderr=STDOUT)
    job_ids = {int(line) for line in slurm_queue.splitlines()}
    return job_ids


def tear_down(old_file):
    with open(old_file + STDOUT_SUFFIX) as f:
        report = f.read().strip()
        logger.debug('report on %s: %s', old_file, report)
    os.remove(old_file + STDOUT_SUFFIX)


main()
