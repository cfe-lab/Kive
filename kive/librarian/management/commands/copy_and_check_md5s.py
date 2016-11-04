""" Copy datasets and check MD5's. Makes no changes to the database. """
import random
import logging
import os
import shutil
import sys
from time import sleep
from traceback import format_exc

from django.core.management.base import BaseCommand
from mpi4py import MPI

from file_access_utils import sandbox_base_path, compute_md5
from fleet.workers import adjust_log_files
from librarian.models import Dataset

logger = logging.getLogger('copy_and_check_md5s')


class Command(BaseCommand):
    help = "Trying to reproduce issue #550 by copying datasets and checking MD5's."

    def add_arguments(self, parser):
        parser.add_argument("-n",
                            "--count",
                            type=int,
                            help="number of workers to launch",
                            default=0)
        parser.add_argument("-w", "--worker", action="store_true", help="run as a worker")
        parser.add_argument("-t",
                            "--target",
                            type=int,
                            help="target load, or number of simultaneous copies",
                            default=1)
        parser.add_argument("-l", "--limit", type=int, help="limit of files in each sandbox", default=10)
        parser.add_argument("-q",
                            "--query",
                            type=int,
                            help="datasets to query, negative for no limit",
                            default=-1)
        parser.add_argument("-p", "--pattern", help="file name pattern in datasets")

    def handle(self, *args, **options):
        worker_count = options["count"]
        if worker_count:
            adjust_log_files(logger, 999)
            logger.info('Launching %d workers.', worker_count)
            comm = self.launch_workers(worker_count, options)
            logger.info('Launched %d workers.', worker_count)
        elif options["worker"]:
            self.run_worker(options["limit"])
            return
        else:
            raise RuntimeError('Did not specify either worker or count options.')

        try:
            idle_ranks = []
            while len(idle_ranks) < worker_count:
                self.receive_and_log(comm, idle_ranks)

            target_load = options["target"]
            for dataset_id in self.find_datasets(options["query"]):
                while len(idle_ranks) - 1 < worker_count - target_load:
                    self.receive_and_log(comm, idle_ranks)
                rank = idle_ranks.pop(random.randrange(len(idle_ranks)))
                comm.send(dataset_id, dest=rank)
        except KeyboardInterrupt:
            pass
        logger.info('Stopping.')

    def receive_and_log(self, comm, idle_ranks):
        """ Wait for a message, then log it and record the source rank.

        :param comm: MPI communicator
        :param list idle_ranks: will have the source rank added to it
        """

        status = MPI.Status()
        logger.debug('Manager waiting to receive.')
        level, message = self.polling_receive(comm, status)
        logger.log(level, message)
        idle_ranks.append(status.source)

    def polling_receive(self, comm, status=None, source=MPI.ANY_SOURCE):
        # Set this to 0 for maximum responsiveness, but that will peg CPU to 100%
        sleep_seconds = 0.1
        if sleep_seconds > 0:
            while not comm.Iprobe(source=source):
                sleep(sleep_seconds)

        return comm.recv(source=source, status=status)

    def create_sandbox(self, rank):
        sandbox_path = os.path.join(sandbox_base_path(),
                                    'copy_and_check_md5s_{}'.format(rank))
        shutil.rmtree(sandbox_path, ignore_errors=True)
        os.makedirs(sandbox_path)
        return sandbox_path

    def find_datasets(self, query_limit):
        """ Find all datasets, and yield them in an endless loop. """
        datasets = Dataset.objects.exclude(dataset_file='', external_path='')
        datasets = datasets.order_by('-id').values_list('id', flat=True)
        if query_limit >= 0:
            datasets = datasets[:query_limit]
        while True:
            for dataset_id in datasets:
                yield dataset_id
            if datasets.count() == 0:
                raise RuntimeError('No datasets found.')

    def check_dataset(self, dataset, sandbox_path, host):
        """ Copy a dataset file, check the MD5, and return a report.

        :return: log_level, message
        """
        dest_filename = os.path.join(sandbox_path,
                                     'ds_{}_{}'.format(dataset.id, dataset.name))

        if dataset.dataset_file:
            source_filename = dataset.dataset_file.path
        else:
            source_filename = dataset.external_absolute_path()

        if not os.path.exists(source_filename):
            return logging.ERROR, 'Dataset file missing: {!r}'.format(source_filename)

        shutil.copyfile(source_filename, dest_filename)
        with open(dest_filename, "rb") as f:
            new_md5 = compute_md5(f)

        if new_md5 != dataset.MD5_checksum:
            message = 'MD5 check failed on {}, dataset id {}: {!r} expected {}, but was {}.'.format(
                host,
                dataset.id,
                source_filename,
                dataset.MD5_checksum,
                new_md5)
            return logging.ERROR, message
        return logging.DEBUG, 'MD5 matched for dataset id {}.'.format(dataset.id)

    def purge_trash(self, sandbox_path, limit):
        filenames = os.listdir(sandbox_path)
        if len(filenames)+1 > limit:
            filepaths = (os.path.join(sandbox_path, filename) for filename in filenames)
            filestats = sorted((os.stat(path).st_mtime, path) for path in filepaths)
            for _mtime, filename in filestats[:len(filestats)+1-limit]:
                os.remove(os.path.join(sandbox_path, filename))

    # noinspection PyArgumentList
    def launch_workers(self, worker_count, options):
        manage_script = sys.argv[0]
        spawn_args = [manage_script,
                      "copy_and_check_md5s",
                      "--worker",
                      "--limit", str(options["limit"])]
        mpi_info = MPI.Info.Create()
        mpi_info.Set("add-hostfile", "kive/hostfile")
        comm = MPI.COMM_SELF.Spawn(sys.executable,
                                   args=spawn_args,
                                   maxprocs=worker_count,
                                   info=mpi_info).Merge()
        return comm

    # noinspection PyArgumentList
    def run_worker(self, limit):
        manager_rank = 0
        comm = MPI.Comm.Get_parent().Merge()
        host = MPI.Get_processor_name()
        rank = comm.Get_rank()
        sandbox_path = self.create_sandbox(rank)
        result = (logging.INFO, 'Worker {} started on {}.'.format(rank, host))
        while True:
            comm.send(result, dest=manager_rank)
            dataset_id = self.polling_receive(comm, source=manager_rank)
            try:
                dataset = Dataset.objects.get(id=dataset_id)
                self.purge_trash(sandbox_path, limit)
                self.check_dataset(dataset, sandbox_path, host)
                result = (logging.DEBUG, 'Checked dataset id {}.'.format(dataset_id))
            except StandardError:
                result = (logging.ERROR, format_exc())
