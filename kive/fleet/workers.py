"""
Defines the manager and the "workers" that manage and carry out the execution of Pipelines.
"""

from collections import defaultdict, deque
import logging
from mpi4py import MPI
import sys
import time
import datetime
import itertools
import os
import glob
import shutil
import threading
import Queue
import socket

from django.db import connection
from django.conf import settings
from django.utils import timezone

import archive.models
from archive.models import Dataset, Run, ExceedsSystemCapabilities

from sandbox.execute import Sandbox, sandbox_glob
from fleet.exceptions import StopExecution

mgr_logger = logging.getLogger("fleet.Manager")
worker_logger = logging.getLogger("fleet.Worker")


def adjust_log_files(target_logger, rank):
    """ Configure a different log file for each worker process.

    Because multiple processes are not allowed to log to the same file, we have
    to adjust the configuration.
    https://docs.python.org/2/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes
    """
    for handler in target_logger.handlers:
        filename = getattr(handler, 'baseFilename', None)
        if filename is not None:
            handler.close()
            fileRoot, fileExt = os.path.splitext(filename)
            handler.baseFilename = '{}.{:03}{}'.format(fileRoot, rank, fileExt)
    if target_logger.parent is not None:
        adjust_log_files(target_logger.parent, rank)


class MPIFleetInterface(object):
    """
    Base class for both MPIManagerInterface and MPIWorkerInterface.
    """
    def __init__(self, comm):
        self.comm = comm

    def get_rank(self):
        return self.comm.Get_rank()

    def get_size(self):
        return self.comm.Get_size()

    @staticmethod
    def get_hostname():
        return MPI.Get_processor_name()


class ThreadFleetInterface(object):
    """
    Base class for both ThreadManagerInterface and ThreadWorkerInterface.
    """
    def get_rank(self):
        raise NotImplementedError()

    def get_size(self):
        raise NotImplementedError()

    @staticmethod
    def get_hostname():
        return socket.gethostname()


class MPIManagerInterface(MPIFleetInterface):
    """
    Object that is used by a Manager to communicate with Workers.

    This handles spawning processes to run Workers.
    """
    def __init__(self, worker_count, manage_script):
        self.worker_count = worker_count
        self.manage_script = manage_script
        mpi_info = MPI.Info.Create()
        mpi_info.Set("add-hostfile", "kive/hostfile")

        spawn_args = [self.manage_script, "fleetworker"]
        super(MPIManagerInterface, self).__init__(
            MPI.COMM_SELF.Spawn(sys.executable,
                                args=spawn_args,
                                maxprocs=self.worker_count,
                                info=mpi_info).Merge()
        )

    def send_task_to_worker(self, task_info, worker_rank):
        self.comm.send(task_info.dict_repr(), dest=worker_rank, tag=Worker.ASSIGNMENT)

    def probe_for_finished_worker(self):
        return self.comm.Iprobe(source=MPI.ANY_SOURCE, tag=Worker.FINISHED)

    def receive_finished(self):
        # This returns the rank of the
        lord_rank, result_pk = self.comm.recv(source=MPI.ANY_SOURCE, tag=Worker.FINISHED)
        return lord_rank, result_pk

    def take_rollcall(self):
        roster = defaultdict(list)
        workers_reported = 0
        workers_expected = self.get_size()
        while workers_reported < workers_expected - 1:
            hostname, rank = self.comm.recv(source=MPI.ANY_SOURCE, tag=Worker.ROLLCALL)
            mgr_logger.info("Worker {} on host {} has reported for duty".format(rank, hostname))
            roster[hostname].append(rank)
            workers_reported += 1

        return roster

    def stop_run(self, foreman):
        """
        Instructs the foreman to stop the task.  Blocks while waiting for a response.
        """
        self.comm.isend("STOP", dest=foreman, tag=Worker.STOP)
        # Either the foreman got the message and ended the task, or it
        # finished the task.
        self.comm.recv(source=foreman, tag=Worker.FINISHED)

    def shut_down_fleet(self):
        for rank in range(self.get_size()):
            if rank != self.get_rank():
                self.comm.send(dest=rank, tag=Worker.SHUTDOWN)
        self.comm.Disconnect()


class ThreadManagerInterface(ThreadFleetInterface):
    def __init__(self, worker_count):
        # Elements of this queue will be 2-tuples (foreman rank, result PK).
        self.finished_queues = [Queue.Queue() for _ in range(worker_count)]

        self.worker_count = worker_count
        self.worker_threads = []
        self.worker_interfaces = [None] * worker_count

        tmi = self

        class WorkerThreadStarter:
            def __init__(self, rank):
                worker_interface = ThreadWorkerInterface(rank=rank, manager_interface=tmi)
                self.worker = Worker(interface=worker_interface)

            def __call__(self, *args, **kwargs):
                self.worker.main_procedure()

        for idx in range(worker_count):
            # Each thread will create a worker that adds itself to self.worker_interfaces.
            worker_thread = threading.Thread(target=WorkerThreadStarter(idx))
            worker_thread.start()
            self.worker_threads.append(worker_thread)

    def send_task_to_worker(self, task_info, worker_rank):
        self.worker_interfaces[worker_rank-1].job_queue.put(
            (task_info.dict_repr(), Worker.ASSIGNMENT),
            block=True
        )

    def probe_for_finished_worker(self):
        for worker_queue in self.finished_queues:
            if not worker_queue.empty():
                return True
        return False

    def receive_finished(self):
        for worker_queue in self.finished_queues:
            if not worker_queue.empty():
                # This looks like (rank, result_pk).
                return worker_queue.get(block=True)

    def get_rank(self):
        return 0

    def get_size(self):
        return self.worker_count

    def take_rollcall(self):
        roster = defaultdict(list)
        for worker_interface in self.worker_interfaces:
            roster[worker_interface.get_hostname()].append(worker_interface.get_rank())
        return roster

    def stop_run(self, foreman):
        foreman.interface.message_queue.put(("STOP", Worker.STOP))
        # Either the foreman got the message and ended the task, or it
        # finished the task.  Either way, we wait for a message from this Worker.
        self.finished_queues[foreman.rank-1].get(block=True)

    def shut_down_fleet(self):
        for worker_interface in self.worker_interfaces:
            worker_interface.job_queue.put(("SHUTDOWN", Worker.SHUTDOWN))
        for thread in self.worker_threads:
            thread.join()


class Manager(object):
    """
    Coordinates the execution of pipelines.

    The manager is responsible for handling new Run requests and
    assigning the resulting tasks to workers.
    """

    def __init__(self, interface, quit_idle=False, history=0):
        self.quit_idle = quit_idle
        self.interface = interface

        # tasks_in_progress tracks what jobs are assigned to what workers:
        # foreman -|--> {"task": task, "vassals": vassals}
        self.tasks_in_progress = {}
        # task_queue is a list of 2-tuples (sandbox, runstep/runcable).
        # We don't use a Queue here because we may need to remove tasks from
        # the queue if their parent Run fails.
        self.task_queue = []
        # A table of currently running sandboxes, indexed by the Run.
        self.active_sandboxes = {}

        # roster will be a dictionary keyed by hostnames whose values are
        # the sets of ranks of processes running on that host.  This will be
        # necessary down the line to help determine which hosts have enough
        # threads to run.
        self.roster = defaultdict(list)

        # A reverse lookup table for the above.
        self.hostnames = {}

        # A queue of recently-completed runs, to a maximum specified by history.
        self.history_queue = deque(maxlen=history)

    def _startup(self):
        """
        Set up/register the workers and prepare to run.

        INPUTS
        roster: a dictionary structured much like a host file, keyed by
        hostnames and containing the number of Workers on each host.
        """
        adjust_log_files(mgr_logger, self.interface.get_rank())
        mgr_logger.info("Manager started on host {}".format(self.interface.get_hostname()))

        self.roster = self.interface.take_rollcall()

        for hostname in self.roster:
            for rank in self.roster[hostname]:
                self.hostnames[rank] = hostname

        self.worker_status = [Worker.READY for _ in range(self.interface.get_size())]
        self.max_host_cpus = max([len(self.roster[x]) for x in self.roster])

    def is_worker_ready(self, rank):
        return self.worker_status[rank] == Worker.READY

    def start_run(self, run_to_start):
        """
        Receive a request to start a pipeline running.
        """
        new_sdbx = Sandbox(run=run_to_start)
        new_sdbx.advance_pipeline()

        # Refresh run_to_start.
        run_to_start = Run.objects.get(pk=run_to_start.pk)

        # If we were able to reuse throughout, then we're totally done.  Otherwise we
        # need to do some bookkeeping.
        finished_already = False
        if run_to_start.is_complete(use_cache=True):
            mgr_logger.info('Run "%s" completely reused (Pipeline: %s, User: %s)',
                            run_to_start, run_to_start.pipeline, run_to_start.user)
            finished_already = True

        elif not run_to_start.is_successful(use_cache=True):
            # The run failed somewhere in reuse.  This hasn't affected any of our maps yet, so we
            # just report it and discard it.
            mgr_logger.info('Run "%s" (pk=%d) (Pipeline: %s, User: %s) failed on reuse',
                            run_to_start, run_to_start.pk, run_to_start.pipeline, run_to_start.user)
            run_to_start.mark_complete()
            finished_already = True

        else:
            self.active_sandboxes[run_to_start] = new_sdbx
            for task in new_sdbx.hand_tasks_to_fleet():
                self.task_queue.append((new_sdbx, task))

        if finished_already:
            run_to_start.stop(save=True)
            run_to_start.complete_clean(use_cache=True)
            if self.history_queue.maxlen > 0:
                self.history_queue.append(new_sdbx)

        return new_sdbx

    def mop_up_terminated_sandbox(self, sandbox):
        """
        Remove all tasks coming from the specified sandbox from the work queue
        and mark them as cancelled.
        """
        new_task_queue = []
        for task_sdbx, task in self.task_queue:
            if task_sdbx != sandbox:
                new_task_queue.append((task_sdbx, task))
            else:
                task.is_cancelled = True
                task.save()

        self.task_queue = new_task_queue

    def assign_task(self, sandbox, task):
        """
        Assign a task to a worker.
        """
        task_info = sandbox.get_task_info(task)

        # First, we find a host that is capable of taking on the task.
        candidate_hosts = self.roster.keys()

        # If we have no hosts that are capable of handling this many threads,
        # we blow up.
        if task_info.threads_required > self.max_host_cpus:
            mgr_logger.info(
                "Task %s requested %d threads but there are only %d workers.  Terminating parent run (%s).",
                task, task_info.threads_required, self.max_host_cpus, task.top_level_run)
            task.not_enough_CPUs.create(threads_requested=task_info.threads_required,
                                        max_available=self.max_host_cpus)
            self.mop_up_terminated_sandbox(sandbox)
            return

        while True:
            for host in candidate_hosts:
                workers_available = [x for x in self.roster[host] if self.is_worker_ready(x)]
                # If there are enough workers available to start the task, then have at it.
                if len(workers_available) >= task_info.threads_required:
                    # We're going to assign the task to workers on this host.
                    team = [workers_available[i] for i in range(task_info.threads_required)]
                    mgr_logger.debug("Assigning task {} to worker(s) {}".format(task, team))

                    # Send the job to the "lord":
                    self.interface.send_task_to_worker(task_info, team[0])
                    vassals = team[1:len(team)]
                    self.tasks_in_progress[team[0]] = {
                        "task": task,
                        "vassals": vassals
                    }
                    self.worker_status[team[0]] = Worker.LORD
                    # Denote the other workers as "vassals".
                    for worker_rank in vassals:
                        self.worker_status[worker_rank] = Worker.VASSAL
                    return

            # Having reached this point, we know that no host was capable of taking on the task.
            # We block and wait for a worker to become ready, so we can try again.
            mgr_logger.debug("Waiting for host to become ready....")
            while not self.interface.probe_for_finished_worker():
                time.sleep(settings.SLEEP_SECONDS)
            lord_rank, result_pk = self.interface.receive_finished()

            # Note the task that just finished, and release its workers.
            # If this fails it will throw an exception.
            self.worker_finished(lord_rank, result_pk)

            source_host = self.hostnames[lord_rank]
            candidate_hosts = [source_host]

            # The task that returned may have belonged to the same sandbox, and
            # failed.  If so, we should cancel this task.
            if sandbox.run not in self.active_sandboxes:
                mgr_logger.debug("Run has been terminated; abandoning this task.")
                return

    def note_progress(self, lord_rank, task_finished):
        """
        Perform bookkeeping for a task that has just finished.
        """
        # Mark this task as having finished.
        just_finished = self.tasks_in_progress.pop(lord_rank)
        curr_sdbx = self.active_sandboxes[task_finished.top_level_run]
        task_execute_info = curr_sdbx.get_task_info(task_finished)

        workers_freed = [lord_rank] + just_finished["vassals"]
        for worker_rank in workers_freed:
            self.worker_status[worker_rank] = Worker.READY

        # Is anything from the run still processing?  If this was a recovery, is anything else
        # recovering for the invoking record?
        tasks_currently_running = False
        task_from_same_recovery = False

        curr_run_tasks = []
        for task_info in self.tasks_in_progress.itervalues():
            if task_info['task'].top_level_run == just_finished['task'].top_level_run:
                curr_run_tasks.append(task_info['task'])
                tasks_currently_running = True

                if task_execute_info.is_recovery():
                    if task_info['task'].recovering_record == task_execute_info.recovering_record:
                        task_from_same_recovery = True

        # If this run has failed (either due to this task or another),
        # we mop up.
        clean_up_now = False
        curr_sdbx.run = Run.objects.get(pk=curr_sdbx.run.pk)
        if not curr_sdbx.run.is_successful(use_cache=True):
            self.mop_up_terminated_sandbox(curr_sdbx)
            if not task_finished.is_successful(use_cache=True):
                mgr_logger.info('Task %s (pk=%d) of run "%s" (pk=%d) (Pipeline: %s, User: %s) failed.',
                                task_finished, task_finished.pk, curr_sdbx.run, curr_sdbx.run.pk,
                                curr_sdbx.pipeline, curr_sdbx.user)

            task_finished.failed_mark_complete(curr_run_tasks)

            if task_execute_info.is_recovery():
                recovering_task = archive.models.RunComponent.objects.get(
                    pk=task_execute_info.recovering_record.pk
                ).definite
                if not recovering_task.is_successful(use_cache=True) and not task_from_same_recovery:
                    recovering_task.failed_mark_complete(curr_run_tasks)
                    recovering_task.save()

            if not tasks_currently_running:
                clean_up_now = True

        else:
            # Was this task a recovery or novel progress?
            if task_execute_info.is_recovery():
                execrecordouts = task_execute_info.execrecord.execrecordouts.all()
                data_newly_available = [execrecordout.dataset
                                        for execrecordout in execrecordouts]
                # Add anything that was waiting on this recovery to the queue.
                curr_sdbx.enqueue_runnable_tasks(data_newly_available)
            else:
                # Update maps and advance the pipeline.
                curr_sdbx.update_sandbox(task_finished)
                curr_sdbx.advance_pipeline(task_completed=just_finished["task"])
                curr_sdbx.run = Run.objects.get(pk=curr_sdbx.run.pk)
                if curr_sdbx.run.is_complete(use_cache=True):
                    mgr_logger.info('Rest of Run "%s" (pk=%d) completely reused (Pipeline: %s, User: %s)',
                                    curr_sdbx.run, curr_sdbx.run.pk, curr_sdbx.pipeline, curr_sdbx.user)
                    if not tasks_currently_running:
                        clean_up_now = True

                elif not curr_sdbx.run.is_successful(use_cache=False):
                    # The task that just finished was unsuccessful.  We mop up the sandbox.
                    self.mop_up_terminated_sandbox(curr_sdbx)
                    if not tasks_currently_running:
                        clean_up_now = True


            if not clean_up_now:
                # The Run is still going and there may be more stuff to do.
                for task in curr_sdbx.hand_tasks_to_fleet():
                    self.task_queue.append((curr_sdbx, task))

        if clean_up_now:
            if not curr_sdbx.run.is_successful(use_cache=True):
                mgr_logger.info('Cleaning up failed run "%s" (pk=%d) (Pipeline: %s, User: %s)',
                                curr_sdbx.run, curr_sdbx.run.pk, curr_sdbx.pipeline, curr_sdbx.user)

            finished_sandbox = self.active_sandboxes.pop(curr_sdbx.run)
            if self.history_queue.maxlen > 0:
                self.history_queue.append(finished_sandbox)

            curr_sdbx.run.mark_complete(mark_all_components=not curr_sdbx.run.is_successful(use_cache=True))
            curr_sdbx.run.stop(save=True)
            curr_sdbx.run.complete_clean(use_cache=True)

            if curr_sdbx.run.is_successful(use_cache=True):
                mgr_logger.info('Finished successful run "%s" (pk=%d) (Pipeline: %s, User: %s)',
                                curr_sdbx.run, curr_sdbx.run.pk, curr_sdbx.pipeline, curr_sdbx.user)

        return workers_freed

    def main_procedure(self):
        try:
            self._startup()
            self.main_loop()
            mgr_logger.info("Manager shutting down.")
        except:
            mgr_logger.error("Manager failed.", exc_info=True)
        self.interface.shut_down_fleet()

    def worker_finished(self, lord_rank, result_pk):
        """Handle bookkeeping when a worker finishes."""

        if result_pk == Worker.FAILURE:
            raise WorkerFailedException("Worker {} reports a failed task (PK {})".format(
                lord_rank,
                self.tasks_in_progress[lord_rank]["task"].pk
            ))

        mgr_logger.info(
            "Worker %d reports task with PK %d is finished",
            lord_rank, result_pk)

        task_finished = archive.models.RunComponent.objects.get(pk=result_pk).definite
        self.note_progress(lord_rank, task_finished)

    def main_loop(self):
        """
        Poll the database for new jobs, and handle running of sandboxes.
        """
        time_to_purge = None
        while True:
            self.find_stopped_runs()

            time_to_poll = time.time() + settings.FLEET_POLLING_INTERVAL
            if not self.assign_tasks(time_to_poll):
                return

            # Everything in the queue has been started, so we check and see if
            # anything has finished.
            if not self.wait_for_polling(time_to_poll):
                return

            self.find_new_runs()
            if time_to_purge is None or time_to_poll > time_to_purge:
                self.purge_sandboxes()
                Dataset.purge()
                time_to_purge = time_to_poll + settings.FLEET_PURGING_INTERVAL

            if self.quit_idle and not self.active_sandboxes:
                mgr_logger.info('Fleet is idle, quitting.')
                return

    def assign_tasks(self, time_to_poll):
        # We can't use a for loop over the task queue because assign_task
        # may add to the queue.
        while len(self.task_queue) > 0 and time.time() < time_to_poll:
            # task_queue entries are (sandbox, run_step)
            self.task_queue.sort(key=lambda entry: entry[0].run.start_time)
            curr_task = self.task_queue[0]  # looks like (sandbox, task)
            task_sdbx = self.active_sandboxes[curr_task[1].top_level_run]
            # We assign this task to a worker, and do not proceed until the task
            # is assigned.
            try:
                self.assign_task(task_sdbx, curr_task[1])
            except WorkerFailedException as e:
                mgr_logger.error(e.error_msg)
                return False
            self.task_queue = self.task_queue[1:]
        return True

    def wait_for_polling(self, time_to_poll):
        while time.time() < time_to_poll:
            if self.interface.probe_for_finished_worker():
                lord_rank, result_pk = self.interface.receive_finished()
                try:
                    self.worker_finished(lord_rank, result_pk)
                    break
                except WorkerFailedException as e:
                    mgr_logger.error(e.error_msg)
                    return False

            try:
                time.sleep(settings.SLEEP_SECONDS)
            except KeyboardInterrupt:
                return False
        return True

    def find_new_runs(self):
        # Look for new jobs to run.  We will also
        # build in a delay here so we don't clog up the database.
        mgr_logger.debug("Looking for new runs....")

        pending_runs = Run.find_unstarted().order_by("time_queued").filter(parent_runstep__isnull=True)

        mgr_logger.debug("Pending runs: {}".format(pending_runs))

        for run_to_process in pending_runs:
            threads_needed = run_to_process.pipeline.threads_needed()
            if threads_needed > self.max_host_cpus:
                mgr_logger.info(
                    "Cannot run Pipeline %s for user %s: %d threads required, %d available",
                    run_to_process.pipeline, run_to_process.user, threads_needed,
                    self.max_host_cpus)
                esc = ExceedsSystemCapabilities(
                    run=run_to_process,
                    threads_requested=threads_needed,
                    max_available=self.max_host_cpus
                )
                esc.save()
                run_to_process.clean()
                continue

            self.start_run(run_to_process)
            mgr_logger.info("Started run id %d, pipeline %s, user %s",
                            run_to_process.pk,
                            run_to_process.pipeline,
                            run_to_process.user)

            mgr_logger.debug("Task queue: {}".format(self.task_queue))
            mgr_logger.debug("Active sandboxes: {}".format(self.active_sandboxes))

    def find_stopped_runs(self):
        """
        Look for currently running Runs that have been stopped by a user.
        """
        mgr_logger.debug("Looking for stopped runs....")
        just_stopped_runs = Run.objects.filter(end_time__isnull=True, stopped_by__isnull=False)

        for run_to_stop in just_stopped_runs:
            self.stop_run(run_to_stop)

    def stop_run(self, run):
        """
        Stop the specified run.
        """
        if run not in self.active_sandboxes:
            # This hasn't started yet, so we can just skip this one.
            mgr_logger.debug("Run (pk={}) has not started yet so cannot be stopped".format(run.pk, run.stopped_by))
            return

        mgr_logger.debug("Stopping run (pk={}) on behalf of user {}".format(run.pk, run.stopped_by))

        sandbox_to_end = self.active_sandboxes[run]

        # Send a message to the foreman in charge of running this task.
        foreman_found = False
        for foreman in self.tasks_in_progress:
            if self.tasks_in_progress[foreman]["task"].top_level_run == run:
                foreman_found = True
                break

        if foreman_found:
            self.interface.stop_run(foreman)

            self.worker_status[foreman] = Worker.READY
            for worker_rank in self.tasks_in_progress[foreman]["vassals"]:
                self.worker_status[worker_rank] = Worker.READY

        # Cancel all tasks on the task queue pertaining to this run.
        self.mop_up_terminated_sandbox(sandbox_to_end)
        run.stop(save=True)

        mgr_logger.debug("Run (pk={}) stopped by user {}".format(run.pk, run.stopped_by))

    @staticmethod
    def purge_sandboxes():
        # Next, look for finished jobs to clean up.
        mgr_logger.debug("Checking for old sandboxes to clean up....")

        purge_interval = datetime.timedelta(days=settings.SANDBOX_PURGE_DAYS,
                                            hours=settings.SANDBOX_PURGE_HOURS,
                                            minutes=settings.SANDBOX_PURGE_MINUTES)
        keep_recent = settings.SANDBOX_KEEP_RECENT

        purge_candidates = Run.objects.filter(
            end_time__isnull=False,
            end_time__lte=timezone.now()-purge_interval,
            purged=False
        )

        # Retain the most recent ones for each PipelineFamily.
        pfs_represented = purge_candidates.values_list("pipeline__family")

        ready_to_purge = []
        for pf in set(pfs_represented):
            # Look for the oldest ones.
            curr_candidates = purge_candidates.filter(pipeline__family=pf).order_by("end_time")
            num_remaining = curr_candidates.count()

            ready_to_purge = itertools.chain(
                ready_to_purge,
                curr_candidates[:max(num_remaining - keep_recent, 0)]
            )

        for rtp in ready_to_purge:
            mgr_logger.debug("Removing sandbox at %r.", rtp.sandbox_path)
            try:
                rtp.collect_garbage()
            except:
                mgr_logger.error('Failed to purge sandbox at %r.',
                                 rtp.sandbox_path,
                                 exc_info=True)
                rtp.purged = True  # Don't try to purge it again.
                rtp.save()

        # Next, look through the sandbox directory and see if there are any orphaned sandboxes
        # to remove.
        mgr_logger.debug("Checking for orphaned sandbox directories to clean up....")

        sdbx_path = os.path.join(settings.MEDIA_ROOT, settings.SANDBOX_PATH)
        for putative_sdbx in glob.glob(os.path.join(sdbx_path, sandbox_glob)):

            # Remove this sandbox if there is no Run that is on record as having used it.
            matching_rtps = Run.objects.filter(
                sandbox_path__startswith=putative_sdbx,
            )
            if not matching_rtps.exists():
                try:
                    path_to_rm = os.path.join(sdbx_path, putative_sdbx)
                    shutil.rmtree(path_to_rm)
                except OSError as e:
                    mgr_logger.warning(e)

    @classmethod
    def execute_pipeline(cls, user, pipeline, inputs, users_allowed=None, groups_allowed=None,
                         name=None, description=None, threaded=True):
        """
        Execute the specified top-level Pipeline with the given inputs.

        This will create a run and start a fleet to run it.  This is only used for testing,
        and so a precondition is that sys.argv[1] is the management script used to invoke
        the tests.
        """
        name = name or ""
        description = description or ""
        run = pipeline.pipeline_instances.create(user=user, _complete=False, _successful=True,
                                                 name=name, description=description)
        users_allowed = users_allowed or []
        groups_allowed = groups_allowed or []
        run.users_allowed.add(*users_allowed)
        run.groups_allowed.add(*groups_allowed)

        for idx, curr_input in enumerate(inputs, start=1):
            run.inputs.create(dataset=curr_input, index=idx)

        # Confirm that the inputs are OK.
        pipeline.check_inputs(inputs)

        # The run is already in the queue, so we can just start the fleet and let it exit
        # when it finishes.
        if not threaded:
            interface = MPIManagerInterface(worker_count=1, manage_script=sys.argv[0])
        else:
            interface = ThreadManagerInterface(worker_count=1)
        manager = cls(interface=interface, quit_idle=True, history=1)
        manager.main_procedure()
        return manager

    def get_last_run(self):
        """
        Retrieve the last completed run from the history.

        If no history is retained, return None.
        """
        if self.history_queue.maxlen == 0 or len(self.history_queue) == 0:
            return None

        last_completed_sdbx = self.history_queue.pop()
        return last_completed_sdbx.run


class MPIWorkerInterface(MPIFleetInterface):
    """
    Object that is used by a Worker to communicate with the Manager.

    This handles setting up the MPI communicator.
    """
    def __init__(self):
        super(MPIWorkerInterface, self).__init__(MPI.Comm.Get_parent().Merge())
        self.rank = super(MPIWorkerInterface, self).get_rank()
        self.hostname = super(MPIWorkerInterface, self).get_hostname()

    def report_for_duty(self):
        return self.comm.send((self.hostname, self.rank), dest=0, tag=Worker.ROLLCALL)

    def stop_run_callback(self):
        if self.comm.Iprobe(source=0, tag=Worker.STOP):
            return self.comm.recv(source=0, tag=Worker.STOP)
        return

    def probe_for_task(self):
        return self.comm.Iprobe(source=0, tag=MPI.ANY_TAG)

    def get_task_info(self):
        status = MPI.Status()
        task_info_dict = self.comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()
        return task_info_dict, tag

    def send_finished_task(self, message):
        return self.comm.send(message, dest=0, tag=Worker.FINISHED)

    def close(self):
        self.comm.Disconnect()


class ThreadWorkerInterface(ThreadFleetInterface):
    """
    Analogue of MPIWorkerInterface where threads are used instead of MPI.
    """
    def __init__(self, rank, manager_interface):
        self.rank = rank
        assert isinstance(manager_interface, ThreadManagerInterface)
        self.manager_interface = manager_interface
        self.manager_interface.worker_interfaces[rank-1] = self
        self.job_queue = Queue.Queue()
        self.stop_queue = Queue.Queue()

    def get_rank(self):
        return self.rank

    def get_size(self):
        return self.manager_interface.get_size()

    def report_for_duty(self):
        # This isn't necessary -- the Manager interface can track the threads it starts by itself.
        pass

    def stop_run_callback(self):
        if not self.stop_queue.empty():
            return self.stop_queue.get(block=True)
        return

    def probe_for_task(self):
        return not self.job_queue.empty()

    def get_task_info(self):
        task_info_dict, tag = self.job_queue.get()
        return task_info_dict, tag

    def send_finished_task(self, message):
        self.manager_interface.finished_queues[self.get_rank()-1].put(message)

    def close(self):
        pass


class Worker(object):
    """
    Performs the actual computational tasks required of Pipelines.
    """
    READY = "ready"
    VASSAL = "vassal"
    LORD = "lord"

    ROLLCALL = 1
    ASSIGNMENT = 2
    FINISHED = 3
    SHUTDOWN = 4
    STOP = 5

    FAILURE = -1

    def __init__(self, interface):
        self.interface = interface
        self.rank = self.interface.get_rank()
        adjust_log_files(worker_logger, self.rank)
        worker_logger.debug("Worker {} started on host {}".format(self.rank, self.interface.get_hostname()))

        # Report to the manager.
        self.interface.report_for_duty()

    def receive_and_perform_task(self):
        """
        Looks for an assigned task and performs it.
        """
        while not self.interface.probe_for_task():
            time.sleep(settings.SLEEP_SECONDS)
        task_info_dict, tag = self.interface.get_task_info()

        if tag == self.SHUTDOWN:
            worker_logger.info("Worker {} shutting down.".format(self.rank))
            return tag

        try:
            task = None
            if "cable_record_pk" in task_info_dict:
                task = archive.models.RunComponent.objects.get(pk=task_info_dict["cable_record_pk"]).definite
            else:
                task = archive.models.RunStep.objects.get(pk=task_info_dict["runstep_pk"])
            worker_logger.info("%s(%d) received by rank %d: %s",
                               task.__class__.__name__,
                               task.pk,
                               self.rank,
                               task)

            try:
                if type(task) == archive.models.RunStep:
                    sandbox_result = Sandbox.finish_step(task_info_dict, self.rank, self.interface.stop_run_callback)
                else:
                    sandbox_result = Sandbox.finish_cable(task_info_dict, self.rank)
                worker_logger.debug(
                    "%s %s completed.  Returning results to Manager.",
                    task.__class__.__name__,
                    task)
                result = sandbox_result.pk
            except StopExecution as e:
                worker_logger.debug(
                    "[%d] %s %s stopped (%s).",
                    self.rank,
                    task.__class__.__name__,
                    task,
                    e,
                    exc_info=True
                )
                result = Worker.STOP
        except:
            result = Worker.FAILURE  # bogus return value
            worker_logger.error("[%d] Task %s failed.", self.rank, task, exc_info=True)

        message = (self.rank, result)
        self.interface.send_finished_task(message)
        worker_logger.debug("Sent {} to Manager".format(message))

        return tag

    def main_procedure(self):
        """
        Loop on receive_and_perform_task.
        """
        tag = None
        while tag != self.SHUTDOWN:
            tag = self.receive_and_perform_task()
        connection.close()


class WorkerFailedException(Exception):
    def __init__(self, error_msg):
        self.error_msg = error_msg
