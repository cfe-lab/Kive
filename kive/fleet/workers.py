"""
Defines the manager and the "workers" that manage and carry out the execution of Pipelines.
"""

from collections import defaultdict
import logging
from mpi4py import MPI
import sys
import time
import datetime
import itertools
import os
import glob
import shutil

from django.conf import settings
from django.utils import timezone

import archive.models
from archive.models import Dataset, Run, ExceedsSystemCapabilities
import sandbox.execute
from fleet.exceptions import StopExecution

mgr_logger = logging.getLogger("fleet.Manager")
worker_logger = logging.getLogger("fleet.Worker")

# Shorter sleep makes worker more responsive, generates more load when idle
SLEEP_SECONDS = 0.1


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


class Manager:
    """
    Coordinates the execution of pipelines.

    The manager is responsible for handling new Run requests and
    assigning the resulting tasks to workers.
    """

    def __init__(self, worker_count, manage_script):
        self.worker_count = worker_count
        self.manage_script = manage_script

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

    def _startup(self, comm):
        """
        Set up/register the workers and prepare to run.

        INPUTS
        roster: a dictionary structured much like a host file, keyed by
        hostnames and containing the number of Workers on each host.
        """
        # Set up our communicator and other MPI info.
        self.comm = comm
        self.rank = self.comm.Get_rank()
        self.count = self.comm.Get_size()
        self.mgr_hostname = MPI.Get_processor_name()

        adjust_log_files(mgr_logger, self.rank)
        mgr_logger.info("Manager started on host {}".format(self.mgr_hostname))

        workers_reported = 0
        while workers_reported < self.count - 1:
            hostname, rank = self.comm.recv(source=MPI.ANY_SOURCE, tag=Worker.ROLLCALL)
            mgr_logger.info("Worker {} on host {} has reported for duty".format(rank, hostname))
            self.roster[hostname].append(rank)
            workers_reported += 1

        for hostname in self.roster:
            for rank in self.roster[hostname]:
                self.hostnames[rank] = hostname

        self.worker_status = [Worker.READY for _ in range(self.count)]
        self.max_host_cpus = max([len(self.roster[x]) for x in self.roster])

    def is_worker_ready(self, rank):
        return self.worker_status[rank] == Worker.READY

    def start_run(self, run_to_start):
        """
        Receive a request to start a pipeline running.
        """
        new_sdbx = sandbox.execute.Sandbox(run=run_to_start)
        new_sdbx.advance_pipeline()

        # If we were able to reuse throughout, then we're totally done.  Otherwise we
        # need to do some bookkeeping.
        if run_to_start.is_complete():
            mgr_logger.info('Run "%s" completely reused (Pipeline: %s, User: %s)',
                            run_to_start, run_to_start.pipeline, run_to_start.user)
            run_to_start.stop(save=True)
            run_to_start.complete_clean()
        else:
            self.active_sandboxes[run_to_start] = new_sdbx
            for task in new_sdbx.hand_tasks_to_fleet():
                self.task_queue.append((new_sdbx, task))

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
                    mgr_logger.debug("Assigning task {} to workers {}".format(task, team))

                    # Send the job to the "lord":
                    self.comm.send(task_info.dict_repr(), dest=team[0], tag=Worker.ASSIGNMENT)
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
            while not self.comm.Iprobe(source=MPI.ANY_SOURCE,
                                       tag=Worker.FINISHED):
                time.sleep(SLEEP_SECONDS)
            lord_rank, result_pk = self.comm.recv(source=MPI.ANY_SOURCE,
                                                  tag=Worker.FINISHED)

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

        # Is anything from the run still processing?
        tasks_currently_running = False
        for task_info in self.tasks_in_progress.itervalues():
            if task_info['task'].run == just_finished['task'].run:
                tasks_currently_running = True
                break

        # If this run has failed (either due to this task or another),
        # we mop up.
        clean_up_now = False
        if not curr_sdbx.run.successful_execution():
            self.mop_up_terminated_sandbox(curr_sdbx)
            if not task_finished.is_successful():
                mgr_logger.info('Task %s (pk=%d) of run "%s" (pk=%d) (Pipeline: %s, User: %s) failed.',
                                task_finished, task_finished.pk, curr_sdbx.run, curr_sdbx.run.pk,
                                curr_sdbx.pipeline, curr_sdbx.user)

            if not tasks_currently_running:
                clean_up_now = True

        else:
            # Was this task a recovery or novel progress?
            if task_execute_info.is_recovery():
                execrecordouts = task_execute_info.execrecord.execrecordouts.all()
                data_newly_available = [execrecordout.symbolicdataset
                                        for execrecordout in execrecordouts]
                # Add anything that was waiting on this recovery to the queue.
                curr_sdbx.enqueue_runnable_tasks(data_newly_available)
            else:
                # Update maps and advance the pipeline.
                curr_sdbx.update_sandbox(task_finished)
                curr_sdbx.advance_pipeline(task_completed=just_finished["task"])
                if curr_sdbx.run.is_complete():
                    mgr_logger.info('Rest of Run "%s" (pk=%d) completely reused (Pipeline: %s, User: %s)',
                                    curr_sdbx.run, curr_sdbx.run.pk, curr_sdbx.pipeline, curr_sdbx.user)
                    if not tasks_currently_running:
                        clean_up_now = True

            if not clean_up_now:
                # The Run is still going and there may be more stuff to do.
                for task in curr_sdbx.hand_tasks_to_fleet():
                    self.task_queue.append((curr_sdbx, task))

        if clean_up_now:
            if not curr_sdbx.run.successful_execution():
                mgr_logger.info('Cleaning up failed run "%s" (pk=%d) (Pipeline: %s, User: %s)',
                                curr_sdbx.run, curr_sdbx.run.pk, curr_sdbx.pipeline, curr_sdbx.user)

            self.active_sandboxes.pop(curr_sdbx.run)
            curr_sdbx.run.stop(save=True)
            curr_sdbx.run.complete_clean()

            if curr_sdbx.run.successful_execution():
                mgr_logger.info('Finished successful run "%s" (pk=%d) (Pipeline: %s, User: %s)',
                                curr_sdbx.run, curr_sdbx.run.pk, curr_sdbx.pipeline, curr_sdbx.user)

        return workers_freed

    def main_procedure(self):
        mpi_info = MPI.Info.Create()
        mpi_info.Set("add-hostfile", "kive/hostfile")

        comm = MPI.COMM_SELF.Spawn(sys.executable,
                                   args=[self.manage_script, 'fleetworker'],
                                   maxprocs=self.worker_count,
                                   info=mpi_info).Merge()
        try:
            self._startup(comm)
            self.main_loop()
            mgr_logger.info("Manager shutting down.")
        except:
            mgr_logger.error("Manager failed.", exc_info=True)
        for rank in range(self.comm.Get_size()):
            if rank != self.comm.Get_rank():
                self.comm.send(dest=rank, tag=Worker.SHUTDOWN)
        comm.Disconnect()

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
            self.purge_sandboxes()
            Dataset.purge()

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
            if self.comm.Iprobe(source=MPI.ANY_SOURCE, tag=Worker.FINISHED):
                lord_rank, result_pk = self.comm.recv(source=MPI.ANY_SOURCE,
                                                      tag=Worker.FINISHED)
                try:
                    self.worker_finished(lord_rank, result_pk)
                    break
                except WorkerFailedException as e:
                    mgr_logger.error(e.error_msg)
                    return False

            try:
                time.sleep(SLEEP_SECONDS)
            except KeyboardInterrupt:
                return False
        return True

    def find_new_runs(self):
        # Look for new jobs to run.  We will also
        # build in a delay here so we don't clog up the database.
        mgr_logger.debug("Looking for new runs....")
        pending_runs = Run.find_unstarted().order_by("time_queued")

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
        mgr_logger.debug("Stopping run (pk={}) on behalf of user {}".format(run.pk, run.stopped_by))
        sandbox_to_end = self.active_sandboxes[run]

        # Send a message to the foreman in charge of running this task.
        foreman_found = False
        for foreman in self.tasks_in_progress:
            if self.tasks_in_progress[foreman]["task"].top_level_run == run:
                foreman_found = True
                break

        if foreman_found:
            self.comm.isend("STOP", dest=foreman, tag=Worker.STOP)
            # Either the foreman got the message and ended the task, or it
            # finished the task.
            self.comm.recv(source=foreman, tag=Worker.FINISHED)

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
            mgr_logger.debug("Removing sandbox at {}".format(rtp.sandbox_path))
            rtp.collect_garbage()

        # Next, look through the sandbox directory and see if there are any orphaned sandboxes
        # to remove.
        mgr_logger.debug("Checking for orphaned sandbox directories to clean up....")

        sdbx_path = os.path.join(settings.MEDIA_ROOT, settings.SANDBOX_PATH)
        for putative_sdbx in glob.glob(os.path.join(sdbx_path, sandbox.execute.sandbox_glob)):

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


class Worker:
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

    def __init__(self, comm):
        self.comm = comm
        self.rank = self.comm.Get_rank()
        self.count = self.comm.Get_size()
        self.wkr_hostname = MPI.Get_processor_name()

        adjust_log_files(worker_logger, self.rank)
        worker_logger.debug("Worker {} started on host {}".format(self.rank, self.wkr_hostname))

        # Report to the manager.
        self.comm.send((self.wkr_hostname, self.rank), dest=0, tag=Worker.ROLLCALL)

    def check_for_stop(self):
        """
        A callback that checks for a Worker.STOP message from the Manager.
        """
        if self.comm.Iprobe(source=0, tag=Worker.STOP):
            return self.comm.recv(source=0, tag=Worker.STOP)
        return

    def receive_and_perform_task(self):
        """
        Looks for an assigned task and performs it.
        """
        status = MPI.Status()
        while not self.comm.Iprobe(source=0, tag=MPI.ANY_TAG):
            time.sleep(SLEEP_SECONDS)
        task_info_dict = self.comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()
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
                    sandbox_result = sandbox.execute.finish_step(task_info_dict, self.rank, self.check_for_stop)

                else:
                    sandbox_result = sandbox.execute.finish_cable(task_info_dict, self.rank)
                worker_logger.debug("{} {} completed.  Returning results to Manager.".format(task.__class__.__name__, task))
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
        self.comm.send(message, dest=0, tag=Worker.FINISHED)
        worker_logger.debug("Sent {} to Manager".format(message))

        return tag

    def main_procedure(self):
        """
        Loop on receive_and_perform_task.
        """
        tag = None
        while tag != self.SHUTDOWN:
            tag = self.receive_and_perform_task()


class WorkerFailedException(Exception):
    def __init__(self, error_msg):
        self.error_msg = error_msg
