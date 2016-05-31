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
import Queue
import socket

from django.db import connection
from django.conf import settings
from django.utils import timezone

import archive.models
from archive.models import Dataset, Run, ExceedsSystemCapabilities
from sandbox.execute import Sandbox, sandbox_glob
from fleet.exceptions import StopExecution
from constants import runcomponentstates

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
            rank_suffix = '.{:03}'.format(rank)
            if not fileRoot.endswith(rank_suffix):
                handler.baseFilename = fileRoot + rank_suffix + fileExt
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


class SingleThreadedFleetInterface(object):
    """ Base class for both single-threaded manager and worker. """
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
        # finished the task (which is fine).
        self.comm.recv(source=foreman, tag=Worker.FINISHED)

    def record_exception(self):
        mgr_logger.error("Manager failed.", exc_info=True)

    def shut_down_fleet(self):
        for rank in range(self.get_size()):
            if rank != self.get_rank():
                self.comm.send(None, dest=rank, tag=Worker.SHUTDOWN)
        self.comm.Disconnect()


class SingleThreadedManagerInterface(SingleThreadedFleetInterface):
    def __init__(self, worker_count):
        # Elements of this queue will be 2-tuples (foreman rank, result PK).
        self.finished_queues = [Queue.Queue() for _ in range(worker_count)]

        self.worker_count = worker_count
        self.workers = []
        self.worker_interfaces = [None] * worker_count

        for rank in range(worker_count):
            # Each worker interface will add itself to self.worker_interfaces.
            worker_interface = SingleThreadedWorkerInterface(
                rank=rank,
                manager_interface=self)
            self.workers.append(Worker(interface=worker_interface))

    def send_task_to_worker(self, task_info, worker_rank):
        self.worker_interfaces[worker_rank-1].job_queue.put(
            (task_info.dict_repr(), Worker.ASSIGNMENT),
            block=True
        )

    def probe_for_finished_worker(self):
        for rank in range(self.worker_count):
            worker_interface = self.worker_interfaces[rank]
            if not worker_interface.job_queue.empty():
                self.workers[rank].receive_and_perform_task()
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

    def record_exception(self):
        # Just report the exception immediately by raising it again
        raise

    def shut_down_fleet(self):
        pass


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
        # A table of sandboxes that are in the process of shutting down/being cancelled.
        self.sandboxes_shutting_down = set()

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
        run_to_start.refresh_from_db()

        # If we were able to reuse throughout, then we're totally done.  Otherwise we
        # need to do some bookkeeping.
        finished_already = False
        if run_to_start.is_successful():
            mgr_logger.info('Run "%s" (Pipeline: %s, User: %s) completely reused successfully',
                            run_to_start, run_to_start.pipeline, run_to_start.user)
            finished_already = True

        elif run_to_start.is_failing() or run_to_start.is_cancelling():
            # The run failed somewhere in preparation.  This hasn't affected any of our maps yet, so we
            # just report it and discard it.
            status_str = "failed" if run_to_start.is_failing() else "cancelled"
            mgr_logger.info('Run "%s" (pk=%d, Pipeline: %s, User: %s) %s before execution',
                            run_to_start, run_to_start.pk, run_to_start.pipeline, run_to_start.user,
                            status_str)
            run_to_start.cancel_components()
            run_to_start.stop(save=True)
            finished_already = True

        else:
            self.active_sandboxes[run_to_start] = new_sdbx
            for task in new_sdbx.hand_tasks_to_fleet():
                self.task_queue.append((new_sdbx, task))

        if finished_already:
            run_to_start.complete_clean()
            if self.history_queue.maxlen > 0:
                self.history_queue.append(new_sdbx)

        return new_sdbx

    def mop_up_terminated_sandbox(self, sandbox):
        """
        Remove this sandbox's tasks from the queue after failure or cancellation.
        """
        # Mark this sandbox as in the process of shutting down.
        self.sandboxes_shutting_down.add(sandbox)
        new_task_queue = []
        # Cancel all tasks that are still on the queue (and thus not actually running yet).
        for task_sdbx, task in self.task_queue:
            if task_sdbx != sandbox:
                new_task_queue.append((task_sdbx, task))
            else:
                if isinstance(task, archive.models.RunStep):
                    for rsic in task.RSICs.filter(
                            _runcomponentstate__pk__in=[runcomponentstates.PENDING_PK,
                                                        runcomponentstates.RUNNING_PK]
                    ):
                        rsic.cancel(save=True)  # this saves rsic
                task.cancel(save=True)  # this saves task

        # Cancel all parts of the run that aren't currently processing.
        steps_processing = []
        incables_processing = []
        outcables_processing = []
        for foreman in self.tasks_in_progress:
            task = self.tasks_in_progress[foreman]["task"]
            if task.top_level_run == sandbox.run:
                if isinstance(task, archive.models.RunStep):
                    steps_processing.append(task)
                elif isinstance(task, archive.models.RunSIC):
                    incables_processing.append(task)
                else:
                    outcables_processing.append(task)
        sandbox.run.cancel_components(except_steps=steps_processing, except_incables=incables_processing,
                                      except_outcables=outcables_processing)

        # Update the queue.
        self.task_queue = new_task_queue

    def remove_sandbox_from_queues(self, sandbox):
        """
        Clear the sandbox out of the queues when it's completely finished running.
        """
        self.active_sandboxes.pop(sandbox.run)
        # If this was already in the process of shutting down, remove the annotation.
        self.sandboxes_shutting_down.discard(sandbox)
        if self.history_queue.maxlen > 0:
            self.history_queue.append(sandbox)

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
                task,
                task_info.threads_required,
                self.max_host_cpus,
                task.top_level_run
            )
            task.top_level_run.not_enough_CPUs.create(threads_requested=task_info.threads_required,
                                                      max_available=self.max_host_cpus)
            self.mop_up_terminated_sandbox(sandbox)
            task.refresh_from_db()
            assert task.is_cancelled()  # this should happen in mop_up_terminated_sandbox
            sandbox.run.cancel(save=True)  # transition: Running->Cancelling

            # If there is nothing currently running from this Run, we can end it.
            end_now = True
            for task_info in self.tasks_in_progress.itervalues():
                if task_info['task'].top_level_run == sandbox.run:
                    end_now = False
                    break

            if end_now:
                # This stops the run and makes the transition: Cancelling->Cancelled
                sandbox.run.stop(save=True)
                self.remove_sandbox_from_queues(sandbox)

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
            if sandbox.run not in self.active_sandboxes or sandbox in self.sandboxes_shutting_down:
                mgr_logger.debug(
                    "Abandoning task %s (pk=%d) because its run has been terminated.",
                    task,
                    task.pk
                )
                return

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

        # Mark this task as having finished.
        just_finished = self.tasks_in_progress.pop(lord_rank)
        assert task_finished == just_finished["task"]
        curr_sdbx = self.active_sandboxes[task_finished.top_level_run]
        task_execute_info = curr_sdbx.get_task_info(task_finished)

        workers_freed = [lord_rank] + just_finished["vassals"]
        for worker_rank in workers_freed:
            self.worker_status[worker_rank] = Worker.READY

        # Is anything from the run actively processing?
        tasks_currently_running = False
        # Recall:
        # a RunStep gives you the step coordinates
        # a RunSIC gives you its parent step coordinates
        # a RunOutputCable gives you the parent run coordinates
        # task_finished_coords[idx] is the component coordinate in
        # the subrun idx levels deep (0 means top-level run).
        task_finished_coords = task_finished.get_coordinates()
        if task_finished.is_outcable:
            # Add a dummy entry at the end so that the 0th to 2nd-last coordinates
            # give the sub-run coordinates in all cases.
            task_finished_coords = task_finished_coords + (None,)

        # At position i, this list denotes whether any other tasks from the sub-run
        # i levels deep (0 means top level run) are currently running.
        subrun_tasks_currently_running = [False for _ in task_finished_coords]

        for task_info in self.tasks_in_progress.itervalues():
            if task_info['task'].top_level_run == just_finished['task'].top_level_run:
                running_task_coords = task_info["task"].get_coordinates()
                if task_info["task"].is_outcable:
                    running_task_coords = running_task_coords + (None,)

                # These belong to the same Run, so we can't bail out yet if task_finished failed.
                tasks_currently_running = True
                subrun_tasks_currently_running[0] = True
                # If either task_finished_coords and running_task_coords are length 1 (i.e. one
                # directly belongs to the top-level run), this does nothing.
                for coord in range(1, min(len(task_finished_coords), len(running_task_coords))):
                    if task_finished_coords[coord] == running_task_coords[coord]:
                        subrun_tasks_currently_running[coord] = True
                    else:
                        # Nothing nested deeper can belong to the same sub-run.
                        break

        # If this run has failed (either due to this task or another),
        # we mop up.
        clean_up_now = False
        curr_sdbx.run.refresh_from_db()
        stop_subruns_if_possible = False

        if task_finished.is_successful():
            if curr_sdbx.run.is_failing() or curr_sdbx.run.is_cancelling():
                assert curr_sdbx in self.sandboxes_shutting_down
                mgr_logger.debug(
                    'Task %s (pk=%d) was successful but run "%s" (pk=%d) (Pipeline: %s, User: %s) %s.',
                    task_finished,
                    task_finished.pk,
                    curr_sdbx.run,
                    curr_sdbx.run.pk,
                    curr_sdbx.pipeline,
                    curr_sdbx.user,
                    "failing" if curr_sdbx.run.is_failing() else "cancelling"
                )

                # Stop any sub-Runs (or the top-level run) that this was the last
                # running task of.
                stop_subruns_if_possible = True

                if not tasks_currently_running:
                    clean_up_now = True

            else:  # run is still processing successfully
                assert curr_sdbx.run.is_running(), "{} != Running".format(curr_sdbx.run.get_state_name())

                # Was this task a recovery or novel progress?
                if task_execute_info.is_recovery():
                    mgr_logger.debug(
                        'Recovering task %s (pk=%d) was successful; '
                        'queueing waiting tasks from run "%s" (pk=%d, Pipeline: %s, User: %s).',
                        task_finished,
                        task_finished.pk,
                        curr_sdbx.run,
                        curr_sdbx.run.pk,
                        curr_sdbx.pipeline,
                        curr_sdbx.user
                    )

                    execrecordouts = task_execute_info.execrecord.execrecordouts.all()
                    data_newly_available = [execrecordout.dataset
                                            for execrecordout in execrecordouts]
                    # Add anything that was waiting on this recovery to the queue.
                    curr_sdbx.enqueue_runnable_tasks(data_newly_available)

                else:
                    mgr_logger.debug(
                        'Task %s (pk=%d) was successful; '
                        'advancing run "%s" (pk=%d, Pipeline: %s, User: %s).',
                        task_finished,
                        task_finished.pk,
                        curr_sdbx.run,
                        curr_sdbx.run.pk,
                        curr_sdbx.pipeline,
                        curr_sdbx.user
                    )

                    # Update maps and advance the pipeline.  Note that advance_pipeline
                    # will transition the states of runs and sub-runs appropriately.
                    curr_sdbx.update_sandbox(task_finished)
                    curr_sdbx.advance_pipeline(task_completed=just_finished["task"])
                    curr_sdbx.run.refresh_from_db()
                    if curr_sdbx.run.is_successful():
                        assert not tasks_currently_running
                        mgr_logger.info('Run "%s" (pk=%d, Pipeline: %s, User: %s) finished successfully',
                                        curr_sdbx.run, curr_sdbx.run.pk, curr_sdbx.pipeline, curr_sdbx.user)
                        clean_up_now = True  # this is the only "successful" clean up condition

                    elif curr_sdbx.run.is_failing() or curr_sdbx.run.is_cancelling():
                        # Something just failed in advance_pipeline.
                        mgr_logger.debug(
                            'Run "%s" (pk=%d, Pipeline: %s, User: %s) failed to advance '
                            'after finishing task %s (pk=%d)',
                            curr_sdbx.run,
                            curr_sdbx.run.pk,
                            curr_sdbx.pipeline,
                            curr_sdbx.user,
                            task_finished,
                            task_finished.pk
                        )

                        if curr_sdbx not in self.sandboxes_shutting_down:
                            self.mop_up_terminated_sandbox(curr_sdbx)
                        if not tasks_currently_running:
                            clean_up_now = True

        else:
            # The component that just finished failed.  Cancellation is handled by stop_run
            # (or assign_task).
            assert task_finished.is_failed(), "{} != Failed".format(task_finished.get_state_name())
            stop_subruns_if_possible = True
            if curr_sdbx.run.is_failing() or curr_sdbx.run.is_cancelling():
                assert curr_sdbx in self.sandboxes_shutting_down
                mgr_logger.debug(
                    'Task %s (pk=%d) failed; run "%s" (pk=%d, Pipeline: %s, User: %s) was already %s',
                    task_finished,
                    task_finished.pk,
                    curr_sdbx.run,
                    curr_sdbx.run.pk,
                    curr_sdbx.pipeline,
                    curr_sdbx.user,
                    "failing" if curr_sdbx.run.is_failing() else "cancelling"
                )

            else:
                assert curr_sdbx.run.is_running(), "{} != Running".format(curr_sdbx.run.get_state_name())
                mgr_logger.info('Task %s (pk=%d) of run "%s" (pk=%d, Pipeline: %s, User: %s) failed; '
                                'marking run as failing',
                                task_finished, task_finished.pk, curr_sdbx.run, curr_sdbx.run.pk,
                                curr_sdbx.pipeline, curr_sdbx.user)

                # Go through and mark all ancestor runs of task_finished as failing.
                curr_sdbx.run.mark_failure(save=True)
                curr_ancestor_run = curr_sdbx.run
                # This does nothing if task_finished_coords is of length 1; i.e. if it's a component belonging
                # to the top-level run.
                for coord in task_finished_coords[:-1]:
                    curr_ancestor_run = curr_ancestor_run.runsteps.get(pipelinestep__step_num=coord).child_run
                    if curr_ancestor_run.is_running():  # skip over this if it's cancelling or failing already
                        curr_ancestor_run.mark_failure(save=True)

                self.mop_up_terminated_sandbox(curr_sdbx)  # this cancels the recovering record as well

            # Now check whether we can do our final clean up.
            if not tasks_currently_running:
                clean_up_now = True

        if stop_subruns_if_possible:

            curr_run = task_finished.parent_run
            for idx in range(len(task_finished_coords)-1, -1, -1):
                # task_finished_coords[idx] is the component coordinate in
                # the subrun idx levels deep (0 means top-level run).
                # curr_run is this subrun.
                if not subrun_tasks_currently_running[idx]:
                    if not curr_run.has_ended():
                        curr_run.stop(save=True)
                    else:
                        curr_run.finish_recovery(save=True)
                else:
                    # By definition everything above this point has stuff
                    # still running.
                    break
                if idx > 0:
                    curr_run = curr_run.parent_runstep.parent_run

        if not clean_up_now:
            # The Run is still going and there may be more stuff to do.
            for task in curr_sdbx.hand_tasks_to_fleet():
                self.task_queue.append((curr_sdbx, task))

        else:
            status_string = "successful"
            if curr_sdbx.run.is_failing():
                status_string = "failed"
            elif curr_sdbx.run.is_cancelling():
                status_string = "cancelled"

            mgr_logger.info(
                'Cleaning up %s run "%s" (pk=%d, Pipeline: %s, User: %s)',
                status_string,
                curr_sdbx.run,
                curr_sdbx.run.pk,
                curr_sdbx.pipeline,
                curr_sdbx.user
            )

            self.remove_sandbox_from_queues(curr_sdbx)
            if not curr_sdbx.run.is_successful():
                curr_sdbx.run.stop(save=True)
            # curr_sdbx.run.complete_clean()

        return workers_freed

    def assign_tasks(self, time_to_poll):
        # We can't use a for loop over the task queue because assign_task
        # may add to the queue.
        while len(self.task_queue) > 0 and time.time() < time_to_poll:
            # task_queue entries are (sandbox, run_step)
            self.task_queue.sort(key=lambda entry: entry[0].run.start_time)
            curr_task = self.task_queue.pop(0)  # looks like (sandbox, task)
            task_sdbx = self.active_sandboxes[curr_task[1].top_level_run]
            # We assign this task to a worker, and do not proceed until the task
            # is assigned.
            try:
                self.assign_task(task_sdbx, curr_task[1])
            except WorkerFailedException as e:
                mgr_logger.error(e.error_msg)
                return False

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
        mgr_logger.debug("Stopping run (pk=%d) on behalf of user %s",
                         run.pk,
                         run.stopped_by)

        if not run.has_started():
            run.start(save=True)

        if run.is_complete():
            # This run already completed, so we ignore this call.
            mgr_logger.warn("Run (pk=%d) is already complete; ignoring stop request.", run.pk)
            return
        elif run not in self.active_sandboxes:
            # This hasn't started yet, or is a remnant from a fleet crash/shutdown,
            # so we can just skip this one.
            mgr_logger.warn("Run (pk=%d) is not active.  Cancelling steps/cables that were unfinished.",
                            run.pk)
            run.cancel_components()
        else:

            # Send messages to the foremen in charge of running this run's task.
            # We don't bother to do anything with the results.  Everything gets cancelled
            # even if it returned successfully or unsuccessfully.
            for foreman in self.tasks_in_progress:
                if self.tasks_in_progress[foreman]["task"].top_level_run == run:
                    curr_task_in_progress = self.tasks_in_progress.pop(foreman)
                    self.interface.stop_run(foreman)
                    self.worker_status[foreman] = Worker.READY
                    for worker_rank in curr_task_in_progress["vassals"]:
                        self.worker_status[worker_rank] = Worker.READY

            sandbox_to_end = self.active_sandboxes[run]
            # Cancel all tasks on the task queue pertaining to this run, and finalize the
            # details.
            self.mop_up_terminated_sandbox(sandbox_to_end)
            self.remove_sandbox_from_queues(sandbox_to_end)

        run.cancel(save=True)
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

    def main_procedure(self):
        try:
            self._startup()
            self.main_loop()
            mgr_logger.info("Manager shutting down.")
        except:
            mgr_logger.error("Manager failed.", exc_info=True)
        self.interface.shut_down_fleet()

    @classmethod
    def execute_pipeline(cls,
                         user,
                         pipeline,
                         inputs,
                         users_allowed=None,
                         groups_allowed=None,
                         name=None,
                         description=None,
                         single_threaded=True):
        """
        Execute the specified top-level Pipeline with the given inputs.

        This will create a run and start a fleet to run it.  This is only used for testing,
        and so a precondition is that sys.argv[1] is the management script used to invoke
        the tests.
        """
        name = name or ""
        description = description or ""
        run = pipeline.pipeline_instances.create(user=user, name=name, description=description)
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
        if not single_threaded:
            interface = MPIManagerInterface(worker_count=1, manage_script=sys.argv[0])
        else:
            interface = SingleThreadedManagerInterface(worker_count=1)
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

    def record_exception(self, rank, task):
        worker_logger.error("[%d] Task %s failed.", self.rank, task, exc_info=True)

    def close(self):
        self.comm.Disconnect()


class SingleThreadedWorkerInterface(SingleThreadedFleetInterface):
    """
    Analogue of MPIWorkerInterface where threads are used instead of MPI.
    """
    def __init__(self, rank, manager_interface):
        self.rank = rank
        assert isinstance(manager_interface, SingleThreadedManagerInterface)
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

    def record_exception(self, rank, task):
        # Just report the exception immediately by raising it again
        raise

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

        elif tag == self.STOP:
            # This was sent as an attempt to stop the last thing this Worker was
            # doing, but was missed.
            worker_logger.info("Worker {} received a stop message too late; ignoring.".format(self.rank))
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
            self.interface.record_exception(self.rank, task)

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
