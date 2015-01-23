"""
Defines the manager and the "workers" that manage and carry out the execution of Pipelines.
"""

from mpi4py import MPI
import numpy
import logging
from collections import defaultdict
import time
from exceptions import Exception

import shipyard.settings  # @UnresolvedImport
import fleet.models
import archive.models
import sandbox.execute
import sys

mgr_logger = logging.getLogger("fleet.Manager")
worker_logger = logging.getLogger("fleet.Worker")


class Manager:
    """
    Coordinates the execution of pipelines.

    The manager is responsible for handling new Run requests and
    assigning the resulting tasks to workers.
    """

    def __init__(self, worker_count, manage_script):
        self.worker_count = worker_count
        self.manage_script = manage_script

        # tasks_in_progress tracks what jobs are assigned to what workers.
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

        # Set up an ongoing non-blocking receive looking for all Worker.FINISHED-tagged
        # messages from Workers.  Any time these are actually used, they should be
        # replaced afresh.
        self._setup_finished_task_receiver()

        mgr_logger.debug("Manager started on host {}".format(self.mgr_hostname))

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

    def _setup_finished_task_receiver(self):
        self.work_finished_result = numpy.empty(2, dtype="i")
        self.work_finished_request = self.comm.Irecv([self.work_finished_result, MPI.INT],
                                                     source=MPI.ANY_SOURCE,
                                                     tag=Worker.FINISHED)

    def is_worker_ready(self, rank):
        return self.worker_status[rank] == Worker.READY

    def start_run(self, user, pipeline_to_run, inputs, sandbox_path=""):
        """
        Receive a request to start a pipeline running.
        """
        if sandbox_path == "":
            sandbox_path = None
        new_sdbx = sandbox.execute.Sandbox(user, pipeline_to_run, inputs, sandbox_path=sandbox_path)
        new_sdbx.advance_pipeline()

        # If we were able to reuse throughout, then we're totally done.  Otherwise we
        # need to do some bookkeeping.
        if not new_sdbx.run.is_complete():
            self.active_sandboxes[new_sdbx.run] = new_sdbx
            for task in new_sdbx.hand_tasks_to_fleet():
                self.task_queue.append((new_sdbx, task))

        return new_sdbx

    def mop_up_failed_sandbox(self, sandbox):
        self.active_sandboxes.pop(sandbox.run)
        self.task_queue = [x for x in self.task_queue if x[0] != sandbox]

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
            self.mop_up_failed_sandbox(sandbox)
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
            self.work_finished_request.wait()

            # Note the task that just finished, and release its workers.
            # If this fails it will throw an exception.
            lord_rank, _ = self.worker_finished()

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

        # If the task just finished was unsuccessful, we should remove anything from the queue belonging to the
        # same sandbox.  Otherwise update the sandbox if this was not a recovery.
        if not task_finished.successful_execution():
            self.mop_up_failed_sandbox(curr_sdbx)
        else:
            # Did this task finish the run?
            is_complete = curr_sdbx.run.is_complete()
            if is_complete:
                for task_info in self.tasks_in_progress.itervalues():
                    if task_info['task'].run == just_finished['task'].run:
                        is_complete = False
                        break
            
            if is_complete:
                self.active_sandboxes.pop(curr_sdbx.run)
                mgr_logger.info('Run "%s" finished (Pipeline: %s, User: %s)', curr_sdbx.run, curr_sdbx.pipeline,
                                curr_sdbx.user)

            else:
                # Was this task a recovery or novel progress??
                if task_execute_info.is_recovery():
                    # Add anything that was waiting on this recovery to the queue.
                        curr_sdbx.enqueue_runnable_tasks()
                else:
                    # Update maps and advance the pipeline.
                    curr_sdbx.update_sandbox(task_finished)
                    curr_sdbx.advance_pipeline(task_completed=just_finished["task"])

                for task in curr_sdbx.hand_tasks_to_fleet():
                    # task is either a RunStep or a RunCable.
                    self.task_queue.append((curr_sdbx, task))

        return workers_freed

    def main_procedure(self):
        mgr_logger.info("Manager starting.")
        mpi_info = MPI.Info.Create()
        mpi_info.Set("add-hostfile", "shipyard/hostfile")
        
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

    def worker_finished(self):
        """
        Handle bookkeeping when a worker finishes.

        PRE: self.work_finished_request.test()[0] is True.
        """
        assert self.work_finished_request.test()[0]

        lord_rank, result_pk = self.work_finished_result
        if result_pk == Worker.FAILURE:
            raise WorkerFailedException("Worker {} reports a failed task (PK {})".format(
                lord_rank,
                self.tasks_in_progress[self.work_finished_result[0]]["task"].pk
            ))

        mgr_logger.info(
            "Worker %d reports task with PK %d is finished",
            lord_rank, result_pk)

        task_finished = archive.models.RunComponent.objects.get(pk=result_pk).definite
        self.note_progress(lord_rank, task_finished)
        self._setup_finished_task_receiver()
        return lord_rank, task_finished

    def main_loop(self):
        """
        Poll the database for new jobs, and handle running of sandboxes.
        """
        while True:
            # We can't use a for loop over the task queue because assign_task may add to the queue.
            while len(self.task_queue) > 0:
                curr_task = self.task_queue[0] # looks like (sandbox, task)
                task_sdbx = self.active_sandboxes[curr_task[1].top_level_run]
                # We assign this task to a worker, and do not proceed until the task
                # is assigned.
                try:
                    self.assign_task(task_sdbx, curr_task[1])
                except WorkerFailedException as e:
                    mgr_logger.error(e.error_msg)
                    return
                self.task_queue = self.task_queue[1:]

            # Everything in the queue has been started, so we check and see if anything has finished.
            for _ in range(shipyard.settings.FLEET_POLLING_INTERVAL):
                test_result = self.work_finished_request.test()
                if test_result[0]:
                    try:
                        self.worker_finished()
                        break
                    except WorkerFailedException as e:
                        mgr_logger.error(e.error_msg)
                        return

                try:
                    time.sleep(1)
                except KeyboardInterrupt:
                    return

            # Look for new jobs to run.  We will also
            # build in a delay here so we don't clog up the database.
            mgr_logger.debug("Looking for new runs....")
            # with transaction.atomic():
            pending_runs = [x for x in fleet.models.RunToProcess.objects.order_by("time_queued") if not x.started]

            mgr_logger.debug("Pending runs: {}".format(pending_runs))

            for run_to_process in pending_runs:
                threads_needed = run_to_process.pipeline.threads_needed()
                if threads_needed > self.max_host_cpus:
                    mgr_logger.info(
                        "Cannot run Pipeline %s for user %s: %d threads required, %d available",
                        run_to_process.pipeline, run_to_process.user, threads_needed,
                        self.max_host_cpus)
                    esc = fleet.models.ExceedsSystemCapabilities(
                        runtoprocess = run_to_process,
                        threads_requested=threads_needed,
                        max_available=self.max_host_cpus
                    )
                    esc.save()
                    run_to_process.clean()
                    continue

                mgr_logger.info("Starting run:\nPipeline: {}\nUser: {}".format(
                    run_to_process.pipeline, run_to_process.user))
                new_sdbx = self.start_run(run_to_process.user, run_to_process.pipeline,
                                          [x.symbolicdataset for x in run_to_process.inputs.order_by("index")],
                                          sandbox_path=run_to_process.sandbox_path)
                run_to_process.run = new_sdbx.run
                run_to_process.save()

                mgr_logger.debug("Task queue: {}".format(self.task_queue))
                mgr_logger.debug("Active sandboxes: {}".format(self.active_sandboxes))


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
    FAILURE = -1

    def __init__(self, comm):
        self.comm = comm
        self.rank = self.comm.Get_rank()
        self.count = self.comm.Get_size()
        self.wkr_hostname = MPI.Get_processor_name()

        worker_logger.debug("Worker {} started on host {}".format(self.rank, self.wkr_hostname))

        # Report to the manager.
        self.comm.send((self.wkr_hostname, self.rank), dest=0, tag=Worker.ROLLCALL)

    def receive_and_perform_task(self):
        """
        Looks for an assigned task and performs it.
        """
        status = MPI.Status()
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
    
            sandbox_result = None
            if type(task) == archive.models.RunStep:
                sandbox_result = sandbox.execute.finish_step(task_info_dict, self.rank)
            else:
                sandbox_result = sandbox.execute.finish_cable(task_info_dict, self.rank)
            worker_logger.debug("{} {} completed.  Returning results to Manager.".format(task.__class__.__name__, task))
            result = sandbox_result.pk
        except:
            result = Worker.FAILURE  # bogus return value
            worker_logger.error("[%d] Task %s failed.", self.rank, task, exc_info=True)
            
        send_buf = numpy.array([self.rank, result], dtype="i")
        self.comm.Send(send_buf, dest=0, tag=Worker.FINISHED)
        worker_logger.debug("Sent {} to Manager".format((self.rank, result)))
        
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