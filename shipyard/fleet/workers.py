"""
Defines the manager and the "workers" that manage and carry out the execution of Pipelines.
"""

from mpi4py import MPI
import numpy
import logging
from collections import defaultdict
from django.db import transaction
import time

import shipyard.settings  # @UnresolvedImport
import fleet.models
import archive.models
import sandbox.execute

mgr_logger = logging.getLogger("fleet.Manager")
worker_logger = logging.getLogger("fleet.Worker")


class Manager:
    """
    Coordinates the execution of pipelines.

    The manager is responsible for handling new Run requests and
    assigning the resulting tasks to workers.
    """

    def __init__(self, comm):
        """
        Set up/register the workers and prepare to run.

        INPUTS
        roster: a dictionary structured much like a host file, keyed by
        hostname and containing the number of Workers on each host.
        """
        # Set up our communicator and other MPI info.
        self.comm = comm
        self.rank = self.comm.Get_rank()
        self.count = self.comm.Get_size()
        self.hostname = MPI.Get_processor_name()

        # Set up an ongoing non-blocking receive looking for all Worker.FINISHED-tagged
        # messages from Workers.  Any time these are actually used, they should be
        # replaced afresh.
        self._setup_finished_task_receiver()

        mgr_logger.info("Manager started on host {}".format(self.hostname))

        # tasks_in_progress tracks what jobs are assigned to what workers.
        self.tasks_in_progress = {}
        # task_queue is a list of 2-tuples (sandbox, runstep/runcable).
        # We don't use a Queue here because we may need to remove tasks from
        # the queue if their parent Run fails.
        self.task_queue = []
        # A table of currently running sandboxes, indexed by the Run.
        self.active_sandboxes = {}

        # roster will be a dictionary keyed by hostname whose values are
        # the sets of ranks of processes running on that host.  This will be
        # necessary down the line to help determine which hosts have enough
        # threads to run.
        self.roster = defaultdict(list)
        workers_reported = 0
        while workers_reported < self.count - 1:
            hostname, rank = self.comm.recv(source=MPI.ANY_SOURCE, tag=Worker.ROLLCALL)
            mgr_logger.info("Worker {} on host {} has reported for duty".format(rank, hostname))
            self.roster[hostname].append(rank)
            workers_reported += 1

        # A reverse lookup table for the above.
        self.hostname = {}
        for hostname in self.roster:
            for rank in self.roster[hostname]:
                self.hostname[rank] = hostname

        self.worker_status = [Worker.READY for _ in range(self.count)]

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

    def assign_task(self, sandbox, task):
        """
        Assign a task to a worker.
        """
        task_info = sandbox.get_task_info(task)

        # First, we find a host that is capable of taking on the task.
        candidate_hosts = self.roster.keys()

        # If we have no hosts that are capable of handling this many threads,
        # we blow up.
        max_host_cpus = max([len(self.roster[x]) for x in self.roster])
        if task_info.threads_required > max_host_cpus:
            sandbox.exceeded_system(max_host_cpus)
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
            lord_rank = self.work_finished_result[0]
            result_pk = self.work_finished_result[1]
            self._setup_finished_task_receiver()

            source_host = self.hostname[lord_rank]
            result = archive.models.RunComponent.objects.get(pk=result_pk).definite
            self.note_progress(lord_rank, result)
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
            self.active_sandboxes.pop(curr_sdbx.run)
            self.task_queue = [x for x in self.task_queue if x[0] != curr_sdbx]
        else:
            # Did this task finish the run?
            if curr_sdbx.run.is_complete():
                self.active_sandboxes.pop(curr_sdbx.run)

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
        try:
            self.main_loop()
            mgr_logger.info("Manager shutting down.")
        except:
            mgr_logger.error("Manager failed.", exc_info=True)
        for rank in range(self.comm.Get_size()):
            if rank != self.comm.Get_rank():
                self.comm.send(dest=rank, tag=Worker.SHUTDOWN)
        
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
                self.assign_task(task_sdbx, curr_task[1])
                self.task_queue = self.task_queue[1:]

            # Everything in the queue has been started, so we check and see if anything has finished.
            worker_returned = False
            for _ in range(shipyard.settings.FLEET_POLLING_INTERVAL):
                test_result = self.work_finished_request.test()
                if test_result[0]:
                    mgr_logger.debug("Worker {} reports task with PK {} is finished".format(
                        self.work_finished_result[0], self.work_finished_result[1]
                    ))
                    worker_returned = True
                    break
                try:
                    time.sleep(1)
                except KeyboardInterrupt:
                    return
            
            if worker_returned:
                task_finished = archive.models.RunComponent.objects.get(pk=self.work_finished_result[1]).definite
                self.note_progress(self.work_finished_result[0], task_finished)
                self._setup_finished_task_receiver()

            # Look for new jobs to run.  We will also
            # build in a delay here so we don't clog up the database.
            mgr_logger.info("Looking for new runs....")
            with transaction.atomic():
                pending_runs = [x for x in fleet.models.RunToProcess.objects.order_by("time_queued") if not x.started]

                mgr_logger.debug("Pending runs: {}".format(pending_runs))

                for run_to_process in pending_runs:
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

    def __init__(self, comm):
        self.comm = comm
        self.rank = self.comm.Get_rank()
        self.count = self.comm.Get_size()
        self.hostname = MPI.Get_processor_name()

        worker_logger.info("Worker {} started on host {}".format(self.rank, self.hostname))

        # Report to the manager.
        self.comm.send((self.hostname, self.rank), dest=0, tag=Worker.ROLLCALL)

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
            worker_logger.info("{} received: {}".format(task.__class__.__name__, task))
    
            sandbox_result = None
            if type(task) == archive.models.RunStep:
                sandbox_result = sandbox.execute.finish_step(task_info_dict)
            else:
                sandbox_result = sandbox.execute.finish_cable(task_info_dict)
            worker_logger.info("{} {} completed.  Returning results to Manager.".format(task.__class__.__name__, task))
            result = sandbox_result.pk
        except:
            result = -1 #bogus return value
            worker_logger.error("Task %s failed.", task, exc_info=True)
            
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
