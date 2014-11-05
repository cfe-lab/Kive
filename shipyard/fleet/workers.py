"""
Defines the manager and the "workers" that manage and carry out the execution of Pipelines.
"""

import mpi4py as MPI
from sandbox.execute import Sandbox
from django.contrib.auth.models import User
import pipeline.models
import archive.models
import logging
from collections import defaultdict
from django.db import transaction
import fleet.models

class Manager:
    """
    Coordinates the execution of pipelines.

    The manager is responsible for handling new Run requests and
    assigning the resulting tasks to workers.
    """
    def __init__(self):
        """
        Set up/register the workers and prepare to run.

        INPUTS
        roster: a dictionary structured much like a host file, keyed by
        hostname and containing the number of Workers on each host.
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        # Set up our communicator and other MPI info.
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.count = self.comm.Get_size()
        self.hostname = MPI.Get_processor_name()

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
        while workers_reported < self.count:
            hostname, rank = self.comm.recv(source=MPI.ANY_SOURCE, tag=Worker.ROLLCALL)
            self.roster[hostname].append(rank)
            workers_reported += 1

        # A reverse lookup table for the above.
        self.hostname = {}
        for hostname in self.roster:
            for rank in self.roster[hostname]:
                self.hostname[rank] = hostname

        self.worker_status = [Worker.READY for i in range(self.count)]

    def is_worker_ready(self, rank):
        return self.worker_status[rank] == Worker.READY

    def start_run(self, user, pipeline_to_run, inputs, sandbox_path=None):
        """
        Receive a request to start a pipeline running.
        """
        new_sdbx = Sandbox(user, pipeline_to_run, inputs, sandbox_path=sandbox_path)
        self.active_sandboxes[new_sdbx.run] = new_sdbx

        new_sdbx.advance_pipeline()
        for task in new_sdbx.hand_tasks_to_fleet():
            self.task_queue.append((new_sdbx, task))

    def assign_task(self, sandbox, task):
        """
        Assign a task to a worker.
        """
        task_info = sandbox.get_task_info(task)

        # First, we find a host that is capable of taking on the task.
        candidate_hosts = self.roster.keys()

        # If we have no hosts that are capable of handling this many threads,
        # we blow up.
        max_host_cpus = max(*[len(self.roster[x]) for x in self.roster])
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
                    self.logger.debug("Assigning task to workers {}".format(team))

                    # Send the job to the "lord":
                    self.comm.send((sandbox, task), dest=team[0])
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
            # We wait for a worker to become ready and try again.
            self.logger.debug("Waiting for host to become ready....")
            source_host, workers_now_available = self.wait_for_progress()
            candidate_hosts = [source_host]

            # The task that returned may have belonged to the same sandbox, and
            # failed.  If so, we should cancel this task.
            if sandbox.run not in self.active_sandboxes:
                self.logger.debug("Run has been terminated; abandoning this task.")
                return

    def wait_for_progress(self):
        """
        Wait for a task to finish, and process the results accordingly.
        """
        # result should be a RunStep or RunCable.
        source_host, lord_rank, result = self.comm.recv(source=MPI.ANY_SOURCE)

        # Mark this task as having finished.
        just_finished = self.tasks_in_progress.pop(lord_rank)
        curr_sdbx = self.active_sandboxes[result.top_level_run]

        task_execute_info = curr_sdbx.get_task_info(result)

        workers_freed = [lord_rank] + just_finished["vassals"]
        for worker_rank in workers_freed:
            self.worker_status[worker_rank] = Worker.READY

        # If the result was unsuccessful, we should remove anything from the queue belonging to the
        # same sandbox.
        if not result.is_successful():
            self.active_sandboxes.pop(curr_sdbx)
            self.task_queue = [x for x in self.task_queue if x[0] != curr_sdbx]
        else:
            curr_sdbx.update_sandbox(result)

            # Was this task a recovery or forward progress?
            if task_execute_info.is_recovery():
                # Add anything that was waiting on this recovery to the queue.
                curr_sdbx.get_runnable_tasks()
            else:
                # Advance the pipeline.
                curr_sdbx.advance_pipeline(task_completed=just_finished["task"])

            for task in curr_sdbx.hand_tasks_to_fleet():
                # task is either a RunStep or a RunCable.
                self.task_queue.append((curr_sdbx, task))

        return source_host, workers_freed

    def main_procedure(self):
        """
        Poll the database for new jobs, and handle running of sandboxes.
        """
        while True:
            # We can't use a for loop over the task queue because assign_task may add to the queue.
            while len(self.task_queue) > 0:
                curr_task = self.task_queue[0]
                task_sdbx = self.active_sandboxes[curr_task.top_level_run]
                # We assign this task to a worker, and do not proceed until the task
                # is assigned.
                self.assign_task(task_sdbx, task)
                self.task_queue = self.task_queue[1:]

            # Everything in the queue has been started, so we look for new jobs to run.  We will also
            # build in a delay here so we don't clog up the database.
            with transaction.atomic():
                pending_runs = fleet.models.RunToProcess.objects.order_by("time_started")

                for run in pending_runs:
                    self.start_run(run.user, run.pipeline,
                                   [x.symbolicdataset for x in run.inputs.order_by("index")],
                                   sandbox_path=run.sandbox_path)
                    run.delete()


class Worker:
    """
    Performs the actual computational tasks required of Pipelines.
    """
    READY = "ready"
    VASSAL = "vassal"
    LORD = "lord"

    ROLLCALL = "rollcall"

    def __init__(self):
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.count = self.comm.Get_size()
        self.hostname = MPI.Get_processor_name()

        # Report to the manager.
        self.comm.send((self.hostname, self.rank), dest=0, tag=Worker.ROLLCALL)

    def receive_and_perform_task(self):
        """
        Looks for an assigned task and performs it.
        """
        sandbox, task = self.comm.recv(source=0)
        assert task.top_level_run == sandbox.run

        result = sandbox.finish_task(task)
        self.comm.send((self.hostname, self.rank, sandbox, result))

    def main_procedure(self):
        """
        Loop on receive_and_perform_task.
        """
        while True:
            self.receive_and_perform_task()