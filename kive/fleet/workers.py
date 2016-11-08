"""
Defines the manager and the "workers" that manage and carry out the execution of Pipelines.
"""

from collections import deque
import logging
import time
import datetime
import itertools
import os
import glob
import shutil
import json

from django.conf import settings
from django.utils import timezone

from archive.models import Dataset, Run, ExceedsSystemCapabilities,\
    RunStep, RunSIC, RunCable
from sandbox.execute import Sandbox, sandbox_glob
from fleet.exceptions import StopExecution

mgr_logger = logging.getLogger("fleet.Manager")
foreman_logger = logging.getLogger("fleet.Foreman")
worker_logger = logging.getLogger("fleet.Worker")


class Manager(object):
    """
    Coordinates the execution of pipelines.

    The manager is responsible for handling new Run requests and
    creating Foreman objects to execute each one.
    """
    # FIXME retrieve max_host_cpus from Slurm
    def __init__(self, max_host_cpus=settings.MAX_HOST_CPUS, quit_idle=False, history=0):
        self.quit_idle = quit_idle
        self.max_host_cpus = max_host_cpus

        # This maps run -|-> foreman
        self.runs_in_progress = {}
        # A table of sandboxes that are in the process of shutting down/being cancelled.
        self.runs_shutting_down = set()

        # A queue of recently-completed runs, to a maximum specified by history.
        self.history_queue = deque(maxlen=history)

    def create_foreman(self, run_to_start):
        """
        Handle a request to start a pipeline running.

        This creates a Foreman object to handle the Pipeline, which
        in turn creates a Sandbox and then coordinates the execution.
        """
        foreman = Foreman(run_to_start)
        self.runs_in_progress[run_to_start] = foreman
        foreman.start_run()

    # FIXME change this into a "watch Slurm queue for finished tasks" function
    def monitor_queue(self, time_to_stop):
        """
        Monitor the queue for completed tasks.

        When a task is finished, it's handed off to the appropriate Foreman for handling.
        """
        while time.time() < time_to_stop:
            # Watch for a finished task, hand it off to the Foreman to handle.
            pass

    def find_new_runs(self, time_to_stop):
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

            self.create_foreman(run_to_process)
            mgr_logger.info("Started run id %d, pipeline %s, user %s",
                            run_to_process.pk,
                            run_to_process.pipeline,
                            run_to_process.user)

            mgr_logger.debug("Task queue: {}".format(self.task_queue))
            mgr_logger.debug("Active runs: {}".format(self.runs_in_progress.keys()))

            if time.time() > time_to_stop:
                # We stop, to avoid possible starvation if new tasks are continually added.
                return

    def find_stopped_runs(self):
        """
        Look for currently running Runs that have been stopped by a user.
        """
        mgr_logger.debug("Looking for stopped runs....")
        just_stopped_runs = Run.objects.filter(end_time__isnull=True, stopped_by__isnull=False)

        for run_to_stop in just_stopped_runs:
            self.runs_in_progress[run_to_stop].stop_run()  # Foreman handles the stopping

    def find_finished_runs(self):
        """
        Check whether any of the Foremen have finished their runs and handle the final mop-up.
        """
        for run in self.runs_in_progress:
            run.refresh_from_db()

            if run.is_complete():
                # If this was already in the process of shutting down, remove the annotation.
                self.runs_shutting_down.discard(run)
                if self.history_queue.maxlen > 0:
                    self.history_queue.append(run)

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

            self.find_new_runs(time_to_poll)
            if time_to_purge is None or time_to_poll > time_to_purge:
                self.purge_sandboxes()
                Dataset.purge()
                time_to_purge = time_to_poll + settings.FLEET_PURGING_INTERVAL

            if self.quit_idle and not self.runs_in_progress:
                mgr_logger.info('Fleet is idle, quitting.')
                return

    def main_procedure(self):
        try:
            self.main_loop()
            mgr_logger.info("Manager shutting down.")
        except:
            mgr_logger.error("Manager failed.", exc_info=True)

        for foreman in self.runs_in_progress.itervalues():
            foreman.shut_down()

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

        # The run is already in the queue, so we can just start the manager and let it exit
        # when it finishes.
        manager = cls(quit_idle=True, history=1)
        manager.main_procedure()
        return manager

    def get_last_run(self):
        """
        Retrieve the last completed run from the history.

        If no history is retained, return None.
        """
        if self.history_queue.maxlen == 0 or len(self.history_queue) == 0:
            return None

        last_completed_run = self.history_queue.pop()
        return last_completed_run


class Foreman(object):
    """
    Coordinates the execution of a Run in a Sandbox.
    """
    def __init__(self, run):
        # tasks_in_progress tracks the Slurm IDs of currently running tasks:
        # task -|--> Slurm ID
        self.tasks_in_progress = {}
        self.sandbox = Sandbox(run=run)
        # A flag to indicate that this Foreman is in the process of terminating its Run and Sandbox.
        self.shutting_down = False

    def start_run(self):
        """
        Receive a request to start a pipeline running.
        """
        self.sandbox.advance_pipeline()

        # Refresh run_to_start.
        self.sandbox.run.refresh_from_db()

        # If we were able to reuse throughout, then we're totally done.  Otherwise we
        # need to do some bookkeeping.
        if self.sandbox.run.is_successful():
            foreman_logger.info(
                'Run "%s" (Pipeline: %s, User: %s) completely reused successfully',
                self.sandbox.run,
                self.sandbox.run.pipeline,
                self.sandbox.run.user
            )

        elif self.sandbox.run.is_failing() or self.sandbox.run.is_cancelling():
            # The run failed somewhere in preparation.  This hasn't affected any of our maps yet, so we
            # just report it and discard it.
            status_str = "failed" if self.sandbox.run.is_failing() else "cancelled"
            foreman_logger.info(
                'Run "%s" (pk=%d, Pipeline: %s, User: %s) %s before execution',
                self.sandbox.run,
                self.sandbox.run.pk,
                self.sandbox.run.pipeline,
                self.sandbox.run.user,
                status_str
            )
            self.sandbox.run.cancel_components()
            self.sandbox.run.stop(save=True)

        else:
            for task in self.sandbox.hand_tasks_to_fleet():
                # FIXME submit jobs directly to Slurm
                self.task_queue.append((self.sandbox, task))

        return self.sandbox

    def mop_up(self):
        """
        Mop up the sandbox after cancellation or failure.
        """
        # Mark this sandbox as in the process of shutting down.
        self.shutting_down = True

        # Cancel all parts of the run that aren't currently processing.
        steps_processing = []
        incables_processing = []
        outcables_processing = []
        for task in self.tasks_in_progress:
            if isinstance(task, RunStep):
                steps_processing.append(task)
            elif isinstance(task, RunSIC):
                incables_processing.append(task)
            else:
                outcables_processing.append(task)
        self.sandbox.run.cancel_components(
            except_steps=steps_processing,
            except_incables=incables_processing,
            except_outcables=outcables_processing
        )

    # FIXME this will submit the task to Slurm
    def assign_task(self, task):
        """
        Assign a task to a worker.
        """
        pass

    def worker_finished(self, finished_task, result):
        """Handle bookkeeping when a worker finishes."""
        if result == Worker.FAILURE:
            raise WorkerFailedException(
                "Task (pk={}, Slurm ID={}) failed".format(
                    finished_task.pk,
                    self.tasks_in_progress[finished_task]
                )
            )

        foreman_logger.info(
            "Run %s (pk=%d) reports task with PK %d is finished",
            self.sandbox.run,
            self.sandbox.run.pk,
            finished_task.pk
        )

        # Mark this task as having finished.
        task_execute_info = self.sandbox.get_task_info(finished_task)

        # Is anything from the run actively processing?
        tasks_currently_running = len(self.tasks_in_progress) > 0
        # Recall:
        # a RunStep gives you the step coordinates
        # a RunSIC gives you its parent step coordinates
        # a RunOutputCable gives you the parent run coordinates
        # finished_task_coords[idx] is the component coordinate in
        # the subrun idx levels deep (0 means top-level run).
        finished_task_coords = finished_task.get_coordinates()
        if finished_task.is_outcable():
            # Add a dummy entry at the end so that the 0th to 2nd-last coordinates
            # give the sub-run coordinates in all cases.
            finished_task_coords = finished_task_coords + (None,)

        # At position i, this list denotes whether any other tasks from the sub-run
        # i levels deep (0 means top level run) are currently running.
        subrun_tasks_currently_running = [False for _ in finished_task_coords]
        subrun_tasks_currently_running[0] = tasks_currently_running

        # for task_info in self.tasks_in_progress.itervalues():
        for running_task in self.tasks_in_progress:
            running_task_coords = running_task.get_coordinates()
            if running_task.is_outcable():
                running_task_coords = running_task_coords + (None,)

            # If either finished_task_coords and running_task_coords are length 1 (i.e. one
            # directly belongs to the top-level run), this does nothing.
            for coord in range(1, min(len(finished_task_coords), len(running_task_coords))):
                if finished_task_coords[coord] == running_task_coords[coord]:
                    subrun_tasks_currently_running[coord] = True
                else:
                    # Nothing nested deeper can belong to the same sub-run.
                    break

        # If this run has failed (either due to this task or another),
        # we mop up.
        clean_up_now = False
        self.sandbox.run.refresh_from_db()
        stop_subruns_if_possible = False

        if finished_task.is_successful():
            if self.sandbox.run.is_failing() or self.sandbox.run.is_cancelling():
                assert self.shutting_down
                mgr_logger.debug(
                    'Task %s (pk=%d) was successful but run "%s" (pk=%d) (Pipeline: %s, User: %s) %s.',
                    finished_task,
                    finished_task.pk,
                    self.sandbox.run,
                    self.sandbox.run.pk,
                    self.sandbox.pipeline,
                    self.sandbox.user,
                    "failing" if self.sandbox.run.is_failing() else "cancelling"
                )

                # Stop any sub-Runs (or the top-level run) that this was the last
                # running task of.
                stop_subruns_if_possible = True

                if not tasks_currently_running:
                    clean_up_now = True

            else:  # run is still processing successfully
                # Was this task a recovery or novel progress?
                if task_execute_info.is_recovery():
                    mgr_logger.debug(
                        'Recovering task %s (pk=%d) was successful; '
                        'queueing waiting tasks from run "%s" (pk=%d, Pipeline: %s, User: %s).',
                        finished_task,
                        finished_task.pk,
                        self.sandbox.run,
                        self.sandbox.run.pk,
                        self.sandbox.pipeline,
                        self.sandbox.user
                    )

                    execrecordouts = task_execute_info.execrecord.execrecordouts.all()
                    data_newly_available = [execrecordout.dataset
                                            for execrecordout in execrecordouts]
                    # Add anything that was waiting on this recovery to the queue.
                    self.sandbox.enqueue_runnable_tasks(data_newly_available)

                else:
                    mgr_logger.debug(
                        'Task %s (pk=%d) was successful; '
                        'advancing run "%s" (pk=%d, Pipeline: %s, User: %s).',
                        finished_task,
                        finished_task.pk,
                        self.sandbox.run,
                        self.sandbox.run.pk,
                        self.sandbox.pipeline,
                        self.sandbox.user
                    )

                    # Update maps and advance the pipeline.  Note that advance_pipeline
                    # will transition the states of runs and sub-runs appropriately.
                    self.sandbox.update_sandbox(finished_task)
                    self.sandbox.advance_pipeline(task_completed=finished_task)
                    self.sandbox.run.refresh_from_db()
                    if self.sandbox.run.is_successful():
                        clean_up_now = not tasks_currently_running
                        if clean_up_now and self.sandbox.run.is_successful():
                            mgr_logger.info(
                                'Run "%s" (pk=%d, Pipeline: %s, User: %s) finished successfully',
                                self.sandbox.run,
                                self.sandbox.run.pk,
                                self.sandbox.pipeline,
                                self.sandbox.user
                            )
                    elif self.sandbox.run.is_failing() or self.sandbox.run.is_cancelling():
                        # Something just failed in advance_pipeline.
                        mgr_logger.debug(
                            'Run "%s" (pk=%d, Pipeline: %s, User: %s) failed to advance '
                            'after finishing task %s (pk=%d)',
                            self.sandbox.run,
                            self.sandbox.run.pk,
                            self.sandbox.pipeline,
                            self.sandbox.user,
                            finished_task,
                            finished_task.pk
                        )

                        if not self.shutting_down:
                            self.mop_up()  # this sets self.shutting_down.
                        clean_up_now = not tasks_currently_running

        else:
            # The component that just finished failed or was cancelled (e.g. a RunCable fails to
            # copy the input to the sandbox).  Cancellation is handled by stop_run
            # (or assign_task).
            assert finished_task.is_failed() or finished_task.is_cancelled(), "{} != Failed or Cancelled".format(
                finished_task.get_state_name()
            )
            stop_subruns_if_possible = True
            if self.sandbox.run.is_failing() or self.sandbox.run.is_cancelling():
                assert self.shutting_down
                mgr_logger.debug(
                    'Task %s (pk=%d) %s; run "%s" (pk=%d, Pipeline: %s, User: %s) was already %s',
                    finished_task,
                    finished_task.pk,
                    finished_task.get_state_name(),
                    self.sandbox.run,
                    self.sandbox.run.pk,
                    self.sandbox.pipeline,
                    self.sandbox.user,
                    "failing" if self.sandbox.run.is_failing() else "cancelling"
                )

            else:
                assert self.sandbox.run.is_running(), "{} != Running".format(self.sandbox.run.get_state_name())
                mgr_logger.info(
                    'Task %s (pk=%d) of run "%s" (pk=%d, Pipeline: %s, User: %s) failed; '
                    'marking run as failing',
                    finished_task,
                    finished_task.pk,
                    self.sandbox.run,
                    self.sandbox.run.pk,
                    self.sandbox.pipeline,
                    self.sandbox.user
                )

                # Go through and mark all ancestor runs of finished_task as failing.
                self.sandbox.run.mark_failure(save=True)
                curr_ancestor_run = self.sandbox.run
                # This does nothing if finished_task_coords is of length 1; i.e. if it's a component belonging
                # to the top-level run.
                for coord in finished_task_coords[:-1]:
                    curr_ancestor_run = curr_ancestor_run.runsteps.get(pipelinestep__step_num=coord).child_run
                    if curr_ancestor_run.is_running():  # skip over this if it's cancelling or failing already
                        curr_ancestor_run.mark_failure(save=True)

                self.mop_up()  # this cancels the recovering record as well

            # Now check whether we can do our final clean up.
            if not tasks_currently_running:
                clean_up_now = True

        if stop_subruns_if_possible:

            curr_run = finished_task.parent_run
            for idx in range(len(finished_task_coords)-1, -1, -1):
                # finished_task_coords[idx] is the component coordinate in
                # the subrun idx levels deep (0 means top-level run).
                # curr_run is this subrun.
                curr_run.refresh_from_db()
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
            for ready_task in self.sandbox.hand_tasks_to_fleet():
                # FIXME make this a Slurm submission
                self.task_queue.append(ready_task)

        else:
            self.sandbox.run.refresh_from_db()
            mgr_logger.info(
                'Cleaning up %s run "%s" (pk=%d, Pipeline: %s, User: %s)',
                self.sandbox.run.get_state_name(),
                self.sandbox.run,
                self.sandbox.run.pk,
                self.sandbox.pipeline,
                self.sandbox.user
            )

            # Having reached here, this run should have been properly stopped in the
            # "if stop_subruns_if_possible" block.
            assert self.sandbox.run.is_complete(), \
                self.sandbox.run.get_state_name() + " is not one of the complete states"

    def stop_run(self):
        """
        Stop this Foreman's run.
        """
        mgr_logger.debug("Stopping run (pk=%d) on behalf of user %s",
                         self.run.pk,
                         self.run.stopped_by)

        if not self.run.has_started():
            self.run.start(save=True)

        if self.run.is_complete():
            # This run already completed, so we ignore this call.
            mgr_logger.warn("Run (pk=%d) is already complete; ignoring stop request.", self.run.pk)
            return

        else:
            # Attempt to stop any tasks that belong to this Run.
            for task, slurm_id in self.tasks_in_progress:
                # FIXME need a function that sends SIGTERM to the Slurm job using scancel.
                pass

            # Cancel all tasks on the task queue pertaining to this run, and finalize the
            # details.
            self.mop_up()

        self.run.cancel(save=True)
        self.run.stop(save=True)

        foreman_logger.debug("Run (pk={}) stopped by user {}".format(self.run.pk, self.run.stopped_by))

    def shut_down(self):
        for running_task in self.task_queue:
            # FIXME signal Slurm and stop the task from running.
            pass

    def change_priority(self, priority):
        pass


# This should now be a thing that's done in Slurm by srun.
# FIXME Need to watch for scancel rather than a shutdown message.
# scancel will send SIGTERM?
class Worker(object):
    """
    Performs the actual computational tasks required of Pipelines.
    """
    FINISHED = 1
    STOPPED = 2
    ERROR = 3

    def __init__(self, task_info_dict, slurm_id):
        self.slurm_id = slurm_id
        # adjust_log_files(worker_logger, self.rank)

        # Unpack task_info_dict.
        self.task_info_dict = task_info_dict

        if "runstep_pk" in task_info_dict:
            self.task_type = "RunStep"
            self.task = RunStep.objects.get(pk=task_info_dict["runstep_pk"])
        else:
            self.task_type = "RunCable"
            self.task_type = RunCable.objects.get(pk=task_info_dict["cable_record_pk"])

        worker_logger.debug(
            "Worker created to handle {} with pk={}".format(
                "RunStep" if "runstep_pk" in task_info_dict else "RunCable",
                (task_info_dict["runstep_pk"] if "runstep_pk" in task_info_dict
                 else task_info_dict["cable_record_pk"])
            )
        )

        self.task_info_dict = task_info_dict

    def write_summary(self, status, message, log_dir=None, summary_prefix="summary_"):
        result_dict = {
            "Slurm ID": self.slurm_id,
            "status": status,
            "message": message
        }

        summary_path = "{}_task{}.log".format(
            summary_prefix,
            self.task.pk
        )
        with open(os.path.join(log_dir, summary_path), "wb") as f:
            json.dump(result_dict, f)

    def perform_task(self):
        """
        Perform the assigned task.
        """
        try:
            if isinstance(self.task, RunStep):
                Sandbox.finish_step(
                    self.task_info_dict,
                    self.slurm_id,
                )
            else:
                Sandbox.finish_cable(self.task_info_dict, self.slurm_id)
            worker_logger.debug(
                "%s %s completed.  Returning results to Manager.",
                self.task.__class__.__name__,
                self.task
            )

        except StopExecution as e:
            # Execution was stopped during actual execution of code.
            message = "[{}] {} {} stopped ({}).".format(
                self.slurm_id,
                self.task_type,
                self.task,
                e
            )

            worker_logger.debug(
                message,
                exc_info=True
            )
            status = Worker.STOPPED

        except KeyboardInterrupt:
            # Execution was stopped somewhere outside of run_code (that would
            # have been caught above and raised a StopExecution).
            self.task.cancel(save=True)
            task_string = str(self.task)
            if isinstance(self.task, RunStep):
                task_string = "{} (method {})".format(
                    task_string,
                    self.task.pipelinestep.transformation.definite
                )
            message = "Execution of {} {} (method {}) was stopped during preamble/postamble.".format(
                self.task_type,
                task_string
            )
            worker_logger.debug(message)
            status = Worker.STOPPED

        except:
            status = Worker.ERROR  # bogus return value
            message = "[{}] Task {} failed.".format(
                self.slurm_id,
                self.task
            )
            worker_logger.error(message, exc_info=True)

        if isinstance(self.task, RunStep):
            self.write_summary(status, message, log_dir=self.task_info_dict["log_dir"])
        worker_logger.debug("Task {} completed.".format(self.task))


class WorkerFailedException(Exception):
    def __init__(self, error_msg):
        self.error_msg = error_msg


class NoWorkersAvailable(Exception):
    def __init__(self, error_msg):
        self.error_msg = error_msg
