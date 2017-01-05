"""
Defines the manager and the "workers" that manage and carry out the execution of Pipelines.
"""

from collections import deque
import logging
import time
import datetime
import itertools
import os
import stat
import glob
import shutil
import json
import tempfile
import inspect

from django.conf import settings
from django.utils import timezone
from django.core.files import File
from django.db import transaction

from archive.models import Dataset, Run, RunStep, RunSIC, MethodOutput, ExecLog
import file_access_utils
from sandbox.execute import Sandbox, sandbox_glob
import fleet.slurmlib

mgr_logger = logging.getLogger("fleet.Manager")
foreman_logger = logging.getLogger("fleet.Foreman")
worker_logger = logging.getLogger("fleet.Worker")


MANAGE_PY = "manage.py"


class Manager(object):
    """
    Coordinates the execution of pipelines.

    The manager is responsible for handling new Run requests and
    creating Foreman objects to execute each one.
    """
    def __init__(
            self,
            quit_idle=False,
            history=0,
            slurm_sched_class=fleet.slurmlib.SlurmScheduler
    ):
        self.shutdown_exception = None
        self.quit_idle = quit_idle

        # This keeps track of runs and the order in which they were most recently
        # serviced.
        self.runs = deque()
        # This maps run -|-> foreman
        self.runs_in_progress = {}

        # A queue of recently-completed Sandboxes, to a maximum specified by history.
        self.history_queue = deque(maxlen=history)

        # A queue of functions to call during idle time.
        self.idle_job_queue = deque()

        self.slurm_sched_class = slurm_sched_class
        # when we start up, check to see whether slurm is running...
        # we do want to be able to switch this off, e.g. when running tests.
        slurm_is_ok = self.slurm_sched_class.slurm_is_alive()
        mgr_logger.info("Slurm is OK: %s" % slurm_is_ok)
        if not slurm_is_ok:
            mgr_logger.error("Slurm cannot be contacted, exiting")
            raise RuntimeError("Slurm is not running")
        # log some slurm information
        mgr_logger.info("Slurm identifies as: '%s'" % self.slurm_sched_class.slurm_ident())
        # also check for the existence of MANAGE_PY at the correct location.
        # If this file is not present, the sbatch commands will crash terribly
        manage_fp = os.path.join(settings.KIVE_HOME, MANAGE_PY)
        if not os.access(manage_fp, os.X_OK):
            mgr_logger.error("An executable '%s' was not found" % manage_fp)
            mgr_logger.error("settings.KIVE_HOME = %s", settings.KIVE_HOME)
            raise RuntimeError("'%s' not found" % manage_fp)
        mgr_logger.info("manager script found at '%s'" % manage_fp)

    def monitor_queue(self, time_to_stop):
        """
        Monitor the queue for completed tasks.

        When a task is finished, it's handed off to the appropriate Foreman for handling.
        """
        while True:  # this will execute at least one time to help with starvation
            try:
                run = self.runs.popleft()
            except IndexError:
                # There are no active runs.
                return
            try:
                foreman = self.runs_in_progress[run]
            except KeyError:
                raise RuntimeError("run in not found in runs_in_progress")

            foreman.monitor_queue()
            run.refresh_from_db()
            if run.is_complete():
                # All done, so remove it from our map.
                self.runs_in_progress.pop(run)
                if self.history_queue.maxlen > 0:
                    self.history_queue.append(foreman.sandbox)
            else:
                # Add it back to the end of the queue.
                self.runs.append(run)

            if time.time() > time_to_stop:
                break

    def _add_idletask(self, newidletask):
        """Add a task for the manager to perform during her idle time.
        The newidletask must be a generator that accepts a single argument of
        type comparable to time.time() when it calls (yield) .
        The argument provides the time limit after which a task must interrupt its task
        and wait for the next allotted time.

        For example, the structure of an idle task would typically be:

        def my_idle_task(some_useful_args):
           init_my_stuff()
           while True:
              time_to_stop = (yield)
              if (time.time() < time_to_stop) and something_todo():
                 do a small amount of work
        """
        if inspect.isgenerator(newidletask):
            # we prime the generator so that it advances to the first time that
            # it encounters a 'time_to_stop = (yield)' statement.
            newidletask.next()
            self.idle_job_queue.append(newidletask)
        else:
            raise RuntimeError("add_idletask: Expecting a generator as a task")

    def _do_idle_tasks(self, time_limit):
        """Perform the registered idle tasks once each, or until time runs out.
        Idle tasks are called in a round-robin fashion.
        """
        num_tasks = len(self.idle_job_queue)
        num_done = 0
        while (time.time() < time_limit) and num_done < num_tasks:
            mgr_logger.info("Running an idle task....")
            jobtodo = self.idle_job_queue[0]
            jobtodo.send(time_limit)
            self.idle_job_queue.rotate(1)
            num_done += 1

    def find_new_runs(self, time_to_stop):
        """
        Look for and begin processing new runs.
        """
        # Look for new jobs to run.  We will also
        # build in a delay here so we don't clog up the database.
        mgr_logger.debug("Looking for new runs....")
        pending_runs = Run.find_unstarted().order_by("time_queued").filter(parent_runstep__isnull=True)
        mgr_logger.debug("Pending runs: {}".format(pending_runs))

        for run_to_process in pending_runs:
            foreman = Foreman(run_to_process, self.slurm_sched_class)
            self.runs.append(run_to_process)
            self.runs_in_progress[run_to_process] = foreman
            foreman.start_run()

            run_to_process.refresh_from_db()
            if run_to_process.is_successful():
                # Well, that was easy.
                mgr_logger.info("Run id %d, pipeline %s, user %s completely reused",
                                run_to_process.pk,
                                run_to_process.pipeline,
                                run_to_process.user)
                if self.history_queue.maxlen > 0:
                    self.history_queue.append(foreman.sandbox)
            else:
                self.runs.append(run_to_process)
                self.runs_in_progress[run_to_process] = foreman
                mgr_logger.info("Started run id %d, pipeline %s, user %s",
                                run_to_process.pk,
                                run_to_process.pipeline,
                                run_to_process.user)

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
            if run_to_stop not in self.runs_in_progress:
                # This hasn't started yet, or is a remnant from a fleet crash/shutdown,
                # so we can just skip this one.
                mgr_logger.warn("Run (pk=%d) is not active.  Cancelling steps/cables that were unfinished.",
                                run_to_stop.pk)
                run_to_stop.cancel_components()

                with transaction.atomic():
                    if run_to_stop.is_running():
                        run_to_stop.cancel(save=True)
                    run_to_stop.stop(save=True)
                continue

            self.runs_in_progress[run_to_stop].stop_run()  # Foreman handles the stopping

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
        # add any idle tasks that should be performed in the mainloop here
        # --
        # check for consistency of external files:
        # self._add_idletask(lambda t: Dataset.idle_externalcheck(t))
        # --
        # purge old files from Dataset:
        self._add_idletask(Dataset.idle_dataset_purge())
        # make Dataset sub-directories for next month
        self._add_idletask(Dataset.idle_create_next_month_upload_dir())
        # purge old log files
        self._add_idletask(MethodOutput.idle_logfile_purge())

        time_to_purge = None
        while True:
            self.find_stopped_runs()

            poll_until = time.time() + settings.FLEET_POLLING_INTERVAL
            self.find_new_runs(poll_until)
            self.monitor_queue(poll_until)

            if time_to_purge is None or poll_until > time_to_purge:
                self.purge_sandboxes()
                Dataset.purge()
                time_to_purge = poll_until + settings.FLEET_PURGING_INTERVAL

            # Some jobs in the queue have been started:
            # if we have time, do some idle tasks until poll_until and
            # then check and see if anything has finished.
            if settings.DO_IDLE_TASKS and time.time() < poll_until:
                self._do_idle_tasks(poll_until)

            if self.quit_idle and not self.runs_in_progress:
                mgr_logger.info('Fleet is idle, quitting.')
                return

            try:
                time.sleep(settings.SLEEP_SECONDS)
            except KeyboardInterrupt:
                return

    def main_procedure(self):
        try:
            self.main_loop()
            mgr_logger.info("Manager shutting down.")
        except Exception as ex:
            mgr_logger.error("Manager failed.", exc_info=True)
            self.shutdown_exception = ex

        for foreman in self.runs_in_progress.itervalues():
            foreman.cancel_all_slurm_jobs()
        self.slurm_sched_class.shutdown()

    @classmethod
    def execute_pipeline(cls,
                         user,
                         pipeline,
                         inputs,
                         users_allowed=None,
                         groups_allowed=None,
                         name=None,
                         description=None,
                         slurm_sched_class=fleet.slurmlib.DummySlurmScheduler):
        """
        Execute the specified top-level Pipeline with the given inputs.

        This will create a run and start a fleet to run it.  This is mainly used for testing,
        and so a precondition is that sys.argv[1] is the management script used to invoke
        the tests.
        """
        if settings.FLEET_POLLING_INTERVAL >= 1:
            raise RuntimeError('FLEET_POLLING_INTERVAL has not been overridden.')
        file_access_utils.create_sandbox_base_path()

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
        manager = cls(quit_idle=True, history=1,
                      slurm_sched_class=slurm_sched_class)
        manager.main_procedure()
        return manager

    def get_last_run(self):
        """
        Retrieve the last completed run from the history.

        If no history is retained, return None.
        """
        if self.shutdown_exception is not None:
            raise self.shutdown_exception
        if self.history_queue.maxlen == 0 or len(self.history_queue) == 0:
            return None

        last_completed_sdbx = self.history_queue.pop()
        return last_completed_sdbx.run


class Foreman(object):
    """
    Coordinates the execution of a Run in a Sandbox.
    """
    def __init__(self, run, slurm_sched_class):
        # tasks_in_progress tracks the Slurm IDs of currently running tasks:
        # If the task is a RunStep:
        # task -|--> {
        #     "setup": [setup handle],
        #     "driver": [driver handle],
        #     "bookkeeping": [bookkeeping handle],
        #     "info_path": [path of file specifying the details needed for execution]
        # }
        # If the task is a RunCable:
        # task -|--> {
        #     "cable": [cable handle],
        #     "info_path": [path of file specifying details of execution]
        # }
        self.slurm_sched_class = slurm_sched_class
        self.tasks_in_progress = {}
        self.sandbox = Sandbox(run=run)
        # A flag to indicate that this Foreman is in the process of terminating its Run and Sandbox.
        self.shutting_down = False
        self.priority = run.priority

    def monitor_queue(self):
        """
        Look to see if any of this Run's tasks are done.
        """
        # For each task, get all relevant Slurm job handles.
        our_slurm_jobs = []
        for task_dict in self.tasks_in_progress.itervalues():
            if "cable" in task_dict:
                our_slurm_jobs.append(task_dict["cable"])
            else:
                our_slurm_jobs.append(task_dict["setup"])
                our_slurm_jobs.append(task_dict["driver"])
                our_slurm_jobs.append(task_dict["bookkeeping"])
        task_accounting_info = self.slurm_sched_class.get_accounting_info(our_slurm_jobs)

        # These flags reflect the status from the actual execution, not
        # the states of the RunComponents in the database.  (We may have to
        # use them to update said database states.)
        failed = False
        cancelled = False
        terminated_during = ""
        still_running = []

        # self.tasks_in_progress may change during this loop, so we iterate over the keys.
        tasks = self.tasks_in_progress.keys()
        for task in tasks:
            task_dict = self.tasks_in_progress[task]

            # Check on the status of the jobs.
            if isinstance(task, RunStep):
                setup_info = task_accounting_info.get(task_dict["setup"].job_id, None)
                driver_info = task_accounting_info.get(task_dict["driver"].job_id, None)
                bookkeeping_info = task_accounting_info.get(task_dict["bookkeeping"].job_id, None)

                setup_state = setup_info["state"] if setup_info is not None else None
                if setup_state is None or setup_state in self.slurm_sched_class.RUNNING_STATES:
                    # This is still going, so we move on.
                    still_running.extend([task_dict["setup"], task_dict["driver"], task_dict["bookkeeping"]])
                    continue
                elif setup_state in self.slurm_sched_class.CANCELLED_STATES:
                    cancelled = True
                    terminated_during = "setup"
                elif setup_state in self.slurm_sched_class.FAILED_STATES:
                    # Something went wrong, so we get ready to bail.
                    failed = True
                    terminated_during = "setup"

                else:
                    # Having reached here, we know that setup is all clear, so check on the driver.
                    # Note that we don't check on whether it's in FAILED_STATES, because
                    # that will be handled in the bookkeeping stage.
                    driver_state = driver_info["state"] if driver_info is not None else None
                    if driver_state is None or driver_state in self.slurm_sched_class.RUNNING_STATES:
                        still_running.extend([task_dict["driver"], task_dict["bookkeeping"]])
                        continue
                    elif driver_state in self.slurm_sched_class.CANCELLED_STATES:
                        # This was externally cancelled, so we get ready to bail.
                        cancelled = True
                        terminated_during = "driver"

                    else:
                        # Having reached here, we know that the driver ran to completion,
                        # successfully or no.  As such, we remove the wrapped driver if necessary
                        # and fill in the ExecLog.
                        # SCO do not remove for debugging..
                        # if hasattr(task_dict["driver"], "wrapped_driver_path"):
                            # driver_path = task_dict["driver"].wrapped_driver_path
                            # if os.path.exists(driver_path):
                            #    os.remove(task_dict["driver"].wrapped_driver_path)
                        task.refresh_from_db()
                        task_log = ExecLog.objects.get(record=task)  # weirdly, task.log doesn't appear to be set
                        with transaction.atomic():
                            task_log.start_time = driver_info["start_time"]
                            task_log.end_time = driver_info["end_time"]
                            task_log.methodoutput.return_code = driver_info["return_code"]

                            step_execute_info = self.sandbox.step_execute_info[(task.parent_run, task.pipelinestep)]
                            with open(step_execute_info.driver_stdout_path(), "rb") as f:
                                task_log.methodoutput.output_log.save(f.name, File(f))
                            with open(step_execute_info.driver_stderr_path(), "rb") as f:
                                task_log.methodoutput.error_log.save(f.name, File(f))

                            task_log.methodoutput.save()
                            task_log.save()

                        # Check on the bookkeeping script.
                        bookkeeping_state = bookkeeping_info["state"] if bookkeeping_info is not None else None
                        if bookkeeping_state is None or bookkeeping_state in self.slurm_sched_class.RUNNING_STATES:
                            still_running.append(task_dict["bookkeeping"])
                            continue
                        elif bookkeeping_state in self.slurm_sched_class.CANCELLED_STATES:
                            cancelled = True
                            terminated_during = "bookkeeping"
                        elif bookkeeping_state in self.slurm_sched_class.FAILED_STATES:
                            # Something went wrong, so we bail.
                            failed = True
                            terminated_during = "bookkeeping"

            else:
                cable_info = task_accounting_info.get(task_dict["cable"].job_id, None)

                cable_state = cable_info["state"] if cable_info is not None else None
                if cable_state is None or cable_state in self.slurm_sched_class.RUNNING_STATES:
                    # This is still going, so we move on.
                    still_running.append(task_dict["cable"])
                    continue
                elif cable_state in self.slurm_sched_class.CANCELLED_STATES:
                    cancelled = True
                    terminated_during = "cable processing"
                elif cable_state in self.slurm_sched_class.FAILED_STATES:
                    # Something went wrong, so we get ready to bail.
                    failed = True
                    terminated_during = "cable processing"

            # Having reached here, we know we're done with this task.
            if os.path.exists(task_dict["info_path"]):
                os.remove(task_dict["info_path"])
            if failed or cancelled:
                foreman_logger.error(
                    'Run "%s" (pk=%d, Pipeline: %s, User: %s) %s while handling task %s (pk=%d) during %s',
                    self.sandbox.run,
                    self.sandbox.run.pk,
                    self.sandbox.pipeline,
                    self.sandbox.user,
                    "failed" if failed else "cancelled",
                    task,
                    task.pk,
                    terminated_during
                )

                if failed:
                    assert task.has_started()
                    # Mark it as failed.
                    task.finish_failure()
                else:
                    task.cancel()

            self.tasks_in_progress.pop(task)

            # At this point, the task has either run to completion or been
            # terminated either through failure or cancellation.
            # The worker_finished routine will handle it from here.
            self.worker_finished(task)

        # Lastly, check if the priority has changed.
        self.sandbox.run.refresh_from_db()
        priority_changed = self.priority != self.sandbox.run.priority
        self.priority = self.sandbox.run.priority
        if priority_changed:
            foreman_logger.debug(
                'Changing priority of Run "%s" (pk=%d, Pipeline: %s, User: %s) to %d',
                self.sandbox.run,
                self.sandbox.run.pk,
                self.sandbox.pipeline,
                self.sandbox.user,
                self.priority
            )
            self.slurm_sched_class.set_job_priority(still_running, self.priority)

    def start_run(self):
        """
        Receive a request to start a pipeline running.
        This is the entry point for the foreman after having been created
        by the Manager.
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
            self.submit_ready_tasks()

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

    def submit_ready_tasks(self):
        """
        Go through the task queue, submitting all ready tasks to Slurm.
        """
        for task in self.sandbox.hand_tasks_to_fleet():
            if isinstance(task, RunStep):
                slurm_info = self.submit_runstep(task)
            else:
                slurm_info = self.submit_runcable(task)

            self.tasks_in_progress[task] = slurm_info

    def submit_runcable(self, runcable):
        """
        Submit a RunCable to Slurm for processing.

        This will use the cable helper management command defined in settings.

        Return a dictionary containing the SlurmJobHandle for the cable helper,
        as well as the path of the execution info dictionary file used by the step.
        """
        # First, serialize the task execution information.
        cable_info = self.sandbox.cable_execute_info[(runcable.parent_run, runcable.component)]
        # We need to get some information about the cable: the step that it feeds (RunSIC)
        # or that it's fed by (RunOutputCable), and the input/output that it feeds/is fed by.
        # We need this so that we can write the stderr and stdout to the appropriate locations.
        # cable_record = cable_info.cable_record

        # Submit the job.
        cable_execute_dict_fd, cable_execute_dict_path = tempfile.mkstemp()
        with os.fdopen(cable_execute_dict_fd, "wb") as f:
            f.write(json.dumps(cable_info.dict_repr()))

        fleet_settings = []
        if settings.FLEET_SETTINGS is not None:
            fleet_settings = ["--settings", settings.FLEET_SETTINGS]

        cable_slurm_handle = self.slurm_sched_class.submit_job(
            settings.KIVE_HOME,
            MANAGE_PY,
            [settings.CABLE_HELPER_COMMAND] + fleet_settings + [cable_execute_dict_path],
            self.sandbox.uid,
            self.sandbox.gid,
            self.sandbox.run.priority,
            cable_info.threads_required,
            cable_info.stdout_path,
            cable_info.stderr_path,
            job_name="run{}_cable{}".format(runcable.parent_run.pk, runcable.pk)
        )

        return {
            "cable": cable_slurm_handle,
            "info_path": cable_execute_dict_path
        }

    def submit_runstep(self, runstep):
        """
        Submit a RunStep to Slurm for processing.

        The RunStep will proceed in three parts:
         - setup
         - driver
         - bookkeeping
        all of which will be submitted to Slurm, with dependencies so that
        each relies on the last.

        Return a dictionary containing SlurmJobHandles for each, as well as
        the path of the execution info dictionary file used by the step.
        """
        fleet_settings = []
        if settings.FLEET_SETTINGS is not None:
            fleet_settings = ["--settings", settings.FLEET_SETTINGS]

        # First, serialize the task execution information.
        step_info = self.sandbox.step_execute_info[(runstep.run, runstep.pipelinestep)]

        # Submit a job for the setup.
        step_execute_dict_fd, step_execute_dict_path = tempfile.mkstemp()
        with os.fdopen(step_execute_dict_fd, "wb") as f:
            f.write(json.dumps(step_info.dict_repr()))

        # NOTE: wrapping the setup script is not required, but can be useful for debugging.
        dowrap = False
        if dowrap:
            # write a mini wrapper
            driver_template = """\
#! /usr/bin/env bash
# python -c "import time; print 'start time', time.time()"
# cd {}
# pwd
{} {} {}
# python -c "import time; print 'stop time', time.time()"
"""
            wrapped_driver_fd, wrapped_driver_path = tempfile.mkstemp(dir=step_info.step_run_dir,
                                                                      prefix="setty")
            # make the job script executable
            os.fchmod(wrapped_driver_fd, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
            with os.fdopen(wrapped_driver_fd, "wb") as f:
                f.write(
                    driver_template.format(
                        settings.KIVE_HOME,
                        MANAGE_PY,
                        settings.STEP_HELPER_COMMAND,
                        " ".join(fleet_settings + [step_execute_dict_path])
                    )
                )
            setup_slurm_handle = self.slurm_sched_class.submit_job(
                settings.KIVE_HOME,
                wrapped_driver_path,
                fleet_settings,
                self.sandbox.uid,
                self.sandbox.gid,
                self.sandbox.run.priority,
                step_info.threads_required,
                step_info.setup_stdout_path(),
                step_info.setup_stderr_path(),
                job_name="r{}s{}_setup".format(runstep.top_level_run.pk,
                                               runstep.get_coordinates())
            )
        else:
            setup_slurm_handle = self.slurm_sched_class.submit_job(
                settings.KIVE_HOME,
                MANAGE_PY,
                [settings.STEP_HELPER_COMMAND] + fleet_settings + [step_execute_dict_path],
                self.sandbox.uid,
                self.sandbox.gid,
                self.sandbox.run.priority,
                step_info.threads_required,
                step_info.setup_stdout_path(),
                step_info.setup_stderr_path(),
                job_name="r{}s{}_setup".format(runstep.top_level_run.pk,
                                               runstep.get_coordinates())
            )

        driver_slurm_handle = self.sandbox.submit_step_execution(
            step_info,
            after_okay=[setup_slurm_handle]
        )

        # Last, submit a job for the bookkeeping.
        bookkeeping_slurm_handle = self.slurm_sched_class.submit_job(
            settings.KIVE_HOME,
            MANAGE_PY,
            [settings.STEP_HELPER_COMMAND, "--bookkeeping"] + fleet_settings + [step_execute_dict_path],
            self.sandbox.uid,
            self.sandbox.gid,
            self.sandbox.run.priority,
            step_info.threads_required,
            step_info.bookkeeping_stdout_path(),
            step_info.bookkeeping_stderr_path(),
            after_any=[driver_slurm_handle],
            job_name="r{}s{}_bookkeeping".format(runstep.top_level_run.pk,
                                                 runstep.get_coordinates())
        )

        return {
            "setup": setup_slurm_handle,
            "driver": driver_slurm_handle,
            "bookkeeping": bookkeeping_slurm_handle,
            "info_path": step_execute_dict_path
        }

    def worker_finished(self, finished_task):
        """Handle bookkeeping when a worker finishes."""
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

        finished_task.refresh_from_db()
        if finished_task.is_successful():
            if self.sandbox.run.is_failing() or self.sandbox.run.is_cancelling():
                assert self.shutting_down
                foreman_logger.debug(
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
                    foreman_logger.debug(
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
                    foreman_logger.debug(
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
                            foreman_logger.info(
                                'Run "%s" (pk=%d, Pipeline: %s, User: %s) finished successfully',
                                self.sandbox.run,
                                self.sandbox.run.pk,
                                self.sandbox.pipeline,
                                self.sandbox.user
                            )
                    elif self.sandbox.run.is_failing() or self.sandbox.run.is_cancelling():
                        # Something just failed in advance_pipeline.
                        foreman_logger.debug(
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
                foreman_logger.debug(
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
                foreman_logger.info(
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
            self.submit_ready_tasks()

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
                         self.sandbox.run.pk,
                         self.sandbox.run.stopped_by)

        if not self.sandbox.run.has_started():
            self.sandbox.run.start(save=True)

        if self.sandbox.run.is_complete():
            # This run already completed, so we ignore this call.
            mgr_logger.warn("Run (pk=%d) is already complete; ignoring stop request.", self.run.pk)
            return

        else:
            self.cancel_all_slurm_jobs()
            self.tasks_in_progress.clear()
            # Mark all RunComponents as cancelled, and finalize the
            # details.
            self.mop_up()

        if self.sandbox.run.is_running():
            # No tasks are running now so there is nothing that could screw up the Run state
            # between the previous line and this line.
            self.sandbox.run.cancel(save=True)
        self.sandbox.run.stop(save=True)

        foreman_logger.debug("Run (pk={}) stopped by user {}".format(self.run.pk, self.run.stopped_by))

    def cancel_all_slurm_jobs(self):
        """
        Cancel all Slurm jobs relating to this Foreman.
        """
        for task, task_dict in self.tasks_in_progress.iteritems():
            if isinstance(task, RunStep):
                for job in ("setup", "driver", "bookkeeping"):
                    self.slurm_sched_class.job_cancel(task_dict[job])
            else:
                self.slurm_sched_class.job_cancel(task_dict["cable"])

            if os.path.exists(task_dict["info_path"]):
                os.remove(task_dict["info_path"])
