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
import inspect

from django.conf import settings
from django.contrib.auth.models import User
from django.core.exceptions import ObjectDoesNotExist
from django.utils import timezone
from django.core.files import File
from django.db import transaction

from archive.models import Dataset, Run, RunStep, RunSIC, MethodOutput, ExecLog
import file_access_utils
from sandbox.execute import Sandbox, sandbox_glob
from fleet.slurmlib import SlurmScheduler, DummySlurmScheduler, BaseSlurmScheduler

mgr_logger = logging.getLogger("fleet.Manager")
foreman_logger = logging.getLogger("fleet.Foreman")


class ActiveRunsException(Exception):
    def __init__(self, count):
        super(ActiveRunsException, self).__init__(
            'Found {} active runs.'.format(count))
        self.count = count


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
            slurm_sched_class=SlurmScheduler,
            stop_username=None,
            no_stop=False):
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
            raise RuntimeError("Slurm is down or badly configured.")
        # log some slurm information
        mgr_logger.info("Slurm identifies as: '%s'" % self.slurm_sched_class.slurm_ident())

        if not no_stop:
            if stop_username is None:
                stop_user = None
            else:
                try:
                    stop_user = User.objects.get(username=stop_username)
                except ObjectDoesNotExist:
                    raise User.DoesNotExist(
                        'Username {!r} not found.'.format(stop_username))
            active_tasks = Run.objects.filter(start_time__isnull=False,
                                              end_time__isnull=True,
                                              stopped_by=None)
            for task in active_tasks:
                if stop_user is None:
                    raise ActiveRunsException(active_tasks.count())
                task.stopped_by = stop_user
                task.save()

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
            mgr_logger.debug("Running an idle task....")
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
            if run_to_process.all_inputs_have_data():
                # lets try and run this run
                foreman = Foreman(run_to_process, self.slurm_sched_class)
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
            else:
                # there is something wrong with the inputs (such as a maliciously moved input file)
                mgr_logger.info("Missing input for run id %d, pipeline %s, user %s: RUN BEING CANCELLED",
                                run_to_process.pk,
                                run_to_process.pipeline,
                                run_to_process.user)
                run_to_process.start(save=True)
                run_to_process.cancel(save=True)
                run_to_process.stop(save=True)
                run_to_process.refresh_from_db()
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
                    # Start anything that hadn't been started yet, i.e. in the Pending state.
                    if not run_to_stop.has_started():
                        run_to_stop.start(save=True)

                    # Cancel anything that is running (leave stuff that's Failing or already Cancelling).
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
            except OSError:
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
        self._add_idletask(Dataset.idle_external_file_check())
        # purge old files from Dataset:
        self._add_idletask(Dataset.idle_dataset_purge())
        # make Dataset sub-directories for next month
        self._add_idletask(Dataset.idle_create_next_month_upload_dir())
        # purge old log files
        self._add_idletask(MethodOutput.idle_logfile_purge())

        time_to_purge = None
        idle_counter = 0
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
                if idle_counter < settings.IDLE_TASK_FACTOR:
                    idle_counter += 1
                else:
                    self._do_idle_tasks(poll_until)
                    idle_counter = 0
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
                         slurm_sched_class=DummySlurmScheduler):
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

    def is_node_fail(self, job_state):
        return job_state == self.slurm_sched_class.NODE_FAIL

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
                if "bookkeeping" in task_dict:
                    our_slurm_jobs.append(task_dict["bookkeeping"])
                else:
                    our_slurm_jobs.append(task_dict["setup"])
                    our_slurm_jobs.append(task_dict["driver"])
        task_accounting_info = self.slurm_sched_class.get_accounting_info(our_slurm_jobs)

        # These flags reflect the status from the actual execution, not
        # the states of the RunComponents in the database.  (We may have to
        # use them to update said database states.)
        node_fail_delta = datetime.timedelta(seconds=settings.NODE_FAIL_TIME_OUT_SECS)
        terminated_during = ""
        still_running = []

        state_keyword = self.slurm_sched_class.ACC_STATE
        raw_state_keyword = self.slurm_sched_class.ACC_RAW_STATE_STRING
        start_keyword = self.slurm_sched_class.ACC_START_TIME
        end_keyword = self.slurm_sched_class.ACC_END_TIME
        return_code_keyword = self.slurm_sched_class.ACC_RETURN_CODE

        # self.tasks_in_progress may change during this loop, so we iterate over the keys.
        tasks = self.tasks_in_progress.keys()
        for task in tasks:
            task_dict = self.tasks_in_progress[task]
            raw_slurm_state = None
            failed = cancelled = is_node_fail = False
            # Check on the status of the jobs.
            if isinstance(task, RunStep):
                if "bookkeeping" not in task_dict:
                    setup_info = task_accounting_info.get(task_dict["setup"].job_id, None)
                    driver_info = task_accounting_info.get(task_dict["driver"].job_id, None)

                    setup_state = None
                    if setup_info is not None and setup_info[state_keyword] != BaseSlurmScheduler.UNKNOWN:
                        setup_state = setup_info[state_keyword]
                        raw_setup_state = setup_info[raw_state_keyword]

                    if setup_state is None or setup_state in self.slurm_sched_class.RUNNING_STATES:
                        # This is still going, so we move on.
                        still_running.extend([task_dict["setup"], task_dict["driver"]])
                        continue
                    elif setup_state in self.slurm_sched_class.CANCELLED_STATES:
                        cancel_end_time = setup_info[end_keyword]
                        if self.is_node_fail(setup_state):
                            expiry_time = cancel_end_time + node_fail_delta
                            now_time = datetime.datetime.now(timezone.get_current_timezone())
                            cancelled = expiry_time < now_time
                            foreman_logger.info("NODE_FAIL ({} vs {}):  {} < {} ==> {}".format(
                                node_fail_delta, now_time-cancel_end_time,
                                expiry_time, now_time, cancelled))
                            if not cancelled:
                                continue
                        else:
                            cancelled = True
                        terminated_during = "setup"
                        raw_slurm_state = raw_setup_state
                    elif setup_state in self.slurm_sched_class.FAILED_STATES:
                        # Something went wrong, so we get ready to bail.
                        failed = True
                        terminated_during = "setup"
                        raw_slurm_state = raw_setup_state
                    else:
                        assert setup_state in self.slurm_sched_class.SUCCESS_STATES, \
                            "Unexpected Slurm state: {} (raw Slurm state: {})".format(
                                setup_state,
                                raw_setup_state
                            )
                        # Having reached here, we know that setup is all clear, so check on the driver.
                        # Note that we don't check on whether it's in FAILED_STATES, because
                        # that will be handled in the bookkeeping stage.
                        driver_state = None
                        if (driver_info is not None and
                                driver_info[state_keyword] != BaseSlurmScheduler.UNKNOWN):
                            driver_state = driver_info[state_keyword]
                            raw_driver_state = driver_info[raw_state_keyword]
                        if driver_state is None or driver_state in self.slurm_sched_class.RUNNING_STATES:
                            still_running.append(task_dict["driver"])
                            continue
                        elif driver_state in self.slurm_sched_class.CANCELLED_STATES:
                            # This was externally cancelled, so we get ready to bail.
                            cancel_end_time = driver_info[end_keyword]
                            if self.is_node_fail(driver_state):
                                expiry_time = cancel_end_time + node_fail_delta
                                now_time = datetime.datetime.now(timezone.get_current_timezone())
                                cancelled = expiry_time < now_time
                                foreman_logger.info("NODE_FAIL ({} vs {}):  {} < {} ==> {}".format(
                                    node_fail_delta, now_time-cancel_end_time,
                                    expiry_time, now_time, cancelled))
                                if not cancelled:
                                    continue
                            else:
                                cancelled = True
                            terminated_during = "driver"
                            raw_slurm_state = raw_driver_state
                        elif driver_info[start_keyword] is not None and driver_info[end_keyword] is not None:
                            # Having reached here, we know that the driver ran to completion,
                            # successfully or no, and has the start and end times properly set.
                            # As such, we remove the wrapped driver if necessary
                            # and fill in the ExecLog.

                            # SCO do not remove for debugging..
                            # if hasattr(task_dict["driver"], "wrapped_driver_path"):
                                # driver_path = task_dict["driver"].wrapped_driver_path
                                # if os.path.exists(driver_path):
                                #    os.remove(task_dict["driver"].wrapped_driver_path)

                            # Weirdly, task.log doesn't appear to be set even if you refresh task from the database,
                            # so we explicitly retrieve it.
                            # noinspection PyUnresolvedReferences
                            task_log = ExecLog.objects.get(record=task)

                            with transaction.atomic():
                                task_log.start_time = driver_info[start_keyword]
                                task_log.end_time = driver_info[end_keyword]
                                task_log.methodoutput.return_code = driver_info[return_code_keyword]

                                step_execute_info = self.sandbox.step_execute_info[(task.parent_run, task.pipelinestep)]
                                # Find the stdout and stderr log files from their prefixes
                                # (since the full filename is produced using some Slurm macros).
                                stdout_pattern = os.path.join(
                                    step_execute_info.log_dir,
                                    "{}*.txt".format(step_execute_info.driver_stdout_path_prefix()))
                                stdout_log = glob.glob(stdout_pattern)
                                expected = "Expected 1 stdout log in {}, found {}".format(
                                    stdout_pattern,
                                    stdout_log)
                                assert len(stdout_log) == 1, expected

                                stderr_pattern = os.path.join(
                                    step_execute_info.log_dir,
                                    "{}*.txt".format(step_execute_info.driver_stderr_path_prefix()))
                                stderr_log = glob.glob(stderr_pattern)
                                expected = "Expected 1 stderr log in {}, found {}".format(
                                    stderr_pattern,
                                    stderr_log)
                                assert len(stderr_log) == 1, expected

                                with open(stdout_log[0], "rb") as f:
                                    task_log.methodoutput.output_log.save(f.name, File(f))
                                with open(stderr_log[0], "rb") as f:
                                    task_log.methodoutput.error_log.save(f.name, File(f))

                                task_log.methodoutput.save()
                                task_log.save()

                            # Now, we can submit the bookkeeping task, and the next time around we'll
                            # watch for it.
                            bookkeeping_job = self.submit_runstep_bookkeeping(task, task_dict["info_path"])
                            self.tasks_in_progress[task]["bookkeeping"] = bookkeeping_job
                            continue

                        else:
                            assert (driver_state in self.slurm_sched_class.FAILED_STATES
                                    or driver_state in self.slurm_sched_class.SUCCESS_STATES), \
                                "Unexpected Slurm state: {} (raw Slurm state: {})".format(
                                    driver_state,
                                    raw_driver_state
                                )

                            # The driver is finished, but sacct hasn't properly gotten the start and end times.
                            # For all intents and purposes, this is still running.
                            foreman_logger.debug(
                                "Driver of task %s appears complete but sacct hasn't set start/end times properly",
                                str(task)
                            )
                            foreman_logger.debug(
                                "sacct returned the following: %s",
                                str(driver_info)
                            )
                            still_running.append(task_dict["driver"])
                            continue

                else:
                    bookkeeping_info = task_accounting_info.get(task_dict["bookkeeping"].job_id, None)
                    # Check on the bookkeeping script.
                    bookkeeping_state = None
                    if (bookkeeping_info is not None and
                            bookkeeping_info[state_keyword] != BaseSlurmScheduler.UNKNOWN):
                        bookkeeping_state = bookkeeping_info[state_keyword]
                        raw_bookkeeping_state = bookkeeping_info[raw_state_keyword]
                    if bookkeeping_state is None or bookkeeping_state in self.slurm_sched_class.RUNNING_STATES:
                        still_running.append(task_dict["bookkeeping"])
                        continue
                    elif bookkeeping_state in self.slurm_sched_class.CANCELLED_STATES:
                        cancel_end_time = bookkeeping_info[end_keyword]
                        if self.is_node_fail(bookkeeping_state):
                            expiry_time = cancel_end_time + node_fail_delta
                            now_time = datetime.datetime.now(timezone.get_current_timezone())
                            cancelled = expiry_time < now_time
                            foreman_logger.info("NODE_FAIL ({} vs {}):  {} < {} ==> {}".format(
                                node_fail_delta, now_time-cancel_end_time,
                                expiry_time, now_time, cancelled))
                            if not cancelled:
                                continue
                        else:
                            cancelled = True
                        terminated_during = "bookkeeping"
                        raw_slurm_state = raw_bookkeeping_state
                    elif bookkeeping_state in self.slurm_sched_class.FAILED_STATES:
                        # Something went wrong, so we bail.
                        failed = True
                        terminated_during = "bookkeeping"
                        raw_slurm_state = raw_bookkeeping_state
                    else:
                        assert bookkeeping_state in self.slurm_sched_class.SUCCESS_STATES, \
                            "Unexpected Slurm state: {} (raw Slurm state: {})".format(
                                bookkeeping_state,
                                raw_bookkeeping_state
                            )

            else:
                cable_info = task_accounting_info.get(task_dict["cable"].job_id, None)
                cable_state = None
                if (cable_info is not None and
                        cable_info[state_keyword] != BaseSlurmScheduler.UNKNOWN):
                    cable_state = cable_info[state_keyword]
                    raw_cable_state = cable_info[raw_state_keyword]

                if cable_state is None or cable_state in self.slurm_sched_class.RUNNING_STATES:
                    # This is still going, so we move on.
                    still_running.append(task_dict["cable"])
                    continue
                elif cable_state in self.slurm_sched_class.CANCELLED_STATES:
                    cancel_end_time = cable_info[end_keyword]
                    if self.is_node_fail(cable_state):
                        expiry_time = cancel_end_time + node_fail_delta
                        now_time = datetime.datetime.now(timezone.get_current_timezone())
                        cancelled = expiry_time < now_time
                        foreman_logger.info("NODE_FAIL ({} vs {}):  {} < {} ==> {}".format(
                            node_fail_delta, now_time-cancel_end_time,
                            expiry_time, now_time, cancelled))
                        if not cancelled:
                            continue
                    else:
                        cancelled = True
                    terminated_during = "cable processing"
                    raw_slurm_state = raw_cable_state
                elif cable_state in self.slurm_sched_class.FAILED_STATES:
                    # Something went wrong, so we get ready to bail.
                    failed = True
                    terminated_during = "cable processing"
                    raw_slurm_state = raw_cable_state
                else:
                    assert cable_state in self.slurm_sched_class.SUCCESS_STATES, \
                        "Unexpected Slurm state: {} (raw Slurm state: {})".format(
                            cable_state,
                            raw_cable_state
                        )

            # Having reached here, we know we're done with this task.
            if is_node_fail:
                foreman_logger.debug('Run "%s" (pk=%d, Pipeline: %s, User: %s) NODE_FAIL while '
                                     'handling task %s (pk=%d) during %s (raw Slurm state: %s)',
                                     self.sandbox.run,
                                     self.sandbox.run.pk,
                                     self.sandbox.pipeline,
                                     self.sandbox.user,
                                     task,
                                     task.pk,
                                     terminated_during,
                                     raw_slurm_state)
            if failed or cancelled:
                foreman_logger.error(
                    'Run "%s" (Pipeline: %s, User: %s) %s while handling task %s (pk=%d) during %s '
                    '(raw Slurm state: %s)',
                    self.sandbox.run,
                    self.sandbox.pipeline,
                    self.sandbox.user,
                    "failed" if failed else "cancelled",
                    task,
                    task.pk,
                    terminated_during,
                    raw_slurm_state
                )

                task.refresh_from_db()
                if not task.is_complete():  # it may have been properly handled already in setup or bookkeeping
                    if failed:
                        assert task.has_started()
                        # Mark it as failed.
                        task.finish_failure()
                    else:
                        task.cancel()

            # At this point, the task has either run to completion or been
            # terminated either through failure or cancellation.
            # The worker_finished routine will handle it from here.
            self.tasks_in_progress.pop(task)
            self.worker_finished(task)

        # Lastly, check if the priority has changed.
        self.sandbox.run.refresh_from_db()
        priority_changed = self.priority != self.sandbox.run.priority
        self.priority = self.sandbox.run.priority
        if priority_changed:
            foreman_logger.debug(
                'Changing priority of Run "%s" (Pipeline: %s, User: %s) to %d',
                self.sandbox.run,
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
                'Run "%s" (Pipeline: %s, User: %s) %s before execution',
                self.sandbox.run,
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
        Submit a RunCable for processing.

        Return a dictionary containing the SlurmJobHandle for the cable helper,
        as well as the path of the execution info dictionary file used by the step.
        """
        cable_slurm_handle, cable_execute_dict_path = self.slurm_sched_class.submit_runcable(
            runcable,
            self.sandbox
        )
        return {
            "cable": cable_slurm_handle,
            "info_path": cable_execute_dict_path
        }

    def submit_runstep(self, runstep):
        """
        Submit a RunStep to Slurm for processing.

        A RunStep proceeds in four parts:
         - setup
         - driver
         - Foreman finishes the ExecLog
         - bookkeeping
        The first two parts, and the last part, are handled by tasks submitted
        to Slurm.  The third part is performed by the Foreman.  This function
        submits the first two, with a dependency so that
        the driver relies on the setup.

        Return a dictionary containing SlurmJobHandles for each, as well as
        the path of the execution info dictionary file used by the step.
        """
        # First, serialize the task execution information.
        step_info = self.sandbox.step_execute_info[(runstep.run, runstep.pipelinestep)]

        setup_slurm_handle, step_execute_dict_path = self.slurm_sched_class.submit_step_setup(
            runstep,
            self.sandbox
        )

        driver_slurm_handle = self.sandbox.submit_step_execution(
            step_info,
            after_okay=[setup_slurm_handle],
            slurm_sched_class=self.slurm_sched_class
        )

        return {
            "setup": setup_slurm_handle,
            "driver": driver_slurm_handle,
            "info_path": step_execute_dict_path
        }

    def submit_runstep_bookkeeping(self, runstep, info_path):
        """
        Submit the bookkeeping part of a RunStep.

        This is to be called after the driver part of a RunStep has been finished
        and the Foreman has completed the ExecLog.  It uses the same step execution
        information path as submit_runstep.
        """
        return self.slurm_sched_class.submit_step_bookkeeping(
            runstep,
            info_path,
            self.sandbox
        )

    def worker_finished(self, finished_task):
        """Handle bookkeeping when a worker finishes."""
        foreman_logger.info(
            "Run %s reports task with PK %d is finished",
            self.sandbox.run,
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
                    'Task %s (pk=%d) was successful but run "%s" (Pipeline: %s, User: %s) %s.',
                    finished_task,
                    finished_task.pk,
                    self.sandbox.run,
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
                        'queueing waiting tasks from run "%s" (Pipeline: %s, User: %s).',
                        finished_task,
                        finished_task.pk,
                        self.sandbox.run,
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
                        'advancing run "%s" (Pipeline: %s, User: %s).',
                        finished_task,
                        finished_task.pk,
                        self.sandbox.run,
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
                                'Run "%s" (Pipeline: %s, User: %s) finished successfully',
                                self.sandbox.run,
                                self.sandbox.pipeline,
                                self.sandbox.user
                            )
                    elif self.sandbox.run.is_failing() or self.sandbox.run.is_cancelling():
                        # Something just failed in advance_pipeline.
                        foreman_logger.debug(
                            'Run "%s" (Pipeline: %s, User: %s) failed to advance '
                            'after finishing task %s (pk=%d)',
                            self.sandbox.run,
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
                    'Task %s (pk=%d) %s; run "%s" (Pipeline: %s, User: %s) was already %s',
                    finished_task,
                    finished_task.pk,
                    finished_task.get_state_name(),
                    self.sandbox.run,
                    self.sandbox.pipeline,
                    self.sandbox.user,
                    "failing" if self.sandbox.run.is_failing() else "cancelling"
                )

            else:
                assert self.sandbox.run.is_running(), "{} != Running".format(self.sandbox.run.get_state_name())
                foreman_logger.info(
                    'Task %s (pk=%d) of run "%s" (Pipeline: %s, User: %s) failed; '
                    'marking run as failing',
                    finished_task,
                    finished_task.pk,
                    self.sandbox.run,
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
            foreman_logger.info(
                'Cleaning up %s run "%s" (Pipeline: %s, User: %s)',
                self.sandbox.run.get_state_name(),
                self.sandbox.run,
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
            mgr_logger.warn("Run (pk=%d) is already complete; ignoring stop request.",
                            self.sandbox.run.pk)
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

        foreman_logger.debug(
            "Run (pk={}) stopped by user {}".format(
                self.sandbox.run.pk,
                self.sandbox.run.stopped_by
            )
        )

    def cancel_all_slurm_jobs(self):
        """
        Cancel all Slurm jobs relating to this Foreman.
        """
        for task, task_dict in self.tasks_in_progress.iteritems():
            if isinstance(task, RunStep):
                parts_to_kill = ("setup", "driver")
                if "bookkeeping" in task_dict:
                    parts_to_kill = parts_to_kill + ("bookkeeping",)
                for job in parts_to_kill:
                    self.slurm_sched_class.job_cancel(task_dict[job])
            else:
                self.slurm_sched_class.job_cancel(task_dict["cable"])

            if os.path.exists(task_dict["info_path"]):
                os.remove(task_dict["info_path"])
