import os
import socket
import pwd
import time
import Queue
import multiprocessing as mp
import logging

from django.conf import settings

from fleet.workers import BaseManagerInterface, Worker, MPIFleetInterface
from slurmlib import SlurmScheduler, ret_dct


# currently, this has only be tested with one worker process
MY_NUM_WORKERS = 1

mgr_logger = logging.getLogger("fleet.slurmManager")
worker_logger = logging.getLogger("fleet.slurmWorker")


class SlurmWorkerInterface(MPIFleetInterface):
    """
    Object that is used by a Worker to communicate with the Manager.

    This handles setting up the MPI communicator.
    """
    def __init__(self, worker_rank, worker_q, man_q, mymanager):
        self.rank = worker_rank
        self._work_q = worker_q
        self._man_q = man_q
        self._mymanager = mymanager

    def report_for_duty(self):
        pass

    def stop_run_callback(self):
        pass

    def probe_for_task(self):
        """ return True is there is a job to do """
        return not self._work_q.empty()

    def get_rank(self):
        return self.rank

    def get_task_info(self):
        """ Get information about the next task to do. Block until we
        get something."""
        jdct, worker_rank, tag = self._work_q.get()
        self.rank = worker_rank
        worker_logger.debug("WORKER %d GOT a task" % worker_rank)
        jdct['SLURM_MANAGER'] = self._mymanager
        return jdct, tag

    def send_finished_task(self, message):
        """ tell the manager that we are finished.
        message contains a worker_rank and a result
        """
        rank, result = message
        worker_logger.debug("WIF: rank %d, result %d" % (rank, result))
        self._man_q.put((rank, result, Worker.FINISHED))
        # return self.comm.send(message, dest=0, tag=Worker.FINISHED)

    def record_exception(self, rank, task):
        worker_logger.error("[%d] Task %s failed.", self.rank, task, exc_info=True)

    def close(self):
        pass


FREE = 999
WORKING = 1001
STAGED = 2002
DONE = 3003


class SlurmManagerInterface(BaseManagerInterface):
    """ Implement the ManagerInterface for Slurm. """

    def __init__(self, *args, **kwrds):
        self._mysched = SlurmScheduler()
        if not self._mysched.Slurm_is_alive():
            mgr_logger.error("Cannot reach the slurmctl daemon")
            raise RuntimeError("Failed to contact slurmctl daemon. Exiting.")
        if settings.KIVE_SANDBOX_WORKER_ACCOUNT:
            pwd_info = pwd.getpwnam(settings.KIVE_SANDBOX_WORKER_ACCOUNT)
            self._sandbox_uid = pwd_info.pw_uid
            self._sandbox_gid = pwd_info.pw_gid
        else:
            # get our own current uid/hid
            self._sandbox_uid = os.getuid()
            self._sandbox_gid = os.getgid()

        self._myman = mp.Manager()
        self._workerq = self._myman.Queue()
        self._manq = self._myman.Queue()
        self._wdct = self._myman.dict()
        # self._proc = _pool.apply_async(worker_loop, (_workerq, self))
        # self._proc = mp.Process(target=worker_loop, args=(_workerq, _manq, self))
        self._myww = Worker(SlurmWorkerInterface(0, self._workerq, self._manq, self))
        self._proc = mp.Process(target=self._myww.main_procedure)
        self._proc.start()

    def _mark_start(self, new_handle, worker_rank, task_pk):
        self._wdct[worker_rank] = (WORKING, new_handle, task_pk)

    def _mark_staging(self, worker_rank):
        self._wdct[worker_rank] = (STAGED, 1, 1)

    def _set_worker_done(self, worker_rank, job_result, tag):
        if tag != Worker.FINISHED:
            raise RuntimeError("unexpected TAG %d" % tag)
        self._wdct[worker_rank] = (DONE, job_result, tag)

    def _set_worker_free(self, worker_rank):
        self._wdct[worker_rank] = (FREE, 1, 1)

    def _find_handle_of_worker(self, worker_rank):
        res = self._wdct.get(worker_rank, None)
        if res is None:
            return None
        else:
            state, handle, task_pk = res
            if state == WORKING:
                return handle
            else:
                return None

    def get_rank(self):
        return 0

    def get_size(self):
        return MY_NUM_WORKERS

    @staticmethod
    def get_hostname():
        return socket.gethostname()

    def send_task_to_worker(self, task_info, worker_rank):
        """Tell a worker to perform a task and return
        before it has finished.

        task_info is an instance of sandbox.execute.RunStepExecuteInfo

        execute_info = RunStepExecuteInfo(
            curr_RS,
            self.user,
            cable_info_list,
            None,
            step_run_dir,
            log_dir,
            output_paths
        )
        """
        jdct = task_info.dict_repr()
        jdct['DO_SLURM'] = True
        # print "JDCT", jdct
        # for now, just run as ourselves...
        jdct['SLURM_UID'] = self._sandbox_uid
        jdct['SLURM_GID'] = self._sandbox_gid
        jdct['SLURM_PRIO'] = 1
        jdct['SLURM_WORKER'] = worker_rank
        if task_info.threads_required is not None:
            num_cpus = task_info.threads_required
        else:
            num_cpus = 1
        jdct['SLURM_NCPU'] = num_cpus

        self._mark_staging(worker_rank)
        self._workerq.put((jdct, worker_rank, Worker.ASSIGNMENT))
        mgr_logger.debug("returning from send_task_to_worker, rank: %d" % worker_rank)

    def helper_invoke(self, worker_rank,
                      run_path, drivername, arglst,
                      stdoutfile, stderrfile,
                      exe_dict, precedent_job_lst):
        """ Run some code and wait until it finishes.
        This is run on a Worker and is meant to replace how a driver would
        be run within a sandbox (e.g. via ssh or Popen()).
        Return its exit_code
        """
        task_pk = exe_dict['runstep_pk']
        worker_logger.debug("helper: SUBMITTING driver '%s', task_pk %d" %
                            (drivername, task_pk))
        newhandle = self._mysched.submit_job(run_path, drivername, arglst,
                                             exe_dict['SLURM_UID'],
                                             exe_dict['SLURM_GID'],
                                             exe_dict['SLURM_PRIO'],
                                             exe_dict['SLURM_NCPU'],
                                             stdoutfile, stderrfile,
                                             precedent_job_lst)
        self._mark_start(newhandle, worker_rank, task_pk)
        worker_logger.debug("helper: AFTER SUBMISSION: taskpk %d, jobid %d" % (task_pk, newhandle._job_id))
        is_done = False
        while not is_done:
            time.sleep(5)
            cur_state = newhandle.get_state()
            is_done = cur_state in SlurmScheduler.STOPPED_SET
            worker_logger.debug("helper: WORKER WAITING FOR jobid %d (state =%s)" %
                                (newhandle._job_id, cur_state))
        retval = ret_dct[cur_state]
        worker_logger.debug("helper_invoke: Returning rank: %d, jobid %d, retval %d"
                            % (worker_rank, newhandle._job_id, retval))
        return retval

    def worker_is_available(self):
        """ Return a boolean value: a worker is available to do work. """
        return any([v[0] == FREE for v in self._wdct.values()])

    def a_task_is_finished(self):
        """ Return a boolean value: a worker has finished a job for whatever reason..
        Do not block.
        """
        # NOTE: this could be improved on for efficiency
        found_one = False
        while not self._manq.empty():
            found_one = True
            has_got = False
            try:
                worker_rank, worker_result, tag = self._manq.get_nowait()
                has_got = True
            except Queue.Empty:
                pass
            if has_got:
                mgr_logger.debug("got task_finished: rank %d, result %d" % (worker_rank, worker_result))
                self._set_worker_done(worker_rank, worker_result, tag)
        # --
        my_res = found_one or any([v[0] == DONE for v in self._wdct.values()])
        mgr_logger.debug("a task is finished: returning %d ", my_res)
        return my_res

    def BLA_translate_state(self, cur_state):
        """ Translate a sched.state into a Worker.state.... """
        trdct = {SlurmScheduler.PREC_WAIT: None,
                 SlurmScheduler.RESO_WAIT: None,
                 SlurmScheduler.RUNNING: None,
                 SlurmScheduler.RUN_FAILED: Worker.FAILURE,
                 SlurmScheduler.CANCELLED: Worker.FAILURE,
                 SlurmScheduler.SUCC_COMPLETED: Worker.FINISHED,
                 SlurmScheduler.UNKNOWN: None}
        return trdct[cur_state]

    def receive_finished(self):
        """ Return a worker_rank and a result_pk of a finished job...
        This should only be called if self.a_task_is_finished() returns True
        """
        done_worker_lst = [(w, v) for w, v in self._wdct.items() if v[0] == DONE]
        if len(done_worker_lst) == 0:
            # should never happen
            raise RuntimeError('receive_finished: no finished worker found')
        mgr_logger.debug("we have %d done workers" % len(done_worker_lst))
        worker_rank, worker_tup = done_worker_lst[0]
        self._set_worker_free(worker_rank)
        _state, job_result, tag = worker_tup
        mgr_logger.debug("receive finished: returning worker %d, result %d" % (worker_rank, job_result))
        return worker_rank, job_result

    def take_rollcall(self):
        """ For now, return one fictitious hostname with MY_NUM_WORKERS 'processors'. """
        mgr_logger.debug("ROLLCALL")
        procset = frozenset(range(MY_NUM_WORKERS))
        for i in procset:
            self._wdct[i] = (FREE, 1, 1)
        return {'slurmyhost': procset}

    def stop_run(self, foreman):
        """
        Instructs the foreman to stop the task.  Blocks while waiting for a response.
        """
        # find the slurm jobid with worker_rank == foreman
        job_handle = self._find_handle_of_worker(foreman)
        if job_handle is not None:
            j_state = job_handle.getstate()
            if j_state not in SlurmScheduler.STOPPED_SET:
                # cancel the job and wait for it to finish
                self._mysched.job_cancel(job_handle)
                is_done = False
                while not is_done:
                    time.sleep(5)
                    j_state = job_handle.get_state()
                    is_done = (j_state in SlurmScheduler.STOPPED_SET)
                    mgr_logger.debug("WAITING FOR slurmid %d (state=%s) " % (job_handle._job_id, j_state))
            self._set_worker_free(foreman, ret_dct[j_state])

    def record_exception(self):
        mgr_logger.error("Manager failed.", exc_info=True)

    def shut_down_fleet(self):
        mgr_logger.debug("Pretending to shut_down_fleet...")
