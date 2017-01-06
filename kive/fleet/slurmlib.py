# A low level interface to slurm using the calls to sbatch, scancel and squeue via Popen.

import os.path
import logging

import multiprocessing as mp
import Queue

import pytz
import re
import subprocess as sp
from datetime import datetime

from django.utils.timezone import get_default_timezone_name


logger = logging.getLogger("fleet.slurmlib")


class SlurmJobHandle:
    def __init__(self, job_id, slurm_sched_class):
        assert isinstance(job_id, str), "job_id must be a string!"
        self.job_id = job_id
        self.slurm_sched_class = slurm_sched_class

    def get_state(self):
        """ Get the current state of this job.
        The 'jobstate': value can be one of the predefined constants
        defined in SlurmScheduler:

        NOTE: If you want the states of many jobhandles at the same time, it is more
        efficient to use SlurmScheduler.get_accounting_info() directly.
        """
        rdct = self.slurm_sched_class.get_accounting_info([self])[self.job_id]
        return rdct['state']

    def __str__(self):
        return "slurm job_id {}".format(self.job_id)


class BaseSlurmScheduler:
    # All possible run states we expose to the outside. In fact, these are states as
    # reported by sacct.
    # These states will be reported by SlurmJobHandle.getstate() and
    # SlurmScheduler.get_accounting_info()
    # RUNNING, RESIZING, SUSPENDED, COMPLETED, CANCELLED, FAILED, TIMEOUT,
    # PREEMPTED, BOOT_FAIL, DEADLINE or NODE_FAIL
    BOOT_FAIL = "BOOT_FAIL"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"
    # CONFIGURING = "CONFIGURING"
    COMPLETING = "COMPLETING"
    DEADLINE = "DEADLINE"
    FAILED = "FAILED"
    NODE_FAIL = "NODE_FAIL"
    # PENDING = "PENDING"
    PREEMPTED = "PREEMPTED"
    RUNNING = "RUNNING"
    RESIZING = "RESIZING"
    SUSPENDED = "SUSPENDED"
    TIMEOUT = "TIMEOUT"
    PENDING = "PENDING"

    # include an unknown state (no accounting information available)
    UNKNOWN = 'UNKNOWN'

    RUNNING_STATES = set([PENDING, RUNNING, COMPLETING, PREEMPTED, RESIZING, SUSPENDED])
    CANCELLED_STATES = set([CANCELLED, BOOT_FAIL, DEADLINE, NODE_FAIL, TIMEOUT])
    FAILED_STATES = set([FAILED])
    SUCCESS_STATES = set([COMPLETED])

    ALL_STATES = RUNNING_STATES | CANCELLED_STATES | FAILED_STATES | SUCCESS_STATES | set([UNKNOWN])

    STOPPED_SET = ALL_STATES - RUNNING_STATES - set([UNKNOWN])

    FINISHED_SET = FAILED_STATES | SUCCESS_STATES

    @classmethod
    def slurm_is_alive(cls):
        """Return True if the slurm configuration is adequate for Kive's purposes."""
        raise NotImplementedError

    @classmethod
    def slurm_ident(cls):
        """Return a string with some pertinent information about the slurm configuration."""
        raise NotImplementedError

    @classmethod
    def shutdown(cls):
        """This routine should be called by the Manager when it exits the main loop."""
        pass

    @classmethod
    def submit_job(cls,
                   workingdir,
                   driver_name,
                   driver_arglst,
                   user_id,
                   group_id,
                   prio_level,
                   num_cpus,
                   stdoutfile,
                   stderrfile,
                   after_okay=None,
                   after_any=None,
                   job_name=None):
        """ Submit a job to the slurm queue.
        The executable submitted will be of the form:

        workingdir/driver_name arglst[0] arglst[1] ...

        workingdir (string): directory name of the job. slurm will set this to the
        'current directory' when the job is run.
        driver_name (string): name of the command to execute as the main job script.
        driver_arglst (list of strings): arguments to the driver_name executable.
        user_id, group_id (integers): the unix user under whose account the jobs will
        be executed .
        prio_level (integer) : a positive number >0. Higher values have higher priority.
        num_cpus: the number of CPU's (in a slurm sense) to reserve for this job.

        stdoutfile, stderrfile (strings or None): file names into which the job's
        std out and err streams will be written.

        after_okay: (list of jobhandles): the jobhandles on whose success this job depends.
        All of these previously submitted jobs must complete successfully before slurm
        will start this one.
        If a job on which this one is cancelled, this job will also be cancelled by slurm.

        after_any: (list of jobhandles): job handles which must all complete, successfully
        or not, before this job runs.

        If a list is provided for both after-any and after_ok, then the two conditions are
        combined using a logical AND operator, i.e. the currently submitted job will only run
        if both conditions are met.

        This method returns a slurmjobhandle on success, or raises a
        subprocess.CalledProcessError exception on an error.
        """
        raise NotImplementedError

    @classmethod
    def job_cancel(cls, jobhandle):
        """Cancel a given job given its jobhandle.
        Raise an exception if an error occurs, otherwise return nothing.
        """
        raise NotImplementedError

    @classmethod
    def get_accounting_info(cls, job_handle_iter=None):
        """
        Get detailed information via sacct, on the specified job(s).

        job_id_iter is an iterable that must contain job handles of previously
        submitted jobs.
        If this list is None, or empty, information about all jobs on the
        accounting system is returned.
        Note that, under slurm, when a job A that is dependent on a pending job B,
        in encountered, no accounting information for job A is available.

        Returns a dictionary which maps job IDs to a dictionary containing
        the following fields:
          - job_name (string)
          - job_id (string)
          - start_time (datetime object)
          - end_time (datetime object)
          - return_code (int)
          - state (string)
          - signal (int: the signal number that caused termination of this step, or 0 if
            it ended normally)
        """
        raise NotImplementedError

    @classmethod
    def set_job_priority(cls, jobhandle_lst, priority):
        """Set the priority of the specified jobs."""
        raise NotImplementedError


class SlurmScheduler(BaseSlurmScheduler):

    @classmethod
    def submit_job(cls,
                   workingdir,
                   driver_name,
                   driver_arglst,
                   user_id,
                   group_id,
                   prio_level,
                   num_cpus,
                   stdoutfile,
                   stderrfile,
                   after_okay=None,
                   after_any=None,
                   job_name=None):
        job_name = job_name or driver_name

        cmd_lst = ["sbatch", "-D", workingdir, "--gid={}".format(group_id),
                   "-J", re.escape(job_name), "--priority={}".format(prio_level),
                   "-s", "--uid={}".format(user_id),
                   "-c", str(num_cpus),
                   "--export=PYTHONPATH={}".format(workingdir)]
        # "--get-user-env",

        if stdoutfile:
            cmd_lst.append("--output=%s" % stdoutfile)
        if stderrfile:
            cmd_lst.append("--error=%s" % stderrfile)
        # handle dependencies. Note that sbatch can only have one --dependency option, or the second
        # one will overwrite the first one...
        # Note that here, multiple dependencies are always combined using an AND Boolean logic
        # (concatenation with a comma not a question mark) . See the sbatch man page for details.
        after_okay = after_okay if after_okay is not None else []
        after_any = after_any if after_any is not None else []
        if (len(after_okay) > 0) or (len(after_any) > 0):
            sdeplst = ["%s:%s" % (lstr, ":".join([jh.job_id for jh in lst])) for lst, lstr
                       in [(after_okay, 'afterok'), (after_any, 'afterany')] if len(lst) > 0]
            cmd_lst.extend(["--dependency=%s" % ",".join(sdeplst),
                            "--kill-on-invalid-dep=yes"])
        cmd_lst.append(os.path.join(workingdir, driver_name))
        cmd_lst.extend(driver_arglst)
        logger.debug(" ".join(cmd_lst))
        try:
            out_str = sp.check_output(cmd_lst)
        except sp.CalledProcessError as e:
            logger.error("sbatch returned an error code '%d'", e.returncode)
            logger.error("sbatch wrote this: '%s' ", e.output)
            raise
        if out_str.startswith("Submitted"):
            cl = out_str.split()
            try:
                job_id = cl[3]
            except:
                logger.error("sbatch completed with '%s'", out_str)
                raise RuntimeError("cannot parse sbatch output")
        else:
            logger.error("sbatch completed with '%s'", out_str)
            raise RuntimeError("cannot parse sbatch output")
        return SlurmJobHandle(job_id, cls)

    @classmethod
    def job_cancel(cls, jobhandle):
        """Cancel a given job given its jobhandle.
        Raise an exception if an error occurs, otherwise return nothing.
        """
        cmd_lst = ["scancel", "{}".format(jobhandle.job_id)]
        try:
            _ = sp.check_output(cmd_lst)
        except sp.CalledProcessError as e:
            logger.error("scancel returned an error code '%s'", e.returncode)
            logger.error("scancel wrote this: '%s' ", e.output)
            raise

    @classmethod
    def slurm_is_alive(cls):
        """Return True if the slurm configuration is adequate for Kive's purposes.
        We have two requirements:
        a) slurm control daemon can be reached (fur submitting jobs).
           This is tested by running 'squeue' and checking for exceptions.
        b) slurm accounting is configured properly.
           This is tested by running 'sacct' and checking for exceptions.
        """
        is_alive = True
        try:
            cls._do_squeue()
        except sp.CalledProcessError:
            is_alive = False
        logger.info("squeue passed: %s" % is_alive)

        if is_alive:
            try:
                cls.get_accounting_info()
            except:
                is_alive = False
            logger.info("sacct passed: %s" % is_alive)
        return is_alive

    @classmethod
    def slurm_ident(cls):
        """Return a string with some pertinent information about the slurm configuration."""
        cmd_lst = ["sinfo"]
        logger.debug(" ".join(cmd_lst))
        try:
            out_str = sp.check_output(cmd_lst)
        except sp.CalledProcessError as E:
            logger.error("sinfo returned an error code '%s'" % E.returncode)
            logger.error("sinfo wrote this: '%s' " % E.output)
            raise
        # NOTE: sinfo adds an empty line to the end of its output. Remove that here.
        lns = [ln for ln in out_str.split('\n') if ln]
        logger.debug("read %d lines" % len(lns))
        nametup = tuple([s.strip() for s in lns[0].split()])
        dctlst = [dict(zip(nametup, [s.strip() for s in ln.split()])) for ln in lns[1:]]
        info_str = ", ".join(["%s: %s: %s" % (dct['PARTITION'], dct['AVAIL'], dct['NODES']) for dct in dctlst])
        return 'Real Slurm: ' + info_str

    @classmethod
    def _do_squeue(cls, job_id_iter=None):
        """Get the status of jobs currently on the queue.
        NOTE: this is an internal helper routine, the user probably wants to
        use SlurmScheduler.get_job_states() to get states of a number of previously
        submitted slurm jobs.

        job_id_iter is an iterable that must contain job ids (strings) of previously
        submitted jobs.
        If this list is None, or empty, information about all jobs on the
        queue is returned.

        This routine returns a dict. of which the
        key: jobid (integer) and
        value :
        a dict containing a row from squeue output. The keys of this dict are
          the squeue column table headers:
          JOBID, PARTITION, NAME, USER, ST, TIME, NODES and 'NODELIST(REASON)'
          The values are the values from the respective row.
          NOTE: all of these values are returned 'as is', i.e. as strings, except for
          the 'JOBID' value, which is converted to an integer.

        See the squeue man pages for more information about these entries.
        """
        cmd_lst = ["squeue"]
        has_jlst = job_id_iter is not None and len(job_id_iter) > 0
        if has_jlst:
            cmd_lst.extend(["-j", ",".join(job_id_iter)])
        logger.debug(" ".join(cmd_lst))
        try:
            out_str = sp.check_output(cmd_lst)
        except sp.CalledProcessError as E:
            logger.error("squeue returned an error code '%s'" % E.returncode)
            logger.error("squeue wrote this: '%s' " % E.output)
            raise
        lns = [ln for ln in out_str.split('\n') if ln]
        logger.debug("read %d lines" % len(lns))
        namelst = [s.strip() for s in lns[0].split()]
        dctlst = [dict(zip(namelst, [s.strip() for s in ln.split()])) for ln in lns[1:]]
        retdct = dict((d['JOBID'], d) for d in dctlst)
        # NOTE: we should always return a dict entry for every jobid requested.
        # However, if the job queue is empty, or a job has finished,
        # squeue will not return information about it.
        # In those cases, set the dict['ST'] = 'UKN'  (to denote unknown)
        if has_jlst:
            for jobid in set(job_id_iter) - set(retdct.keys()):
                retdct[jobid] = {'JOBID': jobid, 'ST': cls._SLURM_STATE_UNKNOWN}
        return retdct

    @classmethod
    def get_accounting_info(cls, job_handle_iter=None):
        # The --parsable2 option creates parsable output: fields are separated by a pipe, with
        # no trailing pipe (the difference between --parsable2 and --parsable).
        cmd_lst = ["sacct", "--parsable2", "--format", "JobID,JobName,Start,End,State,Priority,ExitCode"]
        have_job_handles = job_handle_iter is not None and len(job_handle_iter) > 0
        if have_job_handles:
            cmd_lst.extend(["-j",
                            ",".join(["{}".format(handle.job_id) for handle in job_handle_iter])])
        logger.debug('Running command "{}"'.format(" ".join(cmd_lst)))
        try:
            sacct_output = sp.check_output(cmd_lst)
        except sp.CalledProcessError as e:
            logger.error("sacct returned an error code '%s'", e.returncode)
            logger.error("sacct output: '%s' ", e.output)
            raise

        lines = sacct_output.strip().split('\n')
        logger.debug("read %d lines (including header)", len(lines))
        name_tuple = tuple([s.strip() for s in lines[0].split("|")])
        accounting_info = {}

        for line in lines[1:]:  # skip the header line
            values = tuple([s.strip() for s in line.split("|")])
            raw_job_dict = dict(zip(name_tuple, values))

            # Pre-process the fields.
            job_id = raw_job_dict["JobID"]
            priority = int(raw_job_dict["Priority"])
            # Create proper DateTime objects with the following format string.
            date_format = "%Y-%m-%dT%H:%M:%S"
            curr_timezone = get_default_timezone_name()
            start_time = None
            if raw_job_dict["Start"] != "Unknown":
                start_time = datetime.strptime(raw_job_dict["Start"], date_format)
                start_time = pytz.timezone(curr_timezone).localize(start_time)
            end_time = None
            if raw_job_dict["End"] != "Unknown":
                end_time = datetime.strptime(raw_job_dict["End"], date_format)
                end_time = pytz.timezone(curr_timezone).localize(end_time)

            # Split sacct's ExitCode field, which looks like "[return code]:[signal]".
            return_code, signal = (int(x) for x in raw_job_dict["ExitCode"].split(":"))

            curstate = raw_job_dict["State"]
            if curstate not in cls.ALL_STATES:
                raise RuntimeError("received undefined state from sacct '%s'" % curstate)
            accounting_info[job_id] = {
                "job_name": raw_job_dict["JobName"],
                "start_time": start_time,
                "end_time": end_time,
                "return_code": return_code,
                "state": curstate,
                "signal": signal,
                "job_id": job_id,
                "priority": priority
            }
        # make sure all requested job handles have an entry...
        if have_job_handles:
            needset = set((jh.job_id for jh in job_handle_iter))
            gotset = set(accounting_info.keys())
            for missing_pid in needset - gotset:
                accounting_info[missing_pid] = {'job_name': "",
                                                'start_time': None,
                                                'end_time': None,
                                                'return_code': None,
                                                'state': BaseSlurmScheduler.UNKNOWN,
                                                'signal': None,
                                                'job_id': missing_pid,
                                                "priority": None}
        return accounting_info

    @classmethod
    def set_job_priority(cls, jobhandle_lst, priority):
        """Set the priority of the specified jobs."""
        if jobhandle_lst is None or len(jobhandle_lst) == 0:
            raise RuntimeError("no jobhandles provided")
        jhstr = ",".join([jh.job_id for jh in jobhandle_lst])
        # NOTE: setting 'Priority' instead of 'Nice' here results in a non-zero exit code
        cmd_list = ["scontrol", "update", "JobID={}".format(jhstr),
                    "Nice={}".format(priority)]
        try:
            _ = sp.check_output(cmd_list)
        except sp.CalledProcessError as e:
            logger.error("scontrol returned an error code '%s'", e.returncode)
            logger.error("scontrol wrote this: '%s' ", e.output)
            raise

sco_pid = 100


def startit(wdir, dname, arglst, stdout, stderr):
    """ Start a process with a command.
    NOTE: shell MUST be False here, otherwise the popen.wait() will NOT wait
    for completion of the command.
    """
    act_cmdstr = "cd %s;  ./%s  %s" % (wdir,
                                       dname,
                                       " ".join(arglst))
    # act_cmdstr = "%s/%s %s" % (wdir, dname, " ".join(arglst))
    cclst = ["/bin/bash", "-c", '%s' % act_cmdstr]
    p = sp.Popen(cclst, shell=False, stdout=stdout, stderr=stderr)
    return p


def callit(wdir, dname, arglst, stdout, stderr):
    popen = startit(wdir, dname, arglst, stdout, stderr)
    popen.wait()
    return popen.returncode


class workerproc:

    def __init__(self, jdct):
        self._jdct = jdct
        global sco_pid
        self.sco_pid = "%d" % sco_pid
        sco_pid += 1
        self.sco_retcode = None
        self.start_time = None
        self.end_time = None
        self.set_runstate(BaseSlurmScheduler.PENDING)
        self.prio = jdct["prio_level"]

    def do_run(self):
        """Invoke the code described in the _jdct"""
        self.set_runstate(BaseSlurmScheduler.RUNNING)
        j = self._jdct

        stdout = open(j["stdoutfile"], "w")
        stderr = open(j["stderrfile"], "w")
        self.popen = startit(j["workingdir"], j["driver_name"],
                             j["driver_arglst"], stdout, stderr)

    def check_ready_state(self, findct):
        """ Return a 2 tuple of Boolean:
        is_ready_to run, will_never_run
        """
        j = self._jdct
        after_any = j["after_any"]
        after_okay = j["after_okay"]
        # catch the most common case first: there are no dependencies
        any_cond = after_any is None
        okay_cond = after_okay is None
        if any_cond and okay_cond:
            return True, False
        any_cancel = okay_cancel = False
        finset = set(findct.iterkeys())
        if not any_cond:
            checkset = set([jhandle.job_id for jhandle in after_any])
            common_set = checkset & finset
            # if any jobs in common set are cancelled, we will never run
            any_cancel = any((findct[jid].iscancelled() for jid in common_set))
            any_cond = checkset <= finset
        if not okay_cond:
            checkset = set([jhandle.job_id for jhandle in after_okay])
            stat_dct = {BaseSlurmScheduler.CANCELLED: set(),
                        BaseSlurmScheduler.COMPLETED: set(),
                        BaseSlurmScheduler.FAILED: set()}
            common_set = checkset & finset
            for pid in common_set:
                proc = findct[pid]
                stat_dct[proc.get_runstate()].add(proc.sco_pid)
            ok_set = stat_dct[BaseSlurmScheduler.COMPLETED]
            okay_cancel = (stat_dct[BaseSlurmScheduler.FAILED] != set()) or\
                          (stat_dct[BaseSlurmScheduler.CANCELLED] != set())
            okay_cond = checkset <= ok_set
        has_cancel = any_cancel or okay_cancel
        if has_cancel:
            is_ready = False
        else:
            is_ready = any_cond and okay_cond
        return is_ready, has_cancel

    def do_cancel(self):
        if hasattr(self, "popen"):
            self.popen.kill()
        self.end_time = self.start_time = datetime.now()
        self.set_runstate(BaseSlurmScheduler.CANCELLED)

    def is_finished(self):
        self.popen.poll()
        self.sco_retcode = self.popen.returncode
        if self.sco_retcode is not None:
            self.my_state = BaseSlurmScheduler.COMPLETED if self.sco_retcode == 0 else BaseSlurmScheduler.FAILED
            return True
        else:
            return False

    def iscancelled(self):
        return self.get_runstate() in BaseSlurmScheduler.CANCELLED_STATES

    def get_runstate(self):
        return self.my_state

    def set_runstate(self, newstate):
        assert newstate in BaseSlurmScheduler.ALL_STATES, "illegal state '%s'" % newstate
        self.my_state = newstate

    def get_state_dct(self):
        j = self._jdct
        return {
            "job_name": j["job_name"],
            "start_time": self.start_time,
            "end_time": self.end_time,
            "return_code": self.sco_retcode,
            "state": self.get_runstate(),
            "signal": None,
            "job_id": self.sco_pid,
            "priority": self.prio
        }


class DummySlurmScheduler(BaseSlurmScheduler):

    mproc = None

    @staticmethod
    def _docancel(can_pid, waitdct, rundct, findct):
        """Cancel the job with the provided pid.
        return 0 iff successful.
        """
        if can_pid in waitdct:
            proc = waitdct.pop(can_pid)
            proc.do_cancel()
            findct[can_pid] = proc
            return 0
        if can_pid in rundct:
            proc = rundct.pop(can_pid)
            proc.do_cancel()
            findct[can_pid] = proc
            return 0
        if can_pid in findct:
            findct[can_pid].do_cancel()
            return 0
        return -1

    @staticmethod
    def _setprio(jidlst, prio, waitdct, rundct, findct):
        """ Set the priority levels of the jobs in jidlst.
        """
        jset = set(jidlst)
        for s, dct in [(set(dct.keys()) & jset, dct) for dct in [waitdct, rundct, findct]]:
            for jid in s:
                dct[jid].prio = prio
        # NOTE: we have not checked whether all elements in jidlst have been found.
        # ignore this for now
        return 0

    @staticmethod
    def masterproc(jobqueue, resultqueue):
        waitdct = {}
        rundct = {}
        findct = {}
        while True:
            try:
                jtup = jobqueue.get(block=False, timeout=1)
            except Queue.Empty:
                jtup = None
            if jtup is not None:
                assert isinstance(jtup, tuple), 'Tuple expected'
                assert len(jtup) == 2, 'tuple length 2 expected'
                cmd, payload = jtup
                if cmd == 'new':
                    # received a new submission
                    # create a worker process, but don't necessarily start it
                    newproc = workerproc(payload)
                    # return the job id of the submitted job
                    assert newproc.sco_pid is not None, "newproc pid is NONE"
                    waitdct[newproc.sco_pid] = newproc
                    resultqueue.put(newproc.sco_pid)
                elif cmd == 'query':
                    resultqueue.put(DummySlurmScheduler._getstates(payload, waitdct, rundct, findct))
                elif cmd == 'cancel':
                    resultqueue.put(DummySlurmScheduler._docancel(payload, waitdct, rundct, findct))
                elif cmd == 'prio':
                    jhlst, prio = payload
                    resultqueue.put(DummySlurmScheduler._setprio(jhlst, prio, waitdct, rundct, findct))
                else:
                    raise RuntimeError("masterproc: WEIRD request '%s'" % cmd)

            # lets update our worker dicts
            # first the waiting dct
            rdylst = []
            for pid, proc in waitdct.items():
                is_ready_to_run, will_never_run = proc.check_ready_state(findct)
                if will_never_run:
                    del waitdct[pid]
                    proc.do_cancel()
                    findct[pid] = proc
                if is_ready_to_run:
                    rdylst.append(proc)
            # start the procs in rdylst in order of priority (high first)
            for proc in sorted(rdylst, key=lambda p: p.prio, reverse=True):
                del waitdct[proc.sco_pid]
                proc.start_time = datetime.now()
                proc.do_run()
                rundct[proc.sco_pid] = proc
            # next, check the rundct
            for proc in [p for p in rundct.values() if p.is_finished()]:
                proc.end_time = datetime.now()
                del rundct[proc.sco_pid]
                findct[proc.sco_pid] = proc

    @staticmethod
    def _getstates(qset, waitdct, rundct, findct):
        """Given a query set of job_ids, return a dict of each job's state."""
        waitset = set(waitdct.keys())
        runset = set(rundct.keys())
        finset = set(findct.keys())
        if qset == set():
            qset = waitset | runset | finset
        wp_lst = []
        for s, dct in [(qset & waitset, waitdct),
                       (qset & runset, rundct),
                       (qset & finset, findct)]:
            wp_lst.extend([dct[pid] for pid in s])
        return dict([(wp.sco_pid, wp.get_state_dct()) for wp in wp_lst])

    @classmethod
    def _init_masterproc(cls):
        jq = cls._jobqueue = mp.Queue()
        rq = cls._resqueue = mp.Queue()
        cls.mproc = mp.Process(target=cls.masterproc, args=(jq, rq))
        cls.mproc.start()

    @classmethod
    def slurm_is_alive(cls):
        """Return True if the slurm configuration is adequate for Kive's purposes."""
        if cls.mproc is None:
            cls._init_masterproc()
        return True

    @classmethod
    def slurm_ident(cls):
        """Return a string with some pertinent information about the slurm configuration."""
        return "Dummy Slurm"

    @classmethod
    def submit_job(cls,
                   workingdir,
                   driver_name,
                   driver_arglst,
                   user_id,
                   group_id,
                   prio_level,
                   num_cpus,
                   stdoutfile,
                   stderrfile,
                   after_okay=None,
                   after_any=None,
                   job_name=None):

        if cls.mproc is None:
            cls._init_masterproc()

        # make sure the job script exists and is executable
        full_path = os.path.join(workingdir, driver_name)
        if not os.path.isfile(full_path):
            raise sp.CalledProcessError(cmd=full_path, output=None, returncode=-1)
        jdct = dict([('workingdir', workingdir),
                     ('driver_name', driver_name),
                     ('driver_arglst', driver_arglst),
                     ('user_id', user_id),
                     ('group_id', group_id),
                     ('prio_level', prio_level),
                     ('num_cpus', num_cpus),
                     ('stdoutfile', stdoutfile),
                     ('stderrfile', stderrfile),
                     ('after_okay', after_okay),
                     ('after_any', after_any),
                     ('job_name', job_name)])
        cls._jobqueue.put(('new', jdct))
        jid = cls._resqueue.get()
        return SlurmJobHandle(jid, cls)

    @classmethod
    def job_cancel(cls, jobhandle):
        """Cancel a given job given its jobhandle.
        Raise an exception if an error occurs, otherwise return nothing.
        """
        if cls.mproc is None:
            cls._init_masterproc()
        cls._jobqueue.put(('cancel', jobhandle.job_id))
        res = cls._resqueue.get()
        if res != 0:
            raise sp.CalledProcessError(returncode=res)

    @classmethod
    def get_accounting_info(cls, job_handle_iter=None):
        """
        Get detailed information, i.e. sacct, on the specified job(s).

        job_id_iter is an iterable that must contain job handles of previously
        submitted jobs.
        If this list is None, or empty, information about all jobs on the
        queue is returned.

        Returns a dictionary which maps job IDs to a dictionary containing
        the following fields:
          - job_name (string)
          - job_id (string)
          - start_time (datetime object)
          - end_time (datetime object)
          - return_code (int)
          - state (string)
          - signal (int: the signal number that caused termination of this step, or 0 if
            it ended normally)
        """
        if cls.mproc is None:
            cls._init_masterproc()
        if job_handle_iter is not None and len(job_handle_iter) > 0:
            query_set = set((jh.job_id for jh in job_handle_iter))
        else:
            query_set = set()
        cls._jobqueue.put(('query', query_set))
        accounting_info = cls._resqueue.get()
        return accounting_info

    @classmethod
    def set_job_priority(cls, jobhandle_lst, priority):
        """Set the priority of the specified jobs."""
        if cls.mproc is None:
            cls._init_masterproc()
        cls._jobqueue.put(('prio', ([jh.job_id for jh in jobhandle_lst], priority)))
        res = cls._resqueue.get()
        if res != 0:
            raise sp.CalledProcessError(returncode=res)

    @classmethod
    def shutdown(cls):
        cls.mproc.terminate()
        cls.mproc = None
