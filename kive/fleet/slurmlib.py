
# a low level interface to slurm using the calls to sbatch, scancel and squeue via Popen.

import os.path
import subprocess as sp
import multiprocessing as mp
import Queue

from datetime import datetime
import re

import logging

logger = logging.getLogger("fleet.slurmlib")


class SlurmJobHandle:
    def __init__(self, job_id, slurm_sched_class):
        self.job_id = job_id
        self.slurm_sched_class = slurm_sched_class

    def get_state(self):
        """ Get the current state of this job.
        The 'jobstate': value can be one of the following predefined constants
        defined in SlurmScheduler:

        PREC_WAIT: preceding jobs need to be successfully completed
                   before this one can be run.

        RESO_WAIT: The preceding jobs have completed successfully,
                   and the jobs is waiting in the job queue
                   for computational resources.
        RUNNING:   This job is under way.

        RUN_FAILED:    This job failed during its execution.
                   'exit_code' will contain an exit code (int)
                   'failed_reason' will contain a descriptive string of the error.

        CANCELLED: This job was cancelled 'by the user' after it was submitted or
                   did not run because a preceding run failed to
                   complete successfully.

        SUCC_COMPLETED: This jobs has successfully completed (return code of zero).

        NOTE: If you want the states of many jobhandles at the same time, it is more
        efficient to use SlurmScheduler.get_job_states() directly.

        """
        # return self.slurm_sched_class.get_job_states([self])[0]
        return self.slurm_sched_class.get_accounting_info([self])[self.job_id]

    def __str__(self):
        return "slurm job_id {}".format(self.job_id)


class BaseSlurmScheduler:
    # All possible run states we expose to the outside
    # These states will be reported by SlurmJobHandle.getstate() and
    # SlurmScheduler.get_job_states()
    PREC_WAIT = 'PREC_WAIT'
    RESO_WAIT = 'RESO_WAIT'
    RUNNING = 'RUNNING'
    RUN_FAILED = 'RUN_FAILED'
    CANCELLED = 'CANCELLED'
    SUCC_COMPLETED = 'SUCC_COMPLETED'
    UNKNOWN = 'UNKNOWN_STATE'

    STILL_RUNNING_SET = frozenset([PREC_WAIT, RESO_WAIT, RUNNING])
    # stopped: stopped for whatever reason
    STOPPED_SET = frozenset([RUN_FAILED, CANCELLED, SUCC_COMPLETED])

    # NOTE: These are runstates for internal use. They are strings as reported by squeue
    # PD (pending), R (running), CA (cancelled), CF(configuring), CG (completing),
    # CD  (completed),  F (failed), TO (timeout), NF (node failure) and SE (special exit state)
    _SLURM_STATE_PENDING = 'PD'
    _SLURM_STATE_RUNNING = 'R'
    _SLURM_STATE_CANCELLED = 'CA'
    _SLURM_STATE_CONFIGURING = 'CF'
    _SLURM_STATE_COMPLETING = 'CG'
    _SLURM_STATE_COMPLETED = 'CD'
    _SLURM_STATE_FAILED = 'F'
    _SLURM_STATE_TIME_OUT = 'TO'
    _SLURM_STATE_NODE_FAILURE = 'NF'
    _SLURM_STATE_SPECIAL_EXIT = 'SE'
    # add this one for internal bookkeeping
    _SLURM_STATE_UNKNOWN = 'UKN'

    _slurm_state_set = frozenset([_SLURM_STATE_PENDING, _SLURM_STATE_RUNNING,
                                  _SLURM_STATE_CANCELLED, _SLURM_STATE_CONFIGURING,
                                  _SLURM_STATE_COMPLETING, _SLURM_STATE_COMPLETED,
                                  _SLURM_STATE_FAILED, _SLURM_STATE_TIME_OUT,
                                  _SLURM_STATE_NODE_FAILURE, _SLURM_STATE_SPECIAL_EXIT,
                                  _SLURM_STATE_UNKNOWN])
    _slurm_run_set = frozenset([_SLURM_STATE_RUNNING, _SLURM_STATE_CONFIGURING,
                                _SLURM_STATE_COMPLETING])
    _slurm_failed_set = frozenset([_SLURM_STATE_FAILED, _SLURM_STATE_TIME_OUT,
                                   _SLURM_STATE_NODE_FAILURE, _SLURM_STATE_SPECIAL_EXIT])
    _trans_dct = {'FAILED': _SLURM_STATE_FAILED,
                  'COMPLETED': _SLURM_STATE_COMPLETED,
                  'CANCELLED': _SLURM_STATE_CANCELLED}

    _myjobdct = {}
    _slurm_jobcomp_file = "/var/log/slurm-llnl/job_completions"
    _last_file_pos = 0

    # A lookup that maps Slurm completion states to integers.
    ret_dct = {
        CANCELLED: -2,
        RUN_FAILED: -1,
        SUCC_COMPLETED: 0
    }

    # States as reported by sacct.
    # RUNNING, RESIZING, SUSPENDED, COMPLETED, CANCELLED, FAILED, TIMEOUT, PREEMPTED, BOOT_FAIL, DEADLINE or NODE_FAIL
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

    RUNNING_STATES = set([PENDING, RUNNING, COMPLETING, PREEMPTED, RESIZING, SUSPENDED])
    CANCELLED_STATES = set([CANCELLED, BOOT_FAIL, DEADLINE, NODE_FAIL, TIMEOUT])
    FAILED_STATES = set([FAILED])

    @classmethod
    def _slurmtosched_state(cls, slurm_job_state, job_reason=None):
        """Translate the SLURM job state into a Scheduler job state """
        if slurm_job_state not in cls._slurm_state_set:
            raise RuntimeError('illegal slurm state')

        if slurm_job_state in cls._slurm_run_set:
            return cls.RUNNING
        elif slurm_job_state in cls._slurm_failed_set:
            return cls.RUN_FAILED
        elif slurm_job_state == cls._SLURM_STATE_PENDING:
            # NOTE: there can be a number of reasons, including
            # "(Resources)", ('Priority') and ('None')
            # just map everything except (dependency) to resource wait
            if job_reason == '(Dependency)':
                return cls.PREC_WAIT
            else:
                return cls.RESO_WAIT
        elif slurm_job_state == cls._SLURM_STATE_CANCELLED:
            return cls.CANCELLED
        elif cls._SLURM_STATE_COMPLETED:
            return cls.SUCC_COMPLETED
        else:
            logger.error("slurm_job_state: '%s'" % slurm_job_state)
            raise RuntimeError('failed translation')

    @classmethod
    def slurm_is_alive(cls):
        """Return True if the slurm configuration is adequate for Kive's purposes."""
        raise NotImplementedError

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
        Get detailed information, i.e. sacct, on the specified job(s).

        job_id_iter is an iterable that must contain job handles of previously
        submitted jobs.
        If this list is None, or empty, information about all jobs on the
        queue is returned.

        Returns a dictionary which maps job IDs to a dictionary containing
        the following fields:
          - job_name (string)
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
            sdeplst = ["%s:%s" % (lstr, ":".join(["%d" % jh.job_id for jh in lst])) for lst, lstr
                       in [(after_okay, 'afterok'), (after_any, 'afterany')] if len(lst) > 0]
            cmd_lst.append("--dependency=%s" % ",".join(sdeplst))
            cmd_lst.append("--kill-on-invalid-dep=yes")

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
                job_id = int(cl[3])
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
    def _do_squeue(cls, job_id_iter=None):
        """Get the status of jobs currently on the queue.
        NOTE: this is an internal helper routine, the user probably wants to
        use SlurmScheduler.get_job_states() to get states of a number of previously
        submitted slurm jobs.

        job_id_iter is an iterable that must contain job ids (integers) of previously
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
        if job_id_iter is not None and len(job_id_iter) > 0:
            cmd_lst.append("-j")
            cmd_lst.append(",".join(["%d" % id for id in job_id_iter]))
        logger.debug(" ".join(cmd_lst))
        try:
            out_str = sp.check_output(cmd_lst)
        except sp.CalledProcessError as E:
            logger.error("squeue returned an error code '%s'" % E.returncode)
            logger.error("squeue write this: '%s' " % E.output)
            raise
        lns = out_str.split('\n')
        logger.debug("read %d lines" % len(lns))
        nametup = tuple([s.strip() for s in lns[0].split()])
        retdct = {}
        i = 1
        while i < len(lns)-1:
            valtup = tuple([s.strip() for s in lns[i].split()])
            newdct = dict(zip(nametup, valtup))
            # convert the jobid string into an integer
            jobid = newdct['JOBID'] = int(newdct['JOBID'])
            retdct[jobid] = newdct
            i += 1
        # NOTE: we should always return a dict entry for every jobid requested.
        # However, if the job queue is empty, or a job has finished,
        # squeue will not return information about it.
        # In those cases, set the dict['ST'] = 'UKN'  (to denote unknown)
        if job_id_iter is not None and len(job_id_iter) > 0:
            missing_set = set(job_id_iter) - set(retdct.keys())
            for jobid in missing_set:
                retdct[jobid] = {'JOBID': jobid, 'ST': cls._SLURM_STATE_UNKNOWN}
        return retdct

    @classmethod
    def get_accounting_info(cls, job_handle_iter=None):
        # The --parsable2 option creates parsable output: fields are separated by a pipe, with
        # no trailing pipe (the difference between --parsable2 and --parsable).
        cmd_lst = ["sacct", "--parsable2", "--format", "JobID,JobName,Start,End,State,ExitCode"]
        if job_handle_iter is not None and len(job_handle_iter) > 0:
            cmd_lst.append("-j")
            cmd_lst.append(",".join(["{}".format(handle.job_id) for handle in job_handle_iter]))
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
            job_id = int(raw_job_dict["JobID"])

            # Create proper DateTime objects with the following format string.
            date_format = "%Y-%m-%dT%H:%M:%S"
            start_time = None
            if raw_job_dict["Start"] != "Unknown":
                start_time = datetime.strptime(raw_job_dict["Start"], date_format)
            end_time = None
            if raw_job_dict["End"] != "Unknown":
                end_time = datetime.strptime(raw_job_dict["End"], date_format)

            # Split sacct's ExitCode field, which looks like "[return code]:[signal]".
            return_code, signal = (int(x) for x in raw_job_dict["ExitCode"].split(":"))

            accounting_info[job_id] = {
                "job_name": raw_job_dict["JobName"],
                "start_time": start_time,
                "end_time": end_time,
                "return_code": return_code,
                "state": raw_job_dict["State"],
                "signal": signal
            }

        return accounting_info

    @classmethod
    def OLDget_job_states(cls, jobhandle_lst):
        """ Return a list of job states. Each element i in this list corresponds
        to the state of jobhandle i.

        Strategy:
        a) see if we know already (this is quick)
        b) see if squeue knows (do not update our own table from this, as the state
           is likely to change quite soon.
        c) see if slurm accounting knows. Also update our table at this time.
        d) give up and return an unknown state.
        """
        # we keep track of those job_ids that we are still looking for in a tofind_set
        # we keep the found states in a found_dct
        tofind_set = set([j.job_id for j in jobhandle_lst])
        # try a)
        common_set = tofind_set & set(cls._myjobdct.keys())
        found_dct = dict([itm for itm in cls._myjobdct.items() if itm[0] in common_set])
        tofind_set -= common_set
        if tofind_set:
            # try b)
            squeue_dct = cls._do_squeue(tofind_set)
            for job_id, job_state_dct in squeue_dct.iteritems():
                job_state = job_state_dct['ST']
                job_reason = job_state_dct.get('NODELIST(REASON)', None)
                if job_state != cls._SLURM_STATE_UNKNOWN:
                    found_dct[job_id] = cls._slurmtosched_state(job_state, job_reason)
                    tofind_set.remove(job_id)
        if tofind_set:
            # try c)
            # NOTE: as slurm is writing to this file as we are reading from it,
            # we might get a malformed line, which we should just ignore
            # In addition, we keep track of the position after the last successfully
            # read line, so that next time, we can start from there...
            # NOTE that this strategy will fail if the logfile file is rotated by some
            # cron job or such-like.
            # NOTE: an example of an accounting line is:
            # JobId=223 UserId=walter(1005) GroupId=walter(1005) Name=sleep01.sh \
            #    JobState=COMPLETED Partition=scotest TimeLimit=UNLIMITED \
            #    StartTime=2016-10-25T10:04:21 EndTime=2016-10-25T10:04:31 NodeList=Nibbler \
            #    NodeCnt=1 ProcCnt=12 WorkDir=/home/walter/scotesting/slurm-test-01
            # we just extract JobId and JobState.
            with open(cls._slurm_jobcomp_file, "r") as fi:
                fi.seek(cls._last_file_pos)
                line_is_ok = True
                ll = fi.readline()
                while ll and line_is_ok:
                    clst = ll.split()
                    jobid_str = clst[0]
                    state_str = clst[4]
                    if jobid_str.startswith('JobId'):
                        jlst = jobid_str.split("=")
                        ljob_id = int(jlst[1])
                    else:
                        line_is_ok = False
                    if state_str.startswith('JobState'):
                        jlst = state_str.split("=")
                        ljob_state = cls._slurmtosched_state(cls._trans_dct[jlst[1]])
                    else:
                        line_is_ok = False
                    if line_is_ok:
                        logger.debug("adding from file: %d %s" % (ljob_id, ljob_state))
                        cls._myjobdct[ljob_id] = ljob_state
                        found_dct[ljob_id] = ljob_state
                        cls._last_file_pos = fi.tell()
                        ll = fi.readline()
            # --
        # now return the list in the correct order, filling in those states not
        # found with an unknown state
        return [found_dct.get(jh.job_id, cls.UNKNOWN) for jh in jobhandle_lst]

    @classmethod
    def set_job_priority(cls, jobhandle_lst, priority):
        """Set the priority of the specified jobs."""
        cmd_list = ["scontrol", "update", "JobID={}".format(",".join(jobhandle_lst)),
                    "Priority={}".format(priority)]
        try:
            _ = sp.check_output(cmd_list)
        except sp.CalledProcessError as e:
            logger.error("scontrol returned an error code '%s'", e.returncode)
            logger.error("scontrol wrote this: '%s' ", e.output)
            raise

sco_pid = 100


class workerproc(mp.Process):

    def __init__(self, jdct):
        mp.Process.__init__(self)
        self._jdct = jdct
        global sco_pid
        self.sco_pid = sco_pid
        sco_pid += 1
        self.sco_retcode = None
        self.start_time = None
        self.end_time = None

    def run(self):
        """Invoke the code described in the _jdct"""
        j = self._jdct
        act_cmdstr = "cd %s;  ./%s  %s" % (j["workingdir"],
                                           j["driver_name"],
                                           " ".join(j["driver_arglst"]))
        cclst = ["/bin/bash", "-c", "%s" % act_cmdstr]

        stdout = open(j["stdoutfile"], "w")
        stderr = open(j["stderrfile"], "w")

        # print "popen", cclst
        # self._p = sp.Popen(cclst, shell=False, stdout=stdout, stderr=stderr)
        self.sco_retcode = sp.call(cclst, stdout=stdout, stderr=stderr)
        print "call retty", os.strerror(self.sco_retcode)

    def ready_to_start(self, findct):
        j = self._jdct
        after_any = j["after_any"]
        after_okay = j["after_okay"]
        # catch the most common case first
        any_cond = after_any is None
        okay_cond = after_okay is None
        if any_cond and okay_cond:
            return True
        if not any_cond:
            checkset = set([jhandle.job_id for jhandle in after_any])
            finset = set(findct.iterkeys())
            any_cond = checkset <= finset
        if not okay_cond:
            checkset = set([jhandle.job_id for jhandle in after_okay])
            ok_set = set((proc.sco_pid for proc in findct.itervalues() if proc.sco_retcode == 0))
            okay_cond = checkset <= ok_set
        return any_cond and okay_cond

    def is_finished(self):
        # return hasattr(self, '_p') and self._p.returncode is not None
        self.sco_retcode is not None

    def runstate(self):
        if self.start_time is None:
            return BaseSlurmScheduler.PENDING
        else:
            # we have started
            if self.end_time is None:
                return BaseSlurmScheduler.RUNNING
            else:
                # we have finished
                if self.sco_retcode == 0:
                    return BaseSlurmScheduler.COMPLETED
                else:
                    return BaseSlurmScheduler.FAILED

    def get_state_dct(self):
        j = self._jdct
        return {
            "job_name": j["job_name"],
            "start_time": self.start_time,
            "end_time": self.end_time,
            "return_code": self.sco_retcode,
            "state": self.runstate(),
            "signal": None
        }


class DummySlurmScheduler(BaseSlurmScheduler):

    mproc = None

    @staticmethod
    def masterproc(jobqueue, resultqueue):
        waitdct = {}
        rundct = {}
        findct = {}
        while True:
            # print "masterproc!"
            try:
                jdct = jobqueue.get(block=False, timeout=1)
            except Queue.Empty:
                jdct = None
            # print "GOOLY", jdct
            if jdct is not None:
                if isinstance(jdct, dict):
                    # received a new submission
                    # create a worker process, but don't necessarily start it
                    newproc = workerproc(jdct)
                    print "YAHOOO", newproc.sco_pid
                    # return the job id of the submitted job
                    assert newproc.sco_pid is not None, "newproc pid is NONE"
                    waitdct[newproc.sco_pid] = newproc
                    resultqueue.put(newproc.sco_pid)
                elif isinstance(jdct, set):
                    print "query_set"
                    resultqueue.put(DummySlurmScheduler._getstates(jdct, waitdct, rundct, findct))
                else:
                    print "WEIRD request ", jdct

            # lets update our worker dicts
            # first the waiting dct
            for pid, proc in waitdct.items():
                if proc.ready_to_start(findct):
                    print "starting", pid
                    del waitdct[pid]
                    proc.start_time = datetime.now()
                    proc.start()
                    rundct[pid] = proc
            # next the rundct
            for pid, proc in rundct.items():
                if proc.is_finished():
                    # the job has finished
                    print "finished", pid
                    proc.end_time = datetime.now()
                    del rundct[pid]
                    findct[pid] = proc
                else:
                    # print "still running", pid
                    pass
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
        return True

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

        # make sure the job script exists ans is executable
        full_path = os.path.join(workingdir, driver_name)
        print "FULLY", full_path
        if not os.path.isfile(full_path):
            raise sp.CalledProcessError(cmd=full_path, output=None, returncode=-1)
        print "OKEY"
        
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
        cls._jobqueue.put(jdct)
        jid = cls._resqueue.get()
        print "RET", jid
        return SlurmJobHandle(jid, cls)

    @classmethod
    def job_cancel(cls, jobhandle):
        """Cancel a given job given its jobhandle.
        Raise an exception if an error occurs, otherwise return nothing.
        """
        pass

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
          - start_time (datetime object)
          - end_time (datetime object)
          - return_code (int)
          - state (string)
          - signal (int: the signal number that caused termination of this step, or 0 if
            it ended normally)
        """
        if job_handle_iter is not None and len(job_handle_iter) > 0:
            query_set = set((jh.job_id for jh in job_handle_iter))
        else:
            query_set = set()
        print "QUERY", query_set
        cls._jobqueue.put(query_set)
        accounting_info = cls._resqueue.get()
        return accounting_info

    @classmethod
    def set_job_priority(cls, jobhandle_lst, priority):
        """Set the priority of the specified jobs."""
        pass
