
# a low level interface to slurm using the calls to sbatch, scancel and squeue via Popen.

import os.path
import subprocess as sp
import time

import logging

logger = logging.getLogger("fleet.slurmlib")


class SlurmJobHandle:
    def __init__(self, job_id):
        self._job_id = job_id

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
        return SlurmScheduler.get_job_states([self])[0]

    def __str__(self):
        return "slurm job_id %d" % self._job_id


class SlurmScheduler:
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
    def submit_job(cls, workingdir, drivername, driver_arglst,
                   user_id, group_id,
                   prio_level, num_cpus,
                   stdoutfile, stderrfile,
                   dep_handle_lst=None):
        """ Submit a job to the slurm queue.
        The executable submitted will be of the form:

        workingdir/drivername arglst[0] arglst[1] ...

        workingdir (string): directory name of the job. slurm will set this to the
        'current directory' when the job is run.
        drivername (string): name of the command to execute as the main job script.
        driver_arglst (list of strings): arguments to the drivername executable.
        user_id, group_id (integers): the unix user under whose account the jobs will
        be executed .
        prio_level (integer) : a positive number >0. Higher values have higher priority.
        num_cpus: the number of CPU's (in a slurm sense) to reserve for this job.

        stdoutfile, stderrfile (strings or None): file names into which the job's
        std out and err streams will be written.

        dep_handle_lst: (list of jobhandles): the jobhandles on which this job depends.
        All of these previously submitted jobs must complete successfully before slurm
        will start this one.
        If a job on which this one is cancelled, this job will also be cancelled by slurm.

        This method returns a slurmjobhandle on success, or raises a
        subprocess.CalledProcessError exception.
        """

        cmd_lst = ["sbatch", "-D", workingdir, "--gid=%d" % group_id,
                   "-J", drivername, "--priority=%d" % prio_level,
                   "-s", "--uid=%d" % user_id, "--kill-on-invalid-dep=yes",
                   "-c", num_cpus,
                   "--export=PYTHONPATH=%s" % workingdir]
        # "--get-user-env",

        if stdoutfile:
            cmd_lst.append("--output=%s" % stdoutfile)
        if stderrfile:
            cmd_lst.append("--error=%s" % stderrfile)
        if dep_handle_lst is not None and len(dep_handle_lst) > 0:
            cmd_lst.append("--dependency=afterok:" + ":".join(["%d" % jh._job_id for jh in dep_handle_lst]))

        cmd_lst.append(os.path.join(workingdir, drivername))
        cmd_lst.extend(driver_arglst)

        logger.debug(" ".join(cmd_lst))
        try:
            out_str = sp.check_output(cmd_lst)
        except sp.CalledProcessError as E:
            logger.error("sbatch returned an error code '%d'" % E.returncode)
            logger.error("sbatch wrote this: '%s' " % E.output)
            raise
        if out_str.startswith("Submitted"):
            cl = out_str.split()
            try:
                job_id = int(cl[3])
            except:
                logger.error("sbatch completed with '%s'" % out_str)
                raise RuntimeError("cannot parse sbatch output")
        else:
            logger.error("sbatch completed with '%s'" % out_str)
            raise RuntimeError("cannot parse sbatch output")
        return SlurmJobHandle(job_id)

    @classmethod
    def job_cancel(cls, jobhandle):
        """Cancel a given job given its jobhandle.
        Raise an exception if an error occurs, otherwise return nothing.
        """
        cmd_lst = ["scancel", "%d" % jobhandle._job_id]
        try:
            _ = sp.check_output(cmd_lst)
        except sp.CalledProcessError as E:
            logger.error("scancel returned an error code '%s'" % E.returncode)
            logger.error("scancel wrote this: '%s' " % E.output)
            raise

    @classmethod
    def Slurm_is_alive(cls):
        """Return True if the slurm control daemon can be reached.
        This is tested by running squeue and checking for exceptions.
        """
        is_alive = True
        try:
            cls._do_squeue()
        except sp.CalledProcessError:
            is_alive = False
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
          NOTE: all of these values are ruturned 'as is', i.e. as strings, except for
          the 'JOBID' value, which is converted to an intteger.

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
    def get_job_states(cls, jobhandle_lst):
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
        tofind_set = set([j._job_id for j in jobhandle_lst])
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
        return [found_dct.get(jh._job_id, cls.UNKNOWN) for jh in jobhandle_lst]


ret_dct = {
    SlurmScheduler.CANCELLED: -2,
    SlurmScheduler.RUN_FAILED: -1,
    SlurmScheduler.SUCC_COMPLETED: 0
}