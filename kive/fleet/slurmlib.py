# A low level interface to slurm using the calls to sbatch, scancel and squeue via Popen

import os.path
import logging
import tempfile
import json

import multiprocessing as mp
import Queue

import re
import subprocess as sp
from datetime import datetime

import django.utils.timezone as timezone
from django.conf import settings

# from sandbox.execute import Sandbox
from fleet.exceptions import StopExecution


logger = logging.getLogger("fleet.slurmlib")

MANAGE_PY = "manage.py"


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
        return rdct[BaseSlurmScheduler.ACC_STATE]

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
    WAITING = 'WAITING'
    # include an unknown state (no accounting information available)
    UNKNOWN = 'UNKNOWN'

    RUNNING_STATES = set([PENDING, WAITING, RUNNING, COMPLETING, PREEMPTED, RESIZING, SUSPENDED])
    CANCELLED_STATES = set([CANCELLED, BOOT_FAIL, DEADLINE, NODE_FAIL, TIMEOUT])
    FAILED_STATES = set([FAILED])
    SUCCESS_STATES = set([COMPLETED])

    ALL_STATES = RUNNING_STATES | CANCELLED_STATES | FAILED_STATES | SUCCESS_STATES | set([UNKNOWN])

    STOPPED_SET = ALL_STATES - RUNNING_STATES - set([UNKNOWN])

    FINISHED_SET = FAILED_STATES | SUCCESS_STATES

    # priorities: the lowest is always 0
    # subclasses will define MAX_PRIO and PRIO_SET
    MIN_PRIO = 0

    # get_accounting_info() returns a dictionary containing the following keys.
    ACC_JOB_NAME = 'job_name'
    ACC_START_TIME = 'start_time'
    ACC_END_TIME = 'end_time'
    ACC_SUBMIT_TIME = 'submit_time'
    ACC_RETURN_CODE = 'return_code'
    ACC_STATE = 'state'
    ACC_SIGNAL = 'signal'
    ACC_JOB_ID = 'job_id'
    ACC_PRIONUM = 'prio_num'
    ACC_PRIOSTR = 'prio_str'
    ACC_SET = frozenset([ACC_JOB_NAME, ACC_START_TIME, ACC_END_TIME,
                         ACC_SUBMIT_TIME, ACC_RETURN_CODE, ACC_STATE,
                         ACC_SIGNAL, ACC_JOB_ID, ACC_PRIONUM, ACC_PRIOSTR])

    TIME_UNKNOWN = "Unknown"

    @classmethod
    def _empty_info_dct(cls, jobid):
        """Return an accounting dict for a job for which we have no accounting information."""
        rdct = {cls.ACC_JOB_NAME: "",
                cls.ACC_START_TIME: None,
                cls.ACC_END_TIME: None,
                cls.ACC_SUBMIT_TIME: None,
                cls.ACC_RETURN_CODE: None,
                cls.ACC_STATE: BaseSlurmScheduler.UNKNOWN,
                cls.ACC_SIGNAL: None,
                cls.ACC_JOB_ID: jobid,
                cls.ACC_PRIONUM: None,
                cls.ACC_PRIOSTR: ""}
        assert set(rdct.keys()) == cls.ACC_SET, "messed up empty_info_dct"
        return rdct

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
        be executed.

        prio_level (integer): priority level of the job (0 is lowest).

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
        the following fields defined above:
        - ACC_JOB_NAME (string)
        - ACC_JOB_ID (string)
        - ACC_START_TIME (datetime object)
        - ACC_END_TIME (datetime object)
        - ACC_SUBMIT_TIME (datetime object)
        - ACC_RETURN_CODE (int)
        - ACC_STATE (string, must be contained in ALL_STATES defined above)
        - ACC_SIGNAL (int: the signal number that caused termination of this step, or 0 if
        it ended normally)
        - ACC_PRIONUM (int, must be contained in PRIO_SET define above)
        - ACC_PRIOSTR (str, a descriptive name for the priority)

        All of these keys will be defined in all dictionaries returned.
        Note that if no accounting information is available for a jobid,
        then the values of the dict will be as follows:
        a) ACC_STATE will map to BaseSlurmScheduler.UNKNOWN
        b) ACC_JOB_ID will contain the job_id
        c) all other entries will map to None or "".
        See _empty_info_dct() above for details.
        """
        raise NotImplementedError

    @classmethod
    def set_job_priority(cls, jobhandle_lst, priority):
        """Set the priority of the specified jobs."""
        raise NotImplementedError

    @classmethod
    def submit_runcable(cls, runcable, sandbox):
        """
        Submit a RunCable for processing.

        Return a tuple containing the SlurmJobHandle for the cable helper,
        as well as the path of the execution info dictionary file used by the step.
        """
        raise NotImplementedError

    @classmethod
    def submit_step_setup(cls, runstep, sandbox):
        """
        Submit the setup portion of a RunStep.

        Returns a tuple containing the SlurmJobHandle of the resulting task,
        and the path of the JSON file written out for the task.
        """
        raise NotImplementedError

    @classmethod
    def submit_step_bookkeeping(cls, runstep, info_path, sandbox):
        """
        Submit the bookkeeping part of a RunStep.

        This is to be called after the driver part of a RunStep has been finished
        and the Foreman has completed the ExecLog.  It uses the same step execution
        information path as produced by submit_step_setup.
        """
        raise NotImplementedError


class SlurmScheduler(BaseSlurmScheduler):

    # _qnames: a dictionary that maps an element in PRIO_SET to a slurm partition name
    _qnames = None
    # _revlookup: a dict that maps a slurm partition name to an element in PRIO_SET
    _revlookup = None
    # _acclookup: a dict that maps a PRIO_SET to a a descriptive string (for reporting)
    _acclookup = None
    fleet_settings = [] if settings.FLEET_SETTINGS is None else ["--settings", settings.FLEET_SETTINGS]

    @classmethod
    def _int_prio_to_part_name(cls, intprio):
        """Translate an priority provided as an integer into
        a slurm partition name.
        """
        # priority is an integer; we translate it to one of the Slurm queue names.
        if not isinstance(intprio, int):
            raise RuntimeError('priority must be an integer')
        intprio = min(max(cls.MIN_PRIO, intprio), cls.MAX_PRIO)
        queue_to_use = cls._qnames[intprio]
        return intprio, queue_to_use

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
        if cls._qnames is None:
            raise RuntimeError("Must call slurm_is_alive before submitting jobs")
        if user_id <= 0 or group_id <= 0 or num_cpus <= 0:
            full_path = os.path.join(workingdir, driver_name)
            raise sp.CalledProcessError(cmd=full_path, output=None, returncode=-1)
        prio_level, partname = cls._int_prio_to_part_name(prio_level)
        cmd_lst = ["sbatch", "-D", workingdir, "--gid={}".format(group_id),
                   "-J", re.escape(job_name), "-p", partname,
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
            sp.check_output(cmd_lst)
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
        b) There are three partitions of differing priorities that we can use.
           This is checked by running 'sinfo' and checking its state.
        c) slurm accounting is configured properly.
           This is tested by running 'sacct' and checking for exceptions.
        """
        is_alive = True
        try:
            cls._do_squeue()
        except sp.CalledProcessError:
            logger.exception("_do_squeue")
            is_alive = False
        logger.info("squeue passed: %s" % is_alive)
        if is_alive:
            try:
                is_alive = cls._partitions_are_ok()
            except:
                is_alive = False
                logger.exception("partitions_are_ok")
            logger.info("sinfo (checking partitions) passed: %s" % is_alive)
        if is_alive:
            try:
                cls.get_accounting_info()
            except:
                is_alive = False
                logger.exception("get_accounting_info")
            logger.info("sacct passed: %s" % is_alive)
        if is_alive:
            # also check for the existence of MANAGE_PY at the correct location.
            # If this file is not present, the sbatch commands will crash terribly
            manage_fp = os.path.join(settings.KIVE_HOME, MANAGE_PY)
            if not os.access(manage_fp, os.X_OK):
                logger.error("An executable '%s' was not found" % manage_fp)
                logger.error("settings.KIVE_HOME = %s", settings.KIVE_HOME)
                return False
            logger.info("manager script found at '%s'" % manage_fp)
        return is_alive

    @classmethod
    def _call_to_dict(cls, cmd_lst, splitchar=None):
        """ Helper routine:
        Call a slurm command provided in cmd_lst and parse the tabular output, returning
        a list of dictionaries.
        The first lines of the output should be the table headings, which are used
        as the dictionary keys.
        """
        logger.debug(" ".join(cmd_lst))
        stderr_fd, stderr_path = tempfile.mkstemp()
        try:
            with os.open(stderr_fd, "w") as f:
                out_str = sp.check_output(cmd_lst, stderr=f)
        except sp.CalledProcessError as e:
            logger.error("%s returned an error code '%s'", cmd_lst[0], e.returncode)
            logger.error("it wrote this: '%s' ", e.output)
            with os.open(stderr_fd, "r") as f:
                logger.error("stderr: \n%s", f.read())
            raise
        finally:
            os.remove(stderr_path)
        # NOTE: sinfo et al add an empty line to the end of its output. Remove that here.
        lns = [ln for ln in out_str.split('\n') if ln]
        logger.debug("read %d lines" % len(lns))
        nametup = tuple([s.strip() for s in lns[0].split(splitchar)])
        return [dict(zip(nametup, [s.strip() for s in ln.split(splitchar)])) for ln in lns[1:]]

    @classmethod
    def _partitions_are_ok(cls):
        """
        Determine whether we can call sinfo.

        If SLURM_QUEUES is not None in the settings, then it will check that those partitions
        exist and are up.

        Otherwise, it will attempt to configure itself with partitions as follows:
        It will look for slurm partitions that are up whose names begin with 'kive'.
        NOTE: set the cls._qnames dictionary iff we return True here
        """
        if settings.SLURM_QUEUES is not None:
            # Use sinfo to check that all queues are up.
            defined_queue_names = [x[1] for x in settings.SLURM_QUEUES]
            if len(defined_queue_names) == 0:
                logger.error("slurm partition config error: SLURM_QUEUES is lenght 0")
                return False
            logger.debug("Calling sinfo to get information on queues: {}".
                         format(defined_queue_names))
            defined_queue_set = set(defined_queue_names)
            if len(defined_queue_set) != len(defined_queue_names):
                logger.error("Slurm partition configuration error: partition names are not unique")
                return False
            cmd_list = [
                "sinfo",
                "-O",
                "available,partitionname,{}".format(settings.SLURM_PRIO_KEYWORD),
                "-p",
                ",".join(defined_queue_names)
            ]
            # List of dicts. The keys are: AVAIL, PARTITION and whatever is specified in
            # settings.SLURM_PRIO_COLNAME.
            qn_dct = dict((d['PARTITION'], d) for d in cls._call_to_dict(cmd_list))
            # make sure that all required partitions are up
            errors = []
            gotset = set(qn_dct.keys())
            if gotset != defined_queue_set:
                errors.append("Slurm queues {} are not configured".format(defined_queue_set - gotset))
            downset = set([n for n, d in qn_dct.items() if d['AVAIL'] != 'up'])
            if downset:
                errors.append("Slurm queues {} are unavailable (i.e. not 'up')".format(downset))
            if len(errors) > 0:
                logger.error("Slurm partition configuration error:\n" + "\n".join(errors))
                return False
            # make sure that the actual job priorities as defined by sinfo are in the
            # correct order of priority
            actual_prio_lst = [
                (qname, int(qn_dct[qname][settings.SLURM_PRIO_COLNAME])) for qname in defined_queue_names
            ]
            if len(set([prio for n, prio in actual_prio_lst])) != len(actual_prio_lst):
                logger.error("Matching slurm partition priorities")
                return False
            act_name_lst = [n for n, prio in sorted(actual_prio_lst, key=lambda a:a[1])]
            if act_name_lst != defined_queue_names:
                logger.error("Slurm partition configuration error: priorities in wrong order!")
                return False

            # Having reached here, we know the queues are OK.  Set up _qnames and _revlookup
            num_queue = len(settings.SLURM_QUEUES)
            report_names = [x[0] for x in settings.SLURM_QUEUES]
        else:
            cmd_lst = [
                'sinfo',
                '-a',
                '-O',
                'available,partitionname,{}'.format(settings.SLURM_PRIO_KEYWORD)
            ]
            dictlst = cls._call_to_dict(cmd_lst)
            logger.debug("got information of %d partitions" % len(dictlst))

            # NOTE: the keys are 'AVAIL', 'PARTITION' and settings.SLURM_PRIO_COLNAME
            uplst = [dct for dct in dictlst if dct['AVAIL'] == 'up']
            logger.debug("found %d partitions in 'up' state" % len(uplst))
            # choose only those beginning with 'kive'
            kivelst = [dct for dct in uplst if dct['PARTITION'].startswith('kive')]
            # now we must have some remaining entries of differing priorities.
            num_queue = len(kivelst)
            if num_queue == 0:
                logger.error("slurm partition config error: have found no 'up' partitions starting with 'kive'")
                return False
            priolst = [(int(dct[settings.SLURM_PRIO_COLNAME]), dct['PARTITION']) for dct in kivelst]
            prioset = set([p for p, n in priolst])
            if len(prioset) != num_queue:
                logger.error('slurm config error: need %d different prio levels, got %d' % (num_queue, len(prioset)))
                return False
            defined_queue_names = [n for p, n in sorted(priolst, key=lambda a: a[0])]
            report_names = defined_queue_names
        # --
        ordered_prio = range(num_queue)
        cls.MAX_PRIO = num_queue-1
        cls.PRIO_SET = frozenset(ordered_prio)
        dd = cls._qnames = dict(zip(ordered_prio, defined_queue_names))
        cls._revlookup = dict(zip(defined_queue_names, ordered_prio))
        cls._acclookup = dict(zip(ordered_prio, report_names))
        logger.info(
            'prio mapping: %s',
            " ".join(["{}:{}".format(pk, dd[pk]) for pk in ordered_prio])
        )
        return True

    @classmethod
    def slurm_ident(cls):
        """Return a string with some pertinent information about the slurm configuration."""
        if cls._qnames is None:
            raise RuntimeError("Must call slurm_is_alive before slurm_ident")
        info_str = ", ".join(["{}:{}".format(kive_name, cls._qnames[kive_name]) for kive_name in cls._qnames])
        return 'Real Slurm: ' + info_str

    @classmethod
    def _do_squeue(cls, opts=None, job_id_iter=None):
        """Get the status of jobs currently on the queue.
        NOTE: this is an internal helper routine, the user probably wants to
        use SlurmScheduler.get_accounting_info() to get states of a number of previously
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
        if opts is not None:
            cmd_lst.extend(opts)
        has_jlst = job_id_iter is not None and len(job_id_iter) > 0
        if has_jlst:
            cmd_lst.extend(["-j", ",".join(job_id_iter)])
        return cls._call_to_dict(cmd_lst, splitchar=' ')

    @classmethod
    def get_accounting_info(cls, job_handle_iter=None):
        """ NOTE: sacct only provides information about jobs that are running or have completed.
        It does not provide information about jobs waiting in the queues.
        We therefore call squeue to get any pending jobs first, and then access sacct
        for all other information.
        """
        if cls._qnames is None:
            raise RuntimeError("Must call slurm_is_alive before get_accounting_info")

        have_job_handles = job_handle_iter is not None and len(job_handle_iter) > 0
        idlst = [jh.job_id for jh in job_handle_iter] if have_job_handles else None

        accounting_info = {}
        # Create proper datetime objects with the following format string.
        date_format = "%Y-%m-%dT%H:%M:%S"
        # We also want to make our datetime objects timezone-aware.
        curr_timezone = timezone.get_current_timezone()

        # We do a first pass using squeue to retrieve information for pending jobs;
        # their information will not be available from sacct yet.
        try:
            rdctlst = cls._do_squeue(opts=['--format=%i %j %P %V',
                                           '-p', ",".join(cls._qnames.values())],
                                     job_id_iter=idlst)

            # Keys are: JOBID NAME PARTITION SUBMIT_TIME
            for rdct in rdctlst:
                prio_num = cls._revlookup.get(rdct["PARTITION"], None)
                if prio_num is not None:
                    job_id = rdct["JOBID"]
                    accdct = cls._empty_info_dct(job_id)
                    accdct[cls.ACC_PRIONUM] = prio_num
                    accdct[cls.ACC_PRIOSTR] = cls._acclookup[prio_num]

                    # Localize the submission time.
                    sub_time = rdct["SUBMIT_TIME"]
                    if sub_time == cls.TIME_UNKNOWN:
                        accdct[cls.ACC_SUBMIT_TIME] = None
                    else:
                        raw_sub_time = datetime.strptime(sub_time, date_format)
                        accdct[cls.ACC_SUBMIT_TIME] = timezone.make_aware(raw_sub_time, curr_timezone)

                    accdct[cls.ACC_JOB_NAME] = rdct["NAME"]
                    accdct[cls.ACC_STATE] = BaseSlurmScheduler.WAITING
                    accounting_info[job_id] = accdct
        except sp.CalledProcessError as e:
            # This can happen if we call squeue on a single job that's already finished.
            # The error messages were already logged elsewhere, so we do nothing.
            # Said job information should be handled by sacct below.
            pass

        # Now get accounting information.
        # The --parsable2 option creates parsable output: fields are separated by a pipe, with
        # no trailing pipe (the difference between --parsable2 and --parsable).
        cmd_lst = ["sacct", "--parsable2", "--format",
                   "JobID,JobName,Start,End,State,Partition,Submit,ExitCode"]
        if have_job_handles:
            cmd_lst.extend(["-j", ",".join(idlst)])
        for raw_job_dict in cls._call_to_dict(cmd_lst, splitchar='|'):
            # Pre-process the fields.
            # There might be non-kive slurm partitions. Only report those jobs in partitions
            # we know about, as defined in the list of partition names.
            prio_num = cls._revlookup.get(raw_job_dict["Partition"], None)
            if prio_num is not None:
                job_id = raw_job_dict["JobID"]
                tdct = {}

                # Localize any datetime objects.
                for field_name, field_val in [(fn, raw_job_dict[fn]) for fn in ["Start", "End", "Submit"]]:
                    if field_val == cls.TIME_UNKNOWN:
                        tdct[field_name] = None
                    else:
                        raw_datetime = datetime.strptime(field_val, date_format)
                        tdct[field_name] = timezone.make_aware(raw_datetime, curr_timezone)

                # Split sacct's ExitCode field, which looks like "[return code]:[signal]".
                return_code, signal = (int(x) for x in raw_job_dict["ExitCode"].split(":"))

                curstate = raw_job_dict["State"]
                if curstate not in cls.ALL_STATES:
                    raise RuntimeError("received undefined state from sacct '%s'" % curstate)
                accounting_info[job_id] = {
                    cls.ACC_JOB_NAME: raw_job_dict["JobName"],
                    cls.ACC_START_TIME: tdct["Start"],
                    cls.ACC_END_TIME: tdct["End"],
                    cls.ACC_SUBMIT_TIME: tdct["Submit"],
                    cls.ACC_RETURN_CODE: return_code,
                    cls.ACC_STATE: curstate,
                    cls.ACC_SIGNAL: signal,
                    cls.ACC_JOB_ID: job_id,
                    cls.ACC_PRIONUM: prio_num,
                    cls.ACC_PRIOSTR: cls._acclookup[prio_num]
                }
        # make sure all requested job handles have an entry...
        if have_job_handles:
            needset = set(idlst)
            gotset = set(accounting_info.keys())
            for missing_pid in needset - gotset:
                accounting_info[missing_pid] = cls._empty_info_dct(missing_pid)
        return accounting_info

    @classmethod
    def set_job_priority(cls, jobhandle_lst, intprio):
        """Attempt to set the priority of the specified jobs.
        If a job is already running (rather than still pending) or has completed,
        this routine silently fails.
        """
        if cls._qnames is None:
            raise RuntimeError("Must call slurm_is_alive before setting job priority")
        if jobhandle_lst is None or len(jobhandle_lst) == 0:
            raise RuntimeError("no jobhandles provided")
        intprio, queue_to_use = cls._int_prio_to_part_name(intprio)

        cmd_list = ["scontrol", "update", "job",
                    ",".join([jh.job_id for jh in jobhandle_lst]),
                    "Partition={}".format(queue_to_use)]
        logger.debug(" ".join(cmd_list))
        try:
            sp.check_output(cmd_list)
        except sp.CalledProcessError as e:
            logger.debug("scontrol returned an error code '%s'", e.returncode)
            logger.debug("scontrol wrote this: '%s' ", e.output)
            # scontrol returns 1 if a job is already running or has completed
            # catch this case silently, but raise an exception in all other cases.
            if e.returncode != 1:
                raise

    @classmethod
    def submit_runcable(cls, runcable, sandbox):
        """
        Submit a RunCable to Slurm for processing.
        """
        # First, serialize the task execution information.
        cable_info = sandbox.cable_execute_info[(runcable.parent_run, runcable.component)]

        # Submit the job.
        cable_execute_dict_fd, cable_execute_dict_path = tempfile.mkstemp(
            dir=cable_info.cable_info_dir,
            prefix="cable_info",
            suffix=".json"
        )
        with os.fdopen(cable_execute_dict_fd, "wb") as f:
            f.write(json.dumps(cable_info.dict_repr()))

        cable_slurm_handle = cls.submit_job(
            settings.KIVE_HOME,
            os.path.join(settings.KIVE_HOME, MANAGE_PY),
            [settings.CABLE_HELPER_COMMAND] + cls.fleet_settings + [cable_execute_dict_path],
            sandbox.uid,
            sandbox.gid,
            sandbox.run.priority,
            cable_info.threads_required,
            cable_info.stdout_path,
            cable_info.stderr_path,
            job_name="run{}_cable{}".format(runcable.parent_run.pk, runcable.pk)
        )

        return cable_slurm_handle, cable_execute_dict_path

    @classmethod
    def submit_step_setup(cls, runstep, sandbox):
        """
        Submit the setup portion of a RunStep to Slurm.

        This uses the step helper management command defined in settings.
        """
        # First, serialize the task execution information.
        step_info = sandbox.step_execute_info[(runstep.run, runstep.pipelinestep)]

        step_execute_dict_fd, step_execute_dict_path = tempfile.mkstemp(
            dir=step_info.step_run_dir,
            prefix="step_info",
            suffix=".json"
        )
        with os.fdopen(step_execute_dict_fd, "wb") as f:
            f.write(json.dumps(step_info.dict_repr()))

        setup_slurm_handle = cls.submit_job(
            settings.KIVE_HOME,
            os.path.join(settings.KIVE_HOME, MANAGE_PY),
            [settings.STEP_HELPER_COMMAND] + cls.fleet_settings + [step_execute_dict_path],
            sandbox.uid,
            sandbox.gid,
            sandbox.run.priority,
            1,
            step_info.setup_stdout_path(),
            step_info.setup_stderr_path(),
            job_name="r{}s{}_setup".format(runstep.top_level_run.pk,
                                           runstep.get_coordinates())
        )
        return setup_slurm_handle, step_execute_dict_path

    @classmethod
    def submit_step_bookkeeping(cls, runstep, info_path, sandbox):
        """
        Submit the bookkeeping part of a RunStep to Slurm.
        """
        step_info = sandbox.step_execute_info[(runstep.run, runstep.pipelinestep)]

        # Submit a job for the setup.
        step_execute_dict_path = info_path
        if info_path is None:
            step_execute_dict_fd, step_execute_dict_path = tempfile.mkstemp(
                dir=step_info.step_run_dir,
                prefix="step_info",
                suffix=".json"
            )
            with os.fdopen(step_execute_dict_fd, "wb") as f:
                f.write(json.dumps(step_info.dict_repr()))

        bookkeeping_slurm_handle = cls.submit_job(
            settings.KIVE_HOME,
            os.path.join(settings.KIVE_HOME, MANAGE_PY),
            [settings.STEP_HELPER_COMMAND, "--bookkeeping"] + cls.fleet_settings + [step_execute_dict_path],
            sandbox.uid,
            sandbox.gid,
            sandbox.run.priority,
            1,
            step_info.bookkeeping_stdout_path(),
            step_info.bookkeeping_stderr_path(),
            job_name="r{}s{}_bookkeeping".format(runstep.top_level_run.pk,
                                                 runstep.get_coordinates())
        )

        return bookkeeping_slurm_handle


_dummy_pid = 100
_DUMMY_MAX_PRIO = 2
_dummy_priority_by_index = {
    BaseSlurmScheduler.MIN_PRIO: 'SLOW-Q',
    BaseSlurmScheduler.MIN_PRIO+1: 'MEDIUM-Q',
    BaseSlurmScheduler.MIN_PRIO+2: 'FAST-Q',
}


def startit(wdir, dname, arglst, stdout, stderr):
    """ Start a process with a command.
    NOTE: shell MUST be False here, otherwise the popen.wait() will NOT wait
    for completion of the command.
    """
    act_cmdstr = "cd {}; {} {}".format(wdir, os.path.join(wdir, dname), " ".join(arglst))
    cclst = ["/bin/bash", "-c", '%s' % act_cmdstr]
    p = sp.Popen(cclst, shell=False, stdout=stdout, stderr=stderr)
    return p


def callit(wdir, dname, arglst, stdout, stderr):
    popen = startit(wdir, dname, arglst, stdout, stderr)
    popen.wait()
    return popen.returncode


class dummyjobstate:
    def __init__(self, priolevel, jobname):
        global _dummy_pid
        self.sco_pid = "%d" % _dummy_pid
        _dummy_pid += 1
        self.sco_retcode = None
        self.submit_time = None
        self.start_time = None
        self.end_time = None
        self.set_runstate(BaseSlurmScheduler.PENDING)
        assert priolevel in _dummy_priority_by_index, "{} is an invalid priority".format(priolevel)
        self.prio_num = priolevel
        self.jobname = jobname

    def iscancelled(self):
        return self.get_runstate() in BaseSlurmScheduler.CANCELLED_STATES

    def get_runstate(self):
        return self.my_state

    def set_runstate(self, newstate):
        if newstate in BaseSlurmScheduler.ALL_STATES:
            self.my_state = newstate
        else:
            raise RuntimeError("illegal state '%s'" % newstate)

    def get_state_dct(self):
        cls = BaseSlurmScheduler
        rdct = {
            cls.ACC_JOB_NAME: self.jobname,
            cls.ACC_START_TIME: self.start_time,
            cls.ACC_END_TIME: self.end_time,
            cls.ACC_SUBMIT_TIME: self.submit_time,
            cls.ACC_RETURN_CODE: self.sco_retcode,
            cls.ACC_STATE: self.get_runstate(),
            cls.ACC_SIGNAL: None,
            cls.ACC_JOB_ID: self.sco_pid,
            cls.ACC_PRIONUM: self.prio_num,
            cls.ACC_PRIOSTR: _dummy_priority_by_index[self.prio_num]
        }
        assert set(rdct.keys()) == BaseSlurmScheduler.ACC_SET, "weird state keys"
        return rdct


class dummyworkerproc(dummyjobstate):

    def __init__(self, jdct):
        dummyjobstate.__init__(self, jdct["prio_level"], jdct["job_name"])
        self._jdct = jdct

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
        self.end_time = self.start_time = timezone.now()
        self.set_runstate(BaseSlurmScheduler.CANCELLED)

    def is_finished(self):
        self.popen.poll()
        self.sco_retcode = self.popen.returncode
        if self.sco_retcode is not None:
            self.my_state = BaseSlurmScheduler.COMPLETED if self.sco_retcode == 0 else BaseSlurmScheduler.FAILED
            return True
        else:
            return False


class DummySlurmScheduler(BaseSlurmScheduler):

    mproc = None

    DUMMY_FAIL = -1

    MAX_PRIO = _DUMMY_MAX_PRIO
    PRIO_SET = frozenset(range(BaseSlurmScheduler.MIN_PRIO, MAX_PRIO+1))

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
    def _setprio(jidlst, priolevel, waitdct, rundct, findct):
        """ Set the priority levels of the jobs in jidlst.
        """
        assert priolevel in _dummy_priority_by_index, "{} is an invalid priority".format(priolevel)
        jset = set(jidlst)
        for s, dct in [(set(dct.keys()) & jset, dct) for dct in [waitdct, rundct, findct]]:
            for jid in s:
                dct[jid].prio_num = priolevel
        # NOTE: we have not checked whether all elements in jidlst have been found.
        # ignore this for now
        return 0

    @staticmethod
    def _do_fin_job(jdct, findct):
        """ Create an entry in findct for a completed runcable
        Return the sco_pid iff successful, otherwise return DUMMY_FAIL.
        """
        try:
            newproc = dummyjobstate(jdct["prio_level"], jdct["job_name"])
            # set the state based on the exit code.
            if jdct['return_code'] == 0:
                newproc.set_runstate(BaseSlurmScheduler.COMPLETED)
            else:
                newproc.set_runstate(BaseSlurmScheduler.FAILED)
            pid = newproc.sco_pid
            findct[pid] = newproc
        except RuntimeError:
            pid = DummySlurmScheduler.DUMMY_FAIL
        return pid

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
                    newproc = dummyworkerproc(payload)
                    newproc.submit_time = datetime.now()
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
                elif cmd == 'finstep':
                    resultqueue.put(DummySlurmScheduler._do_fin_job(payload, findct))
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
            for proc in sorted(rdylst, key=lambda p: p.prio_num, reverse=True):
                del waitdct[proc.sco_pid]
                proc.start_time = timezone.now()
                proc.do_run()
                rundct[proc.sco_pid] = proc
            # next, check the rundct
            for proc in [p for p in rundct.values() if p.is_finished()]:
                proc.end_time = timezone.now()
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
                   priority,
                   num_cpus,
                   stdoutfile,
                   stderrfile,
                   after_okay=None,
                   after_any=None,
                   job_name=None):

        if cls.mproc is None:
            cls._init_masterproc()
        if not isinstance(priority, int):
            raise RuntimeError('priority must be an integer')
        prio_level = min(max(priority, cls.MIN_PRIO), cls.MAX_PRIO)
        # make sure the job script exists and is executable
        full_path = os.path.join(workingdir, driver_name)
        if not os.path.isfile(full_path):
            raise sp.CalledProcessError(cmd=full_path, output=None, returncode=-1)
        if user_id <= 0 or group_id <= 0 or num_cpus <= 0:
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
          - submit_time (datetime object)
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
        if not isinstance(priority, int):
            raise RuntimeError('priority must be an integer')
        prio_level = min(max(priority, cls.MIN_PRIO), cls.MAX_PRIO)
        cls._jobqueue.put(('prio', ([jh.job_id for jh in jobhandle_lst], prio_level)))
        res = cls._resqueue.get()
        if res != 0:
            raise sp.CalledProcessError(returncode=res)

    @classmethod
    def shutdown(cls):
        cls.mproc.terminate()
        cls.mproc = None

    @classmethod
    def submit_runcable(cls, runcable, sandbox):
        """
        Submit (and process!) a RunCable for processing.
        """
        # Recreate the procedure in the cable_helper management command.
        cable_info = sandbox.cable_execute_info[(runcable.parent_run, runcable.component)]
        cable_info_dict = cable_info.dict_repr()

        cable_execute_dict_fd, cable_execute_dict_path = tempfile.mkstemp(
            dir=cable_info.cable_info_dir,
            prefix="cable_info",
            suffix=".json"
        )
        with os.fdopen(cable_execute_dict_fd, "wb") as f:
            f.write(json.dumps(cable_info_dict))

        sandbox.__class__.finish_cable(cable_info_dict)
        driver_arglst = [settings.CABLE_HELPER_COMMAND, cable_execute_dict_path]
        job_name = "run{}_cable{}".format(runcable.parent_run.pk, runcable.pk)
        jdct = dict([('workingdir', settings.KIVE_HOME),
                     ('driver_name', os.path.join(settings.KIVE_HOME, MANAGE_PY)),
                     ('driver_arglst', driver_arglst),
                     ('user_id', sandbox.uid),
                     ('group_id', sandbox.gid),
                     ('prio_level', sandbox.run.priority),
                     ('num_cpus', cable_info.threads_required),
                     ('stdoutfile', cable_info.stdout_path),
                     ('stderrfile', cable_info.stderr_path),
                     ('after_okay', None),
                     ('after_any', None),
                     ('job_name', job_name),
                     ('return_code', 0)])
        cls._jobqueue.put(('finstep', jdct))
        jid = cls._resqueue.get()
        if jid == DummySlurmScheduler.DUMMY_FAIL:
            raise sp.CalledProcessError()
        job_handle = SlurmJobHandle(jid, cls)
        return job_handle, cable_execute_dict_path

    @classmethod
    def submit_step_setup(cls, runstep, sandbox):
        """
        Submit the setup portion of a RunStep.
        """
        # First, serialize the task execution information.
        step_info = sandbox.step_execute_info[(runstep.run, runstep.pipelinestep)]
        step_execute_dict = step_info.dict_repr()

        step_execute_dict_fd, step_execute_dict_path = tempfile.mkstemp(
            dir=step_info.step_run_dir,
            prefix="step_info",
            suffix=".json"
        )
        with os.fdopen(step_execute_dict_fd, "wb") as f:
            f.write(json.dumps(step_execute_dict))

        exit_code = 0
        try:
            curr_RS = sandbox.__class__.step_execution_setup(step_execute_dict)
        except StopExecution:
            logger.exception("Execution was stopped during setup.")
            exit_code = 103

        if curr_RS.is_failed():
            exit_code = 101
        elif curr_RS.is_cancelled():
            exit_code = 102
        driver_arglst = [settings.STEP_HELPER_COMMAND, step_execute_dict_path]
        job_name = "r{}s{}_setup".format(runstep.top_level_run.pk, runstep.get_coordinates())
        jdct = dict([('workingdir', settings.KIVE_HOME),
                     ('driver_name', os.path.join(settings.KIVE_HOME, MANAGE_PY)),
                     ('driver_arglst', driver_arglst),
                     ('user_id', sandbox.uid),
                     ('group_id', sandbox.gid),
                     ('prio_level', sandbox.run.priority),
                     ('num_cpus', 1),
                     ('stdoutfile', step_info.setup_stdout_path()),
                     ('stderrfile', step_info.setup_stderr_path()),
                     ('after_okay', None),
                     ('after_any', None),
                     ('job_name', job_name),
                     ('return_code', exit_code)])
        cls._jobqueue.put(('finstep', jdct))
        jid = cls._resqueue.get()
        if jid == DummySlurmScheduler.DUMMY_FAIL:
            raise sp.CalledProcessError()
        setup_slurm_handle = SlurmJobHandle(jid, cls)
        return setup_slurm_handle, step_execute_dict_path

    @classmethod
    def submit_step_bookkeeping(cls, runstep, info_path, sandbox):
        """
        Submit the bookkeeping part of a RunStep.
        """
        step_info = sandbox.step_execute_info[(runstep.run, runstep.pipelinestep)]
        step_execute_dict = step_info.dict_repr()

        # Submit a job for the setup.
        step_execute_dict_path = info_path
        if info_path is None:
            step_execute_dict_fd, step_execute_dict_path = tempfile.mkstemp(
                dir=step_info.step_run_dir,
                prefix="step_info",
                suffix=".json"
            )
            with os.fdopen(step_execute_dict_fd, "wb") as f:
                f.write(json.dumps(step_execute_dict))

        sandbox.__class__.step_execution_bookkeeping(step_execute_dict)
        driver_arglst = [settings.STEP_HELPER_COMMAND, "--bookkeeping", step_execute_dict_path]
        job_name = "r{}s{}_bookkeeping".format(runstep.top_level_run.pk,
                                               runstep.get_coordinates())
        jdct = dict([('workingdir', settings.KIVE_HOME),
                     ('driver_name', os.path.join(settings.KIVE_HOME, MANAGE_PY)),
                     ('driver_arglst', driver_arglst),
                     ('user_id', sandbox.uid),
                     ('group_id', sandbox.gid),
                     ('prio_level', sandbox.run.priority),
                     ('num_cpus', 1),
                     ('stdoutfile', step_info.bookkeeping_stdout_path()),
                     ('stderrfile', step_info.bookkeeping_stderr_path()),
                     ('after_okay', None),
                     ('after_any', None),
                     ('job_name', job_name),
                     ('return_code', 0)])
        cls._jobqueue.put(('finstep', jdct))
        jid = cls._resqueue.get()
        if jid == DummySlurmScheduler.DUMMY_FAIL:
            raise sp.CalledProcessError()
        bookkeeping_slurm_handle = SlurmJobHandle(jid, cls)
        return bookkeeping_slurm_handle
