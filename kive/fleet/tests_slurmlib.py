#!/usr/bin/env python

# some simple  tests for the slurmlib module

import os
import os.path as osp
import time
import subprocess as sp
import datetime

# can't yet use pytest -- sniff
# import pytest
import unittest
from unittest import skipIf

import fleet.slurmlib as slurmlib
from django.conf import settings

from django.test import TestCase

# NOTE: Here, select which SlurmScheduler to test.
# we select the DummySlurmScheduler by default, so that the automatic tests
# can run without slurm
# SlurmScheduler = slurmlib.SlurmScheduler
# SlurmScheduler = slurmlib.DummySlurmScheduler


ACC_STATE = slurmlib.BaseSlurmScheduler.ACC_STATE
ACC_JOB_ID = slurmlib.BaseSlurmScheduler.ACC_JOB_ID
ACC_PRIONUM = slurmlib.BaseSlurmScheduler.ACC_PRIONUM

PRIO_LOW = slurmlib.BaseSlurmScheduler.MIN_PRIO
PRIO_MEDIUM = PRIO_LOW + 1
PRIO_HIGH = PRIO_MEDIUM + 1

# this is the directory where the test jobs to submit to slurm will reside
# NOTE: the job number 02 must return a non-zero exit code for testing
FAIL_JOB_NUMBER = 2
NUM_JOBS = 5
TEST_DIR = osp.join(settings.KIVE_HOME, "fleet/slurm_test_files/slurmrundir")


def _submit_job_n(n, prio, afteroklst=None, afteranylst=None, sched_cls=None):
    wdir = osp.join(TEST_DIR, "job%02d" % n)
    jobname = "sleep%02d.sh" % n
    return sched_cls.submit_job(
        wdir,
        jobname,
        [],
        prio,
        1,
        osp.join(wdir, "out.txt"),
        osp.join(wdir, "err.txt"),
        after_okay=afteroklst,
        after_any=afteranylst
    )


def submit_all(prio, sched_cls=None):
    """ Submit all jobs with a certain priority."""
    return [_submit_job_n(i, prio, sched_cls=sched_cls)
            for i in range(1, NUM_JOBS + 1)]


def get_accounting_info(jhandles=None, sched_cls=None):
    curstates = sched_cls.get_accounting_info(job_handle_iter=jhandles)
    if jhandles is not None and len(jhandles) > 0:
        # check we have entries for all requested jhandles
        jidset = set([jh.job_id for jh in jhandles])
        gotset = set(curstates.keys())
        assert gotset == jidset, "Did not get results from all submitted jobs"

    cls = sched_cls
    for jid, dct in curstates.iteritems():
        # makes sure all required fields are defined
        assert cls.ACC_SET == set(dct.keys()), "inconsistent key set"
        assert jid == dct[cls.ACC_JOB_ID]
        prio = dct[cls.ACC_PRIONUM]
        if prio is not None:
            assert prio in cls.PRIO_SET, "invalid priority value"
        for k in [cls.ACC_START_TIME, cls.ACC_END_TIME, cls.ACC_SUBMIT_TIME]:
            tval = dct[k]
            if tval is not None:
                assert isinstance(tval, datetime.datetime), "wrong type of time field"
        state = dct[cls.ACC_STATE]
        assert state in cls.ALL_STATES, "illegal state"
    return curstates


class SlurmDummyTests(TestCase):
    def setUp(self):
        self.addTypeEqualityFunc(str, self.assertMultiLineEqual)
        self.sched_cls = self.get_slurm_scheduler_class()
        is_alive = self.sched_cls.slurm_is_alive(skip_extras=True)
        if not is_alive:
            raise RuntimeError("slurm is not alive")

    def get_slurm_scheduler_class(self):
        return slurmlib.DummySlurmScheduler

    def tearDown(self):
        self.sched_cls.shutdown()

    def test_callit01(self, lverb=False):
        """ Should return 0 """
        n = 1
        if lverb:
            print "---test_callit01", n
        wdir = osp.join(TEST_DIR, "job%02d" % n)
        jobname = "sleep%02d.sh" % n
        # arglst, stderr, stdout = [], None, None
        arglst = []
        with open("/tmp/out.txt", "w") as stdout, open("/tmp/err.txt", "w") as stderr:
            retval = slurmlib.callit(wdir, jobname, arglst, stdout, stderr)
            if retval != 0:
                print "the error is '%s'" % os.strerror(retval)
                assert retval == 0, "expected retval 0"
        if lverb:
            print "---END test_callit01", n

    def test_callit02(self, lverb=False):
        """ Should return 2 """
        n = FAIL_JOB_NUMBER
        if lverb:
            print "---test_callit01", n
        wdir = osp.join(TEST_DIR, "job%02d" % n)
        jobname = "sleep%02d.sh" % n
        arglst = []
        with open("/tmp/out.txt", "w") as stdout, open("/tmp/err.txt", "w") as stderr:
            retval = slurmlib.callit(wdir, jobname, arglst, stdout, stderr)
            assert retval == 2, "expected retval 2"
        if lverb:
            print "---END test_callit01", n

    def test_is_alive(self):
        """test_is_alive() should return True"""
        is_alive = self.sched_cls.slurm_is_alive()
        assert is_alive, "Calling is_alive fails"

    def test_slurm_ident(self):
        """slurm_ident must return a string"""
        idstr = self.sched_cls.slurm_ident()
        assert isinstance(idstr, str), "slurm ident must be a string"

    def test_submit_job01(self, lverb=False):
        """ Submitting this job should succeed."""
        if lverb:
            print "--test_submit_job01"
        jhandle = _submit_job_n(1, PRIO_MEDIUM, sched_cls=self.sched_cls)
        if lverb:
            print "submitted job", jhandle

    def test_submit_job02(self, lverb=False):
        """Submission should fail (nonexistent job script)
        """
        if lverb:
            print "--test_submit_job02"
        prio = PRIO_MEDIUM
        n, m = 1, 2
        wdir = osp.join(TEST_DIR, "job%02d" % n)
        jobname = "sleep%02d.sh" % m
        with self.assertRaises(sp.CalledProcessError):
            self.sched_cls.submit_job(
                wdir,
                jobname,
                [],
                prio,
                1,
                osp.join(wdir, "out.txt"),
                osp.join(wdir, "err.txt"),
                None
            )

    def test_submit_job03(self, lverb=False):
        """Submission should fail (priority a string instead of int)"""
        if lverb:
            print "--test_submit_job03"
        prio = 'illegal priostring'
        n = 1
        wdir = osp.join(TEST_DIR, "job%02d" % n)
        jobname = "sleep%02d.sh" % n
        with self.assertRaises(RuntimeError):
            self.sched_cls.submit_job(
                wdir,
                jobname,
                [],
                prio,
                1,
                osp.join(wdir, "out.txt"),
                osp.join(wdir, "err.txt"),
                None
            )

    def test_submit_job04(self, lverb=False):
        """Submit a job that should run, but returns a nonzero exit code.
        I.e. submission should succeed, but the job should have a non-zero exit code.
        """
        if lverb:
            print "---test_submit_job04"
        jhandle = _submit_job_n(FAIL_JOB_NUMBER, PRIO_MEDIUM, sched_cls=self.sched_cls)
        if lverb:
            print "successfully launched job %s, now waiting for its failure..." % jhandle
        time.sleep(2)
        num_tries, i = 20, 0
        curstate = jhandle.get_state()
        if lverb:
            print "gotstate", curstate
        while (i < num_tries) and (curstate != self.sched_cls.FAILED):
            if lverb:
                print i, "curstate...", curstate
            time.sleep(5)
            curstate = jhandle.get_state()
            i += 1
        assert curstate == self.sched_cls.FAILED, "failed to get a 'FAILED' state. got {}".format(curstate)
        if lverb:
            print "---test_submit_job04: Success, got an expected FAILED status"

    def test_submit_job07(self, lverb=False):
        """Submission should fail (illegal cpu_number)"""
        if lverb:
            print "--test_submit_job07"
        num_cpu = 0
        prio = PRIO_MEDIUM
        n = 1
        wdir = osp.join(TEST_DIR, "job%02d" % n)
        jobname = "sleep%02d.sh" % n
        with self.assertRaises(sp.CalledProcessError):
            self.sched_cls.submit_job(
                wdir,
                jobname,
                [],
                prio,
                num_cpu,
                osp.join(wdir, "out.txt"),
                osp.join(wdir, "err.txt"),
                None
            )
        if lverb:
            print "--test_submit_job07 SUCCESS"

    def test_dep_jobs01_okay(self, lverb=False):
        """Submit one job dependent on the other with an after_okay dependency.
        Both jobs should succeed."""
        if lverb:
            print "--test_dep_jobs01_okay"
        jobid_01 = _submit_job_n(1, PRIO_MEDIUM, sched_cls=self.sched_cls)
        if lverb:
            print "first job", jobid_01
        jobid_02 = _submit_job_n(3, PRIO_MEDIUM, [jobid_01], sched_cls=self.sched_cls)
        if lverb:
            print "dependent job", jobid_02
        my_handles = [jobid_01, jobid_02]
        jobidlst = [j.job_id for j in my_handles]
        time.sleep(2)
        num_tries, i = 40, 0
        curstate = get_accounting_info(my_handles, sched_cls=self.sched_cls)
        while i < num_tries and curstate[jobid_02.job_id][ACC_STATE] != self.sched_cls.COMPLETED:
            if lverb:
                print "step %02d:" % i, [curstate[jid][ACC_STATE] for jid in jobidlst]
            time.sleep(5)
            curstate = get_accounting_info(my_handles, sched_cls=self.sched_cls)
            i += 1
        if i == num_tries:
            raise RuntimeError("test inconclusive: didn't wait long enough")
        assert curstate[jobid_01.job_id][ACC_STATE] == self.sched_cls.COMPLETED, "job01: failed to run successfully"
        assert curstate[jobid_02.job_id][ACC_STATE] == self.sched_cls.COMPLETED, "job02: failed to run successfully"
        if lverb:
            print "--test_dep_jobs01_okay SUCCESS"

    def test_dep_jobs01_any(self, lverb=False):
        """Submit one job dependent on the other with an after_any dependency.
        Both jobs should succeed."""
        if lverb:
            print "--test_dep_jobs01_any"
        jobid_01 = _submit_job_n(1, PRIO_MEDIUM, sched_cls=self.sched_cls)
        if lverb:
            print "first job", jobid_01
        jobid_02 = _submit_job_n(3, PRIO_MEDIUM, None, [jobid_01], sched_cls=self.sched_cls)
        if lverb:
            print "dependent job", jobid_02
        my_handles = [jobid_01, jobid_02]
        jobidlst = [j.job_id for j in my_handles]
        time.sleep(2)
        num_tries, i = 40, 0
        curstate = get_accounting_info(my_handles, sched_cls=self.sched_cls)
        while i < num_tries and curstate[jobid_02.job_id][ACC_STATE] != self.sched_cls.COMPLETED:
            if lverb:
                print "step %02d:" % i, [curstate[jid][ACC_STATE] for jid in jobidlst]
            time.sleep(5)
            curstate = get_accounting_info(my_handles, sched_cls=self.sched_cls)
            i += 1
        if i == num_tries:
            raise RuntimeError("test inconclusive: didn't wait long enough")
        if lverb:
            print "FINAL STATE", [curstate[jid][ACC_STATE] for jid in jobidlst]
        assert curstate[jobid_01.job_id][ACC_STATE] == self.sched_cls.COMPLETED, "job01: failed to run successfully"
        assert curstate[jobid_02.job_id][ACC_STATE] == self.sched_cls.COMPLETED, "job02: failed to run successfully"
        if lverb:
            print "--test_dep_jobs01_any SUCCESS"

    def test_dep_jobs02_ok(self, lverb=False):
        """Submit job 01, and job 02 dependent on 01 with an after_ok dependency.
        Job 01 will fail. Job 02 must be cancelled.
        """
        if lverb:
            print "--test_dep_jobs02_ok"
        jobid_01 = _submit_job_n(FAIL_JOB_NUMBER, PRIO_MEDIUM, sched_cls=self.sched_cls)
        if lverb:
            print "first job that will fail:", jobid_01
        jobid_02 = _submit_job_n(3, PRIO_MEDIUM, [jobid_01], sched_cls=self.sched_cls)
        if lverb:
            print "dependent job:", jobid_02
        joblst = [jobid_01, jobid_02]
        not_failed, num_tries, i = True, 40, 0
        curstate = None
        while (i < num_tries) and not_failed:
            time.sleep(2)
            curstate = get_accounting_info(joblst, sched_cls=self.sched_cls)
            if lverb:
                print "step %02d:" % i, curstate[jobid_01.job_id][ACC_STATE], curstate[jobid_02.job_id][ACC_STATE]
            not_failed = curstate[jobid_01.job_id][ACC_STATE] != self.sched_cls.FAILED
            i += 1
        if i == num_tries:
            raise RuntimeError("test inconclusive: didn't wait long enough")
        if lverb:
            print "job01 state:", curstate[jobid_01.job_id]
            print "job02 state:", curstate[jobid_02.job_id]
        assert curstate[jobid_01.job_id][ACC_STATE] == self.sched_cls.FAILED, "unexpected state 01"
        assert curstate[jobid_02.job_id][ACC_STATE] == self.sched_cls.CANCELLED, "unexpected state 02"
        if lverb:
            print "--test_dep_jobs02_ok SUCCESS"

    def test_dep_jobs02_any(self, lverb=False):
        """Submit job 01, and job 02 dependent on 01 with an after_any dependency.
        Job 01 will fail. Job 02 must run anyway.
        """
        if lverb:
            print "--test_dep_jobs02_any"
        jobid_01 = _submit_job_n(FAIL_JOB_NUMBER, PRIO_MEDIUM, sched_cls=self.sched_cls)
        if lverb:
            print "first job that will fail:", jobid_01
        jobid_02 = _submit_job_n(3, PRIO_MEDIUM, None, [jobid_01], sched_cls=self.sched_cls)
        if lverb:
            print "dependent job:", jobid_02
        joblst = [jobid_01, jobid_02]
        jidlst = [jh.job_id for jh in joblst]
        if lverb:
            print "waiting for job 01 to fail"
        not_failed, num_tries, i = True, 40, 0
        curstate = None
        while (i < num_tries) and not_failed:
            time.sleep(2)
            curstate = get_accounting_info(joblst, sched_cls=self.sched_cls)
            if lverb:
                print "step %02d:" % i, [curstate[jid][ACC_STATE] for jid in jidlst]
            not_failed = curstate[jobid_01.job_id][ACC_STATE] != self.sched_cls.FAILED
            i += 1
        if i == num_tries:
            raise RuntimeError("test inconclusive: didn't wait long enough")
        # wait for jobid_02 to start running
        if lverb:
            print "OK, waiting for job 02 to run"
        is_running, num_tries, i = curstate[jobid_02.job_id][ACC_STATE] == self.sched_cls.COMPLETED, 40, 0
        while (i < num_tries) and not is_running:
            time.sleep(2)
            curstate = get_accounting_info(joblst, sched_cls=self.sched_cls)
            if lverb:
                print "step %02d:" % i, [curstate[jid][ACC_STATE] for jid in jidlst]
            is_running = curstate[jobid_02.job_id][ACC_STATE] == self.sched_cls.COMPLETED
        if i == num_tries:
            raise RuntimeError("failed: job 02 did not complete")
        if lverb:
            print "job01 state:", curstate[jobid_01.job_id]
            print "job02 state:", curstate[jobid_02.job_id]
        state_02 = curstate[jobid_02.job_id][ACC_STATE]
        ok_state_02 = state_02 == self.sched_cls.RUNNING or state_02 == self.sched_cls.COMPLETED
        assert curstate[jobid_01.job_id][ACC_STATE] == self.sched_cls.FAILED, "unexpected state 01"
        assert ok_state_02, "unexpected state 02"
        if lverb:
            print "--test_dep_jobs02_any SUCCESS"

    def test_dep_jobs01_multi(self, lverb=False):
        """Submit job 01 that will fail.
        Submit job o2 that will succeed.
        Submit job 03, after_any on 01, and after_ok on 02.
        Job 03 must be run.
        """
        if lverb:
            print "--test_dep_jobs01_multi"
        jobid_01 = _submit_job_n(FAIL_JOB_NUMBER, PRIO_MEDIUM, sched_cls=self.sched_cls)
        if lverb:
            print "first job that will fail:", jobid_01
        jobid_02 = _submit_job_n(3, PRIO_MEDIUM, sched_cls=self.sched_cls)
        if lverb:
            print "second job that will succeed", jobid_02
        jobid_03 = _submit_job_n(1, PRIO_MEDIUM, [jobid_02], [jobid_01], sched_cls=self.sched_cls)
        if lverb:
            print "third job that should run", jobid_02
        joblst = [jobid_01, jobid_02, jobid_03]
        jobidlst = [j.job_id for j in joblst]
        still_running, num_tries, i = True, 40, 0
        curstate = None
        while (i < num_tries) and still_running:
            time.sleep(2)
            curstate = get_accounting_info(joblst, sched_cls=self.sched_cls)
            if lverb:
                print "step %02d:" % i, [curstate[jid][ACC_STATE] for jid in jobidlst]
            still_running = (curstate[jobid_01.job_id][ACC_STATE] != self.sched_cls.FAILED) or\
                            (curstate[jobid_02.job_id][ACC_STATE] != self.sched_cls.COMPLETED)
            i += 1
        if i == num_tries:
            raise RuntimeError("test inconclusive: didn't wait long enough")
        state_01 = curstate[jobid_01.job_id][ACC_STATE]
        state_02 = curstate[jobid_02.job_id][ACC_STATE]
        state_03 = curstate[jobid_03.job_id][ACC_STATE]
        if lverb:
            print "state after loop:", [curstate[jid][ACC_STATE] for jid in jobidlst]
        assert state_01 == self.sched_cls.FAILED, "unexpected state 01"
        assert state_02 == self.sched_cls.COMPLETED, "unexpected state 02"
        ok_state_03 = state_03 in self.sched_cls.RUNNING_STATES
        assert ok_state_03, "unexpected state 03"
        if lverb:
            print "--test_dep_jobs01_multi SUCCESS"

    def test_cancel_jobs01(self, lverb=False):
        """Submit a job, then cancel it"""
        if lverb:
            print "--test_cancel_jobs01"
        jobid_01 = _submit_job_n(1, PRIO_MEDIUM, sched_cls=self.sched_cls)
        if lverb:
            print "submitted job", jobid_01
            print "wait for running status..."
        num_tries, i, curstate = 40, 0, jobid_01.get_state()
        while i < num_tries and curstate not in self.sched_cls.RUNNING_STATES:
            if lverb:
                print "step %02d:" % i, curstate
            time.sleep(2)
            i += 1
            curstate = jobid_01.get_state()
        assert curstate in self.sched_cls.RUNNING_STATES, "Job is not running, cannot test cancelling it"
        if lverb:
            print "job is running, now cancelling job 01..."
        self.sched_cls.job_cancel(jobid_01)
        if lverb:
            print "wait for cancelled status...."
        i, curstate = 0, jobid_01.get_state()
        while i < num_tries and curstate in self.sched_cls.RUNNING_STATES:
            if lverb:
                print "step %02d:" % i, curstate
            time.sleep(5)
            i += 1
            curstate = jobid_01.get_state()
        assert curstate == self.sched_cls.CANCELLED, "job is not cancelled: got {}".format(curstate)
        if lverb:
            print "--test_cancel_jobs01 SUCCESS"

    def test_cancel_jobs02(self, lverb=False):
        """Submit a job, then a second one dependent on the first.
        When we cancel the first, slurm should cancel the second one as well.
        """
        if lverb:
            print "---test_cancel_jobs02"
        jobid_01 = _submit_job_n(1, PRIO_MEDIUM, sched_cls=self.sched_cls)
        if lverb:
            print "started 01:", jobid_01
        time.sleep(2)
        jobid_02 = _submit_job_n(3, PRIO_MEDIUM, [jobid_01], sched_cls=self.sched_cls)
        if lverb:
            print "started 02 (dependent on 01):", jobid_02
        joblst = [jobid_01, jobid_02]
        jobidlst = [j.job_id for j in joblst]
        are_ready, i, num_tries = False, 0, 40
        while i < num_tries and not are_ready:
            time.sleep(2)
            i += 1
            curstate = get_accounting_info(joblst, sched_cls=self.sched_cls)
            if lverb:
                print "step %02d:" % i, [curstate[jid][ACC_STATE] for jid in jobidlst]
            are_ready = curstate[jobid_01.job_id][ACC_STATE] in self.sched_cls.RUNNING_STATES
        assert are_ready, "failed to submit the two jobs..."
        if lverb:
            print "OK, two jobs submitted, now cancelling job 01"
        self.sched_cls.job_cancel(jobid_01)
        check_tuple = (self.sched_cls.CANCELLED, self.sched_cls.CANCELLED)
        curstate = get_accounting_info(joblst, sched_cls=self.sched_cls)
        are_cancelled, i = False, 0
        while i < num_tries and not are_cancelled:
            if lverb:
                print "step %02d:" % i, [curstate[jid][ACC_STATE] for jid in jobidlst]
            time.sleep(2)
            i += 1
            curstate = get_accounting_info(joblst, sched_cls=self.sched_cls)
            are_cancelled = tuple([curstate[jid][ACC_STATE] for jid in jobidlst]) == check_tuple
        if lverb:
            print "final states", [curstate[jid][ACC_STATE] for jid in jobidlst]
        assert curstate[jobid_01.job_id][ACC_STATE] == self.sched_cls.CANCELLED,\
            "unexpected state 01: got {}".format(curstate[jobid_01.job_id][ACC_STATE])
        assert curstate[jobid_02.job_id][ACC_STATE] == self.sched_cls.CANCELLED,\
            "unexpected state 02: got {}".format(curstate[jobid_02.job_id][ACC_STATE])
        if lverb:
            print "---test_cancel_jobs02 SUCCESS"

    def test_get_state_01(self, lverb=False):
        """Submit a job, then follow its state using squeue.
        If slurm accounting is not properly installed, we will never get
        a COMPLETED result.
        """
        if lverb:
            print "--test_get_state_01"
        jhandle = _submit_job_n(1, PRIO_MEDIUM, sched_cls=self.sched_cls)
        if lverb:
            print "submitted job", jhandle
        i, num_tries, has_finished = 0, 20, False
        curstate = None
        while (i < num_tries) and not has_finished:
            curstate = jhandle.get_state()
            if lverb:
                print "step %02d:" % i, curstate
            has_finished = (curstate in self.sched_cls.STOPPED_SET)
            time.sleep(5)
            i += 1
        assert curstate == self.sched_cls.COMPLETED, "unexpected final state: got {}".format(curstate)
        if lverb:
            print "--test_get_state_01 SUCCESS"

    def test_set_priority_01(self, lverb=False):
        """Start some jobs with a given priority, then change it.
        See if this was successful.
        """
        if lverb:
            print "--test_set_priority_01"
        # first, submit a number of high prio jobs in order to fill the queue
        # we can only change the priority of a job if it is not yet running
        low_prio = self.sched_cls.MIN_PRIO
        high_prio = low_prio + 1
        for i in xrange(4):
            submit_all(high_prio, sched_cls=self.sched_cls)
        if lverb:
            print "submitting low_prio jobs..."
        jobhandles = submit_all(low_prio, sched_cls=self.sched_cls)
        jobidlst = [jh.job_id for jh in jobhandles]
        if lverb:
            print "job_ids", jobidlst
        time.sleep(2)
        cs = get_accounting_info(jobhandles, sched_cls=self.sched_cls)
        priolst = [(cs[jid][ACC_STATE], cs[jid][ACC_PRIONUM]) for jid in jobidlst]
        if lverb:
            print "state+priority after submission", priolst
        any_done = any([state == self.sched_cls.COMPLETED for state, prio in priolst])
        if any_done:
            raise RuntimeError("Test failed as jobs completed before we could change prio")
        # now change the priority..
        self.sched_cls.set_job_priority(jobhandles, high_prio)
        test_passed = False
        while cs[jobidlst[0]][ACC_STATE] != self.sched_cls.PENDING and not test_passed:
            if lverb:
                print "waiting"
            time.sleep(2)
            cs = get_accounting_info(jobhandles, sched_cls=self.sched_cls)
            priolst = [(cs[jid][ACC_STATE], cs[jid][ACC_PRIONUM]) for jid in jobidlst]
            test_passed = all([prio == high_prio for state, prio in priolst])
        if lverb:
            print " after wait"
        assert test_passed, "setting high prio failed"
        if lverb:
            print "Test passed"
        if lverb:
            cs = get_accounting_info(jobhandles, sched_cls=self.sched_cls)
            priolst = [(cs[jid][ACC_STATE], cs[jid][ACC_PRIONUM]) for jid in jobidlst]
            print "final states:", priolst

    def test_set_priority_02(self):
        """Set an illegal job priority type (str instead of int).
        This should raise an exception."""
        low_prio = PRIO_LOW
        jobhandles = submit_all(low_prio, sched_cls=self.sched_cls)
        with self.assertRaises(RuntimeError):
            self.sched_cls.set_job_priority(jobhandles, 'HI_PRIO')

    def test_set_priority_03(self):
        """Set an job priority that is higher than MAX_PRIO.
        This should simply set the actual priority to MAX_PRIO"""
        low_prio = PRIO_LOW
        jobhandles = submit_all(low_prio, sched_cls=self.sched_cls)
        jidlst = [jh.job_id for jh in jobhandles]
        time.sleep(1)
        high_prio = PRIO_HIGH
        self.sched_cls.set_job_priority(jobhandles, high_prio+10)
        cs = get_accounting_info(jobhandles, sched_cls=self.sched_cls)
        priolst = [cs[jid][ACC_PRIONUM] for jid in jidlst]
        assert all([p == high_prio for p in priolst]), "Failed to set high priority"

    def test_acc_info_01(self, lverb=False):
        """ Get_accounting_info must return information about all job handles
        requested.
        Where accounting info is not available, it must return the UNKNOWN state.
        NOTE: in particular with slurm, accounting information is not available in the following
        situation:
        job A in PENDING in the queue
        job B is dependent on A (with after_ok or after_any).
        ==> there will be no information of job B by accounting.
        """
        if lverb:
            print "--test_acc_info_01:"
        low_prio = PRIO_LOW
        if lverb:
            print "submitting low_prio jobs..."
        jobhandles = submit_all(low_prio, sched_cls=self.sched_cls)
        job01 = jobhandles[0]
        job02 = _submit_job_n(1, PRIO_MEDIUM, [job01], sched_cls=self.sched_cls)
        jobhandles.append(job02)
        jidlst = [jh.job_id for jh in jobhandles]
        time.sleep(1)
        i, numtests, is_finished = 0, 40, False
        while i < numtests and not is_finished:
            cs = get_accounting_info(jobhandles, sched_cls=self.sched_cls)
            if lverb:
                print i, [(cs[jid][ACC_STATE], cs[jid][ACC_PRIONUM]) for jid in jidlst]
            time.sleep(2)
            is_finished = cs[job02.job_id][ACC_STATE] == self.sched_cls.COMPLETED
            i += 1
        # --
        assert i < numtests,\
            "job02 failed to complete in 40 iterations. got state: {}".format(cs[job02.job_id][ACC_STATE])
        if lverb:
            print "--test_acc_info_01(): SUCCESS"

    def test_multi_check_output_echo(self):
        expected_output = 'Lorem ipsum\n'

        output = slurmlib.multi_check_output(['echo', 'Lorem', 'ipsum'])

        self.assertEqual(expected_output, output)

    def test_multi_check_output_echoxxx(self):
        expected_error = (
            '[Errno 2] No such file or directory: echoxxx Lorem ipsum')

        with self.assertRaises(OSError) as context:
            slurmlib.multi_check_output(['echoxxx', 'Lorem', 'ipsum'])

        self.assertEqual(expected_error, str(context.exception))

    def show_squeue_jobs01(self, lverb=False):
        """Submit all jobs with a low priority, then an additional one with high
        priority.
        List all jobs on the queue until the run queue is empty.
        NOTE: this test will not terminate if some other process is adding jobs to the
        queue.
        NOTE: this routine does not assert anything or check for correctness.
        It can be used for the user to see how priorities can/ should work.
        Exactly how priorities are handled by slurm is a configuration issue,
        and priorities could also be ignored.
        """
        if lverb:
            print "--test_squeue_jobs01"
        low_prio = PRIO_LOW
        hi_prio = PRIO_HIGH
        if lverb:
            print "submitting low_prio jobs..."
        jh_lst = submit_all(low_prio, sched_cls=self.sched_cls)
        time.sleep(1)
        jobid_01 = _submit_job_n(1, hi_prio)
        if lverb:
            print "submitted a high prio job", jobid_01
        jh_lst.append(jobid_01)
        is_done, i, num_tries = False, 0, 40
        job_state_dct = None
        while (i < num_tries) and not is_done:
            if lverb:
                print "step %d/%d" % (i, num_tries)
            job_state_dct = get_accounting_info(jh_lst, sched_cls=self.sched_cls)
            for j_state in sorted(job_state_dct.values(), key=lambda a: a[ACC_JOB_ID]):
                # has_finished = (j_state["ST"] == 'CD'  or j_state["ST"] == 'UKN')
                if lverb:
                    print "%02d: %5s  %s" % (i, j_state[ACC_JOB_ID], j_state[ACC_STATE])
                    print
                is_done = job_state_dct[jobid_01.job_id][ACC_STATE] == self.sched_cls.COMPLETED
                time.sleep(5)
                i += 1
        if lverb:
            print "exited  loop...FINAL STATE:"
            for j_state in sorted(job_state_dct.values(), key=lambda a: a[ACC_JOB_ID]):
                print "FINAL: %5s  %s" % (j_state[ACC_JOB_ID], j_state[ACC_STATE])
            print
        assert i < num_tries, "failed to wait for completed jobs!"
        if lverb:
            print "--test_squeue_jobs01 SUCCESS"


@skipIf(not settings.RUN_SLURM_TESTS, "Slurm tests disabled.")
class SlurmTests(SlurmDummyTests):
    def get_slurm_scheduler_class(self):
        return slurmlib.SlurmScheduler


if __name__ == "__main__":
    unittest.main()
