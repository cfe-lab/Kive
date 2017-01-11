#!/usr/bin/env python


# some simple  tests for the slurmlib module

import os
import os.path as osp
import time
import subprocess as sp
import datetime

import pytest

import fleet.slurmlib as slurmlib


# SlurmScheduler = slurmlib.SlurmScheduler
SlurmScheduler = slurmlib.DummySlurmScheduler


ACC_STATE = slurmlib.BaseSlurmScheduler.ACC_STATE
ACC_JOB_ID = slurmlib.BaseSlurmScheduler.ACC_JOB_ID
ACC_PRIORITY = slurmlib.BaseSlurmScheduler.ACC_PRIORITY


@pytest.yield_fixture(scope="session", autouse=True)
def execute_before_any_test():
    # executed before first test
    is_alive = SlurmScheduler.slurm_is_alive()
    if not is_alive:
        raise RuntimeError("slurm is not alive")
    yield
    # will be executed after last test_acc_info_01
    SlurmScheduler.shutdown()


# this is the directory where the test jobs to submit to slurm will reside
# NOTE: the job number 02 must return a non-zero exit code for testing
FAIL_JOB_NUMBER = 2

NUM_JOBS = 5

TEST_DIR = "/home/walter/scotesting/slurm-test-02/scorundir"


def test_callit01():
    """ Should return 0 """
    n = 1
    print "---test_callit01", n
    wdir = osp.join(TEST_DIR, "job%02d" % n)
    jobname = "sleep%02d.sh" % n
    arglst = []
    stderr = stdout = None
    retval = slurmlib.callit(wdir, jobname, arglst, stdout, stderr)
    print "GOOOT 01:", retval
    assert retval == 0, "expected retval 0"
    print "---END test_callit01", n


def test_callit02():
    """ Should return 2 """
    n = 2
    print "---test_callit01", n
    wdir = osp.join(TEST_DIR, "job%02d" % n)
    jobname = "sleep%02d.sh" % n
    arglst = []
    stderr = stdout = None
    retval = slurmlib.callit(wdir, jobname, arglst, stdout, stderr)
    print "GOOOT 02", retval
    assert retval == 2, "expected retval 2"
    print "---END test_callit01", n


def _submit_Njob(n, prio, afteroklst=None, afteranylst=None):
    user_id = os.getuid()
    group_id = os.getgid()
    wdir = osp.join(TEST_DIR, "job%02d" % n)
    jobname = "sleep%02d.sh" % n
    return SlurmScheduler.submit_job(wdir,
                                     jobname, [], user_id, group_id,
                                     prio, 1,
                                     osp.join(wdir, "out.txt"),
                                     osp.join(wdir, "err.txt"),
                                     after_okay=afteroklst,
                                     after_any=afteranylst)


def submit_all(prio):
    """ Submit all jobs with a certain priority."""
    return [_submit_Njob(i, prio) for i in range(1, NUM_JOBS+1)]


def test_is_alive():
    is_alive = SlurmScheduler.slurm_is_alive()
    print "Calling is_alive says:", is_alive


def test_slurm_ident():
    idstr = SlurmScheduler.slurm_ident()
    print "slurm ident is '%s'" % idstr


def test_submit_job01():
    """ Submit a job that should run successfully."""
    print "--test_submit_job01"
    try:
        jhandle = _submit_Njob(1, SlurmScheduler.PRIO_MEDIUM)
    except:
        pytest.fail("job submission failed")
    print "submitted job", jhandle


def test_submit_job02():
    """Submit a job that should NOT run (nonexistent job script)
    Submission should fail.
    """
    print "--test_submit_job02"
    user_id = os.getuid()
    group_id = os.getgid()
    prio = SlurmScheduler.PRIO_MEDIUM
    n = 1
    m = 2
    wdir = osp.join(TEST_DIR, "job%02d" % n)
    jobname = "sleep%02d.sh" % m
    with pytest.raises(sp.CalledProcessError):
        SlurmScheduler.submit_job(wdir,
                                  jobname, [], user_id, group_id,
                                  prio, 1,
                                  osp.join(wdir, "out.txt"),
                                  osp.join(wdir, "err.txt"),
                                  None)


def test_submit_job03():
    """Submit a job that should run, but returns a nonzero exit code.
    I.e. submission should succeed, but the job should have a non-zero exit code.
    """
    print "---test_submit_job03()"
    try:
        jhandle = _submit_Njob(FAIL_JOB_NUMBER, SlurmScheduler.PRIO_MEDIUM)
    except:
        pytest.fail("job submission failed")
    print "successfully launched job %s, now waiting for its failure..." % jhandle
    time.sleep(2)
    NTRY = 20
    i = 0
    print "00, get_state()",
    curstate = jhandle.get_state()
    print curstate
    while (i < NTRY) and (curstate != SlurmScheduler.FAILED):
        print i, "curstate...", curstate
        time.sleep(5)
        curstate = jhandle.get_state()
        i += 1
    assert curstate == SlurmScheduler.FAILED, "failed to get a 'FAILED' state.."
    print "---test_submit_job03(): Success, got an expected FAILED status"


def get_accounting_info(jhandles=None):
    curstates = SlurmScheduler.get_accounting_info(job_handle_iter=jhandles)
    if jhandles is not None and len(jhandles) > 0:
        # check we have entries for all requested jhandles
        jidset = set([jh.job_id for jh in jhandles])
        gotset = set(curstates.keys())
        assert gotset == jidset, "Did not get results from all submitted jobs"

    cls = slurmlib.BaseSlurmScheduler
    for jid, dct in curstates.iteritems():
        # makes sure all required fields are defined
        assert cls.ACC_SET == set(dct.keys()), "inconsistent key set"
        assert jid == dct[cls.ACC_JOB_ID]
        prio = dct[cls.ACC_PRIORITY]
        if prio is not None:
            assert prio in cls.PRIO_SET, "invalid priority value"
        for k in [cls.ACC_START_TIME, cls.ACC_END_TIME, cls.ACC_SUBMIT_TIME]:
            tval = dct[k]
            if tval is not None:
                assert isinstance(tval, datetime.datetime), "wrong type of time field"
        state = dct[cls.ACC_STATE]
        assert state in cls.ALL_STATES, "illegal state"
    return curstates


def test_dep_jobs01_okay():
    """Submit one job dependent on the other with an after_okay dependency.
    Both jobs should succeed."""
    print "--test_dep_jobs01_okay"
    try:
        jobid_01 = _submit_Njob(1, SlurmScheduler.PRIO_MEDIUM)
    except:
        pytest.fail("job01 submission failed")
    print "first job", jobid_01
    try:
        jobid_02 = _submit_Njob(3, SlurmScheduler.PRIO_MEDIUM, [jobid_01])
    except:
        pytest.fail("job02 submission failed")
    print "dependent job", jobid_02
    my_handles = [jobid_01, jobid_02]
    jobidlst = [j.job_id for j in my_handles]
    time.sleep(2)
    NTRY = 40
    i = 0
    curstate = get_accounting_info(my_handles)
    while i < NTRY and curstate[jobid_02.job_id][ACC_STATE] != SlurmScheduler.COMPLETED:
        print "step %02d:" % i, [curstate[jid][ACC_STATE] for jid in jobidlst]
        time.sleep(5)
        curstate = get_accounting_info(my_handles)
        i += 1
    if i == NTRY:
        raise RuntimeError("test inconclusive: didn't wait long enough")
    assert curstate[jobid_01.job_id][ACC_STATE] == SlurmScheduler.COMPLETED, "job01: failed to run successfully"
    assert curstate[jobid_02.job_id][ACC_STATE] == SlurmScheduler.COMPLETED, "job02: failed to run successfully"
    print "--test_dep_jobs01_okay SUCCESS"


def test_dep_jobs01_any():
    """Submit one job dependent on the other with an after_any dependency.
    Both jobs should succeed."""
    print "--test_dep_jobs01_any"
    try:
        jobid_01 = _submit_Njob(1, SlurmScheduler.PRIO_MEDIUM)
    except:
        pytest.fail("job01 submission failed")
    print "first job", jobid_01
    try:
        jobid_02 = _submit_Njob(3, SlurmScheduler.PRIO_MEDIUM, None, [jobid_01])
    except:
        pytest.fail("job02 submission failed")
    print "dependent job", jobid_02
    my_handles = [jobid_01, jobid_02]
    jobidlst = [j.job_id for j in my_handles]
    time.sleep(2)
    NTRY = 40
    i = 0
    curstate = get_accounting_info(my_handles)
    while i < NTRY and curstate[jobid_02.job_id][ACC_STATE] != SlurmScheduler.COMPLETED:
        print "step %02d:" % i, [curstate[jid][ACC_STATE] for jid in jobidlst]
        time.sleep(5)
        curstate = get_accounting_info(my_handles)
        i += 1
    if i == NTRY:
        raise RuntimeError("test inconclusive: didn't wait long enough")
    print "FINAL STATE", [curstate[jid][ACC_STATE] for jid in jobidlst]
    assert curstate[jobid_01.job_id][ACC_STATE] == SlurmScheduler.COMPLETED, "job01: failed to run successfully"
    assert curstate[jobid_02.job_id][ACC_STATE] == SlurmScheduler.COMPLETED, "job02: failed to run successfully"
    print "--test_dep_jobs01_any SUCCESS"


def test_dep_jobs02_ok():
    """Submit job 01, and job 02 dependent on 01 with an after_ok dependency.
    Job 01 will fail. Job 02 must be cancelled.
    """
    print "--test_dep_jobs02_ok"
    jobid_01 = _submit_Njob(FAIL_JOB_NUMBER, SlurmScheduler.PRIO_MEDIUM)
    print "first job that will fail:", jobid_01
    jobid_02 = _submit_Njob(3, SlurmScheduler.PRIO_MEDIUM, [jobid_01])
    print "dependent job:", jobid_02
    joblst = [jobid_01, jobid_02]
    not_failed, NTRY, i = True, 40, 0
    while (i < NTRY) and not_failed:
        time.sleep(2)
        curstate = get_accounting_info(joblst)
        print "step %02d:" % i, curstate[jobid_01.job_id][ACC_STATE], curstate[jobid_02.job_id][ACC_STATE]
        not_failed = curstate[jobid_01.job_id][ACC_STATE] != SlurmScheduler.FAILED
        i += 1
    if i == NTRY:
        raise RuntimeError("test inconclusive: didn't wait long enough")
    print "job01 state:", curstate[jobid_01.job_id]
    print "job02 state:", curstate[jobid_02.job_id]
    assert curstate[jobid_01.job_id][ACC_STATE] == SlurmScheduler.FAILED, "unexpected state 01"
    assert curstate[jobid_02.job_id][ACC_STATE] == SlurmScheduler.CANCELLED, "unexpected state 02"
    print "--test_dep_jobs02_ok SUCCESS"


def test_dep_jobs02_any():
    """Submit job 01, and job 02 dependent on 01 with an after_any dependency.
    Job 01 will fail. Job 02 must run anyway.
    """
    print "--test_dep_jobs02_any"
    jobid_01 = _submit_Njob(FAIL_JOB_NUMBER, SlurmScheduler.PRIO_MEDIUM)
    print "first job that will fail:", jobid_01
    jobid_02 = _submit_Njob(3, SlurmScheduler.PRIO_MEDIUM, None, [jobid_01])
    print "dependent job:", jobid_02
    joblst = [jobid_01, jobid_02]
    jidlst = [jh.job_id for jh in joblst]
    print "waiting for job 01 to fail"
    not_failed, NTRY, i = True, 40, 0
    while (i < NTRY) and not_failed:
        time.sleep(2)
        curstate = get_accounting_info(joblst)
        print "step %02d:" % i, [curstate[jid][ACC_STATE] for jid in jidlst]
        not_failed = curstate[jobid_01.job_id][ACC_STATE] != SlurmScheduler.FAILED
        i += 1
    if i == NTRY:
        raise RuntimeError("test inconclusive: didn't wait long enough")
    # wait for jobid_02 to start running
    print "OK, waiting for job 02 to run"
    is_running, NTRY, i = curstate[jobid_02.job_id][ACC_STATE] == SlurmScheduler.COMPLETED, 40, 0
    while (i < NTRY) and not is_running:
        time.sleep(2)
        curstate = get_accounting_info(joblst)
        print "step %02d:" % i, [curstate[jid][ACC_STATE] for jid in jidlst]
        is_running = curstate[jobid_02.job_id][ACC_STATE] == SlurmScheduler.COMPLETED
    if i == NTRY:
        raise RuntimeError("failed: job 02 did not complete")
    print "job01 state:", curstate[jobid_01.job_id]
    print "job02 state:", curstate[jobid_02.job_id]
    state_02 = curstate[jobid_02.job_id][ACC_STATE]
    ok_state_02 = state_02 == SlurmScheduler.RUNNING or state_02 == SlurmScheduler.COMPLETED
    assert curstate[jobid_01.job_id][ACC_STATE] == SlurmScheduler.FAILED, "unexpected state 01"
    assert ok_state_02, "unexpected state 02"
    print "--test_dep_jobs02_any SUCCESS"


def test_dep_jobs01_multi():
    """Submit job 01 that will fail.
    Submit job o2 that will succeed.
    Submit job 03, after_any on 01, and after_ok on 02.
    Job 03 must be run.
    """
    print "--test_dep_jobs01_multi"
    jobid_01 = _submit_Njob(FAIL_JOB_NUMBER, SlurmScheduler.PRIO_MEDIUM)
    print "first job that will fail:", jobid_01
    jobid_02 = _submit_Njob(3, SlurmScheduler.PRIO_MEDIUM)
    print "second job that will succeed", jobid_02
    jobid_03 = _submit_Njob(1, SlurmScheduler.PRIO_MEDIUM, [jobid_02], [jobid_01])
    print "third job that should run", jobid_02
    joblst = [jobid_01, jobid_02, jobid_03]
    jobidlst = [j.job_id for j in joblst]
    still_running, NTRY, i = True, 40, 0
    while (i < NTRY) and still_running:
        time.sleep(2)
        curstate = get_accounting_info(joblst)
        print "step %02d:" % i, [curstate[jid][ACC_STATE] for jid in jobidlst]
        still_running = (curstate[jobid_01.job_id][ACC_STATE] != SlurmScheduler.FAILED) or\
                        (curstate[jobid_02.job_id][ACC_STATE] != SlurmScheduler.COMPLETED)
        i += 1
    if i == NTRY:
        raise RuntimeError("test inconclusive: didn't wait long enough")
    state_01 = curstate[jobid_01.job_id][ACC_STATE]
    state_02 = curstate[jobid_02.job_id][ACC_STATE]
    state_03 = curstate[jobid_03.job_id][ACC_STATE]
    print "state after loop:", [curstate[jid][ACC_STATE] for jid in jobidlst]
    assert state_01 == SlurmScheduler.FAILED, "unexpected state 01"
    assert state_02 == SlurmScheduler.COMPLETED, "unexpected state 02"
    ok_state_03 = state_03 in SlurmScheduler.RUNNING_STATES
    assert ok_state_03, "unexpected state 03"
    print "--test_dep_jobs01_multi SUCCESS"


def test_cancel_jobs01():
    """Submit a job, then cancel it"""
    print "--test_cancel_jobs01"
    jobid_01 = _submit_Njob(1, SlurmScheduler.PRIO_MEDIUM)
    print "submitted job", jobid_01
    print "wait for running status..."
    NTRY = 40
    i, curstate = 0, jobid_01.get_state()
    while i < NTRY and curstate not in SlurmScheduler.RUNNING_STATES:
        print "step %02d:" % i, curstate
        time.sleep(2)
        i += 1
        curstate = jobid_01.get_state()
    assert curstate in SlurmScheduler.RUNNING_STATES, "Job is not running, cannot test cancelling it"
    print "job is running, now cancelling job 01..."
    SlurmScheduler.job_cancel(jobid_01)
    print "wait for cancelled status...."
    i, curstate = 0, jobid_01.get_state()
    while i < NTRY and curstate in SlurmScheduler.RUNNING_STATES:
        print "step %02d:" % i, curstate
        time.sleep(5)
        i += 1
        curstate = jobid_01.get_state()
    assert curstate == SlurmScheduler.CANCELLED, "job is not cancelled"
    print "--test_cancel_jobs01 SUCCESS"


def test_cancel_jobs02():
    """Submit a job, then a second one dependent on the first.
    When we cancel the first, slurm should cancel the second one as well.
    """
    print "---test_cancel_jobs02"
    jobid_01 = _submit_Njob(1, SlurmScheduler.PRIO_MEDIUM)
    print "started 01:", jobid_01
    time.sleep(2)
    jobid_02 = _submit_Njob(3, SlurmScheduler.PRIO_MEDIUM, [jobid_01])
    print "started 02 (dependent on 01):", jobid_02
    joblst = [jobid_01, jobid_02]
    jobidlst = [j.job_id for j in joblst]
    are_ready, i, NTRY = False, 0, 40
    while i < NTRY and not are_ready:
        time.sleep(2)
        i += 1
        curstate = get_accounting_info(joblst)
        print "step %02d:" % i, [curstate[jid][ACC_STATE] for jid in jobidlst]
        are_ready = curstate[jobid_01.job_id][ACC_STATE] in SlurmScheduler.RUNNING_STATES
    assert are_ready, "failed to submit the two jobs..."
    print "OK, two jobs submitted, now cancelling job 01"
    SlurmScheduler.job_cancel(jobid_01)
    check_tuple = (SlurmScheduler.CANCELLED, SlurmScheduler.CANCELLED)
    curstate = get_accounting_info(joblst)
    are_cancelled, i = False, 0
    while i < NTRY and not are_cancelled:
        print "step %02d:" % i, [curstate[jid][ACC_STATE] for jid in jobidlst]
        time.sleep(2)
        i += 1
        curstate = get_accounting_info(joblst)
        are_cancelled = tuple([curstate[jid][ACC_STATE] for jid in jobidlst]) == check_tuple
    print "final states", [curstate[jid][ACC_STATE] for jid in jobidlst]
    assert curstate[jobid_01.job_id][ACC_STATE] == SlurmScheduler.CANCELLED, "unexpected state 01"
    assert curstate[jobid_02.job_id][ACC_STATE] == SlurmScheduler.CANCELLED, "unexpected state 02"
    print "---test_cancel_jobs02 SUCCESS"


def test_get_state_01():
    """Submit a job, then follow its state using squeue.
    If slurm accounting is not properly installed, we will never get
    a COMPLETED result.
    """
    print "--test_get_state_01"
    jhandle = _submit_Njob(1, SlurmScheduler.PRIO_MEDIUM)
    print "submitted job", jhandle
    i = 0
    NTRY = 20
    has_finished = False
    while (i < NTRY) and not has_finished:
        curstate = jhandle.get_state()
        print "step %02d:" % i, curstate
        has_finished = (curstate in SlurmScheduler.STOPPED_SET)
        time.sleep(5)
        i += 1
    assert curstate == SlurmScheduler.COMPLETED, "unexpected final state"
    print "--test_get_state_01 SUCCESS"


def test_set_priority_01():
    """Start some jobs with a given priority, then change it.
    See if this was successful.
    """
    print "--test_set_priority_01"
    # first, submit a number of high prio jobs in order to fill the queue
    low_prio = SlurmScheduler.PRIO_LOW
    high_prio = SlurmScheduler.PRIO_HIGH
    for i in xrange(4):
        submit_all(high_prio)

    print "submitting low_prio jobs..."
    jobhandles = submit_all(low_prio)
    jobidlst = [jh.job_id for jh in jobhandles]
    print "job_ids", jobidlst
    time.sleep(2)
    cs = get_accounting_info(jobhandles)
    priolst = [(cs[jid][ACC_STATE], cs[jid][ACC_PRIORITY]) for jid in jobidlst]
    print "state+priority after submission", priolst
    SlurmScheduler.set_job_priority(jobhandles, high_prio)
    test_passed = False
    while cs[jobidlst[0]][ACC_STATE] != SlurmScheduler.PENDING and not test_passed:
        print "waiting"
        time.sleep(2)
        cs = get_accounting_info(jobhandles)
        priolst = [(cs[jid][ACC_STATE], cs[jid][ACC_PRIORITY]) for jid in jobidlst]
        test_passed = all([prio == high_prio for state, prio in priolst])
    print " after wait"
    assert test_passed, "setting high prio failed"
    print "Test passed"
    cs = get_accounting_info(jobhandles)
    priolst = [(cs[jid][ACC_STATE], cs[jid][ACC_PRIORITY]) for jid in jobidlst]
    print "final states:", priolst


def test_set_priority_02():
    """ Set an illegal job priority. This should raise an exception."""
    low_prio = SlurmScheduler.PRIO_LOW
    jobhandles = submit_all(low_prio)
    with pytest.raises(RuntimeError):
        SlurmScheduler.set_job_priority(jobhandles, 'HI_PRIO')
    
    
def test_acc_info_01():
    """ Get_accounting_info must return information about all jobs handles
    requested.
    Where accounting info is not available, it must return the UNKNOWN state.
    NOTE: in particular with slurm, accounting information is not available in the following
    situation:
    job A in PENDING in the queue
    job B is dependent on A (with after_ok or after_any).
    ==> there will be no information of job B by accounting.
    """
    print "--test_acc_info_01():"
    low_prio = SlurmScheduler.PRIO_LOW
    print "submitting low_prio jobs..."
    jobhandles = submit_all(low_prio)
    job01 = jobhandles[0]
    job02 = _submit_Njob(1, SlurmScheduler.PRIO_MEDIUM, [job01])
    jobhandles.append(job02)
    jidlst = [jh.job_id for jh in jobhandles]
    time.sleep(1)
    i, numtests, is_finished = 0, 40, False
    while i < numtests and not is_finished:
        cs = get_accounting_info(jobhandles)
        print i, [(cs[jid][ACC_STATE], cs[jid][ACC_PRIORITY]) for jid in jidlst]
        time.sleep(2)
        is_finished = cs[job02.job_id][ACC_STATE] == SlurmScheduler.COMPLETED
        i += 1
    # --
    assert i < numtests, "job02 failed to complete in 40 iterations"
    print "--test_acc_info_01(): SUCCESS"


def show_squeue_jobs01():
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
    print "--test_squeue_jobs01"
    low_prio = SlurmScheduler.PRIO_LOW
    hi_prio = SlurmScheduler.PRIO_HIGH
    print "submitting low_prio jobs..."
    jh_lst = submit_all(low_prio)
    time.sleep(1)
    jobid_01 = _submit_Njob(1, hi_prio)
    print "submitted a high prio job", jobid_01
    jh_lst.append(jobid_01)
    is_done, i, NTRY = False, 0, 40
    while (i < NTRY) and not is_done:
        print "step %d/%d" % (i, NTRY)
        job_state_dct = get_accounting_info(jh_lst)
        for j_state in sorted(job_state_dct.values(), key=lambda a: a[ACC_JOB_ID]):
            # has_finished = (j_state["ST"] == 'CD'  or j_state["ST"] == 'UKN')
            print "%02d: %5s  %s" % (i, j_state[ACC_JOB_ID], j_state[ACC_STATE])
        print
        is_done = job_state_dct[jobid_01.job_id][ACC_STATE] == SlurmScheduler.COMPLETED
        time.sleep(5)
        i += 1
    print "exited  loop...FINAL STATE:"
    for j_state in sorted(job_state_dct.values(), key=lambda a: a[ACC_JOB_ID]):
        print "FINAL: %5s  %s" % (j_state[ACC_JOB_ID], j_state[ACC_STATE])
    print
    assert i < NTRY, "failed to wait for completed jobs!"
    print "--test_squeue_jobs01 SUCCESS"





def do_test_all():
    #test_callit01()
    #test_callit02()
    test_is_alive()
    test_slurm_ident()
    test_submit_job01()
    # test_submit_job02()
    # test_submit_job03()
    # test_dep_jobs01_okay()
    # test_dep_jobs01_any()
    # test_dep_jobs02_any()
    # test_dep_jobs02_ok()
    # test_dep_jobs01_multi()
    # test_cancel_jobs01()
    # test_cancel_jobs02()
    # test_get_state_01()
    # test_squeue_jobs01()
    # test_acc_info_01()
    # test_dep_jobs01_any()
    # test_dep_jobs02_any()
    # test_set_priority_01()
    test_set_priority_02()

if __name__ == "__main__":
    do_test_all()
    SlurmScheduler.shutdown()
