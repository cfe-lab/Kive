from __future__ import unicode_literals

from argparse import Namespace, ArgumentTypeError
from datetime import datetime, timedelta
from io import StringIO

import pytest

from utils.speed_plot_loader import SlurmJob, read_slurm, parse_date


def test_parse_date():
    text = '2 Mar 2010 17:04'
    expected_date = datetime(2010, 3, 2, 17, 4)

    d = parse_date(text)

    assert expected_date == d


def test_parse_date_fails():
    text = '2 Max 2010 17:04'

    with pytest.raises(
            ArgumentTypeError,
            match=r"Date did not fit 'DD Mon YYYY HH:MM' format: '2 Max 2010 17:04'\."):
        parse_date(text)


def test_read_slurm_unfiltered():
    slurm_data = StringIO("""\
JobName,JobID,State,ExitCode,Start,Elapsed,MaxRSS,NodeList
r373204s4_setup,4341163,COMPLETED,0:0,2018-03-21T15:04:42,00:00:10,,n0
batch,4341163.batch,COMPLETED,0:0,2018-03-21T15:04:42,00:00:10,1M,n0
r373204s4driver[prelim_map.py],4341164,COMPLETED,0:0,2018-03-21T15:04:52,00:00:19,,n0
batch,4341164.batch,COMPLETED,0:0,2018-03-21T15:04:52,00:00:19,2044K,n0
run373204_cable13013793,4341165,COMPLETED,0:0,2018-03-21T15:04:42,00:00:10,,n1
batch,4341165.batch,COMPLETED,0:0,2018-03-21T15:04:42,00:00:10,1160K,n1
run373204_cable13013794,4341166,COMPLETED,0:0,2018-03-21T15:04:42,00:00:11,,n0
batch,4341166.batch,COMPLETED,0:0,2018-03-21T15:04:42,00:00:11,1160K,n0
r373204s4_bookkeeping,4341167,COMPLETED,0:0,2018-03-21T15:05:11,00:00:20,,n0
batch,4341167.batch,COMPLETED,0:0,2018-03-21T15:05:11,00:00:20,1160K,n0
""")
    args = Namespace(start_date=datetime(2018, 1, 1),
                     end_date=datetime(2019, 1, 1))
    expected_jobs = [SlurmJob('setup',
                              373204,
                              datetime(2018, 3, 21, 15, 4, 42),
                              timedelta(seconds=10),
                              1,
                              'n0'),
                     SlurmJob('driver[prelim_map.py]',
                              373204,
                              datetime(2018, 3, 21, 15, 4, 52),
                              timedelta(seconds=19),
                              2,
                              'n0'),
                     SlurmJob('cable',
                              373204,
                              datetime(2018, 3, 21, 15, 4, 42),
                              timedelta(seconds=10),
                              1,
                              'n1'),
                     SlurmJob('cable',
                              373204,
                              datetime(2018, 3, 21, 15, 4, 42),
                              timedelta(seconds=11),
                              1,
                              'n0'),
                     SlurmJob('bookkeeping',
                              373204,
                              datetime(2018, 3, 21, 15, 5, 11),
                              timedelta(seconds=20),
                              1,
                              'n0')]

    slurm_jobs = list(read_slurm(slurm_data, args))

    assert expected_jobs == slurm_jobs


def test_read_slurm_filtered():
    slurm_data = StringIO("""\
JobName,JobID,State,ExitCode,Start,Elapsed,MaxRSS,NodeList
r373204s4_setup,4341163,COMPLETED,0:0,2018-03-21T15:04:42,00:00:10,,n0
batch,4341163.batch,COMPLETED,0:0,2018-03-21T15:04:42,00:00:10,1M,n0
r373204s4driver[prelim_map.py],4341164,COMPLETED,0:0,2018-03-21T15:04:52,00:00:19,,n0
batch,4341164.batch,COMPLETED,0:0,2018-03-21T15:04:52,00:00:19,2044K,n0
run373204_cable13013793,4341165,COMPLETED,0:0,2018-03-21T15:04:42,00:00:10,,n1
batch,4341165.batch,COMPLETED,0:0,2018-03-21T15:04:42,00:00:10,1160K,n1
run373204_cable13013794,4341166,COMPLETED,0:0,2018-03-21T15:04:42,00:00:11,,n0
batch,4341166.batch,COMPLETED,0:0,2018-03-21T15:04:42,00:00:11,1160K,n0
r373204s4_bookkeeping,4341167,COMPLETED,0:0,2018-03-21T15:05:11,00:00:20,,n0
batch,4341167.batch,COMPLETED,0:0,2018-03-21T15:05:11,00:00:20,1160K,n0
""")
    args = Namespace(start_date=datetime(2018, 1, 1),
                     end_date=datetime(2019, 1, 1))
    expected_jobs = [SlurmJob('driver[prelim_map.py]',
                              373204,
                              datetime(2018, 3, 21, 15, 4, 52),
                              timedelta(seconds=19),
                              2,
                              'n0')]

    slurm_jobs = list(read_slurm(slurm_data,
                                 args,
                                 job_type_filter='driver[prelim_map.py]'))

    assert expected_jobs == slurm_jobs


def test():
    slurm_data = StringIO("""\
JobName,JobID,State,ExitCode,Start,Elapsed,MaxRSS,NodeList
r373204s4_setup,4341163,COMPLETED,0:0,2018-03-21T15:04:42,00:00:10,,n0
batch,4341163.batch,COMPLETED,0:0,2018-03-21T15:04:42,00:00:10,1M,n0
r373204s4driver[prelim_map.py],4341164,COMPLETED,0:0,2018-03-21T15:04:52,00:00:19,,n0
batch,4341164.batch,COMPLETED,0:0,2018-03-21T15:04:52,00:00:19,2044K,n0
run373204_cable13013793,4341165,COMPLETED,0:0,2018-03-21T15:04:42,00:00:10,,n1
batch,4341165.batch,COMPLETED,0:0,2018-03-21T15:04:42,00:00:10,1160K,n1
run373204_cable13013794,4341166,COMPLETED,0:0,2018-03-21T15:04:42,00:00:11,,n0
batch,4341166.batch,COMPLETED,0:0,2018-03-21T15:04:42,00:00:11,1160K,n0
r373204s4_bookkeeping,4341167,COMPLETED,0:0,2018-03-21T15:05:11,00:00:20,,n0
batch,4341167.batch,COMPLETED,0:0,2018-03-21T15:05:11,00:00:20,1160K,n0
""")
    args = Namespace(start_date=datetime(2018, 3, 21, 15, 4, 50),
                     end_date=datetime(2018, 3, 21, 15, 5))
    expected_jobs = [SlurmJob('driver[prelim_map.py]',
                              373204,
                              datetime(2018, 3, 21, 15, 4, 52),
                              timedelta(seconds=19),
                              2,
                              'n0')]

    slurm_jobs = list(read_slurm(slurm_data, args))

    assert expected_jobs == slurm_jobs


def test_read_slurm_dump():
    slurm_data = StringIO("""\
5888807\t1521402891\t0\t\\N\t\\N\t\\N\t0\t0\t1\t0\t\\N\t0\
\tr373063s9driver[coverage_plots.py]\t0\t0\t4294967294\t\\N\t4340951\t1\t0\
\t0\t1003\t1004\t0\t0\t-1\t\\N\t100\toctomore\t1\t5\tkive-slow\t4293775777\
\t3\t4294967295\t1521402863\t1521402875\t1521402875\t1521402891\t0\t\t\t\t\t\
\t0\t1=1,2=100,4=1\t1=1,2=100,4=1
5888851\t1521403503\t0\t\\N\t\\N\t\\N\t0\t0\t1\t0\t\\N\t0\
\tr373079s6driver[sam2aln.py]\t0\t0\t4294967294\t\\N\t4340995\t1\t0\t0\t1003\
\t1004\t0\t0\t-1\t\\N\t1500\tn2\t1\t5\tkive-slow\t4293775733\t3\t4294967295\
\t1521403280\t1521403290\t1521403290\t1521403503\t00\t1=1,2=1500,4=1\
\t1=1,2=1500,4=1
5888877\t1521404249\t0\t\\N\t\\N\t\\N\t0\t0\t1\t0\t\\N\t0\
\trun373079_cable13013775\t0\t0\t4294967294\t\\N\t4341021\t1\t0\t0\t1003\t1004\
\t0\t0\t-1\t\\N\t100\toctomore\t1\t5\tkive-slow\t4293775707\t3\t4294967295\
\t1521404239\t1521404239\t1521404240\t1521404249\t0\t\t\t\t\t\t0\
\t1=1,2=100,4=1\t1=1,2=100,4=1
""")
    args = Namespace(start_date=datetime(2018, 1, 1),
                     end_date=datetime(2019, 1, 1))
    expected_jobs = [SlurmJob('driver[coverage_plots.py]',
                              373063,
                              datetime(2018, 3, 18, 12, 54, 35),
                              timedelta(seconds=16),
                              None,
                              'octomore'),
                     SlurmJob('driver[sam2aln.py]',
                              373079,
                              datetime(2018, 3, 18, 13, 1, 30),
                              timedelta(seconds=213),
                              None,
                              'n2'),
                     SlurmJob('cable',
                              373079,
                              datetime(2018, 3, 18, 13, 17, 20),
                              timedelta(seconds=9),
                              None,
                              'octomore')]

    slurm_jobs = list(read_slurm(slurm_data, args, is_dump=True))

    assert expected_jobs == slurm_jobs
