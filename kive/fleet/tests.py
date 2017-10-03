from unittest import TestCase

from datetime import datetime

from django.contrib.auth.models import User
from mock import Mock

from archive.models import Run
from fleet.workers import Manager, ActiveRunsException
from kive.mock_setup import mocked_relations


class ManagerMockTest(TestCase):
    def setUp(self):
        self.scheduler_class = Mock(name='MockSchedulerClass')
        self.scheduler_class.slurm_is_alive.return_value = True
        self.docker_class = Mock(name='MockDockerHandlerClass')
        self.docker_class.docker_is_alive.return_value = True

    @mocked_relations(Run)
    def test_simple(self):
        Manager(slurm_sched_class=self.scheduler_class,
                docker_handler_class=self.docker_class)

    @mocked_relations(Run)
    def test_bad_slurm(self):
        self.scheduler_class.slurm_is_alive.return_value = False
        with self.assertRaisesRegexp(RuntimeError,
                                     'Slurm is down or badly configured.'):
            Manager(slurm_sched_class=self.scheduler_class,
                    docker_handler_class=self.docker_class)

    @mocked_relations(Run)
    def test_active_run_aborts(self):
        Run.objects.create(start_time=datetime(2000, 12, 21))
        with self.assertRaises(ActiveRunsException) as result:
            Manager(slurm_sched_class=self.scheduler_class,
                    docker_handler_class=self.docker_class)

        self.assertEqual(1, result.exception.count)

    @mocked_relations(Run)
    def test_active_run_not_stopped(self):
        Run.objects.create(start_time=datetime(2000, 12, 21))
        Manager(slurm_sched_class=self.scheduler_class, no_stop=True,
                docker_handler_class=self.docker_class)

    @mocked_relations(Run, User)
    def test_active_run_stopped(self):
        stop_username = 'jblow'
        User.objects.create(username=stop_username)
        Run.objects.create(start_time=datetime(2000, 12, 21))
        Manager(slurm_sched_class=self.scheduler_class,
                stop_username=stop_username,
                docker_handler_class=self.docker_class)

    @mocked_relations(Run, User)
    def test_unknown_user(self):
        stop_username = 'jblow'
        with self.assertRaises(User.DoesNotExist):
            Manager(slurm_sched_class=self.scheduler_class,
                    stop_username=stop_username,
                    docker_handler_class=self.docker_class)

