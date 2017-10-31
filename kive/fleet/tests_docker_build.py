from mock import patch
from subprocess import STDOUT, CalledProcessError
from unittest import TestCase
from urllib2 import URLError

from fleet.docker_build import main


@patch('fleet.docker_build.check_call')
@patch('fleet.docker_build.check_output')
class DockerBuildTest(TestCase):
    def test_simple(self, mock_check_output, mock_check_call):
        args = ['my-image', 'http://my-host/my-project.git', 'v1.0']
        expected_inspect = ['docker', 'image', 'inspect', 'my-image:v1.0']
        mock_check_output.side_effect = CalledProcessError(1, expected_inspect)
        expected_build = ['docker',
                          'build',
                          '-t', 'my-image:v1.0',
                          'http://my-host/my-project.git#tags/v1.0']

        main(args)

        mock_check_output.assert_called_once_with(expected_inspect,
                                                  stderr=STDOUT)
        mock_check_call.assert_called_once_with(expected_build)

    def test_file_protocol(self, mock_check_output, mock_check_call):
        args = ['my-image', 'file:///home/alex/secret.txt', 'v1.0']

        with self.assertRaisesRegexp(URLError,
                                     'Git repository must use http or https'):
            main(args)

        mock_check_output.assert_not_called()
        mock_check_call.assert_not_called()

    def test_fragment(self, mock_check_output, mock_check_call):
        args = ['my-image',
                'http://my-host/my-project.git#heads/my-branch',
                'v1.0']

        with self.assertRaisesRegexp(URLError,
                                     'Git repository may not contain #fragments'):
            main(args)

        mock_check_output.assert_not_called()
        mock_check_call.assert_not_called()

    def test_image_exists(self, mock_check_output, mock_check_call):
        args = ['my-image', 'http://my-host/my-project.git', 'v1.0']
        expected_inspect = ['docker', 'image', 'inspect', 'my-image:v1.0']

        with self.assertRaisesRegexp(
                RuntimeError,
                'Docker image my-image:v1.0 already exists.'):
            main(args)

        mock_check_output.assert_called_once_with(expected_inspect,
                                                  stderr=STDOUT)
        self.assertEqual([], mock_check_call.call_args_list)
