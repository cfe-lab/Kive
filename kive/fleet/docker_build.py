#!/usr/bin/env python

""" Build a Docker image without requiring full docker permissions.

Allowing a user to run the docker command directly is equivalent to giving them
root access to the host machine. In order to avoid that, this command lets
a system administrator grant a regular user access to build Docker images,
without granting access to the host.

To grant access to this command, edit the sudoers file with visudo, and create
a command alias like this:

    Cmnd_Alias DOCKER_BUILD = /path/to/docker_build.py *

Then grant access to one or more users like this:

    <user1>, <user2> ALL = NOPASSWD: DOCKER_BUILD

See man sudoers for all the gory details, including digest specs.
"""

from argparse import ArgumentParser
from subprocess import check_call, check_output, STDOUT, CalledProcessError
from urllib2 import URLError, Request


def parse_args(argv=None):
    parser = ArgumentParser(description="Build a Docker image.")
    parser.add_argument('image', help='Docker image name (e.g., github/alex/my-project)')
    parser.add_argument(
        'git',
        help='HTTP address of Git repository'
             ' (e.g., https://github.com/gliderlabs/docker-alpine.git)')
    parser.add_argument('tag', help='tag to use in Git and Docker')
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    request = Request(args.git)
    if request.get_type() not in ('http', 'https'):
        raise URLError('Git repository must use http or https.')
    if '#' in args.git:
        raise URLError('Git repository may not contain #fragments.')
    docker_tag = args.image + ':' + args.tag
    try:
        info_args = ['docker',
                     'image',
                     'inspect',
                     docker_tag]
        check_output(info_args, stderr=STDOUT)
        raise RuntimeError('Docker image {} already exists.'.format(docker_tag))
    except CalledProcessError:
        pass
    # sudo docker image inspect --format '{{.Id}}' hello:v1.0
    build_args = ['docker',
                  'build',
                  '-t', docker_tag,
                  args.git + '#tags/' + args.tag]
    check_call(build_args)


if __name__ == '__main__':
    main()
