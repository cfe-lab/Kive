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
The path to docker_build.py can be a symbolic link to this file, if you want
it to be on the system path.
"""

from argparse import ArgumentParser
import os
from subprocess import check_call, check_output, STDOUT, CalledProcessError
from tempfile import NamedTemporaryFile
from six.moves.urllib.error import URLError
from six.moves.urllib.request import Request


def parse_args(argv=None):
    parser = ArgumentParser(description="Build a Docker image.")
    parser.add_argument('image', help='Docker image name (e.g., github/alex/my-project)')
    parser.add_argument(
        'git',
        help='HTTP address of Git repository'
             ' (e.g., https://github.com/gliderlabs/docker-alpine.git)')
    parser.add_argument('tag', help='tag to use in Git and Docker')
    parser.add_argument('--id',
                        action='store_true',
                        help='Display full image id on last line of output.')
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

    build_args = ['docker',
                  'build',
                  '-t', docker_tag,
                  args.git + '#tags/' + args.tag]
    if not args.id:
        id_file_name = None
    else:
        with NamedTemporaryFile(prefix='docker_image_id',
                                suffix='.txt',
                                delete=False) as f:
            id_file_name = f.name
        build_args.extend(('--iidfile', id_file_name))

    try:
        check_call(build_args)
        if args.id:
            with open(id_file_name, 'rU') as f:
                print(f.read())
    finally:
        if id_file_name is not None:
            try:
                os.remove(id_file_name)
            except OSError:
                pass


if __name__ == '__main__':
    main()
