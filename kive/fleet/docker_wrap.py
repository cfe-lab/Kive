#!/usr/bin/env python

""" Run a command in docker without requiring full docker permissions.

Allowing a user to run the docker command directly is equivalent to giving them
root access to the host machine. In order to avoid that, this command lets
a system administrator grant a regular user access to run a specific docker
image, without granting access to the host. All host files are read and written
as the regular user.

To grant access to this command, edit the sudoers file with visudo, and create
a command alias like this with the image name you want:

    Cmnd_Alias DOCKER_<IMAGE> = \
        /path/to/docker_wrap.py import <image> *, \
        /path/to/docker_wrap.py run <image> *, \
        /path/to/docker_wrap.py export <image> *

Then grant access to one or more users on one or more images like this:

    <user1>, <user2> ALL = NOPASSWD: DOCKER_<IMAGE1>, DOCKER_<IMAGE2>

See man sudoers for all the gory details, including digest specs.
"""
from argparse import ArgumentParser
import errno
import os
from subprocess import check_call, Popen, PIPE, CalledProcessError, check_output, STDOUT
import sys
import tarfile
from traceback import print_exc


def parse_args():
    parser = ArgumentParser(
        description='Launch a docker image with inputs and outputs',
        epilog='See the source code for details on configuring sudoers.')
    subparsers = parser.add_subparsers()

    launch_parser = subparsers.add_parser(
        'launch',
        help='Coordinate the import, run, and export commands.'
        ' Example: docker_wrap.py my_img --inputs a.txt b.txt --'
        ' sess1 /out/path cmd arg1')
    launch_parser.set_defaults(handler=handle_launch)
    launch_parser.add_argument('--sudo',
                               action='store_true',
                               help='Launch subcommands under sudo')
    launch_parser.add_argument('--inputs',
                               '-i',
                               nargs='*',
                               metavar='INPUT[{}PATH]'.format(os.pathsep),
                               default=tuple(),
                               help='list of input files and paths inside the'
                                    ' container, path defaults to file name')
    launch_parser.add_argument('--quiet',
                               '-q',
                               action='store_true',
                               help="Don't list inputs and outputs")
    launch_parser.add_argument('--script',
                               '-s',
                               default=__file__,
                               help='script file for subcommands')
    launch_parser.add_argument('image', help='Docker image hash or name')
    launch_parser.add_argument('session',
                               help='Session name for volume and container')
    launch_parser.add_argument('output_path',
                               help='Folder to copy /data/output files into')
    launch_parser.add_argument('command',
                               nargs='*',
                               help='Command and args for main process')

    import_parser = subparsers.add_parser(
        'import',
        help='Import files from tar stream on stdin to a container')
    import_parser.set_defaults(handler=handle_import)
    import_parser.add_argument('image', help='Docker image hash or name')
    import_parser.add_argument('session', help='Session name for volume and container')

    run_parser = subparsers.add_parser('run',
                                       help='Run main container process')
    run_parser.set_defaults(handler=handle_run)
    run_parser.add_argument('image', help='Docker image hash or name')
    run_parser.add_argument('session', help='Session name for volume and container')
    run_parser.add_argument('command',
                            nargs='*',
                            help='Command and args for main process')

    export_parser = subparsers.add_parser(
        'export',
        help='Export files from a container to a tar stream on stdout')
    export_parser.set_defaults(handler=handle_export)
    export_parser.add_argument('image', help='Docker image hash or name')
    export_parser.add_argument('session', help='Session name for volume and container')

    return parser.parse_args()


def handle_launch(args):
    if not args.quiet:
        print('Session: ' + expand_session(args.session))

    # Import
    import_args = create_subcommand('import', args)
    importer = Popen(import_args, stdin=PIPE)
    try:
        send_inputs(args.inputs, importer.stdin, args.quiet)
    except IOError:
        importer.terminate()
        print_exc()
    finally:
        importer.stdin.close()
        importer.wait()
        if importer.returncode:
            raise CalledProcessError(importer.returncode, import_args)

    try:
        # Run
        if not args.quiet:
            print('\nLaunching.')
            sys.stdout.flush()
        run_args = create_subcommand('run', args)
        run_args.append('--')
        for command_arg in args.command:
            run_args.append(command_arg)
        check_call(run_args)
    finally:
        # Export
        export_args = create_subcommand('export', args)
        exporter = Popen(export_args, stdout=PIPE)
        tar_file = tarfile.open(fileobj=exporter.stdout, mode='r|')
        tar_file.extractall(args.output_path,
                            members=exclude_root(tar_file, args.quiet))
        exporter.wait()
        if exporter.returncode:
            raise CalledProcessError(exporter.returncode, export_args)
    if not args.quiet:
        print('\nDone.')


def send_inputs(inputs, f, is_quiet):
    with tarfile.open(fileobj=f, mode='w|') as tar_file:
        if not is_quiet:
            print('Inputs:')
        for input_file in inputs:
            paths = input_file.split(os.pathsep)
            host_path = paths[0]
            container_path = (paths[1]
                              if len(paths) > 1
                              else os.path.basename(host_path))
            if not is_quiet:
                print('  ' + container_path)
            tar_file.add(host_path, arcname=container_path)


def exclude_root(tarinfos, is_quiet):
    if not is_quiet:
        print('\nOutputs:')
    for tarinfo in tarinfos:
        if tarinfo.name != '.':
            if not is_quiet:
                print('  ' + os.path.normpath(tarinfo.name))
            yield tarinfo


def create_subcommand(subcommand, args):
    new_args = [args.script, subcommand, args.image, args.session]
    if args.sudo:
        new_args[:0] = ['sudo']
    return new_args


def expand_session(name):
    user_name = os.environ.get('SUDO_USER') or os.environ['USER']
    return user_name + '_' + name


def handle_import(args):
    session = expand_session(args.session)
    try:
        # Shouldn't find the docker volume.
        check_output(['docker', 'volume', 'inspect', session], stderr=STDOUT)

        # Complain if we found it.
        raise OSError(errno.EEXIST,
                      'Docker volume {} already exists.'.format(session))
    except CalledProcessError:
        pass
    check_output(['docker', 'volume', 'create', '--name', session])
    is_container_created = False
    try:
        check_call(['docker',
                    'run',
                    '--name', session,
                    '-v', session + ':/data',
                    args.image,
                    'mkdir',
                    '/data/input',
                    '/data/output'])
        is_container_created = True

        check_call(['docker', 'cp', '-', session + ':/data/input'])
    except BaseException:
        clean_up(session, is_container_created)
        raise


def handle_run(args):
    docker_args = ['docker',
                   'run',
                   '--rm',
                   '-v', expand_session(args.session) + ':/data',
                   args.image] + args.command
    check_call(docker_args)


def handle_export(args):
    session = expand_session(args.session)
    check_call(['docker', 'cp', session + ':/data/output/.', '-'])
    clean_up(session)


def clean_up(session, is_container_created=True):
    if is_container_created:
        check_output(['docker', 'rm', session])
    check_output(['docker', 'volume', 'rm', session])


def main():
    args = parse_args()
    args.handler(args)


if __name__ == '__main__':
    main()
