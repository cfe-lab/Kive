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
        /usr/local/bin/docker_wrap.py run <image> *, \
        /usr/local/bin/docker_wrap.py export <image> *

Then grant access to one or more users on one or more images like this:

    <user1>, <user2> ALL = NOPASSWD: DOCKER_<IMAGE1>, DOCKER_<IMAGE2>

See man sudoers for all the gory details, including digest specs.
"""
from argparse import ArgumentParser
import copy
import os
from subprocess import check_call, Popen, PIPE, CalledProcessError
import tarfile


def parse_args():
    parser = ArgumentParser(
        description='Launch a docker image with inputs and outputs',
        epilog='See the source code for details on configuring sudoers.')
    subparsers = parser.add_subparsers()

    launch_parser = subparsers.add_parser(
        'launch',
        help='Coordinate the import, run, and export commands.'
        ' Example: docker_wrap.py --inputs a.txt b.txt -- my_img sess1 ~ cmd arg1')
    launch_parser.set_defaults(handler=handle_launch)
    launch_parser.add_argument('--sudo',
                               action='store_true',
                               help='Launch subcommands under sudo')
    launch_parser.add_argument(
        '--inputs',
        '-i',
        nargs='*',
        help='List of input files to copy')
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
    # TODO: add --overwrite option.
    export_parser.add_argument('image', help='Docker image hash or name')
    export_parser.add_argument('session', help='Session name for volume and container')

    return parser.parse_args()


def handle_launch(args):
    try:
        # Import
        print('Inputs:')
        for input_file in args.inputs:
            print(input_file)
        print('\nSession:')
        import_args = create_subcommand('import', args)
        importer = Popen(import_args, stdin=PIPE)
        tar_file = tarfile.open(fileobj=importer.stdin, mode='w|')
        for input_file in args.inputs:
            tar_file.add(input_file, arcname=os.path.basename(input_file))
        tar_file.close()
        importer.stdin.close()
        importer.wait()
        if importer.returncode:
            raise CalledProcessError(importer.returncode, import_args)

        # Run
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
        tar_file.extractall(args.output_path, members=remove_output_parent(tar_file))
        exporter.wait()
        if exporter.returncode:
            raise CalledProcessError(exporter.returncode, export_args)
    print('\nDone.')


def remove_output_parent(tarinfos):
    print('\nOutputs:')
    expected_root = 'output/'
    for tarinfo in tarinfos:
        new_info = copy.copy(tarinfo)
        assert (new_info.name == expected_root[:-1] or
                new_info.name.startswith(expected_root)), new_info.name
        if new_info.name != expected_root[:-1]:
            new_info.name = new_info.name[len(expected_root):]
            print(new_info.name)
            yield new_info


def create_subcommand(subcommand, args):
    new_args = [__file__, subcommand, args.image, args.session]
    if args.sudo:
        new_args.insert(0, 'sudo')
    return new_args


def expand_session(name):
    user_name = os.environ.get('SUDO_USER') or os.environ['USER']
    return user_name + '_' + name


def handle_import(args):
    session = expand_session(args.session)
    check_call(['docker', 'volume', 'create', session])
    check_call(['docker',
                'run',
                '--name', session,
                '-v', session + ':/data',
                args.image,
                'mkdir',
                '/data/input',
                '/data/output'])

    check_call(['docker', 'cp', '-', session + ':/data/input'])


def handle_run(args):
    docker_args = ['docker',
                   'run',
                   '--rm',
                   '-v', expand_session(args.session) + ':/data',
                   args.image] + args.command
    check_call(docker_args)


def handle_export(args):
    session = expand_session(args.session)
    check_call(['docker', 'cp', session + ':/data/output/', '-'])
    check_call(['docker', 'container', 'rm', session])
    check_call(['docker', 'volume', 'rm', session])


def main():
    args = parse_args()
    args.handler(args)


main()
