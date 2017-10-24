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
        /path/to/docker_wrap.py --read <image> *, \
        /path/to/docker_wrap.py --run <image> *, \
        /path/to/docker_wrap.py --write <image> *

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
        epilog='See the source code for details on configuring sudoers.'
        ' Example: docker_wrap.py my_image --inputs a.txt b.txt'
        ' --output /out/path -- session1 command arg1 arg2')
    parser.add_argument('--sudo',
                        action='store_true',
                        help='Launch subcommands under sudo')
    parser.add_argument('--inputs',
                        '-i',
                        nargs='*',
                        metavar='INPUT[{}PATH]'.format(os.pathsep),
                        default=tuple(),
                        help='list of input files to copy into the container,'
                             ' may append paths under /mnt/input after'
                             ' a separator, but paths default to file names')
    parser.add_argument('--output',
                        '-o',
                        default='.',
                        help='folder to copy all /mnt/output files to')
    parser.add_argument('--bin_files',
                        '-b',
                        nargs='*',
                        metavar='BIN_FILE[{}PATH]'.format(os.pathsep),
                        default=tuple(),
                        help='list of binary files to copy into the container,'
                             ' may append paths under /mnt/bin after'
                             ' a separator, but paths default to file names')
    parser.add_argument('--quiet',
                        '-q',
                        action='store_true',
                        help='hide list of inputs and outputs')
    parser.add_argument('--script',
                        '-s',
                        help='script file for read, run, and write subcommands')
    parser.add_argument('image', help='Docker image hash or name')
    parser.add_argument('session',
                        help='session name for volume and container')
    parser.add_argument('command',
                        nargs='*',
                        help='command and args to run in the container')
    subcommands = parser.add_argument_group(
        'subcommand flags',
        'These request special subprocesses, usually run under the root user.'
        ' Regular users can ignore them.')
    subcommands.add_argument(
        '--read',
        action='store_true',
        help='read input files from a tar stream on stdin to a container')
    subcommands.add_argument('--run',
                             action='store_true',
                             help='run main container process')
    subcommands.add_argument(
        '--write',
        action='store_true',
        help='write output files from a container to a tar stream on stdout')

    return parser.parse_args()


def handle_launch(args):
    if args.script is None:
        expected_script = 'docker_wrap.py'
        if os.path.basename(__file__) == expected_script:
            args.script = __file__
        else:
            args.script = check_output(['which', expected_script]).strip()
    if not args.quiet:
        print('Session: ' + expand_session(args.session))

    # Import
    import_args = create_subcommand('--read', args)
    importer = Popen(import_args, stdin=PIPE)
    try:
        send_folders(args, importer)
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
        run_args = create_subcommand('--run', args)
        run_args.append('--')
        for command_arg in args.command:
            run_args.append(command_arg)
        check_call(run_args)
    finally:
        # Export
        export_args = create_subcommand('--write', args)
        exporter = Popen(export_args, stdout=PIPE)
        tar_file = tarfile.open(fileobj=exporter.stdout, mode='r|')
        tar_file.extractall(args.output,
                            members=exclude_root(tar_file, args.quiet))
        exporter.wait()
        if exporter.returncode:
            raise CalledProcessError(exporter.returncode, export_args)
    if not args.quiet:
        print('\nDone.')


def send_folders(args, importer):
    with tarfile.open(fileobj=importer.stdin, mode='w|') as tar_file:
        try:
            if args.bin_files:
                send_folder(args.bin_files, 'bin', tar_file, args.quiet)

            send_folder(args.inputs, 'input', tar_file, args.quiet)
        except IOError:
            importer.terminate()
            print_exc()


def send_folder(file_requests, folder, tar_file, is_quiet):
    if not is_quiet:
        print('{} files:'.format(folder.capitalize()))
    for file_request in file_requests:
        paths = file_request.split(os.pathsep)
        host_path = paths[0]
        container_path = (paths[1]
                          if len(paths) > 1
                          else os.path.basename(host_path))
        if not is_quiet:
            print('  ' + container_path)
        tar_file.add(host_path,
                     arcname=os.path.join(folder, container_path))


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


def handle_read(args):
    session = expand_session(args.session)
    try:
        # Shouldn't find the docker volume.
        check_output(['docker', 'volume', 'inspect', session], stderr=STDOUT)

        # Complain if we found it.
        raise OSError(errno.EEXIST,
                      'Docker volume {} already exists.'.format(session))
    except CalledProcessError:
        pass
    try:
        check_call(['docker',
                    'run',
                    '--name', session,
                    '-v', session + ':/mnt',
                    '--entrypoint', 'mkdir',
                    args.image,
                    '/mnt/bin',
                    '/mnt/input',
                    '/mnt/output'])

        check_call(['docker', 'cp', '-', session + ':/mnt'])
    except BaseException:
        clean_up(session)
        raise


def handle_run(args):
    docker_args = ['docker',
                   'run',
                   '--rm',
                   '-v', expand_session(args.session) + ':/mnt',
                   args.image] + args.command
    check_call(docker_args)


def handle_write(args):
    session = expand_session(args.session)
    check_call(['docker', 'cp', session + ':/mnt/output/.', '-'])
    clean_up(session)


def clean_up(session):
    try:
        check_output(['docker', 'rm', session])
    finally:
        check_output(['docker', 'volume', 'rm', session])


def main():
    args = parse_args()
    if args.read:
        handle_read(args)
    elif args.run:
        handle_run(args)
    elif args.write:
        handle_write(args)
    else:
        handle_launch(args)


if __name__ == '__main__':
    main()
