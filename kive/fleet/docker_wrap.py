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
        /path/to/docker_wrap.py --read <image>\:<tag> *, \
        /path/to/docker_wrap.py --run <image>\:<tag> *, \
        /path/to/docker_wrap.py --write <image>\:<tag> *

Then grant access to one or more users on one or more images like this:

    <user1>, <user2> ALL = NOPASSWD: DOCKER_<IMAGE1>, DOCKER_<IMAGE2>

The user that runs the kive fleet should probably be allowed to run all docker
images, so create a command alias like this:

    Cmnd_Alias DOCKER_ALL = \
        /path/to/docker_wrap.py --read *, \
        /path/to/docker_wrap.py --run *, \
        /path/to/docker_wrap.py --write *

See man sudoers for all the gory details, including digest specs. See the
docker_build.py script for details on building docker images.
"""
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import errno
import os
from subprocess import Popen, PIPE, CalledProcessError, check_output, STDOUT, check_call
import sys
import tarfile
from traceback import print_exc
import signal
import time
import re
import logging
import logging.config

logging.config.dictConfig({
    "version": 1,
    'formatters': {
        'pipe_delimited': {
            'format': "%(asctime)s | %(levelname)s | %(message)s"
        }
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'pipe_delimited'
        }
    },
    'root': {
        # This is the default logger.
        'handlers': ['console'],
        'level': 'DEBUG'
    },
    'loggers': {
        "docker_wrap": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": False
        }
    }
})
logger = logging.getLogger("docker_wrap")


def parse_args():
    parser = ArgumentParser(
        description='Launch a docker image with inputs and outputs',
        epilog='See the source code for details on configuring sudoers.'
        ' Example: docker_wrap.py my_image --inputs a.txt b.txt'
        ' --output /out/path -- session1 command arg1 arg2',
        formatter_class=ArgumentDefaultsHelpFormatter
    )
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
    parser.add_argument('--workdir',
                        '-w',
                        help='working directory inside the container')
    parser.add_argument('--quiet',
                        '-q',
                        action='store_true',
                        help='hide list of inputs and outputs and suppress some logging')
    parser.add_argument('--script',
                        '-s',
                        help='script file for read, run, and write subcommands')
    parser.add_argument(
        "--time_limit",
        type=int,
        default=10,
        help="seconds to wait on a container after an interrupt signal")
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
    subcommands.add_argument(
        "--is_reading",
        action="store_true",
        help="tells the write subcommand that the read command is running (only used by --write)")
    subcommands.add_argument(
        "--is_running",
        action="store_true",
        help="tells the write subcommand that the run command is running (only used by --write)")

    args = parser.parse_args()
    if args.is_reading or args.is_running:
        assert not (args.is_reading and args.is_running), "Read and run should not be simultaneously proceeding"
        assert args.write, "--is_[reading|running] can only be used with the --write subcommand"

    return args


def stop_container(container, stdout=None, stderr=None):
    """
    Helper that invokes `docker stop`.

    This does not use `sudo`, it depends on the calling function to have superuser privileges
    if necessary.
    """
    stop_args = ["docker", "stop", "--time", "0", container]
    try:
        check_call(stop_args, stdout=stdout, stderr=stderr)
    except CalledProcessError:  # the container has already been removed
        pass


def check_container_exists(container, show_all=False):
    """
    Check that the specified container exists using `docker container list`.

    This is safer than using `docker inspect` because that seems to hang if the
    container has already been removed
    """
    check_args = ["docker", "container", "list"]
    if show_all:
        check_args.append("--all")
    container_list_str = check_output(check_args)
    container_entries = container_list_str.splitlines()[1:]
    if len(container_entries) == 0:
        return False

    for container_line in container_entries:
        if re.search(container, container_line):
            return True
    return False


def write_helper(args, is_reading=False, is_running=False):
    """
    Helper routine for handle_launch that performs the write subcommand.
    """
    export_args = create_subcommand('--write',
                                    args,
                                    is_reading=is_reading,
                                    is_running=is_running)
    exporter = Popen(export_args, stdout=PIPE)
    tar_file = tarfile.open(fileobj=exporter.stdout, mode='r|')
    tar_file.extractall(args.output,
                        members=exclude_root(tar_file, args.quiet))
    exporter.wait()
    if exporter.returncode:
        raise CalledProcessError(exporter.returncode, export_args)


def handle_launch(args):
    if args.script is None:
        expected_script = 'docker_wrap.py'
        if os.path.basename(__file__) == expected_script:
            args.script = __file__
        else:
            args.script = check_output(['which', expected_script]).strip()
    if not args.quiet:
        rw_session, run_session = expand_session(args.session)
        print('Read/write session: ' + rw_session)
        print("Run session: " + run_session)

    # Import
    import_args = create_subcommand('--read', args)
    importer = None
    try:
        importer = Popen(import_args, stdin=PIPE)
        send_folders(args, importer)
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt received by launch script.  Calling the `write` subcommand to clean up....")
        importer.stdin.close()  # this ends any `docker cp` command that's waiting for input
        write_helper(args, is_reading=True)
        raise
    finally:
        if importer is not None:
            importer.stdin.close()
            importer.wait()
            if importer.returncode:
                raise CalledProcessError(importer.returncode, import_args)

    runner = None
    cleaned_after_interrupt = False
    try:
        # Run
        if not args.quiet:
            print('\nLaunching.')
            sys.stdout.flush()
        run_args = create_subcommand('--run', args)
        run_args.append('--')
        for command_arg in args.command:
            run_args.append(command_arg)
        runner = Popen(run_args)
        runner.wait()
        if runner.returncode:
            raise CalledProcessError(runner.returncode, run_args)
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt received by launch script.  Calling the `write` subcommand to clean up....")
        write_helper(args, is_running=True)
        cleaned_after_interrupt = True
        if runner is not None:
            runner.wait()
        raise
    finally:
        # Export
        if not cleaned_after_interrupt:
            write_helper(args)

    if not args.quiet:
        print('\nDone.')


def send_folders(args, importer):
    with tarfile.open(fileobj=importer.stdin,
                      mode='w|',
                      dereference=True) as tar_file:
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


def create_subcommand(subcommand, args, is_reading=False, is_running=False):
    if is_reading or is_running:
        assert subcommand == "--write", subcommand
    new_args = []
    if args.sudo:
        new_args.append('sudo')
    new_args.append(args.script)
    new_args.append(subcommand)
    if subcommand == '--write':
        new_args.append('--time_limit')
        new_args.append(str(args.time_limit))
    if args.workdir:
        new_args.append('--workdir')
        new_args.append(args.workdir)
    if args.quiet:
        new_args.append('--quiet')
    if is_reading:
        new_args.append("--is_reading")
    if is_running:
        new_args.append("--is_running")
    new_args.append(args.image)
    new_args.append(args.session)
    return new_args


def expand_session(name):
    """
    Helper function that creates the session names for the original mkdir and the actual code being run.
    """
    user_name = os.environ.get('SUDO_USER') or os.environ['USER']
    return "{}_cp_{}".format(user_name, name), "{}_run_{}".format(user_name, name)


def handle_read(args):
    rw_session, _ = expand_session(args.session)
    try:
        # Shouldn't find the docker volume.
        check_output(['docker', 'volume', 'inspect', rw_session], stderr=STDOUT)

        # Complain if we found it.
        raise OSError(errno.EEXIST,
                      'Docker volume {} already exists.'.format(rw_session))
    except CalledProcessError:
        pass

    # This container is not immediately removed so as to function as a placeholder that prevents
    # the volume from being reaped.
    docker_rw = None
    docker_cp = None
    try:
        docker_run_args = [
            'docker',
            'run',
            '--name', rw_session,
            '-v', rw_session + ':/mnt',
            '--entrypoint', 'mkdir',
            args.image,
            '/mnt/bin',
            '/mnt/input',
            '/mnt/output'
        ]
        docker_rw = Popen(docker_run_args)
        docker_rw.wait()
        if docker_rw.returncode:
            raise CalledProcessError(docker_rw.returncode, docker_run_args)

        # If rw_session has been removed, this will fail and raise CalledProcessError.
        # If it hasn't been removed, then `docker cp` locks the container anyway, so
        # we don't have to worry about it being removed during execution.
        docker_cp_args = ['docker', 'cp', '-', rw_session + ':/mnt']
        docker_cp = Popen(docker_cp_args)
        docker_cp.wait()

    except KeyboardInterrupt:
        if docker_rw is not None and docker_rw.poll() is None:
            logger.warning("KeyboardInterrupt received -- calling `docker stop` on the read/write container....")
            stop_container(rw_session)

        if docker_cp is not None and docker_cp.poll() is None:
            docker_cp.send_signal(signal.SIGINT)
            docker_cp.wait()

        clean_up(rw_session)
        raise

    except BaseException:
        clean_up(rw_session)
        raise


def handle_run(args):
    main_session, docker_run_session = expand_session(args.session)
    docker_args = ['docker',
                   'run',
                   '--name', docker_run_session,
                   '--rm',
                   '-v', main_session + ':/mnt']
    if args.workdir:
        docker_args.append('--workdir')
        docker_args.append(args.workdir)
    docker_args.append(args.image)
    docker_args.extend(args.command)

    docker_run = None
    try:
        docker_run = Popen(docker_args)
        docker_run.wait()
        if docker_run.returncode:
            raise CalledProcessError(docker_run.returncode, docker_args)
    except KeyboardInterrupt:
        if docker_run is not None:
            logger.warning("KeyboardInterrupt received -- calling `docker stop` on the run job....")
            stop_container(docker_run_session)
        # If we get another KeyboardInterrupt in here, well, we can only do so much.
        raise


def handle_write(args):
    rw_session, run_session = expand_session(args.session)
    docker_cp = None
    try:
        # Stop the containers if necessary before doing the copy.
        container_to_stop = None
        # We already know at most one of args.is_reading and args.is_running is True.
        if args.is_reading:
            container_to_stop = rw_session
        elif args.is_running:
            container_to_stop = run_session

        if container_to_stop is not None:
            container_stopped = False
            for i in range(args.time_limit):
                if check_container_exists(container_to_stop, show_all=True):
                    logger.info("Stopping container %s....", container_to_stop)
                    stop_container(container_to_stop, stdout=PIPE, stderr=PIPE)  # suppress output
                    container_stopped = True
                    break
                else:
                    logger.info("Container %s not found yet.  Waiting 1 second....", container_to_stop)
                    time.sleep(1)
            if not container_stopped:
                logger.info("Container %s did not appear after %d seconds.  Moving on.",
                            container_to_stop, args.time_limit)

        # If args.is_reading is True, it does raise the possibility that the `docker cp` was still running;
        # this will wait for that to finish.
        docker_cp_args = ['docker', 'cp', rw_session + ':/mnt/output/.', '-']
        docker_cp = Popen(docker_cp_args)
        docker_cp.wait()
    except KeyboardInterrupt:
        # Quoth Don:
        # "If a user sends two Ctrl-C signals, I have no sympathy for them."
        # So, we don't bother checking the containers again, as a first pass through should have stopped
        # the containers on a first Ctrl-C.
        # We make sure the task has been properly killed before cleanup.
        if docker_cp is not None and docker_cp.poll() is None:
            docker_cp.send_signal(signal.SIGINT)
            docker_cp.wait()
    finally:
        clean_up(rw_session)


def clean_up(session):
    try:
        check_output(['docker', 'rm', session])
    finally:
        check_output(['docker', 'volume', 'rm', session])


def main():
    args = parse_args()

    # Configure the logging.
    if args.quiet:
        logger.setLevel(logging.WARN)

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
