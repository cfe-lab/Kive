# a low level interface to docker via the 'docker' command via Popen

import os
import re
import os.path as osp
import tempfile
import subprocess as sp
import logging

from fleet.slurmlib import multi_check_output
from django.conf import settings


logger = logging.getLogger("fleet.dockerlib")


DOCKER_COMMAND = settings.DOCK_DOCKER_COMMAND
BZIP2_COMMAND = settings.DOCK_BZIP2_COMMAND

DEFAULT_IM_FILE = osp.join(settings.DOCK_IMAGE_DIRECTORY,
                           settings.DOCK_DEFAULT_DOCKER_IMAGE)


# CHECK_OUTPUT = sp.check_output
CHECK_OUTPUT = multi_check_output


class BaseDockerHandler(object):
    """A class that interfaces to the docker run time"""

    # These are strings used in the keys of dicts returned by docker_images
    DOCKER_IMG_REPO_TAG = "REPOSITORY:TAG"
    DOCKER_IMG_IMAGE_ID = "IMAGE ID"
    DOCKER_IMG_CREATED_AT = "CREATED AT"
    DOCKER_IMG_CREATED_SINCE = "CREATED"
    DOCKER_IMG_SIZE = "SIZE"
    DOCKER_IMG_DIGEST = "DIGEST"
    DOCKER_IMG_SET = frozenset([DOCKER_IMG_REPO_TAG, DOCKER_IMG_IMAGE_ID,
                                DOCKER_IMG_CREATED_AT, DOCKER_IMG_CREATED_SINCE,
                                DOCKER_IMG_SIZE, DOCKER_IMG_DIGEST])

    # These are strings used in the keys of dicts returned by docker_ps
    DOCKER_PS_CONTAINER_ID = "CONTAINER ID"
    DOCKER_PS_IMAGE_ID = "IMAGE"
    DOCKER_PS_COMMAND = "COMMAND"
    DOCKER_PS_CREATED_AT = "CREATED AT"
    DOCKER_PS_RUNNING_FOR = "CREATED"
    DOCKER_PS_PORTS = "PORTS"
    DOCKER_PS_STATUS = "STATUS"
    DOCKER_PS_SIZE = "SIZE"
    DOCKER_PS_NAMES = "NAMES"
    DOCKER_PS_LABELS = "LABELS"
    DOCKER_PS_MOUNTS = "MOUNTS"
    DOCKER_PS_NETWORKS = "NETWORKS"
    DOCKER_PS_SET = frozenset([DOCKER_PS_CONTAINER_ID, DOCKER_PS_IMAGE_ID, DOCKER_PS_COMMAND,
                               DOCKER_PS_CREATED_AT, DOCKER_PS_RUNNING_FOR, DOCKER_PS_PORTS,
                               DOCKER_PS_STATUS, DOCKER_PS_SIZE, DOCKER_PS_NAMES,
                               DOCKER_PS_LABELS, DOCKER_PS_MOUNTS, DOCKER_PS_NETWORKS])

    @staticmethod
    def _check_user_settings():
        """Make sure user and group settings have been set."""
        err = False
        if not settings.KIVE_SANDBOX_WORKER_ACCOUNT:
            logger.error("settings.KIVE_SANDBOX_WORKER_ACCOUNT: {}".format(settings.KIVE_SANDBOX_WORKER_ACCOUNT))
            err = True
        if not settings.KIVE_PROCESSING_GROUP:
            logger.error("settings.KIVE_PROCESSING_GROUP: {}".format(settings.KIVE_PROCESSING_GROUP))
            err = True
        if err:
            raise RuntimeError("""dockerlib requires both KIVE_SANDBOX_WORKER_ACCOUNT: '{}'
 and  KIVE_PROCESSING_GROUP: '{}' to be set""".format(
                settings.KIVE_SANDBOX_WORKER_ACCOUNT,
                settings.KIVE_PROCESSING_GROUP))

    @classmethod
    def docker_is_alive(cls):
        """Return True if the docker configuration is adequate for Kive's purposes."""
        raise NotImplementedError

    @classmethod
    def docker_ident(cls):
        """Return a string with some pertinent information about the docker configuration."""
        raise NotImplementedError

    @staticmethod
    def docker_images(repotag_name=None):
        """Perform a 'docker images' command with an optional repotag name.
        If none is provided, information about all images is returned.
        Return a possibly empty list of dicts on success.
        The keys of the dicts are defined as DOCKER_IMG_* above.
        The values are all strings as returned by the docker images command.
        """
        raise NotImplementedError

    @classmethod
    def docker_ps(cls, nlast=None, filter_str=None):
        """Perform a 'docker ps' command, which shows the run state of containers.
        See https://docs.docker.com/engine/reference/commandline/ps/
        nlast (int): if provided, show at most nlast containers (regardless of state,
                     i.e. not just running containers are shown)
        filter_str: select images about which we return information. The format is a comma
        separated keyword=value string, e.g. 'exited=0,'

        Upon success, the routine returns a possibly empty list of dicts, the keys of which
        are defined as DOCKER_PS_* above.
        The values are all strings as returned by the docker ps command.
        """
        raise NotImplementedError

    @classmethod
    def generate_launchstring(cls, host_rundir, docker_rundir, preamble_cmd,
                              cmd, arglst, image_id=None):
        """Return a bash script in string form that can be used to launch a docker
        container that runs a particular command.
        host_rundir and docker_rundir: (strings) names of directories that the running
        container will mount: the host_rundir will be mounted under docker_rundir within
        the container. In addition, the working directory will be set to docker_rundir within
        the container (i.e. before the command cmd is launched in the container).
        preamble_cmd: (string) a command to execute before actual command.
        cmd (string) : the path of the command (within the docker_rundir) to run.
        arglst: (list of strings): the arguments to the command. These will be absolute
        paths.

        image_id (string): the id of the image to launch. If this is None, the default
        image is launched. This default image is determined when DockerHandler is
        initialised (if applicable -- the DummyDockerHandler has no image)"""
        # NOTE: do not use a sudo command to change uid and group, because this requires
        # kive to be in the sudoers group...
        # cmd_lst = ["cd", docker_rundir, ";",
        #           SUDO_COMMAND, "-E",
        #           "-u", settings.KIVE_SANDBOX_WORKER_ACCOUNT,
        #           "-g", settings.KIVE_PROCESSING_GROUP,
        #           cmd]
        # Wrap the driver in a script.
        driver_template = """\
#! /usr/bin/env bash
cd {}
{}
{} {}
"""
        return driver_template.format(re.escape(docker_rundir),
                                      re.escape(preamble_cmd),
                                      re.escape(os.path.join(docker_rundir, cmd)),
                                      " ".join([re.escape(x) for x in arglst]))


class DummyDockerHandler(BaseDockerHandler):
    """A dummy docker handler that does not requires docker to be installed"""
    _is_alive = False

    @classmethod
    def docker_is_alive(cls):
        if not cls._is_alive:
            cls._check_user_settings()
            cls._is_alive = True
        return cls._is_alive

    @classmethod
    def docker_ident(cls):
        if not cls._is_alive:
            raise RuntimeError("Must call docker_is_alive before docker_ident")
        return "DummyDockerHandler"

    @staticmethod
    def docker_images(repotag_name=None):
        return []

    @classmethod
    def docker_ps(cls, nlast=None, filter_str=None):
        """Check input, but don't actually do anything."""
        if not cls._is_alive:
            raise RuntimeError("Must call docker_is_alive before docker_ps")
        if nlast is not None:
            try:
                "%d" % nlast
            except TypeError as e:
                raise RuntimeError("illegal argument to nlast", e)
        return []


class DockerHandler(BaseDockerHandler):

    _is_alive = False

    @staticmethod
    def _run_shell_command(cmd_lst):
        """ Helper routine:
        Call a shell command provided in cmd_lst
        """
        logger.debug(" ".join(cmd_lst))
        stderr_fd, stderr_path = tempfile.mkstemp()
        try:
            with os.fdopen(stderr_fd, "w") as f:
                out_str = CHECK_OUTPUT(cmd_lst, stderr=f)
        except OSError:
            # typically happens if the executable cannot execute at all (e.g. not installed)
            status_report = "failed to execute '%s'" % " ".join(cmd_lst)
            logger.warning(status_report, exc_info=True)
            raise
        except sp.CalledProcessError as e:
            # typically happens if the executable did run, but returned an error
            status_report = "%s returned an error code '%s'\n\nCommand list:\n%s\n\nOutput:\n%s\n\n%s"
            try:
                with open(stderr_path, "r") as f:
                    stderr_str = "stderr:\n{}".format(f.read())
            except IOError as e:
                stderr_str = "The stderr log appears to have been lost!"
            logger.debug(status_report, cmd_lst[0], e.returncode, cmd_lst, e.output, stderr_str,
                         exc_info=True)
            raise
        finally:
            # Clean up the stderr log file.
            try:
                os.remove(stderr_path)
            except OSError:
                pass
        return out_str

    @staticmethod
    def _run_twopipe_command(cmd_lst1, cmd_lst2):
        """Run the equivalent of 'cmd1 | cmd2' and return the
        stdoutput of cmd 2.
        Raise an exception if an error occurs.
        """
        try:
            p1 = sp.Popen(cmd_lst1, stdout=sp.PIPE)
            p2 = sp.Popen(cmd_lst2, stdin=p1.stdout, stdout=sp.PIPE)
            out_tup = p2.communicate()
        except OSError as e:
            # typically happens if the executable cannot execute at all (e.g. not installed)
            status_report = "failed to execute '{} | {} ".format(" ".join(" ".join(cmd_lst1),
                                                                          " ".join(cmd_lst2)))
            logger.warning(status_report, exc_info=True)
            raise
        except sp.CalledProcessError as e:
            # typically happens if the executable did run, but returned an error
            status_report = "%s returned an error code '%s'"
            logger.debug(status_report, e.returncode, exc_info=True)
            raise
        stdout, stderr = out_tup
        return stdout

    @staticmethod
    def _run_shell_command_to_dict(cmd_lst, splitchar=None):
        out_str = DockerHandler._run_shell_command(cmd_lst)
        lns = [ln for ln in out_str.split('\n') if ln]
        logger.debug("read %d lines" % len(lns))
        nametup = tuple([s.strip() for s in lns[0].split(splitchar)])
        return [dict(zip(nametup, [s.strip() for s in ln.split(splitchar)])) for ln in lns[1:]]

    @staticmethod
    def check_is_alive():
        """Return True if the docker configuration is adequate for Kive's purposes.
        This is done by
        1) calling 'docker -v' and checking for exceptions.
        2) calling bzip2 and checking for exceptions. bzip2 is needed when loading
        a docker image from a file.
        3) calling 'docker version' and checking for exceptions.

        NOTE: the difference between 'docker -v' and 'docker version' is that the first
        is not as rigorous a test. The first is used to see whether the docker executable
        can be found. The second is used to see whether the docker daemon can be reached
        and whether the permissions on /var/run/docker.sock (used to connect to the docker daemon)
        allows communication.
        """
        is_alive = True
        for cmd_lst in [[DOCKER_COMMAND, '-v'],
                        [BZIP2_COMMAND, '-h'],
                        [DOCKER_COMMAND, 'version']]:
            try:
                DockerHandler._run_shell_command(cmd_lst)
            except (sp.CalledProcessError, OSError):
                is_alive = False
            logger.info("%s passed: %s" % (" ".join(cmd_lst), is_alive))
            if not is_alive:
                return False
        return is_alive

    @classmethod
    def _load_image_from_file(cls, image_filename):
        """Load a docker image from file into the local docker repository.
        the image_filename is a string, typically the name of a bzipped2 tarfile ('*.tar.bz2').

        Upon success, this routine returns a dictionary, as returned from docker_images(),
        containing information about the loaded image.
        Upon failure, this routine raises an exception.
        """
        if not cls._is_alive:
            raise RuntimeError("Must call docker_is_alive before load_image_from_file")
        cmd1_lst = [BZIP2_COMMAND, "-dc", image_filename]
        cmd2_lst = [DOCKER_COMMAND, "load"]
        out_str = cls._run_twopipe_command(cmd1_lst, cmd2_lst)
        # upon success, the output string will be of the form:
        # Loaded image: scottycfenet/runkive-c7:latest
        # If this is the case, use the tag name to get the ID str and return the info dict
        success_pattern = 'Loaded image:'
        if not out_str.startswith(success_pattern):
            raise RuntimeError("Failed to load docker image, output is '{}'".format(out_str))
        return cls._get_image_info(out_str[len(success_pattern):].strip())

    @staticmethod
    def docker_images(repotag_name=None):
        """Perform a 'docker images' command with an optional repotag name.
        If none is provided, all images are returned by 'docker images'.
        Return a possibly empty list of dicts on success.
        The keys of the dicts are defined as DOCKER_IMG_* above.
        The values are all strings as returned by the docker images command.
        """
        fmt_str = """table {{.Repository}}:{{.Tag}}|{{.ID}}|\
        {{.CreatedAt}}|{{.CreatedSince}}|{{.Digest}}|{{.Size}}"""
        cmd_lst = [DOCKER_COMMAND, "images", "--format", fmt_str]
        if repotag_name is not None:
            cmd_lst.append(repotag_name)
        return DockerHandler._run_shell_command_to_dict(cmd_lst, splitchar="|")

    @staticmethod
    def _get_image_info(repotag_name):
        res_lst = DockerHandler.docker_images(repotag_name)
        if len(res_lst) != 1:
            raise RuntimeError("Failed to find image {}".format(repotag_name))
        return res_lst[0]

    @classmethod
    def docker_is_alive(cls):
        if not cls._is_alive:
            cls._check_user_settings()
            is_alive = cls.check_is_alive()
            # print("CHECKO A!", is_alive)
            if is_alive:
                # make sure the default image is loaded. MUST set the cls boolean before we do this
                cls._is_alive = True
                try:
                    cls._def_dct = cls._load_image_from_file(DEFAULT_IM_FILE)
                except RuntimeError:
                    is_alive = False
            # print("CHECKO B!", is_alive)
            cls._is_alive = is_alive
        return cls._is_alive

    @classmethod
    def docker_ident(cls):
        """Return a string with some pertinent information about the docker configuration."""
        if not cls._is_alive:
            raise RuntimeError("Must call docker_is_alive before docker_ident")
        return DockerHandler._run_shell_command([DOCKER_COMMAND, "version"])

    @classmethod
    def docker_ps(cls, nlast=None, filter_str=None):
        if not cls._is_alive:
            raise RuntimeError("Must call docker_is_alive before docker_ps")
        fmt_str = """table {{.ID}}|{{.Image}}|{{.Command}}|\
{{.CreatedAt}}|{{.RunningFor}}|{{.Ports}}|{{.Status}}|{{.Size}}|{{.Names}}|\
{{.Labels}}|{{.Mounts}}|{{.Networks}}"""
        cmd_lst = [DOCKER_COMMAND, "ps", "--no-trunc", "--format", fmt_str]
        if nlast is not None:
            try:
                cmd_lst.extend(["-n", "%d" % nlast])
            except TypeError as e:
                raise RuntimeError("illegal argument to nlast", e)
        if filter_str is not None:
            cmd_lst.extend(["--filter", filter_str])
        return DockerHandler._run_shell_command_to_dict(cmd_lst, splitchar="|")

    @classmethod
    def generate_launchstring(cls, host_rundir, docker_rundir, preamble_cmd,
                              cmd, arglst, image_id=None):
        if not cls._is_alive:
            raise RuntimeError("Must call docker_is_alive before generate_launchstring")
        image_id = cls._def_dct[cls.DOCKER_IMG_IMAGE_ID] if image_id is None else image_id
        # The docker_rundir is the name of the sandbox that kive thinks it wants to run the
        # code in. The arguments in arglst have this name prepended to them.
        # we want to run the code in the docker container under a standardised directory =>
        # we replace the argument pathnames.
        actual_docker_rundir = "/data"
        actual_arglst = [oldarg.replace(docker_rundir, actual_docker_rundir, 1) for oldarg in arglst]
        dockercmd_lst = [DOCKER_COMMAND, "run",
                         "--user", "%s:%s" % (settings.KIVE_SANDBOX_WORKER_ACCOUNT, settings.KIVE_PROCESSING_GROUP),
                         "-v", "/var/run/docker.sock:/var/run/docker.sock",
                         "-v", "%s:%s" % (host_rundir, actual_docker_rundir),
                         "-w", actual_docker_rundir,
                         image_id, re.escape(os.path.join(actual_docker_rundir, cmd))]
        driver_template = """\
#! /usr/bin/env bash
{}
{} {}
"""
        return driver_template.format(re.escape(preamble_cmd),
                                      " ".join(dockercmd_lst),
                                      " ".join([re.escape(x) for x in actual_arglst]))
