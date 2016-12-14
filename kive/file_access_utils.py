"""
Basic file-checking functionality used by Kive.
"""
import hashlib, glob, os
import errno
import grp
import stat
import random
import logging
import time
import shutil

from django.conf import settings
from django.utils import timezone

from constants import dirnames

logger = logging.getLogger("file_access_utils")


def can_create_new_file(file_to_create):
    """
    Tests whether the specified file can be created.
    
    This tests whether something already exists there, and
    if not, whether the containing directory's permissions
    will allow us to create this file (and whatever 
    subdirectories are required).

    Return (True, None) if we can create this file; return
    (False, [reason why not]) if not.
    """
    reason = None
    is_okay = True
    if os.access(file_to_create, os.F_OK):
        is_okay = False
        reason = "path \"{}\" already exists".format(
            file_to_create)

    else:
        # The path did not exist; see if we can create it.
        output_dir = os.path.dirname(file_to_create)

        # If output_dir is the empty string, i.e. output_path
        # is just in the same directory as we are executing Python,
        # then we don't have to make a directory.  If it *isn't*
        # empty, then we either have to create the directory or
        # see if we can write to it if it already exists.
        if output_dir != "":
            try:
                os.makedirs(output_dir)
            except os.error:
                # Did it fail to create?
                if not os.access(output_dir, os.F_OK):
                    reason = "output directory \"{}\" could not be created".format(output_dir)
                    is_okay = False
                    return (is_okay, reason)

        else:
            output_dir = "."

        # If we reach here, the directory exists and the outputs can
        # be written to it - but only if there are sufficient
        # permissions.
        if not os.access(output_dir, os.W_OK or os.X_OK):
            reason = "insufficient permissions on run path \"{}\"".format(output_dir)
            is_okay = False

    return (is_okay, reason)


def sandbox_base_path():
    """
    Helper that produces the name of the base directory for all sandboxes.
    """
    return os.path.join(settings.MEDIA_ROOT, settings.SANDBOX_PATH)


def configure_sandbox_permissions(path):
    """
    Ensure that the specified path has the correct group and permissions.

    PRE: KIVE_SANDBOX_WORKER_ACCOUNT is either unspecified or it and
    KIVE_PROCESSING_GROUP are properly specified.
    """
    # Do nothing if we aren't using SSH and another unprivileged account
    # for execution.
    if not settings.KIVE_SANDBOX_WORKER_ACCOUNT:
        return

    # KIVE_PROCESSING_GROUP had better be set in settings, and it had
    # better be a valid group.
    kive_group = grp.getgrnam(settings.KIVE_PROCESSING_GROUP)
    os.chown(path, -1, kive_group.gr_gid)
    os.chmod(path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IROTH)


def set_up_directory(directory_to_use, tolerate=False):
    """
    Checks whether the specified directory can be used.

    That is, either we create it with appropriate permissions,
    or it exists already and is writable/executable/empty.

    If tolerate is true, we ignore the directories input_data, logs, and output_data
    """

    try:
        os.makedirs(directory_to_use)
    except os.error:
        # Check if the directory does not exist.
        if not os.access(directory_to_use, os.F_OK):
            raise ValueError("directory \"{}\" could not be created".
                             format(directory_to_use))

        # Otherwise, the directory already existed.  Check that we
        # have sufficient permissions on it, and that it is empty.
        if not os.access(directory_to_use, os.W_OK or os.X_OK):
            raise ValueError(
                "insufficient permissions on directory \"{}\"".
                format(directory_to_use))

        paths = glob.glob(directory_to_use + "/*")
        paths += glob.glob(directory_to_use + "/.*")

        for path in paths:
            if tolerate:
                if (path == os.path.join(directory_to_use, dirnames.IN_DIR) or
                        path == os.path.join(directory_to_use, dirnames.OUT_DIR) or
                        path == os.path.join(directory_to_use, dirnames.LOG_DIR)):
                    continue
            raise ValueError("Directory \"{}\" nonempty; contains file {}".format(directory_to_use, path))


def compute_md5(file_to_checksum):
    """Computes MD5 checksum of specified file.

    file_to_checksum should be an open, readable, file handle, with
    its position at the beginning, i.e. so that .read() gets the
    entire contents of the file.
    """
    md5gen = hashlib.md5()
    for line in file_to_checksum:
        md5gen.update(line)
    return md5gen.hexdigest()


def file_exists(path):
    """Does the given file exist?"""
    try:
        h = open(path, "rb")
        h.close()
        return True
    except IOError as e:
        if e.errno == errno.ENOENT:
            return False
        raise


def copy_and_confirm(source,
                     destination,
                     max_num_tries=settings.CONFIRM_COPY_RETRIES,
                     wait_min=settings.CONFIRM_COPY_WAIT_MIN,
                     wait_max=settings.CONFIRM_COPY_WAIT_MAX):
    """
    A function that copies the file at source to destination and confirms that it was successful.

    Raises an IOError if the copy failed; raises FileCreationError if the
    resulting file fails confirmation.
    """
    shutil.copyfile(source, destination)
    confirm_file_copy(source, destination, max_num_tries, wait_min, wait_max)


def confirm_file_copy(source,
                      destination,
                      max_num_tries=settings.CONFIRM_COPY_RETRIES,
                      wait_min=settings.CONFIRM_COPY_WAIT_MIN,
                      wait_max=settings.CONFIRM_COPY_WAIT_MAX):
    """
    A function to confirm that a file was copied properly.
    """
    orig_file_size = os.stat(source).st_size

    total_wait_time = 0
    for num_tries in range(max_num_tries):
        new_file_size = os.stat(destination).st_size
        if new_file_size == orig_file_size:
            # Looks like the file copied properly.
            return

        wait_time = random.uniform(wait_min, wait_max)
        logger.warning("File %s appears to not be finished copying to %s; "
                       "waiting %f seconds before retrying.",
                       source, destination, wait_time)
        time.sleep(wait_time)
        total_wait_time += wait_time

    # Check one last time.
    new_file_size = os.stat(destination).st_size
    if new_file_size != orig_file_size:
        raise FileCreationError(
            "File {} failed to copy to {}; checked {} times over {} seconds.".format(
                source,
                destination,
                max_num_tries,
                total_wait_time
            )
        )


def confirm_file_created(path,
                         max_num_tries=settings.CONFIRM_FILE_CREATED_RETRIES,
                         wait_min=settings.CONFIRM_FILE_CREATED_WAIT_MIN,
                         wait_max=settings.CONFIRM_FILE_CREATED_WAIT_MAX):
    """
    Confirm that the file is finished being created.

    It does this by checking periodically whether the file size has changed.
    After a certain amount of time has passed without any changes, it declares
    that it is fine.

    Returns the resulting MD5 of the copied file.
    """
    curr_file_size = os.stat(path).st_size if os.path.exists(path) else None

    start_time = timezone.now()
    wait_time = random.uniform(wait_min, wait_max)
    curr_md5 = None

    for num_tries in range(max_num_tries):
        if os.path.exists(path):
            pre_md5_time = timezone.now()
            # While we're waiting, we compute the MD5.
            with open(path, "rb") as f:
                curr_md5 = compute_md5(f)
            post_md5_time = timezone.now()

            seconds_elapsed = (post_md5_time - pre_md5_time).total_seconds()
        else:
            seconds_elapsed = 0

        if seconds_elapsed < wait_time:
            time.sleep(wait_time - seconds_elapsed)

        new_file_size = None
        if os.path.exists(path):
            new_file_size = os.stat(path).st_size
            if new_file_size == curr_file_size:
                # File appears to be done and hasn't changed since our last file size check
                # (so the MD5 is OK).
                return curr_md5

        if num_tries < max_num_tries:
            # Having reached here, we know that the file changed and we haven't given up yet.
            wait_time = random.uniform(wait_min, wait_max)
            logger.warning("File %s appears not to have been fully created yet; "
                           "waiting %f seconds before retrying.",
                           path,
                           wait_time)
            curr_file_size = new_file_size

    # We give up.
    end_time = timezone.now()
    if curr_md5 is not None:
        e = FileCreationError(
            "File {} did not reach a stable file size; checked {} times over {} seconds.".format(
                path,
                max_num_tries,
                (end_time - start_time).total_seconds()
            )
        )
        e.md5 = curr_md5
    else:
        e = FileCreationError(
            "File {} was not created; checked {} times over {} seconds.".format(
                path,
                max_num_tries,
                (end_time - start_time).total_seconds()
            )
        )
        e.file_not_created = True
    raise e


class FileReadHandler:
    """
    Helper class for retrieving a file handle in with-clauses
    where it is unknown a priori if the user will give a file path
    or the actual file handle.

    EG)

    with FileHandle(file_path, file_handle, access_mode) as fh:
        do_stuff

    If the file_handle is given, then seeks the beginning of the file.
    Otherwise, opens the file using the file_path.
    Only closes the file upon exit if the file source was a string file path.
    """
    def __init__(self, file_path, file_handle, access_mode):
        if file_handle:
            self.file_handle = file_handle
            self.file_path = None
        else:
            self.file_path = file_path
            self.file_handle = None
        self.access_mode = access_mode

    def __enter__(self):
        if not self.file_handle:
            self.file_handle = open(self.file_path, self.access_mode)
        else:
            self.file_handle.seek(0)
        return self.file_handle

    def __exit__(self, type, value, traceback):
        if self.file_path:
            self.file_handle.close()


class FileCreationError(Exception):
    pass