"""
Basic file-checking functionality used by Shipyard.
"""
import hashlib, glob, os
import errno
from constants import dirnames
from cStringIO import StringIO

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
            reason = "insufficient permissions on run path \"{}\"".format(run_path)
            is_okay = False

    return (is_okay, reason)

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
