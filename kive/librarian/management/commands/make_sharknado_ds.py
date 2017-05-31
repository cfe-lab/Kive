
import os
import os.path

# PEP 471: scandir will be standard from python 3.5 onward
try:
    from os import scandir
except ImportError:
    import scandir

from django.core.management.base import BaseCommand
from django.conf import settings
from librarian.models import Dataset


# define a consumer decorator according to PEP 342:
# https://www.python.org/dev/peps/pep-0342/

def consumer(func):
    def wrapper(*args, **kw):
        gen = func(*args, **kw)
        gen.next()
        return gen
    wrapper.__name__ = func.__name__
    wrapper.__dict__ = func.__dict__
    wrapper.__doc__ = func.__doc__
    return wrapper


@consumer
def bad_slinks_walk(dirname):
    """ Walk the file tree, yielding dir_entry objects of those slinks that
    do NOT point to a valid file."""
    (yield)
    try:
        for dir_entry in scandir.scandir(dirname):
            try:
                is_file = dir_entry.is_file(follow_symlinks=True)
                is_dir = dir_entry.is_dir()
                is_slink = dir_entry.is_symlink()
                if is_slink:
                    if not is_file and not is_dir:
                        # this is a slink that is dangling -- BAAAD
                        yield dir_entry
                else:
                    if is_dir:
                        # this is a directory (which may not be a slink) -- enter it
                        for de in bad_slinks_walk(dir_entry.path):
                            yield de
                    else:
                        # must be a real file -- (but not a slink): the user should remove this
                        raise RuntimeError("""Found an offending file \
(not a slink) '{}' (f={}, d={}, s={}) \n-- please remove this and \
try again""".format(dir_entry.path, is_file, is_dir, is_slink))
            except OSError as f:
                print "checking slinks failed: {}".format(f)
    except OSError as e:
        print "checking slinks failed: {}".format(e)


class Command(BaseCommand):
    help = """Make soft links (slinks) of uploaded datasets into a \
directory destdir. A slink to the CodeResources directory is also made."""

    def add_arguments(self, parser):
        parser.add_argument(
            "destdir",
            help="""Destination directory: the name of directory into which these datasets
should be copied. The directory must exist and have x and w permissions.""")
        # parser.add_argument("-v", "--verbose", action="store_true", help="be verbose")
        parser.add_argument(
            "-n",
            "--name",
            help="human-readable label for this directory",
            default=""
        )

    def check_slinks(self, destdir):
        """Make sure that all slinks in destdir point to actually existing
        files.
        Report any slinks that do not and remove them.
        """
        print "--- Checking for stale slinks"
        num_deleted = 0
        for dir_entry in bad_slinks_walk(destdir):
            print "deleting invalid slink", dir_entry.path
            try:
                os.remove(dir_entry.path)
            except OSError as e:
                print "failed to remove: {}".format(e)
            num_deleted += 1
        print "==> Removed {} stale slinks ".format(num_deleted)

    @staticmethod
    def _make_slink_code_resources(media_root, destdir):
        cr_name = "CodeResources"
        src_name = os.path.join(media_root, cr_name)
        # NOTE: we are not worried about write permissions...
        isdir, _is_perm = Command._is_writeable_dir(src_name)
        if not isdir:
            raise RuntimeError("CodeResource dir '{}' does not exist or is not a directory".format(src_name))
        dst_name = os.path.join(destdir, cr_name)
        print "--- Checking code resources slink"
        if not os.path.isdir(dst_name):
            print " -- {} -> {}".format(src_name, dst_name)
            try:
                os.symlink(src_name, dst_name)
            except os.error as e:
                print "failed to make slink '{}': {}. exiting".format(dst_name, e)
                raise
        else:
            print " -- {} exists".format(dst_name)

    @staticmethod
    def _is_writeable_dir(dirname):
        """ Return whether two booleans:
        a: the path is a directory
        b: it has w and x permissions"""
        return os.path.isdir(dirname), os.access(dirname, os.W_OK | os.X_OK)

    def handle(self, *args, **options):
        # check for existence of destdir
        destdir = options["destdir"]
        am_verbose = options["verbosity"] > 1
        # print "AM_VERBOSE", am_verbose
        is_dir, is_writeable = self._is_writeable_dir(destdir)
        if not is_dir:
            raise RuntimeError("Destination dir '{}' does not exist or is not a directory".format(destdir))
        if not is_writeable:
            raise RuntimeError("Cannot write to directory '{}'".format(destdir))
        media_root = settings.MEDIA_ROOT
        self._make_slink_code_resources(media_root, destdir)
        num_files_checked = num_files_added = 0
        destdir_set = set()
        print "--- Checking datasets"
        for ds in Dataset.objects.filter(file_source=None):
            num_files_checked += 1
            # src_name: the file name relative to mediaroot
            src_name = ds.dataset_file.name
            subdirname, fname = os.path.split(src_name)
            if am_verbose:
                print ds.name, subdirname
            if subdirname not in destdir_set:
                destdir_set.add(subdirname)
                destdirname = os.path.join(destdir, subdirname)
                is_dir, is_writeable = self._is_writeable_dir(destdirname)
                if not (is_dir and is_writeable):
                    try:
                        os.makedirs(destdirname)
                    except os.error as e:
                        print "Failed to make '{}': {}".format(destdirname, e)
            abs_dst_name = os.path.join(destdir, src_name)
            print " -- '{}'".format(abs_dst_name)
            if os.path.exists(abs_dst_name):
                if am_verbose:
                    print "file {} already exists..".format(abs_dst_name)
            else:
                abs_src_name = os.path.join(media_root, src_name)
                if am_verbose:
                    print "ln -s {} {}".format(abs_src_name, abs_dst_name)
                try:
                    os.symlink(abs_src_name, abs_dst_name)
                except os.error as e:
                    print "failed to make slink '{}': exiting".format(abs_dst_name, e)
                    raise
                num_files_added += 1
        print "==> Added {} new slinks from {} in the database".format(num_files_added, num_files_checked)

        self.check_slinks(destdir)
