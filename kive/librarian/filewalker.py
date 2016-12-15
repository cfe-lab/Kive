#!/usr/bin/env python

import time
from datetime import datetime

# PEP 471: scandir will be standard from python 3.5 onward
try:
    from os import scandir
except ImportError:
    import scandir

import bisect
import csv


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
def iter_walk(dirname, exclude_set, grace_time_limit_ts):
    """
    Walk the file system, returning files if they are older than
    grace_time_limit_ts
    and if there names (in absolute path names) are not in the exclude_set (set of strings)

    See PEP 471 for documentation on how to use scandir().
    https://www.python.org/dev/peps/pep-0471/#os-scandir

    NOTE: This routine calls itself recursively. If this is a problem (stack overflow
    because of too deep a traversal), it could be rewritten.
    """
    (yield)
    try:
        for dir_entry in scandir.scandir(dirname):
            if (not dir_entry.name.startswith(".")) and dir_entry.path not in exclude_set:
                try:
                    stat = dir_entry.stat(follow_symlinks=False)
                    if dir_entry.is_file() and stat.st_atime < grace_time_limit_ts:
                        yield dir_entry
                    elif dir_entry.is_dir():
                        for fn in iter_walk(dir_entry.path, exclude_set, grace_time_limit_ts):
                            yield fn
                except OSError:
                    # some sort of permission or access error, simply skip this entry
                    pass
    except OSError:
        pass


class fileclass(object):
    def __init__(self, direntry):
        if direntry is not None:
            st = direntry.stat(follow_symlinks=False)
            self.setvals(direntry.path, st.st_size, st.st_atime)

    def __new__(cls, direntry):
        """We return None iff we are not able to stat a direntry.
        This can (rarely) happen when a file is removed (e.g. by somebody poking around
        in the filesystem) just after it has been scanned.
        NOTE: if direntry is None, we DO want a uninitialised object
        """
        if direntry is not None:
            try:
                direntry.stat(follow_symlinks=False)
            except:
                return None
        return object.__new__(cls)

    def setvals(self, fn, sz, at):
        self._fname = fn
        self._size = sz
        self._atime = at

    def _too_young(self, cutoff_date):
        return self._atime > cutoff_date

    def __str__(self):
        return "'%s' sz: %d, atime: %s" % (self._fname,
                                           self._size,
                                           datetime.fromtimestamp(self._atime))

    def _get_dict(self):
        return {"filename": self._fname,
                "size": self._size,
                "atime": self._atime}

SECS_PER_HR = 3600.0


class FilePurger:
    """A class that decides which dataset to purge based on a scoring function.
    The file with the biggest score value is the one that will be purged first.

    Strategy: we keep a cache of the MAX_CACHE size of the names and sizes of
    files with the largest score of all files scanned.

    When purging, next_to_purge() can be called, which returns the n files with
    the largest score in the cache.

    The cache is updated periodically, see _next_walk_time.
    """
    def __init__(self, dirname, grace_period_hrs, walk_period_hrs, logger):
        """
        dirname: the directory under which all files considered for purging live.
        grace_period_hrs (real or int): files younger than this will never be
        considered for purging.
        walk_period_hrs (real or int): the cache will be reconstructed if it is older
        than walk_period.
        logger: the logger instance to be used by this instance.
        """
        # the max number of files that we keep information about in this cache.
        self.MAX_CACHE = 1000
        # recalculate the cache if the size is smaller than this number when purging OR
        # we have exceeded _next_walk_time
        self.MIN_CACHE = 20
        self._WALK_PERIOD_SECS = walk_period_hrs*3600.0
        self._next_walk_time = time.time()
        self.dirname = dirname
        self.logger = logger
        self._grace_ts = grace_period_hrs*SECS_PER_HR
        self._empty_cache()

    def _empty_cache(self):
        self._num_scanned_files = 0
        self._totsize = 0
        self.file_lst, self.key_lst = [], []
        self.fname_dct = {}
        self.mima_sz = [0, 0, 0]
        self.mima_ti = [time.time(), 0.0, 0.0]

    def get_scaninfo(self):
        """Return some information about the cache scan in a form suitable
        for human consumption. These results are intended to be used in logging."""
        return {"num_scanned": self._num_scanned_files,
                "totsize": "%d MB" % (self._totsize / 1048576),
                "min_size": "%d bytes" % self.mima_sz[0],
                "max_size": "%d bytes" % self.mima_sz[1],
                "min_time": str(datetime.fromtimestamp(self.mima_ti[0])),
                "max_time": str(datetime.fromtimestamp(self.mima_ti[1]))}

    def _add_new_file_class(self, fclass):
        """Add a new file with a filename to the cache if its score warrants it.
        The file should not already exist in the cache.
        """
        if fclass is None:
            raise RuntimeError("None fclass added!")
        self._num_scanned_files += 1
        self._totsize += fclass._size
        # see if the min,max range is modded by this new element
        # if so, we have to recalculate the score
        recalc = False
        sz, ti = fclass._size, fclass._atime
        if sz < self.mima_sz[0]:
            self.mima_sz[0], recalc = sz, True
        if sz > self.mima_sz[1]:
            self.mima_sz[1], recalc = sz, True
        if ti < self.mima_ti[0]:
            self.mima_ti[0], recalc = ti, True
        if ti > self.mima_ti[1]:
            self.mima_ti[1], recalc = ti, True
        if recalc:
            # we append max-min in each case, and set this to zero if the number is too small
            # this information is used in _score()
            EPS = 1.0E-10
            d_sz = self.mima_sz[1] - self.mima_sz[0]
            if d_sz < EPS:
                if d_sz < 0.0:
                    raise RuntimeError("error in mima sz")
                d_sz = 0.0
            if len(self.mima_sz) == 3:
                self.mima_sz[2] = d_sz
            else:
                self.mima_sz.append(d_sz)
            # --
            d_ti = self.mima_ti[1] - self.mima_ti[0]
            if d_ti < EPS:
                if d_ti < 0.0:
                    raise RuntimeError("error in mima ti")
                d_ti = 0.0
            if len(self.mima_ti) == 3:
                self.mima_ti[2] = d_ti
            else:
                self.mima_ti.append(d_ti)
            # recalculate the score and resort the lists from scratch
            # (this will be an almost sorted list)
            for fc in self.file_lst:
                self._score(fc)
            self.file_lst.sort(key=lambda fc: fc._score)
            self.key_lst = [fc._score for fc in self.file_lst]

        doadd = False
        newscore = self._score(fclass)
        if len(self.file_lst) < self.MAX_CACHE:
            # add to the sorted list
            doadd = True
        else:
            # replace an element if required
            minscore = self.key_lst[0]
            if newscore > minscore:
                # the smallest element is thrown out and we add the new element
                self.file_lst.pop(0)
                self.key_lst.pop(0)
                doadd = True
        if doadd:
            ndx = bisect.bisect_left(self.key_lst, newscore)
            self.key_lst.insert(ndx, newscore)
            self.file_lst.insert(ndx, fclass)
            self.fname_dct[fclass._fname] = fclass

    def _score(self, fc):
        """ Calculate the score of a given fileclass.
        The score-related attributes are calculated and stored in fc, and
        the calculated score is returned.

        NOTE: for each attribute (here only filesize and access time), a weight value
        between [0..1] is calculated. A larger value always signifies that a file is more
        likely to be purged.
        The score of a file is then the average weight of all attribute weights.
        """
        wlst = []
        mi, ma, dx = self.mima_ti
        if dx != 0.0:
            # calculate a weight for file's access time
            # oldest time = biggest score
            wlst.append(1.0 - (fc._atime - mi)/dx)
        mi, ma, dx = self.mima_sz
        if dx != 0.0:
            # calculate weight for file size
            wlst.append((fc._size - mi)/dx)
        # -- add any other property weights here if required, and append the calculated
        # weight to the list.
        N = len(wlst)
        if N == 0:
            s = 0.0
        else:
            s = sum(wlst)/float(N)
        fc._score = score = s
        fc._wlst = wlst
        return score

    @staticmethod
    def _dump_walk(dirname, dump_fname):
        """ Perform a vanilla walk of the provided directory, saving our data to a csv file
        for later analysis.
        This routine is used for development purposes, not in production.
        """
        with open(dump_fname, "w") as csvfile:
            wr = csv.DictWriter(csvfile,
                                fieldnames=["filename", "size", "atime"])
            wr.writeheader()
            for dir_entry in iter_walk(dirname, exclude_set=set(), grace_time_limit_ts=time.time()):
                wr.writerow(fileclass(dir_entry)._get_dict())

    def _read_dumpfile(self, dump_fname, gracetime_hrs):
        """Read in a csv file dumped by _dump_walk() and generate the scoring function and cache.
        This routine is used for development purposes, not in production.
        """
        fc_lst = []
        with open(dump_fname, 'r') as fi:
            reader = csv.DictReader(fi)
            for row in reader:
                newfc = fileclass(None)
                newfc.setvals(row["filename"],
                              float(row["size"]),
                              float(row["atime"]))
                fc_lst.append(newfc)
        # --
        # we remove those elements that are younger than max(time) - gracetime_hrs
        oldest = max(fc_lst, key=lambda a: a._atime)
        limit_ts = oldest._atime - SECS_PER_HR*gracetime_hrs
        too_younglst, cache_lst = [], []
        ddct = {True: too_younglst, False: cache_lst}
        for fc in fc_lst:
            ddct[fc._too_young(limit_ts)].append(fc)
        self._empty_cache()
        for fc in too_younglst:
            self._add_new_file_class(fc)
        return cache_lst, too_younglst

    def _do_walk(self, exclude_set):
        gen = self._walk_gen_finished(exclude_set)
        try:
            while not gen.send(None):
                pass
        except StopIteration:
            pass

    @consumer
    def _walk_gen_finished(self, exclude_set):
        """Recalculate the cache information from scratch using a generator.
        Files or directories accessed after grace_time_limit, defined in the class,
        are not considered for purging.
        Hidden directories as well as directories and files in exclude_set are also omitted.

        This generator yields True when it has finished its walk. It yields False
        if it hasn't finished, but has run out of time (as defined by time_to_stop).
        If time_to_stop is passed in as None (by send()), then it will complete its task
        without checking the time limit until it finishes (See _do_walk() above).
        """
        grace_time_limit = time.time() - self._grace_ts
        self._empty_cache()
        # in order to help us return in time, we try to return slightly earlier.
        SAFETY_MARGIN_SECS = 0.5
        iwalk = iter_walk(self.dirname, exclude_set, grace_time_limit)
        try:
            while True:
                time_to_stop = (yield False)
                if time_to_stop is None:
                    for dir_entry in iwalk:
                        self._add_new_file_class(fileclass(dir_entry))
                    yield True
                else:
                    real_time_limit = time_to_stop - SAFETY_MARGIN_SECS
                    while time.time() < real_time_limit:
                        for i in xrange(5):
                            dir_entry = iwalk.next()
                            self._add_new_file_class(fileclass(dir_entry))
        except StopIteration:
            yield True

    def _pop(self):
        """Remove the fileclass with the largest score, update the self._totsize
        and return the fileclass.
        Return None if the data structure is empty
        """
        if len(self.key_lst) == 0:
            return None
        self.key_lst.pop()
        ret_class = self.file_lst.pop()
        fname = ret_class._fname
        del self.fname_dct[fname]
        self._totsize -= ret_class._size
        return ret_class

    @consumer
    def regenerator(self, exclude_set):
        if len(self.file_lst) < self.MIN_CACHE or time.time() > self._next_walk_time:
            # if time.time() > self._next_walk_time:
            mygen = self._walk_gen_finished(exclude_set)
            try:
                isdone = False
                while not isdone:
                    time_to_stop = (yield False)
                    isdone = mygen.send(time_to_stop)
            except StopIteration:
                pass
            self._next_walk_time = time.time() + self._WALK_PERIOD_SECS
        else:
            yield True

    @consumer
    def _next_to_purge_class(self, maxnum, exclude_set, upper_size_limit, dodelete):
        if maxnum < 1 or maxnum > self.MAX_CACHE:
            raise RuntimeError('maxnum must be 1 <= maxnum <= self.MAX_CACHE = %d' % self.MAX_CACHE)
        try:
            mygen = self.regenerator(exclude_set)
            while True:
                time_to_stop = (yield)
                mygen.send(time_to_stop)
        except StopIteration:
            pass
        # NOTE: below this line, we assume that the operations are fast, and we no
        # longer wait for a time_to_stop
        num_done = 0
        if dodelete:
            # pop elements
            do_more = self._totsize > upper_size_limit
            while do_more:
                fc = self._pop()
                if fc is None:
                    do_more = False
                elif fc._fname not in exclude_set:
                    yield fc
                    num_done += 1
                    do_more = (num_done < maxnum) and (self._totsize > upper_size_limit)
        else:
            # walk through the list from the back without changing it
            # ignore the upper_size_limit in this case, as the size of the files remains unchanged
            num_got = len(self.file_lst)
            num_todo = min(num_got, maxnum)
            ndx = num_got - 1
            while num_done < num_todo and ndx >= 0:
                fc = self.file_lst[ndx]
                ndx -= 1
                if fc._fname not in exclude_set:
                    num_done += 1
                    yield fc

    @consumer
    def next_to_purge(self, maxnum, exclude_set, upper_size_limit, dodelete):
        """A generator that returns (filename, filesize) tuples of a maxnum number of
        elements in the cache in descending score() order.
        maxnum must be in [1,.., self.MAX_CACHE] .
        The actual number of returned items can be smaller.
        Note that this generator will also return None when it has been unable to
        determine the next_to_purge within the alloted time frame.

        No file with a filename in exclude_set will be returned. However, if dodelete=True.
        it will nevertheless be removed from the filepurger cache if its score warrants it.

        If dodelete = True, each element is removed from the cache before its
        information is returned, thus updating the cache information.
        No action whatsoever is performed on the file itself, however.

        time_to_stop: a timestamp value which limits the time this generator will spend
        in the routine.

        upper_size_limit: In the case of dodelete=True, the generator will stop
        yielding (filenames, file_size) tuples when the size of all files becomes lower
        than upper_size_limit.

        If the number of elements is below MIN_CACHE, or the cache was last generated more than
        self._WALK_PERIOD_SECS time ago when this routine is called, the cache is calculated
        from scratch.
        """
        fcgen = self._next_to_purge_class(maxnum, exclude_set,
                                          upper_size_limit, dodelete)
        try:
            time_to_stop = (yield None)
            while True:
                fc = fcgen.send(time_to_stop)
                if fc is None:
                    time_to_stop = yield None
                else:
                    time_to_stop = yield (fc._fname, fc._size)
        except StopIteration:
            pass
