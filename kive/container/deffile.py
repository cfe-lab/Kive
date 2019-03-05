# -*- coding: utf-8 -*-
"""Parse Singularity definition files for information about
installed apps etc."""


_LABEL_SET = frozenset(["%applabels", "%labels"])
_HELP_SET = frozenset(["%apphelp", "%help"])
_RUN_SET = frozenset(["%apprun", "%runscript"])
_MY_KW_SET = _LABEL_SET | _HELP_SET | _RUN_SET


def chunk_string(instr):
    """Convert a single multi-line string into a list of chunks (= list of strings.)
    The beginning of each chunk is denoted by a keyword beginning with '%'.
    Empty lines are ignored.
    Continuation lines (backslash at end of a line) are honoured.
    Comment lines (first non-space character is '#') are ignored.
    """
    # treat lines ending in '\' as continuation lines
    instr = instr.replace('\\\n', ' ')
    ll_lst = [l for l in [l.strip() for l in instr.splitlines() if l] if not l.startswith('#')]
    ndxlst = [ndx for ndx, l in enumerate(ll_lst) if l.startswith('%')] + [len(ll_lst)]
    return [ll_lst[strt:stop] for strt, stop in [(ndxlst[nn], ndxlst[nn+1]) for nn in range(len(ndxlst)-1)]]


class AppInfo:
    """Collect all information needed for a container app. This information is extracted
    from a singularity def file."""

    KW_NUM_THREADS = 'numthreads'
    KW_MEMORY = 'memory'
    KW_IO_ARGS = 'io_args'
    KW_APP_NAME = 'appname'
    KW_HELP_STRING = 'helpstring'
    KW_RUN_STRING = 'runstring'
    KW_LABEL_DICT = 'labeldict'
    KW_ERROR_MESSAGES = 'error_messages'
    KEY_WORD_SET = frozenset([KW_NUM_THREADS, KW_MEMORY, KW_IO_ARGS, KW_APP_NAME, KW_HELP_STRING,
                              KW_RUN_STRING, KW_LABEL_DICT, KW_ERROR_MESSAGES])

    def __init__(self, name):
        self.name = name
        self.helpstr = None
        self.runstr = None
        self._labdct = None
        self.err = False
        self._err_msg = []

    def set_help(self, helpstr):
        """Set the help information of this app."""
        if self.helpstr is None:
            self.helpstr = helpstr
        else:
            self.err = True
            self._err_msg.append('help string set twice')

    def set_label(self, labelstr):
        """Set the label information of this app."""
        if self._labdct is not None:
            self.err = True
            self._err_msg.append('label string set twice')
            return
        # convert this into a dict of labels...
        labdct = {}
        for line in labelstr:
            cols = line.strip().split()
            if not cols:
                continue
            if len(cols) <= 1:
                self.err = True
                self._err_msg.append('empty label definition')
                return
            k = cols.pop(0)
            if k in labdct:
                self.err = True
                self._err_msg.append('label {} defined twice'.format(k))
                return
            labdct[k] = " ".join(cols)
        self._labdct = labdct

    def set_run(self, runstr):
        """Set the run information. This can only occur once."""
        if self.runstr is None:
            self.runstr = runstr
        else:
            self.err = True
            self._err_msg.append('run string set twice')

    def _check_faulty(self):
        """Check sanity of the AppInfo. This will set self.err
        and append to self._err_msg"""
        if self._labdct is None:
            self.err = True
            self._err_msg.append('labels string not set')
        if self.runstr is None:
            self.err = True
            self._err_msg.append('run string not set')
        # some additional checks, which will set self.err if there is a problem
        self.get_num_threads()
        self.get_memory()
        self.get_io_args()

    def __repr__(self):
        inp, outp = self.get_io_args()
        return "appinfo name: {}, inputs: {}, -> outputs: {}".format(self.name, inp, outp)

    def as_dict(self):
        """Return this AppInfo as a dict that can be serialised."""
        self._check_faulty()
        return {AppInfo.KW_NUM_THREADS: self.get_num_threads(),
                AppInfo.KW_MEMORY: self.get_memory(),
                AppInfo.KW_IO_ARGS: self.get_io_args(),
                AppInfo.KW_APP_NAME: self.name,
                AppInfo.KW_HELP_STRING: self.get_helpstring(),
                AppInfo.KW_RUN_STRING: self.get_runstring(),
                AppInfo.KW_LABEL_DICT: self.get_label_dict(),
                AppInfo.KW_ERROR_MESSAGES: self._err_msg if self._err_msg else None}

    def get_io_args(self):
        """Return a tuple (input_arg_string, output_arg_str).
        Return  in those places if KIVE_INPUTS or KIVE_OUTPUTS is not defined.
        E.g. if we have inputs but no defined outputs, we will return ('input_args', None) .
        """
        if self.err or self._labdct is None:
            return None, None
        inputs = self._labdct.get("KIVE_INPUTS", None)
        outputs = self._labdct.get("KIVE_OUTPUTS", None)
        if not inputs:
            self.err = True
            self._err_msg.append('missing label KIVE_INPUTS')
        if not outputs:
            self.err = True
            self._err_msg.append('missing label KIVE_OUTPUTS')
        return inputs, outputs

    def _get_int_label(self, labname):
        """Return an integer value of a labelname.
        None is returned if no label is defined.
        If the conversion to int fails, then self.err becomes True and None is returned.
        """
        if self.err or self._labdct is None:
            return None
        strval = self._labdct.get(labname, None)
        if strval is not None:
            try:
                retval = int(strval)
            except ValueError:
                retval = None
                self.err = True
                self._err_msg.append('value for {} ({}) is not an integer'.format(labname,
                                                                                  strval))
            return retval
        return None

    def get_num_threads(self):
        """Return the value (int) of KIVE_THREADS.
        Return None if this label is not defined, or if the conversion to int fails."""
        return self._get_int_label("KIVE_THREADS")

    def get_memory(self):
        """Return the value (int) of KIVE_MEMORY.
        Return None if this label is not defined, or if the conversion to int fails."""
        return self._get_int_label("KIVE_MEMORY")

    def get_label_dict(self):
        """Return all labels defined to this app in the form of a
        dict[labelname]: label value (string).
        Return None if no labels were previously successfully set.
        """
        if self.err:
            return None
        return self._labdct

    def get_helpstring(self):
        """Return the help string (which can be on multiple lines) as a single string"""
        if self.err or self.helpstr is None:
            return ''
        return "\n".join(self.helpstr)

    def get_runstring(self):
        """Return the singularity container run string (used to lauch the app or container)"""
        if self.err or self.runstr is None:
            return None
        return "\n".join(self.runstr)


_SETTER_FUNK_DCT = {}
for fset, funk in [(_HELP_SET, AppInfo.set_help),
                   (_LABEL_SET, AppInfo.set_label),
                   (_RUN_SET, AppInfo.set_run)]:
    for ftk in fset:
        _SETTER_FUNK_DCT[ftk] = funk


def parse_string(instr):
    """Parse a string read from a singularity definition file, returning a list
    of appinfo instances.
    The main, default application of the container has the name 'main'.
    If no separate apps are defined, this will be the only appinfo instance in the returned list.
    If no appinfo instances can be determined from the deffile, an empty list is returned.
    """
    appdct = {}
    default_app_name = ''
    for chunk in chunk_string(instr):
        hed_info = chunk[0].split()
        got_kw = hed_info[0]
        appname = hed_info[1] if len(hed_info) > 1 else default_app_name
        if got_kw in _MY_KW_SET:
            my_app = appdct.get(appname)
            if my_app is None:
                my_app = appdct[appname] = AppInfo(appname)
            _SETTER_FUNK_DCT[got_kw](my_app, chunk[1:])
    appdct.setdefault(default_app_name, AppInfo(default_app_name))
    return [a.as_dict() for name, a in sorted(appdct.items())]
