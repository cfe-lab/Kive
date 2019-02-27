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
    def __init__(self, name):
        self.name = name
        self.helpstr = None
        self.runstr = None
        self._labdct = None
        self.err = False

    def set_help(self, helpstr):
        """Set the help information of this app."""
        if self.helpstr is None:
            self.helpstr = helpstr
        else:
            self.err = True

    def set_label(self, labelstr):
        """Set the label information of this app."""
        if self._labdct is not None:
            self.err = True
            return
        # convert this into a dict of labels...
        labdct = {}
        for l in labelstr:
            cols = l.split()
            if len(cols) <= 1:
                self.err = True
                return
            k = cols.pop(0)
            v = " ".join(cols)
            if k in labdct:
                self.err = True
                return
            labdct[k] = v
        self._labdct = labdct

    def set_run(self, runstr):
        """Set the run information. This can only occur once."""
        if self.runstr is None:
            self.runstr = runstr
        else:
            self.err = True

    def is_faulty(self):
        """Return true if some inconsistency occurred in the app definition."""
        return self.err or self.helpstr is None or\
            self._labdct is None or\
            self.runstr is None

    def __repr__(self):
        inp, outp = self.get_io_args()
        return "appinfo name: {}, inputs: {}, -> outputs: {}".format(self.name, inp, outp)

    def get_io_args(self):
        """Return a tuple (input_arg_string, output_arg_str).
        Return  in those places if KIVE_INPUTS or KIVE_OUTPUTS is not defined.
        E.g. if we have inputs but no defined outputs, we will return ('input_args', None) .
        """
        if self.err or self._labdct is None:
            return (None, None)
        return (self._labdct.get("KIVE_INPUTS", None),
                self._labdct.get("KIVE_OUTPUTS", None))

    def get_num_threads(self):
        """Return the value (int) of KIVE_THREADS or None if this label is not defined."""
        if self.err or self._labdct is None:
            return None
        strval = self._labdct.get("KIVE_THREADS", None)
        return int(strval) if strval is not None else None

    def get_memory(self):
        """Return the value (int) of KIVE_MEMORY or None if this label is not defined."""
        if self.err or self._labdct is None:
            return None
        strval = self._labdct.get("KIVE_MEMORY", None)
        return int(strval) if strval is not None else None

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
            return None
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
    Raises:
       RuntimeError: if an error occurred parsing the input string.
    """
    appdct = {}
    for chunk in chunk_string(instr):
        hed_info = chunk[0].split()
        got_kw = hed_info[0]
        appname = hed_info[1] if len(hed_info) > 1 else 'main'
        if got_kw in _MY_KW_SET:
            if appname in appdct:
                my_app = appdct[appname]
            else:
                my_app = appdct[appname] = AppInfo(appname)
            _SETTER_FUNK_DCT[got_kw](my_app, chunk[1:])
    for app in appdct.values():
        if app.is_faulty():
            raise RuntimeError("faulty app {}".format(app.name))
    return list(appdct.values())
