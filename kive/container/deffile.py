"""Parse Singularity definition files for information about
installed apps etc."""

label_set = frozenset(["%applabels", "%labels"])
help_set = frozenset(["%apphelp", "%help"])
run_set = frozenset(["%apprun",   "%runscript"])
my_kw_set = label_set | help_set | run_set


def chunk_string(instr):
    ll = [l.strip() for l in instr.splitlines() if not l.startswith('#') and len(l) > 0]
    ndxlst = [ndx for ndx, l in enumerate(ll) if l.startswith('%')]
    ndxlst.append(len(ll))
    return [ll[strt:stop] for strt, stop in [(ndxlst[nn], ndxlst[nn+1]) for nn in range(len(ndxlst)-1)]]


class appinfo:
    def __init__(self, name):
        self.name = name
        self.helpstr = None
        self.runstr = None
        self._labdct = None
        self.err = False

    def _set_help(self, helpstr):
        if self.helpstr is None:
            self.helpstr = helpstr
        else:
            self.err = True

    def _set_label(self, labelstr):
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

    def _set_run(self, runstr):
        if self.runstr is None:
            self.runstr = runstr
        else:
            self.err = True

    def is_faulty(self):
        return self.err or self.helpstr is None or\
              self._labdct is None or\
              self.runstr is None

    def __repr__(self):
        inp, outp = self.get_IO_args()
        return "appinfo name: {}, inputs: {}, -> outputs: {}".format(self.name, inp, outp)

    def get_IO_args(self):
        """Return a tuple (input_arg_string, output_arg_str).
        Return None in those places if KIVE_INPUTS or KIVE_OUTPUTS is not defined.
        E.g. if we have inputs but no defined outputs, we will return ('input_args', None) .
        """
        if self.err or self._labdct is None:
            return (None, None)
        return (self._labdct.get("KIVE_INPUTS", None),
                self._labdct.get("KIVE_OUTPUTS", None))

    def get_num_threads(self):
        """Return the value (string) of KIVE_THREADS or None if this label is not defined."""
        if self.err or self._labdct is None:
            return None
        return self._labdct.get("KIVE_THREADS", None)

    def get_memory(self):
        """Return the value (string) of KIVE_MEMORY or None if this label is not defined."""
        if self.err or self._labdct is None:
            return None
        return self._labdct.get("KIVE_MEMORY", None)

    def get_label_dict(self):
        """Return all labels defined to this app in the form of a
        dict[labelname]: label value (string).
        Return None if no labels were previously successfully set.
        """
        if self.err:
            return None
        return self._labdct

    def get_helpstring(self):
        if self.err or self.helpstr is None:
            return None
        return "\n".join(self.helpstr)

    def get_runstring(self):
        if self.err or self.runstr is None:
            return None
        return "\n".join(self.runstr)

    def as_dict(self):
        """Return the appinfo as a dict."""
        return dict(appname=self.name,
                    helpstring=self.get_helpstring(),
                    runstring=self.get_runstring(),
                    labeldict=self.get_label_dict())

    def as_pipeline_dict(self):
        """Return the appinfo as a dict describing a pipeline.
        The app is described as having a single step.
        """
        dd = self.as_dict()
        inp_str, out_str = self.get_IO_args()
        inp_str = inp_str or ""
        out_str = out_str or ""
        inp_lst = [dict(dataset_name=inp_name, source_step=i) for i, inp_name in enumerate(inp_str.split(), start=1)]
        out_lst = [dict(dataset_name=inp_name, source_step=i) for i, inp_name in enumerate(out_str.split(), start=1)]
        dd['inputs'] = inp_lst
        dd['outputs'] = out_lst
        dd['steps'] = [dict(inputs=inp_lst, outputs=out_lst)]
        return dd


_setter_funk_dct = {}
for fset, funk in [(help_set, appinfo._set_help),
                   (label_set, appinfo._set_label),
                   (run_set, appinfo._set_run)]:
    for ftk in fset:
        _setter_funk_dct[ftk] = funk


def parse_string(instr):
    """Parse a string read from a singularity definition file, returning a list
    of appinfo instances.
    The main, default application of the container has the name 'main'.
    If no apps are defined, this will be only appinfo instance in the returned list.

    Raises:
       RuntimeError: if an error occurred parsing the input string, or if no appinfo
          instances were determined from this string.
    """
    appdct = {}
    for chunk in chunk_string(instr):
        hed_info = chunk[0].split()
        got_kw = hed_info[0]
        appname = hed_info[1] if len(hed_info) > 1 else 'main'
        if got_kw in my_kw_set:
            if appname in appdct:
                my_app = appdct[appname]
            else:
                my_app = appdct[appname] = appinfo(appname)
            _setter_funk_dct[got_kw](my_app, chunk[1:])
    if len(appdct) == 0:
        raise RuntimeError("no appinfo instances could be determined")
    for app in appdct.values():
        if app.is_faulty():
            raise RuntimeError("faulty app {}".format(app.name))
    return list(appdct.values())
