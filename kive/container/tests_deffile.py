# -*- coding: utf-8 -*-
"""Test module for deffile.py"""

from __future__ import unicode_literals
from django.test import TestCase

import container.deffile as deffile
from container.models import PipelineCompletionStatus

deffile_01 = """# Generate the Singularity container to run MiCall on Kive.\nBootstrap: docker\nFrom: python:2.7.15-alpine3.6\n
%help\n    Minimal example that can run simple Python scripts under Kive.\n
    The main app generates \"Hello, World!\" messages for a list of names.\n
%labels\n    MAINTAINER BC CfE in HIV/AIDS https://github.com/cfe-lab/Kive
    KIVE_INPUTS names_csv\n    KIVE_OUTPUTS greetings_csv\n
%files\n    *.py /usr/local/share\n\n%post
    # Create a /mnt directory to mount input and output folders.\n    # mkdir /mnt/input
    # mkdir /mnt/output\n    # mkdir /mnt/bin\n\n    # Trim a bunch of extra features to reduce image size by about 25%.
    cd /usr/local/lib/python2.7\n    rm -r site-packages/* ensurepip hotshot distutils curses
    cd lib-dynload\n    rm pyexpat.so unicodedata.so _ctypes.so _tkinter.so parser.so cPickle.so \\
        _sqlite3.so _ssl.so _socket.so _curses*.so _elementtree.so \\
        zlib.so bz2.so _json.so cmath.so array.so _multibytecodec.so audioop.so \\
        _hotshot.so ossaudiodev.so _ctypes_test.so linuxaudiodev.so\n\n%runscript
    /usr/local/bin/python /usr/local/share/greetings.py \"$@\"\n\n%apphelp sums_and_products
    Read pairs of numbers, then report their sums and products.\n
%applabels sums_and_products\n    KIVE_INPUTS input_csv\n    KIVE_OUTPUTS output_csv\n
%apprun sums_and_products\n    /usr/local/bin/python /usr/local/share/sums_and_products.py \"$@\"\n"""


class DefFileTest(TestCase):

    def test_parse01(self):
        """Retrieve app information from a legal singularity def file."""
        lverb = False
        app_lst = deffile.parse_string(deffile_01)
        assert isinstance(app_lst, list), "list expected"
        assert len(app_lst) == 2, "two expected"
        for app in app_lst:
            assert isinstance(app, deffile.appinfo), "appinfo expected"
            inp, outp = app.get_IO_args()
            assert inp is not None, "inp key expected"
            assert outp is not None, "outp key expected"
            if lverb:
                print("{}: {} -> {}".format(app.name, inp, outp))
                print("   run: {}".format(app.get_runstring()))
                print("  help: {}".format(app.get_helpstring()))
                for k, v in app.get_label_dict().items():
                    print("   label: {}: {}".format(k, v))
            # --
        # assert False, "force fail"

    def test_faulty01(self):
        """Parsing a variery of faulty singularity def files should raise a RuntimeError"""
        faulty01 = """
%help
the help line
%labels
   GOO hello goo
   BLA hello bla
"""
        faulty02 = """
%help
   the simple
   help line
%runscript
   bash echo 'hello world'
"""
        faulty03 = """
%apphelp bla
   the simple
   help line
%apprun blu
   bash echo 'hello world'
%applabels bla
   GOO hello goo
   BLA hello bla
"""
        faulty04 = """
%apphelp bla
   the simple
   help line
%apprun bla
   bash echo 'hello world'
%applabels bla
   GOO
   BLA hello bla
"""
        faulty05 = """
%apphelp bla
   the simple
   help line
%apprun bla
   bash echo 'hello world'
%applabels bla
   GOO  val1
   GOO val2
"""
        faulty06 = """
%applabels bla
   FUNNY val1
   VALENTINE hello bla1 bla2
%apphelp bla
   the simple
   help line
%apprun bla
   bash echo 'hello world'
%applabels bla
   GOO val1
   BLA hello bla
"""
        faulty07 = """
%applabels main
   FUNNY val1
   VALENTINE hello bla1 bla2
%apphelp main
   the simple
   help line
%apprun main
   bash echo 'hello world'
%labels
   GOO val1
   BLA hello bla
%help
 a simple help line



%runscript
  bash do something
"""
        for faulty in [faulty01, faulty02, faulty03,
                       faulty04, faulty05, faulty06, faulty07]:
            with self.assertRaises(RuntimeError):
                deffile.parse_string(faulty)

    def test_ignore01(self):
        """Keywords of no interest to us should be be silently ignored.
        get_IO_args() should return (None, None) when KIVE_INPUTS and KIVE_OUTPUTS
        entries are missing.
        Dito get_num_threads() and get_memory().
        """
        faulty = """
%labels
   GOO hello goo
   BLA hello bla
%help
   the simple
   help line
%bla
gggg
%runscript
   bash echo 'hello world'
"""
        app_lst = deffile.parse_string(faulty)
        assert isinstance(app_lst, list), "list expected"
        assert len(app_lst) == 1, "one expected"
        app = app_lst[0]
        assert isinstance(app, deffile.appinfo), "appinfo expected"
        # print("mainapp: {}".format(app))
        iotup = app.get_IO_args()
        assert iotup == (None, None), "none expected"
        assert app.get_num_threads() is None, "none expected"
        assert app.get_memory() is None, "none expected"
        dct = app.get_label_dict()
        assert isinstance(dct, dict), "dict expected"
        assert sorted(dct.keys()) == ['BLA', 'GOO'], "wrong dict keys"

    def test_valid_pipeline01(self):
        """appinfo.as_dict() should return a dict describing a pipeline
        that can be validated successfully."""
        app_lst = deffile.parse_string(deffile_01)
        assert isinstance(app_lst, list), "list expected"
        assert len(app_lst) == 2, "two expected"
        for app in app_lst:
            pp_dct = app.as_pipeline_dict()
            assert isinstance(pp_dct, dict), "dict expected"
            # print("BLA {}".format(pp_dct))
            # exp_keys = frozenset(['appname', 'helpstring', 'runstring', 'labeldict'])
            # assert set(dd.keys()) == exp_keys, "unexpected dict keys"
            pd = PipelineCompletionStatus(pp_dct)
            if not pd.is_complete():
                print("Pipeline [] Failed".format(app.name))
                print('has inputs{}, steps {}, outputs {}'.format(pd.has_inputs,
                                                                  pd.has_steps,
                                                                  pd.has_outputs))
                print("has inp_not_conn {}, has dangly out {}".format(pd.inputs_not_connected,
                                                                      pd.dangling_outputs))
                raise RuntimeError('pipeline dict failed muster')

    def test_repr01(self):
        app_lst = deffile.parse_string(deffile_01)
        s = ""
        for app in app_lst:
            s += "{}: {}".format(app.name, app)

    def test_get_IO(self):
        """Getting argument from an un-initialised app should return None"""
        app = deffile.appinfo('bla')
        iotup = app.get_IO_args()
        assert iotup == (None, None), "none expected"
        assert app.get_num_threads() is None, "none expected"
        assert app.get_memory() is None, "none expected"
        lab_dct = app.get_label_dict()
        assert lab_dct is None, "none expected"
        h_str = app.get_helpstring()
        assert h_str is None, "none expected"
        r_str = app.get_runstring()
        assert r_str is None, "none expected"
        app.err = True
        lab_dct = app.get_label_dict()
        assert lab_dct is None, "none expected"

    def test_valid_pipeline02(self):
        ok_01 = """
%labels
   GOO hello goo
   BLA hello bla
   KIVE_THREADS 100
   KIVE_MEMORY 1000
%help
   the simple
   help line
%bla
gggg
%runscript
   bash echo 'hello world'
"""
        app_lst = deffile.parse_string(ok_01)
        assert isinstance(app_lst, list), "list expected"
        assert len(app_lst) == 1, "one expected"
        app = app_lst[0]
        n_thread = app.get_num_threads()
        assert isinstance(n_thread, int), "int expected"
        assert n_thread == 100, "100 expected"
        n_mem = app.get_memory()
        assert isinstance(n_mem, int), "int expected"
        assert n_mem == 1000, "1000 expected"
