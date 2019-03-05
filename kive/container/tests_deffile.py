# -*- coding: utf-8 -*-
"""Test module for deffile.py"""

from __future__ import unicode_literals
from django.test import TestCase

import container.deffile as deffile

_DEFFILE_01 = r"""# Generate the Singularity container to run MiCall on Kive.
Bootstrap: docker
From: python:2.7.15-alpine3.6

%help
    Minimal example that can run simple Python scripts under Kive.

    The main app generates "Hello, World!" messages for a list of names.

%labels
    MAINTAINER BC CfE in HIV/AIDS https://github.com/cfe-lab/Kive
    KIVE_INPUTS names_csv
    KIVE_OUTPUTS greetings_csv

%files
    *.py /usr/local/share

%post
    # Create a /mnt directory to mount input and output folders.
    # mkdir /mnt/input
    # mkdir /mnt/output
    # mkdir /mnt/bin
    
    # Trim a bunch of extra features to reduce image size by about 25%.
    cd /usr/local/lib/python2.7
    rm -r site-packages/* ensurepip hotshot distutils curses
    cd lib-dynload
    rm pyexpat.so unicodedata.so _ctypes.so _tkinter.so parser.so cPickle.so \
        _sqlite3.so _ssl.so _socket.so _curses*.so _elementtree.so \
        zlib.so bz2.so _json.so cmath.so array.so _multibytecodec.so audioop.so \
        _hotshot.so ossaudiodev.so _ctypes_test.so linuxaudiodev.so

%runscript
    /usr/local/bin/python /usr/local/share/greetings.py "$@"

%apphelp sums_and_products
    Read pairs of numbers, then report their sums and products.
%applabels sums_and_products
    KIVE_INPUTS input_csv
    KIVE_OUTPUTS output_csv

%apprun sums_and_products
    /usr/local/bin/python /usr/local/share/sums_and_products.py "$@"
"""


class DefFileTest(TestCase):

    def test_parse01(self):
        """Retrieve app information from a legal singularity def file."""
        expected_apps = [dict(appname='',
                              io_args=('names_csv', 'greetings_csv'),
                              memory=None,
                              numthreads=None,
                              error_messages=None),
                         dict(appname='sums_and_products',
                              io_args=('input_csv', 'output_csv'),
                              memory=None,
                              numthreads=None,
                              error_messages=None)]

        app_lst = deffile.parse_string(_DEFFILE_01)

        for app in app_lst:
            del app['helpstring']
            del app['runstring']
            del app['labeldict']
        self.assertEqual(expected_apps, app_lst)

    def test_parse_whitespace_line(self):
        def_file = _DEFFILE_01.replace('\n%files', '    \n%files')
        app_list = deffile.parse_string(def_file)

        main_app = app_list[0]

        self.assertEqual('', main_app['appname'])
        self.assertIsNone(main_app['error_messages'])

    def test_faulty01(self):
        """Parsing a variety of faulty singularity def files should raise a RuntimeError"""
        scenarios = [("""
%help
the help line
%labels
   GOO hello goo
   BLA hello bla
   KIVE_INPUTS in1_csv
   KIVE_OUTPUTS out1_csv
""", {'': ['run string not set']}),
                     ("""
%help
   the simple
   help line
%runscript
   bash echo 'hello world'
""", {'': ['labels string not set']}),
                     ("""
%help
   the simple
   help line
%runscript
   bash echo 'hello world'
%labels
   GOO hello goo
   BLA hello bla
   KIVE_INPUTS in1_csv
""", {'': ['missing label KIVE_OUTPUTS']}),
                     ("""
%help
   the simple
   help line
%runscript
   bash echo 'hello world'
%labels
   GOO hello goo
   BLA hello bla
""", {'': ['missing label KIVE_INPUTS', 'missing label KIVE_OUTPUTS']}),
                     ("""
%apphelp bla
   the simple
   help line
%apprun blu
   bash echo 'hello world'
%applabels bla
   GOO hello goo
   BLA hello bla
   KIVE_INPUTS in1_csv
   KIVE_OUTPUTS out1_csv
""", {
                         '': ['labels string not set', 'run string not set'],
                         'bla': ['run string not set'],
                         'blu': ['labels string not set']}),
                     ("""
%labels
   KIVE_INPUTS in1_csv
   KIVE_OUTPUTS out1_csv
%runscript
   bash echo 'hello world'
%apphelp bla
   the simple
   help line
%apprun bla
   bash echo 'hello world'
%applabels bla
   GOO
   BLA hello bla
""", {'bla': ['empty label definition', 'labels string not set']}),
                     ("""
%labels
   KIVE_INPUTS in1_csv
   KIVE_OUTPUTS out1_csv
%runscript
   bash echo 'hello world'
%apphelp bla
   the simple
   help line
%apprun bla
   bash echo 'hello world'
%applabels bla
   GOO  val1
   GOO val2
""", {'bla': ['label GOO defined twice', 'labels string not set']}),
                     ("""
%labels
   KIVE_INPUTS in1_csv
   KIVE_OUTPUTS out1_csv
%runscript
   bash echo 'hello world'
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
""", {'bla': ['label string set twice']}),
                     ("""
%labels
   KIVE_INPUTS in1_csv
   KIVE_OUTPUTS out1_csv
%runscript
   bash echo 'hello world'
%applabels main
   FUNNY val1
   VALENTINE hello bla1 bla2
   KIVE_THREADS 100 # a comment
%apphelp main
   the simple
   help line
%apprun main
   bash echo 'hello world'
""", {'main': ['value for KIVE_THREADS (100 # a comment) is not an integer']})]

        for faulty_text, expected_errors in scenarios:
            app_list = deffile.parse_string(faulty_text)
            self.assertTrue(app_list)
            error_lists = {
                app_info[deffile.AppInfo.KW_APP_NAME]:
                    app_info[deffile.AppInfo.KW_ERROR_MESSAGES]
                for app_info in app_list
                if app_info[deffile.AppInfo.KW_ERROR_MESSAGES]}
            self.assertEqual(expected_errors, error_lists)

    def test_ignore01(self):
        """Keywords of no interest to us should be be silently ignored.
        get_io_args() should return (None, None) when KIVE_INPUTS and KIVE_OUTPUTS
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
        app_dct = app_lst[0]
        assert isinstance(app_dct, dict), "dict expected"
        assert set(app_dct.keys()) == deffile.AppInfo.KEY_WORD_SET, 'faulty dict keys'
        # print("mainapp: {}".format(app_dct))
        iotup = app_dct[deffile.AppInfo.KW_IO_ARGS]
        assert iotup == (None, None), "none expected"
        assert app_dct[deffile.AppInfo.KW_NUM_THREADS] is None, "none expected"
        assert app_dct[deffile.AppInfo.KW_MEMORY] is None, "none expected"
        # dct = app.get_label_dict()
        # assert isinstance(dct, dict), "dict expected"
        # assert sorted(dct.keys()) == ['BLA', 'GOO'], "wrong dict keys"

    def test_get_io(self):
        """Getting argument from an un-initialised app should return None"""
        app = deffile.AppInfo('bla')
        iotup = app.get_io_args()
        assert iotup == (None, None), "none expected"
        assert app.get_num_threads() is None, "none expected"
        assert app.get_memory() is None, "none expected"
        lab_dct = app.get_label_dict()
        assert lab_dct is None, "none expected"
        h_str = app.get_helpstring()
        self.assertEqual('', h_str)
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
   KIVE_INPUTS in1_csv
   KIVE_OUTPUTS out1_csv
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
        app_dct = app_lst[0]
        assert set(app_dct.keys()) == deffile.AppInfo.KEY_WORD_SET, 'faulty dict keys'
        self.assertIsNone(app_dct[deffile.AppInfo.KW_ERROR_MESSAGES])
        n_thread = app_dct[deffile.AppInfo.KW_NUM_THREADS]
        self.assertEqual(100, n_thread)
        n_mem = app_dct[deffile.AppInfo.KW_MEMORY]
        assert isinstance(n_mem, int), "int expected"
        assert n_mem == 1000, "1000 expected"

    def test_chunk_string01(self):
        t1 = r"""\
# hello
%start
one\
two\
three
%stop
"""
        # NOTE: leading white space before %start
        t2 = r"""\
# hello
  %start
one\
two\
three
# ignore this line
%stop
"""
        # no labels in the string at all...
        t3 = r"""\

hello

"""
        # an empty string
        t4 = r"""\


"""
        # NOTE: leading white space before # line
        t5 = r"""\
# hello
  %start
one\
two\
three
  # ignore this line
%stop
"""
        e_lst = [[u'%start', u'one two three'], [u'%stop']]
        for inp_str, exp_lst in [(t1, e_lst),
                                 (t2, e_lst),
                                 (t3, []),
                                 (t4, []),
                                 (t5, e_lst)]:
            chk_lst = deffile.chunk_string(inp_str)
            # print("CHUNK 02 {}".format(chk_lst))
            self.assertEqual(chk_lst, exp_lst)
        # assert False, "force fail"
