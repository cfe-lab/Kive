"""
Unit tests for Shipyard method models.
"""

import filecmp
import hashlib
import os.path
import re
import shutil
import tempfile
import logging

from django.contrib.auth.models import User
from django.contrib.contenttypes.management import update_all_contenttypes
from django.core.exceptions import ValidationError
from django.core.files import File
from django.core.management import call_command
from django.core.urlresolvers import resolve

from django.test import TestCase, TransactionTestCase
from rest_framework import status
from rest_framework.reverse import reverse
from rest_framework.test import force_authenticate


from constants import datatypes
from kive.tests import BaseTestCases
import librarian.models
from metadata.models import CompoundDatatype, Datatype, everyone_group
import metadata.tests
from method.models import CodeResource, CodeResourceDependency, \
    CodeResourceRevision, Method, MethodFamily
import sandbox.testing_utils as tools
import sandbox.execute


# This was previously defined here but has been moved to metadata.tests.
samplecode_path = metadata.tests.samplecode_path

# For tracking whether we're leaking file descriptors.
fd_count_logger = logging.getLogger("method.tests")


def fd_count(msg):
    fd_count_logger.debug("{}: {}".format(msg, get_open_fds()))


# This is copied from
# http://stackoverflow.com/questions/2023608/check-what-files-are-open-in-python
def get_open_fds():
    """
    Return the number of open file descriptors for the current process.

    Warning: will only work on UNIX-like operating systems.
    """
    import subprocess
    pid = os.getpid()
    procs = subprocess.check_output(
        [ "lsof", '-w', '-Ff', "-p", str( pid ) ] )

    nprocs = len(
        filter(
            lambda s: s and s[ 0 ] == 'f' and s[1: ].isdigit(),
            procs.split( '\n' ) )
        )
    return nprocs


def create_method_test_environment(case):
    """Set up default database state that includes some CRs, CRRs, Methods, etc."""
    # This sets up the DTs and CDTs used in our metadata tests.
    metadata.tests.create_metadata_test_environment(case)

    fd_count("FD count on environment creation")

    # Define comp_cr
    case.comp_cr = CodeResource(
        name="complement",
        description="Complement DNA/RNA nucleotide sequences",
        filename="complement.py",
        user=case.myUser)
    case.comp_cr.save()
    case.comp_cr.grant_everyone_access()

    # Define compv1_crRev for comp_cr
    fn = "complement.py"
    with open(os.path.join(samplecode_path, fn), "rb") as f:
        case.compv1_crRev = CodeResourceRevision(
            coderesource=case.comp_cr,
            revision_name="v1",
            revision_desc="First version",
            content_file=File(f),
            user=case.myUser)
        # case.compv1_crRev.content_file.save(fn, File(f))
        case.compv1_crRev.full_clean()
        case.compv1_crRev.save()
    case.compv1_crRev.grant_everyone_access()

    # Define compv2_crRev for comp_cr
    fn = "complement_v2.py"
    with open(os.path.join(samplecode_path, fn), "rb") as f:
        case.compv2_crRev = CodeResourceRevision(
            coderesource=case.comp_cr,
            revision_name="v2",
            revision_desc="Second version: better docstring",
            revision_parent=case.compv1_crRev,
            content_file=File(f),
            user=case.myUser)
        # case.compv2_crRev.content_file.save(fn, File(f))
        case.compv2_crRev.full_clean()
        case.compv2_crRev.save()
    case.compv2_crRev.grant_everyone_access()

    # The following is for testing code resource dependencies.
    case.test_cr_1 = CodeResource(name="test_cr_1",
                                  filename="test_cr_1.py",
                                  description="CR1",
                                  user=case.myUser)
    case.test_cr_1.save()
    case.test_cr_1.grant_everyone_access()
    case.test_cr_1_rev1 = CodeResourceRevision(coderesource=case.test_cr_1,
                                               revision_name="v1",
                                               revision_desc="CR1-rev1",
                                               user=case.myUser)


    case.test_cr_2 = CodeResource(name="test_cr_2",
                                  filename="test_cr_2.py",
                                  description="CR2",
                                  user=case.myUser)
    case.test_cr_2.save()
    case.test_cr_2.grant_everyone_access()
    case.test_cr_2_rev1 = CodeResourceRevision(coderesource=case.test_cr_2,
                                               revision_name="v1",
                                               revision_desc="CR2-rev1",
                                               user=case.myUser)

    case.test_cr_3 = CodeResource(name="test_cr_3",
                                  filename="test_cr_3.py",
                                  description="CR3",
                                  user=case.myUser)
    case.test_cr_3.save()
    case.test_cr_3.grant_everyone_access()
    case.test_cr_3_rev1 = CodeResourceRevision(coderesource=case.test_cr_3,
                                               revision_name="v1",
                                               revision_desc="CR3-rev1",
                                               user=case.myUser)
    case.test_cr_3_rev1.save()

    case.test_cr_4 = CodeResource(name="test_cr_4",
                                  filename="test_cr_4.py",
                                  description="CR4",
                                  user=case.myUser)
    case.test_cr_4.save()
    case.test_cr_4.grant_everyone_access()
    case.test_cr_4_rev1 = CodeResourceRevision(coderesource=case.test_cr_4,
                                               revision_name="v1",
                                               revision_desc="CR4-rev1",
                                               user=case.myUser)
    case.test_cr_4_rev1.save()

    fn = "test_cr.py"
    with open(os.path.join(samplecode_path, fn), "rb") as f:
        for crr in [case.test_cr_1_rev1, case.test_cr_2_rev1, case.test_cr_3_rev1, case.test_cr_4_rev1]:
            crr.content_file.save(fn, File(f))


    for crr in [case.test_cr_1_rev1, case.test_cr_2_rev1, case.test_cr_3_rev1, case.test_cr_4_rev1]:
        # crr.full_clean()
        crr.save()
        crr.grant_everyone_access()

    # Define DNAcomp_mf
    case.DNAcomp_mf = MethodFamily(
        name="DNAcomplement",
        description="Complement DNA nucleotide sequences.",
        user=case.myUser)
    case.DNAcomp_mf.full_clean()
    case.DNAcomp_mf.save()
    case.DNAcomp_mf.grant_everyone_access()

    # Define DNAcompv1_m (method revision) for DNAcomp_mf with driver compv1_crRev
    case.DNAcompv1_m = case.DNAcomp_mf.members.create(
        revision_name="v1",
        revision_desc="First version",
        driver=case.compv1_crRev,
        user=case.myUser)
    case.DNAcompv1_m.grant_everyone_access()

    # Add input DNAinput_cdt to DNAcompv1_m
    case.DNAinput_ti = case.DNAcompv1_m.create_input(
        compounddatatype = case.DNAinput_cdt,
        dataset_name = "input",
        dataset_idx = 1)
    case.DNAinput_ti.full_clean()
    case.DNAinput_ti.save()

    # Add output DNAoutput_cdt to DNAcompv1_m
    case.DNAoutput_to = case.DNAcompv1_m.create_output(
        compounddatatype = case.DNAoutput_cdt,
        dataset_name = "output",
        dataset_idx = 1)
    case.DNAoutput_to.full_clean()
    case.DNAoutput_to.save()

    # Define DNAcompv2_m for DNAcomp_mf with driver compv2_crRev
    # May 20, 2014: where previously the inputs/outputs would be
    # automatically copied over from the parent using save(), now
    # we explicitly call copy_io_from_parent.
    case.DNAcompv2_m = case.DNAcomp_mf.members.create(
        revision_name="v2",
        revision_desc="Second version",
        revision_parent=case.DNAcompv1_m,
        driver=case.compv2_crRev,
        user=case.myUser)
    case.DNAcompv2_m.full_clean()
    case.DNAcompv2_m.save()
    case.DNAcompv2_m.grant_everyone_access()
    case.DNAcompv2_m.copy_io_from_parent()

    # Define second family, RNAcomp_mf
    case.RNAcomp_mf = MethodFamily(
        name="RNAcomplement",
        description="Complement RNA nucleotide sequences.",
        user=case.myUser)
    case.RNAcomp_mf.full_clean()
    case.RNAcomp_mf.save()
    case.RNAcomp_mf.grant_everyone_access()

    # Define RNAcompv1_m for RNAcomp_mf with driver compv1_crRev
    case.RNAcompv1_m = case.RNAcomp_mf.members.create(
        revision_name="v1",
        revision_desc="First version",
        driver=case.compv1_crRev,
        user=case.myUser)
    case.RNAcompv1_m.grant_everyone_access()

    # Add input RNAinput_cdt to RNAcompv1_m
    case.RNAinput_ti = case.RNAcompv1_m.create_input(
        compounddatatype = case.RNAinput_cdt,
        dataset_name = "input",
        dataset_idx = 1)
    case.RNAinput_ti.full_clean()
    case.RNAinput_ti.save()

    # Add output RNAoutput_cdt to RNAcompv1_m
    case.RNAoutput_to = case.RNAcompv1_m.create_output(
        compounddatatype = case.RNAoutput_cdt,
        dataset_name = "output",
        dataset_idx = 1)
    case.RNAoutput_to.full_clean()
    case.RNAoutput_to.save()

    # Define RNAcompv2_m for RNAcompv1_mf with driver compv2_crRev
    # May 20, 2014: again, we now explicitly copy over the inputs/outputs.
    case.RNAcompv2_m = case.RNAcomp_mf.members.create(
        revision_name="v2",
        revision_desc="Second version",
        revision_parent=case.RNAcompv1_m,
        driver=case.compv2_crRev,
        user=case.myUser)
    case.RNAcompv2_m.full_clean()
    case.RNAcompv2_m.save()
    case.RNAcompv2_m.copy_io_from_parent()
    case.RNAcompv2_m.grant_everyone_access()

    # Create method family for script_1_method / script_2_method / script_3_method
    case.test_mf = MethodFamily(name="Test method family",
                                description="Holds scripts 1/2/3",
                                user=case.myUser)
    case.test_mf.full_clean()
    case.test_mf.save()
    case.test_mf.grant_everyone_access()

    # script_1_sum_and_outputs.py
    # INPUT: 1 csv containing (x,y)
    # OUTPUT: 1 csv containing (x+y,xy)
    case.script_1_cr = CodeResource(name="Sum and product of x and y",
                                    filename="script_1_sum_and_products.py",
                                    description="Addition and multiplication",
                                    user=case.myUser)
    case.script_1_cr.save()
    case.script_1_cr.grant_everyone_access()

    # Add code resource revision for code resource (script_1_sum_and_products )
    case.script_1_crRev = CodeResourceRevision(
        coderesource=case.script_1_cr,
        revision_name="v1",
        revision_desc="First version",
        user=case.myUser
    )
    fn = "script_1_sum_and_products.py"
    with open(os.path.join(samplecode_path, fn), "rb") as f:
        case.script_1_crRev.content_file.save(fn, File(f))
    case.script_1_crRev.save()
    case.script_1_crRev.grant_everyone_access()

    # Establish code resource revision as a method
    case.script_1_method = Method(
        revision_name="script1",
        revision_desc="script1",
        family = case.test_mf,
        driver = case.script_1_crRev,
        user=case.myUser)
    case.script_1_method.save()
    case.script_1_method.grant_everyone_access()

    # Assign tuple as both an input and an output to script_1_method
    case.script_1_method.create_input(compounddatatype = case.tuple_cdt,
                                      dataset_name = "input_tuple",
                                      dataset_idx = 1)
    case.script_1_method.create_output(compounddatatype = case.tuple_cdt,
                                       dataset_name = "input_tuple",
                                       dataset_idx = 1)
    case.script_1_method.full_clean()
    case.script_1_method.save()

    # script_2_square_and_means
    # INPUT: 1 csv containing (a,b,c)
    # OUTPUT-1: 1 csv containing triplet (a^2,b^2,c^2)
    # OUTPUT-2: 1 csv containing singlet mean(a,b,c)
    case.script_2_cr = CodeResource(name="Square and mean of (a,b,c)",
                                    filename="script_2_square_and_means.py",
                                    description="Square and mean - 2 CSVs",
                                    user=case.myUser)
    case.script_2_cr.save()
    case.script_2_cr.grant_everyone_access()

    # Add code resource revision for code resource (script_2_square_and_means)
    fn = "script_2_square_and_means.py"
    case.script_2_crRev = CodeResourceRevision(
        coderesource=case.script_2_cr,
        revision_name="v1",
        revision_desc="First version",
        user=case.myUser)
    with open(os.path.join(samplecode_path, fn), "rb") as f:
        case.script_2_crRev.content_file.save(fn, File(f))
    case.script_2_crRev.save()
    case.script_2_crRev.grant_everyone_access()

    # Establish code resource revision as a method
    case.script_2_method = Method(
        revision_name="script2",
        revision_desc="script2",
        family = case.test_mf,
        driver = case.script_2_crRev,
        user=case.myUser)
    case.script_2_method.save()
    case.script_2_method.grant_everyone_access()

    # Assign triplet as input and output,
    case.script_2_method.create_input(
        compounddatatype = case.triplet_cdt,
        dataset_name = "a_b_c",
        dataset_idx = 1)
    case.script_2_method.create_output(
        compounddatatype = case.triplet_cdt,
        dataset_name = "a_b_c_squared",
        dataset_idx = 1)
    case.script_2_method.create_output(
        compounddatatype = case.singlet_cdt,
        dataset_name = "a_b_c_mean",
        dataset_idx = 2)
    case.script_2_method.full_clean()
    case.script_2_method.save()

    # script_3_product
    # INPUT-1: Single column (k)
    # INPUT-2: Single-row, single column (r)
    # OUTPUT-1: Single column r*(k)
    case.script_3_cr = CodeResource(name="Scalar multiple of k",
                                    filename="script_3_product.py",
                                    description="Product of input",
                                    user=case.myUser)
    case.script_3_cr.save()
    case.script_3_cr.grant_everyone_access()

    # Add code resource revision for code resource (script_3_product)
    with open(os.path.join(samplecode_path, "script_3_product.py"), "rb") as f:
        case.script_3_crRev = CodeResourceRevision(
            coderesource=case.script_3_cr,
            revision_name="v1",
            revision_desc="First version",
            content_file=File(f),
            user=case.myUser)
        case.script_3_crRev.save()
    case.script_3_crRev.grant_everyone_access()

    # Establish code resource revision as a method
    case.script_3_method = Method(
        revision_name="script3",
        revision_desc="script3",
        family = case.test_mf,
        driver = case.script_3_crRev,
        user=case.myUser)
    case.script_3_method.save()
    case.script_3_method.grant_everyone_access()

    # Assign singlet as input and output
    case.script_3_method.create_input(compounddatatype = case.singlet_cdt,
                                      dataset_name = "k",
                                      dataset_idx = 1)

    case.script_3_method.create_input(compounddatatype = case.singlet_cdt,
                                      dataset_name = "r",
                                      dataset_idx = 2,
                                      max_row = 1,
                                      min_row = 1)

    case.script_3_method.create_output(compounddatatype = case.singlet_cdt,
                                       dataset_name = "kr",
                                       dataset_idx = 1)
    case.script_3_method.full_clean()
    case.script_3_method.save()

    ####
    # This next bit was originally in pipeline.tests.

    # DNArecomp_mf is a MethodFamily called DNArecomplement
    case.DNArecomp_mf = MethodFamily(
        name="DNArecomplement",
        description="Re-complement DNA nucleotide sequences.",
        user=case.myUser)
    case.DNArecomp_mf.full_clean()
    case.DNArecomp_mf.save()
    case.DNArecomp_mf.grant_everyone_access()

    # Add to MethodFamily DNArecomp_mf a method revision DNArecomp_m
    case.DNArecomp_m = case.DNArecomp_mf.members.create(
        revision_name="v1",
        revision_desc="First version",
        driver=case.compv2_crRev,
        user=case.myUser)
    case.DNArecomp_m.grant_everyone_access()

    # To this method revision, add inputs with CDT DNAoutput_cdt
    case.DNArecomp_m.create_input(
        compounddatatype = case.DNAoutput_cdt,
        dataset_name = "complemented_seqs",
        dataset_idx = 1)

    # To this method revision, add outputs with CDT DNAinput_cdt
    case.DNArecomp_m.create_output(
        compounddatatype = case.DNAinput_cdt,
        dataset_name = "recomplemented_seqs",
        dataset_idx = 1)

    # Setup used in the "2nd-wave" tests (this was originally in
    # Copperfish_Raw_Setup).

    # Define CR "script_4_raw_in_CSV_out.py"
    # input: raw [but contains (a,b,c) triplet]
    # output: CSV [3 CDT members of the form (a^2, b^2, c^2)]

    # Define CR in order to define CRR
    case.script_4_CR = CodeResource(name="Generate (a^2, b^2, c^2) using RAW input",
        filename="script_4_raw_in_CSV_out.py",
        description="Given (a,b,c), outputs (a^2,b^2,c^2)",
        user=case.myUser)
    case.script_4_CR.save()
    case.script_4_CR.grant_everyone_access()

    # Define CRR for this CR in order to define method
    with open(os.path.join(samplecode_path, "script_4_raw_in_CSV_out.py"), "rb") as f:
        case.script_4_1_CRR = CodeResourceRevision(
            coderesource=case.script_4_CR,
            revision_name="v1",
            revision_desc="v1",
            content_file=File(f),
            user=case.myUser)
        case.script_4_1_CRR.save()
    case.script_4_1_CRR.grant_everyone_access()

    # Define MF in order to define method
    case.test_MF = MethodFamily(
        name="test method family",
        description="method family placeholder",
        user=case.myUser)
    case.test_MF.full_clean()
    case.test_MF.save()
    case.test_MF.grant_everyone_access()

    # Establish CRR as a method within a given method family
    case.script_4_1_M = Method(
        revision_name="s4",
        revision_desc="s4",
        family = case.test_MF,
        driver = case.script_4_1_CRR,
        user=case.myUser)
    case.script_4_1_M.save()
    case.script_4_1_M.grant_everyone_access()

    case.script_4_1_M.create_input(compounddatatype=case.triplet_cdt,
        dataset_name="s4 input", dataset_idx = 1)
    case.script_4_1_M.full_clean()

    # A shorter alias
    case.testmethod = case.script_4_1_M

    # Some code for a no-op method.
    resource = CodeResource(name="noop", filename="noop.sh", user=case.myUser); resource.save()
    resource.grant_everyone_access()
    with tempfile.NamedTemporaryFile() as f:
        f.write("#!/bin/bash\ncat $1")
        case.noop_data_file = f.name
        revision = CodeResourceRevision(coderesource = resource, content_file = File(f),
                                        user=case.myUser)
        revision.clean()
        revision.save()
        revision.grant_everyone_access()

    # Retrieve the string type.
    string_dt = Datatype.objects.get(pk=datatypes.STR_PK)
    string_cdt = CompoundDatatype(user=case.myUser)
    string_cdt.save()
    string_cdt.members.create(datatype=string_dt, column_name="word", column_idx=1)
    string_cdt.grant_everyone_access()
    string_cdt.full_clean()

    mfamily = MethodFamily(name="noop", user=case.myUser); mfamily.save()
    mfamily.grant_everyone_access()
    case.noop_method = Method(
        family=mfamily, driver=revision,
        revision_name = "1", revision_desc = "first version",
        user=case.myUser)
    case.noop_method.save()
    case.noop_method.create_input(compounddatatype=string_cdt, dataset_name = "noop data", dataset_idx=1)
    case.noop_method.grant_everyone_access()
    case.noop_method.full_clean()

    # Some data.
    case.scratch_dir = tempfile.mkdtemp()
    try:
        fd, case.noop_infile = tempfile.mkstemp(dir=case.scratch_dir)
    finally:
        os.close(fd)
    try:
        fd, case.noop_outfile = tempfile.mkstemp(dir=case.scratch_dir)
    finally:
        os.close(fd)
    case.noop_indata = "word\nhello\nworld"

    with open(case.noop_infile, "w") as handle:
        handle.write(case.noop_indata)


def destroy_method_test_environment(case):
    """
    Clean up a TestCase where create_method_test_environment has been called.
    """
    metadata.tests.clean_up_all_files()
    shutil.rmtree(case.scratch_dir)
    CodeResource.objects.all().delete()


class FileAccessTests(TransactionTestCase):
    # fixtures = ["initial_groups", "initial_user", "initial_data"]

    def setUp(self):
        fd_count("FDs (start)")

        # Since these fixtures touch ContentType and Permission, loading them in the
        # 'fixtures' attribute doesn't work.
        # update_all_contenttypes(verbosity=0)
        # call_command("flush", interactive=False)
        # auth_app_config = django_apps.get_app_config("auth")
        # create_permissions(auth_app_config, verbosity=0)
        call_command("loaddata", "initial_groups", verbosity=0)
        call_command("loaddata", "initial_user", verbosity=0)
        call_command("loaddata", "initial_data", verbosity=0)

        # A typical user.
        self.user_randy = User.objects.create_user("Randy", "theotherrford@deco.ca", "hat")
        self.user_randy.save()
        self.user_randy.groups.add(everyone_group())
        self.user_randy.save()

        # Define comp_cr
        self.test_cr = CodeResource(
            name="Test CodeResource",
            description="A test CodeResource to play with file access",
            filename="complement.py",
            user=self.user_randy)
        self.test_cr.save()

        # Define compv1_crRev for comp_cr
        self.fn = "complement.py"

    def tearDown(self):
        metadata.tests.clean_up_all_files()
        fd_count("FDs (end)")
        update_all_contenttypes(verbosity=0)

    def test_close_save(self):
        with open(os.path.join(samplecode_path, self.fn), "rb") as f:
            fd_count("!close->save")

            test_crr = CodeResourceRevision(
                coderesource=self.test_cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f),
                user=self.user_randy)

        self.assertRaises(ValueError, test_crr.save)

    def test_access_close_save(self):
        with open(os.path.join(samplecode_path, self.fn), "rb") as f:
            test_crr = CodeResourceRevision(
                coderesource=self.test_cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f),
                user=self.user_randy)

            fd_count("!access->close->save")
            test_crr.content_file.read()
            fd_count("access-!>close->save")
        fd_count("access->close-!>save")
        self.assertRaises(ValueError, test_crr.save)
        fd_count("access->close->save!")

    def test_close_access_save(self):
        with open(os.path.join(samplecode_path, self.fn), "rb") as f:
            test_crr = CodeResourceRevision(
                coderesource=self.test_cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f),
                user=self.user_randy)

        self.assertRaises(ValueError, test_crr.content_file.read)
        self.assertRaises(ValueError, test_crr.save)

    def test_save_close_access(self):
        with open(os.path.join(samplecode_path, self.fn), "rb") as f:
            test_crr = CodeResourceRevision(
                coderesource=self.test_cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f),
                user=self.user_randy)
            test_crr.save()

        test_crr.content_file.read()
        fd_count("save->close->access")

    def test_save_close_access_close(self):
        with open(os.path.join(samplecode_path, self.fn), "rb") as f:
            fd_count("open-!>File->save->close->access->close")
            test_crr = CodeResourceRevision(
                coderesource=self.test_cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f),
                user=self.user_randy)
            fd_count("open->File-!>save->close->access->close")
            test_crr.save()
            fd_count("open->File->save-!>close->access->close")

        fd_count("open->File->save->close-!>access->close")
        test_crr.content_file.read()
        fd_count("open->File->save->close->access-!>close")
        test_crr.content_file.close()
        fd_count("open->File->save->close->access->close!")

    def test_save_close_clean_close(self):
        with open(os.path.join(samplecode_path, self.fn), "rb") as f:
            fd_count("open-!>File->save->close->clean->close")
            test_crr = CodeResourceRevision(
                coderesource=self.test_cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f),
                user=self.user_randy)
            fd_count("open->File-!>save->close->clean->close")
            test_crr.save()
            fd_count("open->File->save-!>close->clean->close")

        fd_count("open->File->save->close-!>clean->close")
        test_crr.clean()
        fd_count("open->File->save->close->clean-!>close")
        test_crr.content_file.close()
        fd_count("open->File->save->close->clean->close!")

    def test_clean_save_close(self):
        with open(os.path.join(samplecode_path, self.fn), "rb") as f:
            fd_count("open-!>File->clean->save->close")
            test_crr = CodeResourceRevision(
                coderesource=self.test_cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f),
                user=self.user_randy)
            fd_count("open->File-!>clean->save->close")
            test_crr.clean()
            fd_count("open->File->clean-!>save->close")
            test_crr.save()
            fd_count("open->File->clean->save-!>close")
        fd_count("open->File->clean->save->close!")

    def test_clean_save_close_clean_close(self):
        with open(os.path.join(samplecode_path, self.fn), "rb") as f:

            fd_count("open-!>File->clean->save->close->clean->close")
            test_crr = CodeResourceRevision(
                coderesource=self.test_cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f),
                user=self.user_randy)
            fd_count("open->File-!>clean->save->close->clean->close")
            fd_count_logger.debug("FieldFile is open: {}".format(not test_crr.content_file.closed))
            test_crr.clean()
            fd_count("open->File->clean-!>save->close->clean->close")
            fd_count_logger.debug("FieldFile is open: {}".format(not test_crr.content_file.closed))
            test_crr.save()
            fd_count("open->File->clean->save-!>close->clean->close")
            fd_count_logger.debug("FieldFile is open: {}".format(not test_crr.content_file.closed))

        fd_count("open->File->clean->save->close-!>clean->close")
        fd_count_logger.debug("FieldFile is open: {}".format(not test_crr.content_file.closed))
        test_crr.clean()
        fd_count("open->File->clean->save->close->clean-!>close")
        fd_count_logger.debug("FieldFile is open: {}".format(not test_crr.content_file.closed))
        test_crr.content_file.close()
        fd_count("open->File->clean->save->close->clean->close!")
        fd_count_logger.debug("FieldFile is open: {}".format(not test_crr.content_file.closed))


class MethodTestCase(TestCase):
    """
    Set up a database state for unit testing.
    
    This sets up all the stuff used in the Metadata tests, as well as some of the Datatypes
    and CDTs we use here.
    """
    fixtures = ["initial_data", "initial_groups", "initial_user"]

    def setUp(self):
        """Set up default database state for Method unit testing."""
        create_method_test_environment(self)

    def tearDown(self):
        destroy_method_test_environment(self)


class CodeResourceTests(MethodTestCase):
     
    def test_unicode(self):
        """
        unicode should return the codeResource name.
        """
        self.assertEquals(unicode(self.comp_cr), "complement")
  
    def test_valid_name_clean_good(self):
        """
        Clean passes when codeResource name is file-system valid
        """
        valid_cr = CodeResource(name="name", filename="validName", description="desc", user=self.myUser)
        valid_cr.save()
        self.assertIsNone(valid_cr.clean())

    def test_valid_name_with_special_symbols_clean_good(self):
        """
        Clean passes when codeResource name is file-system valid
        """
        valid_cr = CodeResource(name="anotherName", filename="valid.Name with-spaces_and_underscores().py",
                                description="desc", user=self.myUser)
        valid_cr.save()
        self.assertIsNone(valid_cr.clean())

    def test_invalid_name_doubledot_clean_bad(self):
        """
        Clean fails when CodeResource name isn't file-system valid
        """

        invalid_cr = CodeResource(name="test", filename="../test.py", description="desc", user=self.myUser)
        invalid_cr.save()
        self.assertRaisesRegexp(ValidationError, "Invalid code resource filename", invalid_cr.clean_fields)

    def test_invalid_name_starting_space_clean_bad(self):
        """  
        Clean fails when CodeResource name isn't file-system valid
        """
        invalid_cr = CodeResource(name="test", filename=" test.py", description="desc", user=self.myUser)
        invalid_cr.save()
        self.assertRaisesRegexp(ValidationError, "Invalid code resource filename", invalid_cr.clean_fields)

    def test_invalid_name_invalid_symbol_clean_bad(self):
        """  
        Clean fails when CodeResource name isn't file-system valid
        """
        invalid_cr = CodeResource(name="name", filename="test$.py", description="desc", user=self.myUser)
        invalid_cr.save()
        self.assertRaisesRegexp(ValidationError, "Invalid code resource filename", invalid_cr.clean_fields)

    def test_invalid_name_trailing_space_clean_bad(self):
        """  
        Clean fails when CodeResource name isn't file-system valid
        """
        invalid_cr = CodeResource(name="name", filename="test.py ", description="desc", user=self.myUser)
        invalid_cr.save()
        self.assertRaisesRegexp(ValidationError, "Invalid code resource filename", invalid_cr.clean_fields)


class CodeResourceRevisionTests(MethodTestCase):

    def test_unicode(self):
        """
        CodeResourceRevision.unicode() should return it's code resource
        revision name.

        Or, if no CodeResource has been linked, should display a placeholder.
        """
        # Valid crRev should return it's cr.name and crRev.revision_name
        self.assertEquals(unicode(self.compv1_crRev), "complement:1 (v1)")

        # Define a crRev without a linking cr, or a revision_name
        no_cr_set = CodeResourceRevision()
        self.assertEquals(unicode(no_cr_set), "[no revision name]")

        # Define a crRev without a linking cr, with a revision_name of foo
        no_cr_set.revision_name = "foo"
        self.assertEquals(unicode(no_cr_set), "foo")

    # Tests of has_circular_dependence and clean
    def test_has_circular_dependence_nodep(self):
        """A CRR with no dependencies should not have any circular dependence."""
        self.assertEquals(self.test_cr_1_rev1.has_circular_dependence(),
                          False)
        self.assertEquals(self.test_cr_1_rev1.clean(), None)
        self.test_cr_1_rev1.content_file.close()

    def test_has_circular_dependence_single_self_direct_dep(self):
        """A CRR has itself as its lone dependency."""
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_1_rev1,
            depPath=".",
            depFileName="foo")
        self.assertEquals(self.test_cr_1_rev1.has_circular_dependence(), True)
        self.assertRaisesRegexp(ValidationError,
                                "Self-referential dependency",
                                self.test_cr_1_rev1.clean)
        self.test_cr_1_rev1.content_file.close()

    def test_has_circular_dependence_single_other_direct_dep(self):
        """A CRR has a lone dependency (non-self)."""
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath=".",
            depFileName="foo")
        self.assertEquals(self.test_cr_1_rev1.has_circular_dependence(),
                          False)
        self.assertEquals(self.test_cr_1_rev1.clean(), None)
        self.test_cr_1_rev1.content_file.close()

    def test_has_circular_dependence_several_direct_dep_noself(self):
        """A CRR with several direct dependencies (none are itself)."""
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath=".",
            depFileName="foo")
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath=".")
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_4_rev1,
            depPath=".")
        self.assertEquals(self.test_cr_1_rev1.has_circular_dependence(),
                          False)
        self.assertEquals(self.test_cr_1_rev1.clean(), None)
        self.test_cr_1_rev1.content_file.close()

    def test_has_circular_dependence_several_direct_dep_self_1(self):
        """A CRR with several dependencies has itself as the first dependency."""
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_1_rev1,
            depPath=".",
            depFileName="foo")
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath=".")
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath=".")
        self.assertEquals(self.test_cr_1_rev1.has_circular_dependence(),
                          True)

        self.assertRaisesRegexp(ValidationError,
                                "Self-referential dependency",
                                self.test_cr_1_rev1.clean)
        self.test_cr_1_rev1.content_file.close()
        
    def test_has_circular_dependence_several_direct_dep_self_2(self):
        """A CRR with several dependencies has itself as the second dependency."""
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath=".")
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_1_rev1,
            depPath=".",
            depFileName="foo")
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath=".")
        self.assertEquals(self.test_cr_1_rev1.has_circular_dependence(),
                          True)
        self.assertRaisesRegexp(ValidationError,
                                "Self-referential dependency",
                                self.test_cr_1_rev1.clean)
        
    def test_has_circular_dependence_several_direct_dep_self_3(self):
        """A CRR with several dependencies has itself as the last dependency."""
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath=".")
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath=".")
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_1_rev1,
            depPath=".",
            depFileName="foo")
        self.assertEquals(self.test_cr_1_rev1.has_circular_dependence(),
                          True)
        self.assertRaisesRegexp(ValidationError,
                                "Self-referential dependency",
                                self.test_cr_1_rev1.clean)

    def test_has_circular_dependence_several_nested_dep_noself(self):
        """A CRR with several dependencies including a nested one."""
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath=".")
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath=".")
        self.test_cr_3_rev1.dependencies.create(
            requirement=self.test_cr_4_rev1,
            depPath=".")
        self.assertEquals(self.test_cr_1_rev1.has_circular_dependence(),
                          False)
        self.assertEquals(self.test_cr_1_rev1.clean(), None)
        
    def test_has_circular_dependence_several_nested_dep_selfnested(self):
        """A CRR with several dependencies including itself as a nested one."""
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath=".")
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath=".")
        self.test_cr_3_rev1.dependencies.create(
            requirement=self.test_cr_1_rev1,
            depPath=".")
        self.assertEquals(self.test_cr_1_rev1.has_circular_dependence(),
                          True)
        self.assertEquals(self.test_cr_2_rev1.has_circular_dependence(),
                          False)
        # Note that test_cr_3_rev1 *is* circular, as it depends on 1 and
        # 1 has a circular dependence.
        self.assertEquals(self.test_cr_3_rev1.has_circular_dependence(),
                          True)
        self.assertRaisesRegexp(ValidationError,
                                "Self-referential dependency",
                                self.test_cr_1_rev1.clean)
        
    def test_has_circular_dependence_nested_dep_has_circ(self):
        """A nested dependency is circular."""
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath=".")
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath=".")
        self.test_cr_2_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath=".")
        self.assertEquals(self.test_cr_1_rev1.has_circular_dependence(),
                          True)
        self.assertRaisesRegexp(ValidationError,
                                "Self-referential dependency",
                                self.test_cr_1_rev1.clean)
        self.assertEquals(self.test_cr_2_rev1.has_circular_dependence(),
                          True)
        self.assertRaisesRegexp(ValidationError,
                                "Self-referential dependency",
                                self.test_cr_2_rev1.clean)
        
    def test_metapackage_cannot_have_file_bad_clean(self):
        """
        A CRR with a content file should have a filename associated with
        its parent CodeResource.
        """
        cr = CodeResource(
            name="test_complement",
            filename="",
            description="Complement DNA/RNA nucleotide sequences",
            user=self.myUser)
        cr.save()

        # So it's revision does not have a content_file
        with open(os.path.join(samplecode_path, "complement.py"), "rb") as f:
            cr_rev_v1 = CodeResourceRevision(
                coderesource=cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f),
                user=self.myUser)

        self.assertRaisesRegexp(
            ValidationError,
            "If content file exists, it must have a file name",
            cr_rev_v1.clean)

    def test_non_metapackage_must_have_file_bad_clean(self):
        """
        A CRR with no content file should not have a filename associated with
        its parent CodeResource.
        """
        cr = CodeResource(
            name="nonmetapackage",
            filename="foo",
            description="Associated CRRs should have a content file",
            user=self.myUser)
        cr.save()

        # Create a revision without a content_file.
        cr_rev_v1 = CodeResourceRevision(
            coderesource=cr,
            revision_name="v1",
            revision_desc="Has no content file!",
            user=self.myUser)

        self.assertRaisesRegexp(
            ValidationError,
            "Cannot have a filename specified in the absence of a content file",
            cr_rev_v1.clean)

    def test_clean_blank_MD5_on_codeResourceRevision_without_file(self):
        """
        If no file is specified, MD5 should be empty string.
        """
        cr = CodeResource(name="foo",
                          filename="",
                          description="Some metapackage",
                          user=self.myUser)
        cr.save()
        
        # Create crRev with a codeResource but no file contents
        no_file_crRev = CodeResourceRevision(
            coderesource=cr,
            revision_name="foo",
            revision_desc="foo",
            user=self.myUser)
  
        no_file_crRev.clean()

        # After clean(), MD5 checksum should be the empty string
        self.assertEquals(no_file_crRev.MD5_checksum, "")

    def test_clean_valid_MD5_on_codeResourceRevision_with_file(self):
        """
        If file contents are associated with a crRev, an MD5 should exist.
        """

        # Compute the reference MD5
        md5gen = hashlib.md5()
        with open(os.path.join(samplecode_path, "complement.py"), "rb") as f:
            md5gen.update(f.read())

        # Revision should have the correct MD5 checksum
        self.assertEquals(md5gen.hexdigest(), self.comp_cr.revisions.get(revision_name="v1").MD5_checksum)

    def test_dependency_depends_on_nothing_clean_good (self):
        self.assertEqual(self.test_cr_1_rev1.clean(), None)

    def test_dependency_current_folder_same_name_clean_bad(self):
        """
        A depends on B - current folder, same name
        """

        # test_cr_1_rev1 is needed by test_cr_2_rev1
        # It will have the same file name as test_cr_1
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName=self.test_cr_1.filename)

        self.assertRaisesRegexp(ValidationError,
                                "Conflicting dependencies",
                                self.test_cr_1_rev1.clean)

    def test_dependency_current_folder_different_name_clean_good(self):
        """
        1 depends on 2 - current folder, different name
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName="differentName.py")

        self.assertEqual(self.test_cr_1_rev1.clean(), None)

    def test_dependency_inner_folder_same_name_clean_good(self):
        """
        1 depends on 2 - different folder, same name
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="innerFolder/",
            depFileName=self.test_cr_1.filename)

        self.assertEqual(self.test_cr_1_rev1.clean(), None)

    def test_dependency_inner_folder_different_name_clean_good(self):
        """
        1 depends on 2 - different folder, different name
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="innerFolder/",
            depFileName="differentName.py")

        self.assertEqual(self.test_cr_1_rev1.clean(), None)

    def test_dependency_A_depends_BC_same_folder_no_conflicts_clean_good(self):
        """
        A depends on B, A depends on C
        BC in same folder as A
        Nothing conflicts
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName="name1.py")

        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="name2.py")

        self.assertEqual(self.test_cr_1_rev1.clean(), None)

    def test_dependency_A_depends_BC_same_folder_B_conflicts_with_A_clean_bad(self):
        """
        A depends on B, A depends on C
        BC in same folder as A, B conflicts with A
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName="name1.py")

        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName=self.test_cr_1.filename)

        self.assertRaisesRegexp(
            ValidationError,
            "Conflicting dependencies",
            self.test_cr_1_rev1.clean)

    def test_dependency_A_depends_BC_same_folder_C_conflicts_with_A_clean_bad(self):
        """
        A depends on B, A depends on C
        BC in same folder as A, C conflicts with A
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName=self.test_cr_1.filename)

        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="notConflicting.py")

        self.assertRaisesRegexp(
            ValidationError,
            "Conflicting dependencies",
            self.test_cr_1_rev1.clean)

    def test_dependency_A_depends_BC_same_folder_B_conflicts_with_C_clean_bad(self):
        """
        A depends on B, A depends on C
        BC in same folder as A, BC conflict
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName="colliding_name.py")

        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="colliding_name.py")

        self.assertRaisesRegexp(
            ValidationError,
            "Conflicting dependencies",
            self.test_cr_1_rev1.clean)

    def test_dependency_A_depends_BC_B_in_same_folder_no_conflicts_clean_good(self):
        """
        BC in same folder as A, B conflicts with A
        B in same folder, C in different folder, nothing conflicts
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName="no_collision.py")

        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="diffFolder",
            depFileName="differentName.py")

        self.assertEqual(self.test_cr_1_rev1.clean(), None)

    def test_dependency_A_depends_BC_B_in_same_folder_B_conflicts_A_clean_bad(self):
        """
        A depends on B, A depends on C
        B in same folder, C in different folder, B conflicts with A
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName=self.test_cr_1.filename)

        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="diffFolder",
            depFileName="differentName.py")

        self.assertRaisesRegexp(
            ValidationError,
            "Conflicting dependencies",
            self.test_cr_1_rev1.clean)

    def test_dependency_A_depends_BC_C_in_same_folder_no_conflict_clean_good(self):
        """
        A depends on B, A depends on C
        B in different folder, C in same folder, nothing conflicts
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="diffFolder",
            depFileName=self.test_cr_1.filename)

        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="differentName.py")

        self.assertEqual(self.test_cr_1_rev1.clean(), None)

    def test_dependency_A_depends_BC_C_in_same_folder_C_conflicts_with_A_clean_bad(self):
        """
        A depends on B, A depends on C
        B in different folder, C in same folder, C conflicts with A
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="diffFolder",
            depFileName=self.test_cr_1.filename)

        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName=self.test_cr_1.filename)

        self.assertRaisesRegexp(
            ValidationError,
            "Conflicting dependencies",
            self.test_cr_1_rev1.clean)

    def test_dependency_A_depends_B_B_depends_C_all_same_folder_no_conflict_clean_good(self):
        """
        A depends on B, B depends on C
        ABC in same folder - no conflicts
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName="differentName.py")

        self.test_cr_2_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="differetName2.py")

        self.assertEqual(self.test_cr_1_rev1.clean(), None)

    def test_dependency_A_depends_B_B_depends_C_all_same_folder_A_conflicts_C_clean_bad(self):
        """
        A depends on B, B depends on C
        ABC in same folder - A conflicts with C
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName="differentName.py")

        self.test_cr_2_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName=self.test_cr_1.filename)

        self.assertRaisesRegexp(
            ValidationError,
            "Conflicting dependencies",
            self.test_cr_1_rev1.clean)

    def test_dependency_A_depends_B_B_depends_C_all_same_folder_B_conflicts_C_clean_bad(self):
        """
        A depends on B, B depends on C
        ABC in same folder - B conflicts with C
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName=self.test_cr_1.filename)

        self.test_cr_2_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="differentName.py")

        self.assertRaisesRegexp(
            ValidationError,
            "Conflicting dependencies",
            self.test_cr_1_rev1.clean)

    def test_dependency_A_depends_B_B_depends_C_BC_is_nested_no_conflicts_clean_good(self):
        """
        A depends on B, B depends on C
        BC in nested folder - no conflicts
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="nestedFolder",
            depFileName=self.test_cr_1.name)

        self.test_cr_2_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="differentName.py")

        self.assertEqual(self.test_cr_1_rev1.clean(), None)

    def test_dependency_A_depends_B_B_depends_C_BC_is_nested_B_conflicts_C_clean_bad(self):
        """
        A depends on B, B depends on C
        BC in nested folder - B conflicts with C
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="nestedFolder",
            depFileName="conflicting.py")

        self.test_cr_2_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="conflicting.py")

        self.assertRaisesRegexp(
            ValidationError,
            "Conflicting dependencies",
            self.test_cr_1_rev1.clean)

    def test_dependency_A_depends_B_B_depends_C_double_nested_clean_good(self):
        """
        A depends on B, B depends on C
        B in nested folder, C in double nested folder - no conflicts
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="nestedFolder",
            depFileName="conflicting.py")

        self.test_cr_2_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="nestedFolder",
            depFileName="conflicting.py")

        self.assertEqual(self.test_cr_1_rev1.clean(), None)

    def test_dependency_A_depends_B1B2B3_B1_depends_C_all_same_folder_no_conflicts_clean_good(self):
        """
        A depends on B1/B2/B3, B1 depends on C
        A/B1B2B3/C in same folder - no conflicts
        """
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName="1.py")

        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="2.py")

        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="3.py")

        self.test_cr_2_rev1.dependencies.create(
            requirement=self.test_cr_4_rev1,
            depPath="",
            depFileName="4.py")

        self.assertEqual(self.test_cr_1_rev1.clean(), None)

    def test_dependency_A_depends_B1B2B3_B2_depends_C_B1B2B3C_in_nested_B3_conflicts_C_clean_bad(self):
        """
        A depends on B1/B2/B3, B2 depends on C
        B1B2B3C in nested folder - B3 conflicts with C
        """

        # A depends on B1
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="nested",
            depFileName="1.py")

        # A depends on B2
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="nested",
            depFileName="2.py")

        # A depends on B3***
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="nested",
            depFileName="conflict.py")

        # B2 depends on C
        self.test_cr_3_rev1.dependencies.create(
            requirement=self.test_cr_4_rev1,
            depPath="",
            depFileName="conflict.py")

        self.assertRaisesRegexp(
            ValidationError,
            "Conflicting dependencies",
            self.test_cr_1_rev1.clean)

    def test_dependency_A_depends_B1B2B3_B3_depends_C_B2B3C_in_nested_B2_conflicts_B3_clean_bad(self):
        """
        A depends on B1/B2/B3, B3 depends on C
        B2B3 in nested folder - B2 conflicts with B3
        """

        # A depends on B1
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName="1.py")

        # A depends on B2
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="nested",
            depFileName="conflict.py")

        # A depends on B3
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="nested",
            depFileName="conflict.py")

        # B3 depends on C
        self.test_cr_3_rev1.dependencies.create(
            requirement=self.test_cr_4_rev1,
            depPath="",
            depFileName="4.py")

        self.assertRaisesRegexp(
            ValidationError,
            "Conflicting dependencies",
            self.test_cr_1_rev1.clean)

    def test_dependency_list_all_filepaths_recursive_case_1 (self):
        """
        Ensure list_all_filepaths generates the correct list
        A depends on B1/B2, B1 depends on C
        B1 is nested, B2 is not nested, C is nested wrt B1
        """

        # A depends on B1 (Which is nested)
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="B1_nested",
            depFileName="B1.py")

        # A depends on B2 (Which is not nested)
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="B2.py")

        # B1 depends on C (Nested wrt B1)
        self.test_cr_2_rev1.dependencies.create(
            requirement=self.test_cr_4_rev1,
            depPath="C_nested",
            depFileName="C.py")

        self.assertSetEqual(
            set(self.test_cr_1_rev1.list_all_filepaths()),
            {u'test_cr_1.py', u'B1_nested/B1.py', u'B1_nested/C_nested/C.py', u'B2.py'}
        )

    def test_dependency_list_all_filepaths_recursive_case_2 (self):
        """
        Ensure list_all_filepaths generates the correct list
        A depends on B1/B2, B2 depends on C
        B1 is nested, B2 is not nested, C is nested wrt B2
        """
        # A depends on B1 (Which is nested)
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="B1_nested",
            depFileName="B1.py")

        # A depends on B2 (Which is not nested)
        self.test_cr_1_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="",
            depFileName="B2.py")

        # B2 depends on C (Nested wrt B2)
        self.test_cr_3_rev1.dependencies.create(
            requirement=self.test_cr_4_rev1,
            depPath="C_nested",
            depFileName="C.py")

        self.assertSetEqual(
            set(self.test_cr_1_rev1.list_all_filepaths()),
            {u'test_cr_1.py', u'B1_nested/B1.py', u'B2.py', u'C_nested/C.py'}
        )

    def test_dependency_list_all_filepaths_with_metapackage(self):

        # Define a code with a blank filename (metapackage)
        # Give it dependencies
        # Give one more dependency a nested dependency

        # The following is for testing code resource dependencies
        test_cr_6 = CodeResource(name="test_cr_6",
                                 filename="",
                                 description="CR6",
                                 user=self.myUser)
        test_cr_6.save()

        # The revision has no content_file because it's a metapackage
        test_cr_6_rev1 = CodeResourceRevision(coderesource=test_cr_6,
                                              revision_name="v1_metapackage",
                                              revision_desc="CR6-rev1",
                                              user=self.myUser)
        test_cr_6_rev1.save()

        # Current-folder dependencies
        test_cr_6_rev1.dependencies.create(
            requirement=self.test_cr_2_rev1,
            depPath="",
            depFileName="B.py")

        # Sub-folder dependencies
        test_cr_6_rev1.dependencies.create(
            requirement=self.test_cr_3_rev1,
            depPath="nestedFolder",
            depFileName="C.py")

        # Nested dependencies
        self.test_cr_3_rev1.dependencies.create(
            requirement=self.test_cr_4_rev1,
            depPath="deeperNestedFolder",
            depFileName="D.py")

        self.assertSetEqual(
            set(test_cr_6_rev1.list_all_filepaths()),
            {u'B.py', u'nestedFolder/C.py', u'nestedFolder/deeperNestedFolder/D.py'}
        )

        # FIXME
        # test_cr_6_rev1.content_file.delete()
        # test_cr_6_rev1.delete()

    def test_dependency_list_all_filepaths_single_unnested_dep_blank_depFileName(self):
        """List all filepaths when dependency has no depFileName set and is not nested.
        """
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_2_rev1,
                depPath="")
        self.assertEqual(self.test_cr_1_rev1.list_all_filepaths(),
                         [u'test_cr_1.py', u'test_cr_2.py'])

    def test_dependency_list_all_filepaths_single_nested_dep_blank_depFileName(self):
        """List all filepaths when dependency has no depFileName set and is nested.
        """
        self.test_cr_1_rev1.dependencies.create(
                requirement=self.test_cr_2_rev1,
                depPath="nest_folder")
        self.assertEqual(self.test_cr_1_rev1.list_all_filepaths(),
                         [u'test_cr_1.py', u'nest_folder/test_cr_2.py'])


class CodeResourceDependencyTests(MethodTestCase):

    def test_unicode(self):
        """
        Unicode of CodeResourceDependency should return:
        <self.crRev> requires <referenced crRev> as <filePath>
        """

        # v1 is a revision of comp_cr such that revision_name = v1
        v1 = self.comp_cr.revisions.get(revision_name="v1")
        v2 = self.comp_cr.revisions.get(revision_name="v2")

        # Define a fake dependency where v1 requires v2 in subdir/foo.py
        test_crd = CodeResourceDependency(coderesourcerevision=v1,
                                          requirement=v2,
                                          depPath="subdir",
                                          depFileName="foo.py")

        # Display unicode for this dependency under valid conditions
        self.assertEquals(
            unicode(test_crd),
            "complement complement:1 (v1) requires complement complement:2 (v2) as subdir/foo.py")

    def test_invalid_dotdot_path_clean(self):
        """
        Dependency tries to go into a path outside its sandbox.
        """
        v1 = self.comp_cr.revisions.get(revision_name="v1")
        v2 = self.comp_cr.revisions.get(revision_name="v2")

        bad_crd = CodeResourceDependency(coderesourcerevision=v1,
                                         requirement=v2,
                                         depPath="..",
                                         depFileName="foo.py")
        self.assertRaisesRegexp(
            ValidationError,
            "depPath cannot reference \.\./",
            bad_crd.clean)

        bad_crd_2 = CodeResourceDependency(coderesourcerevision=v1,
                                           requirement=v2,
                                           depPath="../test",
                                           depFileName="foo.py")
        self.assertRaisesRegexp(
            ValidationError,
            "depPath cannot reference \.\./",
            bad_crd_2.clean)
        
    def test_valid_path_with_dotdot_clean(self):
        """
        Dependency goes into a path with a directory containing ".." in the name.
        """
        v1 = self.comp_cr.revisions.get(revision_name="v1")
        v2 = self.comp_cr.revisions.get(revision_name="v2")

        good_crd = CodeResourceDependency(coderesourcerevision=v1,
                                          requirement=v2,
                                          depPath="..bar",
                                          depFileName="foo.py")
        self.assertEquals(good_crd.clean(), None)
        
        good_crd_2 = CodeResourceDependency(coderesourcerevision=v1,
                                            requirement=v2,
                                            depPath="bar..",
                                            depFileName="foo.py")
        self.assertEquals(good_crd_2.clean(), None)

        good_crd_3 = CodeResourceDependency(coderesourcerevision=v1,
                                            requirement=v2,
                                            depPath="baz/bar..",
                                            depFileName="foo.py")
        self.assertEquals(good_crd_3.clean(), None)

        good_crd_4 = CodeResourceDependency(coderesourcerevision=v1,
                                            requirement=v2,
                                            depPath="baz/..bar",
                                            depFileName="foo.py")
        self.assertEquals(good_crd_4.clean(), None)

        good_crd_5 = CodeResourceDependency(coderesourcerevision=v1,
                                            requirement=v2,
                                            depPath="baz/..bar..",
                                            depFileName="foo.py")
        self.assertEquals(good_crd_5.clean(), None)

        good_crd_6 = CodeResourceDependency(coderesourcerevision=v1,
                                            requirement=v2,
                                            depPath="..baz/bar..",
                                            depFileName="foo.py")
        self.assertEquals(good_crd_6.clean(), None)

        # This case works because the ".." doesn't take us out of the sandbox
        good_crd_7 = CodeResourceDependency(coderesourcerevision=v1,
                                            requirement=v2,
                                            depPath="baz/../bar",
                                            depFileName="foo.py")
        self.assertEquals(good_crd_7.clean(), None)

        good_crd_8 = CodeResourceDependency(coderesourcerevision=v1,
                                            requirement=v2,
                                            depPath="baz/..bar../blah",
                                            depFileName="foo.py")
        self.assertEquals(good_crd_8.clean(), None)
        
    def test_cr_with_filename_dependency_with_good_path_and_filename_clean(self):
        """
        Check
        """
        # cr_no_filename has name="complement" and filename="complement.py"
        cr = CodeResource(
                name="testing_complement",
                filename="complement.py",
                description="Complement DNA/RNA nucleotide sequences",
                user=self.myUser)
        cr.save()

        # Define cr_rev_v1 for cr
        with open(os.path.join(samplecode_path, "complement.py"), "rb") as f:
            cr_rev_v1 = CodeResourceRevision(
                    coderesource=cr,
                    revision_name="v1",
                    revision_desc="First version",
                    content_file=File(f),
                    user=self.myUser)
            cr_rev_v1.full_clean()
            cr_rev_v1.save()

        # Define cr_rev_v2 for cr
        with open(os.path.join(samplecode_path, "complement.py"), "rb") as f:
            cr_rev_v2 = CodeResourceRevision(
                    coderesource=cr,
                    revision_name="v2",
                    revision_desc="Second version",
                    content_file=File(f),
                    user=self.myUser)
            cr_rev_v2.full_clean()
            cr_rev_v2.save()

        # Define a code resource dependency for cr_rev_v1 with good paths and filenames
        good_crd = CodeResourceDependency(coderesourcerevision=cr_rev_v1,
                                          requirement=cr_rev_v2,
                                          depPath="testFolder/anotherFolder",
                                          depFileName="foo.py")

        self.assertEqual(good_crd.clean(), None)
        
    def test_metapackage_cannot_have_file_names_bad_clean(self):

        # Define a standard code resource
        cr = CodeResource(
                name="test_complement",
                filename="test.py",
                description="Complement DNA/RNA nucleotide sequences",
                user=self.myUser)
        cr.save()

        # Give it a file
        with open(os.path.join(samplecode_path, "complement.py"), "rb") as f:
            cr_rev_v1 = CodeResourceRevision(
                coderesource=cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f),
                user=self.myUser)
            cr_rev_v1.full_clean()
            cr_rev_v1.save()
        
        # Define a metapackage code resource (no file name)
        cr_meta = CodeResource(
                name="test2_complement",
                filename="",
                description="Complement DNA/RNA nucleotide sequences",
                user=self.myUser)
        cr_meta.save()

        # Do not give it a file
        cr_meta_rev_v1 = CodeResourceRevision(
            coderesource=cr_meta,
            revision_name="v1",
            revision_desc="First version",
            user=self.myUser)
        cr_meta_rev_v1.full_clean()
        cr_meta_rev_v1.save()

        # Add metapackage as a dependency to cr_rev_v1, but invalidly give it a depFileName
        bad_crd = CodeResourceDependency(coderesourcerevision=cr_rev_v1,
                                         requirement=cr_meta_rev_v1,
                                         depPath="testFolder/anotherFolder",
                                         depFileName="foo.py")

        self.assertRaisesRegexp(
            ValidationError,
            "Metapackage dependencies cannot have a depFileName",
            bad_crd.clean)

    def test_metapackage_good_clean(self):

        # Define a standard code resource
        cr = CodeResource(
                name="test_complement",
                filename="test.py",
                description="Complement DNA/RNA nucleotide sequences",
                user=self.myUser)
        cr.save()

        # Give it a file
        with open(os.path.join(samplecode_path, "complement.py"), "rb") as f:
            cr_rev_v1 = CodeResourceRevision(
                coderesource=cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f),
                user=self.myUser)
            cr_rev_v1.full_clean()
            cr_rev_v1.save()
        
        # Define a metapackage code resource (no file name)
        cr_meta = CodeResource(
                name="test2_complement",
                filename="",
                description="Complement DNA/RNA nucleotide sequences",
                user=self.myUser)
        cr_meta.save()

        # Do not give it a file
        cr_meta_rev_v1 = CodeResourceRevision(
            coderesource=cr_meta,
            revision_name="v1",
            revision_desc="First version",
            user=self.myUser)
        cr_meta_rev_v1.full_clean()
        cr_meta_rev_v1.save()

        # Add metapackage as a dependency to cr_rev_v1
        good_crd = CodeResourceDependency(coderesourcerevision=cr_rev_v1,
                                         requirement=cr_meta_rev_v1,
                                         depPath="testFolder/anotherFolder",
                                         depFileName="")

        self.assertEqual(good_crd.clean(), None)


class CodeResourceRevisionInstallTests(MethodTestCase):
    """Tests of the install function of CodeResourceRevision."""
    def test_base_case(self):
        """
        Test of base case -- installing a CRR with no dependencies.
        """
        test_path = tempfile.mkdtemp(prefix="test_base_case")

        self.compv1_crRev.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))

        shutil.rmtree(test_path)

    def test_second_revision(self):
        """
        Test of base case -- installing a CRR that is a second revision.
        """
        test_path = tempfile.mkdtemp(prefix="test_base_case")

        self.compv2_crRev.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))

        shutil.rmtree(test_path)

    def test_dependency_same_dir_dot(self):
        """
        Test of installing a CRR with a dependency in the same directory, specified using a dot.
        """
        test_path = tempfile.mkdtemp(prefix="test_dependency_same_dir_dot")

        self.compv1_crRev.dependencies.create(requirement=self.test_cr_1_rev1, depPath=".")
        self.compv1_crRev.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "test_cr_1.py")))

        shutil.rmtree(test_path)

    def test_dependency_same_dir_blank(self):
        """
        Test of installing a CRR with a dependency in the same directory, specified using a blank.
        """
        test_path = tempfile.mkdtemp(prefix="test_dependency_same_dir_blank")

        self.compv1_crRev.dependencies.create(requirement=self.test_cr_1_rev1, depPath="")
        self.compv1_crRev.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "test_cr_1.py")))

        shutil.rmtree(test_path)

    def test_dependency_override_dep_filename(self):
        """
        Test of installing a CRR with a dependency whose filename is overridden.
        """
        test_path = tempfile.mkdtemp(prefix="test_dependency_override_dep_filename")

        self.compv1_crRev.dependencies.create(requirement=self.test_cr_1_rev1, depPath="",
                                              depFileName="foo.py")
        self.compv1_crRev.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "foo.py")))
        self.assertFalse(os.path.exists(os.path.join(test_path, "test_cr_1.py")))

        shutil.rmtree(test_path)

    def test_dependency_in_subdirectory(self):
        """
        Test of installing a CRR with a dependency in a subdirectory.
        """
        test_path = tempfile.mkdtemp(prefix="test_dependency_in_subdirectory")

        self.compv1_crRev.dependencies.create(requirement=self.test_cr_1_rev1, depPath="modules")
        self.compv1_crRev.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))
        self.assertTrue(os.path.isdir(os.path.join(test_path, "modules")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "modules", "test_cr_1.py")))

        shutil.rmtree(test_path)

    def test_dependencies_in_same_subdirectory(self):
        """
        Test of installing a CRR with several dependencies in the same subdirectory.
        """
        test_path = tempfile.mkdtemp(prefix="test_dependencies_in_same_subdirectory")

        self.compv1_crRev.dependencies.create(requirement=self.test_cr_1_rev1, depPath="modules")
        self.compv1_crRev.dependencies.create(requirement=self.test_cr_2_rev1, depPath="modules")
        self.compv1_crRev.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))
        self.assertTrue(os.path.isdir(os.path.join(test_path, "modules")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "modules", "test_cr_1.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "modules", "test_cr_2.py")))

        shutil.rmtree(test_path)

    def test_dependencies_in_same_directory(self):
        """
        Test of installing a CRR with several dependencies in the base directory.
        """
        test_path = tempfile.mkdtemp(prefix="test_dependencies_in_same_directory")

        self.compv1_crRev.dependencies.create(requirement=self.test_cr_1_rev1, depPath="")
        self.compv1_crRev.dependencies.create(requirement=self.test_cr_2_rev1, depPath="")
        self.compv1_crRev.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "test_cr_1.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "test_cr_2.py")))

        shutil.rmtree(test_path)

    def test_dependencies_in_subsub_directory(self):
        """
        Test of installing a CRR with dependencies in sub-sub-directories.
        """
        test_path = tempfile.mkdtemp(prefix="test_dependencies_in_subsub_directory")

        self.compv1_crRev.dependencies.create(requirement=self.test_cr_1_rev1, depPath="modules/foo1")
        self.compv1_crRev.dependencies.create(requirement=self.test_cr_2_rev1, depPath="modules/foo2")
        self.compv1_crRev.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))
        self.assertTrue(os.path.isdir(os.path.join(test_path, "modules/foo1")))
        self.assertTrue(os.path.isdir(os.path.join(test_path, "modules/foo2")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "modules", "foo1", "test_cr_1.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "modules", "foo2", "test_cr_2.py")))

        shutil.rmtree(test_path)

    def test_dependencies_from_same_coderesource_same_dir(self):
        """
        Test of installing a CRR with a dependency having the same CodeResource in the same directory.
        """
        test_path = tempfile.mkdtemp(prefix="test_dependencies_from_same_coderesource_same_dir")

        self.compv1_crRev.dependencies.create(requirement=self.compv2_crRev, depPath="", depFileName="foo.py")
        self.compv1_crRev.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "foo.py")))
        # Test that the right files are in the right places.
        self.assertTrue(
            filecmp.cmp(os.path.join(samplecode_path, "complement.py"),
                        os.path.join(test_path, "complement.py"))
        )
        self.assertTrue(
            filecmp.cmp(os.path.join(samplecode_path, "complement_v2.py"),
                        os.path.join(test_path, "foo.py"))
        )

        shutil.rmtree(test_path)

    def test_dependencies_in_various_places(self):
        """
        Test of installing a CRR with dependencies in several places.
        """
        test_path = tempfile.mkdtemp(prefix="test_dependencies_in_various_places")

        self.compv1_crRev.dependencies.create(requirement=self.test_cr_1_rev1, depPath="modules")
        self.compv1_crRev.dependencies.create(requirement=self.test_cr_2_rev1, depPath="moremodules")
        self.compv1_crRev.dependencies.create(requirement=self.test_cr_3_rev1, depPath="modules/foo")
        self.compv1_crRev.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))
        self.assertTrue(os.path.isdir(os.path.join(test_path, "modules")))
        self.assertTrue(os.path.isdir(os.path.join(test_path, "moremodules")))
        self.assertTrue(os.path.isdir(os.path.join(test_path, "modules", "foo")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "modules", "test_cr_1.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "moremodules", "test_cr_2.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "modules", "foo", "test_cr_3.py")))

        shutil.rmtree(test_path)

    def test_nested_dependencies(self):
        """
        Test of installing a CRR with dependencies that have their own dependencies.
        """
        test_path = tempfile.mkdtemp(prefix="test_nested_dependencies")

        # Make test_cr_1_rev1 have its own dependencies.
        self.test_cr_1_rev1.dependencies.create(requirement=self.script_1_crRev, depPath=".")
        self.test_cr_1_rev1.dependencies.create(requirement=self.script_2_crRev, depPath="cr1mods")

        self.test_cr_2_rev1.dependencies.create(requirement=self.script_3_crRev, depPath="cr2mods")
        self.test_cr_2_rev1.dependencies.create(requirement=self.script_4_1_CRR, depPath="cr2mods/foo")

        self.compv1_crRev.dependencies.create(requirement=self.test_cr_1_rev1, depPath="")
        self.compv1_crRev.dependencies.create(requirement=self.test_cr_2_rev1, depPath="basemods")
        self.compv1_crRev.install(test_path)

        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "test_cr_1.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "script_1_sum_and_products.py")))
        self.assertTrue(os.path.isdir(os.path.join(test_path, "cr1mods")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "cr1mods", "script_2_square_and_means.py")))

        self.assertTrue(os.path.isdir(os.path.join(test_path, "basemods")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "basemods", "test_cr_2.py")))
        self.assertTrue(os.path.isdir(os.path.join(test_path, "basemods", "cr2mods")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "basemods", "cr2mods", "script_3_product.py")))
        self.assertTrue(os.path.isdir(os.path.join(test_path, "basemods", "cr2mods", "foo")))
        self.assertTrue(
            os.path.exists(os.path.join(test_path, "basemods", "cr2mods", "foo",
                                        "script_4_raw_in_CSV_out.py")))

        shutil.rmtree(test_path)

    def _setup_metapackage(self):
        """Helper that sets up a metapackage."""
        # Define comp_cr
        self.metapackage = CodeResource(
            name="metapackage",
            description="Collection of modules",
            filename="",
            user=self.myUser)
        self.metapackage.save()

        self.metapackage_r1 = CodeResourceRevision(
            coderesource=self.metapackage,
            revision_name="v1",
            revision_desc="First version",
            user=self.myUser
        )
        self.metapackage_r1.save()

        # Add dependencies.
        self.metapackage_r1.dependencies.create(requirement=self.script_1_crRev, depPath=".")
        self.metapackage_r1.dependencies.create(requirement=self.script_2_crRev, depPath=".")
        self.metapackage_r1.dependencies.create(requirement=self.script_3_crRev, depPath="metamodules")
        self.metapackage_r1.dependencies.create(requirement=self.script_4_1_CRR, depPath="metamodules/foo")

    def test_metapackage(self):
        """
        Test of installing a metapackage CRR.
        """
        test_path = tempfile.mkdtemp(prefix="test_install_metapackage")
        self._setup_metapackage()

        self.metapackage_r1.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "script_1_sum_and_products.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "script_2_square_and_means.py")))
        self.assertTrue(os.path.isdir(os.path.join(test_path, "metamodules")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "metamodules", "script_3_product.py")))
        self.assertTrue(os.path.isdir(os.path.join(test_path, "metamodules", "foo")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "metamodules", "foo", "script_4_raw_in_CSV_out.py")))

        shutil.rmtree(test_path)

    def test_dependency_is_metapackage(self):
        """
        Test of installing a CRR with a metapackage dependency.
        """
        test_path = tempfile.mkdtemp(prefix="test_dependency_is_metapackage")
        self._setup_metapackage()

        self.compv1_crRev.dependencies.create(requirement=self.metapackage_r1, depPath="modules")

        self.compv1_crRev.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))

        metapackage_path = os.path.join(test_path, "modules")
        self.assertTrue(os.path.isdir(metapackage_path))
        self.assertTrue(os.path.exists(os.path.join(metapackage_path, "script_1_sum_and_products.py")))
        self.assertTrue(os.path.exists(os.path.join(metapackage_path, "script_2_square_and_means.py")))
        self.assertTrue(os.path.isdir(os.path.join(metapackage_path, "metamodules")))
        self.assertTrue(os.path.exists(os.path.join(metapackage_path, "metamodules", "script_3_product.py")))
        self.assertTrue(os.path.isdir(os.path.join(metapackage_path, "metamodules", "foo")))
        self.assertTrue(os.path.exists(os.path.join(metapackage_path, "metamodules", "foo",
                                                    "script_4_raw_in_CSV_out.py")))
        shutil.rmtree(test_path)


class MethodTests(MethodTestCase):

    def test_with_family_unicode(self):
        """
        unicode() for method should return "Method revisionName and family name"
        """

        # DNAcompv1_m has method family DNAcomplement
        self.assertEqual(unicode(self.DNAcompv1_m),
                         "Method DNAcomplement v1")

    def test_without_family_unicode(self):
        """
        unicode() for Test unicode representation when family is unset.
        """
        nofamily = Method(revision_name="foo")

        self.assertEqual(unicode(nofamily),
                         "Method [family unset] foo")

    def test_no_inputs_checkInputIndices_good(self):
        """
        Method with no inputs defined should have
        check_input_indices() return with no exception.
        """

        # Create Method with valid family, revision_name, description, driver
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", driver=self.compv1_crRev, user=self.myUser)
        foo.save()

        # check_input_indices() should not raise a ValidationError
        self.assertEquals(foo.check_input_indices(), None)
        self.assertEquals(foo.clean(), None)

    def test_single_valid_input_checkInputIndices_good(self):
        """
        Method with a single, 1-indexed input should have
        check_input_indices() return with no exception.
        """

        # Create Method with valid family, revision_name, description, driver
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", 
                     driver=self.compv1_crRev, user=self.myUser)
        foo.save()

        # Add one valid input cdt at index 1 named "oneinput" to transformation
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput", dataset_idx=1)

        # check_input_indices() should not raise a ValidationError
        self.assertEquals(foo.check_input_indices(), None)
        self.assertEquals(foo.clean(), None)

    def test_many_ordered_valid_inputs_checkInputIndices_good (self):
        """
        Test check_input_indices on a method with several inputs,
        correctly indexed and in order.
        """

        # Create Method with valid family, revision_name, description, driver
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", 
                     driver=self.compv1_crRev, user=self.myUser)
        foo.save()

        # Add several input cdts that together are valid
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput", dataset_idx=1)
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="twoinput", dataset_idx=2)
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="threeinput", dataset_idx=3)

        # No ValidationErrors should be raised
        self.assertEquals(foo.check_input_indices(), None)
        self.assertEquals(foo.clean(), None)

    def test_many_valid_inputs_scrambled_checkInputIndices_good (self):
        """
        Test check_input_indices on a method with several inputs,
        correctly indexed and in scrambled order.
        """

        # Create Method with valid family, revision_name, description, driver
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", 
                     driver=self.compv1_crRev, user=self.myUser)
        foo.save()

        # Add several input cdts that together are valid
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput", dataset_idx=3)
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="twoinput", dataset_idx=1)
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="threeinput", dataset_idx=2)

        # No ValidationErrors should be raised
        self.assertEquals(foo.check_input_indices(), None)
        self.assertEquals(foo.clean(), None)

    def test_one_invalid_input_checkInputIndices_bad(self):
        """
        Test input index check, one badly-indexed input case.
        """

        # Create Method with valid family, revision_name, description, driver
        foo = Method(family=self.DNAcomp_mf, revision_name="foo",
                     revision_desc="Foo version", 
                     driver=self.compv1_crRev, user=self.myUser)
        foo.save()

        # Add one invalid input cdt at index 4 named "oneinput"
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput", dataset_idx=4)

        # check_input_indices() should raise a ValidationError
        self.assertRaisesRegexp(
            ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            foo.check_input_indices)

        self.assertRaisesRegexp(
            ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            foo.clean)

    def test_many_nonconsective_inputs_scrambled_checkInputIndices_bad(self):
        """Test input index check, badly-indexed multi-input case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo", revision_desc="Foo version", 
                     driver=self.compv1_crRev, user=self.myUser)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput", dataset_idx=2)
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="twoinput", dataset_idx=6)
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="threeinput", dataset_idx=1)
        self.assertRaisesRegexp(
            ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            foo.check_input_indices)

        self.assertRaisesRegexp(
            ValidationError,
            "Inputs are not consecutively numbered starting from 1",
            foo.clean)

    def test_no_outputs_checkOutputIndices_good(self):
        """Test output index check, one well-indexed output case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo", revision_desc="Foo version", 
                     driver=self.compv1_crRev, user=self.myUser)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput", dataset_idx=1)

        self.assertEquals(foo.check_output_indices(), None)
        self.assertEquals(foo.clean(), None)

    def test_one_valid_output_checkOutputIndices_good(self):
        """Test output index check, one well-indexed output case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo", revision_desc="Foo version", 
                     driver=self.compv1_crRev, user=self.myUser)
        foo.save()
        foo.create_output(compounddatatype=self.DNAoutput_cdt,
                          dataset_name="oneoutput", dataset_idx=1)
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput", dataset_idx=1)
        self.assertEquals(foo.check_output_indices(), None)
        self.assertEquals(foo.clean(), None)

    def test_many_valid_outputs_scrambled_checkOutputIndices_good (self):
        """Test output index check, well-indexed multi-output (scrambled order) case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo", revision_desc="Foo version", 
                     driver=self.compv1_crRev, user=self.myUser)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput", dataset_idx=1)
        foo.create_output(compounddatatype=self.DNAoutput_cdt,
                          dataset_name="oneoutput", dataset_idx=3)
        foo.create_output(compounddatatype=self.DNAoutput_cdt,
                          dataset_name="twooutput", dataset_idx=1)
        foo.create_output(compounddatatype=self.DNAoutput_cdt,
                          dataset_name="threeoutput", dataset_idx=2)
        self.assertEquals(foo.check_output_indices(), None)
        self.assertEquals(foo.clean(), None)

    def test_one_invalid_output_checkOutputIndices_bad (self):
        """Test output index check, one badly-indexed output case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo", revision_desc="Foo version", 
                     driver=self.compv1_crRev, user=self.myUser)
        foo.save()
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput", dataset_idx=1)
        foo.create_output(compounddatatype=self.DNAoutput_cdt,
                          dataset_name="oneoutput", dataset_idx=4)
        self.assertRaisesRegexp(
            ValidationError,
            "Outputs are not consecutively numbered starting from 1",
            foo.check_output_indices)

        self.assertRaisesRegexp(
            ValidationError,
            "Outputs are not consecutively numbered starting from 1",
            foo.clean)

    def test_many_invalid_outputs_scrambled_checkOutputIndices_bad(self):
        """Test output index check, badly-indexed multi-output case."""
        foo = Method(family=self.DNAcomp_mf, revision_name="foo", revision_desc="Foo version", 
                     driver=self.compv1_crRev, user=self.myUser)
        foo.save()
        
        foo.create_input(compounddatatype=self.DNAinput_cdt,
                         dataset_name="oneinput", dataset_idx=1)
        foo.create_output(compounddatatype=self.DNAoutput_cdt,
                          dataset_name="oneoutput", dataset_idx=2)
        foo.create_output(compounddatatype=self.DNAoutput_cdt,
                          dataset_name="twooutput", dataset_idx=6)
        foo.create_output(compounddatatype=self.DNAoutput_cdt,
                          dataset_name="threeoutput", dataset_idx=1)
        self.assertRaisesRegexp(
            ValidationError,
            "Outputs are not consecutively numbered starting from 1",
            foo.check_output_indices)

        self.assertRaisesRegexp(
            ValidationError,
            "Outputs are not consecutively numbered starting from 1",
            foo.clean)

    def test_no_copied_parent_parameters_save(self):
        """Test save when no method revision parent is specified."""

        # Define new Method with no parent
        foo = Method(family=self.DNAcomp_mf, revision_name="foo", revision_desc="Foo version", 
                     driver=self.compv1_crRev, user=self.myUser)
        foo.save()

        # There should be no inputs
        self.assertEqual(foo.inputs.count(), 0)
        self.assertEqual(foo.outputs.count(), 0)

        # DNAcompv1_m also has no parents as it is the first revision
        self.DNAcompv1_m.save()

        # DNAcompv1_m was defined to have 1 input and 1 output
        self.assertEqual(self.DNAcompv1_m.inputs.count(), 1)
        self.assertEqual(self.DNAcompv1_m.inputs.all()[0],
                         self.DNAinput_ti)

        self.assertEqual(self.DNAcompv1_m.outputs.count(), 1)
        self.assertEqual(self.DNAcompv1_m.outputs.all()[0],
                         self.DNAoutput_to)

        # Test the multiple-input and multiple-output cases, using
        # script_2_method and script_3_method respectively.  Neither
        # of these have parents.
        self.script_2_method.save()
        # Script 2 has input:
        # compounddatatype = self.triplet_cdt
        # dataset_name = "a_b_c"
        # dataset_idx = 1
        curr_in = self.script_2_method.inputs.all()[0]
        self.assertEqual(curr_in.dataset_name, "a_b_c")
        self.assertEqual(curr_in.dataset_idx, 1)
        self.assertEqual(curr_in.get_cdt(), self.triplet_cdt)
        self.assertEqual(curr_in.get_min_row(), None)
        self.assertEqual(curr_in.get_max_row(), None)
        # Outputs:
        # self.triplet_cdt, "a_b_c_squared", 1
        # self.singlet_cdt, "a_b_c_mean", 2
        curr_out_1 = self.script_2_method.outputs.get(dataset_idx=1)
        curr_out_2 = self.script_2_method.outputs.get(dataset_idx=2)
        self.assertEqual(curr_out_1.dataset_name, "a_b_c_squared")
        self.assertEqual(curr_out_1.dataset_idx, 1)
        self.assertEqual(curr_out_1.get_cdt(), self.triplet_cdt)
        self.assertEqual(curr_out_1.get_min_row(), None)
        self.assertEqual(curr_out_1.get_max_row(), None)
        self.assertEqual(curr_out_2.dataset_name, "a_b_c_mean")
        self.assertEqual(curr_out_2.dataset_idx, 2)
        self.assertEqual(curr_out_2.get_cdt(), self.singlet_cdt)
        self.assertEqual(curr_out_2.get_min_row(), None)
        self.assertEqual(curr_out_2.get_max_row(), None)

        self.script_3_method.save()
        # Script 3 has inputs:
        # self.singlet_cdt, "k", 1
        # self.singlet_cdt, "r", 2, min_row = max_row = 1
        curr_in_1 = self.script_3_method.inputs.get(dataset_idx=1)
        curr_in_2 = self.script_3_method.inputs.get(dataset_idx=2)
        self.assertEqual(curr_in_1.dataset_name, "k")
        self.assertEqual(curr_in_1.dataset_idx, 1)
        self.assertEqual(curr_in_1.get_cdt(), self.singlet_cdt)
        self.assertEqual(curr_in_1.get_min_row(), None)
        self.assertEqual(curr_in_1.get_max_row(), None)
        self.assertEqual(curr_in_2.dataset_name, "r")
        self.assertEqual(curr_in_2.dataset_idx, 2)
        self.assertEqual(curr_in_2.get_cdt(), self.singlet_cdt)
        self.assertEqual(curr_in_2.get_min_row(), 1)
        self.assertEqual(curr_in_2.get_max_row(), 1)
        # Outputs:
        # self.singlet_cdt, "kr", 1
        curr_out = self.script_3_method.outputs.get(dataset_idx=1)
        self.assertEqual(curr_out.dataset_name, "kr")
        self.assertEqual(curr_out.dataset_idx, 1)
        self.assertEqual(curr_out.get_cdt(), self.singlet_cdt)
        self.assertEqual(curr_out.get_min_row(), None)
        self.assertEqual(curr_out.get_max_row(), None)

    def test_copy_io_from_parent(self):
        """Test save when revision parent is specified."""

        # DNAcompv2_m should have 1 input, copied from DNAcompv1
        self.assertEqual(self.DNAcompv2_m.inputs.count(), 1)
        curr_in = self.DNAcompv2_m.inputs.get(dataset_idx=1)
        self.assertEqual(curr_in.dataset_name,
                         self.DNAinput_ti.dataset_name)
        self.assertEqual(curr_in.dataset_idx,
                         self.DNAinput_ti.dataset_idx)
        self.assertEqual(curr_in.get_cdt(),
                         self.DNAinput_ti.get_cdt())

        self.assertEqual(self.DNAcompv2_m.outputs.count(), 1)
        curr_out = self.DNAcompv2_m.outputs.get(dataset_idx=1)
        self.assertEqual(curr_out.dataset_name,
                         self.DNAoutput_to.dataset_name)
        self.assertEqual(curr_out.dataset_idx,
                         self.DNAoutput_to.dataset_idx)
        self.assertEqual(curr_out.get_cdt(),
                         self.DNAoutput_to.get_cdt())

        # Multiple output case (using script_2_method).
        foo = Method(family=self.test_mf, driver=self.script_2_crRev,
                     revision_parent=self.script_2_method, user=self.myUser)
        foo.save()
        foo.copy_io_from_parent()
        # Check that it has the same input as script_2_method:
        # self.triplet_cdt, "a_b_c", 1
        curr_in = foo.inputs.get(dataset_idx=1)
        self.assertEqual(curr_in.dataset_name, "a_b_c")
        self.assertEqual(curr_in.dataset_idx, 1)
        self.assertEqual(curr_in.get_cdt(), self.triplet_cdt)
        self.assertEqual(curr_in.get_min_row(), None)
        self.assertEqual(curr_in.get_max_row(), None)
        # Outputs:
        # self.triplet_cdt, "a_b_c_squared", 1
        # self.singlet_cdt, "a_b_c_mean", 2
        curr_out_1 = foo.outputs.get(dataset_idx=1)
        curr_out_2 = foo.outputs.get(dataset_idx=2)
        self.assertEqual(curr_out_1.get_cdt(), self.triplet_cdt)
        self.assertEqual(curr_out_1.dataset_name, "a_b_c_squared")
        self.assertEqual(curr_out_1.dataset_idx, 1)
        self.assertEqual(curr_out_1.get_min_row(), None)
        self.assertEqual(curr_out_1.get_max_row(), None)
        self.assertEqual(curr_out_2.get_cdt(), self.singlet_cdt)
        self.assertEqual(curr_out_2.dataset_name, "a_b_c_mean")
        self.assertEqual(curr_out_2.dataset_idx, 2)
        self.assertEqual(curr_out_2.get_min_row(), None)
        self.assertEqual(curr_out_2.get_max_row(), None)

        # Multiple input case (using script_3_method).
        bar = Method(family=self.test_mf, driver=self.script_3_crRev,
                     revision_parent=self.script_3_method, user=self.myUser)
        bar.save()
        bar.copy_io_from_parent()
        # Check that the outputs match script_3_method:
        # self.singlet_cdt, "k", 1
        # self.singlet_cdt, "r", 2, min_row = max_row = 1
        curr_in_1 = bar.inputs.get(dataset_idx=1)
        curr_in_2 = bar.inputs.get(dataset_idx=2)
        self.assertEqual(curr_in_1.get_cdt(), self.singlet_cdt)
        self.assertEqual(curr_in_1.dataset_name, "k")
        self.assertEqual(curr_in_1.dataset_idx, 1)
        self.assertEqual(curr_in_1.get_min_row(), None)
        self.assertEqual(curr_in_1.get_max_row(), None)
        self.assertEqual(curr_in_2.get_cdt(), self.singlet_cdt)
        self.assertEqual(curr_in_2.dataset_name, "r")
        self.assertEqual(curr_in_2.dataset_idx, 2)
        self.assertEqual(curr_in_2.get_min_row(), 1)
        self.assertEqual(curr_in_2.get_max_row(), 1)
        # Outputs:
        # self.singlet_cdt, "kr", 1
        curr_out = bar.outputs.get(dataset_idx=1)
        self.assertEqual(curr_out.get_cdt(), self.singlet_cdt)
        self.assertEqual(curr_out.dataset_name, "kr")
        self.assertEqual(curr_out.dataset_idx, 1)
        self.assertEqual(curr_out.get_min_row(), None)
        self.assertEqual(curr_out.get_max_row(), None)


        # If there are already inputs and outputs specified, then
        # they should not be overwritten.

        old_cdt = self.DNAinput_ti.get_cdt()
        old_name = self.DNAinput_ti.dataset_name
        old_idx = self.DNAinput_ti.dataset_idx

        self.DNAcompv1_m.revision_parent = self.RNAcompv2_m
        self.DNAcompv1_m.save()
        self.DNAcompv1_m.copy_io_from_parent()
        self.assertEqual(self.DNAcompv1_m.inputs.count(), 1)
        curr_in = self.DNAcompv1_m.inputs.get(dataset_idx=1)
        self.assertEqual(curr_in.get_cdt(), old_cdt)
        self.assertEqual(curr_in.dataset_name, old_name)
        self.assertEqual(curr_in.dataset_idx, old_idx)

        old_cdt = self.DNAoutput_to.get_cdt()
        old_name = self.DNAoutput_to.dataset_name
        old_idx = self.DNAoutput_to.dataset_idx

        self.assertEqual(self.DNAcompv2_m.outputs.count(), 1)
        curr_out = self.DNAcompv2_m.outputs.get(dataset_idx=1)
        self.assertEqual(curr_out.get_cdt(), old_cdt)
        self.assertEqual(curr_out.dataset_name, old_name)
        self.assertEqual(curr_out.dataset_idx, old_idx)

        # Only inputs specified.
        bar.outputs.all().delete()
        bar.save()
        bar.copy_io_from_parent()
        self.assertEqual(bar.inputs.count(), 2)
        self.assertEqual(bar.outputs.count(), 0)
        curr_in_1 = bar.inputs.get(dataset_idx=1)
        curr_in_2 = bar.inputs.get(dataset_idx=2)
        self.assertEqual(curr_in_1.get_cdt(), self.singlet_cdt)
        self.assertEqual(curr_in_1.dataset_name, "k")
        self.assertEqual(curr_in_1.dataset_idx, 1)
        self.assertEqual(curr_in_1.get_min_row(), None)
        self.assertEqual(curr_in_1.get_max_row(), None)
        self.assertEqual(curr_in_2.get_cdt(), self.singlet_cdt)
        self.assertEqual(curr_in_2.dataset_name, "r")
        self.assertEqual(curr_in_2.dataset_idx, 2)
        self.assertEqual(curr_in_2.get_min_row(), 1)
        self.assertEqual(curr_in_2.get_max_row(), 1)

        # Only outputs specified.
        foo.inputs.all().delete()
        foo.save()
        foo.copy_io_from_parent()
        self.assertEqual(foo.inputs.count(), 0)
        self.assertEqual(foo.outputs.count(), 2)
        curr_out_1 = foo.outputs.get(dataset_idx=1)
        curr_out_2 = foo.outputs.get(dataset_idx=2)
        self.assertEqual(curr_out_1.get_cdt(), self.triplet_cdt)
        self.assertEqual(curr_out_1.dataset_name, "a_b_c_squared")
        self.assertEqual(curr_out_1.dataset_idx, 1)
        self.assertEqual(curr_out_1.get_min_row(), None)
        self.assertEqual(curr_out_1.get_max_row(), None)
        self.assertEqual(curr_out_2.get_cdt(), self.singlet_cdt)
        self.assertEqual(curr_out_2.dataset_name, "a_b_c_mean")
        self.assertEqual(curr_out_2.dataset_idx, 2)
        self.assertEqual(curr_out_2.get_min_row(), None)
        self.assertEqual(curr_out_2.get_max_row(), None)

    def test_driver_is_metapackage(self):
        """
        A metapackage cannot be a driver for a Method.
        """
        # Create a CodeResourceRevision with no content file (ie. a Metapackage).
        res = CodeResource(user=self.myUser); res.save()
        rev = CodeResourceRevision(coderesource=res, content_file=None, user=self.myUser); rev.clean(); rev.save()
        f = MethodFamily(user=self.myUser); f.save()
        m = Method(family=f, driver=rev, user=self.myUser)
        m.save()
        m.create_input(compounddatatype = self.singlet_cdt,
            dataset_name = "input",
            dataset_idx = 1)
        self.assertRaisesRegexp(ValidationError,
                                re.escape('Method "{}" cannot have CodeResourceRevision "{}" as a driver, because it '
                                          'has no content file.'.format(m, rev)),
                                m.clean)

    def test_invoke_code_nooutput(self):
        """
        Invoke a no-output method (which just prints to stdout).
        """
        empty_dir = tempfile.mkdtemp()

        proc = self.noop_method.invoke_code(empty_dir, [self.noop_infile], [])
        proc_out, _ = proc.communicate()

        self.assertEqual(proc_out, self.noop_indata)

        shutil.rmtree(empty_dir)

    def test_invoke_code_dir_not_empty(self):
        """
        Trying to invoke code in a non-empty directory should fail.
        """
        self.assertRaisesRegexp(ValueError,
            "Directory .* nonempty; contains file .*",
            lambda : self.noop_method.invoke_code(self.scratch_dir, [self.noop_infile], []))

    def test_delete_method(self):
        """Deleting a method is possible."""
        self.assertIsNone(Method.objects.first().delete())

    def test_identical_self(self):
        """A Method should be identical to itself."""
        m = Method.objects.first()
        self.assertTrue(m.is_identical(m))

    def test_identical_different_names(self):
        """Two methods differing only in names are identical."""
        m1 = Method.objects.filter(inputs__isnull=False, outputs__isnull=False).first()
        m2 = Method(revision_name="x" + m1.revision_name, driver=m1.driver, family=MethodFamily.objects.first(),
                    user=self.myUser)
        m2.save()
        for input in m1.inputs.order_by("dataset_idx"):
            m2.create_input("x" + input.dataset_name, 
                    compounddatatype=input.compounddatatype,
                    min_row=input.get_min_row(), 
                    max_row=input.get_max_row())
        for output in m1.outputs.order_by("dataset_idx"):
            m2.create_output("x" + output.dataset_name, 
                    compounddatatype=output.compounddatatype,
                    min_row=output.get_min_row(), 
                    max_row=output.get_max_row())
        self.assertFalse(m1.revision_name == m2.revision_name)
        self.assertFalse(m1.inputs.first().dataset_name == m2.inputs.first().dataset_name)
        self.assertFalse(m1.outputs.first().dataset_name == m2.outputs.first().dataset_name)
        self.assertTrue(m1.is_identical(m2))

    def test_identical_different_drivers(self):
        """Two methods with identical IO, but different drivers, are not identical."""
        m1 = Method.objects.filter(inputs__isnull=False, outputs__isnull=False).first()
        driver = CodeResourceRevision.objects.exclude(pk=m1.driver.pk).first()
        m2 = Method(revision_name=m1.revision_name, driver=driver, family=m1.family, user=self.myUser)
        m2.save()
        for input in m1.inputs.order_by("dataset_idx"):
            m2.create_input("x" + input.dataset_name, 
                    compounddatatype=input.compounddatatype,
                    min_row=input.get_min_row(), 
                    max_row=input.get_max_row())
        for output in m1.outputs.order_by("dataset_idx"):
            m2.create_output("x" + output.dataset_name, 
                    compounddatatype=output.compounddatatype,
                    min_row=output.get_min_row(), 
                    max_row=output.get_max_row())
        self.assertTrue(super(Method, m1).is_identical(super(Method, m2)))
        self.assertFalse(m1.driver.pk == m2.driver.pk)
        self.assertFalse(m1.is_identical(m2))

    def test_create(self):
        """Create a new Method by the constructor."""
        names = ["a", "b"]
        cdts = CompoundDatatype.objects.all()[:2]
        family = MethodFamily.objects.first()
        driver = CodeResourceRevision.objects.first()
        m = Method.create(names, compounddatatypes=cdts, num_inputs=1, family=family, driver=driver, user=self.myUser)
        self.assertIsNone(m.complete_clean())

    # The identicality constraint has been relaxed, so this test is no longer valid.
    # def test_create_identical(self):
    #     """Cannot create a duplicate Method."""
    #     m = Method.objects.filter(inputs__isnull=False, outputs__isnull=False).first()
    #     num_inputs = m.inputs.count()
    #
    #     xputs = itertools.chain(m.inputs.order_by("dataset_idx"), m.outputs.order_by("dataset_idx"))
    #     names = []
    #     compounddatatypes = []
    #     row_limits = []
    #     for xput in xputs:
    #         names.append(xput.dataset_name)
    #         compounddatatypes.append(xput.compounddatatype)
    #         row_limits.append((xput.get_min_row(), xput.get_max_row()))
    #
    #     factory = lambda: Method.create(names,
    #             compounddatatypes=compounddatatypes,
    #             row_limits=row_limits,
    #             num_inputs=num_inputs,
    #             driver=m.driver,
    #             family=m.family,
    #             user=self.myUser)
    #     self.assertRaisesRegexp(ValidationError, "An identical method already exists", factory)


class MethodFamilyTests(MethodTestCase):

    def test_unicode(self):
        """
        unicode() for MethodFamily should display it's name.
        """
        
        self.assertEqual(unicode(self.DNAcomp_mf), "DNAcomplement")


class NonReusableMethodTests(TransactionTestCase):
    # fixtures = ["initial_data", "initial_groups", "initial_user"]

    def setUp(self):
        # Loading the fixtures using the 'fixtures' attribute doesn't work due to
        # subtleties in how Django's tests run.
        call_command("loaddata", "initial_groups", verbosity=0)
        call_command("loaddata", "initial_user", verbosity=0)
        call_command("loaddata", "initial_data", verbosity=0)

        # An unpredictable, non-reusable user.
        self.user_rob = User.objects.create_user('rob', 'rford@toronto.ca', 'football')
        self.user_rob.save()
        self.user_rob.groups.add(everyone_group())
        self.user_rob.save()

        # A piece of code that is non-reusable.
        self.rng = tools.make_first_revision(
            "rng", "Generates a random number", "rng.py",
            """#! /usr/bin/env python

import random
import csv
import sys

outfile = sys.argv[1]

with open(outfile, "wb") as f:
    my_writer = csv.writer(f)
    my_writer.writerow(("random number",))
    my_writer.writerow((random.random(),))
""",
            self.user_rob
        )

        self.rng_out_cdt = CompoundDatatype(user=self.user_rob)
        self.rng_out_cdt.save()
        self.rng_out_cdt.members.create(
            column_name="random number", column_idx=1,
            datatype=Datatype.objects.get(pk=datatypes.FLOAT_PK)
        )
        self.rng_out_cdt.grant_everyone_access()

        self.rng_method = tools.make_first_method("rng", "Generate a random number", self.rng,
                                                  self.user_rob)
        self.rng_method.create_output(dataset_name="random_number", dataset_idx=1, compounddatatype=self.rng_out_cdt,
                                      min_row=1, max_row=1)
        self.rng_method.reusable = Method.NON_REUSABLE
        self.rng_method.save()

        self.increment = tools.make_first_revision(
            "increment", "Increments all numbers in its first input file by the number in its second",
            "increment.py",
            """#! /usr/bin/env python

import csv
import sys

numbers_file = sys.argv[1]
increment_file = sys.argv[2]
outfile = sys.argv[3]

incrementor = 0
with open(increment_file, "rb") as f:
    inc_reader = csv.DictReader(f)
    for row in inc_reader:
        incrementor = float(row["incrementor"])
        break

numbers = []
with open(numbers_file, "rb") as f:
    number_reader = csv.DictReader(f)
    for row in number_reader:
        numbers.append(float(row["number"]))

with open(outfile, "wb") as f:
    out_writer = csv.writer(f)
    out_writer.writerow(("incremented number",))
    for number in numbers:
        out_writer.writerow((number + incrementor,))
""",
            self.user_rob
        )

        self.increment_in_1_cdt = CompoundDatatype(user=self.user_rob)
        self.increment_in_1_cdt.save()
        self.increment_in_1_cdt.members.create(
            column_name="number", column_idx=1,
            datatype=Datatype.objects.get(pk=datatypes.FLOAT_PK)
        )
        self.increment_in_1_cdt.grant_everyone_access()

        self.increment_in_2_cdt = CompoundDatatype(user=self.user_rob)
        self.increment_in_2_cdt.save()
        self.increment_in_2_cdt.members.create(
            column_name="incrementor", column_idx=1,
            datatype=Datatype.objects.get(pk=datatypes.FLOAT_PK)
        )
        self.increment_in_2_cdt.grant_everyone_access()

        self.increment_out_cdt = CompoundDatatype(user=self.user_rob)
        self.increment_out_cdt.save()
        self.increment_out_cdt.members.create(
            column_name="incremented number", column_idx=1,
            datatype=Datatype.objects.get(pk=datatypes.FLOAT_PK)
        )
        self.increment_out_cdt.grant_everyone_access()

        self.inc_method = tools.make_first_method(
            "increment", "Increments all numbers in its first input file by the number in its second",
            self.increment, self.user_rob)
        self.inc_method.create_input(dataset_name="numbers", dataset_idx=1, compounddatatype=self.increment_in_1_cdt)
        self.inc_method.create_input(dataset_name="incrementor", dataset_idx=2,
                                     compounddatatype=self.increment_in_2_cdt,
                                     min_row=1, max_row=1)
        self.inc_method.create_output(dataset_name="incremented_numbers", dataset_idx=1,
                                      compounddatatype=self.increment_out_cdt)

        self.test_nonreusable = tools.make_first_pipeline("Non-Reusable", "Pipeline with a non-reusable step",
                                                          self.user_rob)
        self.test_nonreusable.create_input(dataset_name="numbers", dataset_idx=1,
                                           compounddatatype=self.increment_in_1_cdt)
        _step1 = self.test_nonreusable.steps.create(
            step_num=1,
            transformation=self.rng_method,
            name="source of randomness"
        )

        step2 = self.test_nonreusable.steps.create(
            step_num=2,
            transformation=self.inc_method,
            name="incrementor"
        )
        step2.cables_in.create(
            dest=self.inc_method.inputs.get(dataset_name="numbers"),
            source_step=0,
            source=self.test_nonreusable.inputs.get(dataset_name="numbers")
        )
        connecting_cable = step2.cables_in.create(
            dest=self.inc_method.inputs.get(dataset_name="incrementor"),
            source_step=1,
            source=self.rng_method.outputs.get(dataset_name="random_number")
        )
        connecting_cable.custom_wires.create(
            source_pin=self.rng_out_cdt.members.get(column_name="random number"),
            dest_pin=self.increment_in_2_cdt.members.get(column_name="incrementor")
        )

        self.test_nonreusable.create_outcable(
            output_name="incremented_numbers",
            output_idx=1,
            source_step=2,
            source=self.inc_method.outputs.get(dataset_name="incremented_numbers")
        )

        self.test_nonreusable.create_outputs()

        # A data file to add to the database.
        self.numbers = "number\n1\n2\n3\n4\n"
        datafile = tempfile.NamedTemporaryFile(delete=False)
        datafile.write(self.numbers)
        datafile.close()

        # Alice uploads the data to the system.
        self.numbers_symDS = librarian.models.SymbolicDataset.create_SD(
            datafile.name, name="numbers", cdt=self.increment_in_1_cdt,
            user=self.user_rob, description="1-2-3-4",
            make_dataset=True, groups_allowed=[everyone_group()])

    def tearDown(self):
        # Our tests fail post-teardown without this.
        update_all_contenttypes(verbosity=0)

    def test_find_compatible_ER_non_reusable_method(self):
        """
        The ExecRecord of a non-reusable Method should not be found compatible.
        """
        sdbx = sandbox.execute.Sandbox(self.user_rob, self.test_nonreusable, [self.numbers_symDS])
        sdbx.execute_pipeline()

        rng_step = self.test_nonreusable.steps.get(step_num=1)
        self.assertListEqual(self.rng_method.find_compatible_ERs([], rng_step), [])

    def test_execute_does_not_reuse(self):
        """
        Running a non-reusable Method twice does not reuse an ExecRecord, and
        subsequent steps and cables in the same Pipeline will have different ExecRecords also.
        """
        sdbx = sandbox.execute.Sandbox(self.user_rob, self.test_nonreusable, [self.numbers_symDS])
        sdbx.execute_pipeline()
        first_step_1 = sdbx.run.runsteps.get(pipelinestep__step_num=1)
        second_step_1 = sdbx.run.runsteps.get(pipelinestep__step_num=2)
        joining_cable_1 = second_step_1.RSICs.get(PSIC__dest=self.inc_method.inputs.get(dataset_name="incrementor"))

        sdbx2 = sandbox.execute.Sandbox(self.user_rob, self.test_nonreusable, [self.numbers_symDS])
        sdbx2.execute_pipeline()
        first_step_2 = sdbx2.run.runsteps.get(pipelinestep__step_num=1)
        second_step_2 = sdbx2.run.runsteps.get(pipelinestep__step_num=2)
        joining_cable_2 = second_step_2.RSICs.get(PSIC__dest=self.inc_method.inputs.get(dataset_name="incrementor"))

        self.assertNotEqual(first_step_1.execrecord, first_step_2.execrecord)
        self.assertNotEqual(second_step_1.execrecord, second_step_2.execrecord)
        self.assertNotEqual(joining_cable_1.execrecord, joining_cable_2.execrecord)


class MethodFamilyApiTests(BaseTestCases.ApiTestCase):
    fixtures = ['demo']
    
    def setUp(self):
        super(MethodFamilyApiTests, self).setUp()

        self.list_path = reverse("methodfamily-list")
        self.detail_pk = 2
        self.detail_path = reverse("methodfamily-detail",
                                   kwargs={'pk': self.detail_pk})
        self.removal_path = reverse("methodfamily-removal-plan",
                                    kwargs={'pk': self.detail_pk})

        # This should equal metadata.ajax.CompoundDatatypeViewSet.as_view({"get": "list"}).
        self.list_view, _, _ = resolve(self.list_path)
        self.detail_view, _, _ = resolve(self.detail_path)
        self.removal_view, _, _ = resolve(self.removal_path)

    def test_list(self):
        """
        Test the CompoundDatatype API list view.
        """
        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        # There are four CDTs loaded into the Database by default.
        self.assertEquals(len(response.data), 7)
        self.assertEquals(response.data[6]['name'], 'sums and products')

    def test_detail(self):
        request = self.factory.get(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.data['name'], 'sums and products')

    def test_removal_plan(self):
        request = self.factory.get(self.removal_path)
        force_authenticate(request, user=self.kive_user)
        response = self.removal_view(request, pk=self.detail_pk)
        self.assertEquals(response.data['MethodFamilies'], 1)

    def test_removal(self):
        start_count = MethodFamily.objects.all().count()
        
        request = self.factory.delete(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.status_code, status.HTTP_204_NO_CONTENT)

        end_count = MethodFamily.objects.all().count()
        self.assertEquals(end_count, start_count - 1)


class MethodApiTests(BaseTestCases.ApiTestCase):
    fixtures = ['simple_run']
    
    def setUp(self):
        super(MethodApiTests, self).setUp()

        self.list_path = reverse("method-list")
        self.detail_pk = 2
        self.detail_path = reverse("method-detail",
                                   kwargs={'pk': self.detail_pk})
        self.removal_path = reverse("method-removal-plan",
                                    kwargs={'pk': self.detail_pk})

        # This should equal metadata.ajax.CompoundDatatypeViewSet.as_view({"get": "list"}).
        self.list_view, _, _ = resolve(self.list_path)
        self.detail_view, _, _ = resolve(self.detail_path)
        self.removal_view, _, _ = resolve(self.removal_path)

    def test_list(self):
        """
        Test the CompoundDatatype API list view.
        """
        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        # There are four CDTs loaded into the Database by default.
        self.assertEquals(len(response.data), 6)
        self.assertEquals(response.data[0]['revision_name'], 'mC_name')

    def test_detail(self):
        request = self.factory.get(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.data['revision_name'], 'mB_name')

    def test_removal_plan(self):
        request = self.factory.get(self.removal_path)
        force_authenticate(request, user=self.kive_user)
        response = self.removal_view(request, pk=self.detail_pk)
        self.assertEquals(response.data['Methods'], 1)

    def test_removal(self):
        start_count = Method.objects.all().count()
        
        request = self.factory.delete(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.status_code, status.HTTP_204_NO_CONTENT)

        end_count = Method.objects.all().count()
        self.assertEquals(end_count, start_count - 1)


class CodeResourceApiTests(BaseTestCases.ApiTestCase):
    fixtures = ["removal"]

    def setUp(self):
        super(CodeResourceApiTests, self).setUp()

        self.list_path = reverse("coderesource-list")
        self.list_view, _, _ = resolve(self.list_path)

        # This user is defined in the removal fixture.
        self.remover = User.objects.get(pk=2)
        self.noop_cr = CodeResource.objects.get(name="Noop")

        self.detail_path = reverse("coderesource-detail", kwargs={"pk": self.noop_cr.pk})
        self.detail_view, _, _ = resolve(self.detail_path)
        self.removal_plan = self.noop_cr.build_removal_plan()

    def test_list_url(self):
        """
        Test that the API list URL is correctly defined.
        """
        # Check that the URL is correctly defined.
        self.assertEquals(self.list_path, "/api/coderesources/")

    def test_list(self):
        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.remover)
        response = self.list_view(request, pk=None)

        self.assertSetEqual(
            set([x["name"] for x in response.data]),
            set([x.name for x in CodeResource.objects.filter(user=self.remover)])
        )

    def test_detail(self):
        self.assertEquals(self.detail_path, "/api/coderesources/{}/".format(self.noop_cr.pk))

        request = self.factory.get(self.detail_path)
        force_authenticate(request, user=self.remover)
        response = self.detail_view(request, pk=self.noop_cr.pk)
        detail = response.data

        self.assertEquals(detail["id"], self.noop_cr.pk)
        self.assertEquals(detail["num_revisions"], self.noop_cr.num_revisions)
        self.assertEquals(detail["absolute_url"], self.noop_cr.get_absolute_url())

    def test_removal_plan(self):
        cr_removal_path = reverse("coderesource-removal-plan", kwargs={'pk': self.noop_cr.pk})
        cr_removal_view, _, _ = resolve(cr_removal_path)

        request = self.factory.get(cr_removal_path)
        force_authenticate(request, user=self.remover)
        response = cr_removal_view(request, pk=self.noop_cr.pk)

        for key in self.removal_plan:
            self.assertEquals(response.data[key], len(self.removal_plan[key]))
        self.assertEquals(response.data['CodeResources'], 1)

        # Noop is a dependency of Pass Through, so:
        self.assertEquals(response.data["CodeResourceRevisions"], 2)

    def test_removal(self):
        start_count = CodeResource.objects.count()
        start_crr_count = CodeResourceRevision.objects.count()

        request = self.factory.delete(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.noop_cr.pk)
        self.assertEquals(response.status_code, status.HTTP_204_NO_CONTENT)

        end_count = CodeResource.objects.count()
        end_crr_count = CodeResourceRevision.objects.count()
        self.assertEquals(end_count, start_count - len(self.removal_plan["CodeResources"]))
        # Noop is a dependency of Pass Through, so it should also take out the other CodeResourceRevision.
        self.assertEquals(end_crr_count, start_crr_count - len(self.removal_plan["CodeResourceRevisions"]))

    def test_revisions(self):
        cr_revisions_path = reverse("coderesource-revisions", kwargs={"pk": self.noop_cr.pk})
        cr_revisions_view, _, _ = resolve(cr_revisions_path)

        request = self.factory.get(cr_revisions_path)
        force_authenticate(request, user=self.remover)
        response = cr_revisions_view(request, pk=self.noop_cr.pk)

        self.assertSetEqual(set([x.revision_number for x in self.noop_cr.revisions.all()]),
                             set([x["revision_number"] for x in response.data]))


class CodeResourceRevisionApiTests(BaseTestCases.ApiTestCase):
    fixtures = ["removal"]

    def setUp(self):
        super(CodeResourceRevisionApiTests, self).setUp()

        self.list_path = reverse("coderesourcerevision-list")
        self.list_view, _, _ = resolve(self.list_path)

        # This user is defined in the removal fixture.
        self.remover = User.objects.get(pk=2)
        self.noop_cr = CodeResource.objects.get(name="Noop")
        self.noop_crr = self.noop_cr.revisions.get(revision_number=1)

        self.detail_path = reverse("coderesourcerevision-detail", kwargs={"pk": self.noop_crr.pk})
        self.detail_view, _, _ = resolve(self.detail_path)
        self.removal_plan = self.noop_crr.build_removal_plan()

    def test_list(self):
        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.remover)
        response = self.list_view(request, pk=None)

        self.assertItemsEqual(
            [x.pk for x in CodeResourceRevision.objects.filter(user=self.remover)],
            [x["id"] for x in response.data]
        )

    def test_detail(self):
        request = self.factory.get(self.detail_path)
        force_authenticate(request, user=self.remover)
        response = self.detail_view(request, pk=self.noop_crr.pk)
        detail = response.data

        self.assertEquals(detail["id"], self.noop_crr.pk)
        self.assertEquals(detail["absolute_url"], self.noop_crr.get_absolute_url())
        self.assertEquals(detail["revision_name"], self.noop_crr.revision_name)

    def test_removal_plan(self):
        crr_removal_path = reverse("coderesourcerevision-removal-plan", kwargs={'pk': self.noop_crr.pk})
        crr_removal_view, _, _ = resolve(crr_removal_path)

        request = self.factory.get(crr_removal_path)
        force_authenticate(request, user=self.remover)
        response = crr_removal_view(request, pk=self.noop_crr.pk)

        for key in self.removal_plan:
            self.assertEquals(response.data[key], len(self.removal_plan[key]))
        # This CRR is a dependency of another one, so:
        self.assertEquals(response.data["CodeResourceRevisions"], 2)

    def test_removal(self):
        start_count = CodeResourceRevision.objects.count()

        request = self.factory.delete(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.noop_crr.pk)
        self.assertEquals(response.status_code, status.HTTP_204_NO_CONTENT)

        end_count = CodeResourceRevision.objects.count()
        # In the above we confirmed this length is 2.
        self.assertEquals(end_count, start_count - len(self.removal_plan["CodeResourceRevisions"]))
