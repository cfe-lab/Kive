"""
Unit tests for Shipyard method models.
"""

import filecmp
import hashlib
import os.path
import shutil
import tempfile
import copy
import re

from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.core.files import File
from django.core.files.base import ContentFile
from django.core.urlresolvers import resolve
from django.db import transaction

from django.test import TestCase, skipIfDBFeature
from rest_framework import status
from rest_framework.reverse import reverse
from rest_framework.test import force_authenticate

from constants import datatypes
from kive.tests import BaseTestCases, install_fixture_files
import librarian.models
from metadata.models import CompoundDatatype, Datatype, everyone_group, kive_user
import metadata.tests
from method.ajax import MethodViewSet, MethodFamilyViewSet, DockerImageViewSet
from method.models import CodeResource, CodeResourceRevision, \
    Method, MethodFamily, MethodDependency, DockerImage
from method.serializers import CodeResourceRevisionSerializer, MethodSerializer
import kive.testing_utils as tools
from fleet.workers import Manager
from portal.utils import update_all_contenttypes


# This was previously defined here but has been moved to metadata.tests.
samplecode_path = metadata.tests.samplecode_path


@skipIfDBFeature('is_mocked')
class FileAccessTests(TestCase):
    serialized_rollback = True

    def setUp(self):
        tools.fd_count("FDs (start)")

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
        tools.clean_up_all_files()
        tools.fd_count("FDs (end)")
        update_all_contenttypes(verbosity=0)

    def test_close_save(self):
        with open(os.path.join(samplecode_path, self.fn), "rb") as f:
            tools.fd_count("!close->save")

            test_crr = CodeResourceRevision(
                coderesource=self.test_cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f),
                user=self.user_randy)

        with transaction.atomic():
            self.assertRaises(ValueError, test_crr.save)

    def test_access_close_save(self):
        with open(os.path.join(samplecode_path, self.fn), "rb") as f:
            test_crr = CodeResourceRevision(
                coderesource=self.test_cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f),
                user=self.user_randy)

            tools.fd_count("!access->close->save")
            test_crr.content_file.read()
            tools.fd_count("access-!>close->save")
        tools.fd_count("access->close-!>save")
        with transaction.atomic():
            self.assertRaises(ValueError, test_crr.save)
        tools.fd_count("access->close->save!")

    def test_close_access_save(self):
        with open(os.path.join(samplecode_path, self.fn), "rb") as f:
            test_crr = CodeResourceRevision(
                coderesource=self.test_cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f),
                user=self.user_randy)

        with transaction.atomic():
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
        tools.fd_count("save->close->access")

    def test_save_close_access_close(self):
        with open(os.path.join(samplecode_path, self.fn), "rb") as f:
            tools.fd_count("open-!>File->save->close->access->close")
            test_crr = CodeResourceRevision(
                coderesource=self.test_cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f),
                user=self.user_randy)
            tools.fd_count("open->File-!>save->close->access->close")
            test_crr.save()
            tools.fd_count("open->File->save-!>close->access->close")

        tools.fd_count("open->File->save->close-!>access->close")
        test_crr.content_file.read()
        tools.fd_count("open->File->save->close->access-!>close")
        test_crr.content_file.close()
        tools.fd_count("open->File->save->close->access->close!")

    def test_save_close_clean_close(self):
        with open(os.path.join(samplecode_path, self.fn), "rb") as f:
            # Compute the reference MD5
            md5gen = hashlib.md5()
            md5gen.update(f.read())
            f_checksum = md5gen.hexdigest()
            f.seek(0)

            tools.fd_count("open-!>File->save->close->clean->close")
            test_crr = CodeResourceRevision(
                coderesource=self.test_cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f),
                MD5_checksum=f_checksum,
                user=self.user_randy)

            tools.fd_count("open->File-!>save->close->clean->close")
            test_crr.save()
            tools.fd_count("open->File->save-!>close->clean->close")

        tools.fd_count("open->File->save->close-!>clean->close")
        test_crr.clean()
        tools.fd_count("open->File->save->close->clean-!>close")
        test_crr.content_file.close()
        tools.fd_count("open->File->save->close->clean->close!")

    def test_clean_save_close(self):
        with open(os.path.join(samplecode_path, self.fn), "rb") as f:
            tools.fd_count("open-!>File->clean->save->close")
            test_crr = CodeResourceRevision(
                coderesource=self.test_cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f),
                user=self.user_randy)
            tools.fd_count("open->File-!>clean->save->close")
            test_crr.clean()
            tools.fd_count("open->File->clean-!>save->close")
            test_crr.save()
            tools.fd_count("open->File->clean->save-!>close")
        tools.fd_count("open->File->clean->save->close!")

    def test_clean_save_close_clean_close(self):
        with open(os.path.join(samplecode_path, self.fn), "rb") as f:

            tools.fd_count("open-!>File->clean->save->close->clean->close")
            test_crr = CodeResourceRevision(
                coderesource=self.test_cr,
                revision_name="v1",
                revision_desc="First version",
                content_file=File(f),
                user=self.user_randy)
            tools.fd_count("open->File-!>clean->save->close->clean->close")
            tools.fd_count_logger.debug("FieldFile is open: {}".format(not test_crr.content_file.closed))
            test_crr.clean()
            tools.fd_count("open->File->clean-!>save->close->clean->close")
            tools.fd_count_logger.debug("FieldFile is open: {}".format(not test_crr.content_file.closed))
            test_crr.save()
            tools.fd_count("open->File->clean->save-!>close->clean->close")
            tools.fd_count_logger.debug("FieldFile is open: {}".format(not test_crr.content_file.closed))

        tools.fd_count("open->File->clean->save->close-!>clean->close")
        tools.fd_count_logger.debug("FieldFile is open: {}".format(not test_crr.content_file.closed))
        test_crr.clean()
        tools.fd_count("open->File->clean->save->close->clean-!>close")
        tools.fd_count_logger.debug("FieldFile is open: {}".format(not test_crr.content_file.closed))
        test_crr.content_file.close()
        tools.fd_count("open->File->clean->save->close->clean->close!")
        tools.fd_count_logger.debug("FieldFile is open: {}".format(not test_crr.content_file.closed))


@skipIfDBFeature('is_mocked')
class MethodTestCase(TestCase, object):
    """
    Set up a database state for unit testing.

    This sets up all the stuff used in the Metadata tests, as well as some of the Datatypes
    and CDTs we use here.
    """
    def setUp(self):
        """Set up default database state for Method unit testing."""
        tools.create_method_test_environment(self)

    def tearDown(self):
        tools.destroy_method_test_environment(self)


class CodeResourceTests(MethodTestCase):

    def setUp(self):
        super(CodeResourceTests, self).setUp()
        self.cr_filename_err_msg = ('Filename must contain only: alphanumeric characters; spaces; '
                                    'and the characters -._(), '
                                    'and cannot start with a space')

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
        self.assertRaisesRegexp(
            ValidationError,
            re.escape(self.cr_filename_err_msg),
            invalid_cr.clean_fields
        )

    def test_invalid_name_starting_space_clean_bad(self):
        """
        Clean fails when CodeResource name isn't file-system valid
        """
        invalid_cr = CodeResource(name="test", filename=" test.py", description="desc", user=self.myUser)
        invalid_cr.save()
        self.assertRaisesRegexp(
            ValidationError,
            re.escape(self.cr_filename_err_msg),
            invalid_cr.clean_fields
        )

    def test_invalid_name_invalid_symbol_clean_bad(self):
        """
        Clean fails when CodeResource name isn't file-system valid
        """
        invalid_cr = CodeResource(name="name", filename="test$.py", description="desc", user=self.myUser)
        invalid_cr.save()
        self.assertRaisesRegexp(
            ValidationError,
            re.escape(self.cr_filename_err_msg),
            invalid_cr.clean_fields
        )

    def test_invalid_name_trailing_space_clean_bad(self):
        """
        Clean fails when CodeResource name isn't file-system valid
        """
        invalid_cr = CodeResource(name="name", filename="test.py ", description="desc", user=self.myUser)
        invalid_cr.save()
        self.assertRaisesRegexp(
            ValidationError,
            re.escape(self.cr_filename_err_msg),
            invalid_cr.clean_fields
        )


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
        self.assertEquals(unicode(no_cr_set), "[no code resource name]:[no revision number] ([no revision name])")

        # Define a crRev without a linking cr, with a revision_name of foo
        no_cr_set.revision_name = "foo"
        self.assertEquals(unicode(no_cr_set), "[no code resource name]:[no revision number] (foo)")

    def test_clean_valid_MD5(self):
        """
        An MD5 should exist.
        """
        # Compute the reference MD5
        md5gen = hashlib.md5()
        with open(os.path.join(samplecode_path, "complement.py"), "rb") as f:
            md5gen.update(f.read())

        # Revision should have the correct MD5 checksum
        self.assertEquals(md5gen.hexdigest(), self.comp_cr.revisions.get(revision_name="v1").MD5_checksum)

    def test_find_update_not_found(self):
        update = self.compv2_crRev.find_update()

        self.assertEqual(update, None)

    def test_find_update(self):
        update = self.compv1_crRev.find_update()

        self.assertEqual(update, self.compv2_crRev)


class MethodDependencyTests(MethodTestCase):
    def setUp(self):
        super(MethodDependencyTests, self).setUp()

        # self.DNAcompv1_m is defined in the setup, based on v1 of self.comp_cr:
        # family name: "DNAcomplement"
        # revision_name: "v1"
        # revision_desc: "First version"
        # driver: v1 of self.comp_cr
        # v2 is a revision of comp_cr such that revision_name = v2.
        self.v2 = self.comp_cr.revisions.get(revision_name="v2")

    def test_unicode(self):
        """
        Unicode of MethodDependency should return:
        <self.method> requires <referenced CRR> as <filePath>
        """
        # Define a fake dependency where the method requires v2 in subdir/foo.py
        test_dep = MethodDependency(method=self.DNAcompv1_m,
                                    requirement=self.v2,
                                    path="subdir",
                                    filename="foo.py")

        # Display unicode for this dependency under valid conditions
        self.assertEquals(
            unicode(test_dep),
            "DNAcomplement DNAcomplement:1 (v1) requires complement complement:2 (v2) as subdir/foo.py")

    def test_invalid_dotdot_path_clean(self):
        """
        Dependency tries to go into a path outside its sandbox.
        """
        bad_dep = MethodDependency(method=self.DNAcompv1_m,
                                   requirement=self.v2,
                                   path="..",
                                   filename="foo.py")
        self.assertRaisesRegexp(
            ValidationError,
            "path cannot reference \.\./",
            bad_dep.clean)

        bad_dep_2 = MethodDependency(method=self.DNAcompv1_m,
                                     requirement=self.v2,
                                     path="../test",
                                     filename="foo.py")
        self.assertRaisesRegexp(
            ValidationError,
            "path cannot reference \.\./",
            bad_dep_2.clean)

    def test_valid_path_with_dotdot_clean(self):
        """
        Dependency goes into a path with a directory containing ".." in the name.
        """
        v2 = self.comp_cr.revisions.get(revision_name="v2")

        good_md = MethodDependency(
            method=self.DNAcompv1_m,
            requirement=v2,
            path="..bar",
            filename="foo.py"
        )
        self.assertEquals(good_md.clean(), None)

        good_md_2 = MethodDependency(
            method=self.DNAcompv1_m,
            requirement=v2,
            path="bar..",
            filename="foo.py"
        )
        self.assertEquals(good_md_2.clean(), None)

        good_md_3 = MethodDependency(
            method=self.DNAcompv1_m,
            requirement=v2,
            path="baz/bar..",
            filename="foo.py"
        )
        self.assertEquals(good_md_3.clean(), None)

        good_md_4 = MethodDependency(
            method=self.DNAcompv1_m,
            requirement=v2,
            path="baz/..bar",
            filename="foo.py"
        )
        self.assertEquals(good_md_4.clean(), None)

        good_md_5 = MethodDependency(
            method=self.DNAcompv1_m,
            requirement=v2,
            path="baz/..bar..",
            filename="foo.py"
        )
        self.assertEquals(good_md_5.clean(), None)

        good_md_6 = MethodDependency(
            method=self.DNAcompv1_m,
            requirement=v2,
            path="..baz/bar..",
            filename="foo.py"
        )
        self.assertEquals(good_md_6.clean(), None)

        # This case works because the ".." doesn't take us out of the sandbox
        good_md_7 = MethodDependency(
            method=self.DNAcompv1_m,
            requirement=v2,
            path="baz/../bar",
            filename="foo.py"
        )
        self.assertEquals(good_md_7.clean(), None)

        good_md_8 = MethodDependency(
            method=self.DNAcompv1_m,
            requirement=v2,
            path="baz/..bar../blah",
            filename="foo.py")
        self.assertEquals(good_md_8.clean(), None)

    def test_method_dependency_with_good_path_and_filename_clean(self):
        """
        Test a MethodDependency with no problems.
        """
        v2 = self.comp_cr.revisions.get(revision_name="v2")

        # Define a MethodDependency for self.DNAcompv1_m with good paths and filenames.
        good_md = MethodDependency(
            method=self.DNAcompv1_m,
            requirement=v2,
            path="testFolder/anotherFolder",
            filename="foo.py")
        self.assertEqual(good_md.clean(), None)


class MethodInstallTests(MethodTestCase):
    """Tests of the install function of Method."""
    def setUp(self):
        super(MethodInstallTests, self).setUp()

        # This method is defined in testing_utils.create_method_test_environment.
        # It has self.compv1_crRev as its driver, and no dependencies.
        self.independent_method = self.DNAcompv1_m

        # This method is defined in testing_utils.create_method_test_environment.
        # It has self.compv2_crRev as its driver and dna_resource_revision (not
        # bound to self) as a dependency.
        self.dependant_method = self.DNAcompv2_m

    def test_base_case(self):
        """
        Test of base case -- installing a Method with no dependencies.
        """
        test_path = tempfile.mkdtemp(prefix="test_base_case")

        self.independent_method.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))

        shutil.rmtree(test_path)

    def test_second_revision(self):
        """
        Test of base case -- installing a Method that is a second revision.

        This Method does have a dependency.
        """
        test_path = tempfile.mkdtemp(prefix="test_base_case")

        self.dependant_method.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "good_dna.csv")))

        shutil.rmtree(test_path)

    def test_dependency_same_dir_dot(self):
        """
        Test of installing a Method with a dependency in the same directory, specified using a dot.
        """
        test_path = tempfile.mkdtemp(prefix="test_dependency_same_dir_dot")

        self.independent_method.dependencies.create(requirement=self.test_cr_1_rev1, path=".")
        self.independent_method.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "test_cr_1.py")))

        shutil.rmtree(test_path)

    def test_dependency_same_dir_blank(self):
        """
        Test of installing a Method with a dependency in the same directory, specified using a blank.
        """
        test_path = tempfile.mkdtemp(prefix="test_dependency_same_dir_blank")

        self.independent_method.dependencies.create(requirement=self.test_cr_1_rev1, path="")
        self.independent_method.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "test_cr_1.py")))

        shutil.rmtree(test_path)

    def test_dependency_override_dep_filename(self):
        """
        Test of installing a Method with a dependency whose filename is overridden.
        """
        test_path = tempfile.mkdtemp(prefix="test_dependency_override_dep_filename")

        self.independent_method.dependencies.create(requirement=self.test_cr_1_rev1, path="",
                                                    filename="foo.py")
        self.independent_method.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "foo.py")))
        self.assertFalse(os.path.exists(os.path.join(test_path, "test_cr_1.py")))

        shutil.rmtree(test_path)

    def test_dependency_in_subdirectory(self):
        """
        Test of installing a Method with a dependency in a subdirectory.
        """
        test_path = tempfile.mkdtemp(prefix="test_dependency_in_subdirectory")

        self.independent_method.dependencies.create(requirement=self.test_cr_1_rev1, path="modules")
        self.independent_method.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))
        self.assertTrue(os.path.isdir(os.path.join(test_path, "modules")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "modules", "test_cr_1.py")))

        shutil.rmtree(test_path)

    def test_dependencies_in_same_subdirectory(self):
        """
        Test of installing a Method with several dependencies in the same subdirectory.
        """
        test_path = tempfile.mkdtemp(prefix="test_dependencies_in_same_subdirectory")

        self.independent_method.dependencies.create(requirement=self.test_cr_1_rev1, path="modules")
        self.independent_method.dependencies.create(requirement=self.test_cr_2_rev1, path="modules")
        self.independent_method.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))
        self.assertTrue(os.path.isdir(os.path.join(test_path, "modules")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "modules", "test_cr_1.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "modules", "test_cr_2.py")))

        shutil.rmtree(test_path)

    def test_dependencies_in_same_directory(self):
        """
        Test of installing a Method with several dependencies in the base directory.
        """
        test_path = tempfile.mkdtemp(prefix="test_dependencies_in_same_directory")

        self.independent_method.dependencies.create(requirement=self.test_cr_1_rev1, path="")
        self.independent_method.dependencies.create(requirement=self.test_cr_2_rev1, path="")
        self.independent_method.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "test_cr_1.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "test_cr_2.py")))

        shutil.rmtree(test_path)

    def test_dependencies_in_subsub_directory(self):
        """
        Test of installing a Method with dependencies in sub-sub-directories.
        """
        test_path = tempfile.mkdtemp(prefix="test_dependencies_in_subsub_directory")

        self.independent_method.dependencies.create(requirement=self.test_cr_1_rev1, path="modules/foo1")
        self.independent_method.dependencies.create(requirement=self.test_cr_2_rev1, path="modules/foo2")
        self.independent_method.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))
        self.assertTrue(os.path.isdir(os.path.join(test_path, "modules/foo1")))
        self.assertTrue(os.path.isdir(os.path.join(test_path, "modules/foo2")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "modules", "foo1", "test_cr_1.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "modules", "foo2", "test_cr_2.py")))

        shutil.rmtree(test_path)

    def test_dependencies_from_same_coderesource_same_dir(self):
        """
        Test of installing a Method with a dependency having the same CodeResource in the same directory.
        """
        test_path = tempfile.mkdtemp(prefix="test_dependencies_from_same_coderesource_same_dir")

        self.independent_method.dependencies.create(requirement=self.compv2_crRev, path="", filename="foo.py")
        self.independent_method.install(test_path)
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
        Test of installing a Method with dependencies in several places.
        """
        test_path = tempfile.mkdtemp(prefix="test_dependencies_in_various_places")

        self.independent_method.dependencies.create(requirement=self.test_cr_1_rev1, path="modules")
        self.independent_method.dependencies.create(requirement=self.test_cr_2_rev1, path="moremodules")
        self.independent_method.dependencies.create(requirement=self.test_cr_3_rev1, path="modules/foo")
        self.independent_method.install(test_path)
        self.assertTrue(os.path.exists(os.path.join(test_path, "complement.py")))
        self.assertTrue(os.path.isdir(os.path.join(test_path, "modules")))
        self.assertTrue(os.path.isdir(os.path.join(test_path, "moremodules")))
        self.assertTrue(os.path.isdir(os.path.join(test_path, "modules", "foo")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "modules", "test_cr_1.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "moremodules", "test_cr_2.py")))
        self.assertTrue(os.path.exists(os.path.join(test_path, "modules", "foo", "test_cr_3.py")))

        shutil.rmtree(test_path)


class MethodTests(MethodTestCase):

    def setUp(self):
        super(MethodTests, self).setUp()
        self.test_dep_method = tools.make_first_method(
            "TestMethodDependencies",
            "Methods with dependencies",
            self.test_cr_1_rev1,
            self.myUser
        )
        tools.simple_method_io(self.test_dep_method, None, "dummy_in", "dummy_out")

    def test_delete_method(self):
        """Deleting a method is possible."""
        Method.objects.first().delete()

    def test_create(self):
        """Create a new Method by the constructor."""
        names = ["a", "b"]
        cdts = CompoundDatatype.objects.all()[:2]
        family = MethodFamily.objects.first()
        driver = CodeResourceRevision.objects.first()
        m = Method.create(names, compounddatatypes=cdts, num_inputs=1, family=family, driver=driver, user=self.myUser)
        self.assertIsNone(m.complete_clean())


class MethodFamilyTests(MethodTestCase):

    def test_unicode(self):
        """
        unicode() for MethodFamily should display it's name.
        """

        self.assertEqual(unicode(self.DNAcomp_mf), "DNAcomplement")


@skipIfDBFeature('is_mocked')
class NonReusableMethodTests(TestCase):
    def setUp(self):
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
        self.test_nonreusable.steps.create(
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
        self.numbers_dataset = librarian.models.Dataset.create_dataset(
            datafile.name,
            user=self.user_rob,
            groups_allowed=[everyone_group()],
            cdt=self.increment_in_1_cdt, keep_file=True,
            name="numbers", description="1-2-3-4"
        )

    def tearDown(self):
        # Our tests fail post-teardown without this.
        update_all_contenttypes(verbosity=0)

    def test_find_compatible_ER_non_reusable_method(self):
        """
        The ExecRecord of a non-reusable Method should not be found compatible.
        """
        Manager.execute_pipeline(self.user_rob, self.test_nonreusable, [self.numbers_dataset])

        rng_step = self.test_nonreusable.steps.get(step_num=1)
        runstep = rng_step.pipelinestep_instances.first()
        self.assertListEqual(list(runstep.find_compatible_ERs([])), [])


class MethodFamilyApiMockTests(BaseTestCases.ApiTestCase):
    def setUp(self):
        self.mock_viewset(MethodFamilyViewSet)
        super(MethodFamilyApiMockTests, self).setUp()

        self.list_path = reverse("methodfamily-list")
        self.detail_pk = 43
        self.detail_path = reverse("methodfamily-detail",
                                   kwargs={'pk': self.detail_pk})
        self.removal_path = reverse("methodfamily-removal-plan",
                                    kwargs={'pk': self.detail_pk})

        self.list_view, _, _ = resolve(self.list_path)
        self.detail_view, _, _ = resolve(self.detail_path)
        self.removal_view, _, _ = resolve(self.removal_path)

        MethodFamily.objects.add(MethodFamily(pk=42,
                                              user=self.kive_kive_user,
                                              name='mA_name turnip',
                                              description='A_desc'),
                                 MethodFamily(pk=43,
                                              user=self.kive_kive_user,
                                              name='mB_name',
                                              description='B_desc'),
                                 MethodFamily(pk=44,
                                              user=self.kive_kive_user,
                                              name='mC_name',
                                              description='C_desc turnip'))

    def test_list(self):
        """
        Test the API list view.
        """
        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 3)
        self.assertEquals(response.data[2]['name'], 'mC_name')

    def test_detail(self):
        request = self.factory.get(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.data['name'], 'mB_name')

    def test_removal_plan(self):
        request = self.factory.get(self.removal_path)
        force_authenticate(request, user=self.kive_user)
        response = self.removal_view(request, pk=self.detail_pk)
        self.assertEquals(response.data['MethodFamilies'], 1)

    def test_filter_name(self):
        request = self.factory.get(
            self.list_path + "?filters[0][key]=name&filters[0][val]=turnip")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['name'], 'mA_name turnip')

    def test_filter_description(self):
        request = self.factory.get(
            self.list_path + "?filters[0][key]=description&filters[0][val]=B_desc")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['name'], 'mB_name')

    def test_filter_smart(self):
        request = self.factory.get(
            self.list_path + "?filters[0][key]=smart&filters[0][val]=turnip")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 2)
        self.assertEquals(response.data[0]['name'], 'mA_name turnip')
        self.assertEquals(response.data[1]['description'], 'C_desc turnip')

    def test_filter_user(self):
        request = self.factory.get(
            self.list_path + "?filters[0][key]=user&filters[0][val]=kive")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 3)

    def test_filter_unknown(self):
        request = self.factory.get(
            self.list_path + "?filters[0][key]=bogus&filters[0][val]=kive")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals({u'detail': u'Unknown filter key: bogus'},
                          response.data)


@skipIfDBFeature('is_mocked')
class MethodFamilyApiTests(BaseTestCases.ApiTestCase):
    fixtures = ['demo']

    def setUp(self):
        super(MethodFamilyApiTests, self).setUp()

        self.list_path = reverse("methodfamily-list")
        self.detail_pk = MethodFamily.objects.get(name='sums_and_products').pk

        # This should equal metadata.ajax.CompoundDatatypeViewSet.as_view({"get": "list"}).
        self.list_view, _, _ = resolve(self.list_path)

    def add_dependency(self, family, name):
        driver = CodeResourceRevision.objects.first()
        method = family.members.create(user=family.user,
                                       driver=driver)
        code_resource = CodeResource.objects.create(name=name,
                                                    user=family.user)
        revision = code_resource.revisions.create(user=family.user)
        MethodDependency.objects.create(method=method, requirement=revision)

    def test_filter_code_dependency(self):
        target_family = MethodFamily.objects.get(pk=self.detail_pk)
        other_family = MethodFamily.objects.exclude(pk=self.detail_pk).first()
        self.add_dependency(target_family, 'target_code')
        self.add_dependency(other_family, 'other_code')

        request = self.factory.get(
            self.list_path + "?filters[0][key]=code&filters[0][val]=target")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['name'], 'sums_and_products')

    def add_driver(self, family, name):
        code_resource = CodeResource.objects.create(name=name,
                                                    user=family.user)
        driver = code_resource.revisions.create(user=family.user)
        family.members.create(user=family.user, driver=driver)

    def test_filter_code_driver(self):
        target_family = MethodFamily.objects.get(pk=self.detail_pk)
        other_family = MethodFamily.objects.exclude(pk=self.detail_pk).first()
        self.add_driver(target_family, 'target_code')
        self.add_driver(target_family, 'target2_code')  # test dups
        self.add_driver(other_family, 'other_code')

        request = self.factory.get(
            self.list_path + "?filters[0][key]=code&filters[0][val]=target")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['name'], 'sums_and_products')


@skipIfDBFeature('is_mocked')
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
        self.assertRegexpMatches(
            detail["absolute_url"],
            "/resource_revisions/{}/?".format(self.noop_cr.pk)
        )

    def test_removal_plan(self):
        cr_removal_path = reverse("coderesource-removal-plan", kwargs={'pk': self.noop_cr.pk})
        cr_removal_view, _, _ = resolve(cr_removal_path)

        request = self.factory.get(cr_removal_path)
        force_authenticate(request, user=self.remover)
        response = cr_removal_view(request, pk=self.noop_cr.pk)

        for key in self.removal_plan:
            self.assertEquals(response.data[key], len(self.removal_plan[key]))
        self.assertEquals(response.data['CodeResources'], 1)
        self.assertEquals(response.data["CodeResourceRevisions"], 1)

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
        self.assertEquals(end_crr_count, start_crr_count - len(self.removal_plan["CodeResourceRevisions"]))

    def test_revisions(self):
        cr_revisions_path = reverse("coderesource-revisions", kwargs={"pk": self.noop_cr.pk})
        cr_revisions_view, _, _ = resolve(cr_revisions_path)

        request = self.factory.get(cr_revisions_path)
        force_authenticate(request, user=self.remover)
        response = cr_revisions_view(request, pk=self.noop_cr.pk)

        self.assertSetEqual(set([x.revision_number for x in self.noop_cr.revisions.all()]),
                            set([x["revision_number"] for x in response.data]))


def crr_test_setup(case):
    """
    A helper for CodeResourceRevisionApiTests and CodeResourceRevisionSerializerTests.
    """
    # An innocent bystander.
    case.innocent_bystander = User.objects.create_user(
        "InnocentBystander", "innocent_bystander_1@aol.net", password="WhoMe?")

    # A mock request that we pass as context to our serializer.
    class DuckRequest(object):
        pass

    case.duck_request = DuckRequest()
    case.duck_request.user = kive_user()
    case.duck_context = {"request": case.duck_request}

    case.cr_name = "Deserialization Test Family"
    case.cr = CodeResource(name=case.cr_name,
                           filename="HelloWorld.py",
                           description="Hello World",
                           user=kive_user())
    case.cr.save()
    case.cr.grant_everyone_access()

    case.hello_world = """#!/bin/bash

echo "Hello World"
"""
    case.hello_world_file = ContentFile(case.hello_world)

    # Note: we have to add content_file on the fly so we can wrap it in a Django File.
    case.crr_data = {
        "coderesource": case.cr_name,
        "revision_name": "v1",
        "revision_desc": "First version",
        "content_file": case.hello_world_file,
        "groups_allowed": [everyone_group().name]
    }

    # In many situations below, the ContentFile object doesn't work with the DRF serializers.
    # In those situations, we'll open the file in the test and insert it into case.crr_data
    # so that we don't have an open file handle being passed around.
    case.hello_world_fd, case.hello_world_filename = tempfile.mkstemp()
    with os.fdopen(case.hello_world_fd, "wb") as f:
        f.write(case.hello_world)

    case.crr_dependency = """language = ENG"""
    case.crr_dependency_file = ContentFile(case.crr_dependency)
    case.crd = CodeResourceRevision(
        coderesource=case.cr,
        revision_name="dependency",
        revision_desc="Dependency",
        user=kive_user(),
        content_file=case.crr_dependency_file
    )
    case.crd.clean()
    case.crd.save()
    case.crd.grant_everyone_access()


@skipIfDBFeature('is_mocked')
class CodeResourceRevisionSerializerTests(TestCase):
    fixtures = ["removal"]

    def setUp(self):
        install_fixture_files("removal")
        # This user is defined in the removal fixture.
        self.remover = User.objects.get(username="RemOver")
        # noinspection PyTypeChecker
        crr_test_setup(self)

    def tearDown(self):
        tools.clean_up_all_files()

    # Note: all validation tests are redundant.  There is no customized validation code anymore.
    def test_validate(self):
        """
        Test validation of a CodeResourceRevision with no dependencies.
        """
        with open(self.hello_world_filename, "rb") as f:
            self.crr_data["content_file"] = File(f)
            crr_s = CodeResourceRevisionSerializer(
                data=self.crr_data,
                context={"request": self.duck_request}
            )
            self.assertTrue(crr_s.is_valid())

    def test_create(self):
        """
        Test creation of a CodeResourceRevision with no dependencies.
        """
        with open(self.hello_world_filename, "rb") as f:
            self.crr_data["content_file"] = File(f)
            crr_s = CodeResourceRevisionSerializer(
                data=self.crr_data,
                context={"request": self.duck_request}
            )
            crr_s.is_valid()
            crr_s.save()

        # Inspect the revision we just added.
        new_crr = self.cr.revisions.get(revision_name="v1")
        self.assertEquals(new_crr.revision_desc, "First version")


@skipIfDBFeature('is_mocked')
class CodeResourceRevisionApiTests(BaseTestCases.ApiTestCase):
    fixtures = ["removal"]

    def setUp(self):
        install_fixture_files("removal")
        # This user is defined in the removal fixture.
        self.remover = User.objects.get(username="RemOver")
        super(CodeResourceRevisionApiTests, self).setUp()

        self.list_path = reverse("coderesourcerevision-list")
        self.list_view, _, _ = resolve(self.list_path)

        self.noop_cr = CodeResource.objects.get(name="Noop")
        self.noop_crr = self.noop_cr.revisions.get(revision_number=1)

        self.detail_path = reverse("coderesourcerevision-detail", kwargs={"pk": self.noop_crr.pk})
        self.detail_view, _, _ = resolve(self.detail_path)

        self.removal_plan = self.noop_crr.build_removal_plan()

        self.download_path = reverse("coderesourcerevision-download", kwargs={"pk": self.noop_crr.pk})
        self.download_view, _, _ = resolve(self.download_path)

        # noinspection PyTypeChecker
        crr_test_setup(self)

    def tearDown(self):
        tools.clean_up_all_files()
        os.remove(self.hello_world_filename)

    def test_list(self):
        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.remover)
        response = self.list_view(request, pk=None)

        self.assertItemsEqual(
            [x.pk for x in CodeResourceRevision.filter_by_user(user=self.remover)],
            [x["id"] for x in response.data]
        )

    def test_detail(self):
        request = self.factory.get(self.detail_path)
        force_authenticate(request, user=self.remover)
        response = self.detail_view(request, pk=self.noop_crr.pk)
        detail = response.data

        self.assertEquals(detail["id"], self.noop_crr.pk)
        self.assertRegexpMatches(
            detail["absolute_url"],
            "/resource_revision_add/{}/?".format(self.noop_crr.pk)
        )
        self.assertEquals(detail["revision_name"], self.noop_crr.revision_name)

    def test_removal_plan(self):
        crr_removal_path = reverse("coderesourcerevision-removal-plan", kwargs={'pk': self.noop_crr.pk})
        crr_removal_view, _, _ = resolve(crr_removal_path)

        request = self.factory.get(crr_removal_path)
        force_authenticate(request, user=self.remover)
        response = crr_removal_view(request, pk=self.noop_crr.pk)

        for key in self.removal_plan:
            self.assertEquals(response.data[key], len(self.removal_plan[key]))
        self.assertEquals(response.data["CodeResourceRevisions"], 1)

    def test_removal(self):
        start_count = CodeResourceRevision.objects.count()

        request = self.factory.delete(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.noop_crr.pk)
        self.assertEquals(response.status_code, status.HTTP_204_NO_CONTENT)

        end_count = CodeResourceRevision.objects.count()
        # In the above we confirmed this length is 1.
        self.assertEquals(end_count, start_count - len(self.removal_plan["CodeResourceRevisions"]))

    def test_create(self):
        """
        Test creation of a new CodeResourceRevision via the API.
        """
        with open(self.hello_world_filename, "rb") as f:
            self.crr_data["content_file"] = f
            request = self.factory.post(self.list_path, self.crr_data)
            force_authenticate(request, user=kive_user())
            self.list_view(request)

        # Inspect the revision we just added.
        new_crr = self.cr.revisions.get(revision_name="v1")
        self.assertEquals(new_crr.revision_desc, "First version")

    def test_create_clean_fails(self):
        """
        Test that clean is being called during creation.
        """
        # Disallow everyone from accessing self.cr, which will cause clean to fail.
        self.cr.groups_allowed.remove(everyone_group())

        with open(self.hello_world_filename, "rb") as f:
            self.crr_data["content_file"] = f
            request = self.factory.post(self.list_path, self.crr_data)
            force_authenticate(request, user=kive_user())
            response = self.list_view(request)

        self.assertDictEqual(
            response.data,
            {'non_field_errors': "Group(s) Everyone cannot be granted access"}
        )

    def test_download(self):
        request = self.factory.get(self.download_path)
        force_authenticate(request, user=self.remover)
        response = self.download_view(request, pk=self.noop_crr.pk)

        self.assertIn("Content-Disposition", response)
        self.assertTrue(response["Content-Disposition"].startswith("attachment; filename="))


def method_test_setup(case):
    """
    Helper to set up MethodSerializerTests and MethodApiTests.
    """
    crr_test_setup(case)

    # We need a CodeResourceRevision to create a Method from.
    with open(case.hello_world_filename, "rb") as f:
        case.crr_data["content_file"] = File(f)
        crr_s = CodeResourceRevisionSerializer(data=case.crr_data, context=case.duck_context)
        crr_s.is_valid()
        case.crr = crr_s.save()

    # We need a MethodFamily to add the Method to.
    case.dtf_mf = MethodFamily(
        name="Deserialization Test Family Methods",
        description="For testing the Method serializer.",
        user=kive_user()
    )
    case.dtf_mf.save()
    case.dtf_mf.users_allowed.add(case.innocent_bystander)
    case.dtf_mf.grant_everyone_access()
    case.dtf_mf.save()

    case.method_data = {
        "family": case.dtf_mf.name,
        "revision_name": "v1",
        "revision_desc": "First version",
        "users_allowed": [case.innocent_bystander.username],
        "groups_allowed": [everyone_group().name],
        "driver": case.crr.pk,
        "inputs": [
            {
                "dataset_name": "ignored_input",
                "dataset_idx": 1,
                "x": 0.1,
                "y": 0.1
            },
            {
                "dataset_name": "another_ignored_input",
                "dataset_idx": 2,
                "x": 0.1,
                "y": 0.2
            }
        ],
        "outputs": [
            {
                "dataset_name": "empty_output",
                "dataset_idx": 1
            }
        ]
    }

    case.method_data_with_dep = copy.deepcopy(case.method_data)
    case.method_data_with_dep["revision_name"] = "v2"
    case.method_data_with_dep["revision_desc"] = "Has dependencies"
    case.method_data_with_dep["dependencies"] = [
        {
            "requirement": case.crd.pk,
            "filename": "config.dat"
        },
        {
            "requirement": case.crd.pk,
            "path": "configuration.dat",
            "filename": "config_2.dat"
        }
    ]


@skipIfDBFeature('is_mocked')
class MethodSerializerTests(TestCase):
    fixtures = ["removal"]

    def setUp(self):
        install_fixture_files("removal")
        # noinspection PyTypeChecker
        method_test_setup(self)

    def tearDown(self):
        tools.clean_up_all_files()

    def test_create(self):
        """
        Test creation of a Method using the serializer.
        """
        method_s = MethodSerializer(data=self.method_data, context=self.duck_context)
        self.assertTrue(method_s.is_valid())
        new_method = method_s.save()

        # Probe the new method to see that it got created correctly.
        self.assertEquals(new_method.inputs.count(), 2)
        in_1 = new_method.inputs.get(dataset_idx=1)
        in_2 = new_method.inputs.get(dataset_idx=2)

        self.assertEquals(in_1.dataset_name, "ignored_input")
        self.assertEquals(in_2.dataset_name, "another_ignored_input")

        self.assertEquals(new_method.outputs.count(), 1)
        self.assertEquals(new_method.outputs.first().dataset_name, "empty_output")

    def test_create_with_dep(self):
        """
        Test creation of a Method with dependencies.
        """
        method_s = MethodSerializer(data=self.method_data_with_dep, context=self.duck_context)
        self.assertTrue(method_s.is_valid())
        new_method = method_s.save()

        # Inspect the revision we just added.
        self.assertEquals(new_method.revision_name, "v2")
        self.assertEquals(new_method.revision_desc, "Has dependencies")
        self.assertEquals(new_method.dependencies.count(), 2)

        new_dep = new_method.dependencies.get(filename="config.dat")
        self.assertEquals(new_dep.requirement, self.crd)
        self.assertEquals(new_dep.path, "")

        new_dep_2 = new_method.dependencies.get(filename="config_2.dat")
        self.assertEquals(new_dep_2.requirement, self.crd)
        self.assertEquals(new_dep_2.path, "configuration.dat")


@skipIfDBFeature('is_mocked')
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

        self.list_view, _, _ = resolve(self.list_path)
        self.detail_view, _, _ = resolve(self.detail_path)
        self.removal_view, _, _ = resolve(self.removal_path)

        # noinspection PyTypeChecker
        method_test_setup(self)

    def test_removal(self):
        start_count = Method.objects.all().count()

        request = self.factory.delete(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)

        self.assertEquals(response.status_code, status.HTTP_204_NO_CONTENT)

        end_count = Method.objects.all().count()
        self.assertEquals(end_count, start_count - 1)

    def test_create(self):
        request = self.factory.post(self.list_path, self.method_data, format="json")
        force_authenticate(request, user=self.kive_user)
        self.list_view(request)

        # Probe the resulting method.
        new_method = self.dtf_mf.members.get(revision_name=self.method_data["revision_name"])

        self.assertEquals(new_method.inputs.count(), 2)
        self.assertEquals(new_method.outputs.count(), 1)


class MethodApiMockTests(BaseTestCases.ApiTestCase):
    def setUp(self):
        self.mock_viewset(MethodViewSet)
        super(MethodApiMockTests, self).setUp()

        self.list_path = reverse("method-list")
        self.detail_pk = 43
        self.detail_path = reverse("method-detail",
                                   kwargs={'pk': self.detail_pk})
        self.removal_path = reverse("method-removal-plan",
                                    kwargs={'pk': self.detail_pk})

        self.list_view, _, _ = resolve(self.list_path)
        self.detail_view, _, _ = resolve(self.detail_path)
        self.removal_view, _, _ = resolve(self.removal_path)

        Method.objects.add(Method(pk=42,
                                  user=self.kive_kive_user,
                                  revision_name='mA_name turnip',
                                  revision_desc='A_desc'),
                           Method(pk=43,
                                  user=self.kive_kive_user,
                                  revision_name='mB_name',
                                  revision_desc='B_desc'),
                           Method(pk=44,
                                  user=self.kive_kive_user,
                                  revision_name='mC_name',
                                  revision_desc='C_desc turnip'))

    def test_list(self):
        """
        Test the API list view.
        """
        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 3)
        self.assertEquals(response.data[2]['revision_name'], 'mC_name')

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

    def test_filter_description(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=description&filters[0][val]=B_desc")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['revision_name'], 'mB_name')

    def test_filter_smart(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=smart&filters[0][val]=turnip")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 2)
        self.assertEquals(response.data[0]['revision_name'], 'mA_name turnip')
        self.assertEquals(response.data[1]['revision_desc'], 'C_desc turnip')

    def test_filter_user(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=user&filters[0][val]=kive")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 3)

    def test_filter_unknown(self):
        """
        Test the API list view.
        """
        request = self.factory.get(
            self.list_path + "?filters[0][key]=bogus&filters[0][val]=kive")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals({u'detail': u'Unknown filter key: bogus'},
                          response.data)


class DockerImageApiMockTests(BaseTestCases.ApiTestCase):
    def setUp(self):
        self.mock_viewset(DockerImageViewSet)
        super(DockerImageApiMockTests, self).setUp()

        self.list_path = reverse("dockerimage-list")
        self.detail_pk = 43
        self.detail_path = reverse("dockerimage-detail",
                                   kwargs={'pk': self.detail_pk})
        self.removal_path = reverse("dockerimage-removal-plan",
                                    kwargs={'pk': self.detail_pk})

        self.list_view, _, _ = resolve(self.list_path)
        self.detail_view, _, _ = resolve(self.detail_path)
        self.removal_view, _, _ = resolve(self.removal_path)

        DockerImage.objects.add(DockerImage(pk=42,
                                            user=self.kive_kive_user,
                                            name='github/alex/hello',
                                            description='angling'),
                                DockerImage(pk=43,
                                            user=self.kive_kive_user,
                                            name='github/bob/hello',
                                            description='boxing',
                                            git='http://server1.com/hello.git',
                                            tag='v1.0'),
                                DockerImage(pk=44,
                                            user=self.kive_kive_user,
                                            name='github/cindy/hello',
                                            description='carving bob',
                                            git='http://server2.com/hello.git',
                                            tag='v2.0'))

    def test_list(self):
        """
        Test the API list view.
        """
        request = self.factory.get(self.list_path)
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 3)
        self.assertEquals(response.data[2]['name'], 'github/cindy/hello')

    def test_detail(self):
        request = self.factory.get(self.detail_path)
        force_authenticate(request, user=self.kive_user)
        response = self.detail_view(request, pk=self.detail_pk)
        self.assertEquals(response.data['name'], 'github/bob/hello')

    def test_removal_plan(self):
        request = self.factory.get(self.removal_path)
        force_authenticate(request, user=self.kive_user)
        response = self.removal_view(request, pk=self.detail_pk)
        self.assertEquals(response.data['DockerImages'], 1)

    def test_filter_description(self):
        request = self.factory.get(
            self.list_path + "?filters[0][key]=description&filters[0][val]=boxing")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['name'], 'github/bob/hello')

    def test_filter_smart(self):
        request = self.factory.get(
            self.list_path + "?filters[0][key]=smart&filters[0][val]=bob")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 2)
        self.assertEquals(response.data[0]['name'], 'github/bob/hello')
        self.assertEquals(response.data[1]['description'], 'carving bob')

    def test_filter_name(self):
        request = self.factory.get(
            self.list_path + "?filters[0][key]=name&filters[0][val]=bob")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['name'], 'github/bob/hello')

    def test_filter_tag(self):
        request = self.factory.get(
            self.list_path + "?filters[0][key]=tag&filters[0][val]=1.0")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['tag'], 'v1.0')

    def test_filter_git(self):
        request = self.factory.get(
            self.list_path + "?filters[0][key]=git&filters[0][val]=server1")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 1)
        self.assertEquals(response.data[0]['git'], 'http://server1.com/hello.git')

    def test_filter_user(self):
        request = self.factory.get(
            self.list_path + "?filters[0][key]=user&filters[0][val]=kive")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals(len(response.data), 3)

    def test_filter_unknown(self):
        request = self.factory.get(
            self.list_path + "?filters[0][key]=bogus&filters[0][val]=kive")
        force_authenticate(request, user=self.kive_user)
        response = self.list_view(request, pk=None)

        self.assertEquals({u'detail': u'Unknown filter key: bogus'},
                          response.data)
