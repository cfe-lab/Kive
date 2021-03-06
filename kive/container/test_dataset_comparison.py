import typing as ty
from unittest import mock

from django.core.files.base import ContentFile
from django.test import TestCase

from librarian.models import Dataset
import metadata.models

from .models import (
    Container,
    ContainerApp,
    ContainerFamily,
    ContainerArgument,
    ContainerArgumentType,
    ContainerRun,
)
from . import runutils


class TestComparisonStrategy(TestCase):
    "Test the strategy logic in runutils._compare_rerun_datasets"

    def test_apps_must_match(self):
        original = mock.Mock()
        rerun = mock.Mock()
        original.app = 1
        rerun.app = 2
        with self.assertRaises(ValueError):
            runutils.compare_rerun_datasets(original, rerun)

    def test_argtype_must_be_defined(self):
        arg = mock.Mock()
        arg.argtype = None
        run = mock.Mock()
        run.app.arguments.all = mock.Mock(return_value=[arg])
        with self.assertRaises(ValueError):
            runutils.compare_rerun_datasets(run, run)

    def test_argtype_must_be_handled(self):
        arg = mock.Mock()
        arg.argtype = "Not none, but not a known value"
        run = mock.Mock()
        run.app = mock.Mock()
        run.app.arguments.all = mock.Mock(return_value=[arg])
        with self.assertRaises(ValueError):
            runutils.compare_rerun_datasets(run, run)

    @mock.patch("container.runutils._compare_directory_outputs")
    @mock.patch("container.runutils._compare_optional_inputs")
    @mock.patch("container.runutils._compare_monovalent_args")
    def test_comparison_selection(self, compare_mono, compare_optional,
                                  compare_dir):
        "Verify that `compare_rerun_datasets` chooses the right comparison function"

        def reset_mocks():
            compare_mono.reset_mock()
            compare_optional.reset_mock()
            compare_dir.reset_mock()

        def make_mock_run(argtype):
            arg = mock.Mock()
            arg.argtype = argtype
            run = mock.Mock()
            run.app = mock.Mock()
            run.app._argument = arg
            run.app.arguments.all = mock.Mock(return_value=[arg])
            return run

        cases = [
            (ContainerArgumentType.FIXED_INPUT, [compare_mono]),
            (ContainerArgumentType.FIXED_OUTPUT, [compare_mono]),
            (ContainerArgumentType.OPTIONAL_INPUT, [compare_optional]),
            (ContainerArgumentType.OPTIONAL_MULTIPLE_INPUT, [compare_optional
                                                             ]),
            (ContainerArgumentType.FIXED_DIRECTORY_OUTPUT, [compare_dir]),
        ]

        for argtype, expect_called in cases:
            run = make_mock_run(argtype)
            runutils.compare_rerun_datasets(run, run)
            for comparison_mock in [
                    compare_mono, compare_optional, compare_dir
            ]:
                if comparison_mock in expect_called:
                    comparison_mock.assert_called_with(run.app._argument, run, run)
                else:
                    comparison_mock.assert_not_called()
            reset_mocks()


class BaseDatasetComparisonTestCase(TestCase):
    """Common setup for testing dataset comparison functions.

    Test cases that inherit from this class will have:

    - A ContainerApp (with associated Container and ContainerFamily, which
      we don't care about but create to satisfy foreign key requirements.)
    - Datasets containing 'a', 'b', and ''.

    Test cases that inherit from this class can add their own arguments to
    the app (they'll be dropped when the tests finish) and define their own
    ContainerRun and ContainerDataset instances.
    """

    # Pre-computed MD5 values for 'a', 'b', and ''.
    MD5_A = "60b725f10c9c85c70d97880dfe8191b3"
    MD5_B = "3b5d5c3712955042212316173ccf37be"
    MD5_EMPTY = "68b329da9893e34099c7d8ad5cb9c940"

    @classmethod
    def setUpTestData(cls) -> None:
        super().setUpTestData()
        cls._kive_user = metadata.models.kive_user()

        cls.containerfamily = ContainerFamily.objects.create(
            name="Dataset Comparison Test Family",
            description="",
            git="",
            user=cls._kive_user,
        )

        cls.container = Container.objects.create(
            family=cls.containerfamily,
            user=cls._kive_user,
            tag="",
            file=ContentFile(content="", name="empty.simg"),
        )
        cls.container.singularity_validated = True
        cls.container.save()

        cls.app = ContainerApp.objects.create(container=cls.container)

        cls.dataset_a = Dataset.objects.create(
            name="dataset_a",
            user=cls._kive_user,
            dataset_file=ContentFile(content="a", name="a.txt"),
            MD5_checksum=cls.MD5_A,
        )
        cls.dataset_b = Dataset.objects.create(
            name="dataset_b",
            user=cls._kive_user,
            MD5_checksum=cls.MD5_B,
            dataset_file=ContentFile(content="b", name="b.txt"),
        )
        cls.dataset_empty = Dataset.objects.create(
            name="dataset_empty",
            user=cls._kive_user,
            MD5_checksum=cls.MD5_EMPTY,
            dataset_file=ContentFile(content="", name="empty.txt"),
        )

    def check_case(
        self,
        orig_name: str,
        rerun_name: str,
        expected_comparisons: ty.List[str],
    ) -> None:
        original = getattr(self, f"containerrun_{orig_name}")
        rerun = getattr(self, f"containerrun_{rerun_name}")
        comparisons = list(self.comparison_function(self.arg, original, rerun))
        self.assertEqual(
            len(comparisons), len(expected_comparisons),
            f"Mismatched number of comparisons when comparing {original} and {rerun}"
        )
        self.assertEqual(
            [c.is_changed for c in comparisons],
            expected_comparisons,
            f"Mismatched comparisons when comparing {original} and {rerun}",
        )

    def check_cases(self, cases):
        for orig_name, rerun_name, expected_comparsisons in cases:
            with self.subTest(orig=orig_name,
                              rerun=rerun_name,
                              expected=expected_comparsisons):
                self.check_case(orig_name, rerun_name, expected_comparsisons)


class TestFixedInputDatasetComparison(BaseDatasetComparisonTestCase):
    """Compare datasets on two runs with fixed inputs arguments.

    This test is on an imaginary app with one fixed input argument.
    The app has two runs, one with a datasets containing 'a' and one with 'b'.
    """
    @classmethod
    def setUpTestData(cls) -> None:
        super().setUpTestData()
        cls.arg = ContainerArgument.objects.create(
            app=cls.app,
            position=1,
            name="test_fixed_arg",
            type=ContainerArgument.INPUT,
        )
        assert cls.arg.argtype is ContainerArgumentType.FIXED_INPUT

        cls.containerrun_a = cls.app.runs.create(
            user=cls._kive_user,
            state=ContainerRun.COMPLETE,
        )
        cls.containerrun_a.datasets.create(
            dataset=cls.dataset_a,
            argument=cls.arg,
        )

        cls.containerrun_b = cls.app.runs.create(
            user=cls._kive_user,
            state=ContainerRun.COMPLETE,
        )
        cls.containerrun_b.datasets.create(
            dataset=cls.dataset_b,
            argument=cls.arg,
        )

    def test_matching_arguments(self):
        comparison = runutils._compare_monovalent_args(
            self.arg,
            self.containerrun_a,
            self.containerrun_a,
        )
        self.assertEqual(comparison.is_changed, "no",
                         "Expected identical datasets to reveal no changes")

    def test_different_arguments(self):
        comparison = runutils._compare_monovalent_args(
            self.arg,
            self.containerrun_a,
            self.containerrun_b,
        )
        self.assertEqual(comparison.is_changed, "YES",
                         "Expected different datasets to reveal changes")


class TestFixedOutputDatasetComparison(BaseDatasetComparisonTestCase):
    """Compare datasets on two runs with fixed output arguments.

    This test is on an imaginary app with one fixed input argument.
    The app has two runs, one with a datasets containing 'a' and one with 'b'.
    """
    @classmethod
    def setUpTestData(cls) -> None:
        super().setUpTestData()
        cls.arg = ContainerArgument.objects.create(
            app=cls.app,
            position=1,
            name="test_fixed_arg",
            type=ContainerArgument.OUTPUT,
        )
        assert cls.arg.argtype is ContainerArgumentType.FIXED_OUTPUT

        cls.containerrun_a = cls.app.runs.create(
            user=cls._kive_user,
            state=ContainerRun.COMPLETE,
        )
        cls.containerrun_a.datasets.create(
            dataset=cls.dataset_a,
            argument=cls.arg,
        )

        cls.containerrun_b = cls.app.runs.create(
            user=cls._kive_user,
            state=ContainerRun.COMPLETE,
        )
        cls.containerrun_b.datasets.create(
            dataset=cls.dataset_b,
            argument=cls.arg,
        )

    def test_matching_arguments(self):
        comparison = runutils._compare_monovalent_args(
            self.arg,
            self.containerrun_a,
            self.containerrun_a,
        )
        self.assertEqual(
            comparison.is_changed,
            "no",
            "Expected identical datasets to reveal no changes",
        )

    def test_different_arguments(self):
        comparison = runutils._compare_monovalent_args(
            self.arg,
            self.containerrun_a,
            self.containerrun_b,
        )
        self.assertEqual(
            comparison.is_changed,
            "YES",
            "Expected different datasets to reveal changes",
        )


class TestCompareOptionalInputs(BaseDatasetComparisonTestCase):
    """Compare two datasets with optional inputs.

    This test has an imaginary app with one optional input.
    The app has three runs, two with dataset values 'a', 'b' and one without
    a dataset value.
    """

    comparison_function = staticmethod(runutils._compare_optional_inputs)

    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
        cls.arg = ContainerArgument.objects.create(
            app=cls.app,
            name="optional_input_arg",
            type=ContainerArgument.INPUT,
            position=None,
            allow_multiple=False,
        )
        assert cls.arg.argtype is ContainerArgumentType.OPTIONAL_INPUT

        cls.containerrun_a = cls.app.runs.create(
            user=cls._kive_user,
            state=ContainerRun.COMPLETE,
        )
        cls.containerrun_a.datasets.create(
            dataset=cls.dataset_a,
            argument=cls.arg,
        )

        cls.containerrun_b = cls.app.runs.create(
            user=cls._kive_user,
            state=ContainerRun.COMPLETE,
        )
        cls.containerrun_b.datasets.create(
            dataset=cls.dataset_b,
            argument=cls.arg,
        )

        cls.containerrun_none = cls.app.runs.create(
            user=cls._kive_user,
            state=ContainerRun.COMPLETE,
        )

    def test_comparing_optional_inputs(self):
        cases = [
            ("a", "a", ["no"]),
            ("a", "b", ["YES"]),
            ("a", "none", ["MISSING"]),
            ("none", "a", ["NEW"]),
        ]
        self.check_cases(cases)


class TestCompareOptionalMultipleInputs(BaseDatasetComparisonTestCase):
    """Compare container runs that have optional multiple inputs.

    This test has an imaginary app with a single optional multiply-valued
    input.

    The app has runs with the following input values:

    - An empty input (the input being ommitted).
    - Singly valued inputs with datasets A and B.
    - Multiply valued inputs with datasets AA, AB, and BA.

    Because we differentiate between NEW and MISSING datasets, comparisons are
    not symetrical (i.e. `compare(A, B)` is not the same as `compare(B, A)`);
    symmetrical comparisons are tested separately.

    Comparisons are made between:

    - The empty run and runs with values (and vice versa)
    - Singly valued runs with and without changes
    - Multiply valued and singlue valued runs with and without changes.
    - Multiply valued runs with and without changes.
    """

    comparison_function = staticmethod(runutils._compare_optional_inputs)

    @classmethod
    def setUpTestData(cls) -> None:
        super().setUpTestData()

        cls.arg = cls.app.arguments.create(
            name="optional_multiple_input_arg",
            type=ContainerArgument.INPUT,
            position=None,
            allow_multiple=True,
        )
        assert cls.arg.argtype is ContainerArgumentType.OPTIONAL_MULTIPLE_INPUT

        containerrun_cases = [
            ("empty", []),
            ("a", [cls.dataset_a]),
            ("b", [cls.dataset_b]),
            ("aa", [cls.dataset_a, cls.dataset_a]),
            ("bb", [cls.dataset_b, cls.dataset_b]),
            ("ab", [cls.dataset_a, cls.dataset_b]),
            ("ba", [cls.dataset_b, cls.dataset_a]),
        ]

        for name, datasets in containerrun_cases:
            containerrun_name = f"containerrun_{name}"
            containerrun = cls.app.runs.create(
                name=containerrun_name,
                user=cls._kive_user,
                state=ContainerRun.COMPLETE,
            )
            for idx, dataset in enumerate(datasets):
                containerrun.datasets.create(
                    dataset=dataset,
                    argument=cls.arg,
                    multi_position=idx,
                )
            setattr(cls, containerrun_name, containerrun)

    def test_comparing_monovalued_present_inputs(self):
        cases = [
            ("a", "a", ["no"]),
            ("a", "b", ["YES"]),
            ("b", "a", ["YES"]),
        ]
        self.check_cases(cases)

    def test_comparing_against_absent_inputs(self):
        cases = [
            ("empty", "empty", []),
            ("empty", "a", ["NEW"]),
            ("a", "empty", ["MISSING"]),
            ("empty", "aa", ["NEW", "NEW"]),
            ("aa", "empty", ["MISSING", "MISSING"]),
        ]
        self.check_cases(cases)

    def test_comparing_pairs_of_inputs(self):
        cases = [
            ("aa", "aa", ["no", "no"]),
            ("ab", "ab", ["no", "no"]),
            ("aa", "ab", ["no", "YES"]),
            ("ba", "aa", ["YES", "no"]),
            ("aa", "bb", ["YES", "YES"]),
        ]
        self.check_cases(cases)

    def test_comparing_mismatched_numbers_of_inputs(self):
        cases = [
            ("a", "aa", ["no", "NEW"]),
            ("aa", "a", ["no", "MISSING"]),
            ("a", "bb", ["YES", "NEW"]),
            ("bb", "a", ["YES", "MISSING"]),
            ("ab", "a", ["no", "MISSING"]),
            ("a", "ab", ["no", "NEW"]),
            ("a", "ba", ["YES", "NEW"]),
            ("ba", "a", ["YES", "MISSING"]),
        ]
        self.check_cases(cases)


class TestCompareDirectoryOutputs(BaseDatasetComparisonTestCase):
    comparison_function = staticmethod(runutils._compare_directory_outputs)

    @classmethod
    def setUpTestData(cls) -> None:
        super().setUpTestData()

        cls.arg = cls.app.arguments.create(
            name="directory_output_arg",
            type=ContainerArgument.OUTPUT,
            position=1,
            allow_multiple=True,
        )
        assert cls.arg.argtype is ContainerArgumentType.FIXED_DIRECTORY_OUTPUT

        contianerrun_cases = [
            ("ab", [cls.dataset_a, cls.dataset_b]),
            ("a_", [cls.dataset_a, None]),
            ("b_", [cls.dataset_b, None]),
            ("ba", [cls.dataset_b, cls.dataset_a]),
            ("_b", [None, cls.dataset_b]),
            ("_a", [None, cls.dataset_a]),
            ("__", [None, None]),
        ]

        for name, datasets in contianerrun_cases:
            containerrun_name = f"containerrun_{name}"
            containerrun = cls.app.runs.create(
                name=containerrun_name,
                user=cls._kive_user,
                state=ContainerRun.COMPLETE,
            )
            topname, subname = name
            topdataset, subdataset = datasets
            if topdataset is not None:
                containerrun.datasets.create(
                    dataset=topdataset,
                    argument=cls.arg,
                    name=topname,
                )
            if subdataset is not None:
                containerrun.datasets.create(
                    dataset=subdataset,
                    argument=cls.arg,
                    name=f"subdir/{subname}",
                )
            setattr(cls, containerrun_name, containerrun)

    def test_comparing_directory_outputs(self):
        cases = [
            ('ab', 'ab', ["no", "no"]),
            ('ab', 'ba', ["YES", "YES"]),
            ('ab', 'a_', ["no", "MISSING"]),
            ('ab', 'b_', ["YES", "MISSING"]),
            ('ab', '_a', ["MISSING", "YES"]),
            ('ab', '_b', ["MISSING", "no"]),
            ('ab', '__', ["MISSING", "MISSING"]),
            ('ba', 'ab', ["YES", "YES"]),
            ('ba', 'ba', ["no", "no"]),
            ('ba', 'a_', ["YES", "MISSING"]),
            ('ba', 'b_', ["no", "MISSING"]),
            ('ba', '_a', ["MISSING", "no"]),
            ('ba', '_b', ["MISSING", "YES"]),
            ('ba', '__', ["MISSING", "MISSING"]),
            ('a_', 'ab', ["no", "NEW"]),
            ('a_', 'ba', ["YES", "NEW"]),
            ('a_', 'a_', ["no"]),
            ('a_', 'b_', ["YES"]),
            ('a_', '_a', ["MISSING", "NEW"]),
            ('a_', '_b', ["MISSING", "NEW"]),
            ('a_', '__', ["MISSING"]),
            ('b_', 'ab', ["YES", "NEW"]),
            ('b_', 'ba', ["no", "NEW"]),
            ('b_', 'a_', ["YES"]),
            ('b_', 'b_', ["no"]),
            ('b_', '_a', ["MISSING", "NEW"]),
            ('b_', '_b', ["MISSING", "NEW"]),
            ('b_', '__', ["MISSING"]),
            ('_a', 'ab', ["NEW", "YES"]),
            ('_a', 'ba', ["NEW", "no"]),
            ('_a', 'a_', ["NEW", "MISSING"]),
            ('_a', 'b_', ["NEW", "MISSING"]),
            ('_a', '_a', ["no"]),
            ('_a', '_b', ["YES"]),
            ('_a', '__', ["MISSING"]),
            ('_b', 'ab', ["NEW", "no"]),
            ('_b', 'ba', ["NEW", "YES"]),
            ('_b', 'a_', ["NEW", "MISSING"]),
            ('_b', 'b_', ["NEW", "MISSING"]),
            ('_b', '_a', ["YES"]),
            ('_b', '_b', ["no"]),
            ('_b', '__', ["MISSING"]),
            ('__', 'ab', ["NEW", "NEW"]),
            ('__', 'ba', ["NEW", "NEW"]),
            ('__', 'a_', ["NEW"]),
            ('__', 'b_', ["NEW"]),
            ('__', '_a', ["NEW"]),
            ('__', '_b', ["NEW"]),
            ('__', '__', []),
        ]
        self.check_cases(cases)
