import datetime
import typing as ty

from .models import (ContainerArgument, ContainerArgumentType,
                     ContainerDataset, ContainerRun)


class DatasetComparison(ty.NamedTuple):
    "Result of comparing two datasets"
    type: str
    url: str
    name: str
    size: str
    created: datetime.datetime
    is_changed: str

    @staticmethod
    def from_containerdataset(
        containerdataset: ContainerDataset,
        is_changed: str,
    ) -> "DatasetComparison":
        dataset = containerdataset.dataset
        return DatasetComparison(
            type=containerdataset.argument.get_type_display(),
            url=dataset.get_view_url(),
            name=dataset.name,
            size=dataset.get_formatted_filesize(),
            created=dataset.date_created,
            is_changed=is_changed,
        )

    @classmethod
    def compare(
        cls,
        original: ContainerDataset,
        rerun: ContainerDataset,
    ) -> "DatasetComparison":
        if original.dataset.MD5_checksum == rerun.dataset.MD5_checksum:
            is_changed = "no"
        else:
            is_changed = "YES"
        return cls.from_containerdataset(rerun, is_changed)

    @classmethod
    def compare_optional(
        cls,
        original: ty.Optional[ContainerDataset],
        rerun: ty.Optional[ContainerDataset],
    ) -> ty.Optional["DatasetComparison"]:
        if original is None and rerun is None:
            return None
        elif original is None and rerun is not None:
            return cls.from_containerdataset(rerun, is_changed="NEW")
        elif original is not None and rerun is None:
            return cls.from_containerdataset(original, is_changed="MISSING")
        else:
            return cls.compare(original, rerun)


MONOVALENT_ARGTYPES = (ContainerArgumentType.FIXED_INPUT,
                       ContainerArgumentType.FIXED_OUTPUT)
OPTIONAL_INPUT_ARGTYPES = (ContainerArgumentType.OPTIONAL_INPUT,
                           ContainerArgumentType.OPTIONAL_MULTIPLE_INPUT)


def _compare_monovalent_args(
    argument: ContainerArgument,
    original: ContainerRun,
    rerun: ContainerRun,
) -> DatasetComparison:
    "Compare the datasets for a single-valued argument on a re-run and its original."
    original_dataset = original.datasets.filter(argument=argument).first()
    rerun_dataset = rerun.datasets.filter(argument=argument).first()
    return DatasetComparison.compare_optional(original_dataset, rerun_dataset)


def _compare_optional_inputs(
    argument: ContainerArgument,
    original: ContainerRun,
    rerun: ContainerRun,
) -> ty.Iterable[DatasetComparison]:
    argtype = argument.argtype

    errmsg = f"_compare_optional_input only handles OPTIONAL_INPUT and OPTIONAL_MULTIPLE_INPUT, not {argtype}"
    expected_types = (ContainerArgumentType.OPTIONAL_INPUT,
                      ContainerArgumentType.OPTIONAL_MULTIPLE_INPUT)
    assert argtype in expected_types, errmsg

    if argtype is ContainerArgumentType.OPTIONAL_INPUT:
        original_dataset = original.datasets.filter(argument=argument).first()
        rerun_dataset = rerun.datasets.filter(argument=argument).first()
        comparison = DatasetComparison.compare_optional(
            original_dataset, rerun_dataset)
        if comparison is not None:
            yield comparison
    elif argtype is ContainerArgumentType.OPTIONAL_MULTIPLE_INPUT:
        original_datasets = original.datasets.filter(
            argument=argument).order_by("multi_position")
        rerun_datasets = rerun.datasets.filter(
            argument=argument).order_by("multi_position")
        dataset_pairs = itertools.zip_longest(
            original_datasets,
            rerun_datasets,
            fillvalue=None,
        )
        for orig_dataset, rerun_dataset in dataset_pairs:
            comparison = DatasetComparison.compare_optional(
                orig_dataset, rerun_dataset)
            if comparison is not None:
                yield comparison


def _compare_directory_outputs(
        argument: ContainerArgument, original: ContainerRun,
        rerun: ContainerRun) -> ty.Iterable[DatasetComparison]:

    def _by_parent(
        bindings: ty.Iterable[ContainerDataset],
    ) -> dict[str, list[ContainerDataset]]:
        groups: dict[str, list[ContainerDataset]] = {}
        for b in bindings:
            parent = _parent_path(b.name)
            groups.setdefault(parent, []).append(b)
        return groups

    def _sorted_unmatched(
        bindings: list[ContainerDataset],
        matched: set[int],
    ) -> list[ContainerDataset]:
        return sorted(
            (b for b in bindings if id(b) not in matched),
            key=lambda b: (
                b.multi_position if b.multi_position is not None else 0,
                b.name,
                b.pk or 0,
            ),
        )

    original_all = list(original.datasets.filter(argument=argument))
    rerun_all = list(rerun.datasets.filter(argument=argument))
    original_by_parent = _by_parent(original_all)
    rerun_by_parent = _by_parent(rerun_all)
    all_parents = sorted(set(original_by_parent) | set(rerun_by_parent))

    for parent in all_parents:
        orig_bindings = original_by_parent.get(parent, [])
        rerun_bindings = rerun_by_parent.get(parent, [])
        orig_by_name: dict[str, ContainerDataset] = {}
        for b in orig_bindings:
            if b.name in orig_by_name:
                raise RuntimeError(
                    f"Duplicate name {b.name!r} in original run {original.id}")
            orig_by_name[b.name] = b
        rerun_by_name: dict[str, ContainerDataset] = {}
        for b in rerun_bindings:
            if b.name in rerun_by_name:
                raise RuntimeError(
                    f"Duplicate name {b.name!r} in rerun run {rerun.id}")
            rerun_by_name[b.name] = b

        matched: set[int] = set()
        for name in set(orig_by_name) & set(rerun_by_name):
            orig = orig_by_name[name]
            rerun_b = rerun_by_name[name]
            matched.add(id(orig))
            matched.add(id(rerun_b))
            comparison = DatasetComparison.compare_optional(orig, rerun_b)
            if comparison is not None:
                yield comparison

        orig_unmatched = _sorted_unmatched(orig_bindings, matched)
        rerun_unmatched = _sorted_unmatched(rerun_bindings, matched)
        for orig_b, rerun_b in zip(orig_unmatched, rerun_unmatched):
            matched.add(id(orig_b))
            matched.add(id(rerun_b))
            comparison = DatasetComparison.compare_optional(orig_b, rerun_b)
            if comparison is not None:
                yield comparison

        for b in orig_bindings:
            if id(b) not in matched:
                comparison = DatasetComparison.compare_optional(b, None)
                if comparison is not None:
                    yield comparison
        for b in rerun_bindings:
            if id(b) not in matched:
                comparison = DatasetComparison.compare_optional(None, b)
                if comparison is not None:
                    yield comparison


def _parent_path(name: str) -> str:
    idx = name.rfind("/")
    if idx == -1:
        return ""
    return name[:idx]


def _compare_rerun_datasets(
    original: ContainerRun,
    rerun: ContainerRun,
) -> ty.Iterable[DatasetComparison]:
    if original.app != rerun.app:
        raise ValueError(
            "Expect re-runs to have the same App as their original")
    argument: ContainerArgument
    for argument in original.app.arguments.all():
        argtype = argument.argtype
        if argtype is None:
            raise ValueError(
                "Invalid argument (couldn't assign an argtype): {}".format(
                    argument))
        if argtype in MONOVALENT_ARGTYPES:
            yield _compare_monovalent_args(argument, original, rerun)
        elif argtype in OPTIONAL_INPUT_ARGTYPES:
            yield from _compare_optional_inputs(argument, original, rerun)
        elif argtype is ContainerArgumentType.FIXED_DIRECTORY_OUTPUT:
            yield from _compare_directory_outputs(argument, original, rerun)
        else:
            raise ValueError("Un-handled argument type: {}".format(argtype))


def compare_rerun_datasets(
    original: ContainerRun,
    rerun: ContainerRun,
) -> ty.Iterable[DatasetComparison]:
    """Return a list of difference in the datasets between a re-run ContainerRun
    and its original.

    Datasets can be changed, not-changed, missing, or new.
    """
    return list(_compare_rerun_datasets(original, rerun))
