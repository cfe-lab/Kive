import datetime
import itertools
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
        changed: str,
    ) -> "DatasetComparison":
        dataset = containerdataset.dataset
        return DatasetComparison(
            type=containerdataset.argument.type,
            url=dataset.get_view_url(),
            name=dataset.name,
            size=dataset.get_formatted_filesize(),
            created=dataset.date_created,
            is_changed=changed,
        )


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
    original_dataset = original.datasets.get(argument=argument)
    rerun_dataset = rerun.datasets.get(argument=argument)
    if original_dataset.dataset.MD5_checksum == rerun_dataset.dataset.MD5_checksum:
        is_changed = "no"
    else:
        is_changed = "YES"
    return DatasetComparison.from_containerdataset(rerun_dataset, is_changed)


def _compare_optional_inputs(
    argument: ContainerArgument,
    original: ContainerRun,
    rerun: ContainerRun,
) -> ty.Iterable[DatasetComparison]:
    argtype = argument.argtype

    def compare_optional_datasets(
        original_dataset: ty.Optional[ContainerDataset],
        rerun_dataset: ty.Optional[ContainerDataset],
    ) -> ty.Optional[DatasetComparison]:
        if original_dataset is None and rerun_dataset is None:
            return None
        elif original_dataset and rerun_dataset:
            if original_dataset.dataset.MD5_checksum != rerun_dataset.dataset.MD5_checksum:
                changed = "YES"
            else:
                changed = "no"
            return DatasetComparison.from_containerdataset(
                rerun_dataset,
                changed=changed,
            )
        elif original_dataset and not rerun_dataset:
            return DatasetComparison.from_containerdataset(
                original_dataset,
                changed="MISSING",
            )
        elif rerun_dataset and not original_dataset:
            return DatasetComparison.from_containerdataset(
                rerun_dataset,
                changed="NEW",
            )

    if argtype is ContainerArgumentType.OPTIONAL_INPUT:
        original_dataset = original.datasets.filter(argument=argument).first()
        rerun_dataset = rerun.datasets.filter(argument=argument).first()
        comparison = compare_optional_datasets(original_dataset, rerun_dataset)
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
            comparison = compare_optional_datasets(orig_dataset, rerun_dataset)
            if comparison is not None:
                yield comparison
    else:
        raise ValueError(
            "_compare_optional_input only handles OPTIONAL_INPUT and OPTIONAL_MULTIPLE_INPUT arguments"
        )


def _compare_directory_outputs(
        argument: ContainerArgument, original: ContainerRun,
        rerun: ContainerRun) -> ty.Iterable[DatasetComparison]:
    ...


def _compare_rerun_datasets(
    original: ContainerRun,
    rerun: ContainerRun,
) -> ty.Iterable[DatasetComparison]:
    if original.app != rerun.app:
        raise ValueError(
            "Expect re-runs to have the same App as their original")
    argument: ContainerArgument
    for argument in original.app.arguments:
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
