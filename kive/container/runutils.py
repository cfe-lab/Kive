import datetime
import typing as ty

from .models import ContainerRun, ContainerArgument, ContainerArgumentType, ContainerDataset


class DatasetComparison(ty.NamedTuple):
    type: str
    url: str
    name: str
    size: str
    created: datetime.datetime
    is_changed: str

    @staticmethod
    def from_containerdataset(containerdataset: ContainerDataset,
                              changed: str) -> "DatasetComparison":
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


def _compare_monovalent_args(argument: ContainerArgument,
                             original: ContainerRun,
                             rerun: ContainerRun) -> DatasetComparison:
    "Compare the datasets for a single-valued argument on a re-run and its original."
    original_dataset = original.datasets.get(argument=argument)
    rerun_dataset = rerun.datasets.get(argument=argument)
    if original_dataset.dataset.MD5_checksum == rerun_dataset.dataset.MD5_checksum:
        is_changed = "no"
    else:
        is_changed = "YES"
    return DatasetComparison.from_containerdataset(rerun_dataset, is_changed)


def _compare_optional_inputs(
        argument: ContainerArgument, original: ContainerRun,
        rerun: ContainerRun) -> ty.Iterable[DatasetComparison]:
    ...


def _compare_directory_outputs(
        argument: ContainerArgument, original: ContainerRun,
        rerun: ContainerRun) -> ty.Iterable[DatasetComparison]:
    ...


def _compare_rerun_datasets(
        original: ContainerRun,
        rerun: ContainerRun) -> ty.Iterable[DatasetComparison]:
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
        rerun: ContainerRun) -> ty.Iterable[DatasetComparison]:
    return list(_compare_rerun_datasets(original, rerun))
