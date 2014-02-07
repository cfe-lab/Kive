# Constants and odds and ends that are hard-coded into the system.

error_messages = {
    "header_mismatch": 
        'File "{}" should have the header "{}", but it has "{}"',
    "empty_file": 'File "{}" is empty.',
    "driver_metapackage": 
        'Method "{}" cannot have CodeResourceRevision "{}" as a driver, because it has no content file.',
    "transf_noinput": 'Transformation "{}" has no inputs.',
    "method_bad_inputcount": 
        'Method "{}" expects {} inputs and {} outputs, but {} inputs and {} outputs were supplied',
    "pipeline_bad_inputcount":
        'Pipeline "{}" expects {} inputs, but {} were supplied',
    "pipeline_expected_raw":
        'Pipeline "{}" expected input {} to be raw, but got one with compound datatype "{}"',
    "pipeline_expected_nonraw":
        'Pipeline "{}" expected input {} to be of compound datatype "{}", but got raw',
    "pipeline_cdt_mismatch":
        'Pipeline "{}" expected input {} to be of compound datatype "{}", but got one with compound datatype "{}"',
    "pipeline_bad_numrows":
        'Pipeline "{}" expected input {} to have between {} and {} rows, but got a one with {}',
    "dataset_bad_type":
        'Expected source to be either a Dataset or a string, got {}',
    "execlog_swapped_times":
        'The end time of ExecLog "{}" is before its start time.',
    "SD_not_in_pipeline":
        'SymbolicDataset "{}" was not found in Pipeline "{}" and cannot be recovered',
    "SD_pipeline_input":
        'SymbolicDataset "{}" is an input to Pipeline "{}" and cannot be recovered',
    "bad_constraint_checker":
        'Constraint checking method "{}" crashed'
}

warning_messages = {
    "pipeline_already_run": 
        "A pipeline has already been run in Sandbox {}, returning the previous Run"
}


# Primary keys for Datatypes and CDTs that are pre-defined for the user.
class Datatypes:
    pass

datatypes = Datatypes()
datatypes.STR_PK = 1
datatypes.BOOL_PK = 2
datatypes.FLOAT_PK = 3
datatypes.INT_PK = 4
datatypes.NATURALNUMBER_PK = 5

class CDTs:
    pass

CDTs = CDTs()
CDTs.VERIF_IN_PK = 1
CDTs.VERIF_OUT_PK = 2
CDTs.PROTOTYPE_PK = 3
