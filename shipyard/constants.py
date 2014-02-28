# Constants and odds and ends that are hard-coded into the system.

#error_messages = {
#    "header_mismatch": 
#        'File "{}" should have the header "{}", but it has "{}"',
#    "empty_file": 'File "{}" is empty.',
#    "transf_noinput": 'Transformation "{}" has no inputs.',
#    "dataset_bad_type":
#        'Expected source to be either a Dataset or a string, got {}',
#    "execlog_swapped_times":
#        'The end time of ExecLog "{}" is before its start time.',
#    "ccl_swapped_times":
#        'The end time of ContentCheckLog "{}" is before its start time.',
#    "verificationlog_swapped_times":
#        'The end time of VerificationLog "{}" is before its start time.',
#    "verificationlog_incomplete":
#        'VerificationLog "{}" is not complete.',
#    "bad_constraint_checker":
#        'Constraint checking method "{}" crashed',
#    "ER_cable_wiring_DT_mismatch":
#        'ExecRecord \"{}\" represents a cable but Datatype of destination Dataset column {} does not match its source',
#    "DT_multiple_builtin_types":
#        "Datatype \"{}\" restricts supertypes of multiple built-in types other than INT and FLOAT",
#    "DT_prototype_raw":
#        "Prototype Dataset for Datatype \"{}\" is raw",
#    "DT_prototype_wrong_CDT":
#        "Prototype Dataset for Datatype \"{}\" should have CDT identical to PROTOTYPE",
#    "DT_integer_min_max_val_too_narrow":
#        "Datatype \"{}\" has built-in type INT but the interval [{}, {}] does not admit any integers",
#    "DT_min_length_exceeds_max_length":
#        "Datatype \"{}\" effective MIN_LENGTH exceeds effective MAX_LENGTH",
#    "CellError_bad_BC":
#        "CellError \"{}\" refers to a BasicConstraint that does not apply to the associated column",
#    "CellError_bad_CC":
#        "CellError \"{}\" refers to a CustomConstraint that does not apply to the associated column",
#    "BC_DT_not_complete":
#        "Parent Datatype \"{}\" of BasicConstraint \"{}\" is not complete",
#    "BC_val_constraint_parent_non_numeric":
#        "BasicConstraint \"{}\" specifies a bound on a numeric value but its parent Datatype \"{}\" is not a number",
#    "bad_input_file":
#        'The file "{}" does not match the CompoundDatatype "{}"',
#    "incomplete_execlog":
#        'Execlog "{}" is not complete'
#}
#
#warning_messages = {
#    "pipeline_already_run": 
#        "A pipeline has already been run in Sandbox {}, returning the previous Run"
#}


# Primary keys for Datatypes and CDTs that are pre-defined for the user.
class Datatypes:
    pass

datatypes = Datatypes()
datatypes.STR_PK = 1
datatypes.BOOL_PK = 2
datatypes.FLOAT_PK = 3
datatypes.INT_PK = 4
datatypes.NATURALNUMBER_PK = 5
datatypes.NUMERIC_BUILTIN_PKS = set([datatypes.INT_PK, datatypes.FLOAT_PK])

class CDTs:
    pass

CDTs = CDTs()
CDTs.VERIF_IN_PK = 1
CDTs.VERIF_OUT_PK = 2
CDTs.PROTOTYPE_PK = 3
