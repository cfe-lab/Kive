"""
Constants and odds and ends that are hard-coded into the system.
"""


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


# Directory names used by the system when handling sandboxes.
class DirectoryNames:
    pass

dirnames = DirectoryNames()
dirnames.IN_DIR = "input_data"
dirnames.OUT_DIR = "output_data"
dirnames.LOG_DIR = "logs"


class Extensions:
    pass

extensions = Extensions()
extensions.CSV = "csv"
extensions.RAW = "raw"


class MaxLengths:
    pass

maxlengths = MaxLengths()
maxlengths.MAX_FILENAME_LENGTH = 260
maxlengths.MAX_EXTERNAL_PATH_LENGTH = 4096  # this is PATH_MAX on Linux systems
maxlengths.MAX_COLOUR_LENGTH = 100
maxlengths.MAX_NAME_LENGTH = 60
maxlengths.MAX_DESCRIPTION_LENGTH = 1000


class Groups:
    pass

groups = Groups()
groups.EVERYONE_PK = 1
groups.DEVELOPERS_PK = 2
groups.ADMIN_PK = 3


class Users:
    pass

users = Users()
users.KIVE_USER_PK = 1


class RunStates:
    pass

runstates = RunStates()
runstates.PENDING_PK = 1
runstates.RUNNING_PK = 2
runstates.SUCCESSFUL_PK = 3
runstates.CANCELLING_PK = 4
runstates.CANCELLED_PK = 5
runstates.FAILING_PK = 6
runstates.FAILED_PK = 7
runstates.QUARANTINED_PK = 8


class RunComponentStates:
    pass

runcomponentstates = RunComponentStates()
runcomponentstates.PENDING_PK = 1
runcomponentstates.RUNNING_PK = 2
runcomponentstates.SUCCESSFUL_PK = 3
runcomponentstates.CANCELLED_PK = 4
runcomponentstates.FAILED_PK = 5
runcomponentstates.QUARANTINED_PK = 6
