"""
Basic file-checking functionality used by Shipyard.
"""
import os
import hashlib
import models
import csv

def can_create_new_file(file_to_create):
    """
    Tests whether the specified file can be created.
    
    This tests whether something already exists there, and
    if not, whether the containing directory's permissions
    will allow us to create this file (and whatever 
    subdirectories are required).

    Return (True, None) if we can create this file; return
    (False, [reason why not]) if not.
    """
    reason = None
    is_okay = True
    if os.access(file_to_create, os.F_OK):
        is_okay = False
        reason = "path \"{}\" already exists".format(
            file_to_create)

    else:
        # The path did not exist; see if we can create it.
        output_dir = os.path.dirname(file_to_create)

        # If output_dir is the empty string, i.e. output_path
        # is just in the same directory as we are executing Python,
        # then we don't have to make a directory.  If it *isn't*
        # empty, then we either have to create the directory or
        # see if we can write to it if it already exists.
        if output_dir != "":
            try:
                os.makedirs(output_dir)
            except os.error:
                # Did it fail to create?
                if not os.access(output_dir, os.F_OK):
                    reason = "output directory \"{}\" could not be created".format(output_dir)
                    is_okay = False
                    return (is_okay, reason)

        else:
            output_dir = "."

        # If we reach here, the directory exists and the outputs can
        # be written to it - but only if there are sufficient
        # permissions.
        if not os.access(output_dir, os.W_OK or os.X_OK):
            reason = "insufficient permissions on run path \"{}\"".format(run_path)
            is_okay = False

    return (is_okay, reason)

def set_up_directory(directory_to_use):
    """
    Checks whether the specified directory can be used.

    That is, either we create it with appropriate permissions,
    or it exists already and is writable/executable/empty.
    """
    try:
        os.makedirs(directory_to_use)
    except os.error:
        # Check if the directory does not exist.
        if not os.access(directory_to_use, os.F_OK):
            raise ValueError("directory \"{}\" could not be created".
                             format(directory_to_use))

        # Otherwise, the directory already existed.  Check that we
        # have sufficient permissions on it, and that it is empty.
        if not os.access(directory_to_use, os.W_OK or os.X_OK):
            raise ValueError(
                "insufficient permissions on directory \"{}\"".
                format(directory_to_use))

        if (len(glob.glob(directory_to_use + "/*") +
                glob.glob(directory_to_use + "/.*")) > 0):
            raise ValueError(
                "directory \"{}\" is not empty".
                format(directory_to_use))

def compute_md5(file_to_checksum):
    """
    Computes MD5 checksum of specified file.

    file_to_checksum should be an open, readable, file handle, with
    its position at the beginning, i.e. so that .read() gets the
    entire contents of the file.
    """
    md5gen = hashlib.md5()
    md5gen.update(file_to_checksum.read())
    return md5gen.hexdigest()


def summarize_CSV(file_to_check, CDT, testing_path):
    """
    Give metadata on the CSV: its number of rows, and any defects.

    By defects we mean deviations from the CDT.

    file_to_check is an open, readable, file object that
    is set to the beginning.
    
    Returns a dict containing metadata about the file:
    - bad_num_cols: set if the header has the wrong number of columns;
      if so, returns the number of columns in the header.

    - bad_col_indices: set if there are improperly named columns 
      in the header; if so, returns a list of indices of bad columns

    - num_rows: number of rows
    
    - failing_cells: dict of non-conforming cells in the file.  Each
      entry is keyed by (rownum, colname) and contains a list of tests
      failed.
    """
    summary = {}
    
    # A CSV reader which we will use to check individual 
    # cells in the file, as well as creating external CSVs
    # for columns whose DT has a CustomConstraint.
    data_csv = csv.DictReader(file_to_check)

    # Counter for the number of rows.
    num_rows = 0
    
    ####
    # CHECK HEADER
    
    # First, check that the header is OK.
    header = data_csv.fieldnames
            
    cdt_members = CDT.members.all()

    # The number of CSV columns must match the number of CDT members.
    if len(header) != cdt_members.count():
        summary["bad_num_cols"] = len(header)
        return summary

    # CDT definition must be coherent with the CSV header: ith cdt
    # member must have the same name as the ith CSV header.
    bad_col_indices = []
    for cdtm in cdt_members:
        if cdtm.column_name != header[cdtm.column_idx-1]:
            bad_col_indices.append(cdtm.column_idx)
            # raise ValidationError(
            #     "Column {} of Dataset \"{}\" is named {}, not {} as specified by its CDT".
            #         format(cdtm.column_idx, self.symbolicdataset.dataset,
            #                header[cdtm.column_idx-1], cdtm.column_name))

    if len(bad_col_indices) != 0:
        summary["bad_col_indices"] = bad_col_indices
        return summary

    # FINISH CHECKING HEADER
    ####
    
    ####
    # CHECK CONSTRAINTS

    # A list of failing entries.
    failing_cells = {}

    # Check if any columns have CustomConstraints.  We will use this
    # lookup table while we're reading through the CSV file to see
    # which columns need to be copied out for checking against
    # CustomConstraints.

    try:
        # The values are pairs (path to file, writable file handle).
        cols_with_cc = {}
        for cdtm in cdt_members:
            if cdtm.datatype.has_custom_constraint():
                # This column is going to require running a verification
                # method, so we set up a place within testing_path to do
                # so.
                column_test_path = os.path.join(
                    testing_path, "col{}".format(cdtm.column_idx))
            
                set_up_directory(column_test_path)

                test_file_path = os.path.join(column_test_path,
                                              "to_test.csv")
                
                cols_with_cc[cdtm.column_idx] = (
                    test_file_path,
                    open(os.path.join(test_file_path), "wb")
                )

                # Write a CSV header.
                cols_with_cc[cdtm.column_idx][1].write("to_test\n")

        # Now we can actually check the data.
        for i, row in enumerate(data_csv):
            # Note that i is 0-based, but our rows should be 1-based.
            rownum = i + 1
            for cdtm in cdt_members:
                curr_cell_value = row[cdtm.column_name]
                test_result = cdtm.datatype.check_basic_constraints(
                    curr_cell_value)
                
                if len(test_result) != 0:
                    failing_cells[(rownum, cdtm.column_name)] = test_result

                if cdtm.column_idx in cols_with_cc:
                    cols_ith_cc[cdtm.column_idx].write(curr_cell_value + "\n")

    finally:
        for col in cols_with_cc:
            cols_with_cc[col][1].close()
                
    # Now: any column that had a CustomConstraint must be checked 
    # using the specified verification method.
    for col in cols_with_cc:
        # FIXME continue from here!  We need to invoke the verification
        # method using run_code.  All of our inputs are in place.
        pass

    
