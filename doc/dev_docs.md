Datatypes
=========

Shipyard is a tool for manipulating structured data. In fact, from the
point of view of the system, the structure of the data is just as
important as the contents, if not more so. Shipyard wants as much
information as possible about what your data is supposed to look like,
so that it can catch more errors at all steps of Pipeline execution. 

The most basic information about a piece of data is its type. You are
probably familiar with datatypes from your favourite programming
language. C, for example, has only a few datatypes - integer, character,
double, and variations on these. Languages like Python have many
datatypes, including lists, dictionaries, and files.

The datatype system of Shipyard more closely resembles that of Ada or
Haskell (but don't let that scare you off :) ). Datatypes can be
extremely restrictive, and we encourage you to define types as narrowly
as possible for the range of data you expect to work with. For example,
suppose you are working with DNA coming off a sequencing platform which
produces reads between 50 and 300 base pairs long. You could simply
declare these as "strings", but it would be better to define them as
"strings of between 50 and 300 characters from the alphabet {A, T, C,
G}". That way, you can be confident you are always passing around DNA
within your pipelines. We show how to define these sorts of datatypes
below.

Restrictions
------------

When one datatype is a special case of another, we say that the former
/restricts/ the latter. For example, positive integers are a special
case of integers, so positive integers restrict integers. If you are
familiar with object oriented programming, restriction in Datatypes is
analogous to inheritance in classes. Restrictions can be nested
arbitrarily deeply, but cannot be circular (you can't define Datatypes
A, B, and C, such that A restricts B, B restricts C, and C restricts A).
You can also have multiple Datatypes restricting a single one (A and B
both restrict C), or one Datatype which restricts several others (A
restricts both B and C). 

Because Shipyard operates primarily on CSV files, all datatypes in
Shipyard restrict strings. Shipyard is implemented in Python, so all
data is internally handled in Python terms, which means strings are just
Python objects of type "str". When you create a new Datatype, you must
select a Python type for it to restrict (in addition to any other
restrictions you want to define). This may safely be left as string, but
for convenience, you may also declare your Datatype to restrict one of
the Python types "int", "float", or "bool". Internally, these three are
treated as restrictions of "str"; they are there for convenience, so
that you do not have to define these common types on your own.

Constraints
-----------

Restrictions only encapsulate "is-a" type relationships among Datatypes.
While these can be useful, they don't allow you to specify exactly what
format you expect your data to have. That's where constraints come in.
Constraints hold specific rules that data of a particular Datatype must
adhere to. These can be very basic, such as having a particular length,
or arbitrarily complex, defined by code that you write.

There are two types of constraints on Datatypes in Shipyard. /Basic
constraints/ are simple checks built in to Shipyard, which should cover
a good number of data checking cases. These include minimum and maximum
length (for strings), minimum and maximum values (for integers and
floats), matching a regular expression (for strings), and being
formatted as a timestamp (for strings). To define a datatype for strings
of DNA between 50 and 300 base pairs long, I could use the three
constraints "minlen=50", "maxlen=300", and "regexp='[ATCG]+'". More
concisely, I could simply use the one constraint
"regexp='[ATCG]{50,300}'".

The second type of constraint is a /custom constraint/, which allows you
to define arbitrary checks on your data based on code that you write
yourself. Datatypes may have several basic constraints, but only one
custom constraint. To understand custom constraints, you need to be
familiar with CodeResources, CodeResourceRevisions, and Methods; if not,
go read the documentation and come back.

A custom constraint is defined by a special Method which will be used to
check the data. This Method must take as input a column of strings
called "to\_test", and return a column of positive integers called
"failed\_row". For each string in "to\_test", the Method should process
the string and check if it matches the constraint. If it does not match,
the row of the non-matching string should be appended to the output.
Note that rows in Shipyard are indexed from 1, so if the first input
string fails the check, the Method should append a "1" to the output.

Here is an example. Suppose you are processing phylogenetic trees in
Newick format. You could try to write a regular expression to filter
these, but it would be complicated and might not work as expected (some
programs leave annotations in Newick trees, for example). Instead, you
could use the BioPython Phylo module to parse the tree in an external
script, and fail the string if it does not parse correctly. A complete
Python script to do this is the following (check out the documentation
for Python's csv module, and BioPython's Phylo module, if you don't
understand what's going on).

        import sys
        import csv
        from Bio import Phylo
        
        # StringIO lets us do file operations on strings.
        if sys.version_info[0] == 3:
          from io import StringIO # python3
        else:
          from cStringIO import StringIO

        # The Method's driver is called with the input file as its first
        # argument, and the output file as its second argument.
        infile = open(sys.argv[1], "r")
        outfile = open(sys.argv[2], "w")

        # Define a reader for getting the input data, and a writer for
        # writing the output data.
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames="failed_row")

        # Loop through each input string.
        for i, row in enumerate(reader):
            row_num = i+1 # Shipyard uses 1-indexing for rows
            # Try to parse the tree.
            io = StringIO(row["to_test"]) 
            try:
                Phylo.read(io, "newick")
            except Phylo.NewickIO.NewickError:
                writer.writerow({"failed_row": row_num})

To create your Newick tree Datatype, you would first create a Method
with a CodeResourceRevision containing this code as its driver (again,
check the documentation for Methods and CodeResourceRevisions). You
would then add a CustomConstraint to your Datatype, with the Method as
its verification method.

Note that, more than most Methods, the onus is on you to provide code
that works correctly - if your code doesn't work as expected, the checks
on your data will be meaningless. To assist you with writing working
constraint checking methods, we encourage you to define a prototype
Dataset for your Datatypes, which gives examples of valid and invalid
values. A prototype is a Dataset with two columns: "example", and
"valid". "example" contains arbitrary strings, and "valid" is a boolean
field, true if the example is a valid instance of the Datatype, or false
otherwise.

For example, a prototype for a "positive integer" datatype might be
something like this (remember, all types, even integers, are
restrictions of strings).

        example,valid
        123,true
        abc,false
        041,true
        01a,false

When you provide a prototype, Shipyard will test your data checking
method by running it against the values in "example", and ensuring that
only the rows in "valid" which are false are output. If the Method does
not work as expected, nothing will be run on your data until the problem
is fixed. Although prototypes are not required, we highly recommend you
supply them. Checking the integrity of data at all steps of execution is
one of Shipyard's core functions, and bugs in data checking methods can
be difficult to chase down. Moreover, prototypes provide a helpful
reminder for yourself, and an aid for fellow users, of what your
Datatype should look like.
