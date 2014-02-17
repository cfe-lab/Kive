Getting Your Code into Shipyard: CodeResources and CodeResourceRevisions
========================================================================

If you're considering using Shipyard, chances are you have a lot of
scripts hanging around on your computer to perform different analyses
and processing steps on biological datasets. Some of these may be
entirely your own code, or they may call external programs you have
installed. Maybe they are in a version control system like Git or SVN,
or perhaps you rename them whenever you make a significant change. Or,
you may have no versioning at all, and simply edit the scripts in place.
To use them, you may have to pass in the directory name as an argument,
copy the script to a specific place, or edit hardcoded paths within the
script itself. When changes are made to the code that affect its
behaviour, output from the old version may become unreproducible. Making
matters worse, you might not know which version of a given script was
used to generate a particular dataset.

Shipyard aims to streamline your data processing by handling the
versioning and running of your code, and recording every detail of every
run. This way, you can focus on developing new and better ways to
process and analyse your data, with the knowledge that everything you do
will be recorded and reproducible. Your first step will be to upload
some code to Shipyard.

### Readying your Code for Shipyard

Because Shipyard handles running of your code automatically, scripts you
provide must have a specific command line interface. If you are writing
new scripts for Shipyard, you can implement this interface from the
get-go; otherwise, you will need to modify your code. There are two
important changes you'll need to make: your program's command line
interface, and the format of the data your program will read and write.
We discuss these each in turn.

Scripts which are /executable/ (those which will be directly processing
data) must be called from the command line with the syntax

        ./script_name [input_1] ... [input_n] [output_1] ... [output_k]

This script expects n input files and produces k output files, whose
filenames are passed in that order on the command line. Your script may
additionally print messages to standard output and standard error, and
set its return code to a descriptive value, if you like, and all this
information will be recorded in Shipyard. However, your actual data must
be read in from, and output to, the files named on the command line. 

For example, suppose you have written a script which takes two inputs and
produces one output. The names of the two inputs, and then the names of
the output, will be contained in the "argv" array in most programming
languages. For example, in C:

        int main (int argc, const char* argv[]) {
            const char *infile1 = argv[1];
            const char *infile2 = argv[2];
            const char *outfile = argv[3];
            # fopen infile1 and infile 2
            # process data
            # fopen outfile, write results there
            printf("Reading from %s and %s, and writing to %s.",
                infile1, infile2, outfile);
            return 0;
        }

Notice that we print some information to standard output, and then
return 0 to indicate success. Your program may return any number between
0 and 255, and this will be recorded for you to view later, as will the
contents of standard out and standard error (see the ExecLog
documentation for more details). Of course, you do not have to provide a
return code, nor do you need to output anything to either of the
standard streams.

This required interface means that you cannot pass additional command
line arguments to your script. If you want to do that, write a wrapper
for your script, perhaps in a shell scripting language like Bash, which
calls the script with the necessary arguments. In the next example, we
want to call my\_script, which takes 2 inputs and produces 1 output,
with the additional argument "--verbose". We write the following bash
wrapper:

        #!/bin/bash
        my_script --verbose $1 $2 $3

Now my\_script is getting the correct command line arguments for file
names, plus "--verbose". Of course, my\_script needs to be careful not
to treat "--verbose" as the first input file, perhaps by using an
argument parsing library. For this to work, you will need to add
my\_script as a /dependency/ of this wrapper - see the below section on
CodeResources for more information.

The second modification you'll need to make is the format of the data
fed into, and output by, your code. Shipyard passes data around mostly
in RFC 4018-compliant comma-separated value (CSV) format. That means,
for example, if your script outputs data in the form of DNA sequences
with accompanying headers, you will need to output a file which looks
something like the below.

        header,sequence
        sample1,accgcggtct
        sample2,actgccgtct
        sample3,tctgccgtca

Of course, not every file you might want to handle can be reasonably
coerced into CSV format, such as a configuration or settings file.
External programs will probably output results in formats other than
CSV - bioinformatics programs often produce files in NEXUS or FASTA
format. For these use cases, Shipyard also allows you to declare data as
being "raw". Raw data will simply be passed around as-is from one step
in a pipeline to another. However, whenever possible, we /strongly
encourage/ you to write code which inputs and outputs CSV. One of
Shipyard's strengths is its ability to check the integrity of your data
against any constraints you define, which can help catch bugs and
unexpected behaviour in your code (see the section on Datatypes, below).
With raw data, this functionality is lost. In general, raw data should
be reserved for output directly from external programs. If you are
writing the program yourself, output to CSV.

<!--- end of Readying your Code for Shipyard -->
<!--- here will go sections on CodeResource and CodeResourceRevision -->

How Shipyard Structures Data: Datatypes and CompoundDatatypes
=============================================================

Just as important as uploading the code, you need to describe to
Shipyard what kinds of data you will be working with. Shipyard is a tool
for manipulating structured data. In fact, from the point of view of the
system, the structure of the data is just as important as the contents,
if not more so. Shipyard wants as much information as possible about
what your data is supposed to look like, so that it can catch more
errors at all steps of Pipeline execution. 

Shipyard passes most data around as CSV files with headers. In Shipyard
terms, each column of a CSV file has a Datatype, indicated by its
header, and the entire CSV file has a CompoundDatatype, which is simply
composed of the sequence of Datatypes of the individual columns. These
concepts are elaborated below.

Datatypes
---------
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

### Restrictions

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
Shipyard restrict strings. When you create a new Datatype, you must
select one or more Datatypes for it to restrict (in addition to any
other restrictions you want to define). If you do not want to impose any
restrictions on your Datatype, declare it to restrict "string" only. For
convenience, you may also declare your Datatype to restrict one of
the other predefined types "int", "float", or "bool". Internally, these
three are treated as restrictions of "str"; they are there for
convenience, so that you do not have to define these common types on
your own.

### Constraints

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
constraints "minlen=50", "maxlen=300", and "regexp='^[ATCG]+$'". More
concisely, I could simply use the one constraint
"regexp='^[ATCG]{50,300}$'". 

Note that both regular expressions begin with '^' and end with '$', which
indicates that the pattern should match the whole string. If you omit these,
values will be matched if they /begin/ with a string matching your pattern.
For example, the string "ATTA123" is allowed under the constraint 
"regexp='[ATCG]+'", but not under the constraint "regexp='^[ATCG]+$'".

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
