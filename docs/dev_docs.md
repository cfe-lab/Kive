Getting Your Code into Kive: A Guide for Software Application Developers
========================================================================

## Introduction
If you're considering using Kive, chances are you have a lot of
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


This situation can be problematic in a number of situations, for example:

1. When computation on clinical patient data is performed, a calculation
   result becomes part of the patient's medical record which has to
   be archived because medical treatment decisions can be based on these
   results. The input data, output data and exact version of the
   processing software all have to be archived for later reference.

2. When computational results are published in a scientific journal, 
   they should be reproducible. This is important for comparison 
   to other results, and in order to resolve any questions about the
   reported results that might arise after publication.

Kive aims to streamline your data processing by handling the
versioning and running of your code, and recording every detail of every
run. This way, you can focus on developing new and better ways to
process and analyse your data, with the knowledge that everything you do
will be recorded and reproducible.

In summary, Kive will help you keep track of the program and data input files
in your projects; it will help you maintain the inter dependencies between
these different files, and it will help you keep a record of the changes
you make to any of these files. In addition, you can execute a specific
version of a given program on a specific data set to reproduce an earlier
result, even if any program involved has been modified since the data was
last produced.

We will assume that you have an existing project consisting of a number
of different program files. Some of these will be _executable_, that is
they are to be run as a standalone program. We assume that the overall
computation is performed by a number of these executables in sequence.

## Loading Your Scripts ##
There are two options for loading your scripts into Kive: a Singularity image
with scripts and dependencies, or just the scripts. A Singularity image gives
you complete control, but just the scripts might be easier. Whichever option
you choose, your scripts will be called in the same way. Imagine you've written
a `reticulate_splines` script. It reads an input file of splines and writes the
reticulation statistics to an output file. Kive will launch your script with a
command line like this:

    /mnt/bin/reticulate_splines /mnt/input/splines.csv /mnt/output/reticulation.csv

You can write your script to expect the absolute file path to each input or
output file.

### Kive Nomenclature

* **Pipeline** <a name="pipeline-def"></a>

  Your overall computation is called a Kive pipeline. It consists of a number of
  data input definitions, computational steps (we call Methods, see below),
  and a number of data output definitions.

  Overall, a pipeline can be represented as a directed acyclic graph (DAG),
  with Methods as the nodes of the graph, and an edge from node i to j
  if node i produces data that node j needs.

* **Method** <a name="method-def"></a>

  This defines an elementary computational block. Each Kive Method has exactly one
  executable. Each Method will require some input for its computation and produce 
  some output, either for later Methods to read, or as a final result of the pipeline.
  The Method definition includes the number of inputs and outputs 
  of the Method and the data types of these.

  A Method, once it has been defined, can be used in multiple pipelines.
  In addition, different versions of the same Method can be used in different
  pipelines.

* **Dependency** <a name="dependency-def"></a>

  Apart from executables, there will be another kind of program file which acts 
  as a library; these files are themselves not executable, but are needed by other
  programs. We call these files _dependencies_, i.e. if file my\_program.py
  needs a file my_special_functions.py, then my_special_functions.py is a
  dependency of my\_program.py .

  When adding any code to Kive, it needs to be told about these dependencies 
  for two main reasons:

  * In order to be able to reproduce a previously generated dataset, Kive needs
    to know about all software (and the specific version of each software component) that was
    involved in producing that dataset.

  * Kive can run a pipeline in a distributed fashion on a number of machines. In order
    to do this securely, each computation is performed in a separate sandbox separated
    from others. In order for an executable to run in such a sandbox, all dependencies
    must also be copied into the sandbox at run-time.

* **Dataset** <a name="dataset-def"></a>

   In the Kive sense of the word, this is just data that is used as input for
   a Kive pipeline. A dataset is uploaded by a Kive user and can be fed into a 
   chosen pipeline for analysis.
   Kive keeps a record of all Datasets to ensure reproducability.

* **Run** <a name="run-def"></a>

  An instantiation of a specific pipeline with a certain Dataset as input.
  All initial, intermediate and final data related to the computations of 
  the pipeline on the designated input data are recorded.

### Migrating to Kive: the BIIIG Picture
The overall process of migrating an existing project into Kive follows a
'bottom-up' approach. We suggest you work in the following order:

1. **Readying your Code for Kive**

  Before you start uploading your code into Kive, it makes sense to make
  sure that your code is in a form that can best benefit from Kive's functionality.
  If this step is done carefully, the later steps will be straightforward.
  There are two main issues to address: 

  1. *How computations are performed ([The Methods](#method-def))*

     The executables must conform to a simple Kive convention with regard
     to how they read their input and produce their output.
     This is required so that Kive can assemble the Method building blocks
     into an overall pipeline.
     See the [next section](#executable-io-conventions) for more details on 
     how to achieve this.

  2. *How data between computations is transferred ([Datatypes](#datatypes))*
  
    Kive can perform 'sanity checks' on the data produced by a Method when 
    running a pipeline, before passing this data on to other Methods. In order to benefit from
    this functionality, Methods must produce data in one of a number of recognised
    data types, the most common one in Kive being comma-separated value (CSV) format.
    (Kive does have a 'raw' data format, files of which type it simply passes from one 
    Method to another without checks, but this data type is discouraged in a production
    environment.)
	
	CSV files are straightforward to produce, for example by using the csv module in
	a python program.

    **Note** As you are deciding at this stage what file format any intermediate data 
    sent between Methods will have, it could make sense to write some additional small code
    snippets that check the correct format of these files. See the section 
    on [constraints](#constraints) for an example of this.
    These additional snippets should be loaded into Kive as code resources with the rest of
    your code and can then be bound to a Datatype (see 'custom constraint').
    Kive will then call this code to check data integrity at run-time.

  This step is described in more 
  detail [in the next section](#readying-code).

2. **Upload your Code into Kive**

  Every separate file containing code, whether executable or dependency, 
  must be uploaded to Kive as a _Code resource_. Later, if the source code in a 
  Code resource changes, it will be possible to add a _CodeResourceRevision_
  to an existing _Code resource_. Each code resource is granted certain permissions,
  defining who (which kive users) can run it. This process is described 
  [here](#upload-code).

3. **Define Datatypes**

   Before you define Methods to run your programs, it makes sense to define Kive Datatypes 
   and Compound Datatypes for your project (Note, however, that you might opt to 
   create Methods at this stage already, if you want to define Datatypes with 
   custom constraints). Essentially, you are telling Kive what kind of data is expected 
   to be in each column of a particular kind of CSV file
   that your program produces or consumes. This is information you will have 
   determined in step 1. More detailed information about Datatypes can be found
   [here](#datatypes).
   
4. **Define Methods**

   Now Methods are defined using existing Code Resources and existing
   Compound Datatypes as inputs and outputs.
   Each Method will have one Code Resource as a main program, and can have
   a number of additional Code Resources as dependencies.
   Each Method can be defined to have a number of inputs and outputs, each of a certain
   Compound Datatype.
   Methods also have permission settings that define who can invoke them.
   This step is further described [here](#define-methods).

5. ** Build Pipelines **

   Finally, Methods can be assembled into pipelines.
   A pipeline will take a certain number of input files of specified types,
   perform the selected Methods and produce a certain number of output files of
   specified types. This process is further described [here](#create-pipelines).

6. ** Run your Analysis **

   Your work as a Developer is done. *As a User*, you can now upload Datasets (input data)
   and, depending on their permissions, run existing pipelines with selected input
   data. Multiple runs can be set to run at the same time. You inspect the status of
   runs previously started, and stop or rerun any runs.
   This is also where you can see calculated results, and extract them from Kive.
   See [here](#run-your-pipeline) for more information about this step.
   
All of these steps except the first and last can be performed from the 
web interface under the Kive 'Developers' main menu. The first step must be done by
you in a text editor of your choice. The last step is performed in Kive under 
the 'Users' main menu.


## Step 1: Readying your Code for Kive <a name="readying-code"></a>
As mentioned previously, because Kive handles running of your code 
automatically, executables you provide must have a specific command line 
interface. If you are writing new executables for Kive, you can implement
this interface from the get-go; otherwise, you will need to modify your 
code. There are two important changes you'll need to make:

1. your program's command line interface, discussed 
   in [the next section](#executable-io-conventions); and
2. the format of the data your program will read and write.
   This is discussed [here](#data-format-conventions).

### Executable I/O Conventions <a name="executable-io-conventions"></a>
Executables must be called from the command line with the syntax

        ./program_name [input_1] ... [input_n] [output_1] ... [output_k]

Executables can in principle be any file that can be executed under the
rules of the underlying operating system (e.g. under linux, these could be a
binary executable that has been separately compiled and linked, but are more
typically a python or shell script starting with a shebang).
The above expects n input files and produces k output files, whose
file names are passed in in that order on the command line. Your script may
additionally print messages to standard output and standard error, and
set its return code to a descriptive value, if you like, and all this
information will be recorded in Kive. However, your actual data must
be read in from, and output to, the files named on the command line. 

For example, suppose you have written a program which takes two inputs and
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
0 and 255, and this will be recorded for the user to view at run-time, as will the
contents of standard out and standard error (see the ExecLog
documentation for more details). Of course, you do not have to provide a
return code, nor do you need to output anything to either of the
standard streams.


### I/O Convention Restrictions 
#### Argument Name Conventions
Kive puts no restrictions on what characters can appear in your
dataset names, except those imposed by your file system. You must take
care that your programs can handle all possible characters in file
names. For example, on Unix, spaces are allowed. When using scripting
languages such as bash, be careful to enclose your variables in quotes,
(ie. "$2" instead of just $2) to avoid problems caused by spaces.

#### Additional Command-line Arguments
This required interface means that you cannot pass additional command
line arguments to your program. If you want to do that, write a wrapper
for your script, perhaps in a shell scripting language like Bash, which
calls the program with the necessary arguments. In the next example, we
want to call my\_script, which takes 2 inputs and produces 1 output,
with the additional argument "--verbose". We write the following bash
wrapper into a file called run_my_program.sh

        #!/bin/bash
        my_program --verbose $1 $2 $3

Now my\_program is getting the correct command line arguments for file
names, plus "--verbose". Of course, my_program needs to be careful not
to treat "--verbose" as the first input file, perhaps by using an
argument parsing library. For this to work, you will need to add
my\_program as a [dependency](#dependency-def) of run_my_program.sh
when you make a [Method out of it](#define-methods).

#### Variable Number of Arguments
In addition, the above restrictions mean that a program to be run 
in Kive cannot accept a variable number of arguments.
To get around this, you would again define a shell script for each fixed 
situation that you want to call your program with, and create
a shell script that calls your program in each case.


### Data Format Conventions <a name="data-format-conventions"></a>
The second modification you'll need to make is the format of the data
fed into, and output by, your code. Kive passes data around mostly
in RFC 4018-compliant comma-separated value (CSV) format. That means,
for example, if your program outputs data in the form of DNA sequences
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
format. For these use cases, Kive also allows you to declare data as
being "raw". Raw data will simply be passed around as-is from one step
in a pipeline to another. However, whenever possible, we /strongly
encourage/ you to write code which inputs and outputs CSV. One of
Kive's strengths is its ability to check the integrity of your data
against any constraints you define, which can help catch bugs and
unexpected behaviour in your code (see the section on Datatypes, below).
With raw data, this functionality is lost.
In general, raw data should be reserved for output directly from 
external programs. If you are writing the program yourself, 
output to CSV.


## Datatypes and CompoundDatatypes <a name="datatypes"></a>

### Introduction
Just as important as uploading your code to Kive, you need to describe to
Kive what kinds of data you will be working with. Kive is a tool
for manipulating structured data. In fact, from the point of view of the
system, the structure of the data is just as important as the contents,
if not more so. Kive wants as much information as possible about
what your data is supposed to look like, so that it can catch more
errors at all steps of Pipeline execution.

Kive passes most data around as CSV files with headers. In Kive
terms, each column of a CSV file has a Datatype, indicated by its
header, and the entire CSV file has a CompoundDatatype, which is simply
composed of the sequence of Datatypes of the individual columns. These
concepts are elaborated below.

### Datatypes
The most basic information about a piece of data is its type. You are
probably familiar with datatypes from your favourite programming
language. C, for example, has only a few datatypes - integer, character,
double, and variations on these. Languages like Python have many
datatypes, including lists, dictionaries, and files.

The datatype system of Kive more closely resembles that of Ada or
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

#### Restrictions <a=name="restrictions"></a>

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

Because Kive operates primarily on CSV files, all datatypes in
Kive restrict strings. When you create a new Datatype, you must
select one or more Datatypes for it to restrict (in addition to any
other restrictions you want to define). If you do not want to impose any
restrictions on your Datatype, declare it to restrict "string" only. For
convenience, you may also declare your Datatype to restrict one of
the other predefined types "int", "float", or "bool". Internally, these
three are treated as restrictions of "str"; they are there for
convenience, so that you do not have to define these common types on
your own.

#### Constraints<a name="constraints"></a>

Restrictions only encapsulate "is-a" type relationships among Datatypes.
While these can be useful, they don't allow you to specify exactly what
format you expect your data to have. That's where constraints come in.
Constraints hold specific rules that data of a particular Datatype must
adhere to. These can be very basic, such as having a particular length,
or arbitrarily complex, defined by code that you write.

There are two types of constraints on Datatypes in Kive:

1. /Basic constraints/ are simple checks built in to Kive, which should cover
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

2. /Custom constraint/ allows you to define arbitrary checks on your data 
  based on code that you write
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
  Note that rows in Kive are indexed from 1, so if the first input
  string fails the check, the Method should append a "1" to the output.

##### Custom Constraint Example
Suppose you are processing phylogenetic trees in Newick format. You could 
try to write a regular expression to filter these, but it would be complicated and 
might not work as expected (some programs leave annotations in Newick 
trees, for example). Instead, you could use the BioPython Phylo module to 
parse the tree in an external script, and fail the string if it does 
not parse correctly. A complete Python script to do this is the 
following (check out the documentation for Python's csv module, 
and BioPython's Phylo module, if you don't understand what's going on).

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
            row_num = i+1 # Kive uses 1-indexing for rows
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

When you provide a prototype, Kive will test your data checking
method by running it against the values in "example", and ensuring that
only the rows in "valid" which are false are output. If the Method does
not work as expected, nothing will be run on your data until the problem
is fixed. Although prototypes are not required, we highly recommend you
supply them. Checking the integrity of data at all steps of execution is
one of Kive's core functions, and bugs in data checking methods can
be difficult to chase down. Moreover, prototypes provide a helpful
reminder for yourself, and an aid for fellow users, of what your
Datatype should look like.


## Step 2: Upload your code as Code Resources <a name="upload-code"></a>
From the Developers/Code resources menu, individual files can be uploaded into Kive.
At this stage, no dependencies are defined between code resources (This only happens
when Methods are defined).

## Step 3: Define Data types
Data that flows between Methods in Kive will typically take the form of CSV files.
An individual CSV file will contain a number of data records of a certain compound type,
each record on a separate line.
It is this compound type of a CSV file that has to be defined in this step.

This process is divided into two separate menu items:

1. First, simple data types must be defined.

   This occurs under Developers/Datatypes. This section has commonly-used types
  such as int, float and string predefined, and it might well be that you will not
  need to define your own atomic data type, unless you want to create types 
  with [constraints](#constraints) or [restrictions](#restrictions).

2. Secondly, Compound data types can be defined.

   This occurs under Developers/Compund datatypes. The Compound data type is simply 
   a named ordered set of simple data types that describes the composition of an 
   individual line on a CSV file.


## Step 4: Define Methods<a name="define-methods"></a>
Now that you have defined the code resources and compound data types, you 
are ready to define the Methods, representing the individual computational units
of your project. This occurs under the Developers/Methods menu. Each Method
will have a code resource as its 'main program', and it could have further
code resources as dependencies that it needs in order to run.
It will have a number of named inputs and outputs, each of a certain 
compound data type. It will have permissions that determine which Kive users can
access the Method.

Methods can be collected into Method Families of a certain kind for
convenience. The 'main program' code resource must be an executable, which under unix-like
operating systems means that the file should begin with a 'shebang' (a #! on the first line)
if the executable is a script file. If the chosen code resource does not, then Kive will
prompt the user who may overrride this requirement.


## Step 5: Create Pipelines<a name="create-pipelines"></a>

### Pipeline Inputs
When creating a new pipeline in the Kive pipeline editor, the pipeline inputs are a good
place to start. Inputs can be created with the 'Add Input' menu item on the Pipeline
editing page. Inputs must be named and of a Compound type that has been previously defined
in Step 3.

### Creating Steps
The computational nodes of the pipeline are the methods you created in Step 4.
These can be added with the 'Add Method' menu item on the Pipeline editing page.

### Connecting Steps
In order to do work, all inputs of a method have to be connected.
A method's input can come from a pipeline input or from the output of a preceding
method. To make a connection, drag the producing node's output to a consuming
method's input. The connection will only be made if the data types agree.

To create a **pipeline output**, start a drag on a method's output marker. In the top-right
of the screen, a region will appear with the text 'Drag here to create an output'.
Dragging and dropping the method output into this region will create a new pipeline output
that can then be named.

### Finishing up
As an editing aid, the 'View' menu can be used to automatically display the pipeline
in a variety of pleasing ways.
Kive performs a rule check on the pipeline during editing. For example, all inputs of all
methods must be connected. If a pipeline passes the rule check, the 'Submit' button
will display a green dot. When it is in this state, click on it to save your changes.

## Step 6: Run your Pipeline <a name="run-your-pipeline"></a>

Your job as a pipeline developer is done. Now its time to run a pipeline as a user.
For this, go into the 'Users' menu from the Kive top menu. Your aim is to upload
some data and feed it through your newly crafted pipeline to produce output data.

1. **Upload Datasets to be Analysed**

   From the Users/Datasets menu item, you can upload Datasets which will be used 
   as inputs for a pipeline.

2. **Select a Pipeline**

   From the Users/Analysis menu item, choose a pipeline to run, and then select the
   Dataset for each of the pipeline's inputs. This combination will be called a 'Run'
   which you have to name. Once you are ready, click on 'Start Run' in the bottom right
   had corner. Kive will now start the calculation of the pipeline steps.

3. **View Pipeline Run Status**

   From the Users/Runs menu item, the process of a run can be monitored. The first
   page shows a list of all runs; clicking on the name of a run will show its 
   computational progress. If a run has completed, its output files can be viewed
   and downloaded.
