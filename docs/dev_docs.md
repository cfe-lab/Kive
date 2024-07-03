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
in your projects; it will help you maintain the interdependencies between
these different files, and it will help you keep a record of the changes
you make to any of these files. In addition, you can execute a specific
version of a given program on a specific data set to reproduce an earlier
result, even if any program involved has been modified since the data was
last produced.

We will assume that you have an existing project consisting of a number
of different program files. Some of these will be _executable_, that is
they are to be run as a standalone program. We assume that the overall
computation is performed by a number of these executables in sequence.

## Loading Your Scripts
Once you have some scripts running on your workstation, you need to load them
into a Kive container. That lets you record exactly when and how they get run.

There are two types of containers for loading your scripts into Kive: a
Singularity image with scripts and dependencies, or just the scripts in an
archive like a zip file or a tar file. A Singularity image gives you complete
control, but an archive of scripts might be easier. Whichever option you
choose, your scripts will be called in the same way. Imagine you've written a
`reticulate_splines` script. It reads an input file of splines and writes the
reticulation statistics to an output file. Kive will launch your script with a
command line like this:

    /mnt/bin/reticulate_splines /mnt/input/splines.csv /mnt/output/reticulation.csv

Write your script to expect the absolute file paths to all the input and output
files.

For example, you might have a script that reads people's names from a file, and
writes greetings into another file:

    import os
    from csv import DictReader, DictWriter
    
    
    def main():
        with open('host_input/example_names.csv') as names_csv, \
                open('host_output/greetings.csv', 'w') as greetings_csv:
            reader = DictReader(names_csv)
            writer = DictWriter(greetings_csv,
                                ['greeting'],
                                lineterminator=os.linesep)
            writer.writeheader()
            for row in reader:
                writer.writerow(dict(greeting='Hello, ' + row['name']))
    
    
    main()

To let Kive tell the script where to find the files, you have to look at the
command-line arguments. For a simple script like this, you can use `sys.argv`:

    import os
    import sys
    from csv import DictReader, DictWriter
    
    
    def main():
        script_path, names_path, greetings_path = sys.argv
        with open(names_path) as names_csv, \
                open(greetings_path, 'w') as greetings_csv:
            reader = DictReader(names_csv)
            writer = DictWriter(greetings_csv,
                                ['greeting'],
                                lineterminator=os.linesep)
            writer.writeheader()
            for row in reader:
                writer.writerow(dict(greeting='Hello, ' + row['name']))
    
    
    main()

To provide more features like error handling and help, use [the `argparse` module].

    import os
    from argparse import ArgumentParser, FileType
    from csv import DictReader, DictWriter
    
    
    def parse_args():
        parser = ArgumentParser()
        parser.add_argument('names_csv', type=FileType())
        parser.add_argument('greetings_csv', type=FileType('w'))
    
        return parser.parse_args()
    
    
    def main():
        args = parse_args()
        reader = DictReader(args.names_csv)
        writer = DictWriter(args.greetings_csv,
                            ['greeting'],
                            lineterminator=os.linesep)
        writer.writeheader()
        for row in reader:
            writer.writerow(dict(greeting='Hello, ' + row['name']))
    
    
    main()

[the `argparse` module]: https://docs.python.org/3/library/argparse.html

## Singularity Images
When you upload a Singularity image, Kive will try to extract information about how
this image can be run at a later stage from the image itself. This can be more time-efficient
to the user than defining the various apps with input and outputs via the user interface.
Essentially, Kive does this by extracting the `Singularity` definition file that was used
to build the image from the image itself, and looking for Kive-specific keywords in it.
Note that a singularity file created directly from a docker image will not have such a file,
in which case this automatic detection will not work (you must make the app definitions yourself
in the Kive GUI). There's a full example of a Singularity image and a Singularity definition file
in the [`samplecode/singularity`] folder.
Read the [Singularity documentation] for all the details, but here are
the key features that Kive cares about:

* `%runscript` - defines the command that will run for this container's default
    app. You probably want to include `"$@"` in the command arguments to pass
    along the paths to the input and output files.
* `%apprun` - defines the command that will run for another app. Use these to
    define more than one app that runs in the same container.
* `%labels` - defines default configuration for running singularity. You can
    put anything you like in this section, but Kive looks for these labels:
    * `KIVE_INPUTS` - these are just names to tell Kive how many input files
        your command expects. Separate them with spaces.
    * `KIVE_OUTPUTS` - these are just names to tell Kive how many output files
        your command expects. Separate them with spaces.
    * `KIVE_MEMORY` - the number of megabytes of memory to allocate when your
	job runs. If singularity uses more than this, the job will be cancelled by slurm.
    * `KIVE_THREADS` - the number of processors the job will use at one time.
    Kive can't explicitly check this, but you risk overloading the servers if
    your job uses more processors than it asks for.
* `%applabels` - defines default configuration for running one of the apps.
    Kive looks for the same labels as above.
* `%help` - describes the default app. This gets copied to the container
    description.
* `%apphelp` - describes another app. This gets copied to the app description.
* `%environment` and `%appenv` - set environment variables. If you install
    something outside of the default path, you'll need to set `PATH`. If you
    need to encode or decode Unicode text in Python, you'll need to set `LANG`
    to something like `en_CA.UTF-8`.

In other words, if there is a main, 'default' app without a name, then the entries
and labels from `%runscript`, `%labels` and `%help` are used for that app. You can have
any number of additional uniquely named apps defined in the same way using the
`%apprun`, `%applabels` and `%apphelp` entries.

### Building a Singularity Image
Once you've written the `Singularity` definition file, build the image with a command like
this:

    sudo rm -f my_app.simg && sudo singularity build my_app.simg Singularity

That removes any existing image file before starting the build. Otherwise,
Singularity tries to add on to the existing image file. 

### Testing a Singularity Image
If you want to test the image before uploading it to Kive, run it with
commands like this:

    mkdir input
    mkdir output
    cp input_data.csv input
    singularity run my_app.simg --contain --cleanenv \
        -B input:/mnt/input,output:/mnt/output \
        /mnt/input/input_data.csv /mnt/output/output_data.csv
    cat output/output_data.csv

### Uploading a Singularity Image
1. If this is the first version of your container, you'll need to create a new
    container family. Fill in the name, and assign users or groups who are
    allowed to use it.
2. Click on the container family, and click the button to create a new
    container.
  * Choose the Singularity image file you just built.
  * If your scripts are in a version control tool like git, tag the version you
    built from, and record that tag here. Otherwise, just make up a tag name
    for this version.
  * Assign users or groups who are allowed to use this container.
3. Click on the container after you upload it, and check that the apps were
    created correctly. You can also change the settings from the ones defined
    in the Singularity file.
4. Upload some input datasets to Kive, then try launching your container. If it
    fails, look at the stderr log to see what went wrong.

[`samplecode/singularity`]: https://github.com/cfe-lab/Kive/tree/master/samplecode/singularity
[Singularity documentation]: https://www.sylabs.io/guides/2.5/user-guide/

## Archive Files
The other type of container is an archive container. It can be a zip file or a
tar file that holds your scripts.

### Parent Container
An archive container needs to run on top of a Singularity container. If Kive
already has some Singularity containers, pick one of them. Otherwise, you can
build a basic Singularity container from a popular docker image like this:

    sudo singularity pull docker://python:3

Then upload `python-3.simg` as a Singularity container using the instructions
above. You don't need to define any apps for this container. Neither do you
need to install Docker, because Singularity can convert the Docker image for
you.

### Create an Archive
Once you have some scripts that run on your workstation, there are a few steps
to make them run on Kive.

1. Get the paths to the input and output files from the command line. See the
    example above.
2. Tell Kive what language to use. You might be used to running your script
    like `python3 my_script.py`, but Kive doesn't know whether your script is
    in Python, Perl, Bash, or Befunge. Put a comment at the top of your script
    that looks like this: `#!/usr/bin/env python3`. Then, Kive can launch your
    script like `./my_script.py` and it will run with the right command. Do
    this for each of the main scripts that Kive will launch.
3. Put all of the scripts, helper scripts, and data files, into a zip file or
    a tar file. For example, you could use a command like one of these:

    ```shell
    tar cvf greetings.tar greetings.py translations.txt
    ```

    ```shell
    zip greetings.zip greetings.py translations.txt
    ```

### Upload the Archive
Now that you have a zip file or a tar file, log in to Kive and create a new
container. If this is the first version, you'll need to create a container
family first. Pick the singularity container your container will run on, and
upload your archive file.

Once you have created the container, you need to tell Kive which scripts to
run, and what files they will read and write. On the container page, find the
Pipeline section, and click on the Create button. That will take you to a
pipeline page that lets you wire your scripts together into a pipeline.

Click the Add Node menu, and choose New Method. Choose one of your scripts from
the menu, and fill in the inputs and outputs. These are just names to tell Kive
how many input and output files your command expects. Separate them with
spaces.

Repeat for any other scripts that you want to run, then wire the outputs of
some steps to the inputs of others. You can also create inputs for the main
inputs of the pipeline and outputs for the outputs of the pipeline. (Right
click on a method to see some shortcuts for doing that.)

In addition to the wiring, you can also set some defaults for Kive to use when
it launches your pipeline:

* **Memory** - the number of megabytes of memory to allocate. If
    your pipeline uses more than this, the job will be cancelled.
* **Threads** - the number of processors the job will use at one time.
    Kive can't explicitly check this, but you risk overloading the servers if
    your job uses more processors than it asks for.

When everything is wired up, click the Submit button. Check that it created
an app for your pipeline. If not, you may be missing an input on one of your
methods.

Upload some input datasets to Kive, then try launching your container. If it
fails, look at the stderr log to see what went wrong.

### Reusing the Wiring
Once your scripts are wired into a pipeline, Kive adds a `pipeline.json` file
to your archive. You can use that when you copy your pipeline to another copy
of Kive, or when you upload a new version of your scripts.

To copy your pipeline to another copy of Kive, download the archive file and
upload it to another copy of Kive. Then you won't need to redo the wiring.

To reuse the wiring with a new version of your scripts, download the archive
file, and extract the last `pipeline.json` file. Rename it to
`kive/pipeline1.json` and add it the archive file with the new version of your
scripts. Then upload the archive file.
