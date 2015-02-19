Installation Instructions
=========================

Prerequisites
-------------
**shipyard** is a set of Django applications.  As a result, it has the following requirements:

1. Python 2.x (version 2.7 or higher) - unfortunately we do not support Python 3.x.
2. Django (version 1.7 or higher)
3. OpenMPI
4. mpi4py
5. numpy
6. PostgreSQL
7. psycopg (Python library for interfacing with PostgreSQL)

It also requires the [Expect automation tool](http://sourceforge.net/projects/expect/) to run some configuration steps.

Source code or binaries for Python can be obtained from the official website, [python.org](www.python.org).  Most *nix distributions (including OS X) come with some version of Python.  

Instructions for downloading and installing Django can be found at [djangoproject.com](https://www.djangoproject.com/download/).
If you are already running Django 1.6, installing should be as painless as running

    pip install Django==1.7.4

You will likely need to run this using `sudo` unless you only installed Django locally and not
system-wide.  Also, many systems have multiple Python installations, so make sure that 
`pip` is using the correct one.

Installing PostgreSQL
---------------------
Instructions for downloading and installing PostgreSQL may be found at [postgresql.org](http://www.postgresql.org/).
PostgreSQL is available as pre-compiled binary executables.  On Linux systems, the easiest way to obtain the binaries is to use a package manager such as `yum` or `apt-get`, depending on your distribution.  On Mac OS-X, there are number of methods including third-party installers and package managers such as `MacPorts` or `homebrew`.  The only option listed on the PostgreSQL website for installing PostgreSQL for Windows is a third-party installer distributed by [EnterpriseDB](http://www.enterprisedb.com/).

#### Installation on OS-X with MacPorts
[MacPorts](https://www.macports.org/) is a package manager for OS-X that installs packages into a separate directory at `/opt` so that any files installed by MacPorts can be removed without compromising the original filesystem.
Instructions for installing MacPorts can be found [here](https://www.macports.org/install.php).

Install the PostgreSQL server port by the following:

    sudo port install postgresql93-server

This will create a new user on your system called `postgres`.  To confirm that this user account has been created, use the command

    dscl . list /Users | grep postgres

which should return `postgres`.  
MacPorts will have installed a number of files in different locations under the `/opt` tree including  `/opt/local/bin`, `/opt/local/include/postgresql93` and `/opt/local/lib/postgresql93`.  
A number of utility programs are installed at `/opt/local/lib/postgresql93/bin`.  To make it more convenient to access these utilities, add this path to the `postgres` user's `$PATH` environment variable, log in as `postgres` and create a bash profile:

    sudo su - postgres
    vi .profile

In the `vi` editor, type `O` to open the file for editing and write this line:

    export PATH=/opt/local/lib/postgresql93/bin:$PATH



To create a database install, MacPorts also provides the following instructions:

    sudo mkdir -p /opt/local/var/db/postgresql93/defaultdb
    sudo chown postgres:postgres /opt/local/var/db/postgresql93/defaultdb
    sudo su postgres -c '/opt/local/lib/postgresql93/bin/initdb -D /opt/local/var/db/postgresql93/defaultdb' 

This creates a new folder under the `/opt` MacPorts tree that is owned by the `postgres` user, and then initializes a new PostgreSQL database in this new folder called `defaultdb`.  Note that the path (`/opt/local/var/db/postgresql93`) and name (`defaultdb`) for the database are optional.

Now that we've created a database, we need to start a server to handle database transactions.  Based on the instructions provided on the [PostgreSQL website](http://www.postgresql.org/docs/9.3/static/creating-cluster.html), transfer ownership of the `postgres` user's home directory to itself:

    sudo chown postgres:postgres /opt/local/var/db/postgresql93


login as the `postgres` user

    sudo su - postgres

and start the server as a background process by this command

    postgres -D /opt/local/var/db/postgresql93/defaultdb >logfile 2>&1 &

which redirects standard output and error messages to a file `logfile`.
A slightly less obtuse way to issue this command is to call the `pg_ctl` utility from the `postgres` user home directory:

    pg_ctl -D defaultdb -l logfile start


The PostgreSQL server will be automatically started when you reboot your system through a LaunchDaemon script that was installed by MacPorts at `/Library/LaunchDaemons/org.macports.postgresql93-server.plist`.  To inspect what this script is actually doing, you can look at the contents of the wrapper `/opt/local/etc/LaunchDaemons/org.macports.postgresql93-server/postgresql93-server.wrapper`.
Note that if you used a database location or name other than the MacPorts default, then you may need to modify this wrapper accordingly.



Installing psycopg
------------------
Psycopg is a PostgreSQL adaptor for Python.  It is mandatory in order for Django to use a PostgreSQL database.  Instructions for downloading and installing psycopg2 may be found at [initd.org](http://initd.org/psycopg/).

#### Installing psycopg2 in OS-X

First, note that OS-X ships with its own version of Python (often referred to as System Python).  However, this version may be older and lack features required by open source software.  System Python also has some irregularities about the installation of modules that can complicate the installation and upgrading process of third-party modules.  For such reasons, users often like to install another version of Python.  

If you are using a MacPorts binary of Python, you can easily install the psycopg2 port, by running

    sudo port install py27-psycopg2

This may also install a number of dependencies if they are not already present on your system, such as `libxslt`.

If you compiled your own version of Python from source, then you can obtain the source code for psycopg2 [here](http://initd.org/psycopg/download/).
Compiling pscyopg2 requires the `pg_config` executable that might not be in your `$PATH` -- for example, if you used MacPorts to install the PostgreSQL server, it is located at `/opt/local/lib/postgresql93/bin/pg_config`.  
You can direct the `setup.py` script to this executable by modifying the file `setup.cfg` and editing the last line in the block:

    # "pg_config" is required to locate PostgreSQL headers and libraries needed to
    # build psycopg2. If pg_config is not in the path or is installed under a
    # different name uncomment the following option and set it to the pg_config
    # full path.
    pg_config

so that it reads

    pg_config=/opt/local/lib/postgresql93/bin/pg_config

Then you can compile and install this module by running `sudo python setup.py install`.
To confirm that the module is installed, start an interaction session by calling `python` on the command line and then enter `import psycopg2`.  If this raises an `ImportError` then something has gone wrong - for example, the version of Python used to install the module is different from the version running the interactive session.


Project structure
-----------------
The root directory of **shipyard** should contain the following subdirectories:
* `/doc`
* `/samplecode`
* `/shipyard`

`/shipyard` is the top-level directory for the Django package that contains the project subdirectory (that by convention has the same name as the project folder, 'shipyard'), as well as a number of application subdirectories.  From now on, we will assume that you are in this project directory; *i.e.*, all paths will be defined relative to this directory.


Create database
---------------
**shipyard** uses PostgreSQL as its default database backend, and it must be set
up prior to using **shipyard**.  The following instructions are based on step
seven of the instructions from [digitalocean.com][digitalocean]. Feel free to
change the user name or database name to something other than "shipyard".  

First, install PostgreSQL and psycopg as is appropriate for your system.  During
the setup, a `postgres` system user account should have been set up: this
account is the administrator of the database.  To set up the database, log into
this user account:

    sudo su - postgres

Create a database for **shipyard**.

    createdb shipyard

Next, create a user (or "role") for **shipyard** to use when accessing the
database, and follow the prompts (the `-P` allows you to specify a password):

    createuser -P shipyard

Now, we need to grant this user the appropriate privileges.  As the postgres
system user, using the `psql` SQL console, enter at the prompt:

    GRANT ALL PRIVILEGES ON DATABASE shipyard TO shipyard;

We are almost done.  In order to run the **shipyard** test suites, the
`shipyard` database account must have the ability to create temporary test
databases.  As instructed on [Stack Overflow][test-permission], we grant the
user this privilege by running a command in `psql`:

    ALTER USER shipyard CREATEDB;

Exit `psql` with the `\q` command, then exit from the postgres user's shell to
get back to your regular prompt.

On a Mac, the PostgreSQL database defaults to accept connections from any user,
so you are finished creating the database.  On Ubuntu or CentOS, however, the default is to only accept
connections from system users. To allow the shipyard database user to connect,
you have to change the [authentication setting][pg_hba] in PostgreSQL's
configuration file. Replace 9.3 with whichever version you have.

    sudo vi /etc/postgresql/9.3/main/pg_hba.conf
    # Add the following line before the default for user postgres or all
    local   all             shipyard                                md5
    # Then save the file and reload the PostgreSQL configuration
    /etc/init.d/postgresql reload

To test that the `shipyard` user can connect to the `shipyard` database, connect
with `psql` and then exit.

    psql shipyard shipyard
    \q


Creating database tables
------------------------
Having created the database, we must now create the tables that will be used by
Shipyard.  This is handled by the `migrate` command to the `manage.py` script,
which follows instructions created by the developers on how to lay out the tables:
    ./manage.py migrate

Should you ever need to completely remove the database and start over, you can run
the following commands:

    sudo su - postgres  # enter password if it asks
    dropdb shipyard  # enter shipyard user's password if it asks
    createdb shipyard  # etc.
    exit
    ./manage.py migrate

This follows the above steps, except with an additional step that removes the
shipyard database, and skipping the already-done configuration steps.

[digitalocean]: https://www.digitalocean.com/community/tutorials/how-to-install-and-configure-django-with-postgres-nginx-and-gunicorn
[test-permission]: http://stackoverflow.com/q/14186055/4794
[pg_hba]: http://stackoverflow.com/a/18664239/4794


Remove test database (if necessary)
-----------------------------------
Sometimes, if something goes wrong while running the test suite, the system
will leave a database named `test_shipyard` in place, which will give the
following prompt the next time you run the tests:

    Creating test database for alias 'default'...
    Got an error creating the test database: database "test_shipyard" already exists

    Type 'yes' if you would like to try deleting the test database 'test_shipyard', or 'no' to cancel:

Then, on entering `yes`:

    Destroying old test database 'default'...
    Got an error recreating the test database: must be owner of database test_shipyard

To properly dispose of this database, run the following commands:

    sudo su - postgres  # enter password if it asks
    dropdb test_shipyard  # enter shipyard user's password if it asks
    exit
    
This is similar to the incantation used to completely remove the `shipyard` database,
but simpler because we are only trying to get rid of the database and do not need to
recreate or reinitialize anything.

Settings
--------
Since **shipyard** is a Django project, the majority of the installation
procedure follows the standard instructions for Django.  The first thing you
need to do is to make a copy of `/shipyard/settings_default.py` called
`settings.py` (remember, all paths are relative to `/shipyard` so we mean
`/shipyard/shipyard/settings_default.py`).  This is a standard step in the
installation of a Django project where you configure project settings.  Within
the `DATABASES['default']` dictionary, modify the respective values to indicate
the type, location, and access credentials of your database.  For example, using
postgres as your database engine, you would specify
`'django.db.backends.postgresql_psycopg2'` under the `ENGINE` key, and the name
of the database Shipyard is to use under the key `NAME` (e.g. `'shipyard'`).
This is a database that must be created by an administrator prior to using
Shipyard.

Set `MEDIA_ROOT` to the absolute path of a directory that can hold all the
working files for the server and any uploaded files. 

You may also wish to modify
the `TIME_ZONE` setting to your region, although this localization is not
strictly necessary.

Another configuration file is `hostfile` in the same folder as `settings.py`.
Copy `hostfile_default` to `hostfile`, and uncomment the `localhost` line. If
you want to launch worker processes on multiple hosts, add a line for each host.
Options are described in the [Open MPI FAQ][mpifaq].

[mpifaq]: http://www.open-mpi.org/faq/?category=running#mpirun-hostfile

Initialize the system
---------------------
The following instructions will initialize your system with a clean instance.
If you are running Kive as a production server with Apache, (re-)deploy the static files by running the following command.  If you are running Kive as a development server on your workstation, then there is no need to collect static files and you can ignore this.

    sudo LD_LIBRARY_PATH=:/usr/local/lib ./manage.py collectstatic

Next, clear and re-populate the database with the following command:

    ./manage.py reset --load=demo

You can leave the load parameter off, or set it to other fixture names, like
`converter_pipeline`.  `demo` refers to a set of fixture files that populate the database
with two pipelines that were used in the development of Kive, and for demonstrating the software.

You are now ready to run a local Django webserver:

    python manage.py runserver

Then navigate to `localhost:8000` in your web browser!

To launch a fleet manager and workers, you need to run the following command
and replace X with the number of workers you want:

    python manage.py runfleet --workers X

Running a pipeline
------------------
Load a sample pipeline by running the `./nukeForDemo.bash` or `./nukeSimple.bash`.
Then make sure the fleet is running as described above. Finally, navigate to
the user portal, analysis, and launch the pipeline. 

Building the documentation
--------------------------
The project uses LaTeX for some of its documentation, so you might want to install LaTeX to build it.
On Ubuntu, you need to install the following packages:

    sudo apt-get install texlive texlive-latex-extra texlive-fonts-extra

To build a LaTeX file into PDF format, use the `pdflatex` command.

Running unit tests
------------------
If you want to run your unit tests faster, you can run them against an
in-memory SQLite database with this command:

    ./manage.py test --settings shipyard.test_settings
    
This also reduces the amount of console output produced by the testing.  
Testing with a SQLite database may have slightly different behaviour from 
the PostgreSQL database, so you should occasionally run the tests with 
the default settings.  Alternatively, to run the tests with all the default
settings but with reduced console output:
    
    ./manage.py test --settings shipyard.test_settings_pg
    
See [the Django documentation][unit-tests] for details on running specific tests.

If you want to time your unit tests to see which ones are slowest, [install
HotRunner][hotrunner].

    sudo pip install django-hotrunner

Then add these two lines to `settings.py`:

    TEST_RUNNER = 'hotrunner.HotRunner'
    HOTRUNNER_XUNIT_FILENAME = 'testreport.xml'

Finally, run the unit tests and the script to summarize them.

    ./manage.py test --settings shipyard.test_settings
    ./slow_test_report.py

[unit-tests]: https://docs.djangoproject.com/en/dev/topics/testing/overview/#running-tests
[hotrunner]: https://pypi.python.org/pypi/django-hotrunner/0.2.2

Deploying a Release
===================
See the project wiki for instructions on how to [start a production server][wiki].
Once you have set up your production server, this is how to deploy a new release:

1. Make sure the code works in your development environment. Run all the unit
    tests.
    
    ./manage.py test
    
2. Check that all the issues in the current milestone are closed.
3. [Create a release][release] on Github. Use "vX.Y" as the tag, where X.Y
    matches the version on the milestone. If you have to redo
    a release, you can create additional releases with tags vX.Y.1, vX.Y.2, and
    so on. Mark the release as pre-release until you finish deploying it.
4. TODO: Check whether there are problems with doing a deployment while a run
    is executing. Does restarting apache restart the run?
5. Get the code from Github onto the server.

        ssh user@server
        cd /usr/local/share/Shipyard/shipyard
        git fetch
        git checkout tags/vX.Y

6. Check if you need to set any new settings by running
    `diff shipyard/settings_default.py shipyard/settings.py`. Do the same
    comparison of `hostfile`.
7. Recreate the database as described in the Initialize Database section, and
    deploy the static files.
    
        ssh user@server
        cd /usr/local/share/Shipyard/shipyard
        ./manage.py migrate
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./manage.py collectstatic
        
8. TODO: Check whether an apache restart is needed. What about the fleet manager?

        ps aux | grep runfleet
        sudo kill <pid for runfleet>
        sudo /usr/sbin/apachectl restart
        sudo -u apache LD_LIBRARY_PATH=$LD_LIBRARY_PATH PATH=$PATH ./manage.py runfleet --workers 151 &>/dev/null &

9. Remove the pre-release flag from the release.
10. Close the milestone for this release, create one for the next release, and
    decide which issues you will include in that milestone.

[release]: https://help.github.com/categories/85/articles
