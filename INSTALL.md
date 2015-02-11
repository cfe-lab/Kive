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

Instructions for downloading and installing PostgreSQL may be found at [postgresql.org](http://www.postgresql.org/).

Instructions for downloading and installing psycopg may be found at [initd.org](http://initd.org/psycopg/).

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
so you are finished. On Ubuntu or CentOS, however, the default is to only accept
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

You may also wish to modify the `TIME_ZONE` setting to your region, although this localization is not strictly necessary.

Another configuration file is `hostfile` in the same folder as `settings.py`.
Copy `hostfile_default` to `hostfile`, and uncomment the `localhost` line. If
you want to launch worker processes on multiple hosts, add a line for each host.
Options are described in the [Open MPI FAQ][mpifaq].

[mpifaq]: http://www.open-mpi.org/faq/?category=running#mpirun-hostfile

Initialize the system
---------------------
To start fresh with a clean system, (re-)deploy the static files and
run the reset command.

    sudo LD_LIBRARY_PATH=:/usr/local/lib ./manage.py collectstatic
    ./manage.py reset --load=demo

You can leave the load parameter off, or set it to other fixture names, like
`converter_pipeline`.

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
8. TODO: Check whether an apache restart is needed. What about the fleet manager?

        ps aux | grep runfleet
        sudo kill <pid for runfleet>
        sudo /usr/sbin/apachectl restart
        sudo -u apache LD_LIBRARY_PATH=$LD_LIBRARY_PATH PATH=$PATH ./manage.py runfleet --workers 151 &>/dev/null &

9. Remove the pre-release flag from the release.
10. Close the milestone for this release, create one for the next release, and
    decide which issues you will include in that milestone.

[release]: https://help.github.com/categories/85/articles
