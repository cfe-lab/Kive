Installation Instructions
=========================

Prerequisites
-------------
**shipyard** is a set of Django applications.  As a result, it has the following requirements:

1. Python 2.x (version 2.7 or higher) - unfortunately we do not support Python 3.x.
2. Django (version 1.6 or higher)
3. OpenMPI
4. mpi4py
5. numpy
6. PostgreSQL
7. psycopg (Python library for interfacing with PostgreSQL)

It also requires the [Expect automation tool](http://sourceforge.net/projects/expect/) to run some configuration steps.

Source code or binaries for Python can be obtained from the official website, [python.org](www.python.org).  Most *nix distributions (including OS X) come with some version of Python.  

Instructions for downloading and installing Django can be found at [djangoproject.com](https://www.djangoproject.com/download/).

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
so you are finished. On Ubuntu, however, the default is to only accept
connections from system users. To allow the shipyard database user to connect,
you have to change the [authentication setting][pg_hba] in PostgreSQL's
configuration file. Replace 9.3 with whichever version you have.

    sudo vi /etc/postgresql/9.3/main/pg_hba.conf
    # Add the following line after the default for user postgres
    local   all             shipyard                                md5
    # Then save the file and reload the PostgreSQL configuration
    /etc/init.d/postgresql reload

To test that the `shipyard` user can connect to the `shipyard` database, connect
with `psql` and then exit.

    psql shipyard shipyard
    \q

[digitalocean]: https://www.digitalocean.com/community/tutorials/how-to-install-and-configure-django-with-postgres-nginx-and-gunicorn
[test-permission]: http://stackoverflow.com/q/14186055/4794
[pg_hba]: http://stackoverflow.com/a/18664239/4794

Settings
--------
Since **shipyard** is a Django project, the majority of the installation procedure follows the standard instructions for Django.  The first thing you need to do is to make a copy of `/shipyard/settings_default.py` called `settings.py` (remember, all paths are relative to `/shipyard` so we mean `/shipyard/shipyard/settings_default.py`).  This is a standard step in the installation of a Django project where you configure project settings.  Within the `DATABASES['default']` dictionary, modify the respective values to indicate the type, location, and access credentials of your database.  For example, using postgres as your database engine, you would specify `'django.db.backends.postgresql_psycopg2'` under the `ENGINE` key, and the name of the database Shipyard is to use under the key `NAME` (e.g. `'shipyard'`).  This is a database that must be created by an admistration prior to using Shipyard.

You may also wish to modify the `TIME_ZONE` setting to your region, although this localization is not strictly necessary.



Initialize database
-------------------
If you have just created a new database, run the bash script `./initDB.bash`.
Whenever you make database changes, you can update the tables and wipe out all
the data by running the bash script `./nukeDB.bash`. There are also some other
versions of the nuke script that load different sets of sample data.

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

This may have slightly different behaviour from the PostgreSQL database, so you
should occasionally run the tests with the default settings. See [the Django
documentation][unit-tests] for details on running specific tests.

[unit-tests]: https://docs.djangoproject.com/en/dev/topics/testing/overview/#running-tests
