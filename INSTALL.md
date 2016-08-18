Installation Instructions
=========================

Prerequisites
-------------

Before installing Kive, you need to install some other software.

* Python 2.x (version 2.7 or higher) - unfortunately we do not support Python 3.x.
* Django (version 1.9 or higher)
* the Django REST framework (version 3.3 or higher)
* PostgreSQL
* psycopg2 (Python library for interfacing with PostgreSQL)
* pytz
* OpenMPI (optional, for running Kive on a cluster)
* mpi4py (optional, for running Kive on a cluster)
* django-extensions (optional, for creating a UML diagram of the backend
  models used to store all the records in the database)
* pygraphviz (optional, for the UML diagram)
* requests (optional, for using the Kive API wrapper to make API calls)


Installing Python
-----------------

Source code or binaries for Python can be obtained from the official website,
[python.org](www.python.org).  Most *nix distributions (including OS X) come
with some version of Python. CentOS 6.7 requires you to install Python 2.7 as
a software collection.

    scl enable python27 bash

The fleet workers launch very slowly on compute nodes with a network file
system. They will launch much faster if you run the fleet within a Python
[virtualenv][]. Check to see the latest version of virtualenv in the
[Python package index][pypi]. This is optional, but you can create the
virtualenv as follows:

    # Create a bootstrap environment in the kive user's home folder
    sudo su kiveuser
    curl -O https://pypi.python.org/packages/source/v/virtualenv/virtualenv-X.Y.Z.tar.gz
    tar xzf virtualenv-X.Y.Z.tar.gz
    python virtualenv-X.Y.Z/virtualenv.py vbootstrap
    # Install virtualenv into the bootstrap virtual environment
    vbootstrap/bin/pip install virtualenv-X.Y.Z.tar.gz
    rm -rf virtualenv-X.Y.Z virtualenv-X.Y.Z.tar.gz
    # Now create a virtualenv just for Kive fleet workers.
    vbootstrap/bin/virtualenv vkive
    # Start using the virtualenv
    source vkive/bin/activate

[virtualenv]: http://docs.python-guide.org/en/latest/dev/virtualenvs/ 
[pypi]: https://pypi.python.org/pypi/virtualenv

Installing Django
-----------------

Instructions for downloading and installing Django can be found at [djangoproject.com](https://www.djangoproject.com/download/).
If you are already running an earlier version of Django, installing should be
as painless as running

    pip install Django==1.9.2

(substitute the appropriate version number if you wish to use a different version -- a newer one, for example).  You will likely need to run this using `sudo` unless you only installed Django locally and not
system-wide.  Also, many systems have multiple Python installations, so make sure that
`pip` is using the correct one. Django REST Framework can also be installed via

    pip install djangorestframework==3.3.2
    
You will also need the `pytz` module, which can simply be installed with

    pip install pytz

If you want to generate UML diagrams for your models, install
`django-extensions`. Instructions for downloading and installing may be found
[here](http://django-extensions.readthedocs.org/en/latest/installation_instructions.html).
The easiest way is to use `pip` again.

    pip install django-extensions

Installing PostgreSQL
---------------------
Instructions for downloading and installing PostgreSQL may be found at [postgresql.org](http://www.postgresql.org/).
PostgreSQL is available as pre-compiled binary executables.  On Linux systems, the easiest way to obtain the binaries is to use a package manager such as `yum` or `apt-get`, depending on your distribution.  On Mac OS-X, there are number of methods including third-party installers and package managers such as `MacPorts` or `homebrew`.  The only option listed on the PostgreSQL website for installing PostgreSQL for Windows is a third-party installer distributed by [EnterpriseDB](http://www.enterprisedb.com/).

#### Installing on OS X

##### Using the Graphical Installer provided by EnterpriseDB
To install PostgreSQL under OS X using the graphical installer (download [here](http://www.enterprisedb.com/products-services-training/pgdownload#osx)), the instructions for Windows and PostgreSQL 9.3 are [here](http://www.enterprisedb.com/docs/en/9.3/pginstguide/PostgreSQL_Installation_Guide-07.htm#TopOfPage)); the procedures for OS X, and for different versions of the database, are similar.  Take note of the install directory for Postgres and the data directory, as well as the port on which the server listens.  On a default installation, Postgres is installed in `/Library/PostgreSQL/9.3`, and the data directory is `/Library/PostgreSQL/9.3/data`.  (All paths are given assuming version 9.3; substitute the appropriate version number for your system.)  We will need some of the utilities installed in `/Library/PostgreSQL/9.3/bin` later.  It will also ask you to specify a password for the default system `postgres` user.  You will have to be logged in as this user to perform several necessary administrative tasks, so be sure to remember this password!  At the end of the installation, it will ask if you wish to start Stack Builder.  Everything we need should already be installed, so you can pass on this.

After installation, Postgres will have been automatically launched in the background, and configured to automatically start on reboot using a LaunchDaemon script at (in a default installation) `com.edb.launchd.postgresql-9.3.plist`.  A `postgres` user will also have been installed on your computer, whose home directory is `/Library/PostgreSQL/[version number]`.

##### Using MacPorts
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

Log in as the `postgres` user:

    sudo su - postgres

and start the server as a background process using this command:

    postgres -D /opt/local/var/db/postgresql93/defaultdb >logfile 2>&1 &

This redirects standard output and error messages to a file `logfile`.
A slightly less obtuse way to issue this command is to call the `pg_ctl` utility from the `postgres` user home directory:

    pg_ctl -D defaultdb -l logfile start

The PostgreSQL server will be automatically started when you reboot your system through a LaunchDaemon script that was installed by MacPorts at `/Library/LaunchDaemons/org.macports.postgresql93-server.plist`.  To inspect what this script is actually doing, you can look at the contents of the wrapper `/opt/local/etc/LaunchDaemons/org.macports.postgresql93-server/postgresql93-server.wrapper`.
Note that if you used a database location or name other than the MacPorts default, then you may need to modify this wrapper accordingly.

#### Installing on Ubuntu

Install the database server, client, and administrative tools with the
package manager.

    sudo apt-get install postgresql postgresql-contrib postgresql-client

Create a password for the `postgres` user.

    sudo -u postgres psql postgres
    \password postgres

Press Control+D twice to exit the Postgres prompt and log out of the
postgres account.

The database server should already have been started when you installed 
postgresql, and will be automatically restarted on reboot. If you need
to start or restart the server, see `man pg_ctlcluster`.

The postgresql data directory will have been put in a default place,
likely `/var/lib/postgresql/9.3/main`. You can figure out where the data
directory is by typing `show data_directory;` into the postgres prompt.
Don't worry about creating a database yet, that will get done later on.

Typically Postgres caps the number of simultaneous database connections
  at 100.  This can be a problem if you intend to run more than 100 worker
  processes (i.e. processes that will handle running your pipelines).  To
  change the cap, you should change the `max_connections` setting in the
  Postgres config file (usually `postgresql.conf` in Postgres' data 
  directory).  For example, if you are running 191 workers, then you might
  add the line
  
    max_connections = 200
  
  to this file (this affords you some breathing room to access the database
  outside of Kive as well).  Then restart the Postgres server.


Installing psycopg2
------------------
Psycopg is a PostgreSQL adaptor for Python.  It is mandatory in order for Django to use a PostgreSQL database.  Instructions for downloading and installing it may be found at [initd.org](http://initd.org/psycopg/).

#### Installing psycopg2 in OS-X

First, note that OS-X ships with its own version of Python (often referred to as System Python).  However, this version may be older and lack features required by open source software.  System Python also has some irregularities about the installation of modules that can complicate the installation and upgrading process of third-party modules.  For such reasons, users often like to install another version of Python.  

##### Using MacPorts
If you are using a MacPorts binary of Python, you can easily install the psycopg2 port, by running

    sudo port install py27-psycopg2

This may also install a number of dependencies if they are not already present on your system, such as `libxslt`.

#### Using apt-get

If you are on Ubuntu, psycopg2 can be installed with the package
manager.

    sudo apt-get install python-psycopg2

##### Using pip
The developers recommend using a binary package where possible, using fink or ports.
However, if you don't want to do this or if it isn't feasible -- for example, if you're using either
the system Python on a Mac or an official version obtained from [python.org][www.python.org] -- it's relatively
easy to install using `pip`: instructions may be found [here](http://initd.org/psycopg/docs/install.html#install-from-source).

Some key hangups you may encounter when installing using this method are making sure that you have the `pg_config` utility 
on your PATH; this is one of the utilities installed by PostgreSQL.  If you installed PostgreSQL using the graphical installer, this is typically at `/Library/PostgreSQL/9.3/bin`; in a typical MacPorts install it is located at `/opt/local/lib/postgresql93/bin/pg_config`.  You may also need to update your LD_LIBRARY_PATH and your DYLD_FALLBACK_LIBRARY_PATH to point at the directory where PostgreSQL installed its libraries.  Depending on your system, you may also encounter problems because your system's libraries are not the right versions required by Psycopg.  In these cases, a package manager such as MacPorts may be of use.

##### Directly from source
You may also wish to directly compile psycopg2 from the source (available [here](http://initd.org/psycopg/download/)).  The same gotchas as above apply.  An alternative to putting `pg_config` on your PATH is to direct the `setup.py` script to this executable by modifying the file `setup.cfg` and editing the last line in the block:

    # "pg_config" is required to locate PostgreSQL headers and libraries needed to
    # build psycopg2. If pg_config is not in the path or is installed under a
    # different name uncomment the following option and set it to the pg_config
    # full path.
    pg_config

so that it reads

    pg_config=/opt/local/lib/postgresql93/bin/pg_config

Then you can compile and install this module by running `sudo python setup.py install`.

To confirm that the module is installed, start an interactive session by calling
`python` on the command line and then enter `import psycopg2`.  If this raises
an `ImportError` then something has gone wrong - for example, the version of
Python used to install the module is different from the version running the
interactive session.

Installing OpenMPI and mpi4py
-----------------------------

### Ubuntu

Just directly install the mpi4py library with

    sudo apt-get install python-mpi4py

This will automatically install the necessary components of OpenMPI.

### CentOS 6.7
Install `mpi4py` into the `vkive` virtual environment using `pip`.

    pip install mpi4py

There are several versions of MPI available as modules. The only version
currently compatible with `mpi4py` is 1.6.5.

    module load openmpi/gnu/1.6.5

Project structure
-----------------
Download the Kive source code, and install it in a location where the apache
user can access it.

The root directory of Kive should contain the following subdirectories:
* `/api`
* `/doc`
* `/samplecode`
* `/kive`

`/kive` is the top-level directory for the Django package that contains the project subdirectory (that by convention has the same name as the project folder, 'kive'), as well as a number of application subdirectories.  From now on, we will assume that you are in this project directory; *i.e.*, all paths will be defined relative to this directory.


Create database
---------------
Kive uses PostgreSQL as its default database backend, and it must be set
up prior to using Kive.  The following instructions are based on step
seven of the instructions from [digitalocean.com][digitalocean]. Feel free to
change the user name or database name to something other than "kive".  

First, install PostgreSQL and psycopg as is appropriate for your system.  During
the setup, a `postgres` system user account should have been set up: this
account is the administrator of the database.  To set up the database, log into
this user account:

    sudo su - postgres

Create a database for Kive.  If the Postgres utilities have not been
automatically added to your PATH (they won't have been if you followed the above instructions
installing the database using the graphical installer), make sure you specify the path
of the commands in the following:

    createdb kive

Next, create a database user (or "role") for Kive to use when accessing the
database, and follow the prompts (the `-P` allows you to specify a password):

    createuser -P kive

Now, we need to grant this user the appropriate privileges.  As the postgres
system user, using the `psql` SQL console, enter at the prompt:

    GRANT ALL PRIVILEGES ON DATABASE kive TO kive;

We are almost done.  In order to run the Kive test suites, the
`kive` database account must have the ability to create temporary test
databases.  As instructed on [Stack Overflow][test-permission], we grant the
user this privilege by running a command in `psql`:

    ALTER USER kive CREATEDB;

Exit `psql` with the `\q` command, then exit from the postgres user's shell to
get back to your regular prompt.

On a Mac, the PostgreSQL database defaults to accept connections from any user,
so you are finished creating the database.  On Ubuntu or CentOS, however, the default is to only accept
connections from system users. To allow the kive database user to connect,
you have to change the [authentication setting][pg_hba] in PostgreSQL's
configuration file. Replace 9.3 with whichever version you have.

    sudo vi /etc/postgresql/9.3/main/pg_hba.conf
    # Add the following line before the default for user postgres or all
    local   all             kive                                md5
    # Then save the file and reload the PostgreSQL configuration
    sudo /etc/init.d/postgresql reload

To test that the `kive` user can connect to the `kive` database, connect
with `psql` and then exit.

    psql kive kive
    \q

Restricted user for running user-submitted code
-----------------------------------------------

By its nature, Kive runs user-submitted code under an account that does not
necessarily belong to that user.  For example, on a development machine, this 
code will typically run as the developer (i.e. you), and on a production 
machine, code may run as the `apache` user (in the examples below, this user 
is named `kiveuser`).  This poses obvious security risks.  To reduce these risks, 
Kive supports running pipelines as another user -- preferably an unprivileged one 
that cannot access any vital system information such as the database -- via SSH 
on localhost.

Before this can be enabled, you must create a suitable user account (let's say 
this user is named `sandboxworker`) however is typical on your system.  If you're
running on a production system, you should also create another user account under
which to run the Kive fleet, so that `apache` isn't running the fleet; i.e. 
`kiveuser` should not be `apache`.  We'll explain why shortly.

Next, create a system group that contains both this user and the 
`kiveuser` user, whoever that is on your system.  This group will represent all
users that are able to access or modify the Kive sandboxes in which our pipelines
run.  You may be tempted to simply use a group named `kive`, but be careful in 
doing so: if such a group exists, and all Kive administrators belong to it, then
the files your Kive application directory may belong to that group.  If files such as
`settings.py` belong to this group, then adding `sandboxworker` to this group 
grants the user access to potentially sensitive information -- including the
database.  In our examples below, this group will be called `kiveprocessing`.

Third, make sure that `sandboxworker` is able to access any system software 
required by the Methods in the system.  For example, if any Methods in your
Kive system require Python, then `sandboxworker` needs to be able to run Python.
This also includes any libraries, modules, or plugins to this software: Python
modules, R libraries, Ruby gems, and the like.  This may require modifying the
user's `.bashrc` file.

Because the files created by `sandboxworker` will be owned by `sandboxworker`,
but need to be cleaned up by Kive (running as `kiveuser`), ensure that 
`sandboxworker` creates files that are writable by their group; Kive will make
sure that these files belong to the `kiveprocessing` group so that it can 
remove them.  To do this, include the command `umask 0002` in `sandboxworker`'s 
`.bashrc` file.

Fourth, set up passwordless SSH access to the `sandboxworker` account from the
normal Kive account.  (This is why having `kiveuser` not be `apache` is a good 
idea: granting the webserver any extra privileges beyond the defaults is risky.)
Typically this proceeds as follows:

- Produce an SSH private/public key pair with no passphrase for `kiveuser` if 
  that user doesn't already have one.
  
    (To see if you already have one, look for a file named `id_rsa.pub` in the
  `~kiveuser/.ssh` directory.  Try skipping to the next step and see if you
  are able to `ssh` into `sandboxworker@localhost` without a passphrase.  If so,
  you are done; if not, you'll need to follow these instructions and set up another
  key pair that does not have a passphrase.)
  
    To create this key pair, log in as `kiveuser` and run the command

        ssh-keygen -t rsa

    When it prompts you for a passphrase, leave it blank.  Take note of where
  it places the `id_rsa.pub` file; typically this will be in the directory
  `~kiveuser/.ssh/id_rsa.pub`.  Make this file accessible to `sandboxworker`.
  
- As `sandboxworker`, add the contents of the `id_rsa.pub` file that was just 
  created to `.ssh/authorized_keys` file:
  
        cat id_rsa.pub >> ~/.ssh/authorized_keys
        
- As `sandboxworker`, make sure that the `.ssh` directory has `rwx` permissions 
  for the user and none for anyone else, and that the `~/.ssh/authorized_keys` 
  file has `rw` permissions for the user, `r` for the group, and none for
  anyone else.  To do this:
  
        chmod 700 ~/.ssh
        chmod 640 ~/.ssh/authorized_keys
    
- As `kiveuser`, attempt to SSH into the system as `sandboxworker` with the 
  command `ssh sandboxworker@localhost`.  If this is the first time you've
  ever done this, it will produce a prompt:
  
        The authenticity of host '12.34.56.78 (12.34.56.78)' can't be established.
        RSA key fingerprint is b1:2d:33:67:ce:35:4d:5f:f3:a8:cd:c0:c4:48:86:12.
        Are you sure you want to continue connecting (yes/no)?
        
    Enter `yes` to finish the setup, then log out of the SSH session.
      
    You may wish to verify that passwordless SSH is properly set up by running
    `ssh sandboxworker@localhost` one more time.  This time, it should take you
    directly to a login shell without any prompts.

Details of setting up Kive to execute code as this user may be found below.

Settings
--------
Since Kive is a Django project, the majority of the installation
procedure follows the standard instructions for Django.  The first thing you
need to do is to make a copy of `/kive/settings_default.py` called
`/kive/settings.py` (remember, all paths are relative to `/kive` so we mean
`/kive/kive/settings_default.py`).  This is a standard step in the
installation of a Django project where you configure project settings.  Within
the `DATABASES['default']` dictionary, modify the respective values to indicate
the type, location, and access credentials of your database.  For example, using
postgres as your database engine, you would specify
`'django.db.backends.postgresql_psycopg2'` under the `ENGINE` key, and the name
of the database Kive is to use under the key `NAME` (e.g. `'kive'`).
This is a database that must be created by an administrator prior to using
Kive.

Set `MEDIA_ROOT` to the absolute path of a directory that can hold all the
working files for the server and any uploaded files. Create the directory if it
doesn't already exist.

Set `STATIC_ROOT` to the absolute path of a directory that can hold static files
for the web server to serve. You don't need this setting for a development
server on your workstation.

You may also wish to modify the `TIME_ZONE` setting to your region, although 
this localization is not strictly necessary.

It's easiest to leave `DEBUG` set to `True`, but that can consume a lot of
memory after you run several pipelines. If you want to process a lot of
data in your development environment, you will probably need to set it to
`False`. However, that means you'll have to set
`ALLOWED_HOSTS` to `['localhost']`. When you launch the server, you'll also
need to call `./manage.py runserver --insecure`. That lets it serve static
files.

Another configuration file is `hostfile` in the same folder as `settings.py`.
Copy `hostfile_default` to `hostfile`, and uncomment the `localhost` line. If
you want to launch worker processes on multiple hosts, add a line for each host.
Options are described in the [Open MPI FAQ][mpifaq].

[mpifaq]: http://www.open-mpi.org/faq/?category=running#mpirun-hostfile

#### Enabling Kive to run code as an unprivileged user

Assuming that you've set up an unprivileged user as in the "Restricted user 
for running sandboxes" section, enable running code as this user by 
setting the value of `KIVE_SANDBOX_WORKER_ACCOUNT` and `KIVE_PROCESSING_GROUP` to the
appropriate values (e.g. `sandboxworker` and `kiveprocessing`, if you followed the
above instructions exactly).

If this is a fresh install, Kive will make sure the `Sandboxes` directory is readable
and writable by the appropriate users and groups.  However, if not, then you must
make these changes manually (you may need to use `sudo` for these commands)

    chgrp -R kiveprocessing [MEDIA_ROOT]/Sandboxes
    chmod -R g+w [MEDIA_ROOT]/Sandboxes
    find [MEDIA_ROOT]/Sandboxes -type d -exec chmod g+s {} \;
    
Additionally, if you were previously using `apache` to launch the fleet and you've switched
to using another account called `kiveuser`, you should also change the permissions on the other
data directories so that `kiveuser` can access them, as previously they likely belonged solely
to `apache`.  If all your Kive users belong to group `kive`, then:

    chgrp -R kive [MEDIA_ROOT]/CodeResources
    chmod -R g+w [MEDIA_ROOT]/CodeResources
    find [MEDIA_ROOT]/CodeResources -type d -exec chmod g+s {} \;
    
and repeat this for `Datasets` and `Logs`.

Creating database tables
------------------------
Having created the database, we must now create the tables that will be used by
Kive.  This is handled by the `migrate` command to the `manage.py` script,
which follows instructions created by the developers on how to lay out the tables:

    ./manage.py migrate

Should you ever need to completely remove the database and start over, you can run
the following commands:

    sudo su - postgres  # enter password if it asks
    dropdb kive  # enter kive user's password if it asks
    createdb kive
    exit
    ./manage.py migrate

This follows the above steps, except with an additional step that removes the
kive database, and skipping the already-done configuration steps.

[digitalocean]: https://www.digitalocean.com/community/tutorials/how-to-install-and-configure-django-with-postgres-nginx-and-gunicorn
[test-permission]: http://stackoverflow.com/q/14186055/4794
[pg_hba]: http://stackoverflow.com/a/18664239/4794


Remove test database (if necessary)
-----------------------------------
Sometimes, if something goes wrong while running the test suite, the system
will leave a database named `test_kive` in place, which will give the
following prompt the next time you run the tests:

    Creating test database for alias 'default'...
    Got an error creating the test database: database "test_kive" already exists

    Type 'yes' if you would like to try deleting the test database 'test_kive', or 'no' to cancel:

Then, on entering `yes`:

    Destroying old test database 'default'...
    Got an error recreating the test database: must be owner of database test_kive

To properly dispose of this database, run the following commands:

    sudo su - postgres  # enter password if it asks
    dropdb test_kive  # enter kive user's password if it asks
    exit
    
This is similar to the incantation used to completely remove the `kive` database,
but simpler because we are only trying to get rid of the database and do not need to
recreate or reinitialize anything.

Initialize the system
---------------------
The following instructions will initialize your system with a clean instance.
You should run this after you first install Kive, or after updating the source
code on a developer workstation.

1. If you are running Kive as a production server with Apache, (re-)deploy the
    static files by running the following command.  If you are running Kive as a
    development server on your workstation, then there is no need to collect
    static files and you can skip to step 2.

        sudo LD_LIBRARY_PATH=:/usr/local/lib ./manage.py collectstatic

2. Clear and re-populate the database with the following command:

        ./manage.py reset --load=demo

    You can leave the load parameter off, or set it to other fixture names, like
    `converter_pipeline`.  `demo` refers to a set of fixture files that populate
    the database with two pipelines that were used in the development of Kive
    and for demonstrating the software.
3. You are now ready to run a local Django webserver:

        python manage.py runserver

4. Navigate to `localhost:8000` in your web browser!
5. To launch a fleet manager and workers, you need to run the following command
    and replace X with the number of workers you want:

        python manage.py runfleet --workers X
    
Creating a UML diagram of the backend
-------------------------------------
The optional `django-extensions` module adds a command to `./manage.py` that
creates a UML representation of the database design using either `pydot` or
`pygraphviz`.  For example, to build a UML diagram of the `method` app, you may
use:

    ./manage.py graph_models --pygraphviz --settings=kive.UML_settings method -g > method.dot
    
This creates a `.dot` file that can be opened using GraphViz.  Note that this
command specifies an alternate settings file that is provided in the code base:
this was done to ensure that you don't need to install `django-extensions` to
run the system normally.  If you prefer to directly create a PDF file, you can
use the `-o` option:

    ./manage.py graph_models --pygraphviz --settings=kive.UML_settings method -g -o method.pdf
    
To create a diagram of the entire database:

    ./manage.py graph_models --pygraphviz --settings=kive.UML_settings -a -g -o kive.pdf

Running a pipeline
------------------
To erase all the data in your database and load a sample pipeline:

    ./manage.py reset --load demo

Then make sure the fleet is running as described above. Finally, navigate to
the user portal, analysis, and launch the pipeline. 

Building the documentation
--------------------------
The project uses LaTeX for some of its documentation, so you might want to install LaTeX to build it.
On Ubuntu, you need to install the following packages:

    sudo apt-get install texlive texlive-latex-extra texlive-fonts-extra

To build a LaTeX file into PDF format, use the `pdflatex` command.
