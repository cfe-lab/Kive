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

Instructions for downloading and installing psycopg may be found at (http://initd.org/psycopg/).
Project structure
-----------------

The root directory of **shipyard** should contain the following subdirectories:
* `/doc`
* `/samplecode`
* `/shipyard`

`/shipyard` is the top-level directory for the Django package that contains the project subdirectory (that by convention has the same name as the project folder, 'shipyard'), as well as a number of application subdirectories.  From now on, we will assume that you are in this project directory; *i.e.*, all paths will be defined relative to this directory.


Create database
---------------

**shipyard** uses PostgreSQL as its default database backend, and it must be set up prior to using **shipyard**.  Setup of PostgreSQL follows step seven of the instructions [here](https://www.digitalocean.com/community/tutorials/how-to-install-and-configure-django-with-postgres-nginx-and-gunicorn).  

First, install PostgreSQL and psycopg as is appropriate for your system.  During the setup, a `postgres` system user account should have been set up: this account is the administrator of the database.  To set up the database, log into this user account (for example, using `sudo su - postgres`) and create a database for **shipyard**.  In these instructions we will call this database `'shipyard'`, but you may use whatever name you like -- simply substitute your name for `shipyard` in the appropriate places in the commands below.  You may do this in a SQL console such as the `'psql'` shell that comes with PostgreSQL, but it's easier to use the `'createdb'` utility provided by PostgreSQL (look for it in whatever directory your PostgreSQL installation put its executables into):

`createdb shipyard`

(substituting your preferred database name if you so desire).  Next, create a user (or "role") for **shipyard** to use when accessing the database (in these instructions we will call this user `'shipyard'`, but again you are free to use whatever you like -- simply substitute it for `shipyard` in the appropriate places in the commands below).  Again, this may be done in an SQL console, but it's easier to use the `'createuser'` utility:

`createuser -P shipyard`

(again substituting your preferred user name if you like) and follow the prompts (the `-P` allows you to specify a password).  Now, we need to grant this user the appropriate privileges.  As the postgres system user, using the `'psql'` SQL console provided with your postgres installation, enter at the prompt:

`GRANT ALL PRIVILEGES ON DATABASE shipyard TO shipyard;`

We are almost done.  In order to run the **shipyard** test suites, the `shipyard` database account must have the ability to create temporary test databases.  As instructed in [this Stack Overflow thread](http://stackoverflow.com/questions/14186055/django-test-app-error-got-an-error-creating-the-test-database-permission-deni), we grant the user this privilege by running the command, again as the `postgres` system user using the `psql` SQL console,

`ALTER USER shipyard CREATEDB;`

The database is now fully set up and ready to go.


Settings
--------

Since **shipyard** is a Django project, the majority of the installation procedure follows the standard instructions for Django.  The first thing you need to do is to make a copy of `/shipyard/settings_default.py` called `settings.py` (remember, all paths are relative to `/shipyard` so we mean `/shipyard/shipyard/settings_default.py`).  This is a standard step in the installation of a Django project where you configure project settings.  Within the `DATABASES['default']` dictionary, modify the respective values to indicate the type, location, and access credentials of your database.  For example, using postgres as your database engine, you would specify `'django.db.backends.postgresql_psycopg2'` under the `ENGINE` key, and the name of the database Shipyard is to use under the key `NAME` (e.g. `'shipyard'`).  This is a database that must be created by an admistration prior to using Shipyard.

You may also wish to modify the `TIME_ZONE` setting to your region, although this localization is not strictly necessary.



Initialize database
-------------------

Next, you need to make a copy of `./nukeDB_default.expect` and call it `nukeDB.expect`.  You need to replace all text that is highlighted in square brackets, as follows:

* `[YOUR E-MAIL ADDRESS HERE]` - for creating an admin account with the utility that is packaged with the Django distribution (`django.contrib.admin`).  Generally, it is not necessary to use this admin interface but we leave it as an open possibility.  It is okay to leave this blank, *i.e.,* as an empty string followed by a carriage return `"\r"`.
* `[YOUR PASSWORD]` - similarly, this is also used to initialize an admin account for the Django admin interface.  Unless you are really keen to use the admin tool, it is fine to enter an empty string here: `"\r"`.
* `[YOUR PASSWORD AGAIN]` - obviously, this should match the previous entry.


Finally, execute this *expect* script using the bash script `./nukeDB.bash`. This is a simple wrapper that calls `nukeDB.expect` and then executes a Django function that will populate the new database with some initial data.

You are now ready to run a local Django webserver - you just need to type `python manage.py runserver` and navigate to `localhost:8000` in your web browser!

To launch a fleet manager and workers, you need to run the following command
and replace X with the number of workers you want, plus one for the manager:

    DJANGO_SETTINGS_MODULE=shipyard.settings mpirun -np X initialize_fleet.py

Running a pipeline
------------------

This process still requires some manual steps. To create and run your first pipeline, do the following:

1. Create a raw data set.

        cd ~/git/Shipyard/shipyard
        python manage.py shell
        from librarian.models import SymbolicDataset
        from django.contrib.auth.models import User
        u = User.objects.get(username='shipyard')
        SymbolicDataset.create_SD('../samplecode/script_1_sum_and_products_input.csv', user=u, name='2cols', description='two columns of numbers')
        exit()

2. Go to the Shipyard web interface, and navigate to Developer portal: Code resources.
3. Click Add new code resource.
4. Choose a code resource file. For example, `samplecode/script_1_sum_and_products.py`.
5. Give the resource a name and description, then submit it.
6. Navigate back up to the Developer portal, then to the Methods page.
7. Click Add a new method.
8. Select the code resource you just created, type a family name and description.
9. Type a name for the input and choose the unstructured datatype. Do the same for the output.
10. Submit the method.
11. Navigate back up to the Developer portal, then to the Pipeline assembly page.
12. Click Add a new pipeline.
13. Type a label for the input, and click Add Input.
14. Select the method, type a label for it, and click Add Method.
15. Wire the input and output for your method.
16. Type a name and description for the pipeline, then click Submit.
17. Don't worry if there is no response, just go back to the list of pipelines and check that yours appears.
18. Navigate up to the home page, and then down to Users portal: Analysis.
19. Select your pipeline in the middle section, and then your input in the left section.
20. Click the Run button.

Building the documentation
--------------------------

The project uses LaTeX for some of its documentation, so you might want to install LaTeX to build it.
On Ubuntu, you need to install the following packages:

    sudo apt-get install texlive texlive-latex-extra texlive-fonts-extra

To build a LaTeX file into PDF format, use the `pdflatex` command.

