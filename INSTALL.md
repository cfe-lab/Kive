Installation Instructions
=========================

Prerequisites
-------------

**shipyard** is a set of Django applications.  As a result, it has the following requirements:

1. Python 2.x (version 2.7 or higher)
2. Django (version 1.6 or higher recommended, version 1.5 supported)

Source code or binaries for Python can be obtained from the official website, [python.org](www.python.org).  Most *nix distributions (including OS X) come with some version of Python.  

Instructions for downloading and installing Django can be found at [djangoproject.com](https://www.djangoproject.com/download/).


Project structure
-----------------

The root directory of **shipyard** should contain the following subdirectories:
* `/doc`
* `/samplecode`
* `/shipyard`

`/shipyard` is the top-level directory for the Django package that contains the project subdirectory (that by convention has the same name as the project folder, 'shipyard'), as well as a number of application subdirectories.  From now on, we will assume that you are in this project directory; *i.e.*, all paths will be defined relative to this directory.


Settings
--------

The first thing you need to do is to make a copy of `/shipyard/settings_default.py` called `settings.py` (remember, all paths are relative to `/shipyard` so we mean `/shipyard/shipyard/settings_default.py`).  This is a standard step in the installation of a Django project where you configure project settings.  Within the `DATABASES['default']` dictionary, modify the respective values to indicate the type, location, and access credentials of your database.  For example, if you are using sqlite3 as your database engine, you would enter `'sqlite3'` under the key `ENGINE`, and the absolute path to the sqlite3 database file under the key `NAME`.  Note that this file does not have to exist - it will be created later.

You may also wish to modify the `TIME_ZONE` setting to your region, although this localization is not strictly necessary.


Initialize database
-------------------

Next, you need to make a copy of `./nukeDB_default.expect` and call it `nukeDB.expect`.  You need to replace all text that is highlighted in square brackets, as follows:

* `[PATH TO YOUR DB]` - an absolute or relative path to your database file, if you are using sqlite3.  **WARNING:** This will overwrite an existing database at this path, so you will lose everything if you execute the `nukeDB.expect` script after having used **shipyard** for any length of time.  As a precaution, you (as system administrator) may consider changing the user permission settings on all `nukeDB.*` files.
* `[YOUR E-MAIL ADDRESS HERE]` - for creating an admin account 
* `[YOUR PASSWORD]`
* `[YOUR PASSWORD AGAIN]`




