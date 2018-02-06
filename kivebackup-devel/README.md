Docker file for Kive backup. Currently, backup scripts performing a postgres dump of the kive database and for backup rotation are provided.

Compilation
-----------
Use the Makefile to build the docker image. It can also be run in interactive
mode for testing.

Installation
------------
a) Build the docker image.
   The docker image must be present on the machine performing the backups.
   Inspect the docker file kivebackup-base.dock. No changes
   should be necessary, unless:
   a) you are not the Pacific time zone. The timezone of the OS in the
      docker image influences the format of the time stamp used in naming
      the backup files. 
   b) You want or need a different OS version in the docker image. In order
      to change this, specify a different version in the FROM: statement.

   Build the docker image by running 'make build'. Next and optionally,
   the image can be tested by running it interactively using 'make run'.

b) The next step is to configure shell scripts that launch the docker 
   containers.
   Two shell scripts, rundump_configme.sh and runrotate_configme.sh are
   provided. Copy these to rundump.sh and runrotate.sh and modify them to suit
   your needs.
   ** IMPORTANT: As the rundump.sh script contains the password to access
   the database, make sure that this file cannot be read by unauthorised users.
   You may run the scripts to make sure that they work as required.

c) The scripts now need to be entered into the crontab entries
   so that they are run automatically on the machine. As root, run
   'crontab -e' . The following entries will run the two scripts daily:
0 0 * * * /path/to/script/rundump.sh
0 1 * * * /path/to/script/runrotate.sh







