#!/bin/bash

# Copy this file and change these settings to suit
#***** NOTE: because this file will contain a password, make sure to 
#***** change its permissions to read only after editing

export KIVEBACKUP_BACKUP_DIR=/backupmount/dragonite
export DBHOST=bulbasaur
export DBNAME=Druidia
export DBUSER=KingRoland
export PGPASSWORD=1234


# this must match the docker image names used in the build Makefile
export BACKUP_IMAGE_NAME=kivebackup-base


docker run --rm --net="host" -e PGPASSWORD -v ${KIVEBACKUP_BACKUP_DIR}:/kivebackup ${BACKUP_IMAGE_NAME} ./dodockerdump.sh /kivebackup ${DBHOST} ${DBNAME} ${DBUSER} 

