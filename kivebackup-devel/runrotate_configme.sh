#!/bin/bash

# Copy this file and change these settings to suit
export KIVEBACKUP_BACKUP_DIR=/backupmount/dragonite
export ND=5


# this must match the docker image names used in the build Makefile
export BACKUP_IMAGE_NAME=kivebackup-base


docker run --rm --net="host" -v ${KIVEBACKUP_BACKUP_DIR}:/kivebackup ${BACKUP_IMAGE_NAME} ./rotatedaily.sh  /kivebackup ${ND}



