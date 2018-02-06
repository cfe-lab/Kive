#!/bin/bash
if [ $# -ne 2 ]; then
    echo "usage: rotatedaily.sh backupdir ND"
    echo ""
    echo "Rotate backup files in a backup directory. The files' date is"
    echo "determined from their file name. Only files ending in '*.gz'"
    echo "are considered for rotation."
    echo ""
    echo "backdir: The directory containing the backup files."
    echo "ND     : The number of daily backups to keep. This must be an integer."
    echo "         The ND'th most recent backup files are kept as is (no name change),"
    echo "         while older backups are deleted."
    echo ""
    echo "example: rotatedaily.sh /backupdir 5"
    exit 1
fi

export BACKUPDIR=$1
export ND=$2

/usr/bin/rotate-backups -d ${ND}  -I "*.gz" ${BACKUPDIR}


