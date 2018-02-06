#!/bin/bash

if [ $# -ne 4 ]; then
    echo "usage: dodockerdump.sh backupdir dbhost kivedb kivedbuser"
    echo ""
    echo "Write a gzipped postgres dump of the kive database to a backup directory."
    echo ""
    echo "backupdir : the existing directory into which the backup should be written."
    echo "dbhost    : the name of the computer hosting the database"
    echo "kivedb    : the name of the kive database to back up"
    echo "kivedbuser: the postgres username required to access the kive database"
    echo ""
    echo "example:"
    echo "PGPASSWORD=my_1234_kive_password dodockerbackup.sh /kivebackup bulbasaur kive kive"
    exit 1
fi


export BACKUPDIR=$1
export DBHOST=$2
export DBNAME=$3
export DBUSER=$4

export TODAY=`/usr/bin/date --iso-8601=seconds`

/usr/bin/pg_dump -h ${DBHOST} -Fc -ow ${DBNAME} -U ${DBUSER} | /usr/bin/gzip > ${BACKUPDIR}/kivedump-${HOSTNAME}-${TODAY}.gz



