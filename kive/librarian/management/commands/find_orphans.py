#! /opt/venv_kive/bin/python
from __future__ import print_function
import psycopg2
import argparse
import os
from django.core.management.base import BaseCommand

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('-df', '--delete_files', action='store_true', help='Delete orphaned files')
        parser.add_argument('-dr', '--delete_records', action='store_true', help='Delete database records for orphaned files')
        parser.add_argument('-da', '--delete_all', action='store_true', help='Combine options -df and -dr')
        parser.add_argument('-r', '--root_path', default='/var/kive/media_root', help='Path to kive media root folder')

    def handle(self, *args, **options):
        name = 'kive'
        user = ''
        password = ''

        conn = psycopg2.connect(
            dbname=name,
            user=user,
            password=password
        )

        cur = conn.cursor()
        query = 'SELECT * FROM librarian_dataset AS ld WHERE ld.id NOT IN (SELECT dataset_id FROM container_containerdataset)'
        cur.execute(query)
        rows = cur.fetchall()

        n_orphaned = len(rows)
        if options['verbosity'] > 0:
            for row in rows:
                path = os.path.join(options['root_path'], row[4])
                print(path)
        else:
            print(n_orphaned)
        
        if (options['delete_files'] or options['delete_records'] or options['delete_all']):
            for row in rows:
                path = os.path.join(options['root_path'], row[4])
                if (options['delete_files'] or options['delete_all']):
                    print('Deleting file {} ...'.format(path))
                    os.remove(path)
                    print('File deleted successfully')
                if (options['delete_records'] or options['delete_all']):
                    print('Removing database record ...')
                    delete_query = 'DELETE FROM librarian_dataset WHERE id = {}'.format(row[0])
                    print('Executing SQL query "{}"'.format(delete_query))
                    cur.execute(delete_query)
                    conn.commit()
                    print('Query executed successfully')

        cur.close()
        conn.close()