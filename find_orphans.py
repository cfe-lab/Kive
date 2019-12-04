#! /opt/venv_kive/bin/python
import psycopg2
import argparse
import os

def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter, description='Report the number of orphaned files found, optionally list their paths and/or delete them')

    parser.add_argument('-v', '--verbose', action='store_true', help='Print file paths')
    parser.add_argument('-df', '--delete_files', action='store_true', help='Delete orphaned files')
    parser.add_argument('-dr', '--delete_records', action='store_true', help='Delete database records for orphaned files')
    parser.add_argument('-da', '--delete_all', action='store_true', help='Combine options -df and -dr')
    parser.add_argument('-r', '--root_path', default='/var/kive/media_root', help='Path to kive media root folder')

    args = parser.parse_args()
    return args

def main():
    args = parse_args()

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

    if args.verbose:
        for row in rows:
            path = os.path.join(args.root_path, row[4])
            print(path)
    else:
        print(n_orphaned)
    
    if args.delete:
        for row in rows:
            path = os.path.join(args.root_path, row[4])
            if (args.delete_files or args.delete_all):
                print('Deleting file {} ...'.format(path))
                os.remove(path)
                print('File deleted successfully')
            if (args.delete_records or args.delete_all):
                print('Removing database record ...')
                delete_query = 'DELETE FROM librarian_dataset WHERE id = {}'.format(row[0])
                print('Executing SQL query "{}"'.format(delete_query))
                cur.execute(delete_query)
                conn.commit()
                print('Query executed successfully')

    cur.close()
    conn.close()

if __name__ == '__main__':
    main()