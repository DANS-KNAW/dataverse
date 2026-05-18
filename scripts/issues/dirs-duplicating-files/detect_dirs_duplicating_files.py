import psycopg2
import psycopg2.extras
import argparse
from collections import deque

parser = argparse.ArgumentParser(description="Detect directories duplicating files in Dataverse.")
parser.add_argument('--name', default='dvndb', help='Database name (default: dvndb)')
parser.add_argument('--user', default='postgres', help='Database user (default: postgres)')
parser.add_argument('--password', required=True, help='Database password')
parser.add_argument('--host', default='dev.archaeology.datastations.nl', help='Database host (default: dev..archaeology.datastations.nl)')
parser.add_argument('--port', type=int, default=5432, help='Database port (default: 5432)')
args = parser.parse_args()

DB = {
    'name': args.name,
    'user': args.user,
    'password': args.password,
    'host': args.host,
    'port': args.port
}

dir_query = """
    SELECT DISTINCT datasetversion_id, directorylabel
    FROM filemetadata
    WHERE directorylabel IS NOT NULL
    ORDER BY datasetversion_id, directorylabel
    """

file_query = """
    SELECT DISTINCT datasetversion_id, directorylabel, label
    FROM filemetadata
    ORDER BY datasetversion_id, directorylabel, label
    """

dataset_query = """
    SELECT dso.protocol, dso.authority, dso.identifier, dv.versionnumber, dv.minorversionnumber
    FROM datasetversion dv
    JOIN dvobject       dso  ON dso.id = dv.dataset_id
    WHERE dv.id = %s
    """


def get_full_path(directorylabel, label):
    if directorylabel and directorylabel.strip():
        return f"{directorylabel}/{label}"
    else:
        return label


def build_ancestors(path):
    if not path or not path.strip():
        return []

    parts = [p for p in path.split('/') if p]
    return ['/'.join(parts[:i]) for i in range(1, len(parts) + 1)]

def fetch_dict(cur):
    row = cur.fetchone()
    return dict(row) if row is not None else None


def next_ancestor(dir_cur, dir_row, ancestors):
    while True:
        if ancestors:
            return ancestors.popleft(), dir_row

        if dir_row is None:
            return None, None

        ancestors.extend(build_ancestors(dir_row['directorylabel']))
        if ancestors:
            return ancestors.popleft(), dir_row

        dir_row = fetch_dict(dir_cur)


with psycopg2.connect(
        dbname=DB['name'],
        user=DB['user'],
        password=DB['password'],
        host=DB['host'],
        port=DB['port']
) as conn:
    with conn.cursor(name='dir_cur', cursor_factory=psycopg2.extras.DictCursor) as dir_cur, \
            conn.cursor(name='file_cur', cursor_factory=psycopg2.extras.DictCursor) as file_cur, \
            conn.cursor(name='ds_cur', cursor_factory=psycopg2.extras.DictCursor) as ds_cur:
        dir_cur.execute(dir_query)
        file_cur.execute(file_query)

        # Initialize the first rows
        file_row = fetch_dict(file_cur)
        dir_row = fetch_dict(dir_cur)
        ancestors = deque()
        ancestor, dir_row = next_ancestor(dir_cur, dir_row, ancestors)

        while dir_row is not None and file_row is not None and ancestor is not None:
            full_path = get_full_path(file_row['directorylabel'], file_row['label'])
            dir_key = (dir_row['datasetversion_id'], ancestor)
            file_key = (file_row['datasetversion_id'], full_path)
            if dir_key < file_key:
                ancestor, dir_row = next_ancestor(dir_cur, dir_row, ancestors)
            elif dir_key > file_key:
                file_row = fetch_dict(file_cur)
            else:
                # match
                ds_cur.execute(dataset_query, (dir_row['datasetversion_id'],))
                ds_info = dict(ds_cur.fetchone())
                ds_info['directory'] = ancestor
                print(ds_info)
                # advance both
                file_row = fetch_dict(file_cur)
                ancestor, dir_row = next_ancestor(dir_cur, dir_row, ancestors)
