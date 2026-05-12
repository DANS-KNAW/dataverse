import psycopg2
import argparse

parser = argparse.ArgumentParser(description="Detect directories duplicating files in Dataverse.")
parser.add_argument('--name', required=True, default='dvndb', help='Database name (default: dvndb)')
parser.add_argument('--user', required=True, defualt='postgres', help='Database user (default: postgres)')
parser.add_argument('--password', required=True, help='Database password')
parser.add_argument('--host', required=True, default='dev.archaeology.datastations.nl', help='Database host (default: dev..archaeology.datastations.nl)')
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
    SELECT DISTINCT directorylabel
    FROM filemetadata
    WHERE directorylabel IS NOT NULL
    ORDER BY directorylabel
    """

file_query = """
    SELECT id, directorylabel, label
    FROM filemetadata
    ORDER BY directorylabel, label
    """

dataset_query = """
    SELECT dso.protocol, dso.authority, dso.identifier, dv.versionnumber, dv.minorversionnumber
    FROM datasetversion dv   ON dv.id = %s
    JOIN dvobject       dso  ON dso.id = dv.dataset_id
    """

def get_full_path(directorylabel, label):
    if directorylabel and directorylabel.strip():
        return f"{directorylabel}/{label}"
    else:
        return label

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

        dir_row = dict(dir_cur.fetchone())
        file_row = dict(file_cur.fetchone())

        while dir_row is not None and file_row is not None:
            full_path = get_full_path(file_row['directorylabel'], file_row['label'])
            dir_label = dir_row['directorylabel']
            if (dir_label == full_path):
                ds_cur.execute(dataset_query, (dir_row(['datasetversion_id'],)))
                ds_info = dict(ds_cur.fetchone())
                ds_info['directorylabel'] = dir_row['directorylabel']
                print (ds_info)
                # Advance both
                dir_row = dict(dir_cur.fetchone())
                file_row = dict(file_cur.fetchone())
            elif dir_label < full_path:
                dir_row = dict(dir_cur.fetchone())
            else:
                file_row = dict(file_cur.fetchone())
