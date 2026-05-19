#!/usr/bin/env python3
import argparse
import psycopg2
from pathlib import Path
from textwrap import dedent

def read_sql(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    return "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("\\")
    )


def fetch_dv_ids(conn, find_dv_ids_sql: str) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(find_dv_ids_sql)
        rows = cur.fetchall()

    # Query returns dv_id as first selected column in your file.
    return [int(row[0]) for row in rows]


def run_find_duplicates(conn, find_duplicates_sql: str):
    with conn.cursor() as cur:
        cur.execute(find_duplicates_sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

    print("\t".join(cols))
    for row in rows:
        print("\t".join("" if v is None else str(v) for v in row))


def main():
    class RawDefaultsFormatter(
        argparse.ArgumentDefaultsHelpFormatter,
        argparse.RawDescriptionHelpFormatter,
    ):
        pass

    parser = argparse.ArgumentParser(
        description=dedent("""
            Execute `find_duplicates.sql` for dv_ids returned by `find_dv_ids.sql`.
            `find_dv_ids.sql` returns the latest version per dataset.
        """),
        formatter_class=RawDefaultsFormatter,
    )
    parser.add_argument("--min-id", type=int, default=0, help="`find_dv_ids.sql` looks for larger ID's")
    parser.add_argument("--nr-of-ids", type=int, default=50, help="number of ID's returned by `find_dv_ids.sql`")
    parser.add_argument("--db-name", default="dvndb", help="name of the dataverse database")
    sub = parser.add_subparsers(dest="dbmode", required=False)
    tcp = sub.add_parser("tcp", formatter_class=RawDefaultsFormatter, help="Transmission Control Protocol, required when not running as DB user.")
    tcp.add_argument("--host", default="localhost", help="host where the database is located")
    tcp.add_argument("--port", type=int, default=5432, help="port number to connect to the database")
    tcp.add_argument("--user", default="postgres", help="database user name")
    tcp.add_argument("--password", required=True, help="password for the database user")
    args = parser.parse_args()
    conn_kwargs = {"dbname": args.db_name} if args.dbmode != "tcp" else {
        "host": args.host,
        "port": args.port,
        "dbname": args.name,
        "user": args.user,
        "password": args.password,
    }

    script_dir = Path(__file__).resolve().parent

    dup_sql_raw = read_sql(script_dir / "find_duplicates.sql")

    dv_sql = read_sql(script_dir / "find_dv_ids.sql")
    dv_sql = dv_sql.replace(":min_id", str(args.min_id))
    dv_sql = dv_sql.replace(":nr_of_ids", str(args.nr_of_ids))

    with psycopg2.connect(**conn_kwargs) as conn:
        dv_ids = fetch_dv_ids(conn, dv_sql)

        if not dv_ids:
            print("No dv_id values returned by find_dv_ids.sql")
            return

        ids_csv = ",".join(str(i) for i in dv_ids)
        print(f"dataset version ids: {ids_csv}")
        run_find_duplicates(conn, dup_sql_raw.replace(":ids", ids_csv))


if __name__ == "__main__":
    main()
