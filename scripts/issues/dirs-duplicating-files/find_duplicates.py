#!/usr/bin/env python3
import argparse
import psycopg2
from pathlib import Path


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
    parser = argparse.ArgumentParser(
        description="Execute find_duplicates.sql for dv_ids returned by find_dv_ids.sql"
    )
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--name", default="dvndb")
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password", required=True)

    parser.add_argument("--min-id", type=int, required=True, help="numbers beyond this values are used")
    parser.add_argument("--nr-of-ids", type=int, default=50)

    args = parser.parse_args()

    SCRIPT_DIR = Path(__file__).resolve().parent

    dup_sql_raw = read_sql(SCRIPT_DIR / "find_duplicates.sql")

    dv_sql = read_sql(SCRIPT_DIR / "find_dv_ids.sql")
    dv_sql = dv_sql.replace(":min_id", str(min_id))
    dv_sql = dv_sql.replace(":nr_of_ids", str(nr_of_ids))

    with psycopg2.connect(
            dbname=args.name,
            user=args.user,
            password=args.password,
            host=args.host,
            port=args.port,
    ) as conn:
        dv_ids = fetch_dv_ids(conn, dv_sql)

        if not dv_ids:
            print("No dv_id values returned by find_dv_ids.sql")
            return

        ids_csv = ",".join(str(i) for i in dv_ids)
        print(f"dv_ids count: {len(dv_ids)}")
        print(f"dv_ids: {ids_csv}")
        run_find_duplicates(conn, dup_sql_raw.replace(":ids", ids_csv))


if __name__ == "__main__":
    main()
