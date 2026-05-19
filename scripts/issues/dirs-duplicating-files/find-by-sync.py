#!/usr/bin/env python3
import argparse
from dataclasses import dataclass
from typing import Iterable, Iterator, Optional, Sequence, Tuple

import psycopg2
from textwrap import dedent



DIR_QUERY = """
            SELECT DISTINCT
                datasetversion_id,
                NULLIF(BTRIM(directorylabel), '') AS directorylabel
            FROM filemetadata
            WHERE NULLIF(BTRIM(directorylabel), '') IS NOT NULL
            ORDER BY datasetversion_id, directorylabel
            """

FILE_QUERY = """
             SELECT DISTINCT
                 datasetversion_id,
                 CASE
                     WHEN NULLIF(BTRIM(directorylabel), '') IS NULL THEN label
                     ELSE NULLIF(BTRIM(directorylabel), '') || '/' || label
                     END AS path
             FROM filemetadata
             ORDER BY datasetversion_id, path
             """


@dataclass(frozen=True, order=True)
class DvPath:
    datasetversion_id: int
    path: str


def split_ancestors(path: str) -> list[str]:
    # shortest first: "a/b/c" -> ["a", "a/b", "a/b/c"]
    parts = [p for p in path.split("/") if p]
    out = []
    acc = []
    for p in parts:
        acc.append(p)
        out.append("/".join(acc))
    return out


def iter_dir_ancestors(cur) -> Iterator[DvPath]:
    """
    Streams (datasetversion_id, ancestor_path), sorted.
    Assumes DIR_QUERY ORDER BY datasetversion_id, directorylabel.
    """
    cur.execute(DIR_QUERY)

    current_dv: Optional[int] = None
    ancestors: set[str] = set()

    def flush(dv_id: Optional[int], anc: set[str]) -> Iterator[DvPath]:
        if dv_id is None:
            return
        for path in sorted(anc):
            yield DvPath(dv_id, path)

    for dv_id_raw, directorylabel in cur:
        dv_id = int(dv_id_raw)
        if not directorylabel:
            continue

        if current_dv is None:
            current_dv = dv_id
        elif dv_id != current_dv:
            yield from flush(current_dv, ancestors)
            ancestors.clear()
            current_dv = dv_id

        ancestors.update(split_ancestors(directorylabel))

    yield from flush(current_dv, ancestors)


def iter_file_paths(cur) -> Iterator[DvPath]:
    """
    Streams (datasetversion_id, full_file_path), sorted.
    Assumes FILE_QUERY ORDER BY datasetversion_id, path.
    """
    cur.execute(FILE_QUERY)
    for dv_id, path in cur:
        if path:
            yield DvPath(int(dv_id), path)


def merge_intersect(
        dirs: Iterator[DvPath], files: Iterator[DvPath]
) -> Iterator[DvPath]:
    """
    Merge-sync two sorted iterators and emit intersection, SQL-INTERSECT style.
    """
    d = next(dirs, None)
    f = next(files, None)
    last_out: Optional[DvPath] = None

    while d is not None and f is not None:
        if d < f:
            d = next(dirs, None)
        elif d > f:
            f = next(files, None)
        else:
            # equal
            if d != last_out:
                yield d
                last_out = d
            d = next(dirs, None)
            f = next(files, None)


def main() -> None:

    class RawDefaultsFormatter(
        argparse.ArgumentDefaultsHelpFormatter,
        argparse.RawDescriptionHelpFormatter,
    ):
        pass

    parser = argparse.ArgumentParser(
        description=dedent("""
                Execute as owner of dvndb.
                
                For millions of files, itersize tuning rule:
                - too small (e.g. 100): many round-trips, slower
                - too large (e.g. 100k): more client memory spikes, less responsive
                - good starting range: 5000–20000 (often 10k is a good first try)
                Quick heuristic:
                - if network latency is high and RAM is fine -> increase itersize
                - if Python memory pressure rises -> decrease itersize
            """),
        formatter_class=RawDefaultsFormatter,
    )
    parser.add_argument("--itersize_dirs", type=int, default=5000, help="batch size for rows fetched from PostgreSQL")
    parser.add_argument("--itersize_files", type=int, default=5000, help="batch size for rows fetched from PostgreSQL")
    args = parser.parse_args()

    conn_kwargs = {
        "dbname": 'dvndb',
    }
    with psycopg2.connect(**conn_kwargs) as conn:
        # named cursors to stream results instead of loading all rows
        with conn.cursor(name="dir_stream") as dir_cur, conn.cursor(name="file_stream") as file_cur:
            dir_cur.itersize = args.itersize_dirs
            file_cur.itersize = args.itersize_files
            dirs = iter_dir_ancestors(dir_cur)
            files = iter_file_paths(file_cur)

            print("datasetversion_id\tpath")
            for row in merge_intersect(dirs, files):
                print(f"{row.datasetversion_id}\t{row.path}")


if __name__ == "__main__":
    main()
