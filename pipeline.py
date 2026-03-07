#!/usr/bin/env python3
"""
Consolidated pipeline: fetch recent papers from API → ensure DB table → upsert → data quality tests.

Uses existing logic from fetch_recent_ai_papers, load_papers_from_json, and papers_schema.sql.
"""
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
env_path = REPO_ROOT / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip().strip('"').strip("'")

import psycopg

from fetch_recent_ai_papers import fetch_recent_ai_papers
from load_papers_from_json import (
    ensure_papers_schema,
    work_to_row,
    upsert_papers,
)

DATABASE_URL = os.environ.get("DATABASE_URL")


class Pipeline:
    """Orchestrates: fetch papers → ensure schema → upsert → data quality tests."""

    def __init__(self, database_url: str | None = None, temp_dir: str | Path | None = None):
        self.database_url = database_url or DATABASE_URL
        self.temp_dir = Path(temp_dir) if temp_dir is not None else REPO_ROOT / "temp"

    def run(self, *, skip_fetch: bool = False, skip_dq: bool = False) -> None:
        """
        Run full pipeline: fetch → ensure table → upsert → quality tests.
        Set skip_fetch=True to only load from existing DB (no API call).
        Set skip_dq=True to skip data quality tests.
        """
        if not self.database_url:
            print("ERROR: DATABASE_URL not set in .env", file=sys.stderr)
            sys.exit(1)

        # 1) Query API for recent papers
        if skip_fetch:
            papers = []
            print("Skipping fetch (skip_fetch=True). No new papers to load.", file=sys.stderr)
        else:
            papers = fetch_recent_ai_papers(save_dir=str(self.temp_dir))
            if not papers:
                print("No papers fetched. Exiting.", file=sys.stderr)
                return

        # 2) Connect, ensure table, upsert
        with psycopg.connect(self.database_url) as conn:
            ensure_papers_schema(conn)

            if papers:
                rows = []
                for w in papers:
                    row = work_to_row(w)
                    if row:
                        rows.append(row)
                    else:
                        print("Skipping work without openalex id.", file=sys.stderr)
                if rows:
                    n = upsert_papers(conn, rows)
                    conn.commit()
                    print(f"Upserted {n} rows into papers.", file=sys.stderr)
                else:
                    conn.rollback()
            else:
                conn.rollback()

            # 3) Data quality tests (run against current DB state)
            if not skip_dq:
                self._run_quality_tests(conn)

    def _run_quality_tests(self, conn) -> None:
        """Run data quality checks on the papers table and print results."""
        print("\n--- Data quality tests ---", file=sys.stderr)
        failed = 0
        with conn.cursor() as cur:
            # DQ1: Table exists
            cur.execute("""
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'papers';
            """)
            if not cur.fetchone():
                print("  FAIL: Table 'papers' does not exist.", file=sys.stderr)
                failed += 1
            else:
                print("  PASS: Table 'papers' exists.", file=sys.stderr)

            # DQ2: Row count (informational)
            cur.execute("SELECT count(*) FROM papers;")
            total = cur.fetchone()[0]
            print(f"  INFO: papers row count = {total}.", file=sys.stderr)

            # DQ3: No duplicate openalex_id
            cur.execute("""
                SELECT count(*) - count(DISTINCT openalex_id) FROM papers;
            """)
            dup = cur.fetchone()[0]
            if dup != 0:
                print(f"  FAIL: Duplicate openalex_id count = {dup}.", file=sys.stderr)
                failed += 1
            else:
                print("  PASS: No duplicate openalex_id.", file=sys.stderr)

            # DQ4: Required fields non-null (openalex_id is PK; title NOT NULL)
            cur.execute("SELECT count(*) FROM papers WHERE openalex_id IS NULL OR title IS NULL OR trim(title) = '';")
            null_required = cur.fetchone()[0]
            if null_required != 0:
                print(f"  FAIL: Rows with null/empty openalex_id or title = {null_required}.", file=sys.stderr)
                failed += 1
            else:
                print("  PASS: All rows have openalex_id and non-empty title.", file=sys.stderr)

            # DQ5: publication_year in reasonable range (e.g. 1900–2100) when present
            cur.execute("""
                SELECT count(*) FROM papers
                WHERE publication_year IS NOT NULL
                  AND (publication_year < 1900 OR publication_year > 2100);
            """)
            bad_year = cur.fetchone()[0]
            if bad_year != 0:
                print(f"  FAIL: Rows with publication_year outside 1900–2100 = {bad_year}.", file=sys.stderr)
                failed += 1
            else:
                print("  PASS: publication_year in valid range (or null).", file=sys.stderr)
        if failed:
            print(f"  Total: {failed} test(s) failed.", file=sys.stderr)
            sys.exit(1)
        print("  All data quality tests passed.", file=sys.stderr)


def main() -> None:
    import argparse
    p = argparse.ArgumentParser(description="Run papers pipeline: fetch → schema → upsert → DQ tests.")
    p.add_argument("--skip-fetch", action="store_true", help="Do not call API; only ensure schema and run DQ.")
    p.add_argument("--skip-dq", action="store_true", help="Skip data quality tests.")
    args = p.parse_args()
    pipeline = Pipeline()
    pipeline.run(skip_fetch=args.skip_fetch, skip_dq=args.skip_dq)


if __name__ == "__main__":
    main()
