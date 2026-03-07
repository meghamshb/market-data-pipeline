#!/usr/bin/env python3
"""
Load OpenAlex papers from a JSON file into the papers table.

- Accepts a JSON file path (e.g. temp/ai_papers_*.json).
- Loads .env and connects to the database (same as check_db_status).
- Creates the papers table and indexes if they do not exist (papers_schema.sql).
- Maps each JSON work to the flattened papers schema and inserts with deduplication
  (ON CONFLICT on openalex_id: update existing row).
"""
import json
import os
import sys
from pathlib import Path

# Load .env (strip spaces from values)
REPO_ROOT = Path(__file__).resolve().parent
env_path = REPO_ROOT / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip().strip('"').strip("'")

import psycopg

BATCH_SIZE = 100

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set in .env", file=sys.stderr)
    sys.exit(1)


def ensure_papers_schema(conn):
    """Run papers_schema.sql so the table and indexes exist."""
    schema_path = REPO_ROOT / "papers_schema.sql"
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    sql = schema_path.read_text()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def parse_date(s):
    """Return date string YYYY-MM-DD or None."""
    if not s:
        return None
    if isinstance(s, str) and "T" in s:
        s = s.split("T")[0]
    return s[:10] if len(s) >= 10 else None


def work_to_row(work):
    """Map one OpenAlex work (dict) to a flat row for the papers table."""
    openalex_id = work.get("id") or work.get("ids", {}).get("openalex")
    if not openalex_id:
        return None

    doi = work.get("doi")
    if doi and not doi.startswith("http"):
        doi = f"https://doi.org/{doi}" if doi else None

    title = work.get("title") or work.get("display_name") or ""
    pub_date = parse_date(work.get("publication_date"))
    pub_year = work.get("publication_year")
    if pub_year is not None:
        pub_year = int(pub_year)

    type_ = work.get("type")
    language = work.get("language")

    oa = work.get("open_access") or {}
    oa_status = oa.get("oa_status")
    is_oa = oa.get("is_oa")

    primary_loc = work.get("primary_location") or {}
    source = primary_loc.get("source") or {}
    source_name = source.get("display_name")

    primary_topic = work.get("primary_topic")
    if not primary_topic and work.get("topics"):
        primary_topic = work.get("topics")[0]
    topic_display_name = None
    topic_subfield_name = None
    topic_field_name = None
    topic_domain_name = None
    if primary_topic:
        topic_display_name = primary_topic.get("display_name")
        sub = primary_topic.get("subfield") or {}
        topic_subfield_name = sub.get("display_name")
        field = primary_topic.get("field") or {}
        topic_field_name = field.get("display_name")
        domain = primary_topic.get("domain") or {}
        topic_domain_name = domain.get("display_name")

    cited_by_count = work.get("cited_by_count")
    if cited_by_count is not None:
        cited_by_count = int(cited_by_count)
    referenced_works_count = work.get("referenced_works_count")
    if referenced_works_count is not None:
        referenced_works_count = int(referenced_works_count)
    authorships = work.get("authorships") or []
    authors_count = len(authorships) if authorships else None

    percentile_obj = work.get("citation_normalized_percentile") or {}
    pct_value = percentile_obj.get("value")
    if pct_value is not None:
        # OpenAlex gives 0–1; schema expects 0–100
        pct_value = float(pct_value) * 100.0
    citation_percentile_year = None
    percentile_year = work.get("cited_by_percentile_year") or {}
    if percentile_year.get("min") is not None:
        citation_percentile_year = int(percentile_year["min"])
    elif percentile_year.get("max") is not None:
        citation_percentile_year = int(percentile_year["max"])

    fwci = work.get("fwci")
    if fwci is not None:
        fwci = float(fwci)
    is_retracted = work.get("is_retracted") or False
    created_date = parse_date(work.get("created_date"))
    updated_date = parse_date(work.get("updated_date"))

    return {
        "openalex_id": openalex_id,
        "doi": doi,
        "title": title,
        "publication_date": pub_date,
        "publication_year": pub_year,
        "type": type_,
        "language": language,
        "oa_status": oa_status,
        "is_oa": is_oa,
        "source_name": source_name,
        "topic_display_name": topic_display_name,
        "topic_subfield_name": topic_subfield_name,
        "topic_field_name": topic_field_name,
        "topic_domain_name": topic_domain_name,
        "cited_by_count": cited_by_count,
        "referenced_works_count": referenced_works_count,
        "authors_count": authors_count,
        "citation_percentile_year": citation_percentile_year,
        "citation_normalized_percentile": pct_value,
        "fwci": fwci,
        "is_retracted": is_retracted,
        "created_date": created_date,
        "updated_date": updated_date,
    }


PAPERS_COLUMNS = [
    "openalex_id", "doi", "title", "publication_date", "publication_year",
    "type", "language", "oa_status", "is_oa", "source_name",
    "topic_display_name", "topic_subfield_name", "topic_field_name", "topic_domain_name",
    "cited_by_count", "referenced_works_count", "authors_count",
    "citation_percentile_year", "citation_normalized_percentile", "fwci",
    "is_retracted", "created_date", "updated_date",
]


def upsert_papers(conn, rows: list[dict], batch_size: int = BATCH_SIZE) -> int:
    """
    Insert or update rows into the papers table (deduplication by openalex_id).
    conn: open psycopg connection. Caller must commit. Returns number of rows upserted.
    """
    if not rows:
        return 0
    col_list = ", ".join(PAPERS_COLUMNS)
    ncols = len(PAPERS_COLUMNS)
    one_row = "(" + ", ".join(["%s"] * ncols) + ")"
    conflict_set = """
        ON CONFLICT (openalex_id) DO UPDATE SET
            doi = EXCLUDED.doi,
            title = EXCLUDED.title,
            publication_date = EXCLUDED.publication_date,
            publication_year = EXCLUDED.publication_year,
            type = EXCLUDED.type,
            language = EXCLUDED.language,
            oa_status = EXCLUDED.oa_status,
            is_oa = EXCLUDED.is_oa,
            source_name = EXCLUDED.source_name,
            topic_display_name = EXCLUDED.topic_display_name,
            topic_subfield_name = EXCLUDED.topic_subfield_name,
            topic_field_name = EXCLUDED.topic_field_name,
            topic_domain_name = EXCLUDED.topic_domain_name,
            cited_by_count = EXCLUDED.cited_by_count,
            referenced_works_count = EXCLUDED.referenced_works_count,
            authors_count = EXCLUDED.authors_count,
            citation_percentile_year = EXCLUDED.citation_percentile_year,
            citation_normalized_percentile = EXCLUDED.citation_normalized_percentile,
            fwci = EXCLUDED.fwci,
            is_retracted = EXCLUDED.is_retracted,
            created_date = EXCLUDED.created_date,
            updated_date = EXCLUDED.updated_date,
            ingested_at = now()
        """
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            values_placeholders = ", ".join([one_row] * len(batch))
            upsert_sql = f"""
                INSERT INTO papers ({col_list})
                VALUES {values_placeholders}
                {conflict_set}
            """
            flat = [row[c] for row in batch for c in PAPERS_COLUMNS]
            cur.execute(upsert_sql, flat)
    return len(rows)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Load papers from OpenAlex JSON into the papers table.")
    parser.add_argument("json_path", type=Path, help="Path to JSON file (e.g. temp/ai_papers_20260306_173356.json)")
    parser.add_argument("--dry-run", action="store_true", help="Only load and transform; do not connect or insert.")
    args = parser.parse_args()

    json_path = args.json_path
    if not json_path.is_absolute():
        json_path = (REPO_ROOT / json_path).resolve()
    if not json_path.exists():
        print(f"ERROR: File not found: {json_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Loading {json_path} ...", file=sys.stderr)
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    works = data if isinstance(data, list) else [data]
    rows = []
    for w in works:
        row = work_to_row(w)
        if row:
            rows.append(row)
        else:
            print("Skipping work without openalex id.", file=sys.stderr)

    if not rows:
        print("No papers to insert.", file=sys.stderr)
        sys.exit(0)

    print(f"Processed {len(rows)} papers.", file=sys.stderr)
    if args.dry_run:
        print("Dry run: not connecting or inserting.", file=sys.stderr)
        return

    with psycopg.connect(DATABASE_URL) as conn:
        ensure_papers_schema(conn)
        upsert_papers(conn, rows)
        conn.commit()
    print(f"Inserted/updated {len(rows)} rows in papers (deduplicated by openalex_id).", file=sys.stderr)


if __name__ == "__main__":
    main()
