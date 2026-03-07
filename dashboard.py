#!/usr/bin/env python3
"""
Neon database dashboard – connect to Neon PostgreSQL and display fundamental
insights about the papers data (OpenAlex works).
"""
import os
from pathlib import Path

# Load .env (same pattern as check_db_status.py)
_env_path = Path(__file__).resolve().parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip().strip('"').strip("'")

import streamlit as st
import psycopg
import pandas as pd

DATABASE_URL = os.environ.get("DATABASE_URL")


@st.cache_resource
def get_connection():
    """Cached Neon DB connection."""
    if not DATABASE_URL:
        return None
    try:
        return psycopg.connect(DATABASE_URL)
    except Exception:
        return None


def run_query(conn, sql: str, params=None):
    """Run a query and return rows as list of tuples."""
    if conn is None:
        return []
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()
    except Exception:
        return []


def run_query_columns(conn, sql: str, params=None):
    """Run a query and return (column_names, rows)."""
    if conn is None:
        return [], []
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            cols = [d.name for d in cur.description]
            return cols, cur.fetchall()
    except Exception:
        return [], []


def papers_table_exists(conn) -> bool:
    """Return True if public.papers exists."""
    rows = run_query(
        conn,
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'papers'
        """,
    )
    return bool(rows)


def main():
    st.set_page_config(
        page_title="Papers Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.title("📊 Papers dashboard")
    st.caption("Insights from your Neon PostgreSQL papers data (OpenAlex)")

    conn = get_connection()
    if conn is None:
        st.error("Could not connect to the database. Set `DATABASE_URL` in `.env`.")
        return

    # Sidebar: connection status
    with st.sidebar:
        st.header("Connection")
        version = run_query(conn, "SELECT version();")
        if version:
            st.success("Connected to Neon")
            st.caption(version[0][0].split(",")[0] if version[0] else "")
        else:
            st.error("Connection failed")

        st.divider()
        st.header("Navigation")
        section = st.radio(
            "Section",
            ["Overview", "By year & OA", "Sources & topics", "Citations", "Recent papers"],
            label_visibility="collapsed",
        )

    if not papers_table_exists(conn):
        st.warning("The `papers` table does not exist yet. Run the pipeline to fetch and load data.")
        return

    if section == "Overview":
        _render_overview(conn)
    elif section == "By year & OA":
        _render_by_year_oa(conn)
    elif section == "Sources & topics":
        _render_sources_topics(conn)
    elif section == "Citations":
        _render_citations(conn)
    else:
        _render_recent_papers(conn)


def _render_overview(conn):
    st.header("Overview")

    # Metrics row
    total = run_query(conn, "SELECT count(*) FROM papers")
    n_papers = total[0][0] if total else 0

    oa_count = run_query(conn, "SELECT count(*) FROM papers WHERE is_oa = true")
    n_oa = oa_count[0][0] if oa_count else 0
    oa_pct = (100 * n_oa / n_papers) if n_papers else 0

    years = run_query(conn, "SELECT count(DISTINCT publication_year) FROM papers WHERE publication_year IS NOT NULL")
    n_years = years[0][0] if years else 0

    total_cites = run_query(conn, "SELECT coalesce(sum(cited_by_count), 0) FROM papers")
    total_citations = total_cites[0][0] if total_cites else 0

    cols = st.columns(4)
    cols[0].metric("Total papers", f"{n_papers:,}")
    cols[1].metric("Open access", f"{n_oa:,} ({oa_pct:.1f}%)")
    cols[2].metric("Publication years", n_years)
    cols[3].metric("Total citations", f"{total_citations:,}")

    st.divider()
    st.subheader("Papers by publication year")
    col_names, rows = run_query_columns(
        conn,
        """
        SELECT publication_year AS year, count(*) AS papers
        FROM papers
        WHERE publication_year IS NOT NULL
        GROUP BY publication_year
        ORDER BY publication_year
        """,
    )
    if rows:
        df = pd.DataFrame(rows, columns=col_names).astype({"year": "Int64", "papers": "int64"})
        st.bar_chart(df.set_index("year"))
    else:
        st.info("No publication year data yet.")


def _render_by_year_oa(conn):
    st.header("By year & open access")

    st.subheader("Papers per year")
    col_names, rows = run_query_columns(
        conn,
        """
        SELECT publication_year AS year, count(*) AS count
        FROM papers
        WHERE publication_year IS NOT NULL
        GROUP BY publication_year
        ORDER BY year
        """,
    )
    if rows:
        st.bar_chart(pd.DataFrame(rows, columns=col_names).set_index("year"))

    st.subheader("Open access status")
    oa_cols, oa_rows = run_query_columns(
        conn,
        """
        SELECT coalesce(oa_status, 'unknown') AS oa_status, count(*) AS count
        FROM papers
        GROUP BY oa_status
        ORDER BY count DESC
        """,
    )
    if oa_rows:
        oa_df = pd.DataFrame(oa_rows, columns=oa_cols)
        c1, c2 = st.columns([1, 1])
        with c1:
            st.dataframe(oa_df, use_container_width=True, hide_index=True)
        with c2:
            st.bar_chart(oa_df.set_index("oa_status"))
    else:
        st.info("No OA status data.")


def _render_sources_topics(conn):
    st.header("Sources & topics")

    st.subheader("Top sources (venues)")
    cols, rows = run_query_columns(
        conn,
        """
        SELECT source_name, count(*) AS papers
        FROM papers
        WHERE source_name IS NOT NULL AND trim(source_name) != ''
        GROUP BY source_name
        ORDER BY papers DESC
        LIMIT 20
        """,
    )
    if rows:
        st.dataframe(pd.DataFrame(rows, columns=cols), use_container_width=True, hide_index=True)
        df = pd.DataFrame(rows, columns=cols)
        st.bar_chart(df.set_index("source_name"))
    else:
        st.info("No source data yet.")

    st.subheader("Top topic fields")
    tcols, trows = run_query_columns(
        conn,
        """
        SELECT topic_field_name AS field, count(*) AS papers
        FROM papers
        WHERE topic_field_name IS NOT NULL AND trim(topic_field_name) != ''
        GROUP BY topic_field_name
        ORDER BY papers DESC
        LIMIT 15
        """,
    )
    if trows:
        st.dataframe(pd.DataFrame(trows, columns=tcols), use_container_width=True, hide_index=True)
    else:
        st.info("No topic field data yet.")


def _render_citations(conn):
    st.header("Citations")

    st.subheader("Most cited papers")
    cols, rows = run_query_columns(
        conn,
        """
        SELECT title, cited_by_count, publication_year, source_name
        FROM papers
        WHERE cited_by_count > 0
        ORDER BY cited_by_count DESC
        LIMIT 25
        """,
    )
    if rows:
        df = pd.DataFrame(rows, columns=cols)
        # Truncate long titles for display
        if "title" in df.columns:
            df["title"] = df["title"].where(df["title"].str.len() <= 80, df["title"].str.slice(0, 80) + "…")
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No citation data yet.")

    st.subheader("Citation distribution (buckets)")
    bcols, brows = run_query_columns(
        conn,
        """
        SELECT
            case
                when cited_by_count = 0 then '0'
                when cited_by_count <= 5 then '1-5'
                when cited_by_count <= 20 then '6-20'
                when cited_by_count <= 100 then '21-100'
                else '100+'
            end AS bucket,
            count(*) AS papers
        FROM papers
        GROUP BY 1
        ORDER BY min(cited_by_count)
        """,
    )
    if brows:
        st.bar_chart(pd.DataFrame(brows, columns=bcols).set_index("bucket"))


def _render_recent_papers(conn):
    st.header("Recent papers")
    cols, rows = run_query_columns(
        conn,
        """
        SELECT title, publication_date, publication_year, source_name, cited_by_count, oa_status
        FROM papers
        ORDER BY publication_date DESC NULLS LAST, ingested_at DESC NULLS LAST
        LIMIT 100
        """,
    )
    if not rows:
        st.info("No papers in the database yet.")
        return
    df = pd.DataFrame(rows, columns=cols)
    if "title" in df.columns:
        df["title"] = df["title"].where(df["title"].str.len() <= 100, df["title"].str.slice(0, 100) + "…")
    st.dataframe(df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
