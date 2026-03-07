#!/usr/bin/env python3
"""
Fetch recent AI research papers from OpenAlex and save to a timestamped JSON file.

Filtering is done entirely on the API: only works matching the topic and date
filter are returned (no irrelevant papers downloaded). We filter by
primary_topic so that only works whose main topic is AI are included.
"""

import json
import os
import re
from datetime import datetime, timedelta, timezone

from pyalex import Fields, Subfields, Works

# Normalized name we match against (field or subfield display_name)
AI_TOPIC_NAME = "artificial intelligence"


def _normalize(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip().lower())


def _get_openalex_id(url_or_id: str) -> str:
    """Return short ID from OpenAlex URL or id (e.g. 'https://openalex.org/fields/17' -> '17')."""
    s = (url_or_id or "").strip()
    if "/" in s:
        s = s.rstrip("/").rsplit("/", 1)[-1]
    return s


def fetch_recent_ai_papers(save_dir: str | None = None) -> list[dict]:
    """
    Query OpenAlex for recent AI papers (primary_topic filter + last 3 days).
    Returns list of work dicts. If save_dir is set, also save JSON there and print path.
    """
    target = _normalize(AI_TOPIC_NAME)
    field_ids: list[str] = []
    subfield_ids: list[str] = []

    # Resolve "Artificial intelligence" as field or subfield (by display_name, topic key)
    search_filter = {"display_name.search": AI_TOPIC_NAME}
    for sub in list(Subfields().filter(**search_filter).get(per_page=25)):
        if _normalize(sub.get("display_name") or "") == target:
            subfield_ids.append(_get_openalex_id(sub["id"]))
    for f in list(Fields().filter(**search_filter).get(per_page=25)):
        if _normalize(f.get("display_name") or "") == target:
            field_ids.append(_get_openalex_id(f["id"]))

    if not field_ids and not subfield_ids:
        raise SystemExit(
            f"No field or subfield found with display_name '{AI_TOPIC_NAME}'. "
            "Check OpenAlex Fields/Subfields API."
        )
    print(
        f"Using primary_topic filter (API-side): field.id in {field_ids or '[]'}, subfield.id in {subfield_ids or '[]'}"
    )

    to_date = datetime.now(timezone.utc).date()
    from_date = to_date - timedelta(days=3)
    from_str = from_date.isoformat()
    to_str = to_date.isoformat()

    # All filtering is done by the OpenAlex API; only matching works are returned.
    # Use primary_topic (not topics) so we only get works whose main topic is AI.
    date_filter = {
        "from_publication_date": from_str,
        "to_publication_date": to_str,
    }

    def build_works_query(**topic_filter: str):
        """Works API with primary_topic + date filter (server returns only matching works)."""
        return Works().filter(**date_filter, **topic_filter)

    if field_ids and subfield_ids:
        # API doesn't support OR across different filter keys; run two filtered queries and merge by id
        seen_ids: set[str] = set()
        papers = []
        for filter_key, ids in (
            ("primary_topic.field.id", field_ids),
            ("primary_topic.subfield.id", subfield_ids),
        ):
            q = build_works_query(**{filter_key: "|".join(ids)})
            for page in q.paginate(per_page=200, n_max=None):
                for work in page:
                    wid = work.get("id") or ""
                    if wid not in seen_ids:
                        seen_ids.add(wid)
                        papers.append(dict(work))
    elif field_ids:
        works_query = build_works_query(**{"primary_topic.field.id": "|".join(field_ids)})
        papers = [
            dict(work)
            for page in works_query.paginate(per_page=200, n_max=None)
            for work in page
        ]
    else:
        works_query = build_works_query(
            **{"primary_topic.subfield.id": "|".join(subfield_ids)}
        )
        papers = [
            dict(work)
            for page in works_query.paginate(per_page=200, n_max=None)
            for work in page
        ]

    print(f"Fetched {len(papers)} paper(s) from {from_str} to {to_str}")

    if save_dir is not None:
        os.makedirs(save_dir, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(save_dir, f"ai_papers_{timestamp}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(papers, f, ensure_ascii=False, indent=2)
        print(f"Saved to {out_path}")

    return papers


def main() -> None:
    repo_dir = os.path.dirname(os.path.abspath(__file__))
    temp_dir = os.path.join(repo_dir, "temp")
    fetch_recent_ai_papers(save_dir=temp_dir)


if __name__ == "__main__":
    main()
