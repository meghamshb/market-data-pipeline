#!/usr/bin/env python3
"""
Fetch recent AI research papers from OpenAlex and save to a timestamped JSON file.

Steps:
1. Search OpenAlex Concepts for "artificial intelligence"
2. Get the concept ID from the search results
3. Use that concept ID to filter Works from the last 3 days
4. Save all papers (with all fields) in temp/ as a timestamped JSON file.
"""

import json
import os
import warnings
from datetime import datetime, timedelta, timezone

from pyalex import Concepts, Works

# Use Concepts as specified (OpenAlex recommends Topics for new code)
warnings.filterwarnings("ignore", message=".*Concepts is deprecated.*", category=DeprecationWarning)


def main() -> None:
    # 1. Search OpenAlex Concepts for "artificial intelligence"
    concepts = Concepts().search("artificial intelligence").get(per_page=5)
    if not concepts:
        raise SystemExit("No concepts found for 'artificial intelligence'")

    # 2. Get the concept ID from the first (best) match
    concept = concepts[0]
    concept_id = concept["id"]
    concept_name = concept.get("display_name", concept_id)
    print(f"Using concept: {concept_name} ({concept_id})")

    # 3. Filter Works: this concept + last 3 days (publication date)
    to_date = datetime.now(timezone.utc).date()
    from_date = to_date - timedelta(days=3)
    from_str = from_date.isoformat()
    to_str = to_date.isoformat()

    works_query = (
        Works()
        .filter(
            concepts={"id": concept_id},
            from_publication_date=from_str,
            to_publication_date=to_str,
        )
    )

    # Paginate to fetch all works (full records, no field selection)
    papers: list[dict] = []
    for page in works_query.paginate(per_page=200, n_max=None):
        for work in page:
            papers.append(dict(work))

    print(f"Fetched {len(papers)} paper(s) from {from_str} to {to_str}")

    # 4. Save to temp/ with a timestamped filename
    temp_dir = os.path.join(os.path.dirname(__file__), "temp")
    os.makedirs(temp_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(temp_dir, f"ai_papers_{timestamp}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(papers, f, ensure_ascii=False, indent=2)
    print(f"Saved to {out_path}")


if __name__ == "__main__":
    main()
