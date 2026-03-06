"""Temporary script: fetch and print titles of 5 AI-related papers via OpenAlex (pyalex)."""
from pyalex import Works

works = Works().search("artificial intelligence").get(per_page=5)

for i, work in enumerate(works, 1):
    title = work.get("title") or "(no title)"
    print(f"{i}. {title}")
