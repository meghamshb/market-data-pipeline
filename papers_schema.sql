-- Unnested schema for OpenAlex papers (works) for dashboard use.
-- Only fields needed for a dashboard: identifiers, quantitative metrics,
-- and a few categorical dimensions. No nested JSON arrays.

CREATE TABLE IF NOT EXISTS papers (
  -- Identifiers
  openalex_id   text PRIMARY KEY,                    -- e.g. https://openalex.org/W7133503369
  doi           text,                                -- optional DOI link

  -- Display
  title         text NOT NULL,
  publication_date date,                             -- for time-series and recency
  publication_year smallint,                        -- for yearly aggregates and filters

  -- Type and language (categorical)
  type          text,                                -- article, dataset, etc.
  language      text,                                -- en, etc.

  -- Open access (categorical – good for dashboard filters/charts)
  oa_status     text,                                -- green, gold, hybrid, closed, bronze
  is_oa         boolean,

  -- Venue / source (one denormalized name for dashboard)
  source_name   text,                                -- primary_location.source.display_name

  -- Primary topic (one level for dashboard; from primary_topic or first topic)
  topic_display_name text,                           -- e.g. "Computability, Logic, AI Algorithms"
  topic_subfield_name text,                          -- e.g. "Computational Theory and Mathematics"
  topic_field_name   text,                          -- e.g. "Computer Science"
  topic_domain_name  text,                          -- e.g. "Physical Sciences"

  -- Quantitative metrics
  cited_by_count            int DEFAULT 0,           -- citations received
  referenced_works_count    int DEFAULT 0,           -- references (outgoing)
  authors_count             int,                     -- number of authors (if available)
  citation_percentile_year   smallint,                -- year used for percentile
  citation_normalized_percentile float,              -- 0–100
  fwci                      float,                   -- field-weighted citation impact

  -- Quality / status flags
  is_retracted boolean DEFAULT false,

  -- Timestamps (for “last updated” and sync)
  created_date  date,
  updated_date  date,
  ingested_at   timestamptz DEFAULT now()
);

-- Indexes for common dashboard queries
CREATE INDEX IF NOT EXISTS idx_papers_publication_year ON papers (publication_year);
CREATE INDEX IF NOT EXISTS idx_papers_publication_date ON papers (publication_date);
CREATE INDEX IF NOT EXISTS idx_papers_oa_status ON papers (oa_status);
CREATE INDEX IF NOT EXISTS idx_papers_topic_field ON papers (topic_field_name);
CREATE INDEX IF NOT EXISTS idx_papers_cited_by_count ON papers (cited_by_count);
CREATE INDEX IF NOT EXISTS idx_papers_source_name ON papers (source_name);

COMMENT ON TABLE papers IS 'Flattened OpenAlex works for dashboard: metrics, OA status, primary topic and venue.';
