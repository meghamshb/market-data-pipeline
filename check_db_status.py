#!/usr/bin/env python3
"""Connect to Neon PostgreSQL and report status."""
import os
import sys

# Load .env (strip spaces from values)
from pathlib import Path
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip().strip('"').strip("'")

import psycopg

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set in .env")
    sys.exit(1)

def main():
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                # Server version
                cur.execute("SELECT version();")
                version = cur.fetchone()[0]
                print("Connection: OK")
                print(f"Server: {version}")

                # Current database and user
                cur.execute("SELECT current_database(), current_user;")
                db, user = cur.fetchone()
                print(f"Database: {db}")
                print(f"User: {user}")

                # List tables (if any)
                cur.execute("""
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                    ORDER BY table_schema, table_name;
                """)
                tables = cur.fetchall()
                if tables:
                    print(f"\nTables ({len(tables)}):")
                    for schema, name in tables:
                        print(f"  - {schema}.{name}")
                else:
                    print("\nTables: (none)")

        print("\nStatus: Database is reachable and healthy.")
    except Exception as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
