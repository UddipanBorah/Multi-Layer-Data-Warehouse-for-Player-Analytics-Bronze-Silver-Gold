"""
run_pipeline.py
================
Executes the full Bronze -> Silver -> Gold medallion pipeline against a
DuckDB database file, in order, and runs a set of sanity/validation checks
at the end (row-count checks + a comparison of event-derived retention
against the REAL published Cookie Cats retention rates).

Usage:
    python3 scripts/run_pipeline.py
"""

import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "game_analytics.duckdb"

SQL_FILES_IN_ORDER = [
    ROOT / "sql" / "bronze" / "01_bronze.sql",
    ROOT / "sql" / "silver" / "01_silver_events.sql",
    ROOT / "sql" / "silver" / "02_silver_users.sql",
    ROOT / "sql" / "gold" / "01_gold_retention.sql",
    ROOT / "sql" / "gold" / "02_gold_monetization.sql",
    ROOT / "sql" / "gold" / "03_gold_engagement_segments.sql",
]


def run_sql_file(con: duckdb.DuckDBPyConnection, path: Path):
    print(f"\n{'=' * 80}\n-- Running {path.relative_to(ROOT)}\n{'=' * 80}")
    sql = path.read_text()
    result = con.execute(sql)
    try:
        df = result.fetch_df()
        if not df.empty:
            print(df.to_string(index=False))
    except Exception:
        pass


def validate(con: duckdb.DuckDBPyConnection):
    print(f"\n{'=' * 80}\n-- VALIDATION\n{'=' * 80}")

    checks = []

    # 1) no data lost: every real user in the source dataset exists in silver.users_dim
    real_count, silver_count = con.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM bronze.raw_experiment_assignments),
            (SELECT COUNT(*) FROM silver.users_dim)
        """
    ).fetchone()
    checks.append(("real users preserved (bronze -> silver.users_dim)", real_count == silver_count,
                    f"real={real_count} silver={silver_count}"))

    # 2) deduplication actually removed the injected duplicate events
    raw_events, clean_events = con.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM bronze.raw_events),
            (SELECT COUNT(*) FROM silver.events_clean)
        """
    ).fetchone()
    checks.append(("deduplication removed rows (bronze.raw_events > silver.events_clean)",
                    raw_events > clean_events, f"raw={raw_events} clean={clean_events}"))

    checks.append(("no duplicate event_ids remain in silver.events_clean",
                    con.execute("SELECT COUNT(*) FROM (SELECT event_id, COUNT(*) c FROM silver.events_clean GROUP BY event_id HAVING COUNT(*) > 1)").fetchone()[0] == 0,
                    ""))

    # 3) event-derived retention should closely track the REAL published rates
    rows = con.execute(
        """
        SELECT ab_version,
               real_retention_1_rate, event_derived_retention_1_rate,
               real_retention_7_rate, event_derived_retention_7_rate
        FROM gold.mart_retention_validation
        """
    ).fetchall()
    for ab_version, r1, e1, r7, e7 in rows:
        checks.append((f"[{ab_version}] event-derived D1 retention within 5pp of real ({r1} vs {e1})",
                        abs(r1 - e1) < 0.05, ""))
        checks.append((f"[{ab_version}] event-derived D7 retention within 5pp of real ({r7} vs {e7})",
                        abs(r7 - e7) < 0.05, ""))

    # 4) every session belongs to a user that exists in users_dim (referential integrity)
    orphan_sessions = con.execute(
        "SELECT COUNT(*) FROM silver.sessions s LEFT JOIN silver.users_dim u ON u.userid = s.userid WHERE u.userid IS NULL"
    ).fetchone()[0]
    checks.append(("no orphan sessions (all sessions map to a known user)", orphan_sessions == 0, f"orphans={orphan_sessions}"))

    print()
    all_pass = True
    for name, passed, detail in checks:
        status = "PASS" if passed else "FAIL"
        all_pass = all_pass and passed
        print(f"  [{status}] {name} {detail}")

    print(f"\nOverall: {'ALL CHECKS PASSED' if all_pass else 'SOME CHECKS FAILED'}")
    return all_pass


def main():
    if DB_PATH.exists():
        DB_PATH.unlink()

    con = duckdb.connect(str(DB_PATH))
    con.execute(f"SET file_search_path='{ROOT}';")
    import os
    os.chdir(ROOT)  # so relative paths in the .sql files (data/...) resolve

    for sql_file in SQL_FILES_IN_ORDER:
        run_sql_file(con, sql_file)

    ok = validate(con)
    con.close()

    print(f"\nDuckDB database written to: {DB_PATH}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
