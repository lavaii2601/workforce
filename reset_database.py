from __future__ import annotations

import argparse
from pathlib import Path

from backend.db import DB_PATH, get_conn, init_db, is_postgres_backend


def _confirm_reset(skip_confirm: bool) -> bool:
    if skip_confirm:
        return True

    print("[reset-db] WARNING: This will permanently delete all current data.")
    print("[reset-db] Type RESET to continue, or anything else to cancel.")
    answer = input("> ").strip()
    if answer != "RESET":
        print("[reset-db] Cancelled.")
        return False
    return True


def _reset_sqlite(db_path: Path) -> None:
    if db_path.exists():
        db_path.unlink()
        print(f"[reset-db] Deleted SQLite file: {db_path}")
    else:
        print(f"[reset-db] SQLite file not found, creating new one: {db_path}")

    init_db()
    print("[reset-db] SQLite reset complete.")


def _reset_postgres() -> None:
    conn = get_conn()
    conn.execute(
        """
        DO $$
        DECLARE
            r RECORD;
        BEGIN
            FOR r IN (
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = 'public'
            ) LOOP
                EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', r.tablename);
            END LOOP;
        END
        $$;
        """
    )
    conn.commit()
    conn.close()

    init_db()
    print("[reset-db] Postgres reset complete.")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reset workforce-manager database to a clean state (manual operation only)."
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip interactive confirmation prompt.",
    )
    args = parser.parse_args()

    if not _confirm_reset(skip_confirm=args.yes):
        return 1

    if is_postgres_backend():
        print("[reset-db] Detected backend: Postgres")
        _reset_postgres()
    else:
        print(f"[reset-db] Detected backend: SQLite ({DB_PATH})")
        _reset_sqlite(DB_PATH)

    print("[reset-db] Done. Database is clean and re-initialized.")
    print("[reset-db] This script only runs when you execute it manually.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
