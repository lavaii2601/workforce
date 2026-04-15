import os
import re
import socket
import sqlite3
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlparse

from werkzeug.security import generate_password_hash
from .constants import SHIFT_DEFINITIONS


POSTGRES_NOW_TEXT_EXPR = "(to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'))"
ID_TABLES = {
    "branches",
    "users",
    "shift_preferences",
    "weekly_schedule",
    "ceo_chat_messages",
    "auth_sessions",
    "attendance_logs",
    "attendance_employee_codes",
    "attendance_qr_one_time_codes",
    "shift_attendance_marks",
    "issue_reports",
    "issue_report_replies",
    "audit_logs",
}


def _resolve_database_url():
    candidates = [
        os.getenv("DATABASE_URL"),
        os.getenv("SUPABASE_DATABASE_URL"),
        os.getenv("POSTGRES_URL"),
        os.getenv("POSTGRES_PRISMA_URL"),
        os.getenv("POSTGRES_URL_NON_POOLING"),
    ]
    for value in candidates:
        if value and value.strip():
            return value.strip()
    return ""


def _ensure_sslmode(url):
    """Append sslmode=require if not already present — required by Supabase."""
    if not url:
        return url
    if "sslmode" in url:
        return url
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}sslmode=require"


def _resolve_host_to_ipv4(hostname):
    """Resolve hostname to an IPv4 address.

    Vercel Lambda does not support IPv6 outbound connections.
    Supabase DNS may return an IPv6 (AAAA) record which causes
    'Cannot assign requested address' errors on Vercel.
    """
    try:
        results = socket.getaddrinfo(hostname, None, socket.AF_INET, socket.SOCK_STREAM)
        if results:
            return results[0][4][0]
    except (socket.gaierror, OSError, IndexError):
        pass
    return None


def _prepare_database_url(url):
    """Prepare a DATABASE_URL for serverless deployment.

    1. Ensure sslmode=require (Supabase requirement)
    2. Add connect_timeout for cold starts
    """
    if not url:
        return url

    url = _ensure_sslmode(url)

    # Add connect_timeout if missing
    if "connect_timeout" not in url:
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}connect_timeout=10"

    return url


DATABASE_URL = _prepare_database_url(_resolve_database_url())
IS_POSTGRES = bool(DATABASE_URL)


def is_postgres_backend():
    return IS_POSTGRES


def _transform_sql_for_postgres(sql):
    return _transform_sql_for_postgres_cached(str(sql or ""))


@lru_cache(maxsize=1024)
def _transform_sql_for_postgres_cached(sql):
    transformed = sql
    has_nocase = "COLLATE NOCASE" in transformed.upper()
    if has_nocase:
        transformed = re.sub(r"\s+COLLATE\s+NOCASE", "", transformed, flags=re.IGNORECASE)
        transformed = re.sub(r"\s+LIKE\s+", " ILIKE ", transformed, flags=re.IGNORECASE)

    transformed = re.sub(
        r"GROUP_CONCAT\(([^,\)]+),\s*'([^']*)'\)",
        r"STRING_AGG((\1)::text, '\2')",
        transformed,
        flags=re.IGNORECASE,
    )
    transformed = re.sub(
        r"GROUP_CONCAT\(([^\)]+)\)",
        r"STRING_AGG((\1)::text, ',')",
        transformed,
        flags=re.IGNORECASE,
    )

    transformed = re.sub(r"\bCURRENT_TIMESTAMP\b", POSTGRES_NOW_TEXT_EXPR, transformed)

    transformed = re.sub(r"\?", "%s", transformed)
    return transformed


class _PgCursorAdapter:
    def __init__(self, pg_cursor):
        self._cursor = pg_cursor
        self.lastrowid = None

    def execute(self, sql, params=None):
        transformed_sql = _transform_sql_for_postgres(sql)
        final_sql = transformed_sql

        insert_table_match = re.match(
            r"\s*INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)",
            transformed_sql,
            flags=re.IGNORECASE,
        )
        if insert_table_match and "RETURNING" not in transformed_sql.upper():
            table_name = insert_table_match.group(1).lower()
            if table_name in ID_TABLES:
                final_sql = f"{transformed_sql.rstrip()} RETURNING id"

        self._cursor.execute(final_sql, tuple(params or ()))
        if final_sql != transformed_sql:
            row = self._cursor.fetchone()
            self.lastrowid = row["id"] if row else None
        else:
            self.lastrowid = None
        return self

    def executemany(self, sql, seq_of_params):
        transformed_sql = _transform_sql_for_postgres(sql)
        self._cursor.executemany(transformed_sql, [tuple(item) for item in seq_of_params])
        self.lastrowid = None
        return self

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()


class _PgConnAdapter:
    def __init__(self, pg_conn):
        self._conn = pg_conn

    def execute(self, sql, params=None):
        cur = _PgCursorAdapter(self._conn.cursor())
        return cur.execute(sql, params)

    def executemany(self, sql, seq_of_params):
        cur = _PgCursorAdapter(self._conn.cursor())
        return cur.executemany(sql, seq_of_params)

    def cursor(self):
        return _PgCursorAdapter(self._conn.cursor())

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()


def _resolve_db_path():
    configured = (os.getenv("SQLITE_PATH") or "").strip()
    if configured:
        return Path(configured)

    if os.getenv("VERCEL") == "1":
        # Vercel filesystem is read-only except /tmp.
        return Path("/tmp") / "data.db"

    return Path(__file__).resolve().parent.parent / "data.db"


DB_PATH = _resolve_db_path()


def get_conn(*, autocommit=False):
    if IS_POSTGRES:
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise RuntimeError(
                "Postgres backend selected but psycopg is unavailable. "
                "Ensure dependencies include psycopg and psycopg-binary for serverless runtime."
            ) from exc

        connect_kwargs = {
            "conninfo": DATABASE_URL,
            "row_factory": dict_row,
            "autocommit": autocommit,
        }
        # Resolve hostname to IPv4 — Vercel Lambda does not support IPv6
        try:
            parsed = urlparse(DATABASE_URL)
            hostname = parsed.hostname
            if hostname:
                ipv4 = _resolve_host_to_ipv4(hostname)
                if ipv4:
                    connect_kwargs["hostaddr"] = ipv4
        except Exception:
            pass
        pg_conn = psycopg.connect(**connect_kwargs)
        return _PgConnAdapter(pg_conn)

    # Use a longer wait and WAL to reduce transient "database is locked" errors
    # when multiple requests hit SQLite close together.
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    try:
        conn.execute("PRAGMA journal_mode = WAL")
    except sqlite3.DatabaseError:
        # Ignore if the database file cannot switch journal mode in this environment.
        pass
    return conn


def init_db():
    try:
        _init_db_inner()
    except Exception as exc:
        print(f"WARNING: Cannot initialize database: {exc}")
        print("App will continue running but database may not be ready.")


def _init_db_inner():
    conn = get_conn(autocommit=IS_POSTGRES)
    cur = conn.cursor()

    schema_sql = """
        CREATE TABLE IF NOT EXISTS branches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            location TEXT,
            network_ip TEXT
        );

        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            display_name TEXT NOT NULL,
            role TEXT NOT NULL CHECK (role IN ('employee', 'manager', 'ceo')),
            branch_id INTEGER,
            password_hash TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            avatar_data_url TEXT,
            full_name TEXT,
            date_of_birth TEXT,
            phone_number TEXT,
            address TEXT,
            job_position TEXT,
            FOREIGN KEY (branch_id) REFERENCES branches(id)
        );

        CREATE TABLE IF NOT EXISTS employee_branch_access (
            employee_id INTEGER NOT NULL,
            branch_id INTEGER NOT NULL,
            PRIMARY KEY (employee_id, branch_id),
            FOREIGN KEY (employee_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS shift_preferences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id INTEGER NOT NULL,
            week_start TEXT NOT NULL,
            branch_id INTEGER NOT NULL,
            shift_code TEXT NOT NULL,
            day_of_week INTEGER NOT NULL DEFAULT 0,
            registration_type TEXT NOT NULL DEFAULT 'individual' CHECK (registration_type IN ('individual', 'group')),
            group_code TEXT,
            flexible_start_at TEXT,
            flexible_end_at TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (employee_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS weekly_schedule (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            week_start TEXT NOT NULL,
            branch_id INTEGER NOT NULL,
            employee_id INTEGER NOT NULL,
            shift_code TEXT NOT NULL,
            day_of_week INTEGER NOT NULL DEFAULT 0,
            registration_type TEXT NOT NULL DEFAULT 'individual' CHECK (registration_type IN ('individual', 'group')),
            group_code TEXT,
            flexible_start_at TEXT,
            flexible_end_at TEXT,
            assigned_by INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE,
            FOREIGN KEY (employee_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (assigned_by) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS ceo_chat_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id INTEGER NOT NULL,
            sender_type TEXT NOT NULL DEFAULT 'user',
            sender_label TEXT,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (sender_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS auth_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            token TEXT NOT NULL UNIQUE,
            expires_at TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS attendance_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id INTEGER NOT NULL,
            branch_id INTEGER,
            check_in_at TEXT NOT NULL,
            confirmed_at TEXT,
            check_out_at TEXT,
            minutes_worked INTEGER,
            scheduled_shift_start_at TEXT,
            minutes_late INTEGER DEFAULT 0,
            checked_in_by_manager_id INTEGER,
            manager_check_in_note TEXT,
            note TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (employee_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE SET NULL,
            FOREIGN KEY (checked_in_by_manager_id) REFERENCES users(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS attendance_confirm_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            attendance_log_id INTEGER NOT NULL,
            employee_id INTEGER NOT NULL,
            branch_id INTEGER,
            confirmed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            source TEXT NOT NULL DEFAULT 'employee_confirm',
            note TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (attendance_log_id) REFERENCES attendance_logs(id) ON DELETE CASCADE,
            FOREIGN KEY (employee_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS shift_registration_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_code TEXT NOT NULL,
            group_name TEXT NOT NULL,
            week_start TEXT NOT NULL,
            branch_id INTEGER NOT NULL,
            max_members INTEGER,
            created_by_employee_id INTEGER NOT NULL,
            note TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE,
            FOREIGN KEY (created_by_employee_id) REFERENCES users(id) ON DELETE CASCADE,
            UNIQUE (week_start, branch_id, group_code)
        );

        CREATE TABLE IF NOT EXISTS shift_registration_group_members (
            group_id INTEGER NOT NULL,
            employee_id INTEGER NOT NULL,
            joined_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (group_id, employee_id),
            FOREIGN KEY (group_id) REFERENCES shift_registration_groups(id) ON DELETE CASCADE,
            FOREIGN KEY (employee_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS attendance_employee_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id INTEGER NOT NULL,
            branch_id INTEGER NOT NULL,
            code TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            consumed_at TEXT,
            request_ip TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (employee_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS attendance_qr_one_time_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id INTEGER NOT NULL,
            qr_token TEXT NOT NULL,
            one_time_code TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            consumed_at TEXT,
            generated_by_manager_id INTEGER NOT NULL,
            consumed_by_employee_id INTEGER,
            request_ip TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE,
            FOREIGN KEY (generated_by_manager_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (consumed_by_employee_id) REFERENCES users(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS shift_attendance_marks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            week_start TEXT NOT NULL,
            day_of_week INTEGER NOT NULL,
            shift_code TEXT NOT NULL,
            branch_id INTEGER NOT NULL,
            employee_id INTEGER NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('present', 'absent', 'present_override')),
            source TEXT NOT NULL DEFAULT 'system',
            attendance_log_id INTEGER,
            note TEXT,
            marked_by_manager_id INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE,
            FOREIGN KEY (employee_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (attendance_log_id) REFERENCES attendance_logs(id) ON DELETE SET NULL,
            FOREIGN KEY (marked_by_manager_id) REFERENCES users(id) ON DELETE SET NULL,
            UNIQUE (week_start, day_of_week, shift_code, branch_id, employee_id)
        );

        CREATE TABLE IF NOT EXISTS issue_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reporter_id INTEGER NOT NULL,
            reporter_role TEXT NOT NULL,
            branch_id INTEGER,
            target_employee_id INTEGER,
            title TEXT NOT NULL,
            details TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'in_review', 'escalated', 'resolved')),
            escalated_to_ceo INTEGER NOT NULL DEFAULT 0,
            manager_note TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (reporter_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE SET NULL,
            FOREIGN KEY (target_employee_id) REFERENCES users(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS issue_report_replies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            issue_id INTEGER NOT NULL,
            sender_id INTEGER NOT NULL,
            sender_role TEXT NOT NULL CHECK (sender_role IN ('manager', 'ceo')),
            message TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (issue_id) REFERENCES issue_reports(id) ON DELETE CASCADE,
            FOREIGN KEY (sender_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor_user_id INTEGER NOT NULL,
            actor_username TEXT NOT NULL,
            action TEXT NOT NULL,
            target_type TEXT NOT NULL,
            target_id INTEGER,
            details TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (actor_user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS branch_shift_requirements (
            branch_id INTEGER NOT NULL,
            shift_code TEXT NOT NULL,
            min_staff INTEGER NOT NULL DEFAULT 3,
            max_staff INTEGER NOT NULL DEFAULT 4,
            PRIMARY KEY (branch_id, shift_code),
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_employee_branch_access_branch_employee
        ON employee_branch_access(branch_id, employee_id);

        CREATE UNIQUE INDEX IF NOT EXISTS ux_users_single_manager_per_branch
        ON users(branch_id)
        WHERE role = 'manager' AND branch_id IS NOT NULL;

        CREATE INDEX IF NOT EXISTS idx_shift_preferences_employee_week
        ON shift_preferences(employee_id, week_start);

        CREATE INDEX IF NOT EXISTS idx_shift_preferences_week_branch
        ON shift_preferences(week_start, branch_id);

        CREATE INDEX IF NOT EXISTS idx_weekly_schedule_week_branch
        ON weekly_schedule(week_start, branch_id);

        CREATE INDEX IF NOT EXISTS idx_weekly_schedule_employee_week
        ON weekly_schedule(employee_id, week_start);

        CREATE INDEX IF NOT EXISTS idx_weekly_schedule_group_week_branch
        ON weekly_schedule(week_start, branch_id, registration_type, group_code, day_of_week, shift_code);

        CREATE INDEX IF NOT EXISTS idx_auth_sessions_token_expiry
        ON auth_sessions(token, expires_at);

        CREATE INDEX IF NOT EXISTS idx_attendance_logs_employee_checkin
        ON attendance_logs(employee_id, check_in_at);

        CREATE INDEX IF NOT EXISTS idx_attendance_logs_employee_confirmed
        ON attendance_logs(employee_id, confirmed_at, check_out_at);

        CREATE UNIQUE INDEX IF NOT EXISTS ux_attendance_logs_employee_open_session
        ON attendance_logs(employee_id)
        WHERE check_out_at IS NULL;

        CREATE INDEX IF NOT EXISTS idx_attendance_logs_branch_checkin
        ON attendance_logs(branch_id, check_in_at);

        CREATE INDEX IF NOT EXISTS idx_attendance_employee_codes_employee_branch
        ON attendance_employee_codes(employee_id, branch_id, expires_at);

        CREATE INDEX IF NOT EXISTS idx_attendance_confirm_logs_attendance
        ON attendance_confirm_logs(attendance_log_id, confirmed_at);

        CREATE INDEX IF NOT EXISTS idx_attendance_confirm_logs_employee
        ON attendance_confirm_logs(employee_id, confirmed_at);

        CREATE INDEX IF NOT EXISTS idx_attendance_qr_one_time_codes_branch_code
        ON attendance_qr_one_time_codes(branch_id, one_time_code, expires_at);

        CREATE INDEX IF NOT EXISTS idx_attendance_qr_one_time_codes_validate
        ON attendance_qr_one_time_codes(branch_id, qr_token, one_time_code, consumed_at, expires_at, id);

        CREATE INDEX IF NOT EXISTS idx_attendance_qr_one_time_codes_expiry
        ON attendance_qr_one_time_codes(expires_at);

        CREATE INDEX IF NOT EXISTS idx_attendance_qr_one_time_codes_consumed
        ON attendance_qr_one_time_codes(consumed_at);

        CREATE INDEX IF NOT EXISTS idx_shift_attendance_marks_branch_week_day
        ON shift_attendance_marks(branch_id, week_start, day_of_week, shift_code, status);

        CREATE INDEX IF NOT EXISTS idx_shift_registration_groups_week_branch
        ON shift_registration_groups(week_start, branch_id, group_code);

        CREATE INDEX IF NOT EXISTS idx_shift_registration_groups_creator
        ON shift_registration_groups(created_by_employee_id, week_start);

        CREATE INDEX IF NOT EXISTS idx_shift_registration_group_members_employee
        ON shift_registration_group_members(employee_id, group_id);

        CREATE INDEX IF NOT EXISTS idx_shift_attendance_marks_employee_week
        ON shift_attendance_marks(employee_id, week_start, day_of_week, shift_code);

        CREATE INDEX IF NOT EXISTS idx_issue_reports_branch_status
        ON issue_reports(branch_id, status);

        CREATE INDEX IF NOT EXISTS idx_issue_reports_target_employee
        ON issue_reports(target_employee_id, created_at);

        CREATE INDEX IF NOT EXISTS idx_issue_reports_escalated
        ON issue_reports(escalated_to_ceo, created_at);

        CREATE INDEX IF NOT EXISTS idx_issue_report_replies_issue_created
        ON issue_report_replies(issue_id, created_at);

        CREATE INDEX IF NOT EXISTS idx_audit_logs_target
        ON audit_logs(target_type, target_id, created_at);

        CREATE INDEX IF NOT EXISTS idx_audit_logs_actor
        ON audit_logs(actor_user_id, created_at);

        CREATE INDEX IF NOT EXISTS idx_branch_shift_requirements_branch
        ON branch_shift_requirements(branch_id);

        CREATE INDEX IF NOT EXISTS idx_weekly_schedule_employee_branch_week_day
        ON weekly_schedule(employee_id, branch_id, week_start, day_of_week, shift_code);

        CREATE INDEX IF NOT EXISTS idx_shift_preferences_employee_week_branch_shift_day
        ON shift_preferences(employee_id, week_start, branch_id, shift_code, day_of_week);

        CREATE INDEX IF NOT EXISTS idx_shift_preferences_week_branch_group
        ON shift_preferences(week_start, branch_id, registration_type, group_code);
        """

    if IS_POSTGRES:
        _execute_postgres_script(cur, schema_sql)
        _run_postgres_migrations(conn)
    else:
        # Legacy SQLite databases may not have branch_id in old tables yet.
        # Skip branch-dependent indexes here and create them after migrations.
        sqlite_schema_sql = schema_sql.replace(
            """
        CREATE INDEX IF NOT EXISTS idx_shift_preferences_week_branch
        ON shift_preferences(week_start, branch_id);

        CREATE INDEX IF NOT EXISTS idx_weekly_schedule_week_branch
        ON weekly_schedule(week_start, branch_id);
""",
            "",
        )
        try:
            cur.executescript(sqlite_schema_sql)
        except sqlite3.DatabaseError:
            # Old SQLite files may not yet have newly added columns referenced by indexes.
            # Migrations below will add missing columns/tables first, then recreate indexes safely.
            pass
        _run_migrations(conn)

    seed_data(conn)
    if not IS_POSTGRES:
        conn.commit()
    conn.close()


def _execute_postgres_script(cur, schema_sql):
    transformed = schema_sql
    # Keep ID type as integer-compatible so existing INTEGER foreign keys remain valid.
    transformed = transformed.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
    transformed = re.sub(
        r"\bCURRENT_TIMESTAMP\b",
        POSTGRES_NOW_TEXT_EXPR,
        transformed,
    )

    statements = [segment.strip() for segment in transformed.split(";") if segment.strip()]
    for statement in statements:
        try:
            cur.execute(statement)
        except Exception as exc:
            # Old Postgres schemas may miss newly added columns referenced by new indexes.
            sqlstate = getattr(exc, "sqlstate", "")
            if statement.upper().startswith("CREATE INDEX") and sqlstate in {"42703", "42P01"}:
                continue
            raise


def _pg_table_has_column(conn, table_name, column_name):
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        (table_name, column_name),
    ).fetchone()
    return bool(row)


def _run_postgres_migrations(conn):
    # Keep Postgres migration minimal and targeted for backwards compatibility.
    if not _pg_table_has_column(conn, "issue_reports", "target_employee_id"):
        conn.execute("ALTER TABLE issue_reports ADD COLUMN target_employee_id INTEGER")

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_issue_reports_target_employee ON issue_reports(target_employee_id, created_at)"
    )


def _table_has_column(conn, table_name, column_name):
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(row["name"] == column_name for row in rows)


def _run_migrations(conn):
    cur = conn.cursor()

    if not _table_has_column(conn, "branches", "location"):
        cur.execute("ALTER TABLE branches ADD COLUMN location TEXT")
    if not _table_has_column(conn, "branches", "network_ip"):
        cur.execute("ALTER TABLE branches ADD COLUMN network_ip TEXT")

    if not _table_has_column(conn, "users", "password_hash"):
        cur.execute("ALTER TABLE users ADD COLUMN password_hash TEXT")
    if not _table_has_column(conn, "users", "is_active"):
        cur.execute("ALTER TABLE users ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
    if not _table_has_column(conn, "users", "avatar_data_url"):
        cur.execute("ALTER TABLE users ADD COLUMN avatar_data_url TEXT")
    if not _table_has_column(conn, "users", "full_name"):
        cur.execute("ALTER TABLE users ADD COLUMN full_name TEXT")
    if not _table_has_column(conn, "users", "date_of_birth"):
        cur.execute("ALTER TABLE users ADD COLUMN date_of_birth TEXT")
    if not _table_has_column(conn, "users", "phone_number"):
        cur.execute("ALTER TABLE users ADD COLUMN phone_number TEXT")
    if not _table_has_column(conn, "users", "address"):
        cur.execute("ALTER TABLE users ADD COLUMN address TEXT")
    if not _table_has_column(conn, "users", "job_position"):
        cur.execute("ALTER TABLE users ADD COLUMN job_position TEXT")

    if not _table_has_column(conn, "ceo_chat_messages", "sender_type"):
        cur.execute(
            "ALTER TABLE ceo_chat_messages ADD COLUMN sender_type TEXT NOT NULL DEFAULT 'user'"
        )
    if not _table_has_column(conn, "ceo_chat_messages", "sender_label"):
        cur.execute("ALTER TABLE ceo_chat_messages ADD COLUMN sender_label TEXT")

    if not _table_has_column(conn, "shift_preferences", "day_of_week"):
        cur.execute("ALTER TABLE shift_preferences ADD COLUMN day_of_week INTEGER NOT NULL DEFAULT 0")
    if not _table_has_column(conn, "shift_preferences", "branch_id"):
        cur.execute("ALTER TABLE shift_preferences ADD COLUMN branch_id INTEGER")
        cur.execute(
            """
            UPDATE shift_preferences
            SET branch_id = COALESCE(
                (
                    SELECT eba.branch_id
                    FROM employee_branch_access eba
                    WHERE eba.employee_id = shift_preferences.employee_id
                    ORDER BY eba.branch_id
                    LIMIT 1
                ),
                (
                    SELECT u.branch_id
                    FROM users u
                    WHERE u.id = shift_preferences.employee_id
                )
            )
            WHERE branch_id IS NULL
            """
        )
        cur.execute("DELETE FROM shift_preferences WHERE branch_id IS NULL")
    if not _table_has_column(conn, "shift_preferences", "registration_type"):
        cur.execute("ALTER TABLE shift_preferences ADD COLUMN registration_type TEXT NOT NULL DEFAULT 'individual'")
    if not _table_has_column(conn, "shift_preferences", "group_code"):
        cur.execute("ALTER TABLE shift_preferences ADD COLUMN group_code TEXT")
    if not _table_has_column(conn, "shift_preferences", "flexible_start_at"):
        cur.execute("ALTER TABLE shift_preferences ADD COLUMN flexible_start_at TEXT")
    if not _table_has_column(conn, "shift_preferences", "flexible_end_at"):
        cur.execute("ALTER TABLE shift_preferences ADD COLUMN flexible_end_at TEXT")
    if not _table_has_column(conn, "weekly_schedule", "day_of_week"):
        cur.execute("ALTER TABLE weekly_schedule ADD COLUMN day_of_week INTEGER NOT NULL DEFAULT 0")
    if not _table_has_column(conn, "weekly_schedule", "branch_id"):
        cur.execute("ALTER TABLE weekly_schedule ADD COLUMN branch_id INTEGER")
        cur.execute(
            """
            UPDATE weekly_schedule
            SET branch_id = COALESCE(
                (
                    SELECT u.branch_id
                    FROM users u
                    WHERE u.id = weekly_schedule.assigned_by
                ),
                (
                    SELECT u.branch_id
                    FROM users u
                    WHERE u.id = weekly_schedule.employee_id
                )
            )
            WHERE branch_id IS NULL
            """
        )
        cur.execute("DELETE FROM weekly_schedule WHERE branch_id IS NULL")
    if not _table_has_column(conn, "weekly_schedule", "registration_type"):
        cur.execute("ALTER TABLE weekly_schedule ADD COLUMN registration_type TEXT NOT NULL DEFAULT 'individual'")
    if not _table_has_column(conn, "weekly_schedule", "group_code"):
        cur.execute("ALTER TABLE weekly_schedule ADD COLUMN group_code TEXT")
    if not _table_has_column(conn, "weekly_schedule", "flexible_start_at"):
        cur.execute("ALTER TABLE weekly_schedule ADD COLUMN flexible_start_at TEXT")
    if not _table_has_column(conn, "weekly_schedule", "flexible_end_at"):
        cur.execute("ALTER TABLE weekly_schedule ADD COLUMN flexible_end_at TEXT")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS auth_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            token TEXT NOT NULL UNIQUE,
            expires_at TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS attendance_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id INTEGER NOT NULL,
            branch_id INTEGER,
            check_in_at TEXT NOT NULL,
            confirmed_at TEXT,
            check_out_at TEXT,
            minutes_worked INTEGER,
            scheduled_shift_start_at TEXT,
            minutes_late INTEGER DEFAULT 0,
            checked_in_by_manager_id INTEGER,
            manager_check_in_note TEXT,
            note TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (employee_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE SET NULL,
            FOREIGN KEY (checked_in_by_manager_id) REFERENCES users(id) ON DELETE SET NULL
        )
        """
    )
    if not _table_has_column(conn, "attendance_logs", "confirmed_at"):
        cur.execute("ALTER TABLE attendance_logs ADD COLUMN confirmed_at TEXT")
    if not _table_has_column(conn, "attendance_logs", "scheduled_shift_start_at"):
        cur.execute("ALTER TABLE attendance_logs ADD COLUMN scheduled_shift_start_at TEXT")
    if not _table_has_column(conn, "attendance_logs", "minutes_late"):
        cur.execute("ALTER TABLE attendance_logs ADD COLUMN minutes_late INTEGER DEFAULT 0")
    if not _table_has_column(conn, "attendance_logs", "checked_in_by_manager_id"):
        cur.execute("ALTER TABLE attendance_logs ADD COLUMN checked_in_by_manager_id INTEGER")
    if not _table_has_column(conn, "attendance_logs", "manager_check_in_note"):
        cur.execute("ALTER TABLE attendance_logs ADD COLUMN manager_check_in_note TEXT")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS attendance_employee_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id INTEGER NOT NULL,
            branch_id INTEGER NOT NULL,
            code TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            consumed_at TEXT,
            request_ip TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (employee_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS attendance_qr_one_time_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id INTEGER NOT NULL,
            qr_token TEXT NOT NULL,
            one_time_code TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            consumed_at TEXT,
            generated_by_manager_id INTEGER NOT NULL,
            consumed_by_employee_id INTEGER,
            request_ip TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE,
            FOREIGN KEY (generated_by_manager_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (consumed_by_employee_id) REFERENCES users(id) ON DELETE SET NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS attendance_confirm_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            attendance_log_id INTEGER NOT NULL,
            employee_id INTEGER NOT NULL,
            branch_id INTEGER,
            confirmed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            source TEXT NOT NULL DEFAULT 'employee_confirm',
            note TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (attendance_log_id) REFERENCES attendance_logs(id) ON DELETE CASCADE,
            FOREIGN KEY (employee_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE SET NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS shift_registration_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_code TEXT NOT NULL,
            group_name TEXT NOT NULL,
            week_start TEXT NOT NULL,
            branch_id INTEGER NOT NULL,
            max_members INTEGER,
            created_by_employee_id INTEGER NOT NULL,
            note TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE,
            FOREIGN KEY (created_by_employee_id) REFERENCES users(id) ON DELETE CASCADE,
            UNIQUE (week_start, branch_id, group_code)
        )
        """
    )
    if not _table_has_column(conn, "shift_registration_groups", "max_members"):
        cur.execute("ALTER TABLE shift_registration_groups ADD COLUMN max_members INTEGER")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS shift_registration_group_members (
            group_id INTEGER NOT NULL,
            employee_id INTEGER NOT NULL,
            joined_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (group_id, employee_id),
            FOREIGN KEY (group_id) REFERENCES shift_registration_groups(id) ON DELETE CASCADE,
            FOREIGN KEY (employee_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS shift_attendance_marks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            week_start TEXT NOT NULL,
            day_of_week INTEGER NOT NULL,
            shift_code TEXT NOT NULL,
            branch_id INTEGER NOT NULL,
            employee_id INTEGER NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('present', 'absent', 'present_override')),
            source TEXT NOT NULL DEFAULT 'system',
            attendance_log_id INTEGER,
            note TEXT,
            marked_by_manager_id INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE,
            FOREIGN KEY (employee_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (attendance_log_id) REFERENCES attendance_logs(id) ON DELETE SET NULL,
            FOREIGN KEY (marked_by_manager_id) REFERENCES users(id) ON DELETE SET NULL,
            UNIQUE (week_start, day_of_week, shift_code, branch_id, employee_id)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS issue_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reporter_id INTEGER NOT NULL,
            reporter_role TEXT NOT NULL,
            branch_id INTEGER,
            target_employee_id INTEGER,
            title TEXT NOT NULL,
            details TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'in_review', 'escalated', 'resolved')),
            escalated_to_ceo INTEGER NOT NULL DEFAULT 0,
            manager_note TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (reporter_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE SET NULL,
            FOREIGN KEY (target_employee_id) REFERENCES users(id) ON DELETE SET NULL
        )
        """
    )

    if not _table_has_column(conn, "issue_reports", "target_employee_id"):
        cur.execute("ALTER TABLE issue_reports ADD COLUMN target_employee_id INTEGER")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS issue_report_replies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            issue_id INTEGER NOT NULL,
            sender_id INTEGER NOT NULL,
            sender_role TEXT NOT NULL CHECK (sender_role IN ('manager', 'ceo')),
            message TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (issue_id) REFERENCES issue_reports(id) ON DELETE CASCADE,
            FOREIGN KEY (sender_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor_user_id INTEGER NOT NULL,
            actor_username TEXT NOT NULL,
            action TEXT NOT NULL,
            target_type TEXT NOT NULL,
            target_id INTEGER,
            details TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (actor_user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS branch_shift_requirements (
            branch_id INTEGER NOT NULL,
            shift_code TEXT NOT NULL,
            min_staff INTEGER NOT NULL DEFAULT 3,
            max_staff INTEGER NOT NULL DEFAULT 4,
            PRIMARY KEY (branch_id, shift_code),
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE
        )
        """
    )

    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_users_single_manager_per_branch ON users(branch_id) WHERE role = 'manager' AND branch_id IS NOT NULL"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_attendance_logs_employee_checkin ON attendance_logs(employee_id, check_in_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_attendance_logs_employee_confirmed ON attendance_logs(employee_id, confirmed_at, check_out_at)"
    )
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_attendance_logs_employee_open_session ON attendance_logs(employee_id) WHERE check_out_at IS NULL"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_attendance_logs_branch_checkin ON attendance_logs(branch_id, check_in_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_attendance_employee_codes_employee_branch ON attendance_employee_codes(employee_id, branch_id, expires_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_attendance_confirm_logs_attendance ON attendance_confirm_logs(attendance_log_id, confirmed_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_attendance_confirm_logs_employee ON attendance_confirm_logs(employee_id, confirmed_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_attendance_qr_one_time_codes_branch_code ON attendance_qr_one_time_codes(branch_id, one_time_code, expires_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_attendance_qr_one_time_codes_validate ON attendance_qr_one_time_codes(branch_id, qr_token, one_time_code, consumed_at, expires_at, id)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_attendance_qr_one_time_codes_expiry ON attendance_qr_one_time_codes(expires_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_attendance_qr_one_time_codes_consumed ON attendance_qr_one_time_codes(consumed_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_shift_attendance_marks_branch_week_day ON shift_attendance_marks(branch_id, week_start, day_of_week, shift_code, status)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_shift_registration_groups_week_branch ON shift_registration_groups(week_start, branch_id, group_code)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_shift_registration_groups_creator ON shift_registration_groups(created_by_employee_id, week_start)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_shift_registration_group_members_employee ON shift_registration_group_members(employee_id, group_id)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_shift_attendance_marks_employee_week ON shift_attendance_marks(employee_id, week_start, day_of_week, shift_code)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_issue_reports_branch_status ON issue_reports(branch_id, status)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_issue_reports_target_employee ON issue_reports(target_employee_id, created_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_issue_reports_escalated ON issue_reports(escalated_to_ceo, created_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_issue_report_replies_issue_created ON issue_report_replies(issue_id, created_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_shift_preferences_week_day_shift ON shift_preferences(week_start, day_of_week, shift_code)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_shift_preferences_week_branch ON shift_preferences(week_start, branch_id)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_weekly_schedule_week_day_shift ON weekly_schedule(week_start, day_of_week, shift_code)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_weekly_schedule_week_branch ON weekly_schedule(week_start, branch_id)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_audit_logs_target ON audit_logs(target_type, target_id, created_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_audit_logs_actor ON audit_logs(actor_user_id, created_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_branch_shift_requirements_branch ON branch_shift_requirements(branch_id)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_weekly_schedule_employee_branch_week_day ON weekly_schedule(employee_id, branch_id, week_start, day_of_week, shift_code)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_shift_preferences_employee_week_branch_shift_day ON shift_preferences(employee_id, week_start, branch_id, shift_code, day_of_week)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_shift_preferences_week_branch_group ON shift_preferences(week_start, branch_id, registration_type, group_code)"
    )


def seed_data(conn):
    cur = conn.cursor()

    # Keep bootstrap data minimal: only a CEO account is seeded.
    # Branches and operational users are intentionally empty so CEO configures everything.
    cur.execute("UPDATE users SET is_active = 1 WHERE is_active IS NULL")

    ceo_user = cur.execute(
        "SELECT id, role, password_hash FROM users WHERE username = ?",
        ("ceo",),
    ).fetchone()

    if ceo_user:
        if ceo_user["role"] != "ceo":
            cur.execute(
                "UPDATE users SET role = 'ceo', branch_id = NULL WHERE id = ?",
                (ceo_user["id"],),
            )
        if not ceo_user["password_hash"]:
            cur.execute(
                "UPDATE users SET password_hash = ?, is_active = 1 WHERE id = ?",
                (generate_password_hash("123456"), ceo_user["id"]),
            )
    else:
        has_any_ceo = cur.execute(
            "SELECT 1 FROM users WHERE role = 'ceo' LIMIT 1"
        ).fetchone()
        if not has_any_ceo:
            cur.execute(
                """
                INSERT INTO users(username, display_name, role, branch_id, password_hash, is_active)
                VALUES (?, ?, 'ceo', NULL, ?, 1)
                """,
                ("ceo", "CEO Tong", generate_password_hash("123456")),
            )
