import sqlite3
import os
from pathlib import Path

from werkzeug.security import generate_password_hash
from .constants import SHIFT_DEFINITIONS


def _resolve_db_path():
    configured = (os.getenv("SQLITE_PATH") or "").strip()
    if configured:
        return Path(configured)

    if os.getenv("VERCEL") == "1":
        # Vercel filesystem is read-only except /tmp.
        return Path("/tmp") / "data.db"

    return Path(__file__).resolve().parent.parent / "data.db"


DB_PATH = _resolve_db_path()


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.executescript(
        """
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
            check_out_at TEXT,
            minutes_worked INTEGER,
            note TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (employee_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE SET NULL
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
            title TEXT NOT NULL,
            details TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'in_review', 'escalated', 'resolved')),
            escalated_to_ceo INTEGER NOT NULL DEFAULT 0,
            manager_note TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (reporter_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE SET NULL
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

        CREATE INDEX IF NOT EXISTS idx_shift_preferences_employee_week
        ON shift_preferences(employee_id, week_start);

        CREATE INDEX IF NOT EXISTS idx_shift_preferences_week_branch
        ON shift_preferences(week_start, branch_id);

        CREATE INDEX IF NOT EXISTS idx_weekly_schedule_week_branch
        ON weekly_schedule(week_start, branch_id);

        CREATE INDEX IF NOT EXISTS idx_weekly_schedule_employee_week
        ON weekly_schedule(employee_id, week_start);

        CREATE INDEX IF NOT EXISTS idx_auth_sessions_token_expiry
        ON auth_sessions(token, expires_at);

        CREATE INDEX IF NOT EXISTS idx_attendance_logs_employee_checkin
        ON attendance_logs(employee_id, check_in_at);

        CREATE INDEX IF NOT EXISTS idx_attendance_employee_codes_employee_branch
        ON attendance_employee_codes(employee_id, branch_id, expires_at);

        CREATE INDEX IF NOT EXISTS idx_attendance_qr_one_time_codes_branch_code
        ON attendance_qr_one_time_codes(branch_id, one_time_code, expires_at);

        CREATE INDEX IF NOT EXISTS idx_shift_attendance_marks_branch_week_day
        ON shift_attendance_marks(branch_id, week_start, day_of_week, shift_code, status);

        CREATE INDEX IF NOT EXISTS idx_issue_reports_branch_status
        ON issue_reports(branch_id, status);

        CREATE INDEX IF NOT EXISTS idx_issue_reports_escalated
        ON issue_reports(escalated_to_ceo, created_at);

        CREATE INDEX IF NOT EXISTS idx_audit_logs_target
        ON audit_logs(target_type, target_id, created_at);

        CREATE INDEX IF NOT EXISTS idx_audit_logs_actor
        ON audit_logs(actor_user_id, created_at);

        CREATE INDEX IF NOT EXISTS idx_branch_shift_requirements_branch
        ON branch_shift_requirements(branch_id);
        """
    )

    _run_migrations(conn)

    seed_data(conn)
    conn.commit()
    conn.close()


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
    if not _table_has_column(conn, "weekly_schedule", "day_of_week"):
        cur.execute("ALTER TABLE weekly_schedule ADD COLUMN day_of_week INTEGER NOT NULL DEFAULT 0")

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
            check_out_at TEXT,
            minutes_worked INTEGER,
            note TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (employee_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE SET NULL
        )
        """
    )

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
            title TEXT NOT NULL,
            details TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'in_review', 'escalated', 'resolved')),
            escalated_to_ceo INTEGER NOT NULL DEFAULT 0,
            manager_note TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (reporter_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE SET NULL
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
        "CREATE INDEX IF NOT EXISTS idx_attendance_logs_employee_checkin ON attendance_logs(employee_id, check_in_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_attendance_employee_codes_employee_branch ON attendance_employee_codes(employee_id, branch_id, expires_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_attendance_qr_one_time_codes_branch_code ON attendance_qr_one_time_codes(branch_id, one_time_code, expires_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_shift_attendance_marks_branch_week_day ON shift_attendance_marks(branch_id, week_start, day_of_week, shift_code, status)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_issue_reports_branch_status ON issue_reports(branch_id, status)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_issue_reports_escalated ON issue_reports(escalated_to_ceo, created_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_shift_preferences_week_day_shift ON shift_preferences(week_start, day_of_week, shift_code)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_weekly_schedule_week_day_shift ON weekly_schedule(week_start, day_of_week, shift_code)"
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
