-- Workforce Manager schema for Supabase (Postgres)
-- Run this file once in Supabase SQL Editor.

BEGIN;

CREATE TABLE IF NOT EXISTS branches (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    location TEXT,
    network_ip TEXT
);

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
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
    id SERIAL PRIMARY KEY,
    employee_id INTEGER NOT NULL,
    week_start TEXT NOT NULL,
    branch_id INTEGER NOT NULL,
    shift_code TEXT NOT NULL,
    day_of_week INTEGER NOT NULL DEFAULT 0,
    registration_type TEXT NOT NULL DEFAULT 'individual' CHECK (registration_type IN ('individual', 'group')),
    group_code TEXT,
    flexible_start_at TEXT,
    flexible_end_at TEXT,
    created_at TEXT NOT NULL DEFAULT (to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
    FOREIGN KEY (employee_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS weekly_schedule (
    id SERIAL PRIMARY KEY,
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
    created_at TEXT NOT NULL DEFAULT (to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
    FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE,
    FOREIGN KEY (employee_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (assigned_by) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS ceo_chat_messages (
    id SERIAL PRIMARY KEY,
    sender_id INTEGER NOT NULL,
    sender_type TEXT NOT NULL DEFAULT 'user',
    sender_label TEXT,
    message TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
    FOREIGN KEY (sender_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS auth_sessions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    token TEXT NOT NULL UNIQUE,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS attendance_logs (
    id SERIAL PRIMARY KEY,
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
    created_at TEXT NOT NULL DEFAULT (to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
    FOREIGN KEY (employee_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE SET NULL,
    FOREIGN KEY (checked_in_by_manager_id) REFERENCES users(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS attendance_confirm_logs (
    id SERIAL PRIMARY KEY,
    attendance_log_id INTEGER NOT NULL,
    employee_id INTEGER NOT NULL,
    branch_id INTEGER,
    confirmed_at TEXT NOT NULL DEFAULT (to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
    source TEXT NOT NULL DEFAULT 'employee_confirm',
    note TEXT,
    created_at TEXT NOT NULL DEFAULT (to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
    FOREIGN KEY (attendance_log_id) REFERENCES attendance_logs(id) ON DELETE CASCADE,
    FOREIGN KEY (employee_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS shift_registration_groups (
    id SERIAL PRIMARY KEY,
    group_code TEXT NOT NULL,
    group_name TEXT NOT NULL,
    week_start TEXT NOT NULL,
    branch_id INTEGER NOT NULL,
    max_members INTEGER,
    created_by_employee_id INTEGER NOT NULL,
    note TEXT,
    created_at TEXT NOT NULL DEFAULT (to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
    FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE,
    FOREIGN KEY (created_by_employee_id) REFERENCES users(id) ON DELETE CASCADE,
    UNIQUE (week_start, branch_id, group_code)
);

CREATE TABLE IF NOT EXISTS shift_registration_group_members (
    group_id INTEGER NOT NULL,
    employee_id INTEGER NOT NULL,
    joined_at TEXT NOT NULL DEFAULT (to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
    PRIMARY KEY (group_id, employee_id),
    FOREIGN KEY (group_id) REFERENCES shift_registration_groups(id) ON DELETE CASCADE,
    FOREIGN KEY (employee_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS attendance_employee_codes (
    id SERIAL PRIMARY KEY,
    employee_id INTEGER NOT NULL,
    branch_id INTEGER NOT NULL,
    code TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    consumed_at TEXT,
    request_ip TEXT,
    created_at TEXT NOT NULL DEFAULT (to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
    FOREIGN KEY (employee_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS attendance_qr_one_time_codes (
    id SERIAL PRIMARY KEY,
    branch_id INTEGER NOT NULL,
    qr_token TEXT NOT NULL,
    one_time_code TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    consumed_at TEXT,
    generated_by_manager_id INTEGER NOT NULL,
    consumed_by_employee_id INTEGER,
    request_ip TEXT,
    created_at TEXT NOT NULL DEFAULT (to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
    FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE,
    FOREIGN KEY (generated_by_manager_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (consumed_by_employee_id) REFERENCES users(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS shift_attendance_marks (
    id SERIAL PRIMARY KEY,
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
    created_at TEXT NOT NULL DEFAULT (to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
    updated_at TEXT NOT NULL DEFAULT (to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
    FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE,
    FOREIGN KEY (employee_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (attendance_log_id) REFERENCES attendance_logs(id) ON DELETE SET NULL,
    FOREIGN KEY (marked_by_manager_id) REFERENCES users(id) ON DELETE SET NULL,
    UNIQUE (week_start, day_of_week, shift_code, branch_id, employee_id)
);

CREATE TABLE IF NOT EXISTS issue_reports (
    id SERIAL PRIMARY KEY,
    reporter_id INTEGER NOT NULL,
    reporter_role TEXT NOT NULL,
    branch_id INTEGER,
    target_employee_id INTEGER,
    title TEXT NOT NULL,
    details TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'in_review', 'escalated', 'resolved')),
    escalated_to_ceo INTEGER NOT NULL DEFAULT 0,
    manager_note TEXT,
    created_at TEXT NOT NULL DEFAULT (to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
    updated_at TEXT NOT NULL DEFAULT (to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
    FOREIGN KEY (reporter_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (target_employee_id) REFERENCES users(id) ON DELETE SET NULL,
    FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE SET NULL
);

-- Backward-compatible migration for existing Supabase projects.
ALTER TABLE issue_reports
ADD COLUMN IF NOT EXISTS target_employee_id INTEGER;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        WHERE t.relname = 'issue_reports'
          AND c.contype = 'f'
          AND pg_get_constraintdef(c.oid) ILIKE '%(target_employee_id)%REFERENCES users(id)%'
    ) THEN
        ALTER TABLE issue_reports
        ADD CONSTRAINT fk_issue_reports_target_employee
        FOREIGN KEY (target_employee_id) REFERENCES users(id) ON DELETE SET NULL;
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS issue_report_replies (
    id SERIAL PRIMARY KEY,
    issue_id INTEGER NOT NULL,
    sender_id INTEGER NOT NULL,
    sender_role TEXT NOT NULL CHECK (sender_role IN ('manager', 'ceo')),
    message TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
    FOREIGN KEY (issue_id) REFERENCES issue_reports(id) ON DELETE CASCADE,
    FOREIGN KEY (sender_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS audit_logs (
    id SERIAL PRIMARY KEY,
    actor_user_id INTEGER NOT NULL,
    actor_username TEXT NOT NULL,
    action TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id INTEGER,
    details TEXT,
    created_at TEXT NOT NULL DEFAULT (to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')),
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

-- Backward-compatible migration block for existing Supabase databases.
-- Keep this block updated whenever schema columns change in code.
ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar_data_url TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS full_name TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS date_of_birth TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS phone_number TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS address TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS job_position TEXT;

ALTER TABLE ceo_chat_messages ADD COLUMN IF NOT EXISTS sender_type TEXT DEFAULT 'user';
ALTER TABLE ceo_chat_messages ADD COLUMN IF NOT EXISTS sender_label TEXT;

ALTER TABLE shift_preferences ADD COLUMN IF NOT EXISTS day_of_week INTEGER DEFAULT 0;
ALTER TABLE shift_preferences ADD COLUMN IF NOT EXISTS registration_type TEXT DEFAULT 'individual';
ALTER TABLE shift_preferences ADD COLUMN IF NOT EXISTS group_code TEXT;
ALTER TABLE shift_preferences ADD COLUMN IF NOT EXISTS flexible_start_at TEXT;
ALTER TABLE shift_preferences ADD COLUMN IF NOT EXISTS flexible_end_at TEXT;

ALTER TABLE weekly_schedule ADD COLUMN IF NOT EXISTS day_of_week INTEGER DEFAULT 0;
ALTER TABLE weekly_schedule ADD COLUMN IF NOT EXISTS registration_type TEXT DEFAULT 'individual';
ALTER TABLE weekly_schedule ADD COLUMN IF NOT EXISTS group_code TEXT;
ALTER TABLE weekly_schedule ADD COLUMN IF NOT EXISTS flexible_start_at TEXT;
ALTER TABLE weekly_schedule ADD COLUMN IF NOT EXISTS flexible_end_at TEXT;

ALTER TABLE attendance_logs ADD COLUMN IF NOT EXISTS confirmed_at TEXT;
ALTER TABLE attendance_logs ADD COLUMN IF NOT EXISTS scheduled_shift_start_at TEXT;
ALTER TABLE attendance_logs ADD COLUMN IF NOT EXISTS minutes_late INTEGER DEFAULT 0;
ALTER TABLE attendance_logs ADD COLUMN IF NOT EXISTS checked_in_by_manager_id INTEGER;
ALTER TABLE attendance_logs ADD COLUMN IF NOT EXISTS manager_check_in_note TEXT;

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

COMMIT;
