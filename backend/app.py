import secrets
import csv
import io
import base64
import json
import os
import re
import hmac
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock

import qrcode
from flask import Flask, Response, jsonify, request, send_from_directory
from werkzeug.exceptions import RequestEntityTooLarge
from werkzeug.security import check_password_hash, generate_password_hash

from .constants import SHIFT_CODE_SET, SHIFT_DEFINITIONS
from .db import get_conn, init_db, is_postgres_backend
from .routes.attendance_routes import register_attendance_routes
from .routes.general_routes import register_general_routes
from .routes.leadership_routes import register_leadership_routes
from .routes.operations_routes import register_operations_routes
from .services.openjarvis_service import (
    generate_jarvis_response,
)


FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"
TOKEN_LIFETIME_DAYS = 7
DEFAULT_STATELESS_SESSION_SECRET = "workforce-session-secret"
DEFAULT_ATTENDANCE_QR_SECRET = "workforce-attendance-qr-secret"

ROLE_PERMISSIONS = {
    "employee": [
        "employee.preferences:read",
        "employee.preferences:write",
        "employee.issues:write",
        "employee.issues:read",
        "attendance:write",
        "attendance:read",
    ],
    "manager": [
        "manager.preferences:read",
        "manager.schedule:read",
        "manager.schedule:write",
        "manager.employees:read",
        "manager.employees:write",
        "manager.issues:read",
        "manager.issues:write",
        "manager.payroll:export",
        "attendance:write",
        "attendance:read",
    ],
    "ceo": [
        "ceo.chat:read",
        "ceo.chat:write",
        "admin.users:read",
        "admin.users:write",
        "ceo.issues:read",
        "ceo.payroll:export",
    ],
}


def create_app():
    app = Flask(__name__, static_folder=str(FRONTEND_DIR), static_url_path="")
    app.config["MAX_CONTENT_LENGTH"] = 2 * 1024 * 1024
    init_db()

    PROFILE_REQUIRED_ROLES = {"employee", "manager"}
    ATTENDANCE_QR_ONE_TIME_TTL_SECONDS = 45
    ATTENDANCE_QR_SECRET = os.getenv("ATTENDANCE_QR_SECRET", DEFAULT_ATTENDANCE_QR_SECRET)
    ATTENDANCE_QR_ENABLED = True
    STATELESS_SESSION_SECRET = os.getenv("SESSION_TOKEN_SECRET", DEFAULT_STATELESS_SESSION_SECRET)
    IS_VERCEL = os.getenv("VERCEL") == "1"
    DB_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    SHIFT_DEFINITION_MAP = {item["code"]: item for item in SHIFT_DEFINITIONS}
    LOGIN_WINDOW_SECONDS = 10 * 60
    LOGIN_MAX_FAILURES = 5
    login_attempts_lock = Lock()
    login_attempts = {}

    requires_stateless_for_ephemeral_db = IS_VERCEL and not is_postgres_backend()
    stateless_session_enabled = os.getenv("STATELESS_SESSION") == "1" or requires_stateless_for_ephemeral_db

    def _prune_login_attempts(now_ts):
        stale_keys = [key for key, values in login_attempts.items() if values and values[-1] < now_ts - LOGIN_WINDOW_SECONDS]
        for key in stale_keys:
            del login_attempts[key]

    def _is_login_rate_limited(client_ip, username):
        now_ts = int(datetime.utcnow().timestamp())
        key = f"{client_ip}:{(username or '').lower()}"
        with login_attempts_lock:
            _prune_login_attempts(now_ts)
            attempts = [ts for ts in login_attempts.get(key, []) if ts >= now_ts - LOGIN_WINDOW_SECONDS]
            login_attempts[key] = attempts
            return len(attempts) >= LOGIN_MAX_FAILURES

    def _record_login_failure(client_ip, username):
        now_ts = int(datetime.utcnow().timestamp())
        key = f"{client_ip}:{(username or '').lower()}"
        with login_attempts_lock:
            attempts = [ts for ts in login_attempts.get(key, []) if ts >= now_ts - LOGIN_WINDOW_SECONDS]
            attempts.append(now_ts)
            login_attempts[key] = attempts

    def _clear_login_failures(client_ip, username):
        key = f"{client_ip}:{(username or '').lower()}"
        with login_attempts_lock:
            if key in login_attempts:
                del login_attempts[key]

    @app.before_request
    def _reject_invalid_json_requests():
        if not request.path.startswith("/api/"):
            return None

        if request.method in {"POST", "PUT", "PATCH"}:
            content_type = (request.headers.get("Content-Type") or "").lower()
            if content_type and "application/json" not in content_type:
                return jsonify({"error": "Content-Type must be application/json"}), 415
        return None

    @app.after_request
    def _set_security_headers(response):
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        response.headers["Cache-Control"] = "no-store"
        response.headers["Pragma"] = "no-cache"
        if os.getenv("VERCEL") == "1":
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        csp = (
            "default-src 'self'; "
            "script-src 'self'; "
            "style-src 'self'; "
            "img-src 'self' data:; "
            "connect-src 'self'; "
            "frame-ancestors 'none'; "
            "base-uri 'self'; "
            "form-action 'self'"
        )
        response.headers["Content-Security-Policy"] = csp
        return response

    @app.errorhandler(RequestEntityTooLarge)
    def _handle_request_too_large(_error):
        return jsonify({"error": "Payload too large"}), 413

    def _is_stateless_session_enabled():
        return stateless_session_enabled

    def _is_weak_secret(secret_value, default_value):
        text = (secret_value or "").strip()
        return len(text) < 32 or text == default_value

    def _hash_session_token(raw_token):
        return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()

    if stateless_session_enabled and _is_weak_secret(STATELESS_SESSION_SECRET, DEFAULT_STATELESS_SESSION_SECRET):
        if requires_stateless_for_ephemeral_db:
            app.logger.warning(
                "SESSION_TOKEN_SECRET is weak or missing on Vercel SQLite. "
                "Continuing with stateless sessions using the fallback secret to avoid random logouts. "
                "Set SESSION_TOKEN_SECRET (>=32 chars) to secure production sessions."
            )
        else:
            stateless_session_enabled = False
            app.logger.warning(
                "SESSION_TOKEN_SECRET is weak or missing; falling back to DB-backed sessions. "
                "Set SESSION_TOKEN_SECRET (>=32 chars) and optionally STATELESS_SESSION=1 to re-enable stateless sessions."
            )
    if IS_VERCEL and _is_weak_secret(ATTENDANCE_QR_SECRET, DEFAULT_ATTENDANCE_QR_SECRET):
        ATTENDANCE_QR_ENABLED = False
        app.logger.warning(
            "ATTENDANCE_QR_SECRET is weak or missing on Vercel. "
            "Attendance QR endpoints are disabled. "
            "Set ATTENDANCE_QR_SECRET (>=32 chars) to re-enable QR check-in."
        )

    def _build_stateless_session_token(user_id):
        expires_ts = int((datetime.utcnow() + timedelta(days=TOKEN_LIFETIME_DAYS)).timestamp())
        payload_json = json.dumps({"uid": int(user_id), "exp": expires_ts}, separators=(",", ":"))
        payload_b64 = base64.urlsafe_b64encode(payload_json.encode("utf-8")).decode("ascii").rstrip("=")
        signature = hmac.new(
            STATELESS_SESSION_SECRET.encode("utf-8"),
            payload_b64.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return f"st1.{payload_b64}.{signature}", expires_ts

    def _parse_stateless_session_token(token):
        parts = (token or "").split(".")
        if len(parts) != 3 or parts[0] != "st1":
            return None, "Invalid token format"

        payload_b64 = parts[1]
        signature = parts[2]
        expected_signature = hmac.new(
            STATELESS_SESSION_SECRET.encode("utf-8"),
            payload_b64.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(signature, expected_signature):
            return None, "Invalid token signature"

        padded = payload_b64 + "=" * (-len(payload_b64) % 4)
        try:
            payload_raw = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
            payload = json.loads(payload_raw)
            user_id = int(payload.get("uid"))
            expires_ts = int(payload.get("exp"))
        except (ValueError, TypeError, json.JSONDecodeError):
            return None, "Invalid token payload"

        if expires_ts < int(datetime.utcnow().timestamp()):
            return None, "Token expired"

        return {"user_id": user_id, "expires_ts": expires_ts}, None

    def _build_profile_payload(user_row):
        row = dict(user_row)
        return {
            "avatar_data_url": row.get("avatar_data_url") or None,
            "full_name": (row.get("full_name") or "").strip(),
            "date_of_birth": (row.get("date_of_birth") or "").strip(),
            "phone_number": (row.get("phone_number") or "").strip(),
            "address": (row.get("address") or "").strip(),
        }

    def _is_profile_completed(user_row):
        payload = _build_profile_payload(user_row)
        required = [
            payload["avatar_data_url"],
            payload["full_name"],
            payload["date_of_birth"],
            payload["phone_number"],
            payload["address"],
        ]
        return all(bool(item) for item in required)

    def _permission_payload(user_dict):
        payload = dict(user_dict)
        payload["permissions"] = ROLE_PERMISSIONS.get(payload["role"], [])
        payload["profile"] = _build_profile_payload(payload)
        payload["profile_completed"] = _is_profile_completed(payload)
        payload["needs_profile_completion"] = (
            payload["role"] in PROFILE_REQUIRED_ROLES and not payload["profile_completed"]
        )
        return payload

    def _get_access_token():
        auth_header = request.headers.get("Authorization") or ""
        if auth_header.lower().startswith("bearer "):
            return auth_header[7:].strip()
        return request.headers.get("X-Auth-Token")

    def _get_user_from_token(required=True, roles=None):
        token = _get_access_token()
        if not token:
            if required:
                return None, (jsonify({"error": "Missing access token"}), 401)
            return None, None

        token_hash = _hash_session_token(token)

        conn = get_conn()
        user = conn.execute(
            """
                        SELECT u.id,
                                     u.username,
                                     u.display_name,
                                     u.role,
                                     u.branch_id,
                                     u.is_active,
                                     u.avatar_data_url,
                                     u.full_name,
                                     u.date_of_birth,
                                     u.phone_number,
                                       u.address,
                                     u.job_position,
                                     s.token
            FROM auth_sessions s
            JOIN users u ON u.id = s.user_id
            WHERE s.token = ?
              AND s.expires_at > CURRENT_TIMESTAMP
            """,
                        (token_hash,),
        ).fetchone()

        if not user:
            # Vercel serverless may serve requests from different instances where
            # SQLite session rows are not shared. Fallback to stateless token.
            if _is_stateless_session_enabled() and token.startswith("st1."):
                parsed, parse_error = _parse_stateless_session_token(token)
                if parsed:
                    user = conn.execute(
                        """
                        SELECT id,
                               username,
                               display_name,
                               role,
                               branch_id,
                               is_active,
                               avatar_data_url,
                               full_name,
                               date_of_birth,
                               phone_number,
                               address,
                               job_position
                        FROM users
                        WHERE id = ?
                        """,
                        (parsed["user_id"],),
                    ).fetchone()
                else:
                    conn.close()
                    return None, (jsonify({"error": parse_error or "Invalid or expired session"}), 401)

            if not user:
                conn.close()
                return None, (jsonify({"error": "Invalid or expired session"}), 401)

        user_dict = dict(user)
        if not user_dict.get("is_active", 1):
            conn.execute("DELETE FROM auth_sessions WHERE token = ?", (token_hash,))
            conn.commit()
            conn.close()
            return None, (jsonify({"error": "User is inactive"}), 403)

        if roles and user_dict["role"] not in roles:
            conn.close()
            return None, (jsonify({"error": "Forbidden"}), 403)

        conn.close()
        return _permission_payload(user_dict), None

    def _manager_can_manage_employee(conn, manager_branch_id, employee_id):
        if not manager_branch_id:
            return False
        row = conn.execute(
            """
            SELECT 1
            FROM users u
            JOIN employee_branch_access eba ON eba.employee_id = u.id
            WHERE u.id = ?
              AND u.role = 'employee'
              AND eba.branch_id = ?
            LIMIT 1
            """,
            (employee_id, manager_branch_id),
        ).fetchone()
        return bool(row)

    def _week_range(week_start):
        start = datetime.strptime(week_start, "%Y-%m-%d")
        end = start + timedelta(days=7)
        return start.strftime("%Y-%m-%d 00:00:00"), end.strftime("%Y-%m-%d 00:00:00")

    def _format_db_datetime(dt_value):
        return dt_value.strftime(DB_DATETIME_FORMAT)

    def _parse_db_datetime(raw_value):
        return datetime.strptime(raw_value, DB_DATETIME_FORMAT)

    def _week_start_and_day_for_datetime(current_dt):
        monday_dt = current_dt - timedelta(days=current_dt.weekday())
        return monday_dt.strftime("%Y-%m-%d"), current_dt.weekday() + 1

    def _is_valid_ipv4(ip_value):
        text = (ip_value or "").strip()
        if not text:
            return True
        parts = text.split(".")
        if len(parts) != 4:
            return False
        for part in parts:
            if not part.isdigit():
                return False
            num = int(part)
            if num < 0 or num > 255:
                return False
        return True

    def _shift_start_datetime(week_start, day_of_week, shift_code):
        shift = SHIFT_DEFINITION_MAP.get(shift_code)
        if not shift:
            return None
        start_h, start_m = [int(part) for part in shift["start"].split(":")]
        day_dt = datetime.strptime(week_start, "%Y-%m-%d") + timedelta(days=day_of_week - 1)
        return day_dt.replace(hour=start_h, minute=start_m, second=0, microsecond=0)

    def _upsert_shift_attendance_mark(
        conn,
        *,
        week_start,
        day_of_week,
        shift_code,
        branch_id,
        employee_id,
        status,
        source,
        note=None,
        attendance_log_id=None,
        marked_by_manager_id=None,
    ):
        conn.execute(
            """
            INSERT INTO shift_attendance_marks(
                week_start,
                day_of_week,
                shift_code,
                branch_id,
                employee_id,
                status,
                source,
                attendance_log_id,
                note,
                marked_by_manager_id,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(week_start, day_of_week, shift_code, branch_id, employee_id)
            DO UPDATE SET
                status = excluded.status,
                source = excluded.source,
                attendance_log_id = excluded.attendance_log_id,
                note = excluded.note,
                marked_by_manager_id = excluded.marked_by_manager_id,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                week_start,
                day_of_week,
                shift_code,
                branch_id,
                employee_id,
                status,
                source,
                attendance_log_id,
                note,
                marked_by_manager_id,
            ),
        )

    def _weekly_hours_rows(conn, week_start, branch_id=None):
        start_dt, end_dt = _week_range(week_start)
        if branch_id:
            rows = conn.execute(
                """
                SELECT u.id AS employee_id,
                       u.display_name AS employee_name,
                       u.username,
                       u.role,
                       b.name AS branch_name,
                       COALESCE(SUM(COALESCE(a.minutes_worked, 0)), 0) AS total_minutes,
                       COUNT(a.id) AS attendance_sessions
                FROM users u
                JOIN branches b ON b.id = ?
                LEFT JOIN employee_branch_access eba
                       ON eba.employee_id = u.id
                      AND eba.branch_id = b.id
                LEFT JOIN attendance_logs a
                       ON a.employee_id = u.id
                      AND a.branch_id = b.id
                      AND a.check_in_at >= ?
                      AND a.check_in_at < ?
                WHERE (
                        (u.role = 'employee' AND eba.employee_id IS NOT NULL)
                     OR (u.role = 'manager' AND u.branch_id = b.id)
                )
                GROUP BY u.id, u.display_name, u.username, u.role, b.name
                ORDER BY u.display_name
                """,
                (branch_id, start_dt, end_dt),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT u.id AS employee_id,
                       u.display_name AS employee_name,
                       u.username,
                       u.role,
                       COALESCE(b.name, 'Toan he thong') AS branch_name,
                       COALESCE(SUM(COALESCE(a.minutes_worked, 0)), 0) AS total_minutes,
                       COUNT(a.id) AS attendance_sessions
                FROM users u
                LEFT JOIN branches b ON b.id = u.branch_id
                LEFT JOIN attendance_logs a
                       ON a.employee_id = u.id
                      AND a.check_in_at >= ?
                      AND a.check_in_at < ?
                WHERE u.role IN ('employee', 'manager')
                GROUP BY u.id, u.display_name, u.username, u.role, COALESCE(b.name, 'Toan he thong')
                ORDER BY u.display_name
                """,
                (start_dt, end_dt),
            ).fetchall()
        return [dict(row) for row in rows]

    def _weekly_attendance_detail_rows(conn, week_start, branch_id=None):
        start_dt, end_dt = _week_range(week_start)
        if branch_id:
            rows = conn.execute(
                """
                SELECT a.id AS attendance_id,
                       u.id AS employee_id,
                       u.username,
                       u.display_name AS employee_name,
                       u.role,
                       COALESCE(b.name, '-') AS branch_name,
                       a.check_in_at,
                       a.check_out_at,
                       a.scheduled_shift_start_at,
                       a.minutes_late,
                       COALESCE(a.minutes_worked, 0) AS minutes_worked
                FROM attendance_logs a
                JOIN users u ON u.id = a.employee_id
                LEFT JOIN branches b ON b.id = a.branch_id
                WHERE a.branch_id = ?
                  AND a.check_in_at >= ?
                  AND a.check_in_at < ?
                ORDER BY u.display_name, a.check_in_at, a.id
                """,
                (branch_id, start_dt, end_dt),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT a.id AS attendance_id,
                       u.id AS employee_id,
                       u.username,
                       u.display_name AS employee_name,
                       u.role,
                       COALESCE(b.name, 'Toan he thong') AS branch_name,
                       a.check_in_at,
                       a.check_out_at,
                       a.scheduled_shift_start_at,
                       a.minutes_late,
                       COALESCE(a.minutes_worked, 0) AS minutes_worked
                FROM attendance_logs a
                JOIN users u ON u.id = a.employee_id
                LEFT JOIN branches b ON b.id = a.branch_id
                WHERE a.check_in_at >= ?
                  AND a.check_in_at < ?
                ORDER BY u.display_name, a.check_in_at, a.id
                """,
                (start_dt, end_dt),
            ).fetchall()
        return [dict(row) for row in rows]

    def _csv_response(filename, headers, rows):
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)
        return Response(
            buf.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    def _build_weekly_payroll_csv(rows, week_start):
        headers = [
            "ma_nhan_vien",
            "tai_khoan",
            "ten_nhan_vien",
            "vai_tro",
            "chi_nhanh",
            "so_gio_lam",
            "so_phien_cham_cong",
            "tuan_bat_dau",
        ]
        csv_rows = [
            [
                item["employee_id"],
                item["username"],
                item["employee_name"],
                item["role"],
                item["branch_name"],
                round(item["total_minutes"] / 60, 2),
                item["attendance_sessions"],
                week_start,
            ]
            for item in rows
        ]
        return headers, csv_rows

    def _csv_sections_response(filename, sections):
        buf = io.StringIO()
        writer = csv.writer(buf)
        for section_index, section in enumerate(sections):
            title = section.get("title")
            headers = section.get("headers") or []
            rows = section.get("rows") or []
            if title:
                writer.writerow([title])
            if headers:
                writer.writerow(headers)
            for row in rows:
                writer.writerow(row)
            if section_index < len(sections) - 1:
                writer.writerow([])
        return Response(
            buf.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    def _build_weekly_payroll_csv_sections(summary_rows, detail_rows, week_start):
        summary_headers = [
            "ma_nhan_vien",
            "tai_khoan",
            "ten_nhan_vien",
            "vai_tro",
            "chi_nhanh",
            "so_gio_lam",
            "so_phien_cham_cong",
            "tuan_bat_dau",
        ]
        summary_csv_rows = [
            [
                item["employee_id"],
                item["username"],
                item["employee_name"],
                item["role"],
                item["branch_name"],
                round(item["total_minutes"] / 60, 2),
                item["attendance_sessions"],
                week_start,
            ]
            for item in summary_rows
        ]

        detail_headers = [
            "ma_phien_cham_cong",
            "ma_nhan_vien",
            "tai_khoan",
            "ten_nhan_vien",
            "vai_tro",
            "chi_nhanh",
            "gio_check_in",
            "gio_check_out",
            "gio_bat_dau_ca_ke_hoach",
            "so_phut_di_tre",
            "so_phut_lam_viec",
            "tuan_bat_dau",
        ]
        detail_csv_rows = [
            [
                item["attendance_id"],
                item["employee_id"],
                item["username"],
                item["employee_name"],
                item["role"],
                item["branch_name"],
                item["check_in_at"],
                item["check_out_at"],
                item["scheduled_shift_start_at"],
                item["minutes_late"],
                item["minutes_worked"],
                week_start,
            ]
            for item in detail_rows
        ]

        return [
            {
                "title": "BAO CAO TONG HOP",
                "headers": summary_headers,
                "rows": summary_csv_rows,
            },
            {
                "title": "BAO CAO CHI TIET",
                "headers": detail_headers,
                "rows": detail_csv_rows,
            },
        ]

    def _parse_pagination(default_page=1, default_page_size=10, max_page_size=100):
        page_raw = (request.args.get("page") or str(default_page)).strip()
        page_size_raw = (request.args.get("page_size") or str(default_page_size)).strip()
        try:
            page = max(1, int(page_raw))
            page_size = max(1, min(max_page_size, int(page_size_raw)))
        except ValueError:
            return None, None, (jsonify({"error": "page/page_size must be integers"}), 400)
        return page, page_size, None

    def _create_audit_log(conn, actor_user, action, target_type, target_id=None, details=None):
        conn.execute(
            """
            INSERT INTO audit_logs(actor_user_id, actor_username, action, target_type, target_id, details)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                actor_user["id"],
                actor_user["username"],
                action,
                target_type,
                target_id,
                details,
            ),
        )

    def _audit_text(value, empty_label="chua cap nhat"):
        text = (value or "").strip()
        return text if text else empty_label

    def _build_branch_create_audit_details(name, location, network_ip):
        return (
            f"Da tao chi nhanh \"{name}\". "
            f"Dia diem: {_audit_text(location)}. "
            f"IP router: {_audit_text(network_ip, 'chua cau hinh')}."
        )

    def _build_branch_update_audit_details(branch_before, *, name, location, network_ip):
        old_name = (branch_before["name"] or "").strip()
        old_location = (branch_before["location"] or "").strip()
        old_network_ip = (branch_before["network_ip"] or "").strip()

        new_name = (name or "").strip()
        new_location = (location or "").strip()
        new_network_ip = (network_ip or "").strip()

        changes = []
        if old_name != new_name:
            changes.append(f"Doi ten tu \"{_audit_text(old_name)}\" sang \"{_audit_text(new_name)}\".")
        if old_location != new_location:
            changes.append(
                f"Cap nhat dia diem tu \"{_audit_text(old_location)}\" sang \"{_audit_text(new_location)}\"."
            )
        if old_network_ip != new_network_ip:
            changes.append(
                "Cap nhat IP router "
                f"tu \"{_audit_text(old_network_ip, 'chua cau hinh')}\" "
                f"sang \"{_audit_text(new_network_ip, 'chua cau hinh')}\"."
            )

        if not changes:
            return f"Da mo cap nhat cho chi nhanh \"{_audit_text(new_name)}\" nhung khong co thay doi du lieu."

        return f"Cap nhat chi nhanh \"{_audit_text(new_name)}\": " + " ".join(changes)

    def _build_branch_delete_audit_details(branch_before):
        name = _audit_text(branch_before["name"])
        location = _audit_text(branch_before["location"])
        network_ip = _audit_text(branch_before["network_ip"], "chua cau hinh")
        return f"Da xoa chi nhanh \"{name}\". Dia diem cu: {location}. IP router cu: {network_ip}."

    def _normalize_day_of_week(value, *, allow_zero=False):
        try:
            day = int(value)
        except (TypeError, ValueError):
            return None
        if allow_zero and day == 0:
            return 0
        if 1 <= day <= 7:
            return day
        return None

    def _get_client_ip():
        forwarded = request.headers.get("X-Forwarded-For") or ""
        if forwarded:
            return forwarded.split(",")[0].strip()
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip.strip()
        return (request.remote_addr or "").strip()

    def _is_branch_ip_allowed(branch_row, client_ip):
        expected_ip = (branch_row["network_ip"] or "").strip() if branch_row else ""
        if not expected_ip:
            # If branch has no configured network IP, skip the IP gate.
            return True
        return expected_ip == client_ip

    def _build_attendance_qr_token(branch_id, expires_ts, nonce):
        payload = f"{branch_id}.{expires_ts}.{nonce}"
        signature = hmac.new(
            ATTENDANCE_QR_SECRET.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return f"{payload}.{signature}"

    def _verify_attendance_qr_token(token, expected_branch_id):
        parts = (token or "").strip().split(".")
        if len(parts) != 4:
            return False, "Invalid QR token format"

        branch_id_raw, expires_raw, nonce, signature = parts
        try:
            branch_id = int(branch_id_raw)
            expires_ts = int(expires_raw)
        except ValueError:
            return False, "Invalid QR token payload"

        if branch_id != expected_branch_id:
            return False, "QR token does not belong to selected branch"
        if expires_ts < int(datetime.now().timestamp()):
            return False, "QR token expired"

        payload = f"{branch_id}.{expires_ts}.{nonce}"
        expected_signature = hmac.new(
            ATTENDANCE_QR_SECRET.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(signature, expected_signature):
            return False, "Invalid QR token signature"

        return True, None

    def _generate_one_time_attendance_code(length=8):
        try:
            length = int(length)
        except (TypeError, ValueError):
            length = 8
        if length < 1:
            length = 1
        alphabet = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"
        return "".join(secrets.choice(alphabet) for _ in range(length))

    def _build_one_time_qr_payload(branch_id, one_time_code, qr_token):
        return f"WM1|{branch_id}|{one_time_code}|{qr_token}"

    def _build_static_branch_qr_payload(branch_id, qr_token):
        return f"WM2|{branch_id}|{qr_token}"

    def _build_qr_image_data_url(content):
        qr = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_M, box_size=7, border=2)
        qr.add_data(content)
        qr.make(fit=True)
        image = qr.make_image(fill_color="black", back_color="white")
        buffer = io.BytesIO()
        image.save(buffer, format="PNG")
        payload = base64.b64encode(buffer.getvalue()).decode("ascii")
        return f"data:image/png;base64,{payload}"

    def _parse_one_time_qr_payload(raw_payload):
        parts = (raw_payload or "").strip().split("|")
        if len(parts) != 4 or parts[0] != "WM1":
            return None, None, None, "Invalid QR one-time payload format"

        try:
            branch_id = int(parts[1])
        except (TypeError, ValueError):
            return None, None, None, "Invalid branch in QR payload"

        one_time_code = (parts[2] or "").strip().upper()
        qr_token = (parts[3] or "").strip()
        if not one_time_code or not qr_token:
            return None, None, None, "Invalid QR one-time payload content"

        return branch_id, one_time_code, qr_token, None

    def _parse_attendance_qr_payload(raw_payload):
        parts = (raw_payload or "").strip().split("|")
        if not parts:
            return None, None, None, None, "Invalid QR payload format"

        version = parts[0]
        if version == "WM1":
            branch_id, one_time_code, qr_token, parse_error = _parse_one_time_qr_payload(raw_payload)
            if parse_error:
                return None, None, None, None, parse_error
            return "legacy", branch_id, one_time_code, qr_token, None

        if version == "WM2":
            if len(parts) != 3:
                return None, None, None, None, "Invalid static QR payload format"
            try:
                branch_id = int(parts[1])
            except (TypeError, ValueError):
                return None, None, None, None, "Invalid branch in static QR payload"

            qr_token = (parts[2] or "").strip()
            if not qr_token:
                return None, None, None, None, "Invalid static QR payload content"
            return "static", branch_id, None, qr_token, None

        return None, None, None, None, "Unsupported QR payload version"

    def _cleanup_one_time_qr_codes(conn):
        now_dt = datetime.now()
        expired_before = _format_db_datetime(now_dt - timedelta(days=2))
        consumed_before = _format_db_datetime(now_dt - timedelta(days=7))
        conn.execute(
            """
            DELETE FROM attendance_qr_one_time_codes
            WHERE expires_at < ?
               OR (consumed_at IS NOT NULL AND consumed_at < ?)
            """,
            (expired_before, consumed_before),
        )

    def _get_branch_staffing_rules(conn, branch_id):
        rows = conn.execute(
            """
            SELECT shift_code, min_staff, max_staff
            FROM branch_shift_requirements
            WHERE branch_id = ?
            """,
            (branch_id,),
        ).fetchall()

        rules = {row["shift_code"]: {"min_staff": row["min_staff"], "max_staff": row["max_staff"]} for row in rows}
        # Fallback defaults for old data or missing rows.
        for shift in SHIFT_DEFINITIONS:
            rules.setdefault(shift["code"], {"min_staff": 3, "max_staff": 4})
        return rules

    def _resolve_today_shift_for_checkin(conn, employee_id, branch_id, current_dt):
        week_start, day_of_week = _week_start_and_day_for_datetime(current_dt)
        rows = conn.execute(
            """
            SELECT id, week_start, day_of_week, shift_code
            FROM weekly_schedule
            WHERE employee_id = ?
              AND branch_id = ?
              AND week_start = ?
              AND day_of_week = ?
            ORDER BY shift_code
            """,
            (employee_id, branch_id, week_start, day_of_week),
        ).fetchall()
        if not rows:
            return None, None, None, None, "Không có ca được phân cho hôm nay"

        enriched = []
        for row in rows:
            start_dt = _shift_start_datetime(row["week_start"], row["day_of_week"], row["shift_code"])
            if not start_dt:
                continue
            late_deadline = start_dt + timedelta(minutes=15)
            enriched.append((row, start_dt, late_deadline))

        if not enriched:
            return None, None, None, None, "Không xác định được thời gian ca"

        enriched.sort(key=lambda item: item[1])
        for row, start_dt, late_deadline in enriched:
            if current_dt <= late_deadline:
                return row, start_dt, late_deadline, week_start, None

        row, start_dt, late_deadline = enriched[-1]
        return row, start_dt, late_deadline, week_start, None

    register_general_routes(
        app,
        {
            "get_conn": get_conn,
            "_get_user_from_token": _get_user_from_token,
            "_get_access_token": _get_access_token,
            "_get_client_ip": _get_client_ip,
            "_is_login_rate_limited": _is_login_rate_limited,
            "_record_login_failure": _record_login_failure,
            "_clear_login_failures": _clear_login_failures,
            "_is_stateless_session_enabled": _is_stateless_session_enabled,
            "_build_stateless_session_token": _build_stateless_session_token,
            "_hash_session_token": _hash_session_token,
            "_permission_payload": _permission_payload,
            "_build_profile_payload": _build_profile_payload,
            "_is_profile_completed": _is_profile_completed,
            "TOKEN_LIFETIME_DAYS": TOKEN_LIFETIME_DAYS,
            "SHIFT_DEFINITIONS": SHIFT_DEFINITIONS,
            "ROLE_PERMISSIONS": ROLE_PERMISSIONS,
            "PROFILE_REQUIRED_ROLES": PROFILE_REQUIRED_ROLES,
        },
    )

    register_attendance_routes(
        app,
        {
            "get_conn": get_conn,
            "_get_user_from_token": _get_user_from_token,
            "_week_range": _week_range,
            "_format_db_datetime": _format_db_datetime,
            "_parse_db_datetime": _parse_db_datetime,
            "_resolve_today_shift_for_checkin": _resolve_today_shift_for_checkin,
            "_upsert_shift_attendance_mark": _upsert_shift_attendance_mark,
            "_cleanup_one_time_qr_codes": _cleanup_one_time_qr_codes,
            "_get_client_ip": _get_client_ip,
            "_is_branch_ip_allowed": _is_branch_ip_allowed,
            "_verify_attendance_qr_token": _verify_attendance_qr_token,
            "_parse_attendance_qr_payload": _parse_attendance_qr_payload,
            "_build_attendance_qr_token": _build_attendance_qr_token,
            "_build_static_branch_qr_payload": _build_static_branch_qr_payload,
            "_build_qr_image_data_url": _build_qr_image_data_url,
            "_generate_one_time_attendance_code": _generate_one_time_attendance_code,
            "_weekly_attendance_detail_rows": _weekly_attendance_detail_rows,
            "ATTENDANCE_QR_ONE_TIME_TTL_SECONDS": ATTENDANCE_QR_ONE_TIME_TTL_SECONDS,
            "ATTENDANCE_QR_ENABLED": ATTENDANCE_QR_ENABLED,
            "SHIFT_DEFINITIONS": SHIFT_DEFINITIONS,
        },
    )

    register_operations_routes(
        app,
        {
            "get_conn": get_conn,
            "_get_user_from_token": _get_user_from_token,
            "_normalize_day_of_week": _normalize_day_of_week,
            "_get_branch_staffing_rules": _get_branch_staffing_rules,
            "_week_start_and_day_for_datetime": _week_start_and_day_for_datetime,
            "_shift_start_datetime": _shift_start_datetime,
            "_parse_db_datetime": _parse_db_datetime,
            "_format_db_datetime": _format_db_datetime,
            "_upsert_shift_attendance_mark": _upsert_shift_attendance_mark,
            "_weekly_hours_rows": _weekly_hours_rows,
            "_weekly_attendance_detail_rows": _weekly_attendance_detail_rows,
            "_csv_response": _csv_response,
            "_csv_sections_response": _csv_sections_response,
            "_build_weekly_payroll_csv": _build_weekly_payroll_csv,
            "_build_weekly_payroll_csv_sections": _build_weekly_payroll_csv_sections,
            "_manager_can_manage_employee": _manager_can_manage_employee,
            "_create_audit_log": _create_audit_log,
            "SHIFT_CODE_SET": SHIFT_CODE_SET,
            "SHIFT_DEFINITIONS": SHIFT_DEFINITIONS,
        },
    )

    register_leadership_routes(
        app,
        {
            "get_conn": get_conn,
            "_get_user_from_token": _get_user_from_token,
            "_weekly_hours_rows": _weekly_hours_rows,
            "_weekly_attendance_detail_rows": _weekly_attendance_detail_rows,
            "_csv_response": _csv_response,
            "_csv_sections_response": _csv_sections_response,
            "_build_weekly_payroll_csv": _build_weekly_payroll_csv,
            "_build_weekly_payroll_csv_sections": _build_weekly_payroll_csv_sections,
            "_create_audit_log": _create_audit_log,
            "_parse_pagination": _parse_pagination,
            "_is_valid_ipv4": _is_valid_ipv4,
            "_build_branch_create_audit_details": _build_branch_create_audit_details,
            "_build_branch_update_audit_details": _build_branch_update_audit_details,
            "_build_branch_delete_audit_details": _build_branch_delete_audit_details,
        },
    )

    @app.get("/")
    def root():
        return send_from_directory(FRONTEND_DIR, "index.html")

    return app


if __name__ == "__main__":
    debug_enabled = os.getenv("FLASK_DEBUG") == "1"
    create_app().run(host="0.0.0.0", port=5000, debug=debug_enabled)
