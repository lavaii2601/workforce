import secrets
import csv
import io
import base64
import re
import hmac
import hashlib
from datetime import datetime, timedelta
from pathlib import Path

import qrcode
from flask import Flask, Response, jsonify, request, send_from_directory
from werkzeug.security import check_password_hash, generate_password_hash

from .constants import SHIFT_CODE_SET, SHIFT_DEFINITIONS
from .db import get_conn, init_db
from .services.openjarvis_service import (
    generate_hr_anomaly_report,
    should_trigger_jarvis,
)


FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"
TOKEN_LIFETIME_DAYS = 7

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
    init_db()

    PROFILE_REQUIRED_ROLES = {"employee", "manager"}
    ATTENDANCE_QR_ONE_TIME_TTL_SECONDS = 45
    ATTENDANCE_QR_SECRET = "workforce-attendance-qr-secret"
    DB_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    SHIFT_DEFINITION_MAP = {item["code"]: item for item in SHIFT_DEFINITIONS}

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
            (token,),
        ).fetchone()

        if not user:
            conn.close()
            return None, (jsonify({"error": "Invalid or expired session"}), 401)

        user_dict = dict(user)
        if not user_dict.get("is_active", 1):
            conn.execute("DELETE FROM auth_sessions WHERE token = ?", (token,))
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
                       b.name AS branch_name,
                       COALESCE(SUM(COALESCE(a.minutes_worked, 0)), 0) AS total_minutes,
                       COUNT(a.id) AS attendance_sessions
                FROM users u
                JOIN employee_branch_access eba
                     ON eba.employee_id = u.id
                    AND eba.branch_id = ?
                JOIN branches b ON b.id = eba.branch_id
                LEFT JOIN attendance_logs a
                       ON a.employee_id = u.id
                      AND a.branch_id = eba.branch_id
                      AND a.check_in_at >= ?
                      AND a.check_in_at < ?
                WHERE u.role = 'employee'
                GROUP BY u.id, u.display_name, u.username, b.name
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
                       'Toan he thong' AS branch_name,
                       COALESCE(SUM(COALESCE(a.minutes_worked, 0)), 0) AS total_minutes,
                       COUNT(a.id) AS attendance_sessions
                FROM users u
                LEFT JOIN attendance_logs a
                       ON a.employee_id = u.id
                      AND a.check_in_at >= ?
                      AND a.check_in_at < ?
                WHERE u.role = 'employee'
                GROUP BY u.id, u.display_name, u.username
                ORDER BY u.display_name
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

    def _generate_one_time_attendance_code():
        alphabet = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"
        return "".join(secrets.choice(alphabet) for _ in range(8))

    def _build_one_time_qr_payload(branch_id, one_time_code, qr_token):
        return f"WM1|{branch_id}|{one_time_code}|{qr_token}"

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

    @app.get("/api/health")
    def health():
        return jsonify({"status": "ok"})

    @app.get("/api/demo-users")
    def demo_users():
        conn = get_conn()
        rows = conn.execute(
            "SELECT id, username, display_name, role FROM users WHERE is_active = 1 ORDER BY role, display_name"
        ).fetchall()
        conn.close()
        return jsonify([dict(row) for row in rows])

    @app.post("/api/login")
    def login():
        body = request.get_json(silent=True) or {}
        username = (body.get("username") or "").strip()
        password = body.get("password") or ""
        if not username:
            return jsonify({"error": "username is required"}), 400
        if not password:
            return jsonify({"error": "password is required"}), 400

        conn = get_conn()
        user = conn.execute(
            """
            SELECT id,
                   username,
                   display_name,
                   role,
                   branch_id,
                   password_hash,
                   is_active,
                   avatar_data_url,
                   full_name,
                   date_of_birth,
                   phone_number,
                     address,
                   job_position
            FROM users
            WHERE username = ?
            """,
            (username,),
        ).fetchone()

        if not user:
            conn.close()
            return jsonify({"error": "User not found"}), 404
        if not user["is_active"]:
            conn.close()
            return jsonify({"error": "User is inactive"}), 403
        if not user["password_hash"] or not check_password_hash(user["password_hash"], password):
            conn.close()
            return jsonify({"error": "Invalid username or password"}), 401

        expires_at = (datetime.utcnow() + timedelta(days=TOKEN_LIFETIME_DAYS)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        token = secrets.token_urlsafe(32)
        conn.execute("DELETE FROM auth_sessions WHERE expires_at <= CURRENT_TIMESTAMP")
        conn.execute(
            "INSERT INTO auth_sessions(user_id, token, expires_at) VALUES (?, ?, ?)",
            (user["id"], token, expires_at),
        )
        conn.execute(
            "DELETE FROM auth_sessions WHERE user_id = ? AND token != ?",
            (user["id"], token),
        )
        conn.commit()
        conn.close()

        user_payload = {
            "id": user["id"],
            "username": user["username"],
            "display_name": user["display_name"],
            "role": user["role"],
            "branch_id": user["branch_id"],
            "is_active": user["is_active"],
            "avatar_data_url": user["avatar_data_url"],
            "full_name": user["full_name"],
            "date_of_birth": user["date_of_birth"],
            "phone_number": user["phone_number"],
            "address": user["address"],
            "job_position": user["job_position"],
        }
        return jsonify(
            {
                "token": token,
                "expires_at": expires_at,
                "user": _permission_payload(user_payload),
            }
        )

    @app.post("/api/change-password-login")
    def change_password_login():
        body = request.get_json(silent=True) or {}
        username = (body.get("username") or "").strip()
        current_password = body.get("current_password") or ""
        new_password = body.get("new_password") or ""

        if not username:
            return jsonify({"error": "username is required"}), 400
        if not current_password:
            return jsonify({"error": "current_password is required"}), 400
        if not new_password:
            return jsonify({"error": "new_password is required"}), 400
        if len(new_password) < 8:
            return jsonify({"error": "Mật khẩu mới phải có ít nhất 8 ký tự"}), 400
        if new_password == current_password:
            return jsonify({"error": "Mật khẩu mới phải khác mật khẩu hiện tại"}), 400

        conn = get_conn()
        user = conn.execute(
            """
            SELECT id, username, password_hash, is_active
            FROM users
            WHERE username = ?
            """,
            (username,),
        ).fetchone()

        if not user:
            conn.close()
            return jsonify({"error": "User not found"}), 404
        if not user["is_active"]:
            conn.close()
            return jsonify({"error": "User is inactive"}), 403
        if not user["password_hash"] or not check_password_hash(user["password_hash"], current_password):
            conn.close()
            return jsonify({"error": "Mật khẩu hiện tại không đúng"}), 401

        conn.execute(
            "UPDATE users SET password_hash = ? WHERE id = ?",
            (generate_password_hash(new_password), user["id"]),
        )
        conn.execute("DELETE FROM auth_sessions WHERE user_id = ?", (user["id"],))
        conn.commit()
        conn.close()
        return jsonify({"message": "Đổi mật khẩu thành công. Vui lòng đăng nhập lại."})

    @app.post("/api/logout")
    def logout():
        token = _get_access_token()
        if not token:
            return jsonify({"message": "No active token"})

        conn = get_conn()
        conn.execute("DELETE FROM auth_sessions WHERE token = ?", (token,))
        conn.commit()
        conn.close()
        return jsonify({"message": "Logged out"})

    @app.post("/api/change-password")
    def change_password():
        user, error = _get_user_from_token(required=True)
        if error:
            return error

        body = request.get_json(silent=True) or {}
        old_password = body.get("old_password") or ""
        new_password = body.get("new_password") or ""
        if len(new_password) < 6:
            return jsonify({"error": "new_password must be at least 6 characters"}), 400

        conn = get_conn()
        db_user = conn.execute(
            "SELECT id, password_hash FROM users WHERE id = ?", (user["id"],)
        ).fetchone()
        if not db_user or not check_password_hash(db_user["password_hash"], old_password):
            conn.close()
            return jsonify({"error": "Old password is incorrect"}), 400

        conn.execute(
            "UPDATE users SET password_hash = ? WHERE id = ?",
            (generate_password_hash(new_password), user["id"]),
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "Password updated"})

    @app.get("/api/profile/me")
    def get_my_profile():
        user, error = _get_user_from_token(required=True)
        if error:
            return error
        payload = {
            "id": user["id"],
            "username": user["username"],
            "role": user["role"],
            "display_name": user["display_name"],
            "profile": _build_profile_payload(user),
            "profile_completed": _is_profile_completed(user),
            "needs_profile_completion": (
                user["role"] in PROFILE_REQUIRED_ROLES and not _is_profile_completed(user)
            ),
        }
        return jsonify(payload)

    @app.put("/api/profile/me")
    def upsert_my_profile():
        user, error = _get_user_from_token(required=True)
        if error:
            return error

        body = request.get_json(silent=True) or {}
        full_name = (body.get("full_name") or "").strip()
        date_of_birth = (body.get("date_of_birth") or "").strip()
        phone_number = (body.get("phone_number") or "").strip()
        address = (body.get("address") or "").strip()
        avatar_data_url = (body.get("avatar_data_url") or "").strip()

        if not full_name:
            return jsonify({"error": "full_name is required"}), 400
        if not date_of_birth:
            return jsonify({"error": "date_of_birth is required"}), 400
        if not phone_number:
            return jsonify({"error": "phone_number is required"}), 400
        if not re.fullmatch(r"[0-9+]{8,15}", phone_number):
            return jsonify({"error": "phone_number must be 8-15 digits or include '+'"}), 400
        if not address:
            return jsonify({"error": "address is required"}), 400
        if not avatar_data_url:
            return jsonify({"error": "avatar_data_url is required"}), 400
        if not avatar_data_url.startswith("data:image/"):
            return jsonify({"error": "avatar_data_url must be a valid image data URL"}), 400

        conn = get_conn()
        conn.execute(
            """
            UPDATE users
            SET full_name = ?,
                date_of_birth = ?,
                phone_number = ?,
                address = ?,
                avatar_data_url = ?,
                display_name = ?
            WHERE id = ?
            """,
            (full_name, date_of_birth, phone_number, address, avatar_data_url, full_name, user["id"]),
        )
        conn.commit()

        updated = conn.execute(
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
            (user["id"],),
        ).fetchone()
        conn.close()
        updated_payload = _permission_payload(dict(updated))
        return jsonify({"message": "Profile updated", "user": updated_payload})

    @app.get("/api/permissions")
    def permissions_matrix():
        _, error = _get_user_from_token(required=True)
        if error:
            return error
        return jsonify({"roles": ROLE_PERMISSIONS})

    @app.get("/api/meta")
    def meta():
        conn = get_conn()
        branches = conn.execute("SELECT id, name FROM branches ORDER BY name").fetchall()
        conn.close()
        return jsonify(
            {
                "shifts": SHIFT_DEFINITIONS,
                "branches": [dict(row) for row in branches],
            }
        )

    @app.post("/api/attendance/check-in")
    def attendance_check_in():
        user, error = _get_user_from_token(roles={"employee", "manager"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        branch_id = body.get("branch_id")
        note = (body.get("note") or "").strip() or None

        conn = get_conn()
        if user["role"] == "manager":
            branch_id = user["branch_id"]
        else:
            allowed = {
                row["branch_id"]
                for row in conn.execute(
                    "SELECT branch_id FROM employee_branch_access WHERE employee_id = ?",
                    (user["id"],),
                ).fetchall()
            }
            if not allowed:
                conn.close()
                return jsonify({"error": "No branch assigned for this employee"}), 400
            if branch_id is None:
                branch_id = next(iter(allowed))
            if branch_id not in allowed:
                conn.close()
                return jsonify({"error": "Invalid branch for attendance"}), 400

        open_log = conn.execute(
            """
            SELECT id FROM attendance_logs
            WHERE employee_id = ?
              AND check_out_at IS NULL
            ORDER BY id DESC
            LIMIT 1
            """,
            (user["id"],),
        ).fetchone()
        if open_log:
            conn.close()
            return jsonify({"error": "Please check-out current session before new check-in"}), 400

        cur = conn.execute(
            """
            INSERT INTO attendance_logs(employee_id, branch_id, check_in_at, note)
            VALUES (?, ?, CURRENT_TIMESTAMP, ?)
            """,
            (user["id"], branch_id, note),
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "Checked in", "attendance_id": cur.lastrowid}), 201

    @app.post("/api/attendance/check-out")
    def attendance_check_out():
        user, error = _get_user_from_token(roles={"employee", "manager"})
        if error:
            return error

        conn = get_conn()
        open_log = conn.execute(
            """
            SELECT id, check_in_at
            FROM attendance_logs
            WHERE employee_id = ?
              AND check_out_at IS NULL
            ORDER BY id DESC
            LIMIT 1
            """,
            (user["id"],),
        ).fetchone()
        if not open_log:
            conn.close()
            return jsonify({"error": "No open attendance session to check-out"}), 400

        check_in_dt = _parse_db_datetime(open_log["check_in_at"])
        minutes = max(1, int((datetime.now() - check_in_dt).total_seconds() // 60))
        conn.execute(
            """
            UPDATE attendance_logs
            SET check_out_at = CURRENT_TIMESTAMP,
                minutes_worked = ?
            WHERE id = ?
            """,
            (minutes, open_log["id"]),
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "Checked out", "minutes_worked": minutes})

    @app.get("/api/attendance/my-week")
    def attendance_my_week():
        user, error = _get_user_from_token(roles={"employee", "manager"})
        if error:
            return error

        week_start = (request.args.get("week_start") or "").strip()
        if not week_start:
            return jsonify({"error": "week_start is required"}), 400
        start_dt, end_dt = _week_range(week_start)

        conn = get_conn()
        rows = conn.execute(
            """
            SELECT a.id,
                   a.check_in_at,
                   a.check_out_at,
                   COALESCE(a.minutes_worked, 0) AS minutes_worked,
                   a.note,
                   COALESCE(b.name, '-') AS branch_name
            FROM attendance_logs a
            LEFT JOIN branches b ON b.id = a.branch_id
            WHERE a.employee_id = ?
              AND a.check_in_at >= ?
              AND a.check_in_at < ?
            ORDER BY a.check_in_at DESC
            """,
            (user["id"], start_dt, end_dt),
        ).fetchall()
        conn.close()
        data = [dict(row) for row in rows]
        total_minutes = sum(item["minutes_worked"] for item in data)
        return jsonify({"items": data, "total_minutes": total_minutes})

    @app.post("/api/manager/attendance-qr-one-time")
    def manager_attendance_qr_one_time():
        user, error = _get_user_from_token(roles={"manager"})
        if error:
            return error

        if not user.get("branch_id"):
            return jsonify({"error": "Manager has no branch assigned"}), 400

        conn = get_conn()
        branch = conn.execute(
            "SELECT id, name, network_ip FROM branches WHERE id = ?",
            (user["branch_id"],),
        ).fetchone()
        if not branch:
            conn.close()
            return jsonify({"error": "Branch not found"}), 404

        now_dt = datetime.now()
        expires_at_dt = now_dt.replace(hour=23, minute=59, second=59, microsecond=0)
        if expires_at_dt <= now_dt:
            expires_at_dt = expires_at_dt + timedelta(days=1)
        expires_ts = int(expires_at_dt.timestamp())
        qr_nonce = secrets.token_hex(4)
        qr_token = _build_attendance_qr_token(branch["id"], expires_ts, qr_nonce)
        one_time_code = _generate_one_time_attendance_code()
        expires_at = _format_db_datetime(expires_at_dt)

        _cleanup_one_time_qr_codes(conn)

        conn.execute(
            """
            INSERT INTO attendance_qr_one_time_codes(
                branch_id,
                qr_token,
                one_time_code,
                expires_at,
                generated_by_manager_id
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (branch["id"], qr_token, one_time_code, expires_at, user["id"]),
        )
        conn.commit()
        conn.close()

        return jsonify(
            {
                "branch_id": branch["id"],
                "branch_name": branch["name"],
                "network_ip": branch["network_ip"],
                "qr_token": qr_token,
                "one_time_code": one_time_code,
                "qr_payload": _build_one_time_qr_payload(branch["id"], one_time_code, qr_token),
                "qr_image_data_url": _build_qr_image_data_url(
                    _build_one_time_qr_payload(branch["id"], one_time_code, qr_token)
                ),
                "expires_at": expires_at,
                "ttl_seconds": max(1, int((expires_at_dt - now_dt).total_seconds())),
            }
        )

    @app.post("/api/attendance/check-in-qr-one-time")
    def attendance_check_in_qr_one_time():
        user, error = _get_user_from_token(roles={"employee"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        qr_token = (body.get("qr_token") or "").strip()
        one_time_code = (body.get("one_time_code") or "").strip().upper()
        note = (body.get("note") or "").strip() or None
        branch_id = body.get("branch_id")

        try:
            branch_id = int(branch_id)
        except (TypeError, ValueError):
            return jsonify({"error": "branch_id is required and must be integer"}), 400
        if not qr_token:
            return jsonify({"error": "qr_token is required"}), 400
        if not one_time_code:
            return jsonify({"error": "one_time_code is required"}), 400

        token_ok, token_error = _verify_attendance_qr_token(qr_token, branch_id)
        if not token_ok:
            return jsonify({"error": token_error}), 400

        conn = get_conn()
        branch = conn.execute(
            "SELECT id, name, network_ip FROM branches WHERE id = ?",
            (branch_id,),
        ).fetchone()
        if not branch:
            conn.close()
            return jsonify({"error": "Branch not found"}), 404

        allowed = conn.execute(
            "SELECT 1 FROM employee_branch_access WHERE employee_id = ? AND branch_id = ? LIMIT 1",
            (user["id"], branch_id),
        ).fetchone()
        if not allowed:
            conn.close()
            return jsonify({"error": "Branch is not in employee access scope"}), 403

        client_ip = _get_client_ip()
        if not _is_branch_ip_allowed(branch, client_ip):
            conn.close()
            return jsonify({"error": "You must connect from branch network to check in"}), 403

        now_dt = datetime.now()
        shift_row, shift_start_dt, late_deadline_dt, shift_week_start, shift_error = _resolve_today_shift_for_checkin(
            conn,
            user["id"],
            branch_id,
            now_dt,
        )
        if shift_error:
            conn.close()
            return jsonify({"error": shift_error}), 400

        existing_mark = conn.execute(
            """
            SELECT status
            FROM shift_attendance_marks
            WHERE week_start = ?
              AND day_of_week = ?
              AND shift_code = ?
              AND branch_id = ?
              AND employee_id = ?
            LIMIT 1
            """,
            (
                shift_week_start,
                shift_row["day_of_week"],
                shift_row["shift_code"],
                branch_id,
                user["id"],
            ),
        ).fetchone()
        if existing_mark and existing_mark["status"] in {"present", "present_override"}:
            conn.close()
            return jsonify({"error": "Ca này đã được xác nhận đi làm"}), 400

        now_raw = _format_db_datetime(now_dt)
        _cleanup_one_time_qr_codes(conn)

        one_time_row = conn.execute(
            """
            SELECT id, expires_at
            FROM attendance_qr_one_time_codes
            WHERE branch_id = ?
              AND qr_token = ?
              AND one_time_code = ?
              AND consumed_at IS NULL
              AND expires_at >= ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (branch_id, qr_token, one_time_code, now_raw),
        ).fetchone()
        if not one_time_row:
            conn.close()
            return jsonify({"error": "Invalid, consumed, or expired one-time QR code"}), 400

        try:
            expires_at_dt = _parse_db_datetime(one_time_row["expires_at"])
        except (TypeError, ValueError):
            conn.close()
            return jsonify({"error": "One-time key không hợp lệ"}), 400
        if now_dt > expires_at_dt:
            conn.close()
            return jsonify({"error": "One-time key đã hết hạn"}), 400

        if now_dt > late_deadline_dt:
            _upsert_shift_attendance_mark(
                conn,
                week_start=shift_week_start,
                day_of_week=shift_row["day_of_week"],
                shift_code=shift_row["shift_code"],
                branch_id=branch_id,
                employee_id=user["id"],
                status="absent",
                source="auto_late",
                note="Vào ca trễ quá 15 phút",
            )
            conn.commit()
            conn.close()
            return (
                jsonify(
                    {
                        "error": "Bạn vào ca trễ quá 15 phút. Hệ thống đã đánh vắng cho ca này",
                        "shift_code": shift_row["shift_code"],
                        "shift_start_at": _format_db_datetime(shift_start_dt),
                        "late_deadline_at": _format_db_datetime(late_deadline_dt),
                    }
                ),
                400,
            )

        open_log = conn.execute(
            """
            SELECT id FROM attendance_logs
            WHERE employee_id = ?
              AND check_out_at IS NULL
            ORDER BY id DESC
            LIMIT 1
            """,
            (user["id"],),
        ).fetchone()
        if open_log:
            conn.close()
            return jsonify({"error": "Please check-out current session before new check-in"}), 400

        conn.execute(
            """
            UPDATE attendance_qr_one_time_codes
            SET consumed_at = CURRENT_TIMESTAMP,
                consumed_by_employee_id = ?,
                request_ip = ?
            WHERE id = ?
            """,
            (user["id"], client_ip, one_time_row["id"]),
        )
        cur = conn.execute(
            """
            INSERT INTO attendance_logs(employee_id, branch_id, check_in_at, note)
            VALUES (?, ?, CURRENT_TIMESTAMP, ?)
            """,
            (user["id"], branch_id, note),
        )

        _upsert_shift_attendance_mark(
            conn,
            week_start=shift_week_start,
            day_of_week=shift_row["day_of_week"],
            shift_code=shift_row["shift_code"],
            branch_id=branch_id,
            employee_id=user["id"],
            status="present",
            source="one_time_qr",
            note=note,
            attendance_log_id=cur.lastrowid,
        )
        conn.commit()
        conn.close()
        return (
            jsonify(
                {
                    "message": "Checked in with one-time QR",
                    "attendance_id": cur.lastrowid,
                    "shift_code": shift_row["shift_code"],
                    "shift_start_at": _format_db_datetime(shift_start_dt),
                }
            ),
            201,
        )

    @app.post("/api/attendance/scan-qr-one-time")
    def attendance_scan_qr_one_time():
        user, error = _get_user_from_token(roles={"employee"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        qr_payload = body.get("qr_payload")
        branch_id, one_time_code, qr_token, parse_error = _parse_one_time_qr_payload(qr_payload)
        if parse_error:
            return jsonify({"error": parse_error}), 400

        token_ok, token_error = _verify_attendance_qr_token(qr_token, branch_id)
        if not token_ok:
            return jsonify({"error": token_error}), 400

        conn = get_conn()
        branch = conn.execute(
            "SELECT id, name, network_ip FROM branches WHERE id = ?",
            (branch_id,),
        ).fetchone()
        if not branch:
            conn.close()
            return jsonify({"error": "Branch not found"}), 404

        allowed = conn.execute(
            "SELECT 1 FROM employee_branch_access WHERE employee_id = ? AND branch_id = ? LIMIT 1",
            (user["id"], branch_id),
        ).fetchone()
        if not allowed:
            conn.close()
            return jsonify({"error": "Branch is not in employee access scope"}), 403

        client_ip = _get_client_ip()
        if not _is_branch_ip_allowed(branch, client_ip):
            conn.close()
            return jsonify({"error": "Ban phai ket noi Wi-Fi chi nhanh de quet QR"}), 403

        now_raw = _format_db_datetime(datetime.now())
        _cleanup_one_time_qr_codes(conn)

        one_time_row = conn.execute(
            """
            SELECT id, expires_at
            FROM attendance_qr_one_time_codes
            WHERE branch_id = ?
              AND qr_token = ?
              AND one_time_code = ?
              AND consumed_at IS NULL
              AND expires_at >= ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (branch_id, qr_token, one_time_code, now_raw),
        ).fetchone()
        conn.close()
        if not one_time_row:
            return jsonify({"error": "QR one-time da het han hoac da duoc su dung"}), 400

        try:
            expires_at_dt = _parse_db_datetime(one_time_row["expires_at"])
        except (TypeError, ValueError):
            return jsonify({"error": "QR one-time da het han hoac da duoc su dung"}), 400
        if datetime.now() > expires_at_dt:
            return jsonify({"error": "QR one-time da het han hoac da duoc su dung"}), 400

        return jsonify(
            {
                "branch_id": branch_id,
                "qr_token": qr_token,
                "random_key": one_time_code,
                "expires_at": _format_db_datetime(expires_at_dt),
            }
        )

    @app.post("/api/issues")
    def create_issue():
        user, error = _get_user_from_token(roles={"employee", "manager"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        title = (body.get("title") or "").strip()
        details = (body.get("details") or "").strip()
        branch_id = body.get("branch_id")
        if not title or not details:
            return jsonify({"error": "title and details are required"}), 400

        conn = get_conn()
        if user["role"] == "employee":
            allowed = {
                row["branch_id"]
                for row in conn.execute(
                    "SELECT branch_id FROM employee_branch_access WHERE employee_id = ?",
                    (user["id"],),
                ).fetchall()
            }
            if not allowed:
                conn.close()
                return jsonify({"error": "No branch assigned for this employee"}), 400
            if branch_id is None:
                branch_id = next(iter(allowed))
            if branch_id not in allowed:
                conn.close()
                return jsonify({"error": "Invalid branch for issue report"}), 400
        elif user["role"] == "manager":
            branch_id = user["branch_id"]

        conn.execute(
            """
            INSERT INTO issue_reports(reporter_id, reporter_role, branch_id, title, details)
            VALUES (?, ?, ?, ?, ?)
            """,
            (user["id"], user["role"], branch_id, title, details),
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "Issue report submitted"}), 201

    @app.get("/api/issues/my")
    def my_issues():
        user, error = _get_user_from_token(roles={"employee", "manager"})
        if error:
            return error

        conn = get_conn()
        rows = conn.execute(
            """
            SELECT i.id,
                   i.title,
                   i.details,
                   i.branch_id,
                   i.status,
                   i.escalated_to_ceo,
                   i.manager_note,
                   i.created_at,
                   i.updated_at,
                   COALESCE(b.name, '-') AS branch_name
            FROM issue_reports i
            LEFT JOIN branches b ON b.id = i.branch_id
            WHERE i.reporter_id = ?
            ORDER BY i.id DESC
            """,
            (user["id"],),
        ).fetchall()
        conn.close()
        return jsonify([dict(row) for row in rows])

    @app.get("/api/employee/branches")
    def employee_branches():
        user, error = _get_user_from_token(roles={"employee"})
        if error:
            return error

        conn = get_conn()
        rows = conn.execute(
            """
            SELECT b.id, b.name
            FROM employee_branch_access eba
            JOIN branches b ON b.id = eba.branch_id
            WHERE eba.employee_id = ?
            ORDER BY b.name
            """,
            (user["id"],),
        ).fetchall()
        conn.close()

        return jsonify([dict(row) for row in rows])

    @app.put("/api/employee/preferences")
    def upsert_preferences():
        user, error = _get_user_from_token(roles={"employee"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        week_start = (body.get("week_start") or "").strip()
        selections = body.get("selections") or []

        if not week_start:
            return jsonify({"error": "week_start is required"}), 400
        if not isinstance(selections, list):
            return jsonify({"error": "selections must be a list"}), 400

        conn = get_conn()
        allowed_branch_ids = {
            row["branch_id"]
            for row in conn.execute(
                "SELECT branch_id FROM employee_branch_access WHERE employee_id = ?",
                (user["id"],),
            ).fetchall()
        }

        valid_rows = []
        seen = set()
        for item in selections:
            branch_id = item.get("branch_id")
            shift_code = item.get("shift_code")
            day_of_week = _normalize_day_of_week(item.get("day_of_week"))
            if branch_id not in allowed_branch_ids:
                conn.close()
                return jsonify({"error": f"Branch {branch_id} not allowed"}), 400
            if shift_code not in SHIFT_CODE_SET:
                conn.close()
                return jsonify({"error": f"Invalid shift code: {shift_code}"}), 400
            if day_of_week is None:
                conn.close()
                return jsonify({"error": "day_of_week must be in range 1..7"}), 400
            key = (branch_id, shift_code, day_of_week)
            if key in seen:
                continue
            seen.add(key)
            valid_rows.append((user["id"], week_start, branch_id, shift_code, day_of_week))

        conn.execute(
            "DELETE FROM shift_preferences WHERE employee_id = ? AND week_start = ?",
            (user["id"], week_start),
        )
        if valid_rows:
            conn.executemany(
                """
                INSERT INTO shift_preferences(employee_id, week_start, branch_id, shift_code, day_of_week)
                VALUES (?, ?, ?, ?, ?)
                """,
                valid_rows,
            )
        conn.commit()

    @app.get("/api/employee/preferences")
    def employee_preferences():
        user, error = _get_user_from_token(roles={"employee"})
        if error:
            return error

        week_start = (request.args.get("week_start") or "").strip()
        if not week_start:
            return jsonify({"error": "week_start is required"}), 400

        conn = get_conn()
        rows = conn.execute(
            """
            SELECT sp.id, sp.week_start, sp.branch_id, b.name AS branch_name, sp.shift_code, sp.day_of_week
            FROM shift_preferences sp
            JOIN branches b ON b.id = sp.branch_id
            WHERE sp.employee_id = ? AND sp.week_start = ?
            ORDER BY sp.day_of_week, b.name, sp.shift_code
            """,
            (user["id"], week_start),
        ).fetchall()
        conn.close()

        return jsonify([dict(row) for row in rows])

    @app.get("/api/employee/assigned-schedule")
    def employee_assigned_schedule():
        user, error = _get_user_from_token(roles={"employee"})
        if error:
            return error

        week_start = (request.args.get("week_start") or "").strip()
        if not week_start:
            return jsonify({"error": "week_start is required"}), 400

        conn = get_conn()
        rows = conn.execute(
            """
            SELECT ws.id,
                   ws.week_start,
                   ws.shift_code,
                 ws.day_of_week,
                   ws.branch_id,
                   b.name AS branch_name,
                   m.display_name AS assigned_by_name
            FROM weekly_schedule ws
            JOIN branches b ON b.id = ws.branch_id
            JOIN users m ON m.id = ws.assigned_by
            WHERE ws.employee_id = ?
              AND ws.week_start = ?
                        ORDER BY ws.day_of_week, ws.shift_code, b.name
            """,
            (user["id"], week_start),
        ).fetchall()
        conn.close()

        return jsonify([dict(row) for row in rows])

    @app.get("/api/manager/preferences")
    def manager_view_preferences():
        user, error = _get_user_from_token(roles={"manager"})
        if error:
            return error

        week_start = (request.args.get("week_start") or "").strip()
        if not week_start:
            return jsonify({"error": "week_start is required"}), 400

        conn = get_conn()
        rows = conn.execute(
            """
            SELECT sp.id,
                   sp.employee_id,
                   u.display_name AS employee_name,
                   sp.week_start,
                   sp.branch_id,
                   b.name AS branch_name,
                     sp.shift_code,
                     sp.day_of_week
            FROM shift_preferences sp
            JOIN users u ON u.id = sp.employee_id
            JOIN branches b ON b.id = sp.branch_id
            WHERE sp.week_start = ?
              AND sp.branch_id = ?
                        ORDER BY sp.day_of_week, sp.shift_code, u.display_name
            """,
            (week_start, user["branch_id"]),
        ).fetchall()
        conn.close()

        return jsonify([dict(row) for row in rows])

    @app.put("/api/manager/schedule")
    def manager_save_schedule():
        user, error = _get_user_from_token(roles={"manager"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        week_start = (body.get("week_start") or "").strip()
        assignments = body.get("assignments") or []

        if not week_start:
            return jsonify({"error": "week_start is required"}), 400
        if not isinstance(assignments, list):
            return jsonify({"error": "assignments must be a list"}), 400

        conn = get_conn()
        staffing_rules = _get_branch_staffing_rules(conn, user["branch_id"])
        normalized = []
        seen = set()
        for item in assignments:
            employee_id = item.get("employee_id")
            shift_code = item.get("shift_code")
            day_of_week = _normalize_day_of_week(item.get("day_of_week"))
            if shift_code not in SHIFT_CODE_SET:
                conn.close()
                return jsonify({"error": f"Invalid shift code: {shift_code}"}), 400
            if day_of_week is None:
                conn.close()
                return jsonify({"error": "day_of_week must be in range 1..7"}), 400

            pref_exists = conn.execute(
                """
                SELECT 1
                FROM shift_preferences
                WHERE employee_id = ?
                  AND week_start = ?
                  AND branch_id = ?
                  AND shift_code = ?
                                    AND (day_of_week = ? OR day_of_week = 0)
                """,
                                (employee_id, week_start, user["branch_id"], shift_code, day_of_week),
            ).fetchone()
            if not pref_exists:
                conn.close()
                return jsonify(
                    {
                        "error": "Only selected shifts can be assigned",
                        "employee_id": employee_id,
                        "shift_code": shift_code,
                        "day_of_week": day_of_week,
                    }
                ), 400

            key = (employee_id, shift_code, day_of_week)
            if key in seen:
                continue
            seen.add(key)
            normalized.append(
                (week_start, user["branch_id"], employee_id, shift_code, day_of_week, user["id"])
            )

        # Enforce staffing size per day+shift for assigned slots.
        counts = {}
        for _, _, _, shift_code, day_of_week, _ in normalized:
            key = (shift_code, day_of_week)
            counts[key] = counts.get(key, 0) + 1

        violations = []
        for (shift_code, day_of_week), count in counts.items():
            rule = staffing_rules.get(shift_code, {"min_staff": 3, "max_staff": 4})
            min_staff = int(rule["min_staff"])
            max_staff = int(rule["max_staff"])
            if count < min_staff or count > max_staff:
                violations.append(
                    {
                        "shift_code": shift_code,
                        "day_of_week": day_of_week,
                        "count": count,
                        "min_staff": min_staff,
                        "max_staff": max_staff,
                    }
                )

        if violations:
            conn.close()
            first = violations[0]
            return (
                jsonify(
                    {
                        "error": (
                            f"Staffing out of range at {first['shift_code']} (day {first['day_of_week']}): "
                            f"{first['count']} assigned, required {first['min_staff']}-{first['max_staff']}"
                        ),
                        "violations": violations,
                    }
                ),
                400,
            )

        conn.execute(
            "DELETE FROM weekly_schedule WHERE week_start = ? AND branch_id = ?",
            (week_start, user["branch_id"]),
        )
        if normalized:
            conn.executemany(
                """
                INSERT INTO weekly_schedule(
                    week_start,
                    branch_id,
                    employee_id,
                    shift_code,
                    day_of_week,
                    assigned_by
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                normalized,
            )
        conn.commit()
        conn.close()
        return jsonify({"message": "Weekly schedule saved", "count": len(normalized)})

    @app.get("/api/manager/staffing-rules")
    def manager_staffing_rules_get():
        user, error = _get_user_from_token(roles={"manager"})
        if error:
            return error

        conn = get_conn()
        rules = _get_branch_staffing_rules(conn, user["branch_id"])
        conn.close()

        payload = []
        for shift in SHIFT_DEFINITIONS:
            rule = rules.get(shift["code"], {"min_staff": 3, "max_staff": 4})
            payload.append(
                {
                    "shift_code": shift["code"],
                    "shift_name": shift["name"],
                    "min_staff": int(rule["min_staff"]),
                    "max_staff": int(rule["max_staff"]),
                }
            )
        return jsonify(payload)

    @app.put("/api/manager/staffing-rules")
    def manager_staffing_rules_put():
        user, error = _get_user_from_token(roles={"manager"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        rules = body.get("rules") or []
        if not isinstance(rules, list) or not rules:
            return jsonify({"error": "rules must be a non-empty list"}), 400

        normalized = []
        seen = set()
        for item in rules:
            shift_code = item.get("shift_code")
            if shift_code not in SHIFT_CODE_SET:
                return jsonify({"error": f"Invalid shift code: {shift_code}"}), 400
            if shift_code in seen:
                continue
            seen.add(shift_code)

            try:
                min_staff = int(item.get("min_staff"))
                max_staff = int(item.get("max_staff"))
            except (TypeError, ValueError):
                return jsonify({"error": "min_staff/max_staff must be integers"}), 400

            if min_staff < 0 or max_staff < 1 or min_staff > max_staff:
                return jsonify({"error": f"Invalid range for {shift_code}. Ensure 0 <= min <= max and max >= 1"}), 400

            normalized.append((user["branch_id"], shift_code, min_staff, max_staff))

        conn = get_conn()
        conn.executemany(
            """
            INSERT INTO branch_shift_requirements(branch_id, shift_code, min_staff, max_staff)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(branch_id, shift_code)
            DO UPDATE SET min_staff = excluded.min_staff, max_staff = excluded.max_staff
            """,
            normalized,
        )
        conn.commit()
        conn.close()

        return jsonify({"message": "Staffing rules updated", "count": len(normalized)})

    @app.get("/api/manager/schedule")
    def manager_get_schedule():
        user, error = _get_user_from_token(roles={"manager"})
        if error:
            return error

        week_start = (request.args.get("week_start") or "").strip()
        if not week_start:
            return jsonify({"error": "week_start is required"}), 400

        conn = get_conn()
        rows = conn.execute(
            """
            SELECT ws.id,
                   ws.employee_id,
                   u.display_name AS employee_name,
                   ws.shift_code,
                                     ws.day_of_week,
                   ws.week_start,
                   ws.branch_id,
                   b.name AS branch_name
            FROM weekly_schedule ws
            JOIN users u ON u.id = ws.employee_id
            JOIN branches b ON b.id = ws.branch_id
            WHERE ws.week_start = ?
              AND ws.branch_id = ?
                        ORDER BY ws.day_of_week, ws.shift_code, u.display_name
            """,
            (week_start, user["branch_id"]),
        ).fetchall()
        conn.close()

        return jsonify([dict(row) for row in rows])

    @app.get("/api/manager/attendance-shifts/today")
    def manager_attendance_shifts_today():
        user, error = _get_user_from_token(roles={"manager"})
        if error:
            return error

        current_dt = datetime.now()
        week_start, day_of_week = _week_start_and_day_for_datetime(current_dt)

        conn = get_conn()
        rows = conn.execute(
            """
            SELECT ws.id AS schedule_id,
                   ws.week_start,
                   ws.day_of_week,
                   ws.shift_code,
                   ws.branch_id,
                   ws.employee_id,
                   u.display_name AS employee_name,
                   m.status,
                   m.source,
                   m.note,
                   m.updated_at,
                   m.marked_by_manager_id,
                   m.attendance_log_id
            FROM weekly_schedule ws
            JOIN users u ON u.id = ws.employee_id
            LEFT JOIN shift_attendance_marks m
                   ON m.week_start = ws.week_start
                  AND m.day_of_week = ws.day_of_week
                  AND m.shift_code = ws.shift_code
                  AND m.branch_id = ws.branch_id
                  AND m.employee_id = ws.employee_id
            WHERE ws.branch_id = ?
              AND ws.week_start = ?
              AND ws.day_of_week = ?
            ORDER BY ws.shift_code, u.display_name
            """,
            (user["branch_id"], week_start, day_of_week),
        ).fetchall()
        conn.close()

        items = []
        for row in rows:
            start_dt = _shift_start_datetime(row["week_start"], row["day_of_week"], row["shift_code"])
            if not start_dt:
                continue
            late_deadline_dt = start_dt + timedelta(minutes=15)
            status = row["status"]
            if not status:
                status = "late_unmarked" if current_dt > late_deadline_dt else "pending"

            items.append(
                {
                    "schedule_id": row["schedule_id"],
                    "week_start": row["week_start"],
                    "day_of_week": row["day_of_week"],
                    "shift_code": row["shift_code"],
                    "employee_id": row["employee_id"],
                    "employee_name": row["employee_name"],
                    "status": status,
                    "source": row["source"] or "",
                    "note": row["note"] or "",
                    "attendance_log_id": row["attendance_log_id"],
                    "shift_start_at": _format_db_datetime(start_dt),
                    "late_deadline_at": _format_db_datetime(late_deadline_dt),
                    "updated_at": row["updated_at"],
                }
            )

        return jsonify(
            {
                "server_now": _format_db_datetime(current_dt),
                "week_start": week_start,
                "day_of_week": day_of_week,
                "items": items,
            }
        )

    @app.put("/api/manager/attendance-shifts/override")
    def manager_attendance_shift_override():
        user, error = _get_user_from_token(roles={"manager"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        schedule_id = body.get("schedule_id")
        note = (body.get("note") or "").strip() or "Quản lý xác nhận đi làm đúng giờ (quên chấm công)"

        try:
            schedule_id = int(schedule_id)
        except (TypeError, ValueError):
            return jsonify({"error": "schedule_id is required and must be integer"}), 400

        conn = get_conn()
        schedule = conn.execute(
            """
            SELECT id, week_start, day_of_week, shift_code, branch_id, employee_id
            FROM weekly_schedule
            WHERE id = ?
              AND branch_id = ?
            LIMIT 1
            """,
            (schedule_id, user["branch_id"]),
        ).fetchone()
        if not schedule:
            conn.close()
            return jsonify({"error": "Schedule not found in your branch"}), 404

        _upsert_shift_attendance_mark(
            conn,
            week_start=schedule["week_start"],
            day_of_week=schedule["day_of_week"],
            shift_code=schedule["shift_code"],
            branch_id=schedule["branch_id"],
            employee_id=schedule["employee_id"],
            status="present_override",
            source="manager_override",
            note=note,
            marked_by_manager_id=user["id"],
        )
        conn.commit()
        conn.close()

        return jsonify({"message": "Đã cập nhật trạng thái: đã đi làm"})

    @app.get("/api/manager/self-preferences")
    def manager_self_preferences_get():
        user, error = _get_user_from_token(roles={"manager"})
        if error:
            return error

        week_start = (request.args.get("week_start") or "").strip()
        if not week_start:
            return jsonify({"error": "week_start is required"}), 400

        conn = get_conn()
        rows = conn.execute(
            """
            SELECT id, shift_code, day_of_week
            FROM shift_preferences
            WHERE employee_id = ?
              AND branch_id = ?
              AND week_start = ?
            ORDER BY day_of_week, shift_code
            """,
            (user["id"], user["branch_id"], week_start),
        ).fetchall()
        conn.close()
        return jsonify([dict(row) for row in rows])

    @app.put("/api/manager/self-preferences")
    def manager_self_preferences_put():
        user, error = _get_user_from_token(roles={"manager"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        week_start = (body.get("week_start") or "").strip()
        shifts = body.get("shift_codes") or []
        selections = body.get("selections") or []
        if not week_start:
            return jsonify({"error": "week_start is required"}), 400
        if not isinstance(shifts, list):
            return jsonify({"error": "shift_codes must be a list"}), 400
        if not isinstance(selections, list):
            return jsonify({"error": "selections must be a list"}), 400

        normalized = []
        seen = set()
        for item in selections:
            shift_code = item.get("shift_code")
            day_of_week = _normalize_day_of_week(item.get("day_of_week"))
            if shift_code not in SHIFT_CODE_SET:
                return jsonify({"error": f"Invalid shift code: {shift_code}"}), 400
            if day_of_week is None:
                return jsonify({"error": "day_of_week must be in range 1..7"}), 400
            key = (shift_code, day_of_week)
            if key in seen:
                continue
            seen.add(key)
            normalized.append((shift_code, day_of_week))

        if not normalized:
            # Backward compatibility with previous payload shape.
            for shift_code in shifts:
                if shift_code not in SHIFT_CODE_SET:
                    return jsonify({"error": f"Invalid shift code: {shift_code}"}), 400
                for day_of_week in range(1, 8):
                    key = (shift_code, day_of_week)
                    if key in seen:
                        continue
                    seen.add(key)
                    normalized.append((shift_code, day_of_week))

        conn = get_conn()
        conn.execute(
            "DELETE FROM shift_preferences WHERE employee_id = ? AND branch_id = ? AND week_start = ?",
            (user["id"], user["branch_id"], week_start),
        )
        if normalized:
            conn.executemany(
                """
                INSERT INTO shift_preferences(employee_id, week_start, branch_id, shift_code, day_of_week)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (user["id"], week_start, user["branch_id"], shift_code, day_of_week)
                    for shift_code, day_of_week in normalized
                ],
            )
        conn.commit()
        conn.close()
        return jsonify({"message": "Saved manager shift preferences", "count": len(normalized)})

    @app.get("/api/manager/issues")
    def manager_issues():
        user, error = _get_user_from_token(roles={"manager"})
        if error:
            return error

        conn = get_conn()
        rows = conn.execute(
            """
            SELECT i.id,
                   i.title,
                   i.details,
                   i.status,
                   i.escalated_to_ceo,
                   i.manager_note,
                   i.created_at,
                   i.updated_at,
                   u.display_name AS reporter_name,
                   u.role AS reporter_role,
                   COALESCE(b.name, '-') AS branch_name
            FROM issue_reports i
            JOIN users u ON u.id = i.reporter_id
            LEFT JOIN branches b ON b.id = i.branch_id
            WHERE i.branch_id = ?
            ORDER BY i.id DESC
            """,
            (user["branch_id"],),
        ).fetchall()
        conn.close()
        return jsonify([dict(row) for row in rows])

    @app.put("/api/manager/issues/<int:issue_id>")
    def manager_issue_update(issue_id):
        user, error = _get_user_from_token(roles={"manager"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        status = body.get("status")
        manager_note = (body.get("manager_note") or "").strip() or None
        escalate = 1 if body.get("escalate_to_ceo", False) else 0
        allowed = {"open", "in_review", "escalated", "resolved"}
        if status not in allowed:
            return jsonify({"error": "Invalid status"}), 400

        conn = get_conn()
        issue = conn.execute(
            "SELECT id, branch_id FROM issue_reports WHERE id = ?",
            (issue_id,),
        ).fetchone()
        if not issue:
            conn.close()
            return jsonify({"error": "Issue not found"}), 404
        if issue["branch_id"] != user["branch_id"]:
            conn.close()
            return jsonify({"error": "Forbidden for this branch"}), 403

        final_status = "escalated" if escalate else status
        conn.execute(
            """
            UPDATE issue_reports
            SET status = ?,
                escalated_to_ceo = ?,
                manager_note = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (final_status, escalate, manager_note, issue_id),
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "Issue updated"})

    @app.get("/api/manager/payroll-export.csv")
    def manager_payroll_export():
        user, error = _get_user_from_token(roles={"manager"})
        if error:
            return error

        week_start = (request.args.get("week_start") or "").strip()
        if not week_start:
            return jsonify({"error": "week_start is required"}), 400

        conn = get_conn()
        rows = _weekly_hours_rows(conn, week_start, branch_id=user["branch_id"])
        conn.close()
        csv_rows = [
            [
                item["employee_id"],
                item["username"],
                item["employee_name"],
                item["branch_name"],
                round(item["total_minutes"] / 60, 2),
                item["attendance_sessions"],
                week_start,
            ]
            for item in rows
        ]
        return _csv_response(
            filename=f"payroll_branch_{user['branch_id']}_{week_start}.csv",
            headers=[
                "employee_id",
                "username",
                "employee_name",
                "branch_name",
                "hours_worked",
                "attendance_sessions",
                "week_start",
            ],
            rows=csv_rows,
        )

    @app.get("/api/manager/employees")
    def manager_list_employees():
        user, error = _get_user_from_token(roles={"manager"})
        if error:
            return error

        keyword = (request.args.get("q") or "").strip()

        conn = get_conn()
        sql = """
            SELECT u.id,
                   u.username,
                   u.display_name,
                   u.full_name,
                     u.avatar_data_url,
                   u.phone_number,
                   u.address,
                   u.date_of_birth,
                   u.job_position,
                   u.is_active,
                   GROUP_CONCAT(b.name, ', ') AS branch_names,
                   GROUP_CONCAT(eba.branch_id, ',') AS branch_ids
            FROM users u
            JOIN employee_branch_access eba ON eba.employee_id = u.id
            JOIN branches b ON b.id = eba.branch_id
            WHERE u.role = 'employee'
              AND u.id IN (
                  SELECT employee_id FROM employee_branch_access WHERE branch_id = ?
              )
        """
        params = [user["branch_id"]]
        if keyword:
            sql += """
              AND (
                          u.username LIKE ? COLLATE NOCASE
                      OR u.display_name LIKE ? COLLATE NOCASE
                      OR COALESCE(u.full_name, '') LIKE ? COLLATE NOCASE
                      OR COALESCE(u.phone_number, '') LIKE ? COLLATE NOCASE
              )
            """
            like_kw = f"%{keyword}%"
            params.extend([like_kw, like_kw, like_kw, like_kw])

        sql += """
            GROUP BY u.id,
                     u.username,
                     u.display_name,
                     u.full_name,
                     u.avatar_data_url,
                     u.phone_number,
                     u.address,
                     u.date_of_birth,
                     u.job_position,
                     u.is_active
            ORDER BY u.display_name
        """
        rows = conn.execute(sql, tuple(params)).fetchall()

        branch_rows = conn.execute(
            """
            SELECT b.id, b.name
            FROM branches b
            WHERE b.id = ?
            ORDER BY b.name
            """,
            (user["branch_id"],),
        ).fetchall()

        conn.close()

        employees = []
        for row in rows:
            item = dict(row)
            item["branch_names"] = [name.strip() for name in (item.get("branch_names") or "").split(",") if name.strip()]
            item["branch_ids"] = [
                int(branch_id)
                for branch_id in (item.get("branch_ids") or "").split(",")
                if branch_id.strip().isdigit()
            ]
            item["contact_ready"] = bool((item.get("phone_number") or "").strip())
            employees.append(item)

        return jsonify(
            {
                "employees": employees,
                "branches": [dict(row) for row in branch_rows],
                "default_branch_ids": [user["branch_id"]],
            }
        )

    @app.post("/api/manager/employees")
    def manager_create_employee():
        user, error = _get_user_from_token(roles={"manager"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        username = (body.get("username") or "").strip()
        display_name = (body.get("display_name") or "").strip()
        password = body.get("password") or ""
        branch_ids = body.get("branch_ids") or [user["branch_id"]]

        if not username or not display_name:
            return jsonify({"error": "username and display_name are required"}), 400
        if len(password) < 6:
            return jsonify({"error": "password must be at least 6 characters"}), 400
        if not isinstance(branch_ids, list) or not branch_ids:
            return jsonify({"error": "branch_ids must be a non-empty list"}), 400

        try:
            normalized_branch_ids = sorted({int(branch_id) for branch_id in branch_ids})
        except (TypeError, ValueError):
            return jsonify({"error": "branch_ids must contain valid integer ids"}), 400
        if normalized_branch_ids != [user["branch_id"]]:
            return jsonify({"error": "Manager can only create employee in own branch scope"}), 403

        conn = get_conn()
        existing = conn.execute(
            "SELECT 1 FROM users WHERE username = ?",
            (username,),
        ).fetchone()
        if existing:
            conn.close()
            return jsonify({"error": "username already exists"}), 409

        valid_branch_count = conn.execute(
            f"SELECT COUNT(*) AS c FROM branches WHERE id IN ({','.join(['?'] * len(normalized_branch_ids))})",
            tuple(normalized_branch_ids),
        ).fetchone()["c"]
        if valid_branch_count != len(normalized_branch_ids):
            conn.close()
            return jsonify({"error": "Some branch_ids are invalid"}), 400

        cur = conn.execute(
            """
            INSERT INTO users(username, display_name, role, branch_id, password_hash, is_active)
            VALUES (?, ?, 'employee', NULL, ?, 1)
            """,
            (username, display_name, generate_password_hash(password)),
        )
        employee_id = cur.lastrowid

        conn.executemany(
            "INSERT INTO employee_branch_access(employee_id, branch_id) VALUES (?, ?)",
            [(employee_id, branch_id) for branch_id in normalized_branch_ids],
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "Employee account created", "employee_id": employee_id}), 201

    @app.put("/api/manager/employees/<int:employee_id>")
    def manager_update_employee(employee_id):
        user, error = _get_user_from_token(roles={"manager"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        display_name = (body.get("display_name") or "").strip()
        full_name = (body.get("full_name") or "").strip()
        phone_number = (body.get("phone_number") or "").strip()
        address = (body.get("address") or "").strip()
        date_of_birth = (body.get("date_of_birth") or "").strip()
        job_position = (body.get("job_position") or "").strip()

        if not display_name:
            return jsonify({"error": "Tên hiển thị không được để trống"}), 400
        if not full_name:
            return jsonify({"error": "Họ tên không được để trống"}), 400
        if len(display_name) > 80:
            return jsonify({"error": "Tên hiển thị quá dài"}), 400
        if len(full_name) > 120:
            return jsonify({"error": "Họ tên quá dài"}), 400
        if phone_number and not re.fullmatch(r"\+?[0-9]{9,15}", phone_number):
            return jsonify({"error": "Số điện thoại không hợp lệ"}), 400
        if date_of_birth:
            try:
                datetime.strptime(date_of_birth, "%Y-%m-%d")
            except ValueError:
                return jsonify({"error": "Ngày sinh không đúng định dạng YYYY-MM-DD"}), 400
        if len(address) > 255:
            return jsonify({"error": "Địa chỉ quá dài"}), 400
        if len(job_position) > 120:
            return jsonify({"error": "Vị trí công việc quá dài"}), 400

        conn = get_conn()
        target = conn.execute(
            "SELECT id, role FROM users WHERE id = ?",
            (employee_id,),
        ).fetchone()
        if not target:
            conn.close()
            return jsonify({"error": "User not found"}), 404
        if target["role"] != "employee":
            conn.close()
            return jsonify({"error": "Manager can only edit employee accounts"}), 400
        if not _manager_can_manage_employee(conn, user["branch_id"], employee_id):
            conn.close()
            return jsonify({"error": "You can only edit employees in your branch scope"}), 403

        conn.execute(
            """
            UPDATE users
            SET display_name = ?,
                full_name = ?,
                phone_number = ?,
                address = ?,
                date_of_birth = ?,
                job_position = ?
            WHERE id = ?
            """,
            (
                display_name,
                full_name,
                phone_number or None,
                address or None,
                date_of_birth or None,
                job_position or None,
                employee_id,
            ),
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "Employee profile updated"})

    @app.delete("/api/manager/employees/<int:employee_id>")
    def manager_delete_employee(employee_id):
        user, error = _get_user_from_token(roles={"manager"})
        if error:
            return error

        conn = get_conn()
        target = conn.execute(
            "SELECT id, role, display_name FROM users WHERE id = ?",
            (employee_id,),
        ).fetchone()
        if not target:
            conn.close()
            return jsonify({"error": "User not found"}), 404
        if target["role"] != "employee":
            conn.close()
            return jsonify({"error": "Manager can only delete employee accounts"}), 400
        if not _manager_can_manage_employee(conn, user["branch_id"], employee_id):
            conn.close()
            return jsonify({"error": "You can only delete employees in your branch scope"}), 403

        conn.execute("DELETE FROM users WHERE id = ?", (employee_id,))
        conn.commit()
        conn.close()
        return jsonify({"message": "Employee account deleted"})

    @app.get("/api/ceo/chat")
    def get_ceo_chat():
        _, error = _get_user_from_token(roles={"ceo"})
        if error:
            return error

        conn = get_conn()
        rows = conn.execute(
            """
            SELECT m.id,
                   m.message,
                   m.created_at,
                   m.sender_type,
                   COALESCE(m.sender_label, u.display_name, 'Unknown') AS sender_name
            FROM ceo_chat_messages m
            LEFT JOIN users u ON u.id = m.sender_id
            ORDER BY m.id DESC
            LIMIT 200
            """
        ).fetchall()
        conn.close()
        return jsonify([dict(row) for row in reversed(rows)])

    @app.get("/api/ceo/issues")
    def ceo_issues():
        _, error = _get_user_from_token(roles={"ceo"})
        if error:
            return error

        conn = get_conn()
        rows = conn.execute(
            """
            SELECT i.id,
                   i.title,
                   i.details,
                   i.status,
                   i.escalated_to_ceo,
                   i.manager_note,
                   i.created_at,
                   i.updated_at,
                   u.display_name AS reporter_name,
                   u.role AS reporter_role,
                   COALESCE(b.name, '-') AS branch_name
            FROM issue_reports i
            JOIN users u ON u.id = i.reporter_id
            LEFT JOIN branches b ON b.id = i.branch_id
            WHERE i.escalated_to_ceo = 1 OR i.status = 'escalated'
            ORDER BY i.id DESC
            """
        ).fetchall()
        conn.close()
        return jsonify([dict(row) for row in rows])

    @app.get("/api/ceo/payroll-export.csv")
    def ceo_payroll_export():
        _, error = _get_user_from_token(roles={"ceo"})
        if error:
            return error

        week_start = (request.args.get("week_start") or "").strip()
        if not week_start:
            return jsonify({"error": "week_start is required"}), 400

        branch_id_raw = (request.args.get("branch_id") or "").strip()
        branch_id = None
        if branch_id_raw:
            try:
                branch_id = int(branch_id_raw)
            except ValueError:
                return jsonify({"error": "branch_id must be an integer"}), 400

        conn = get_conn()
        branch_label = "all"
        if branch_id is not None:
            branch = conn.execute(
                "SELECT id, name FROM branches WHERE id = ?",
                (branch_id,),
            ).fetchone()
            if not branch:
                conn.close()
                return jsonify({"error": "Branch not found"}), 404
            branch_label = str(branch["id"])

        rows = _weekly_hours_rows(conn, week_start, branch_id=branch_id)
        conn.close()
        csv_rows = [
            [
                item["employee_id"],
                item["username"],
                item["employee_name"],
                item["branch_name"],
                round(item["total_minutes"] / 60, 2),
                item["attendance_sessions"],
                week_start,
            ]
            for item in rows
        ]
        return _csv_response(
            filename=f"payroll_{branch_label}_{week_start}.csv",
            headers=[
                "employee_id",
                "username",
                "employee_name",
                "branch_scope",
                "hours_worked",
                "attendance_sessions",
                "week_start",
            ],
            rows=csv_rows,
        )

    @app.post("/api/ceo/chat")
    def post_ceo_chat():
        user, error = _get_user_from_token(roles={"ceo"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        message = (body.get("message") or "").strip()
        if not message:
            return jsonify({"error": "message is required"}), 400

        conn = get_conn()
        conn.execute(
            """
            INSERT INTO ceo_chat_messages(sender_id, sender_type, sender_label, message)
            VALUES (?, 'user', ?, ?)
            """,
            (user["id"], user["display_name"], message),
        )

        if should_trigger_jarvis(message):
            jarvis_message = generate_hr_anomaly_report(conn, message)
            conn.execute(
                """
                INSERT INTO ceo_chat_messages(sender_id, sender_type, sender_label, message)
                VALUES (?, 'jarvis', 'Tro ly tong hop', ?)
                """,
                (user["id"], jarvis_message),
            )

        conn.commit()
        conn.close()
        return jsonify({"message": "Message sent"}), 201

    @app.get("/api/admin/users")
    def admin_users():
        _, error = _get_user_from_token(roles={"ceo"})
        if error:
            return error

        conn = get_conn()
        users = conn.execute(
            """
            SELECT u.id,
                   u.username,
                   u.display_name,
                   u.role,
                   u.branch_id,
                   u.is_active,
                   b.name AS branch_name
            FROM users u
            LEFT JOIN branches b ON b.id = u.branch_id
            ORDER BY u.role, u.display_name
            """
        ).fetchall()
        branches = conn.execute("SELECT id, name FROM branches ORDER BY name").fetchall()
        conn.close()
        return jsonify(
            {
                "users": [dict(row) for row in users],
                "branches": [dict(row) for row in branches],
            }
        )

    @app.post("/api/admin/users")
    def admin_create_user():
        actor, error = _get_user_from_token(roles={"ceo"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        username = (body.get("username") or "").strip()
        display_name = (body.get("display_name") or "").strip()
        password = body.get("password") or ""
        role = (body.get("role") or "").strip()
        branch_id = body.get("branch_id")
        branch_ids = body.get("branch_ids") or []

        if not username or not display_name:
            return jsonify({"error": "username and display_name are required"}), 400
        if len(password) < 6:
            return jsonify({"error": "password must be at least 6 characters"}), 400
        if role not in {"employee", "manager"}:
            return jsonify({"error": "role must be employee or manager"}), 400

        conn = get_conn()
        exists = conn.execute("SELECT 1 FROM users WHERE username = ?", (username,)).fetchone()
        if exists:
            conn.close()
            return jsonify({"error": "username already exists"}), 409

        if role == "manager":
            try:
                branch_id = int(branch_id)
            except (TypeError, ValueError):
                conn.close()
                return jsonify({"error": "branch_id is required for manager role"}), 400

            valid_branch = conn.execute("SELECT id, name FROM branches WHERE id = ?", (branch_id,)).fetchone()
            if not valid_branch:
                conn.close()
                return jsonify({"error": "Invalid branch_id"}), 400

            cur = conn.execute(
                """
                INSERT INTO users(username, display_name, role, branch_id, password_hash, is_active)
                VALUES (?, ?, 'manager', ?, ?, 1)
                """,
                (username, display_name, branch_id, generate_password_hash(password)),
            )
            _create_audit_log(
                conn,
                actor,
                action="user.create",
                target_type="user",
                target_id=cur.lastrowid,
                details=(
                    f"Created manager username={username}, display_name={display_name}, "
                    f"branch_id={branch_id}"
                ),
            )
            conn.commit()
            conn.close()
            return jsonify({"message": "User created", "user_id": cur.lastrowid}), 201

        if not isinstance(branch_ids, list) or not branch_ids:
            if branch_id is not None:
                branch_ids = [branch_id]
            else:
                conn.close()
                return jsonify({"error": "branch_ids is required for employee role"}), 400

        try:
            normalized_branch_ids = sorted({int(item) for item in branch_ids})
        except (TypeError, ValueError):
            conn.close()
            return jsonify({"error": "branch_ids must contain integer ids"}), 400

        valid_count = conn.execute(
            f"SELECT COUNT(*) AS c FROM branches WHERE id IN ({','.join(['?'] * len(normalized_branch_ids))})",
            tuple(normalized_branch_ids),
        ).fetchone()["c"]
        if valid_count != len(normalized_branch_ids):
            conn.close()
            return jsonify({"error": "Some branch_ids are invalid"}), 400

        cur = conn.execute(
            """
            INSERT INTO users(username, display_name, role, branch_id, password_hash, is_active)
            VALUES (?, ?, 'employee', NULL, ?, 1)
            """,
            (username, display_name, generate_password_hash(password)),
        )
        employee_id = cur.lastrowid

        conn.executemany(
            "INSERT INTO employee_branch_access(employee_id, branch_id) VALUES (?, ?)",
            [(employee_id, bid) for bid in normalized_branch_ids],
        )
        _create_audit_log(
            conn,
            actor,
            action="user.create",
            target_type="user",
            target_id=employee_id,
            details=(
                f"Created employee username={username}, display_name={display_name}, "
                f"branch_ids={','.join(str(bid) for bid in normalized_branch_ids)}"
            ),
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "User created", "user_id": employee_id}), 201

    @app.put("/api/admin/users/<int:user_id>")
    def admin_update_user(user_id):
        actor, error = _get_user_from_token(roles={"ceo"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        role = body.get("role")
        is_active = 1 if body.get("is_active", True) else 0
        branch_id = body.get("branch_id")
        allowed_roles = {"employee", "manager", "ceo"}
        if role not in allowed_roles:
            return jsonify({"error": "Invalid role"}), 400

        conn = get_conn()
        target = conn.execute(
            "SELECT id, role FROM users WHERE id = ?", (user_id,)
        ).fetchone()
        if not target:
            conn.close()
            return jsonify({"error": "User not found"}), 404
        if target["id"] == actor["id"] and role != "ceo":
            conn.close()
            return jsonify({"error": "CEO cannot remove own CEO role"}), 400

        if role != "manager":
            branch_id = None

        conn.execute(
            "UPDATE users SET role = ?, is_active = ?, branch_id = ? WHERE id = ?",
            (role, is_active, branch_id, user_id),
        )

        if role != "employee":
            conn.execute("DELETE FROM employee_branch_access WHERE employee_id = ?", (user_id,))

        conn.commit()
        conn.close()
        return jsonify({"message": "User updated"})

    @app.delete("/api/admin/users/<int:user_id>")
    def admin_delete_user(user_id):
        actor, error = _get_user_from_token(roles={"ceo"})
        if error:
            return error

        conn = get_conn()
        target = conn.execute(
            "SELECT id, username, display_name, role FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()
        if not target:
            conn.close()
            return jsonify({"error": "User not found"}), 404

        if target["id"] == actor["id"]:
            conn.close()
            return jsonify({"error": "CEO cannot delete own account"}), 400

        if target["role"] not in {"employee", "manager"}:
            conn.close()
            return jsonify({"error": "Only employee/manager accounts can be deleted"}), 400

        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
        _create_audit_log(
            conn,
            actor,
            action="user.delete",
            target_type="user",
            target_id=user_id,
            details=(
                f"Deleted user username={target['username']}, "
                f"display_name={target['display_name']}, role={target['role']}"
            ),
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "User deleted"})

    @app.get("/api/admin/branches")
    def admin_list_branches():
        _, error = _get_user_from_token(roles={"ceo"})
        if error:
            return error

        page, page_size, parse_error = _parse_pagination(default_page=1, default_page_size=8)
        if parse_error:
            return parse_error

        query = (request.args.get("q") or "").strip()
        where_clause = ""
        params = []
        if query:
            where_clause = "WHERE LOWER(b.name) LIKE ?"
            params.append(f"%{query.lower()}%")

        conn = get_conn()
        total = conn.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM branches b
            {where_clause}
            """,
            tuple(params),
        ).fetchone()["c"]

        offset = (page - 1) * page_size
        rows = conn.execute(
            f"""
            SELECT b.id,
                   b.name,
                     b.location,
                     b.network_ip,
                   (SELECT COUNT(*) FROM users u WHERE u.role = 'manager' AND u.branch_id = b.id) AS manager_count,
                   (SELECT COUNT(DISTINCT eba.employee_id) FROM employee_branch_access eba WHERE eba.branch_id = b.id) AS employee_count
            FROM branches b
            {where_clause}
            ORDER BY b.name
            LIMIT ? OFFSET ?
            """,
            tuple(params + [page_size, offset]),
        ).fetchall()
        conn.close()

        return jsonify(
            {
                "items": [dict(row) for row in rows],
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total": total,
                    "total_pages": max(1, (total + page_size - 1) // page_size),
                },
                "query": query,
            }
        )

    @app.post("/api/admin/branches")
    def admin_create_branch():
        actor, error = _get_user_from_token(roles={"ceo"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        name = (body.get("name") or "").strip()
        location = (body.get("location") or "").strip() or None
        network_ip = (body.get("network_ip") or "").strip() or None
        if not name:
            return jsonify({"error": "name is required"}), 400
        if network_ip and not _is_valid_ipv4(network_ip):
            return jsonify({"error": "network_ip must be a valid IPv4 address"}), 400

        conn = get_conn()
        exists = conn.execute("SELECT 1 FROM branches WHERE LOWER(name) = LOWER(?)", (name,)).fetchone()
        if exists:
            conn.close()
            return jsonify({"error": "Branch name already exists"}), 409

        cur = conn.execute(
            "INSERT INTO branches(name, location, network_ip) VALUES (?, ?, ?)",
            (name, location, network_ip),
        )
        _create_audit_log(
            conn,
            actor,
            action="branch.create",
            target_type="branch",
            target_id=cur.lastrowid,
            details=f"Created branch: {name} | location: {location or '-'}",
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "Branch created", "branch_id": cur.lastrowid}), 201

    @app.put("/api/admin/branches/<int:branch_id>")
    def admin_update_branch(branch_id):
        actor, error = _get_user_from_token(roles={"ceo"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        name = (body.get("name") or "").strip()
        location = (body.get("location") or "").strip() or None
        network_ip = (body.get("network_ip") or "").strip() or None
        if not name:
            return jsonify({"error": "name is required"}), 400
        if network_ip and not _is_valid_ipv4(network_ip):
            return jsonify({"error": "network_ip must be a valid IPv4 address"}), 400

        conn = get_conn()
        branch = conn.execute(
            "SELECT id, name, location, network_ip FROM branches WHERE id = ?",
            (branch_id,),
        ).fetchone()
        if not branch:
            conn.close()
            return jsonify({"error": "Branch not found"}), 404

        dup = conn.execute(
            "SELECT 1 FROM branches WHERE LOWER(name) = LOWER(?) AND id != ?",
            (name, branch_id),
        ).fetchone()
        if dup:
            conn.close()
            return jsonify({"error": "Branch name already exists"}), 409

        conn.execute(
            "UPDATE branches SET name = ?, location = ?, network_ip = ? WHERE id = ?",
            (name, location, network_ip, branch_id),
        )
        _create_audit_log(
            conn,
            actor,
            action="branch.update",
            target_type="branch",
            target_id=branch_id,
            details=(
                "Updated branch "
                f"from name='{branch['name']}', location='{branch['location'] or '-'}' "
                f"to name='{name}', location='{location or '-'}', network_ip='{network_ip or '-'}'"
            ),
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "Branch updated"})

    @app.get("/api/admin/branches/<int:branch_id>/employees")
    def admin_branch_employees(branch_id):
        _, error = _get_user_from_token(roles={"ceo"})
        if error:
            return error

        conn = get_conn()
        branch = conn.execute(
            "SELECT id, name, location, network_ip FROM branches WHERE id = ?",
            (branch_id,),
        ).fetchone()
        if not branch:
            conn.close()
            return jsonify({"error": "Branch not found"}), 404

        employee_rows = conn.execute(
            """
            SELECT u.id,
                   u.username,
                   u.display_name,
                   u.is_active
            FROM users u
            JOIN employee_branch_access eba ON eba.employee_id = u.id
            WHERE u.role = 'employee'
              AND eba.branch_id = ?
            ORDER BY u.display_name
            """,
            (branch_id,),
        ).fetchall()

        manager_rows = conn.execute(
            """
            SELECT u.id,
                   u.username,
                   u.display_name,
                   u.is_active
            FROM users u
            WHERE u.role = 'manager'
              AND u.branch_id = ?
            ORDER BY u.display_name
            """,
            (branch_id,),
        ).fetchall()
        conn.close()

        return jsonify(
            {
                "branch": dict(branch),
                "managers": [dict(row) for row in manager_rows],
                "employees": [dict(row) for row in employee_rows],
            }
        )

    @app.delete("/api/admin/branches/<int:branch_id>")
    def admin_delete_branch(branch_id):
        actor, error = _get_user_from_token(roles={"ceo"})
        if error:
            return error

        conn = get_conn()
        branch = conn.execute("SELECT id, name FROM branches WHERE id = ?", (branch_id,)).fetchone()
        if not branch:
            conn.close()
            return jsonify({"error": "Branch not found"}), 404

        has_manager = conn.execute(
            "SELECT 1 FROM users WHERE role = 'manager' AND branch_id = ? LIMIT 1",
            (branch_id,),
        ).fetchone()
        has_employee_access = conn.execute(
            "SELECT 1 FROM employee_branch_access WHERE branch_id = ? LIMIT 1",
            (branch_id,),
        ).fetchone()
        has_schedule = conn.execute(
            "SELECT 1 FROM weekly_schedule WHERE branch_id = ? LIMIT 1",
            (branch_id,),
        ).fetchone()

        if has_manager or has_employee_access or has_schedule:
            conn.close()
            return jsonify(
                {
                    "error": "Cannot delete branch with managers/employees/schedules. Reassign data first."
                }
            ), 400

        conn.execute("DELETE FROM branches WHERE id = ?", (branch_id,))
        _create_audit_log(
            conn,
            actor,
            action="branch.delete",
            target_type="branch",
            target_id=branch_id,
            details=f"Deleted branch: {branch['name']}",
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "Branch deleted"})

    @app.get("/api/admin/branch-audit-logs")
    def admin_branch_audit_logs():
        _, error = _get_user_from_token(roles={"ceo"})
        if error:
            return error

        page, page_size, parse_error = _parse_pagination(default_page=1, default_page_size=10)
        if parse_error:
            return parse_error

        branch_id_raw = (request.args.get("branch_id") or "").strip()
        branch_id = None
        if branch_id_raw:
            try:
                branch_id = int(branch_id_raw)
            except ValueError:
                return jsonify({"error": "branch_id must be an integer"}), 400

        where_clause = "WHERE al.target_type = 'branch'"
        params = []
        if branch_id is not None:
            where_clause += " AND al.target_id = ?"
            params.append(branch_id)

        conn = get_conn()
        total = conn.execute(
            f"SELECT COUNT(*) AS c FROM audit_logs al {where_clause}",
            tuple(params),
        ).fetchone()["c"]

        offset = (page - 1) * page_size
        rows = conn.execute(
            f"""
            SELECT al.id,
                   al.actor_user_id,
                   al.actor_username,
                   al.action,
                   al.target_id,
                   al.details,
                   al.created_at
            FROM audit_logs al
            {where_clause}
            ORDER BY al.id DESC
            LIMIT ? OFFSET ?
            """,
            tuple(params + [page_size, offset]),
        ).fetchall()
        conn.close()

        return jsonify(
            {
                "items": [dict(row) for row in rows],
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total": total,
                    "total_pages": max(1, (total + page_size - 1) // page_size),
                },
            }
        )

    @app.get("/")
    def root():
        return send_from_directory(FRONTEND_DIR, "index.html")

    @app.get("/api/current-user")
    def current_user():
        user, error = _get_user_from_token(required=True)
        if error:
            return error

        payload = dict(user)
        if payload["role"] == "manager" and payload["branch_id"]:
            conn = get_conn()
            branch = conn.execute(
                "SELECT id, name FROM branches WHERE id = ?", (payload["branch_id"],)
            ).fetchone()
            conn.close()
            payload["branch"] = dict(branch) if branch else None
        return jsonify({"user": payload})

    @app.get("/api/server-time")
    def server_time():
        return jsonify({"iso": datetime.utcnow().isoformat() + "Z"})

    return app


if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=5000, debug=True)
