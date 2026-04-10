import os
from datetime import datetime, timedelta
from collections import deque
from threading import Lock
from typing import Deque, Dict

from flask import jsonify, request


def register_attendance_routes(app, deps):
    get_conn = deps["get_conn"]
    get_user_from_token = deps["_get_user_from_token"]
    week_range = deps["_week_range"]
    format_db_datetime = deps["_format_db_datetime"]
    parse_db_datetime = deps["_parse_db_datetime"]
    resolve_today_shift_for_checkin = deps["_resolve_today_shift_for_checkin"]
    upsert_shift_attendance_mark = deps["_upsert_shift_attendance_mark"]
    cleanup_one_time_qr_codes = deps["_cleanup_one_time_qr_codes"]
    get_client_ip = deps["_get_client_ip"]
    is_branch_ip_allowed = deps["_is_branch_ip_allowed"]
    verify_attendance_qr_token = deps["_verify_attendance_qr_token"]
    parse_attendance_qr_payload = deps["_parse_attendance_qr_payload"]
    build_attendance_qr_token = deps["_build_attendance_qr_token"]
    build_static_branch_qr_payload = deps["_build_static_branch_qr_payload"]
    build_qr_image_data_url = deps["_build_qr_image_data_url"]
    generate_one_time_attendance_code = deps["_generate_one_time_attendance_code"]
    shift_definitions = deps["SHIFT_DEFINITIONS"]

    shift_definition_map = {item["code"]: item for item in shift_definitions}
    auto_checkout_grace_minutes = 10

    attendance_qr_one_time_ttl_seconds = deps["ATTENDANCE_QR_ONE_TIME_TTL_SECONDS"]
    attendance_qr_enabled = deps.get("ATTENDANCE_QR_ENABLED", True)
    qr_rate_limit_lock = Lock()
    qr_scan_attempts = {}
    qr_checkin_attempts = {}
    qr_rate_limit_window_seconds = 60
    qr_scan_limit_per_window = 25
    qr_checkin_limit_per_window = 15

    def _prune_bucket_window(bucket: Dict[str, Deque[int]], key: str, now_ts: int) -> Deque[int]:
        attempts = bucket.get(key)
        if attempts is None:
            attempts = deque()
            bucket[key] = attempts
        window_start = now_ts - qr_rate_limit_window_seconds
        while attempts and attempts[0] < window_start:
            attempts.popleft()
        return attempts

    def _is_rate_limited(bucket: Dict[str, Deque[int]], key: str, limit: int) -> bool:
        now_ts = int(datetime.utcnow().timestamp())
        with qr_rate_limit_lock:
            attempts = _prune_bucket_window(bucket, key, now_ts)
            return len(attempts) >= limit

    def _record_rate_attempt(bucket: Dict[str, Deque[int]], key: str) -> None:
        now_ts = int(datetime.utcnow().timestamp())
        with qr_rate_limit_lock:
            attempts = _prune_bucket_window(bucket, key, now_ts)
            attempts.append(now_ts)

    def _log_attendance_confirmation(conn, attendance_id, employee_id, branch_id, source, note=None):
        confirmed_at = format_db_datetime(datetime.now())
        conn.execute(
            """
            INSERT INTO attendance_confirm_logs(attendance_log_id, employee_id, branch_id, confirmed_at, source, note)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (attendance_id, employee_id, branch_id, confirmed_at, source, note),
        )
        conn.execute(
            "UPDATE attendance_logs SET confirmed_at = ? WHERE id = ?",
            (confirmed_at, attendance_id),
        )

    def _ensure_attendance_qr_enabled():
        if attendance_qr_enabled:
            return None
        return (
            jsonify(
                {
                    "error": "Attendance QR is disabled because ATTENDANCE_QR_SECRET is missing or weak on Vercel",
                    "required_env": "ATTENDANCE_QR_SECRET",
                }
            ),
            503,
        )

    def _week_start_and_day_for_datetime(current_dt):
        monday_dt = current_dt - timedelta(days=current_dt.weekday())
        return monday_dt.strftime("%Y-%m-%d"), current_dt.weekday() + 1

    def _normalize_hhmm(value):
        text = (value or "").strip()
        if len(text) != 5 or text[2] != ":":
            return None
        hour_text = text[:2]
        minute_text = text[3:]
        if not hour_text.isdigit() or not minute_text.isdigit():
            return None
        hour = int(hour_text)
        minute = int(minute_text)
        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            return None
        return f"{hour:02d}:{minute:02d}"

    def _build_daytime_datetime(week_start, day_of_week, hhmm_value):
        text = _normalize_hhmm(hhmm_value)
        if not text:
            return None
        day_dt = datetime.strptime(week_start, "%Y-%m-%d") + timedelta(days=int(day_of_week) - 1)
        hour, minute = [int(part) for part in text.split(":")]
        return day_dt.replace(hour=hour, minute=minute, second=0, microsecond=0)

    def _resolve_shift_window_for_log(conn, attendance_log):
        if attendance_log is None:
            return None, None, None

        def _row_value(row_obj, key):
            if isinstance(row_obj, dict):
                return row_obj.get(key)
            try:
                return row_obj[key]
            except (KeyError, TypeError, IndexError):
                return None

        check_in_raw = _row_value(attendance_log, "check_in_at")
        branch_id = _row_value(attendance_log, "branch_id")
        employee_id = _row_value(attendance_log, "employee_id")
        if not check_in_raw or not branch_id or not employee_id:
            return None, None, None

        try:
            check_in_dt = parse_db_datetime(check_in_raw)
        except (TypeError, ValueError):
            return None, None, None

        week_start, day_of_week = _week_start_and_day_for_datetime(check_in_dt)
        rows = conn.execute(
            """
            SELECT shift_code, week_start, day_of_week, flexible_start_at, flexible_end_at
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
            return None, None, None

        candidates = []
        for row in rows:
            shift_code = row["shift_code"]
            shift_def = shift_definition_map.get(shift_code)
            if not shift_def:
                continue

            if shift_code == "FLEX":
                start_hhmm = row["flexible_start_at"] or shift_def.get("start")
                end_hhmm = row["flexible_end_at"] or shift_def.get("end")
            else:
                start_hhmm = shift_def.get("start")
                end_hhmm = shift_def.get("end")

            shift_start_dt = _build_daytime_datetime(row["week_start"], row["day_of_week"], start_hhmm)
            shift_end_dt = _build_daytime_datetime(row["week_start"], row["day_of_week"], end_hhmm)
            if not shift_start_dt or not shift_end_dt:
                continue
            if shift_end_dt <= shift_start_dt:
                shift_end_dt += timedelta(days=1)

            in_window = shift_start_dt - timedelta(hours=2) <= check_in_dt <= shift_end_dt + timedelta(hours=2)
            score = (0 if in_window else 1, abs((check_in_dt - shift_start_dt).total_seconds()))
            candidates.append((score, shift_code, shift_start_dt, shift_end_dt))

        if not candidates:
            return None, None, None

        candidates.sort(key=lambda item: item[0])
        _, shift_code, shift_start_dt, shift_end_dt = candidates[0]
        return shift_code, shift_start_dt, shift_end_dt

    def _create_auto_checkout_notice(conn, attendance_log, shift_code, check_out_raw):
        employee_id = int(attendance_log["employee_id"])
        branch_id = attendance_log["branch_id"]
        check_in_raw = attendance_log["check_in_at"]

        manager_row = conn.execute(
            """
            SELECT id
            FROM users
            WHERE role = 'manager'
              AND branch_id = ?
              AND COALESCE(is_active, 1) = 1
            ORDER BY id
            LIMIT 1
            """,
            (branch_id,),
        ).fetchone()

        reporter_id = int(manager_row["id"]) if manager_row else employee_id
        reporter_role = "manager" if manager_row else "employee"
        title = "Thong bao he thong: Tu dong check-out"
        details = (
            f"He thong da tu dong check-out sau {auto_checkout_grace_minutes} phut qua gio ket ca "
            f"({shift_code}). Check-in: {check_in_raw}. Auto check-out: {check_out_raw}."
        )
        now_raw = format_db_datetime(datetime.now())

        conn.execute(
            """
            INSERT INTO issue_reports(
                reporter_id,
                reporter_role,
                branch_id,
                target_employee_id,
                title,
                details,
                status,
                escalated_to_ceo,
                manager_note,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 'in_review', 0, ?, ?, ?)
            """,
            (
                reporter_id,
                reporter_role,
                branch_id,
                employee_id,
                title,
                details,
                "He thong da tu dong check-out do qua gio ket ca",
                now_raw,
                now_raw,
            ),
        )

    def _auto_checkout_open_logs_for_employee(conn, employee_id=None):
        now_dt = datetime.now()
        if employee_id is None:
            open_logs = conn.execute(
                """
                SELECT id, employee_id, branch_id, check_in_at
                FROM attendance_logs
                WHERE check_out_at IS NULL
                ORDER BY id ASC
                """
            ).fetchall()
        else:
            open_logs = conn.execute(
                """
                SELECT id, employee_id, branch_id, check_in_at
                FROM attendance_logs
                WHERE employee_id = ?
                  AND check_out_at IS NULL
                ORDER BY id ASC
                """,
                (employee_id,),
            ).fetchall()

        auto_closed = 0
        for log_row in open_logs:
            if not log_row["branch_id"]:
                continue
            try:
                check_in_dt = parse_db_datetime(log_row["check_in_at"])
            except (TypeError, ValueError):
                continue

            shift_code, shift_start_dt, shift_end_dt = _resolve_shift_window_for_log(conn, log_row)
            if not shift_end_dt:
                continue

            auto_checkout_dt = shift_end_dt + timedelta(minutes=auto_checkout_grace_minutes)
            if now_dt <= auto_checkout_dt:
                continue

            check_out_raw = format_db_datetime(auto_checkout_dt)
            minutes_worked = max(1, int((auto_checkout_dt - check_in_dt).total_seconds() // 60))
            updated = conn.execute(
                """
                UPDATE attendance_logs
                SET check_out_at = ?,
                    minutes_worked = ?
                WHERE id = ?
                  AND check_out_at IS NULL
                """,
                (check_out_raw, minutes_worked, log_row["id"]),
            )
            if (updated.rowcount or 0) < 1:
                continue

            # Notice creation must never break attendance APIs; isolate it with a savepoint.
            conn.execute("SAVEPOINT auto_checkout_notice")
            try:
                _create_auto_checkout_notice(conn, log_row, shift_code or "-", check_out_raw)
            except Exception:
                conn.execute("ROLLBACK TO SAVEPOINT auto_checkout_notice")
            finally:
                conn.execute("RELEASE SAVEPOINT auto_checkout_notice")
            auto_closed += 1

        if auto_closed:
            conn.commit()
        return auto_closed

    @app.post("/api/attendance/check-in")
    def attendance_check_in():
        user, error = get_user_from_token(roles={"employee", "manager"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        branch_id = body.get("branch_id")
        note = (body.get("note") or "").strip() or None
        
        conn = get_conn()
        _auto_checkout_open_logs_for_employee(conn, user["id"])
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

        # Always use server real-time for attendance check-in.
        check_in_dt = datetime.now()
        
        scheduled_shift_start_at_str = None
        minutes_late = 0
        shift_row, shift_start_dt, late_deadline_dt, shift_week_start, shift_error = resolve_today_shift_for_checkin(
            conn,
            user["id"],
            branch_id,
            check_in_dt,
        )
        if shift_error or not shift_row:
            conn.close()
            return jsonify({"error": shift_error or "Bạn chưa có ca làm hôm nay"}), 400

        if shift_start_dt:
            scheduled_shift_start_at_str = format_db_datetime(shift_start_dt)
            minutes_late = max(0, int((check_in_dt - shift_start_dt).total_seconds() // 60))
        
        check_in_time_str = format_db_datetime(check_in_dt)
        cur = conn.execute(
            """
            INSERT INTO attendance_logs(employee_id, branch_id, check_in_at, scheduled_shift_start_at, minutes_late, confirmed_at, note)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (user["id"], branch_id, check_in_time_str, scheduled_shift_start_at_str, minutes_late, check_in_time_str, note),
        )
        _log_attendance_confirmation(
            conn,
            attendance_id=cur.lastrowid,
            employee_id=user["id"],
            branch_id=branch_id,
            source="check_in",
            note="Xac nhan cham cong khi check-in",
        )
        conn.commit()
        conn.close()
        return jsonify({
            "message": "Checked in", 
            "attendance_id": cur.lastrowid,
            "check_in_at": check_in_time_str,
            "scheduled_shift_start_at": scheduled_shift_start_at_str,
            "minutes_late": minutes_late
        }), 201

    @app.post("/api/attendance/confirm-open")
    def attendance_confirm_open():
        user, error = get_user_from_token(roles={"employee", "manager"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        note = (body.get("note") or "").strip() or None

        conn = get_conn()
        _auto_checkout_open_logs_for_employee(conn, user["id"])
        open_log = conn.execute(
            """
            SELECT id, branch_id
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
            return jsonify({"error": "No open attendance session to confirm"}), 400

        _log_attendance_confirmation(
            conn,
            attendance_id=open_log["id"],
            employee_id=user["id"],
            branch_id=open_log["branch_id"],
            source="manual_confirm",
            note=note,
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "Attendance confirmed", "attendance_id": open_log["id"]})

    @app.post("/api/attendance/check-out")
    def attendance_check_out():
        user, error = get_user_from_token(roles={"employee", "manager"})
        if error:
            return error

        conn = get_conn()
        _auto_checkout_open_logs_for_employee(conn, user["id"])
        open_log = conn.execute(
            """
                        SELECT id, check_in_at, confirmed_at
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

        # Business rule: payroll minutes must be anchored to the actual check-in time.
        anchor_raw = open_log["check_in_at"]
        anchor_dt = parse_db_datetime(anchor_raw)
        now_dt = datetime.now()
        check_out_at = format_db_datetime(now_dt)
        minutes = max(1, int((now_dt - anchor_dt).total_seconds() // 60))
        conn.execute(
            """
            UPDATE attendance_logs
            SET check_out_at = ?,
                minutes_worked = ?
            WHERE id = ?
            """,
            (check_out_at, minutes, open_log["id"]),
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "Checked out", "minutes_worked": minutes, "minutes_anchor": "check_in_at"})

    @app.get("/api/attendance/checkin-availability")
    def attendance_checkin_availability():
        user, error = get_user_from_token(roles={"employee", "manager"})
        if error:
            return error

        now_dt = datetime.now()
        conn = get_conn()
        _auto_checkout_open_logs_for_employee(conn, user["id"])

        if user["role"] == "manager":
            branch_id = user.get("branch_id")
            if not branch_id:
                conn.close()
                return jsonify({"can_check_in": False, "reason": "Manager chưa được gán chi nhánh"})

            shift_row, shift_start_dt, late_deadline_dt, shift_week_start, shift_error = resolve_today_shift_for_checkin(
                conn,
                user["id"],
                branch_id,
                now_dt,
            )
            conn.close()
            if shift_error or not shift_row:
                return jsonify({"can_check_in": False, "reason": shift_error or "Bạn chưa có ca làm hôm nay"})
            return jsonify(
                {
                    "can_check_in": True,
                    "branch_id": branch_id,
                    "shift_code": shift_row["shift_code"],
                    "shift_start_at": format_db_datetime(shift_start_dt) if shift_start_dt else None,
                }
            )

        branch_rows = conn.execute(
            "SELECT branch_id FROM employee_branch_access WHERE employee_id = ? ORDER BY branch_id",
            (user["id"],),
        ).fetchall()
        branch_ids = [int(row["branch_id"]) for row in branch_rows]
        if not branch_ids:
            conn.close()
            return jsonify({"can_check_in": False, "reason": "Bạn chưa được gán chi nhánh"})

        first_reason = None
        for branch_id in branch_ids:
            shift_row, shift_start_dt, late_deadline_dt, shift_week_start, shift_error = resolve_today_shift_for_checkin(
                conn,
                user["id"],
                branch_id,
                now_dt,
            )
            if not shift_error and shift_row:
                conn.close()
                return jsonify(
                    {
                        "can_check_in": True,
                        "branch_id": branch_id,
                        "shift_code": shift_row["shift_code"],
                        "shift_start_at": format_db_datetime(shift_start_dt) if shift_start_dt else None,
                    }
                )
            if shift_error and not first_reason:
                first_reason = shift_error

        conn.close()
        return jsonify({"can_check_in": False, "reason": first_reason or "Nhân viên không có ca làm, không thể check-in"})

    @app.get("/api/attendance/my-week")
    def attendance_my_week():
        user, error = get_user_from_token(roles={"employee", "manager"})
        if error:
            return error

        week_start = (request.args.get("week_start") or "").strip()
        if not week_start:
            return jsonify({"error": "week_start is required"}), 400
        start_dt, end_dt = week_range(week_start)

        conn = get_conn()
        _auto_checkout_open_logs_for_employee(conn, user["id"])
        rows = conn.execute(
            """
            SELECT a.id,
                   a.check_in_at,
                   a.scheduled_shift_start_at,
                   a.minutes_late,
                   a.confirmed_at,
                   a.check_out_at,
                   COALESCE(a.minutes_worked, 0) AS minutes_worked,
                   CASE
                       WHEN a.checked_in_by_manager_id IS NOT NULL THEN 'manager_override'
                       ELSE 'employee_self'
                   END AS attendance_source,
                   a.note,
                   COALESCE(b.name, '-') AS branch_name,
                   COALESCE(u.display_name, '-') AS checked_in_by_manager_name,
                   a.manager_check_in_note
            FROM attendance_logs a
            LEFT JOIN branches b ON b.id = a.branch_id
            LEFT JOIN users u ON u.id = a.checked_in_by_manager_id
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
        qr_disabled = _ensure_attendance_qr_enabled()
        if qr_disabled:
            return qr_disabled

        user, error = get_user_from_token(roles={"manager"})
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
        qr_nonce = f"DAY_{now_dt.strftime('%Y%m%d')}_{int(now_dt.timestamp())}_{generate_one_time_attendance_code(6)}"
        qr_token = build_attendance_qr_token(branch["id"], expires_ts, qr_nonce)
        expires_at = format_db_datetime(expires_at_dt)
        conn.close()

        payload = build_static_branch_qr_payload(branch["id"], qr_token)
        return jsonify(
            {
                "branch_id": branch["id"],
                "branch_name": branch["name"],
                "network_ip": branch["network_ip"],
                "qr_token": qr_token,
                "qr_payload": payload,
                "qr_image_data_url": build_qr_image_data_url(payload),
                "expires_at": expires_at,
                "ttl_seconds": max(1, int((expires_at_dt - now_dt).total_seconds())),
            }
        )

    @app.post("/api/attendance/check-in-qr-one-time")
    def attendance_check_in_qr_one_time():
        qr_disabled = _ensure_attendance_qr_enabled()
        if qr_disabled:
            return qr_disabled

        user, error = get_user_from_token(roles={"employee"})
        if error:
            return error

        client_ip = get_client_ip()
        checkin_limit_key = f"checkin:{user['id']}:{client_ip}"
        if _is_rate_limited(qr_checkin_attempts, checkin_limit_key, qr_checkin_limit_per_window):
            return jsonify({"error": "Bạn thao tác check-in quá nhanh. Vui lòng thử lại sau 1 phút"}), 429
        _record_rate_attempt(qr_checkin_attempts, checkin_limit_key)

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

        token_ok, token_error = verify_attendance_qr_token(qr_token, branch_id)
        if not token_ok:
            return jsonify({"error": token_error}), 400

        conn = get_conn()
        _auto_checkout_open_logs_for_employee(conn, user["id"])

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

        if not is_branch_ip_allowed(branch, client_ip):
            conn.close()
            return jsonify({"error": "You must connect from branch network to check in"}), 403

        now_dt = datetime.now()
        shift_row, shift_start_dt, late_deadline_dt, shift_week_start, shift_error = resolve_today_shift_for_checkin(
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

        now_raw = format_db_datetime(now_dt)
        cleanup_one_time_qr_codes(conn)

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

        if now_dt > late_deadline_dt:
            upsert_shift_attendance_mark(
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
                        "shift_start_at": format_db_datetime(shift_start_dt),
                        "late_deadline_at": format_db_datetime(late_deadline_dt),
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

        updated = conn.execute(
            """
            UPDATE attendance_qr_one_time_codes
            SET consumed_at = CURRENT_TIMESTAMP,
                consumed_by_employee_id = ?,
                request_ip = ?
            WHERE id = ?
              AND consumed_at IS NULL
            """,
            (user["id"], client_ip, one_time_row["id"]),
        )
        if (updated.rowcount or 0) < 1:
            conn.close()
            return jsonify({"error": "One-time key đã được sử dụng. Vui lòng quét lại QR để lấy key mới"}), 400
        
        # Calculate scheduled shift start time and minutes late
        scheduled_shift_start_at_str = format_db_datetime(shift_start_dt) if shift_start_dt else None
        check_in_dt = now_dt
        minutes_late = max(0, int((check_in_dt - shift_start_dt).total_seconds() // 60)) if shift_start_dt else 0
        
        cur = conn.execute(
            """
            INSERT INTO attendance_logs(employee_id, branch_id, check_in_at, scheduled_shift_start_at, minutes_late, confirmed_at, note)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (user["id"], branch_id, now_raw, scheduled_shift_start_at_str, minutes_late, now_raw, note),
        )

        _log_attendance_confirmation(
            conn,
            attendance_id=cur.lastrowid,
            employee_id=user["id"],
            branch_id=branch_id,
            source="check_in_qr_one_time",
            note="Xac nhan cham cong khi check-in QR",
        )

        upsert_shift_attendance_mark(
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
                    "shift_start_at": format_db_datetime(shift_start_dt),
                }
            ),
            201,
        )

    @app.get("/api/system/cron/attendance-auto-checkout")
    def cron_attendance_auto_checkout():
        expected_secret = (os.getenv("CRON_SECRET") or "").strip()
        provided_bearer = (request.headers.get("Authorization") or "").strip()
        provided_query = (request.args.get("secret") or "").strip()
        is_vercel_cron = bool((request.headers.get("x-vercel-cron") or "").strip())

        authorized = False
        if expected_secret:
            authorized = provided_bearer == f"Bearer {expected_secret}" or provided_query == expected_secret
        elif os.getenv("VERCEL") == "1":
            authorized = is_vercel_cron

        if not authorized:
            return jsonify({"error": "Unauthorized cron trigger"}), 401

        conn = get_conn()
        auto_closed = _auto_checkout_open_logs_for_employee(conn)
        conn.close()
        return jsonify(
            {
                "message": "Auto checkout scan completed",
                "auto_closed_count": int(auto_closed or 0),
                "grace_minutes": auto_checkout_grace_minutes,
                "server_now": format_db_datetime(datetime.now()),
            }
        )

    @app.post("/api/attendance/scan-qr-one-time")
    def attendance_scan_qr_one_time():
        qr_disabled = _ensure_attendance_qr_enabled()
        if qr_disabled:
            return qr_disabled

        user, error = get_user_from_token(roles={"employee"})
        if error:
            return error

        client_ip = get_client_ip()
        scan_limit_key = f"scan:{user['id']}:{client_ip}"
        if _is_rate_limited(qr_scan_attempts, scan_limit_key, qr_scan_limit_per_window):
            return jsonify({"error": "Bạn quét QR quá nhanh. Vui lòng thử lại sau 1 phút"}), 429
        _record_rate_attempt(qr_scan_attempts, scan_limit_key)

        body = request.get_json(silent=True) or {}
        qr_payload = body.get("qr_payload")
        payload_type, branch_id, one_time_code, qr_token, parse_error = parse_attendance_qr_payload(qr_payload)
        if parse_error:
            return jsonify({"error": parse_error}), 400

        token_ok, token_error = verify_attendance_qr_token(qr_token, branch_id)
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

        if not is_branch_ip_allowed(branch, client_ip):
            conn.close()
            return jsonify({"error": "Ban phai ket noi Wi-Fi chi nhanh de quet QR"}), 403

        if payload_type == "static":
            manager_row = conn.execute(
                """
                SELECT id
                FROM users
                WHERE role = 'manager'
                  AND branch_id = ?
                  AND is_active = 1
                ORDER BY id
                LIMIT 1
                """,
                (branch_id,),
            ).fetchone()
            if not manager_row:
                conn.close()
                return jsonify({"error": "Chi nhanh chua co quan ly hoat dong de cap key"}), 400

            cleanup_one_time_qr_codes(conn)
            issued_one_time_code = generate_one_time_attendance_code()
            issued_expires_at_dt = datetime.now() + timedelta(seconds=attendance_qr_one_time_ttl_seconds)
            issued_expires_at = format_db_datetime(issued_expires_at_dt)

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
                (branch_id, qr_token, issued_one_time_code, issued_expires_at, manager_row["id"]),
            )
            conn.commit()
            conn.close()
            return jsonify(
                {
                    "branch_id": branch_id,
                    "qr_token": qr_token,
                    "random_key": issued_one_time_code,
                    "expires_at": issued_expires_at,
                }
            )

        now_raw = format_db_datetime(datetime.now())
        cleanup_one_time_qr_codes(conn)

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

        return jsonify(
            {
                "branch_id": branch_id,
                "qr_token": qr_token,
                "random_key": one_time_code,
                "expires_at": one_time_row["expires_at"],
            }
        )
