from datetime import datetime, timedelta

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

    attendance_qr_one_time_ttl_seconds = deps["ATTENDANCE_QR_ONE_TIME_TTL_SECONDS"]
    attendance_qr_enabled = deps.get("ATTENDANCE_QR_ENABLED", True)

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

    @app.post("/api/attendance/check-in")
    def attendance_check_in():
        user, error = get_user_from_token(roles={"employee", "manager"})
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
        user, error = get_user_from_token(roles={"employee", "manager"})
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

        check_in_dt = parse_db_datetime(open_log["check_in_at"])
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
        user, error = get_user_from_token(roles={"employee", "manager"})
        if error:
            return error

        week_start = (request.args.get("week_start") or "").strip()
        if not week_start:
            return jsonify({"error": "week_start is required"}), 400
        start_dt, end_dt = week_range(week_start)

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
        qr_nonce = f"DAY_{now_dt.strftime('%Y%m%d')}"
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

        client_ip = get_client_ip()
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

        try:
            expires_at_dt = parse_db_datetime(one_time_row["expires_at"])
        except (TypeError, ValueError):
            conn.close()
            return jsonify({"error": "One-time key không hợp lệ"}), 400
        if now_dt > expires_at_dt:
            conn.close()
            return jsonify({"error": "One-time key đã hết hạn"}), 400

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

    @app.post("/api/attendance/scan-qr-one-time")
    def attendance_scan_qr_one_time():
        qr_disabled = _ensure_attendance_qr_enabled()
        if qr_disabled:
            return qr_disabled

        user, error = get_user_from_token(roles={"employee"})
        if error:
            return error

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

        client_ip = get_client_ip()
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

        try:
            expires_at_dt = parse_db_datetime(one_time_row["expires_at"])
        except (TypeError, ValueError):
            return jsonify({"error": "QR one-time da het han hoac da duoc su dung"}), 400
        if datetime.now() > expires_at_dt:
            return jsonify({"error": "QR one-time da het han hoac da duoc su dung"}), 400

        return jsonify(
            {
                "branch_id": branch_id,
                "qr_token": qr_token,
                "random_key": one_time_code,
                "expires_at": format_db_datetime(expires_at_dt),
            }
        )
