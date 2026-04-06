import re
import secrets
import base64
import hashlib
import json
from datetime import datetime, timedelta

from flask import jsonify, request, Response
from werkzeug.security import generate_password_hash


def register_operations_routes(app, deps):
    get_conn = deps["get_conn"]
    get_user_from_token = deps["_get_user_from_token"]
    normalize_day_of_week = deps["_normalize_day_of_week"]
    get_branch_staffing_rules = deps["_get_branch_staffing_rules"]
    week_start_and_day_for_datetime = deps["_week_start_and_day_for_datetime"]
    shift_start_datetime = deps["_shift_start_datetime"]
    parse_db_datetime = deps["_parse_db_datetime"]
    format_db_datetime = deps["_format_db_datetime"]
    upsert_shift_attendance_mark = deps["_upsert_shift_attendance_mark"]
    weekly_hours_rows = deps["_weekly_hours_rows"]
    csv_response = deps["_csv_response"]
    build_weekly_payroll_csv = deps["_build_weekly_payroll_csv"]
    manager_can_manage_employee = deps["_manager_can_manage_employee"]
    create_audit_log = deps.get("_create_audit_log")

    shift_code_set = deps["SHIFT_CODE_SET"]
    shift_definitions = deps["SHIFT_DEFINITIONS"]

    def _normalize_registration_type(value):
        text = (value or "individual").strip().lower()
        if text in {"group", "individual"}:
            return text
        return None

    def _sanitize_group_code(value):
        text = re.sub(r"[^A-Za-z0-9_-]", "", (value or "").strip().upper())
        return text[:32]

    def _normalize_hhmm(value):
        text = (value or "").strip()
        if not text:
            return None
        if not re.match(r"^\d{2}:\d{2}$", text):
            return None
        hour, minute = [int(part) for part in text.split(":")]
        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            return None
        return f"{hour:02d}:{minute:02d}"

    def _flex_shift_start_datetime(week_start, day_of_week, flexible_start_at):
        text = _normalize_hhmm(flexible_start_at)
        if not text:
            return None
        hour_text, minute_text = text.split(":")
        day_dt = datetime.strptime(week_start, "%Y-%m-%d") + timedelta(days=int(day_of_week) - 1)
        return day_dt.replace(hour=int(hour_text), minute=int(minute_text), second=0, microsecond=0)

    def _schedule_rows_for_branch_week(conn, week_start, branch_id):
        rows = conn.execute(
            """
            SELECT employee_id,
                   shift_code,
                   day_of_week,
                   registration_type,
                   group_code,
                   flexible_start_at,
                   flexible_end_at
            FROM weekly_schedule
            WHERE week_start = ?
              AND branch_id = ?
            ORDER BY day_of_week, shift_code, employee_id
            """,
            (week_start, branch_id),
        ).fetchall()
        return [dict(row) for row in rows]

    def _schedule_row_tuple(row):
        return (
            int(row.get("employee_id") or 0),
            str(row.get("shift_code") or ""),
            int(row.get("day_of_week") or 0),
            str(row.get("registration_type") or "individual"),
            str(row.get("group_code") or ""),
            str(row.get("flexible_start_at") or ""),
            str(row.get("flexible_end_at") or ""),
        )

    def _schedule_revision(rows):
        payload = [_schedule_row_tuple(row) for row in rows]
        raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    def _format_schedule_tuple(item, employee_name_map):
        employee_id, shift_code, day_of_week, registration_type, group_code, flex_start, flex_end = item
        employee_name = employee_name_map.get(employee_id, f"employee_id={employee_id}")
        text = f"{employee_name} | {shift_code} | day={day_of_week}"
        if registration_type == "group" and group_code:
            text += f" | group={group_code}"
        if shift_code == "FLEX" and flex_start and flex_end:
            text += f" | {flex_start}-{flex_end}"
        return text

    @app.post("/api/issues")
    def create_issue():
        user, error = get_user_from_token(roles={"employee", "manager"})
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
        user, error = get_user_from_token(roles={"employee", "manager"})
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

    @app.get("/api/issues/my/<int:issue_id>/replies")
    def my_issue_replies(issue_id):
        user, error = get_user_from_token(roles={"employee", "manager"})
        if error:
            return error

        conn = get_conn()
        issue = conn.execute(
            """
            SELECT id
            FROM issue_reports
            WHERE id = ?
              AND reporter_id = ?
            LIMIT 1
            """,
            (issue_id, user["id"]),
        ).fetchone()
        if not issue:
            conn.close()
            return jsonify({"error": "Issue not found or access denied"}), 404

        rows = conn.execute(
            """
            SELECT r.id,
                   r.issue_id,
                   r.sender_id,
                   r.sender_role,
                   r.message,
                   r.created_at,
                   COALESCE(u.display_name, '-') AS sender_name
            FROM issue_report_replies r
            LEFT JOIN users u ON u.id = r.sender_id
            WHERE r.issue_id = ?
            ORDER BY r.created_at ASC, r.id ASC
            """,
            (issue_id,),
        ).fetchall()
        conn.close()
        return jsonify([dict(row) for row in rows])

    @app.get("/api/employee/branches")
    def employee_branches():
        user, error = get_user_from_token(roles={"employee"})
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

    @app.get("/api/employee/registration-groups")
    def employee_registration_groups_get():
        user, error = get_user_from_token(roles={"employee"})
        if error:
            return error

        week_start = (request.args.get("week_start") or "").strip()
        if not week_start:
            return jsonify({"error": "week_start is required"}), 400

        conn = get_conn()
        rows = conn.execute(
            """
            SELECT g.id,
                   g.group_code,
                   g.group_name,
                   g.week_start,
                   g.branch_id,
                     g.max_members,
                   b.name AS branch_name,
                   g.created_by_employee_id,
                   g.note,
                   g.created_at,
                                     COUNT(DISTINCT m.employee_id) AS member_count
            FROM shift_registration_groups g
            JOIN branches b ON b.id = g.branch_id
            LEFT JOIN shift_registration_group_members m ON m.group_id = g.id
            WHERE g.week_start = ?
              AND g.branch_id IN (
                  SELECT branch_id FROM employee_branch_access WHERE employee_id = ?
              )
            GROUP BY g.id, g.group_code, g.group_name, g.week_start, g.branch_id, b.name,
                     g.max_members, g.created_by_employee_id, g.note, g.created_at
            ORDER BY g.created_at DESC
            """,
            (week_start, user["id"]),
        ).fetchall()

        group_ids = [int(row["id"]) for row in rows]
        members_by_group = {}
        if group_ids:
            placeholders = ",".join(["?"] * len(group_ids))
            member_rows = conn.execute(
                f"""
                SELECT m.group_id,
                       u.display_name AS member_name
                FROM shift_registration_group_members m
                JOIN users u ON u.id = m.employee_id
                WHERE m.group_id IN ({placeholders})
                ORDER BY u.display_name
                """,
                tuple(group_ids),
            ).fetchall()
            for member_row in member_rows:
                group_id = int(member_row["group_id"])
                members_by_group.setdefault(group_id, []).append(member_row["member_name"])

        conn.close()

        payload = []
        for row in rows:
            item = dict(row)
            members = members_by_group.get(int(item["id"]), [])
            item["members"] = members
            item["members_text"] = ", ".join(members)
            payload.append(item)
        return jsonify(payload)

    @app.get("/api/manager/registration-groups")
    def manager_registration_groups_get():
        user, error = get_user_from_token(roles={"manager"})
        if error:
            return error

        week_start = (request.args.get("week_start") or "").strip()
        if not week_start:
            return jsonify({"error": "week_start is required"}), 400

        conn = get_conn()
        rows = conn.execute(
            """
            SELECT g.id,
                   g.group_code,
                   g.group_name,
                   g.week_start,
                   g.branch_id,
                   g.max_members,
                   g.created_by_employee_id,
                   g.note,
                   g.created_at,
                   COUNT(m.employee_id) AS member_count
            FROM shift_registration_groups g
            LEFT JOIN shift_registration_group_members m ON m.group_id = g.id
            WHERE g.week_start = ?
              AND g.branch_id = ?
            GROUP BY g.id, g.group_code, g.group_name, g.week_start, g.branch_id,
                     g.max_members, g.created_by_employee_id, g.note, g.created_at
            ORDER BY g.created_at DESC
            """,
            (week_start, user["branch_id"]),
        ).fetchall()
        conn.close()
        return jsonify([dict(row) for row in rows])

    @app.post("/api/manager/registration-groups")
    def manager_registration_groups_create():
        user, error = get_user_from_token(roles={"manager"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        week_start = (body.get("week_start") or "").strip()
        group_name = (body.get("group_name") or "").strip()
        note = (body.get("note") or "").strip() or None
        group_code = _sanitize_group_code(body.get("group_code"))

        try:
            max_members = int(body.get("max_members"))
        except (TypeError, ValueError):
            return jsonify({"error": "max_members is required and must be an integer"}), 400

        if max_members < 1 or max_members > 500:
            return jsonify({"error": "max_members must be between 1 and 500"}), 400
        if not week_start:
            return jsonify({"error": "week_start is required"}), 400
        if not group_name:
            return jsonify({"error": "group_name is required"}), 400

        if not group_code:
            seed = secrets.token_hex(2).upper()
            group_code = _sanitize_group_code(f"G{week_start.replace('-', '')}{seed}")

        conn = get_conn()
        conn.execute(
            """
            INSERT INTO shift_registration_groups(
                group_code,
                group_name,
                week_start,
                branch_id,
                max_members,
                created_by_employee_id,
                note
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (group_code, group_name, week_start, user["branch_id"], max_members, user["id"], note),
        )
        conn.commit()
        conn.close()

        return jsonify({"message": "Manager created registration group", "group_code": group_code, "max_members": max_members}), 201

    @app.post("/api/employee/registration-groups")
    def employee_registration_groups_create():
        user, error = get_user_from_token(roles={"employee"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        week_start = (body.get("week_start") or "").strip()
        group_name = (body.get("group_name") or "").strip()
        note = (body.get("note") or "").strip() or None
        branch_id = body.get("branch_id")
        group_code = _sanitize_group_code(body.get("group_code"))
        max_members_raw = body.get("max_members")

        if not week_start:
            return jsonify({"error": "week_start is required"}), 400
        if not group_name:
            return jsonify({"error": "group_name is required"}), 400
        try:
            branch_id = int(branch_id)
        except (TypeError, ValueError):
            return jsonify({"error": "branch_id must be an integer"}), 400

        max_members = None
        if max_members_raw is not None:
            try:
                max_members = int(max_members_raw)
            except (TypeError, ValueError):
                return jsonify({"error": "max_members must be an integer"}), 400
            if max_members < 1 or max_members > 500:
                return jsonify({"error": "max_members must be between 1 and 500"}), 400

        conn = get_conn()
        allowed = conn.execute(
            "SELECT 1 FROM employee_branch_access WHERE employee_id = ? AND branch_id = ? LIMIT 1",
            (user["id"], branch_id),
        ).fetchone()
        if not allowed:
            conn.close()
            return jsonify({"error": "Branch is not in employee access scope"}), 403

        if not group_code:
            seed = secrets.token_hex(2).upper()
            group_code = _sanitize_group_code(f"G{week_start.replace('-', '')}{seed}")

        cur = conn.execute(
            """
            INSERT INTO shift_registration_groups(
                group_code, group_name, week_start, branch_id, max_members, created_by_employee_id, note
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (group_code, group_name, week_start, branch_id, max_members, user["id"], note),
        )
        conn.execute(
            """
            INSERT INTO shift_registration_group_members(group_id, employee_id)
            VALUES (?, ?)
            ON CONFLICT(group_id, employee_id) DO NOTHING
            """,
            (cur.lastrowid, user["id"]),
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "Created registration group", "group_code": group_code}), 201

    @app.post("/api/employee/registration-groups/join")
    def employee_registration_groups_join():
        user, error = get_user_from_token(roles={"employee"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        week_start = (body.get("week_start") or "").strip()
        group_code = _sanitize_group_code(body.get("group_code"))
        branch_id = body.get("branch_id")

        if not week_start or not group_code:
            return jsonify({"error": "week_start and group_code are required"}), 400
        try:
            branch_id = int(branch_id)
        except (TypeError, ValueError):
            return jsonify({"error": "branch_id must be an integer"}), 400

        conn = get_conn()
        allowed = conn.execute(
            "SELECT 1 FROM employee_branch_access WHERE employee_id = ? AND branch_id = ? LIMIT 1",
            (user["id"], branch_id),
        ).fetchone()
        if not allowed:
            conn.close()
            return jsonify({"error": "Branch is not in employee access scope"}), 403

        group_row = conn.execute(
            """
                        SELECT id, max_members
            FROM shift_registration_groups
            WHERE week_start = ?
              AND branch_id = ?
              AND group_code = ?
            LIMIT 1
            """,
            (week_start, branch_id, group_code),
        ).fetchone()
        if not group_row:
            conn.close()
            return jsonify({"error": "Registration group not found"}), 404

        if group_row["max_members"] is not None:
            current_members = conn.execute(
                "SELECT COUNT(*) AS c FROM shift_registration_group_members WHERE group_id = ?",
                (group_row["id"],),
            ).fetchone()["c"]
            if current_members >= int(group_row["max_members"]):
                conn.close()
                return jsonify({"error": "Nhóm đã đủ số lượng thành viên"}), 400

        conn.execute(
            """
            INSERT INTO shift_registration_group_members(group_id, employee_id)
            VALUES (?, ?)
            ON CONFLICT(group_id, employee_id) DO NOTHING
            """,
            (group_row["id"], user["id"]),
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "Joined registration group", "group_code": group_code})

    @app.put("/api/employee/preferences")
    def upsert_preferences():
        user, error = get_user_from_token(roles={"employee"})
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
        existing_count = conn.execute(
            "SELECT COUNT(*) AS c FROM shift_preferences WHERE employee_id = ? AND week_start = ?",
            (user["id"], week_start),
        ).fetchone()["c"]
        if existing_count > 0:
            conn.close()
            return jsonify(
                {
                    "error": "Bạn đã chốt đăng ký ca cho tuần này. Vui lòng đợi sang tuần mới để chỉnh sửa.",
                    "locked": True,
                }
            ), 409

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
            if not isinstance(item, dict):
                conn.close()
                return jsonify({"error": "Each selection must be an object"}), 400
            try:
                branch_id = int(item.get("branch_id"))
            except (TypeError, ValueError):
                conn.close()
                return jsonify({"error": "branch_id must be an integer"}), 400
            shift_code = item.get("shift_code")
            day_of_week = normalize_day_of_week(item.get("day_of_week"))
            registration_type = _normalize_registration_type(item.get("registration_type"))
            group_code = _sanitize_group_code(item.get("group_code"))
            flexible_start_at = _normalize_hhmm(item.get("flexible_start_at"))
            flexible_end_at = _normalize_hhmm(item.get("flexible_end_at"))
            if branch_id not in allowed_branch_ids:
                conn.close()
                return jsonify({"error": f"Branch {branch_id} not allowed"}), 400
            if shift_code not in shift_code_set:
                conn.close()
                return jsonify({"error": f"Invalid shift code: {shift_code}"}), 400
            if day_of_week is None:
                conn.close()
                return jsonify({"error": "day_of_week must be in range 1..7"}), 400
            if registration_type is None:
                conn.close()
                return jsonify({"error": "registration_type must be individual or group"}), 400
            if registration_type == "group":
                if not group_code:
                    conn.close()
                    return jsonify({"error": "group_code is required when registration_type=group"}), 400
                group_exists = conn.execute(
                    """
                    SELECT 1
                    FROM shift_registration_groups
                    WHERE week_start = ?
                      AND branch_id = ?
                      AND group_code = ?
                    LIMIT 1
                    """,
                    (week_start, branch_id, group_code),
                ).fetchone()
                if not group_exists:
                    conn.close()
                    return jsonify({"error": f"Group code {group_code} not found for selected week/branch"}), 400
            else:
                group_code = None

            if shift_code == "FLEX":
                if not flexible_start_at or not flexible_end_at:
                    conn.close()
                    return jsonify({"error": "Ca linh hoạt yêu cầu flexible_start_at và flexible_end_at theo định dạng HH:MM"}), 400
                if flexible_end_at <= flexible_start_at:
                    conn.close()
                    return jsonify({"error": "flexible_end_at phải lớn hơn flexible_start_at"}), 400
            else:
                flexible_start_at = None
                flexible_end_at = None

            key = (branch_id, shift_code, day_of_week, registration_type, group_code, flexible_start_at, flexible_end_at)
            if key in seen:
                continue
            seen.add(key)
            valid_rows.append(
                (
                    user["id"],
                    week_start,
                    branch_id,
                    shift_code,
                    day_of_week,
                    registration_type,
                    group_code,
                    flexible_start_at,
                    flexible_end_at,
                )
            )

        conn.execute(
            "DELETE FROM shift_preferences WHERE employee_id = ? AND week_start = ?",
            (user["id"], week_start),
        )
        if valid_rows:
            conn.executemany(
                """
                INSERT INTO shift_preferences(
                    employee_id,
                    week_start,
                    branch_id,
                    shift_code,
                    day_of_week,
                    registration_type,
                    group_code,
                    flexible_start_at,
                    flexible_end_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                valid_rows,
            )
        conn.commit()
        conn.close()
        return jsonify({"message": "Saved preferences", "count": len(valid_rows)})

    @app.get("/api/employee/preferences")
    def employee_preferences():
        user, error = get_user_from_token(roles={"employee"})
        if error:
            return error

        week_start = (request.args.get("week_start") or "").strip()
        if not week_start:
            return jsonify({"error": "week_start is required"}), 400

        conn = get_conn()
        rows = conn.execute(
            """
            SELECT sp.id,
                   sp.week_start,
                   sp.branch_id,
                   b.name AS branch_name,
                   sp.shift_code,
                   sp.day_of_week,
                   sp.registration_type,
                   sp.group_code,
                   sp.flexible_start_at,
                   sp.flexible_end_at
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
        user, error = get_user_from_token(roles={"employee"})
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
                     ws.registration_type,
                     ws.group_code,
                     ws.flexible_start_at,
                     ws.flexible_end_at,
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

        row_items = [dict(row) for row in rows]
        team_key_to_members = {}
        for item in row_items:
            if item.get("registration_type") != "group" or not item.get("group_code"):
                continue
            team_key = (
                item["week_start"],
                int(item["branch_id"]),
                int(item["day_of_week"]),
                item["shift_code"],
                item["group_code"],
            )
            if team_key not in team_key_to_members:
                team_key_to_members[team_key] = []

        if team_key_to_members:
            branch_ids = sorted({team_key[1] for team_key in team_key_to_members.keys()})
            placeholders = ",".join(["?"] * len(branch_ids))
            team_rows = conn.execute(
                f"""
                SELECT ws.week_start,
                       ws.branch_id,
                       ws.day_of_week,
                       ws.shift_code,
                       ws.group_code,
                       u.display_name AS member_name
                FROM weekly_schedule ws
                JOIN users u ON u.id = ws.employee_id
                WHERE ws.week_start = ?
                  AND ws.registration_type = 'group'
                  AND ws.group_code IS NOT NULL
                  AND ws.branch_id IN ({placeholders})
                ORDER BY u.display_name
                """,
                tuple([week_start] + branch_ids),
            ).fetchall()

            for team_row in team_rows:
                team_key = (
                    team_row["week_start"],
                    int(team_row["branch_id"]),
                    int(team_row["day_of_week"]),
                    team_row["shift_code"],
                    team_row["group_code"],
                )
                if team_key in team_key_to_members:
                    team_key_to_members[team_key].append(team_row["member_name"])

        conn.close()

        for item in row_items:
            if item.get("registration_type") == "group" and item.get("group_code"):
                team_key = (
                    item["week_start"],
                    int(item["branch_id"]),
                    int(item["day_of_week"]),
                    item["shift_code"],
                    item["group_code"],
                )
                members = team_key_to_members.get(team_key, [])
                item["team_members"] = members
                item["team_members_text"] = ", ".join(members)
                item["team_size"] = len(members)
            else:
                item["team_members"] = []
                item["team_members_text"] = ""
                item["team_size"] = 0

        return jsonify(row_items)

    @app.get("/api/manager/preferences")
    def manager_view_preferences():
        user, error = get_user_from_token(roles={"manager"})
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
                     sp.day_of_week,
                     sp.registration_type,
                     sp.group_code,
                     sp.flexible_start_at,
                     sp.flexible_end_at
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
        user, error = get_user_from_token(roles={"manager"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        week_start = (body.get("week_start") or "").strip()
        schedule_revision = (body.get("schedule_revision") or "").strip()
        assignments = body.get("assignments") or []

        if not week_start:
            return jsonify({"error": "week_start is required"}), 400
        if not isinstance(assignments, list):
            return jsonify({"error": "assignments must be a list"}), 400

        conn = get_conn()
        current_schedule_rows = _schedule_rows_for_branch_week(conn, week_start, user["branch_id"])
        current_schedule_revision = _schedule_revision(current_schedule_rows)
        if schedule_revision and schedule_revision != current_schedule_revision:
            conn.close()
            return (
                jsonify(
                    {
                        "error": "Lịch đã được cập nhật bởi người khác. Vui lòng tải lại trước khi lưu.",
                        "code": "schedule_conflict",
                        "current_revision": current_schedule_revision,
                    }
                ),
                409,
            )

        staffing_rules = get_branch_staffing_rules(conn, user["branch_id"])
        normalized = []
        seen = set()
        parsed_items = []
        for item in assignments:
            if not isinstance(item, dict):
                conn.close()
                return jsonify({"error": "Each assignment must be an object"}), 400
            try:
                employee_id = int(item.get("employee_id"))
            except (TypeError, ValueError):
                conn.close()
                return jsonify({"error": "employee_id must be an integer"}), 400
            shift_code = item.get("shift_code")
            day_of_week = normalize_day_of_week(item.get("day_of_week"))
            if shift_code not in shift_code_set:
                conn.close()
                return jsonify({"error": f"Invalid shift code: {shift_code}"}), 400
            if day_of_week is None:
                conn.close()
                return jsonify({"error": "day_of_week must be in range 1..7"}), 400

            parsed_items.append(
                {
                    "employee_id": employee_id,
                    "shift_code": shift_code,
                    "day_of_week": day_of_week,
                }
            )

        employee_ids = sorted({item["employee_id"] for item in parsed_items})
        placeholders = ",".join(["?"] * len(employee_ids)) if employee_ids else ""
        pref_rows = []
        if employee_ids:
            pref_rows = conn.execute(
                f"""
                SELECT employee_id,
                       shift_code,
                       day_of_week,
                       registration_type,
                       group_code,
                       flexible_start_at,
                       flexible_end_at
                FROM shift_preferences
                WHERE employee_id IN ({placeholders})
                  AND week_start = ?
                  AND branch_id = ?
                """,
                tuple(employee_ids + [week_start, user["branch_id"]]),
            ).fetchall()

        pref_exact = {}
        pref_fallback = {}
        for pref in pref_rows:
            employee_id = int(pref["employee_id"])
            shift_code = pref["shift_code"]
            day_of_week = int(pref["day_of_week"])
            pref_value = (
                pref["registration_type"] or "individual",
                pref["group_code"],
                pref["flexible_start_at"],
                pref["flexible_end_at"],
            )
            if day_of_week == 0:
                pref_fallback[(employee_id, shift_code)] = pref_value
            else:
                pref_exact[(employee_id, shift_code, day_of_week)] = pref_value

        group_member_cache = {}

        def _group_member_ids(group_code):
            cached = group_member_cache.get(group_code)
            if cached is not None:
                return cached

            rows = conn.execute(
                """
                SELECT DISTINCT gm.employee_id
                FROM shift_registration_groups g
                JOIN shift_registration_group_members gm ON gm.group_id = g.id
                WHERE g.week_start = ?
                  AND g.branch_id = ?
                  AND g.group_code = ?
                """,
                (week_start, user["branch_id"], group_code),
            ).fetchall()
            member_ids = [int(row["employee_id"]) for row in rows]
            group_member_cache[group_code] = member_ids
            return member_ids

        expanded_items = []
        expanded_seen = set()
        target_employee_ids = set()

        for item in parsed_items:
            employee_id = item["employee_id"]
            shift_code = item["shift_code"]
            day_of_week = item["day_of_week"]

            registration_type = "individual"
            group_code = None
            flexible_start_at = None
            flexible_end_at = None
            pref_value = pref_exact.get((employee_id, shift_code, day_of_week))
            if pref_value is None:
                pref_value = pref_fallback.get((employee_id, shift_code))
            if pref_value is not None:
                registration_type, group_code, flexible_start_at, flexible_end_at = pref_value

            target_ids = [employee_id]
            if registration_type == "group" and group_code:
                member_ids = _group_member_ids(group_code)
                if member_ids:
                    target_ids = member_ids

            for target_employee_id in target_ids:
                key = (
                    target_employee_id,
                    shift_code,
                    day_of_week,
                    registration_type,
                    group_code,
                    flexible_start_at,
                    flexible_end_at,
                )
                if key in expanded_seen:
                    continue
                expanded_seen.add(key)
                target_employee_ids.add(target_employee_id)
                expanded_items.append(
                    (
                        target_employee_id,
                        shift_code,
                        day_of_week,
                        registration_type,
                        group_code,
                        flexible_start_at,
                        flexible_end_at,
                    )
                )

        if target_employee_ids:
            target_id_list = sorted(target_employee_ids)
            target_placeholders = ",".join(["?"] * len(target_id_list))
            in_scope_rows = conn.execute(
                f"""
                SELECT DISTINCT u.id
                FROM users u
                LEFT JOIN employee_branch_access eba
                       ON eba.employee_id = u.id
                      AND eba.branch_id = ?
                WHERE u.id IN ({target_placeholders})
                  AND (
                        (u.role = 'employee' AND eba.employee_id IS NOT NULL)
                     OR (u.role = 'manager' AND u.branch_id = ?)
                  )
                """,
                tuple([user["branch_id"]] + target_id_list + [user["branch_id"]]),
            ).fetchall()
            in_scope_ids = {int(row["id"]) for row in in_scope_rows}

            out_of_scope = next((employee_id for employee_id in target_id_list if employee_id not in in_scope_ids), None)
            if out_of_scope is not None:
                conn.close()
                return jsonify({"error": "Employee is outside manager branch scope", "employee_id": out_of_scope}), 400

        for (
            employee_id,
            shift_code,
            day_of_week,
            registration_type,
            group_code,
            flexible_start_at,
            flexible_end_at,
        ) in expanded_items:
            normalized.append(
                (
                    week_start,
                    user["branch_id"],
                    employee_id,
                    shift_code,
                    day_of_week,
                    registration_type,
                    group_code,
                    flexible_start_at,
                    flexible_end_at,
                    user["id"],
                )
            )

        counts = {}
        for _, _, _, shift_code, day_of_week, *_ in normalized:
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
                    registration_type,
                    group_code,
                    flexible_start_at,
                    flexible_end_at,
                    assigned_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                normalized,
            )

        previous_set = {_schedule_row_tuple(row) for row in current_schedule_rows}
        new_schedule_rows = [
            {
                "employee_id": item[2],
                "shift_code": item[3],
                "day_of_week": item[4],
                "registration_type": item[5],
                "group_code": item[6],
                "flexible_start_at": item[7],
                "flexible_end_at": item[8],
            }
            for item in normalized
        ]
        new_set = {_schedule_row_tuple(row) for row in new_schedule_rows}
        added = sorted(new_set - previous_set)
        removed = sorted(previous_set - new_set)

        if create_audit_log:
            all_employee_ids = sorted({item[0] for item in (added + removed)})
            employee_name_map = {}
            if all_employee_ids:
                placeholders = ",".join(["?"] * len(all_employee_ids))
                employee_rows = conn.execute(
                    f"SELECT id, display_name FROM users WHERE id IN ({placeholders})",
                    tuple(all_employee_ids),
                ).fetchall()
                employee_name_map = {
                    int(row["id"]): str(row["display_name"] or f"employee_id={row['id']}")
                    for row in employee_rows
                }

            details_payload = {
                "week_start": week_start,
                "branch_id": user["branch_id"],
                "before_count": len(previous_set),
                "after_count": len(new_set),
                "added_count": len(added),
                "removed_count": len(removed),
                "added_preview": [
                    _format_schedule_tuple(item, employee_name_map)
                    for item in added[:12]
                ],
                "removed_preview": [
                    _format_schedule_tuple(item, employee_name_map)
                    for item in removed[:12]
                ],
                "revision_before": current_schedule_revision,
                "revision_after": _schedule_revision(new_schedule_rows),
            }
            create_audit_log(
                conn,
                user,
                action="manager.schedule.update",
                target_type="branch_schedule",
                target_id=user["branch_id"],
                details=json.dumps(details_payload, ensure_ascii=True, separators=(",", ":")),
            )

        conn.commit()
        conn.close()
        return jsonify(
            {
                "message": "Weekly schedule saved",
                "count": len(normalized),
                "schedule_revision": _schedule_revision(new_schedule_rows),
            }
        )

    @app.get("/api/manager/staffing-rules")
    def manager_staffing_rules_get():
        user, error = get_user_from_token(roles={"manager"})
        if error:
            return error

        conn = get_conn()
        rules = get_branch_staffing_rules(conn, user["branch_id"])
        conn.close()

        payload = []
        for shift in shift_definitions:
            default_rule = {"min_staff": 0, "max_staff": 999} if shift["code"] == "FLEX" else {"min_staff": 3, "max_staff": 4}
            rule = rules.get(shift["code"], default_rule)
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
        user, error = get_user_from_token(roles={"manager"})
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
            if shift_code not in shift_code_set:
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
        user, error = get_user_from_token(roles={"manager"})
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
                     ws.registration_type,
                     ws.group_code,
                     ws.flexible_start_at,
                     ws.flexible_end_at,
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
        payload = [dict(row) for row in rows]
        response = jsonify(payload)
        response.headers["X-Schedule-Revision"] = _schedule_revision(payload)
        conn.close()

        return response

    @app.get("/api/manager/schedule-revision")
    def manager_get_schedule_revision():
        user, error = get_user_from_token(roles={"manager"})
        if error:
            return error

        week_start = (request.args.get("week_start") or "").strip()
        if not week_start:
            return jsonify({"error": "week_start is required"}), 400

        conn = get_conn()
        rows = _schedule_rows_for_branch_week(conn, week_start, user["branch_id"])
        conn.close()
        return jsonify(
            {
                "week_start": week_start,
                "branch_id": user["branch_id"],
                "schedule_revision": _schedule_revision(rows),
                "count": len(rows),
            }
        )

    @app.get("/api/manager/attendance-shifts/today")
    def manager_attendance_shifts_today():
        user, error = get_user_from_token(roles={"manager"})
        if error:
            return error

        current_dt = datetime.now()
        week_start, day_of_week = week_start_and_day_for_datetime(current_dt)

        conn = get_conn()
        rows = conn.execute(
            """
            SELECT ws.id AS schedule_id,
                   ws.week_start,
                   ws.day_of_week,
                   ws.shift_code,
                     ws.flexible_start_at,
                     ws.flexible_end_at,
                   ws.branch_id,
                   ws.employee_id,
                   u.display_name AS employee_name,
                   m.status,
                   m.source,
                   m.note,
                   m.updated_at,
                   m.marked_by_manager_id,
                     m.attendance_log_id,
                     al.check_in_at AS attendance_check_in_at,
                     al.check_out_at AS attendance_check_out_at
            FROM weekly_schedule ws
            JOIN users u ON u.id = ws.employee_id
            LEFT JOIN shift_attendance_marks m
                   ON m.week_start = ws.week_start
                  AND m.day_of_week = ws.day_of_week
                  AND m.shift_code = ws.shift_code
                  AND m.branch_id = ws.branch_id
                  AND m.employee_id = ws.employee_id
                 LEFT JOIN attendance_logs al
                     ON al.id = m.attendance_log_id
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
            if row["shift_code"] == "FLEX":
                start_dt = _flex_shift_start_datetime(
                    row["week_start"], row["day_of_week"], row["flexible_start_at"]
                )
            else:
                start_dt = shift_start_datetime(row["week_start"], row["day_of_week"], row["shift_code"])
            if not start_dt:
                continue
            late_deadline_dt = start_dt + timedelta(minutes=15)
            status = row["status"]
            if not status:
                status = "late_unmarked" if current_dt > late_deadline_dt else "pending"

            check_in_raw = row["attendance_check_in_at"]
            late_minutes = 0
            if check_in_raw:
                try:
                    check_in_dt = parse_db_datetime(str(check_in_raw))
                    if check_in_dt > start_dt:
                        late_minutes = int((check_in_dt - start_dt).total_seconds() // 60)
                except (TypeError, ValueError):
                    late_minutes = 0

            is_late_shortage_override = (
                status == "present_override"
                and (row["source"] or "") == "manager_override_shortage"
                and late_minutes > 0
            )

            items.append(
                {
                    "schedule_id": row["schedule_id"],
                    "week_start": row["week_start"],
                    "day_of_week": row["day_of_week"],
                    "shift_code": row["shift_code"],
                    "flexible_start_at": row["flexible_start_at"],
                    "flexible_end_at": row["flexible_end_at"],
                    "employee_id": row["employee_id"],
                    "employee_name": row["employee_name"],
                    "status": status,
                    "source": row["source"] or "",
                    "note": row["note"] or "",
                    "attendance_log_id": row["attendance_log_id"],
                    "attendance_check_in_at": row["attendance_check_in_at"],
                    "attendance_check_out_at": row["attendance_check_out_at"],
                    "shift_start_at": format_db_datetime(start_dt),
                    "late_deadline_at": format_db_datetime(late_deadline_dt),
                    "late_minutes": late_minutes,
                    "is_late_shortage_override": is_late_shortage_override,
                    "updated_at": row["updated_at"],
                }
            )

        return jsonify(
            {
                "server_now": format_db_datetime(current_dt),
                "week_start": week_start,
                "day_of_week": day_of_week,
                "items": items,
            }
        )

    @app.put("/api/manager/attendance-shifts/override")
    def manager_attendance_shift_override():
        user, error = get_user_from_token(roles={"manager"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        schedule_id = body.get("schedule_id")
        note = (body.get("note") or "").strip() or "Quan ly xac nhan vao ca do thieu nhan su"
        
        # Manager can specify actual check-in time (e.g., when employee arrives late)
        actual_check_in_time_str = (body.get("actual_check_in_time") or "").strip() or None
        actual_check_in_dt = None
        if actual_check_in_time_str:
            try:
                actual_check_in_dt = parse_db_datetime(actual_check_in_time_str)
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid actual_check_in_time format"}), 400

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

        # Ensure there is an open attendance session anchored at real override time.
        open_log = conn.execute(
            """
            SELECT id, check_in_at
            FROM attendance_logs
            WHERE employee_id = ?
              AND branch_id = ?
              AND check_out_at IS NULL
            ORDER BY id DESC
            LIMIT 1
            """,
            (schedule["employee_id"], schedule["branch_id"]),
        ).fetchone()

        attendance_log_id = None
        if open_log:
            schedule_day_dt = datetime.strptime(schedule["week_start"], "%Y-%m-%d") + timedelta(
                days=int(schedule["day_of_week"]) - 1
            )
            try:
                open_check_in_dt = parse_db_datetime(open_log["check_in_at"])
            except (TypeError, ValueError):
                open_check_in_dt = None

            if open_check_in_dt and open_check_in_dt.date() == schedule_day_dt.date():
                # Reuse same-day open session as employee's active check-in.
                attendance_log_id = open_log["id"]
            else:
                # Auto-close stale open session so manager override can create a fresh check-in for this shift.
                conn.execute(
                    """
                    UPDATE attendance_logs
                    SET check_out_at = ?,
                        minutes_worked = COALESCE(minutes_worked, 1)
                    WHERE id = ?
                    """,
                    (format_db_datetime(datetime.now()), open_log["id"]),
                )

        if not attendance_log_id:
            # Calculate scheduled shift start time
            shift_start_dt = shift_start_datetime(schedule["week_start"], schedule["day_of_week"], schedule["shift_code"])
            scheduled_shift_start_at_str = format_db_datetime(shift_start_dt) if shift_start_dt else None
            
            # Calculate minutes late
            check_in_dt = actual_check_in_dt if actual_check_in_dt else datetime.now()
            minutes_late = 0
            if shift_start_dt:
                minutes_late = max(0, int((check_in_dt - shift_start_dt).total_seconds() // 60))
            
            cur = conn.execute(
                """
                INSERT INTO attendance_logs(
                    employee_id, branch_id, check_in_at, confirmed_at, 
                    scheduled_shift_start_at, minutes_late, 
                    checked_in_by_manager_id, manager_check_in_note, note
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    schedule["employee_id"],
                    schedule["branch_id"],
                    format_db_datetime(check_in_dt),
                    format_db_datetime(check_in_dt),
                    scheduled_shift_start_at_str,
                    minutes_late,
                    user["id"],
                    note,
                    "Manager override - thieu nhan su",
                ),
            )
            attendance_log_id = cur.lastrowid

        upsert_shift_attendance_mark(
            conn,
            week_start=schedule["week_start"],
            day_of_week=schedule["day_of_week"],
            shift_code=schedule["shift_code"],
            branch_id=schedule["branch_id"],
            employee_id=schedule["employee_id"],
            status="present_override",
            source="manager_override_shortage",
            note=note,
            attendance_log_id=attendance_log_id,
            marked_by_manager_id=user["id"],
        )
        conn.commit()
        conn.close()

        return jsonify({"message": "Đã cập nhật trạng thái: đã đi làm", "attendance_log_id": attendance_log_id})

    @app.get("/api/manager/self-preferences")
    def manager_self_preferences_get():
        user, error = get_user_from_token(roles={"manager"})
        if error:
            return error

        week_start = (request.args.get("week_start") or "").strip()
        if not week_start:
            return jsonify({"error": "week_start is required"}), 400

        conn = get_conn()
        rows = conn.execute(
            """
                        SELECT id, shift_code, day_of_week, registration_type, group_code, flexible_start_at, flexible_end_at
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

    @app.get("/api/employee/preferences-lock")
    def employee_preferences_lock():
        user, error = get_user_from_token(roles={"employee"})
        if error:
            return error

        week_start = (request.args.get("week_start") or "").strip()
        if not week_start:
            return jsonify({"error": "week_start is required"}), 400

        conn = get_conn()
        saved_count = conn.execute(
            "SELECT COUNT(*) AS c FROM shift_preferences WHERE employee_id = ? AND week_start = ?",
            (user["id"], week_start),
        ).fetchone()["c"]
        conn.close()

        return jsonify(
            {
                "week_start": week_start,
                "locked": saved_count > 0,
                "saved_count": int(saved_count),
            }
        )

    @app.put("/api/manager/self-preferences")
    def manager_self_preferences_put():
        user, error = get_user_from_token(roles={"manager"})
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
            if not isinstance(item, dict):
                return jsonify({"error": "Each selection must be an object"}), 400
            shift_code = item.get("shift_code")
            day_of_week = normalize_day_of_week(item.get("day_of_week"))
            flexible_start_at = _normalize_hhmm(item.get("flexible_start_at"))
            flexible_end_at = _normalize_hhmm(item.get("flexible_end_at"))
            if shift_code not in shift_code_set:
                return jsonify({"error": f"Invalid shift code: {shift_code}"}), 400
            if day_of_week is None:
                return jsonify({"error": "day_of_week must be in range 1..7"}), 400
            if shift_code == "FLEX":
                if not flexible_start_at or not flexible_end_at:
                    return jsonify({"error": "Ca linh hoạt yêu cầu flexible_start_at và flexible_end_at theo định dạng HH:MM"}), 400
                if flexible_end_at <= flexible_start_at:
                    return jsonify({"error": "flexible_end_at phải lớn hơn flexible_start_at"}), 400
            else:
                flexible_start_at = None
                flexible_end_at = None
            key = (shift_code, day_of_week)
            if key in seen:
                continue
            seen.add(key)
            normalized.append((shift_code, day_of_week, flexible_start_at, flexible_end_at))

        if not normalized:
            for shift_code in shifts:
                if shift_code not in shift_code_set:
                    return jsonify({"error": f"Invalid shift code: {shift_code}"}), 400
                for day_of_week in range(1, 8):
                    if shift_code == "FLEX":
                        return jsonify({"error": "Vui lòng chọn FLEX theo từng ngày và nhập giờ vào/ra"}), 400
                    key = (shift_code, day_of_week)
                    if key in seen:
                        continue
                    seen.add(key)
                    normalized.append((shift_code, day_of_week, None, None))

        conn = get_conn()
        conn.execute(
            "DELETE FROM shift_preferences WHERE employee_id = ? AND branch_id = ? AND week_start = ?",
            (user["id"], user["branch_id"], week_start),
        )
        if normalized:
            conn.executemany(
                """
                INSERT INTO shift_preferences(
                    employee_id,
                    week_start,
                    branch_id,
                    shift_code,
                    day_of_week,
                    registration_type,
                    group_code,
                    flexible_start_at,
                    flexible_end_at
                )
                VALUES (?, ?, ?, ?, ?, 'individual', NULL, ?, ?)
                """,
                [
                    (
                        user["id"],
                        week_start,
                        user["branch_id"],
                        shift_code,
                        day_of_week,
                        flexible_start_at,
                        flexible_end_at,
                    )
                    for shift_code, day_of_week, flexible_start_at, flexible_end_at in normalized
                ],
            )
        conn.commit()
        conn.close()
        return jsonify({"message": "Saved manager shift preferences", "count": len(normalized)})

    @app.get("/api/manager/issues")
    def manager_issues():
        user, error = get_user_from_token(roles={"manager"})
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

    @app.post("/api/manager/issues/report-ceo")
    def manager_report_ceo():
        user, error = get_user_from_token(roles={"manager"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        title = (body.get("title") or "").strip()
        details = (body.get("details") or "").strip()
        if not title or not details:
            return jsonify({"error": "title and details are required"}), 400

        conn = get_conn()
        cur = conn.execute(
            """
            INSERT INTO issue_reports(
                reporter_id,
                reporter_role,
                branch_id,
                title,
                details,
                status,
                escalated_to_ceo
            )
            VALUES (?, 'manager', ?, ?, ?, 'escalated', 1)
            """,
            (user["id"], user["branch_id"], title, details),
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "Manager report sent to CEO", "issue_id": cur.lastrowid}), 201

    @app.get("/api/manager/issues/<int:issue_id>/replies")
    def manager_issue_replies(issue_id):
        user, error = get_user_from_token(roles={"manager"})
        if error:
            return error

        conn = get_conn()
        issue = conn.execute(
            "SELECT id, branch_id FROM issue_reports WHERE id = ?",
            (issue_id,),
        ).fetchone()
        if not issue:
            conn.close()
            return jsonify({"error": "Issue not found"}), 404
        if int(issue["branch_id"] or 0) != int(user["branch_id"] or 0):
            conn.close()
            return jsonify({"error": "Forbidden for this branch"}), 403

        rows = conn.execute(
            """
            SELECT r.id,
                   r.issue_id,
                   r.sender_id,
                   r.sender_role,
                   r.message,
                   r.created_at,
                   COALESCE(u.display_name, u.username, 'Unknown') AS sender_name
            FROM issue_report_replies r
            LEFT JOIN users u ON u.id = r.sender_id
            WHERE r.issue_id = ?
            ORDER BY r.id ASC
            """,
            (issue_id,),
        ).fetchall()
        conn.close()
        return jsonify([dict(row) for row in rows])

    @app.post("/api/manager/issues/<int:issue_id>/replies")
    def manager_issue_reply_create(issue_id):
        user, error = get_user_from_token(roles={"manager"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        message = (body.get("message") or "").strip()
        if not message:
            return jsonify({"error": "message is required"}), 400

        conn = get_conn()
        issue = conn.execute(
            "SELECT id, branch_id FROM issue_reports WHERE id = ?",
            (issue_id,),
        ).fetchone()
        if not issue:
            conn.close()
            return jsonify({"error": "Issue not found"}), 404
        if int(issue["branch_id"] or 0) != int(user["branch_id"] or 0):
            conn.close()
            return jsonify({"error": "Forbidden for this branch"}), 403

        conn.execute(
            """
            INSERT INTO issue_report_replies(issue_id, sender_id, sender_role, message, created_at)
            VALUES (?, ?, 'manager', ?, ?)
            """,
            (issue_id, user["id"], message, format_db_datetime(datetime.now())),
        )
        conn.execute(
            "UPDATE issue_reports SET updated_at = ? WHERE id = ?",
            (format_db_datetime(datetime.now()), issue_id),
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "Reply posted"}), 201

    @app.put("/api/manager/issues/<int:issue_id>")
    def manager_issue_update(issue_id):
        user, error = get_user_from_token(roles={"manager"})
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
        user, error = get_user_from_token(roles={"manager"})
        if error:
            return error

        week_start = (request.args.get("week_start") or "").strip()
        if not week_start:
            return jsonify({"error": "week_start is required"}), 400

        conn = get_conn()
        rows = weekly_hours_rows(conn, week_start, branch_id=user["branch_id"])
        conn.close()
        headers, csv_rows = build_weekly_payroll_csv(rows, week_start)
        return csv_response(
            filename=f"payroll_branch_{user['branch_id']}_{week_start}.csv",
            headers=headers,
            rows=csv_rows,
        )

    @app.get("/api/manager/employees")
    def manager_list_employees():
        user, error = get_user_from_token(roles={"manager"})
        if error:
            return error

        keyword = (request.args.get("q") or "").strip()

        conn = get_conn()
        sql = """
            SELECT u.id,
                   u.username,
                   u.display_name,
                   u.full_name,
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
            item["avatar_url"] = f"/api/manager/employees/{item['id']}/avatar"
            employees.append(item)

        return jsonify(
            {
                "employees": employees,
                "branches": [dict(row) for row in branch_rows],
                "default_branch_ids": [user["branch_id"]],
            }
        )

    @app.get("/api/manager/employees/<int:employee_id>/avatar")
    def manager_employee_avatar(employee_id):
        user, error = get_user_from_token(roles={"manager"})
        if error:
            return error

        conn = get_conn()
        if not manager_can_manage_employee(conn, user["branch_id"], employee_id):
            conn.close()
            return jsonify({"error": "You can only view avatars for employees in your branch scope"}), 403

        row = conn.execute(
            "SELECT avatar_data_url FROM users WHERE id = ? AND role = 'employee'",
            (employee_id,),
        ).fetchone()
        conn.close()
        if not row:
            return jsonify({"error": "User not found"}), 404

        data_url = (row["avatar_data_url"] or "").strip()
        if not data_url:
            return ("", 404)

        match = re.match(r"^data:image/([a-zA-Z0-9.+-]+);base64,(.+)$", data_url)
        if not match:
            return ("", 404)

        image_format = match.group(1).lower()
        image_b64 = match.group(2)
        try:
            image_bytes = base64.b64decode(image_b64, validate=True)
        except (ValueError, base64.binascii.Error):
            return ("", 404)

        response = Response(image_bytes, mimetype=f"image/{image_format}")
        response.headers["Cache-Control"] = "private, no-store, no-cache, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    @app.post("/api/manager/employees")
    def manager_create_employee():
        user, error = get_user_from_token(roles={"manager"})
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
        user, error = get_user_from_token(roles={"manager"})
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
        if not manager_can_manage_employee(conn, user["branch_id"], employee_id):
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
        user, error = get_user_from_token(roles={"manager"})
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
        if not manager_can_manage_employee(conn, user["branch_id"], employee_id):
            conn.close()
            return jsonify({"error": "You can only delete employees in your branch scope"}), 403

        conn.execute("DELETE FROM users WHERE id = ?", (employee_id,))
        conn.commit()
        conn.close()
        return jsonify({"message": "Employee account deleted"})
