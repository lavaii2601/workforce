from flask import jsonify, request
from werkzeug.security import generate_password_hash

from ..services.openjarvis_service import generate_jarvis_response


def register_leadership_routes(app, deps):
    get_conn = deps["get_conn"]
    get_user_from_token = deps["_get_user_from_token"]
    weekly_hours_rows = deps["_weekly_hours_rows"]
    csv_response = deps["_csv_response"]
    create_audit_log = deps["_create_audit_log"]
    parse_pagination = deps["_parse_pagination"]
    is_valid_ipv4 = deps["_is_valid_ipv4"]
    build_branch_create_audit_details = deps["_build_branch_create_audit_details"]
    build_branch_update_audit_details = deps["_build_branch_update_audit_details"]
    build_branch_delete_audit_details = deps["_build_branch_delete_audit_details"]

    @app.get("/api/ceo/chat")
    def get_ceo_chat():
        _, error = get_user_from_token(roles={"ceo"})
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
        _, error = get_user_from_token(roles={"ceo"})
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
        _, error = get_user_from_token(roles={"ceo"})
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

        rows = weekly_hours_rows(conn, week_start, branch_id=branch_id)
        conn.close()
        csv_rows = [
            [
                item["employee_id"],
                item["username"],
                item["employee_name"],
                item["role"],
                item["branch_name"],
                round(item["total_minutes"] / 60, 2),
                item["attendance_sessions"],
                item.get("work_dates", ""),
                item.get("check_in_times", ""),
                item.get("check_out_times", ""),
                item.get("late_minutes_total", 0),
                item.get("penalty_minutes_recommended", 0),
                item.get("late_shortage_override_sessions", 0),
                week_start,
            ]
            for item in rows
        ]
        return csv_response(
            filename=f"payroll_{branch_label}_{week_start}.csv",
            headers=[
                "employee_id",
                "username",
                "employee_name",
                "role",
                "branch_scope",
                "hours_worked",
                "attendance_sessions",
                "work_dates",
                "check_in_times",
                "check_out_times",
                "late_minutes_total",
                "penalty_minutes_recommended",
                "late_shortage_override_sessions",
                "week_start",
            ],
            rows=csv_rows,
        )

    @app.post("/api/ceo/chat")
    def post_ceo_chat():
        user, error = get_user_from_token(roles={"ceo"})
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

        history_rows = conn.execute(
            """
            SELECT sender_type, message
            FROM ceo_chat_messages
            ORDER BY id DESC
            LIMIT 24
            """
        ).fetchall()

        history = []
        for row in reversed(history_rows):
            sender_type = (row["sender_type"] or "").strip().lower()
            if sender_type == "jarvis":
                role = "assistant"
            elif sender_type == "user":
                role = "user"
            else:
                continue

            text = (row["message"] or "").strip()
            if text:
                history.append({"role": role, "content": text})

        jarvis_message = generate_jarvis_response(conn, message, chat_history=history)
        conn.execute(
            """
            INSERT INTO ceo_chat_messages(sender_id, sender_type, sender_label, message)
            VALUES (?, 'jarvis', 'OpenJarvis AI', ?)
            """,
            (user["id"], jarvis_message),
        )

        conn.commit()
        conn.close()
        return jsonify({"message": "Message sent"}), 201

    @app.get("/api/admin/users")
    def admin_users():
        _, error = get_user_from_token(roles={"ceo"})
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
        actor, error = get_user_from_token(roles={"ceo"})
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
            create_audit_log(
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
        create_audit_log(
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
        actor, error = get_user_from_token(roles={"ceo"})
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
        actor, error = get_user_from_token(roles={"ceo"})
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
        create_audit_log(
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
        _, error = get_user_from_token(roles={"ceo"})
        if error:
            return error

        page, page_size, parse_error = parse_pagination(default_page=1, default_page_size=8)
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
        actor, error = get_user_from_token(roles={"ceo"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        name = (body.get("name") or "").strip()
        location = (body.get("location") or "").strip() or None
        network_ip = (body.get("network_ip") or "").strip() or None
        if not name:
            return jsonify({"error": "name is required"}), 400
        if network_ip and not is_valid_ipv4(network_ip):
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
        create_audit_log(
            conn,
            actor,
            action="branch.create",
            target_type="branch",
            target_id=cur.lastrowid,
            details=build_branch_create_audit_details(name, location, network_ip),
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "Branch created", "branch_id": cur.lastrowid}), 201

    @app.put("/api/admin/branches/<int:branch_id>")
    def admin_update_branch(branch_id):
        actor, error = get_user_from_token(roles={"ceo"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        name = (body.get("name") or "").strip()
        location = (body.get("location") or "").strip() or None
        if not name:
            return jsonify({"error": "name is required"}), 400

        conn = get_conn()
        branch = conn.execute(
            "SELECT id, name, location, network_ip FROM branches WHERE id = ?",
            (branch_id,),
        ).fetchone()
        if not branch:
            conn.close()
            return jsonify({"error": "Branch not found"}), 404

        if "network_ip" in body:
            network_ip = (body.get("network_ip") or "").strip() or None
            if network_ip and not is_valid_ipv4(network_ip):
                conn.close()
                return jsonify({"error": "network_ip must be a valid IPv4 address"}), 400
        else:
            network_ip = branch["network_ip"]

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
        create_audit_log(
            conn,
            actor,
            action="branch.update",
            target_type="branch",
            target_id=branch_id,
            details=build_branch_update_audit_details(
                branch,
                name=name,
                location=location,
                network_ip=network_ip,
            ),
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "Branch updated"})

    @app.get("/api/admin/branches/<int:branch_id>/employees")
    def admin_branch_employees(branch_id):
        _, error = get_user_from_token(roles={"ceo"})
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
        actor, error = get_user_from_token(roles={"ceo"})
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
        create_audit_log(
            conn,
            actor,
            action="branch.delete",
            target_type="branch",
            target_id=branch_id,
            details=build_branch_delete_audit_details(branch),
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "Branch deleted"})

    @app.get("/api/admin/branch-audit-logs")
    def admin_branch_audit_logs():
        _, error = get_user_from_token(roles={"ceo"})
        if error:
            return error

        page, page_size, parse_error = parse_pagination(default_page=1, default_page_size=10)
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
