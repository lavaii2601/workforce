from flask import jsonify, request
from werkzeug.security import generate_password_hash
from datetime import datetime

from ..services.openjarvis_service import generate_jarvis_response


def register_leadership_routes(app, deps):
    get_conn = deps["get_conn"]
    get_user_from_token = deps["_get_user_from_token"]
    weekly_hours_rows = deps["_weekly_hours_rows"]
    weekly_attendance_detail_rows = deps["_weekly_attendance_detail_rows"]
    csv_sections_response = deps["_csv_sections_response"]
    build_weekly_payroll_csv = deps["_build_weekly_payroll_csv"]
    build_weekly_payroll_csv_sections = deps["_build_weekly_payroll_csv_sections"]
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

    @app.get("/api/ceo/issues/<int:issue_id>/replies")
    def ceo_issue_replies(issue_id):
        _, error = get_user_from_token(roles={"ceo"})
        if error:
            return error

        conn = get_conn()
        issue = conn.execute(
            """
            SELECT id
            FROM issue_reports
            WHERE id = ?
              AND (escalated_to_ceo = 1 OR status = 'escalated')
            """,
            (issue_id,),
        ).fetchone()
        if not issue:
            conn.close()
            return jsonify({"error": "Issue not found or not escalated to CEO"}), 404

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

    @app.post("/api/ceo/issues/<int:issue_id>/replies")
    def ceo_issue_reply_create(issue_id):
        user, error = get_user_from_token(roles={"ceo"})
        if error:
            return error

        body = request.get_json(silent=True) or {}
        message = (body.get("message") or "").strip()
        if not message:
            return jsonify({"error": "message is required"}), 400

        conn = get_conn()
        issue = conn.execute(
            """
            SELECT id
            FROM issue_reports
            WHERE id = ?
              AND (escalated_to_ceo = 1 OR status = 'escalated')
            """,
            (issue_id,),
        ).fetchone()
        if not issue:
            conn.close()
            return jsonify({"error": "Issue not found or not escalated to CEO"}), 404

        now_raw = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            """
            INSERT INTO issue_report_replies(issue_id, sender_id, sender_role, message, created_at)
            VALUES (?, ?, 'ceo', ?, ?)
            """,
            (issue_id, user["id"], message, now_raw),
        )
        conn.execute(
            "UPDATE issue_reports SET updated_at = ? WHERE id = ?",
            (now_raw, issue_id),
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "Reply posted"}), 201

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
        detail_rows = weekly_attendance_detail_rows(conn, week_start, branch_id=branch_id)
        conn.close()
        sections = build_weekly_payroll_csv_sections(rows, detail_rows, week_start)
        return csv_sections_response(
            filename=f"payroll_{branch_label}_{week_start}.csv",
            sections=sections,
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

        if history and history[-1]["role"] == "user" and history[-1]["content"] == message:
            history.pop()

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

            existing_manager = conn.execute(
                """
                SELECT id, username, display_name
                FROM users
                WHERE role = 'manager' AND branch_id = ?
                LIMIT 1
                """,
                (branch_id,),
            ).fetchone()
            if existing_manager:
                conn.close()
                return jsonify(
                    {
                        "error": "Each branch can have only one manager",
                        "branch_id": branch_id,
                        "existing_manager": {
                            "id": existing_manager["id"],
                            "username": existing_manager["username"],
                            "display_name": existing_manager["display_name"],
                        },
                    }
                ), 409

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
        else:
            try:
                branch_id = int(branch_id)
            except (TypeError, ValueError):
                conn.close()
                return jsonify({"error": "branch_id is required for manager role"}), 400

            valid_branch = conn.execute("SELECT id FROM branches WHERE id = ?", (branch_id,)).fetchone()
            if not valid_branch:
                conn.close()
                return jsonify({"error": "Invalid branch_id"}), 400

            existing_manager = conn.execute(
                """
                SELECT id, username, display_name
                FROM users
                WHERE role = 'manager'
                  AND branch_id = ?
                  AND id != ?
                LIMIT 1
                """,
                (branch_id, user_id),
            ).fetchone()
            if existing_manager:
                conn.close()
                return jsonify(
                    {
                        "error": "Each branch can have only one manager",
                        "branch_id": branch_id,
                        "existing_manager": {
                            "id": existing_manager["id"],
                            "username": existing_manager["username"],
                            "display_name": existing_manager["display_name"],
                        },
                    }
                ), 409

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
            where_clause = "WHERE b.name LIKE ? COLLATE NOCASE"
            params.append(f"%{query}%")

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
                   COALESCE(mgr.manager_count, 0) AS manager_count,
                   COALESCE(emp.employee_count, 0) AS employee_count
            FROM branches b
            LEFT JOIN (
                SELECT branch_id, COUNT(*) AS manager_count
                FROM users
                WHERE role = 'manager'
                GROUP BY branch_id
            ) mgr ON mgr.branch_id = b.id
            LEFT JOIN (
                SELECT branch_id, COUNT(DISTINCT employee_id) AS employee_count
                FROM employee_branch_access
                GROUP BY branch_id
            ) emp ON emp.branch_id = b.id
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

        member_rows = conn.execute(
            """
            SELECT u.id,
                   u.username,
                   u.display_name,
                   u.is_active,
                   u.role
            FROM users u
            LEFT JOIN employee_branch_access eba
                   ON eba.employee_id = u.id
                  AND eba.branch_id = ?
            WHERE (u.role = 'manager' AND u.branch_id = ?)
               OR (u.role = 'employee' AND eba.employee_id IS NOT NULL)
            ORDER BY u.role, u.display_name
            """,
            (branch_id, branch_id),
        ).fetchall()
        conn.close()

        managers = []
        employees = []
        for row in member_rows:
            item = dict(row)
            item.pop("role", None)
            if row["role"] == "manager":
                managers.append(item)
            else:
                employees.append(item)

        return jsonify(
            {
                "branch": dict(branch),
                "managers": managers,
                "employees": employees,
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
