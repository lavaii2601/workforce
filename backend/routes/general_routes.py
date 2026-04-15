import base64
import binascii
import re
import secrets
from datetime import datetime, timedelta

from flask import jsonify, request
from werkzeug.security import check_password_hash, generate_password_hash


MAX_AVATAR_DATA_URL_LENGTH = 350_000


def register_general_routes(app, deps):
    get_conn = deps["get_conn"]
    get_user_from_token = deps["_get_user_from_token"]
    get_access_token = deps["_get_access_token"]
    get_client_ip = deps["_get_client_ip"]
    is_login_rate_limited = deps["_is_login_rate_limited"]
    record_login_failure = deps["_record_login_failure"]
    clear_login_failures = deps["_clear_login_failures"]
    is_stateless_session_enabled = deps["_is_stateless_session_enabled"]
    build_stateless_session_token = deps["_build_stateless_session_token"]
    hash_session_token = deps["_hash_session_token"]
    permission_payload = deps["_permission_payload"]
    build_profile_payload = deps["_build_profile_payload"]
    is_profile_completed = deps["_is_profile_completed"]

    token_lifetime_days = deps["TOKEN_LIFETIME_DAYS"]
    shift_definitions = deps["SHIFT_DEFINITIONS"]
    role_permissions = deps["ROLE_PERMISSIONS"]
    profile_required_roles = deps["PROFILE_REQUIRED_ROLES"]
    meta_cache = deps.get("_meta_cache")

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

        client_ip = get_client_ip()
        if is_login_rate_limited(client_ip, username):
            return jsonify({"error": "Too many failed login attempts. Please try again later."}), 429

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
            record_login_failure(client_ip, username)
            return jsonify({"error": "Invalid username or password"}), 401
        if not user["is_active"]:
            conn.close()
            return jsonify({"error": "User is inactive"}), 403
        if not user["password_hash"] or not check_password_hash(user["password_hash"], password):
            conn.close()
            record_login_failure(client_ip, username)
            return jsonify({"error": "Invalid username or password"}), 401

        stateless_enabled = is_stateless_session_enabled()
        if stateless_enabled:
            token, expires_ts = build_stateless_session_token(user["id"])
            expires_at = datetime.utcfromtimestamp(expires_ts).strftime("%Y-%m-%d %H:%M:%S")
        else:
            expires_at = (datetime.utcnow() + timedelta(days=token_lifetime_days)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            token = secrets.token_urlsafe(32)

        token_hash = hash_session_token(token)
        conn.execute("DELETE FROM auth_sessions WHERE expires_at <= CURRENT_TIMESTAMP")
        if not stateless_enabled:
            conn.execute(
                "INSERT INTO auth_sessions(user_id, token, expires_at) VALUES (?, ?, ?)",
                (user["id"], token_hash, expires_at),
            )
            conn.execute(
                "DELETE FROM auth_sessions WHERE user_id = ? AND token != ?",
                (user["id"], token_hash),
            )
        conn.commit()
        conn.close()
        clear_login_failures(client_ip, username)

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
                "user": permission_payload(user_payload),
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

        client_ip = get_client_ip()
        if is_login_rate_limited(client_ip, username):
            return jsonify({"error": "Too many failed attempts. Please try again later."}), 429

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
            record_login_failure(client_ip, username)
            return jsonify({"error": "Invalid username or password"}), 401
        if not user["is_active"]:
            conn.close()
            return jsonify({"error": "User is inactive"}), 403
        if not user["password_hash"] or not check_password_hash(user["password_hash"], current_password):
            conn.close()
            record_login_failure(client_ip, username)
            return jsonify({"error": "Mật khẩu hiện tại không đúng"}), 401

        conn.execute(
            "UPDATE users SET password_hash = ? WHERE id = ?",
            (generate_password_hash(new_password), user["id"]),
        )
        conn.execute("DELETE FROM auth_sessions WHERE user_id = ?", (user["id"],))
        conn.commit()
        conn.close()
        clear_login_failures(client_ip, username)
        return jsonify({"message": "Đổi mật khẩu thành công. Vui lòng đăng nhập lại."})

    @app.post("/api/logout")
    def logout():
        token = get_access_token()
        if not token:
            return jsonify({"message": "No active token"})

        if is_stateless_session_enabled() and token.startswith("st1."):
            # Stateless tokens cannot be revoked without shared storage.
            return jsonify({"message": "Logged out"})

        token_hash = hash_session_token(token)
        conn = get_conn()
        conn.execute("DELETE FROM auth_sessions WHERE token = ?", (token_hash,))
        conn.commit()
        conn.close()
        return jsonify({"message": "Logged out"})

    @app.post("/api/change-password")
    def change_password():
        user, error = get_user_from_token(required=True)
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
        user, error = get_user_from_token(required=True)
        if error:
            return error
        profile_completed = is_profile_completed(user)
        payload = {
            "id": user["id"],
            "username": user["username"],
            "role": user["role"],
            "display_name": user["display_name"],
            "profile": build_profile_payload(user),
            "profile_completed": profile_completed,
            "needs_profile_completion": (
                user["role"] in profile_required_roles and not profile_completed
            ),
        }
        return jsonify(payload)

    @app.put("/api/profile/me")
    def upsert_my_profile():
        user, error = get_user_from_token(required=True)
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
        if not re.fullmatch(r"\+?[0-9]{7,14}", phone_number):
            return jsonify({"error": "phone_number must be a valid phone number"}), 400
        if not address:
            return jsonify({"error": "address is required"}), 400
        if not avatar_data_url:
            return jsonify({"error": "avatar_data_url is required"}), 400
        if not avatar_data_url.startswith("data:image/"):
            return jsonify({"error": "avatar_data_url must be a valid image data URL"}), 400
        if avatar_data_url.lower().startswith("data:image/svg"):
            return jsonify({"error": "SVG avatars are not supported"}), 400
        if not re.fullmatch(r"data:image/(png|jpeg|jpg|webp|gif);base64,[A-Za-z0-9+/=]+", avatar_data_url, flags=re.IGNORECASE):
            return jsonify({"error": "avatar_data_url must be a valid base64 image data URL"}), 400
        if len(avatar_data_url) > MAX_AVATAR_DATA_URL_LENGTH:
            return jsonify({"error": "Avatar image is too large. Please choose a smaller image."}), 400
        try:
            encoded = avatar_data_url.split(",", 1)[1]
            base64.b64decode(encoded, validate=True)
        except (IndexError, binascii.Error, ValueError):
            return jsonify({"error": "avatar_data_url has invalid base64 content"}), 400

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
        updated_payload = permission_payload(dict(updated))
        return jsonify({"message": "Profile updated", "user": updated_payload})

    @app.get("/api/permissions")
    def permissions_matrix():
        _, error = get_user_from_token(required=True)
        if error:
            return error
        return jsonify({"roles": role_permissions})

    @app.get("/api/meta")
    def meta():
        from datetime import datetime
        
        # Serve from cache if still valid
        if (meta_cache and meta_cache["data"] and 
            datetime.utcnow().timestamp() < meta_cache["expires_at"]):
            return jsonify(meta_cache["data"])
        
        conn = get_conn()
        branches = conn.execute("SELECT id, name FROM branches ORDER BY name").fetchall()
        conn.close()
        
        response_data = {
            "shifts": shift_definitions,
            "branches": [dict(row) for row in branches],
        }
        
        # Update cache
        if meta_cache is not None:
            meta_cache["data"] = response_data
            meta_cache["expires_at"] = datetime.utcnow().timestamp() + 300  # 5 minutes
        
        return jsonify(response_data)

    @app.get("/api/current-user")
    def current_user():
        user, error = get_user_from_token(required=True)
        if error:
            return error

        payload = dict(user)
        if payload["role"] == "manager" and payload["branch_id"]:
            branch = None
            if meta_cache and meta_cache.get("data"):
                for item in meta_cache["data"].get("branches", []):
                    if int(item.get("id", 0)) == int(payload["branch_id"]):
                        branch = item
                        break
            if not branch:
                conn = get_conn()
                row = conn.execute(
                    "SELECT id, name FROM branches WHERE id = ?", (payload["branch_id"],)
                ).fetchone()
                conn.close()
                branch = dict(row) if row else None
            payload["branch"] = branch
        return jsonify({"user": payload})

    @app.get("/api/server-time")
    def server_time():
        return jsonify({"iso": datetime.utcnow().isoformat() + "Z"})
