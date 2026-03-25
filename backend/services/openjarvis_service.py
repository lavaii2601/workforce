from datetime import datetime, timedelta
import json
import os
from urllib import error as urlerror
from urllib import request as urlrequest

SHIFT_HOURS = {
    "S1": 4,
    "S2": 4,
    "S3": 4,
    "S4": 3,
}


def _as_float(value, default):
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _as_int(value, default):
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _openjarvis_config():
    base_url = (os.getenv("OPENJARVIS_API_URL") or "http://127.0.0.1:8000").strip().rstrip("/")
    enabled = (os.getenv("OPENJARVIS_ENABLED") or "1").strip().lower() not in {"0", "false", "no", "off"}
    return {
        "enabled": enabled,
        "base_url": base_url,
        "model": (os.getenv("OPENJARVIS_MODEL") or "qwen3:8b").strip(),
        "temperature": _as_float(os.getenv("OPENJARVIS_TEMPERATURE"), 0.2),
        "max_tokens": _as_int(os.getenv("OPENJARVIS_MAX_TOKENS"), 700),
        "timeout_seconds": _as_float(os.getenv("OPENJARVIS_TIMEOUT_SECONDS"), 6.0),
    }


def _hours_by_employee_from_schedule(conn, week_start):
    rows = conn.execute(
        """
        SELECT u.id AS employee_id,
               u.display_name AS employee_name,
               ws.shift_code
        FROM users u
        LEFT JOIN weekly_schedule ws
               ON ws.employee_id = u.id
              AND ws.week_start = ?
        WHERE u.role = 'employee'
        """,
        (week_start,),
    ).fetchall()

    result = {}
    for row in rows:
        employee_id = row["employee_id"]
        if employee_id not in result:
            result[employee_id] = {
                "employee_name": row["employee_name"],
                "hours": 0,
                "shift_count": 0,
            }
        shift_code = row["shift_code"]
        if shift_code:
            result[employee_id]["hours"] += SHIFT_HOURS.get(shift_code, 0)
            result[employee_id]["shift_count"] += 1
    return result


def _hours_by_employee_from_attendance(conn, week_start):
    start = datetime.strptime(week_start, "%Y-%m-%d")
    end = start + timedelta(days=7)
    rows = conn.execute(
        """
        SELECT u.id AS employee_id,
               u.display_name AS employee_name,
               COALESCE(SUM(COALESCE(a.minutes_worked, 0)), 0) AS total_minutes,
               COUNT(a.id) AS sessions
        FROM users u
        LEFT JOIN attendance_logs a
               ON a.employee_id = u.id
              AND a.check_in_at >= ?
              AND a.check_in_at < ?
        WHERE u.role = 'employee'
        GROUP BY u.id, u.display_name
        """,
        (start.strftime("%Y-%m-%d 00:00:00"), end.strftime("%Y-%m-%d 00:00:00")),
    ).fetchall()

    result = {}
    for row in rows:
        result[row["employee_id"]] = {
            "employee_name": row["employee_name"],
            "hours": round((row["total_minutes"] or 0) / 60, 2),
            "session_count": row["sessions"],
        }
    return result


def _absence_streak_weeks(conn, employee_id, candidate_weeks):
    streak = 0
    for week_start in candidate_weeks:
        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM weekly_schedule
            WHERE employee_id = ? AND week_start = ?
            """,
            (employee_id, week_start),
        ).fetchone()
        if row["c"] == 0:
            streak += 1
        else:
            break
    return streak


def generate_hr_anomaly_report(conn, question):
    week_row = conn.execute(
        "SELECT week_start FROM weekly_schedule GROUP BY week_start ORDER BY week_start DESC LIMIT 1"
    ).fetchone()

    if not week_row:
        return (
            "OpenJarvis: Chua co du lieu lich lam de phan tich bat thuong. "
            "Quan ly can phan lich tuan truoc khi danh gia nhan su."
        )

    latest_week = week_row["week_start"]
    plan_hours_map = _hours_by_employee_from_schedule(conn, latest_week)
    attendance_hours_map = _hours_by_employee_from_attendance(conn, latest_week)

    recent_weeks = conn.execute(
        "SELECT week_start FROM weekly_schedule GROUP BY week_start ORDER BY week_start DESC LIMIT 4"
    ).fetchall()
    candidate_weeks = [row["week_start"] for row in recent_weeks]

    low_hours = []
    long_absence = []

    for employee_id, stats in plan_hours_map.items():
        actual = attendance_hours_map.get(employee_id, {"hours": 0, "session_count": 0})
        effective_hours = actual["hours"] if actual["hours"] > 0 else stats["hours"]
        if effective_hours < 12:
            low_hours.append(
                (
                    stats["employee_name"],
                    effective_hours,
                    stats["shift_count"],
                    actual.get("session_count", 0),
                )
            )

        streak = _absence_streak_weeks(conn, employee_id, candidate_weeks)
        if streak >= 2:
            long_absence.append((stats["employee_name"], streak))

    low_hours.sort(key=lambda item: item[1])
    long_absence.sort(key=lambda item: item[1], reverse=True)

    lines = [
        f"OpenJarvis bao cao nhan su bat thuong (tuan {latest_week})",
        "- Tieu chi gio thap: < 12 gio/tuan",
        "- Tieu chi nghi dai: >= 2 tuan lien tiep khong co ca",
    ]

    if not low_hours and not long_absence:
        lines.append("- Khong phat hien bat thuong ro rang trong du lieu hien tai.")
    else:
        if low_hours:
            lines.append("- Nhan vien gio thap:")
            for name, hours, shift_count, session_count in low_hours:
                lines.append(
                    f"  * {name}: {hours} gio (ke hoach {shift_count} ca, cham cong {session_count} phien)"
                )
        if long_absence:
            lines.append("- Nhan vien nghi nhieu tuan:")
            for name, weeks in long_absence:
                lines.append(f"  * {name}: {weeks} tuan lien tiep")

    escalated_issues = conn.execute(
        """
        SELECT i.title, u.display_name AS reporter_name, COALESCE(b.name, '-') AS branch_name
        FROM issue_reports i
        JOIN users u ON u.id = i.reporter_id
        LEFT JOIN branches b ON b.id = i.branch_id
        WHERE i.escalated_to_ceo = 1 OR i.status = 'escalated'
        ORDER BY i.id DESC
        LIMIT 5
        """
    ).fetchall()
    if escalated_issues:
        lines.append("- Van de can cap cao xu ly:")
        for item in escalated_issues:
            lines.append(
                f"  * [{item['branch_name']}] {item['reporter_name']}: {item['title']}"
            )

    lines.append("- Nguon du lieu: lich phan ca + cham cong + bao cao van de.")
    lines.append(f"- Truy van goc: {question}")
    lines.append(f"- Thoi diem tao bao cao: {datetime.utcnow().isoformat()}Z")

    return "\n".join(lines)


def _call_openjarvis_chat(*, messages, config):
    payload = {
        "model": config["model"],
        "messages": messages,
        "temperature": config["temperature"],
        "max_tokens": config["max_tokens"],
        "stream": False,
    }
    req = urlrequest.Request(
        f"{config['base_url']}/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urlrequest.urlopen(req, timeout=config["timeout_seconds"]) as resp:
        raw = resp.read().decode("utf-8", errors="replace")

    body = json.loads(raw)
    choices = body.get("choices") or []
    if not choices:
        return None

    message = choices[0].get("message") or {}
    content = (message.get("content") or "").strip()
    return content or None


def _should_include_hr_context(question):
    lower = (question or "").lower()
    hints = [
        "bat thuong",
        "bất thường",
        "nhan su",
        "nhân sự",
        "ca lam",
        "ca làm",
        "cham cong",
        "chấm công",
        "vang",
        "vắng",
        "nghi",
        "nghỉ",
        "gio",
        "giờ",
        "branch",
        "chi nhanh",
        "chi nhánh",
        "issue",
        "bao cao",
        "báo cáo",
        "rủi ro",
        "rui ro",
    ]
    return any(item in lower for item in hints)


def _build_local_fallback(question, hr_context):
    if hr_context:
        return (
            "OpenJarvis dang tam thoi khong san sang. Toi tra ve phan tich noi bo de CEO tiep tuc van hanh:\n\n"
            f"{hr_context}"
        )

    return (
        "OpenJarvis dang tam thoi khong ket noi duoc den model. "
        "CEO co the tiep tuc hoi, hoac bo sung cau hoi lien quan nhan su/cham cong/chi nhanh "
        "de toi phan tich du lieu noi bo ngay khi ket noi phuc hoi."
    )


def generate_jarvis_response(conn, question, chat_history=None):
    include_hr_context = _should_include_hr_context(question)
    hr_context = generate_hr_anomaly_report(conn, question) if include_hr_context else None
    config = _openjarvis_config()
    if not config["enabled"]:
        return _build_local_fallback(question, hr_context)

    system_prompt = (
        "Ban la OpenJarvis dong vai tro tro ly CEO cho he thong workforce manager. "
        "Tro chuyen tu nhien nhu mot AI assistant, tra loi bang tieng Viet ro rang, ngan gon nhung day du. "
        "Khi co du lieu van hanh thi dua ra nhan dinh va de xuat hanh dong cu the. "
        "Duoc phep dung Markdown (tieu de, bullet, bang, code block) neu phu hop. "
        "Khong duoc tu y biet du lieu ben ngoai he thong."
    )

    messages = [{"role": "system", "content": system_prompt}]
    for item in (chat_history or []):
        role = (item.get("role") or "").strip().lower()
        content = (item.get("content") or "").strip()
        if role not in {"user", "assistant"} or not content:
            continue
        messages.append({"role": role, "content": content})

    if include_hr_context and hr_context:
        messages.append(
            {
                "role": "system",
                "content": (
                    "Day la du lieu noi bo cap nhat nhat de tham chieu khi nguoi dung hoi ve van hanh/nhan su:\n\n"
                    f"{hr_context}"
                ),
            }
        )

    messages.append({"role": "user", "content": question})

    try:
        ai_answer = _call_openjarvis_chat(messages=messages, config=config)
        if ai_answer:
            return ai_answer
    except (urlerror.URLError, TimeoutError, json.JSONDecodeError, ValueError):
        # Fall back to local response when OpenJarvis is unavailable.
        pass

    return _build_local_fallback(question, hr_context)
