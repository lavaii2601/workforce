from datetime import datetime, timedelta

SHIFT_HOURS = {
    "S1": 4,
    "S2": 4,
    "S3": 4,
    "S4": 3,
}


def should_trigger_jarvis(message):
    lower = (message or "").lower()
    keywords = [
        "jarvis",
        "openjarvis",
        "bat thuong",
        "bất thường",
        "it hon",
        "ít hơn",
        "nghi",
        "nghỉ",
    ]
    return any(keyword in lower for keyword in keywords)


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
