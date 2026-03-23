# Workforce Manager (Mobile + Web)

Ung dung quan ly nhan cong da chi nhanh, co the dung tren web va mobile browser.

## Tinh nang da lam

- Nhan vien dang ky ca lam cho tuan toi (co the dang ky nhieu chi nhanh).
- Quan ly chi nhanh xem danh sach dang ky va phan lich tuan.
- Dang nhap co mat khau + token session.
- Phan quyen theo role va trang quan tri role/active cho CEO.
- He thong chatbox tong danh rieng cho CEO, tich hop OpenJarvis de loc nhan vien bat thuong.
- Cua hang truong co the cap tai khoan nhan vien moi va xoa tai khoan nhan vien khi nghi viec.
- Da tao file placeholder cho he thong cham cong: `backend/services/timekeeping_service.py`.
- Giao dien dashboard hien dai, chuyen trang theo vai tro sau khi dang nhap.
- Cham cong, bao cao van de, va luong thong tin cap cao cho CEO.
- Xuat CSV tong gio lam trong tuan de gui phong nhan su.

## 4 ca co dinh

- S1: 07:00 - 11:00
- S2: 11:00 - 15:00
- S3: 15:00 - 19:00
- S4: 19:00 - 22:00

## Tai khoan khoi tao mac dinh

- CEO: `ceo`
- Mat khau mac dinh: `123456`

He thong khong seed chi nhanh, quan ly, hoac nhan vien mac dinh.
CEO dang nhap lan dau de tu tao thong tin doanh nghiep.

## Chay local

### Cach nhanh (1 lenh quickstart)

```bash
cd workforce-manager
python quickstart.py
```

Hoac tren Windows:

```powershell
.\quickstart.ps1
```

```bat
quickstart.bat
```

Lenh quickstart se tu dong tao `.venv`, cai dependencies, va chay app local.

```bash
cd workforce-manager
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python start.py
```

Mo trinh duyet: `http://127.0.0.1:5000`

## Reset database thu cong

Neu ban muon xoa sach du lieu va tao moi hoan toan, dung script rieng:

```bash
python reset_database.py
```

Script nay chi chay khi ban goi thu cong, khong duoc goi tu `start.py` hoac `quickstart.py`.

Neu muon bo qua buoc xac nhan:

```bash
python reset_database.py --yes
```

Ket qua sau reset:

- Tao lai schema day du.
- Seed tai khoan CEO mac dinh theo logic hien tai.

## Kien truc nhanh

- Backend: Flask + SQLite (`data.db`) + REST API.
- Frontend: HTML/CSS/JS responsive (mobile-first).
- Auth demo: chon user va dang nhap qua API `/api/login`.

## Deploy len Vercel

Du an da duoc toi uu de chay tren Vercel voi Flask serverless.

### File da co san cho Vercel

- `api/index.py`: entrypoint serverless (WSGI app).
- `vercel.json`: rewrite moi route ve Flask de phuc vu ca API + frontend.

### Cach deploy

1. Push source code len GitHub.
2. Tren Vercel, chon `New Project` va import repo.
3. Framework Preset de mac dinh (`Other`) la du.
4. Deploy.

### Luu y quan trong ve SQLite tren Vercel

- Vercel co filesystem tam thoi cho function, chi ghi duoc trong `/tmp`.
- Backend da tu dong dung `/tmp/data.db` khi detect moi truong Vercel.
- Du lieu SQLite se **khong ben vung** giua cac lan cold start/redeploy.

Neu can ben vung du lieu production, nen doi sang DB ngoai (Postgres/MySQL/Supabase/Neon).

### Bao mat + Vercel (khuyen nghi)

- Tren Vercel, neu co `DATABASE_URL` (Postgres), he thong se uu tien session luu trong DB (an toan hon va co the revoke).
- Chi nen dung stateless session khi that su can thiet (ho tro bang `STATELESS_SESSION=1`).
- Khi stateless session bat, bat buoc dat `SESSION_TOKEN_SECRET` manh (>= 32 ky tu random), neu khong app se tu choi khoi dong de tranh token de doan.
- Tren Vercel, bat buoc dat `ATTENDANCE_QR_SECRET` manh (>= 32 ky tu random), neu khong app se tu choi khoi dong.

### Cau hinh de du lieu ben vung tren Vercel (khuyen nghi)

1. Tao 1 Postgres database (Neon/Supabase/Railway/Postgres bat ky).
2. Trong Vercel Project -> Settings -> Environment Variables, them:
	- `DATABASE_URL=<postgres-connection-string>`
3. Redeploy project.

Khi co `DATABASE_URL`, backend se tu dong chuyen sang Postgres.
Neu khong co, he thong fallback sang SQLite (phu hop local/dev, khong phu hop production tren Vercel).

### Bien moi truong tuy chon

- `SQLITE_PATH`: ghi de duong dan file SQLite (uu tien cao nhat).
- `DATABASE_URL`: chuoi ket noi Postgres (uu tien cao nhat, dung cho production).
- `SUPABASE_DATABASE_URL`: fallback khi ban muon dat rieng connection string Supabase.
- Bao mat:
	- `SESSION_TOKEN_SECRET`: secret ky token phien dang nhap (nen dat chuoi dai, random).
	- `ATTENDANCE_QR_SECRET`: secret ky QR cham cong (nen khac voi secret khac).
	- `STATELESS_SESSION`: dat `1` neu ban muon bat stateless token tren moi truong khong dung DB session.
	- `FLASK_DEBUG`: de `0` trong production (mac dinh an toan).
- Nhom OpenJarvis (neu dung):
	- `OPENJARVIS_ENABLED`
	- `OPENJARVIS_API_URL`
	- `OPENJARVIS_MODEL`
	- `OPENJARVIS_TIMEOUT_SECONDS`
	- `OPENJARVIS_TEMPERATURE`
	- `OPENJARVIS_MAX_TOKENS`

## API chinh

- `POST /api/login`
- `POST /api/logout`
- `POST /api/change-password`
- `GET /api/meta`
- `GET /api/employee/branches`
- `GET/PUT /api/employee/preferences`
- `GET/PUT /api/manager/schedule`
- `GET /api/manager/preferences`
- `GET/POST /api/manager/employees`
	- Ho tro `GET /api/manager/employees?q=<tu_khoa>` de tim theo ten, username, so dien thoai.
- `DELETE /api/manager/employees/<id>`
- `POST /api/attendance/check-in`
- `POST /api/attendance/check-in-qr-one-time`
- `POST /api/attendance/scan-qr-one-time`
- `POST /api/attendance/check-out`
- `GET /api/attendance/my-week`
- `POST /api/manager/attendance-qr-one-time`
- `GET /api/manager/attendance-shifts/today`
- `PUT /api/manager/attendance-shifts/override`
- `POST /api/issues`
- `GET /api/issues/my`
- `GET /api/manager/issues`
- `PUT /api/manager/issues/<id>`
- `GET /api/manager/payroll-export.csv`
- `GET /api/manager/self-preferences`
- `PUT /api/manager/self-preferences`
- `GET/POST /api/ceo/chat`
- `GET /api/ceo/issues`
- `GET /api/ceo/payroll-export.csv`
- `GET /api/admin/users`
- `PUT /api/admin/users/<id>`

## Quy tắc chấm công one-time mới

- QR one-time hết hạn vào 24h00 (cuối ngày hiện tại theo thời gian server).
- Nhân viên được check-in từ trước ca đến trễ tối đa 15 phút sau giờ bắt đầu ca.
- Quá 15 phút: hệ thống tự đánh vắng cho ca đó và từ chối check-in.
- Quản lý có thể vào màn chấm công để sửa trạng thái sang `đã đi làm` khi có bằng chứng hợp lệ.

## OpenJarvis trong chat CEO

- Khi CEO nhap cau co tu khoa nhu `jarvis`, `bat thuong`, `nghi`, he thong se tao them 1 tin phan hoi tu OpenJarvis.
- He thong uu tien goi OpenJarvis API (`/v1/chat/completions`) theo chuan OpenAI-compatible.
- Neu OpenJarvis khong san sang, he thong tu dong fallback ve bao cao noi bo de khong lam gian doan chatbox.
- OpenJarvis va fallback deu phan tich tren du lieu `weekly_schedule`:
	- Nhan vien gio thap (< 12 gio/tuan).
	- Nhan vien nghi lien tiep >= 2 tuan gan nhat.
- Bao cao se chinh xac hon sau khi module cham cong duoc cap nhat du lieu check-in/check-out.

### Cau hinh OpenJarvis (tu chon)

- `OPENJARVIS_ENABLED` (mac dinh: `1`)
- `OPENJARVIS_API_URL` (mac dinh: `http://127.0.0.1:8000`)
- `OPENJARVIS_MODEL` (mac dinh: `qwen3:8b`)
- `OPENJARVIS_TIMEOUT_SECONDS` (mac dinh: `6`)
- `OPENJARVIS_TEMPERATURE` (mac dinh: `0.2`)
- `OPENJARVIS_MAX_TOKENS` (mac dinh: `700`)
