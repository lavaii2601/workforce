from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str]) -> None:
    print("[quickstart]", " ".join(cmd))
    subprocess.check_call(cmd)


def main() -> int:
    root = Path(__file__).resolve().parent
    venv_dir = root / ".venv"

    if os.name == "nt":
        venv_python = venv_dir / "Scripts" / "python.exe"
    else:
        venv_python = venv_dir / "bin" / "python"

    if not venv_python.exists():
        print("[quickstart] Creating virtual environment...")
        run([sys.executable, "-m", "venv", str(venv_dir)])

    print("[quickstart] Installing dependencies...")
    run([str(venv_python), "-m", "pip", "install", "--upgrade", "pip"])
    run([str(venv_python), "-m", "pip", "install", "-r", str(root / "requirements.txt")])

    print("[quickstart] Starting app on http://127.0.0.1:5000")
    try:
        run([str(venv_python), str(root / "start.py")])
    except KeyboardInterrupt:
        print("\n[quickstart] Server stopped by user.")
        return 0
    except subprocess.CalledProcessError as exc:
        print(f"[quickstart] App exited with code {exc.returncode}.")
        return exc.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
