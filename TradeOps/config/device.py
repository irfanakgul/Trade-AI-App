from __future__ import annotations

import hashlib
import platform
import socket
import subprocess
from pathlib import Path


def get_runtime_device_id(username: str) -> str:
    machine_fingerprint = build_machine_fingerprint()
    raw_value = f"{username.strip()}|{machine_fingerprint}"
    device_hash = hashlib.sha256(raw_value.encode("utf-8")).hexdigest()[:12].upper()
    safe_username = username.strip().replace(" ", "_")
    return f"{safe_username}_{device_hash}"


def build_machine_fingerprint() -> str:
    system_name = platform.system().lower()

    if system_name == "darwin":
        machine_id = _get_macos_platform_uuid()
    elif system_name == "linux":
        machine_id = _get_linux_machine_id()
    elif system_name == "windows":
        machine_id = _get_windows_machine_guid()
    else:
        machine_id = None

    fallback_parts = [
        machine_id or "",
        socket.gethostname() or "",
        platform.system() or "",
        platform.machine() or "",
    ]

    return "|".join(fallback_parts)


def _get_macos_platform_uuid() -> str | None:
    try:
        result = subprocess.run(
            ["ioreg", "-rd1", "-c", "IOPlatformExpertDevice"],
            capture_output=True,
            text=True,
            check=True,
        )
        for line in result.stdout.splitlines():
            if "IOPlatformUUID" in line:
                parts = line.split("=")
                if len(parts) == 2:
                    return parts[1].strip().strip('"')
    except Exception:
        return None
    return None


def _get_linux_machine_id() -> str | None:
    try:
        machine_id_path = Path("/etc/machine-id")
        if machine_id_path.exists():
            return machine_id_path.read_text(encoding="utf-8").strip()
    except Exception:
        return None
    return None


def _get_windows_machine_guid() -> str | None:
    try:
        result = subprocess.run(
            [
                "reg",
                "query",
                r"HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Cryptography",
                "/v",
                "MachineGuid",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        for line in result.stdout.splitlines():
            if "MachineGuid" in line:
                parts = line.split()
                if parts:
                    return parts[-1].strip()
    except Exception:
        return None
    return None