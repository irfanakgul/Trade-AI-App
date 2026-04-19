from __future__ import annotations
import os
from pathlib import Path
from app.services.telegram_bot_chat_service import telegram_send_message # type: ignore

from app.backup.config import BackupConfig
from app.backup.service import PostgresBackupService


def main() -> int:
    project_root = Path(__file__).resolve().parent
    config = BackupConfig.from_project_root(project_root=project_root)

    service = PostgresBackupService(config)
    result = service.create_backup()

    print("=" * 60)
    print("BACKUP RESULT")
    print(f"backup_source     : {config.backup_source}")
    print(f"success           : {result.success}")
    print(f"message           : {result.message}")
    print(f"started_at        : {result.started_at}")
    print(f"finished_at       : {result.finished_at}")
    print(f"duration_seconds  : {result.duration_seconds:.2f}")
    print(f"backup_file       : {result.backup_file}")
    print("=" * 60)

    return 0 if result.success else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
        if os.getenv("ENV_TELEGRAM_NOTIF")=="True":
            telegram_send_message(
                title="BACKUP run",
                text="✅ cloud db has been download into local")
    except Exception as e:
        if os.getenv("ENV_TELEGRAM_NOTIF")=="True":
            telegram_send_message(
                title="BACKUP ERROR!",
                text=f"❌ cloud db stopt with error!\nERROR: {e}")