from __future__ import annotations

from pathlib import Path

from app.backup.config import BackupConfig
from app.backup.restore import PostgresRestoreService


def main() -> int:
    project_root = Path(__file__).resolve().parent
    config = BackupConfig.from_project_root(project_root=project_root)

    restore_service = PostgresRestoreService(config)

    if config.local_db_for_using:
        result = restore_service.restore_backup(config.local_db_for_using)
    else:
        result = restore_service.restore_latest_backup()

    print("=" * 60)
    print("LOCAL RESTORE RESULT")
    print(f"success          : {result.success}")
    print(f"message          : {result.message}")
    print(f"started_at       : {result.started_at}")
    print(f"finished_at      : {result.finished_at}")
    print(f"duration_seconds : {result.duration_seconds:.2f}")
    print(f"backup_file      : {result.backup_file}")
    print("=" * 60)

    return 0 if result.success else 1


if __name__ == "__main__":
    raise SystemExit(main())