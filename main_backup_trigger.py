from __future__ import annotations

from pathlib import Path

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
    raise SystemExit(main())