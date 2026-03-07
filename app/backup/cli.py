from __future__ import annotations

import argparse
from pathlib import Path

from app.backup.config import BackupConfig
from app.backup.restore import PostgresRestoreService
from app.backup.service import PostgresBackupService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="PostgreSQL backup / restore utility")

    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parents[2]),
        help="Proje root path",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help=".env dosya yolu",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    backup_parser = subparsers.add_parser("backup", help="Cloud PostgreSQL backup al")
    backup_parser.add_argument(
        "--retention-days",
        type=int,
        default=None,
        help="Eski backup saklama günü",
    )

    restore_parser = subparsers.add_parser("restore-latest", help="En son backup'ı local DB'ye restore et")

    restore_file_parser = subparsers.add_parser("restore-file", help="Belirli backup dosyasını local DB'ye restore et")
    restore_file_parser.add_argument("--backup-file", required=True, help="Restore edilecek .dump veya .dump.gz dosyası")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    config = BackupConfig.from_project_root(
        project_root=args.project_root,
        env_file=args.env_file,
    )

    if getattr(args, "retention_days", None) is not None:
        config.retention_days = args.retention_days

    if args.command == "backup":
        service = PostgresBackupService(config)
        result = service.create_backup()
        print(result.message)
        return 0 if result.success else 1

    if args.command == "restore-latest":
        service = PostgresRestoreService(config)
        result = service.restore_latest_backup()
        print(result.message)
        return 0 if result.success else 1

    if args.command == "restore-file":
        service = PostgresRestoreService(config)
        result = service.restore_backup(args.backup_file)
        print(result.message)
        return 0 if result.success else 1

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())