from __future__ import annotations

import gzip
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from app.backup.config import BackupConfig
from app.backup.logger import get_logger


@dataclass
class RestoreResult:
    success: bool
    backup_file: Path
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    message: str


class PostgresRestoreService:
    def __init__(self, config: BackupConfig):
        self.config = config
        self.logger = get_logger(
            name="postgres_restore",
            log_file=self.config.logs_dir / "restore.out.log",
        )

    def _get_latest_backup(self) -> Path:
        candidates = list(self.config.backup_dir.glob("*.dump")) + list(self.config.backup_dir.glob("*.dump.gz"))
        if not candidates:
            raise FileNotFoundError(f"Backup bulunamadı: {self.config.backup_dir}")
        return max(candidates, key=lambda p: p.stat().st_mtime)

    def _ensure_local_db_exists(self) -> None:
        env = os.environ.copy()
        if self.config.local_db_password:
            env["PGPASSWORD"] = self.config.local_db_password

        check_cmd = [
            "psql",
            "-h", self.config.local_db_host,
            "-p", str(self.config.local_db_port),
            "-U", self.config.local_db_user,
            "-d", "postgres",
            "-tAc",
            f"SELECT 1 FROM pg_database WHERE datname = '{self.config.local_db_name}'",
        ]

        self.logger.info(
            "Local DB kontrol ediliyor | host=%s | port=%s | db=%s | user=%s",
            self.config.local_db_host,
            self.config.local_db_port,
            self.config.local_db_name,
            self.config.local_db_user,
        )

        result = subprocess.run(
            check_cmd,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            raise RuntimeError(f"Local DB kontrol edilemedi: {result.stderr.strip()}")

        exists = result.stdout.strip() == "1"
        if exists:
            self.logger.info("Local DB zaten mevcut | db=%s", self.config.local_db_name)
            return

        create_cmd = [
            self.config.createdb_path,
            "-h", self.config.local_db_host,
            "-p", str(self.config.local_db_port),
            "-U", self.config.local_db_user,
            self.config.local_db_name,
        ]

        create_result = subprocess.run(
            create_cmd,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

        if create_result.returncode != 0:
            raise RuntimeError(f"Local DB oluşturulamadı: {create_result.stderr.strip()}")

        self.logger.info("Local DB oluşturuldu | db=%s", self.config.local_db_name)

    def restore_latest_backup(self) -> RestoreResult:
        latest_backup = self._get_latest_backup()
        return self.restore_backup(latest_backup)

    def restore_backup(self, backup_file: str | Path) -> RestoreResult:
        started_at = datetime.now()

        raw_backup_path = Path(backup_file)

        if raw_backup_path.is_absolute():
            backup_path = raw_backup_path.resolve()
        else:
            candidate_in_backup_dir = (self.config.backup_dir / raw_backup_path.name).resolve()
            if candidate_in_backup_dir.exists():
                backup_path = candidate_in_backup_dir
            else:
                backup_path = raw_backup_path.resolve()

        if not backup_path.exists():
            raise FileNotFoundError(f"Backup dosyası bulunamadı: {backup_path}")

        self.logger.info(
            "Restore başladı | backup=%s | local_db=%s",
            backup_path,
            self.config.local_db_name,
        )

        env = os.environ.copy()
        if self.config.local_db_password:
            env["PGPASSWORD"] = self.config.local_db_password

        temp_dump_path: Path | None = None
        temp_list_path: Path | None = None

        try:
            self._ensure_local_db_exists()

            restore_source = backup_path

            if backup_path.suffix == ".gz":
                tmp_dir = Path(tempfile.mkdtemp(prefix="pg_restore_"))
                temp_dump_path = tmp_dir / backup_path.stem

                with gzip.open(backup_path, "rb") as src, temp_dump_path.open("wb") as dst:
                    shutil.copyfileobj(src, dst)

                restore_source = temp_dump_path
                self.logger.info("Gzip backup açıldı | temp_file=%s", restore_source)

            # Restore listesi çıkar
            list_cmd = [
                self.config.pg_restore_path,
                "-l",
                str(restore_source),
            ]

            list_result = subprocess.run(
                list_cmd,
                env=env,
                capture_output=True,
                text=True,
                check=True,
            )

            toc_lines = list_result.stdout.splitlines()

            # pg_repack extension ve comment satırlarını hariç tut
            filtered_lines = []
            skipped_lines = []

            for line in toc_lines:
                lower_line = line.lower().strip()

                is_pgrepack_extension = ("pg_repack" in lower_line and "extension" in lower_line)
                is_pgrepack_comment = ("pg_repack" in lower_line and "comment" in lower_line)

                if is_pgrepack_extension or is_pgrepack_comment:
                    skipped_lines.append(line)
                    continue

                filtered_lines.append(line)

            tmp_list_dir = Path(tempfile.mkdtemp(prefix="pg_restore_list_"))
            temp_list_path = tmp_list_dir / "filtered_restore.list"
            temp_list_path.write_text("\n".join(filtered_lines) + "\n", encoding="utf-8")

            if skipped_lines:
                self.logger.info("Restore listesinden çıkarılan objeler:\n%s", "\n".join(skipped_lines))

            cmd = [
                self.config.pg_restore_path,
                "-h", self.config.local_db_host,
                "-p", str(self.config.local_db_port),
                "-U", self.config.local_db_user,
                "-d", self.config.local_db_name,
                "--clean",
                "--if-exists",
                "--no-owner",
                "--no-privileges",
                "-v",
                "-L", str(temp_list_path),
                str(restore_source),
            ]

            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                check=True,
            )

            if result.stdout.strip():
                self.logger.info("pg_restore stdout:\n%s", result.stdout.strip())

            if result.stderr.strip():
                self.logger.info("pg_restore stderr:\n%s", result.stderr.strip())

            finished_at = datetime.now()
            duration_seconds = (finished_at - started_at).total_seconds()

            self.logger.info(
                "Restore başarılı | backup=%s | local_db=%s | duration_seconds=%.2f",
                backup_path,
                self.config.local_db_name,
                duration_seconds,
            )

            return RestoreResult(
                success=True,
                backup_file=backup_path,
                started_at=started_at,
                finished_at=finished_at,
                duration_seconds=duration_seconds,
                message="Restore başarılı",
            )

        except subprocess.CalledProcessError as exc:
            finished_at = datetime.now()
            duration_seconds = (finished_at - started_at).total_seconds()

            self.logger.error("Restore başarısız | returncode=%s", exc.returncode)
            if exc.stdout:
                self.logger.error("pg_restore stdout:\n%s", exc.stdout.strip())
            if exc.stderr:
                self.logger.error("pg_restore stderr:\n%s", exc.stderr.strip())

            return RestoreResult(
                success=False,
                backup_file=backup_path,
                started_at=started_at,
                finished_at=finished_at,
                duration_seconds=duration_seconds,
                message=f"Restore başarısız: {exc}",
            )

        except Exception as exc:
            finished_at = datetime.now()
            duration_seconds = (finished_at - started_at).total_seconds()

            self.logger.exception("Beklenmeyen restore hatası | error=%s", exc)

            return RestoreResult(
                success=False,
                backup_file=backup_path,
                started_at=started_at,
                finished_at=finished_at,
                duration_seconds=duration_seconds,
                message=f"Beklenmeyen hata: {exc}",
            )

        finally:
            if temp_dump_path and temp_dump_path.exists():
                try:
                    parent_dir = temp_dump_path.parent
                    temp_dump_path.unlink(missing_ok=True)
                    parent_dir.rmdir()
                except Exception:
                    self.logger.exception("Geçici restore dump dosyaları temizlenemedi")

            if temp_list_path and temp_list_path.exists():
                try:
                    parent_dir = temp_list_path.parent
                    temp_list_path.unlink(missing_ok=True)
                    parent_dir.rmdir()
                except Exception:
                    self.logger.exception("Geçici restore list dosyaları temizlenemedi")