from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Set

import yaml
from zoneinfo import ZoneInfo


WEEKDAY_MAP = {
    "MON": 0,
    "TUE": 1,
    "WED": 2,
    "THU": 3,
    "FRI": 4,
    "SAT": 5,
    "SUN": 6,
}


@dataclass(frozen=True)
class Job:
    name: str
    cmd: str
    at: Optional[str] = None          # "HH:MM"
    depends_on: Optional[str] = None  # parent job name
    lock: Optional[str] = None        # lock key


@dataclass(frozen=True)
class ScheduleConfig:
    timezone: str
    run_days: Set[int]
    poll_interval_seconds: int
    jobs: List[Job]


class SchedulerState:
    """
    State is stored as a JSON file:
    {
      "YYYY-MM-DD": {
        "job_name": {"status": "done|failed", "ts": "iso"}
      }
    }
    """
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data: Dict[str, Dict[str, Dict[str, str]]] = {}
        self._load()

    def _load(self) -> None:
        if self.path.exists():
            try:
                self._data = json.loads(self.path.read_text())
            except Exception:
                self._data = {}

    def _save(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._data, indent=2, sort_keys=True))
        tmp.replace(self.path)

    def _today_key(self, today: date) -> str:
        return today.strftime("%Y-%m-%d")

    def get_status(self, today: date, job_name: str) -> Optional[str]:
        k = self._today_key(today)
        return self._data.get(k, {}).get(job_name, {}).get("status")

    def mark(self, today: date, job_name: str, status: str) -> None:
        k = self._today_key(today)
        self._data.setdefault(k, {})
        self._data[k][job_name] = {"status": status, "ts": datetime.now().isoformat(timespec="seconds")}
        self._save()


class FileLock:
    """
    Very simple lock with O_EXCL.
    """
    def __init__(self, key: str):
        self.key = key
        self.path = Path("/tmp") / f"tradeai_{key}.lock"
        self.fd: Optional[int] = None

    def acquire(self) -> bool:
        try:
            self.fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            os.write(self.fd, str(os.getpid()).encode("utf-8"))
            return True
        except FileExistsError:
            return False

    def release(self) -> None:
        try:
            if self.fd is not None:
                os.close(self.fd)
        finally:
            self.fd = None
            try:
                self.path.unlink(missing_ok=True)  # py3.8+: ok. py3.11: ok.
            except Exception:
                pass


def load_schedule(path: Path) -> ScheduleConfig:
    raw = yaml.safe_load(path.read_text())

    tz = raw.get("timezone", "Europe/Amsterdam")
    poll = int(raw.get("poll_interval_seconds", 20))

    rd = raw.get("run_days", ["MON", "TUE", "WED", "THU", "FRI"])
    run_days = {WEEKDAY_MAP[x.upper().strip()] for x in rd}

    jobs_raw = raw.get("jobs", [])
    jobs: List[Job] = []
    names: Set[str] = set()

    for j in jobs_raw:
        job = Job(
            name=str(j["name"]).strip(),
            cmd=str(j["cmd"]).strip(),
            at=str(j["at"]).strip() if "at" in j else None,
            depends_on=str(j["depends_on"]).strip() if "depends_on" in j else None,
            lock=str(j["lock"]).strip() if "lock" in j else None,
        )
        if job.name in names:
            raise ValueError(f"Duplicate job name: {job.name}")
        names.add(job.name)
        jobs.append(job)

    # Validate dependencies
    job_names = {j.name for j in jobs}
    for j in jobs:
        if j.depends_on and j.depends_on not in job_names:
            raise ValueError(f"Job '{j.name}' depends_on unknown job '{j.depends_on}'")

    return ScheduleConfig(timezone=tz, run_days=run_days, poll_interval_seconds=poll, jobs=jobs)


def now_in_tz(tz: str) -> datetime:
    return datetime.now(ZoneInfo(tz))


def is_run_day(cfg: ScheduleConfig, dt: datetime) -> bool:
    return dt.weekday() in cfg.run_days


def parse_hhmm(hhmm: str) -> tuple[int, int]:
    parts = hhmm.split(":")
    if len(parts) != 2:
        raise ValueError(f"Invalid time format (HH:MM): {hhmm}")
    return int(parts[0]), int(parts[1])


def due_root_jobs(cfg: ScheduleConfig, dt: datetime, state: SchedulerState) -> List[Job]:
    """
    Root jobs are jobs with an 'at' schedule.
    We consider a job due when current local time matches HH:MM exactly.
    State ensures it runs only once per day.
    """
    today = dt.date()
    out: List[Job] = []
    for j in cfg.jobs:
        if not j.at:
            continue
        if state.get_status(today, j.name) in ("done", "failed"):
            continue

        h, m = parse_hhmm(j.at)
        if dt.hour == h and dt.minute == m:
            out.append(j)
    return out


def dependents_of(cfg: ScheduleConfig, parent_name: str) -> List[Job]:
    return [j for j in cfg.jobs if j.depends_on == parent_name]


def run_job(cfg: ScheduleConfig, job: Job, workdir: Path, logs_dir: Path) -> int:
    """
    Run a job command as a subprocess and stream logs to files.
    """
    logs_dir.mkdir(parents=True, exist_ok=True)

    out_path = logs_dir / f"{job.name}.out.log"
    err_path = logs_dir / f"{job.name}.err.log"

    with out_path.open("a", buffering=1) as out_f, err_path.open("a", buffering=1) as err_f:
        start_line = f"\n[{job.name}] START {now_in_tz(cfg.timezone).strftime('%Y-%m-%d %H:%M:%S')}\n"
        out_f.write(start_line)
        out_f.flush()

        cmd_parts = shlex.split(job.cmd)
        p = subprocess.Popen(
            cmd_parts,
            cwd=str(workdir),
            stdout=out_f,
            stderr=err_f,
            env=os.environ.copy(),
        )
        rc = p.wait()

        end_line = f"[{job.name}] END rc={rc} {now_in_tz(cfg.timezone).strftime('%Y-%m-%d %H:%M:%S')}\n"
        out_f.write(end_line)
        out_f.flush()

    return rc


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="app/scheduler/schedule.yaml")
    ap.add_argument("--state", default="var/scheduler_state.json")
    ap.add_argument("--logs", default="logs")
    ap.add_argument("--workdir", default=".")
    ap.add_argument("--once", action="store_true", help="Run one scheduler tick then exit.")
    args = ap.parse_args()

    cfg = load_schedule(Path(args.config))
    state = SchedulerState(Path(args.state))
    logs_dir = Path(args.logs)
    workdir = Path(args.workdir).resolve()

    print(f"[SCHED] loaded jobs={len(cfg.jobs)} tz={cfg.timezone} poll={cfg.poll_interval_seconds}s workdir={workdir}", flush=True)

    while True:
        dt = now_in_tz(cfg.timezone)

        if not is_run_day(cfg, dt):
            if args.once:
                print("[SCHED] not a run day; exiting (once).", flush=True)
                return 0
            time.sleep(cfg.poll_interval_seconds)
            continue

        # Root jobs due at this minute
        queue: List[Job] = due_root_jobs(cfg, dt, state)

        # Process queue sequentially
        while queue:
            job = queue.pop(0)
            today = dt.date()

            # State check again (in case it was completed earlier in same tick)
            if state.get_status(today, job.name) in ("done", "failed"):
                continue

            lock_key = job.lock or job.name
            lock = FileLock(lock_key)
            if not lock.acquire():
                # Another run is active
                continue

            try:
                rc = run_job(cfg, job, workdir=workdir, logs_dir=logs_dir)
                if rc == 0:
                    state.mark(today, job.name, "done")
                    # Enqueue dependents immediately (dependency chain)
                    for child in dependents_of(cfg, job.name):
                        # Only run child if it hasn't run today
                        if state.get_status(today, child.name) is None:
                            queue.append(child)
                else:
                    state.mark(today, job.name, "failed")
                    # Do not run dependents if parent failed
            finally:
                lock.release()

            # Refresh time (long jobs may pass midnight/time changes)
            dt = now_in_tz(cfg.timezone)

        if args.once:
            print("[SCHED] tick complete; exiting (once).", flush=True)
            return 0

        time.sleep(cfg.poll_interval_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
