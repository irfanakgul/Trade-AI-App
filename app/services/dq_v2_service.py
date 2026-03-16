from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Sequence

from sqlalchemy import text

from app.infrastructure.database.repository import PostgresRepository


@dataclass(frozen=True)
class DQTableConfig:
    exchange: str
    schema_name: str
    table_name: str
    interval: str  # "daily" or "1min"

    ts_col: str = "TS"
    timestamp_col: str = "TIMESTAMP"
    symbol_col: str = "SYMBOL"
    row_id_col: str = "ROW_ID"

    numeric_cols: tuple[str, ...] = ("OPEN", "LOW", "HIGH", "CLOSE", "VOLUME")
    required_cols: tuple[str, ...] = ("SYMBOL", "TIMESTAMP")

    checks: tuple[str, ...] = (
        "END_DATE_CHECK",
        "NULL_CHECK",
        "DUPLICATE_CHECK",
        "BAR_CHECK",
    )

    expected_close_hour: int = 18
    expected_close_minute: int = 0
    end_tolerance_minutes: int = 30
    bar_threshold: int = 360


@dataclass(frozen=True)
class DQRunConfig:
    job_name: str
    active_table: str  # "bist_dq_check" or "usa_dq_check"
    specific_trading_calendar: bool = False
    trading_calendar_schema: str = "raw"
    trading_calendar_table: str | None = None
    known_holidays: tuple[date, ...] = field(default_factory=tuple)
    as_of_date: date | None = None


class DQV2Service:
    def __init__(self, repo: PostgresRepository):
        self.repo = repo

    def run_exchange_checks(
        self,
        run_cfg: DQRunConfig,
        table_cfgs: Sequence[DQTableConfig],
    ) -> int:
        """
        Runs DQ checks for a single exchange into one active DQ table.
        Then archives active rows into logs.log_dq_check.
        Returns DQ_RUN_ID.
        """
        dq_run_id = self.repo.get_next_dq_run_id()

        self.repo.truncate_table("logs", run_cfg.active_table)

        for cfg in table_cfgs:
            self._run_table_checks(
                dq_run_id=dq_run_id,
                run_cfg=run_cfg,
                cfg=cfg,
            )

        archived = self.repo.archive_dq_rows(run_cfg.active_table)
        failed_symbols = self.repo.count_distinct_failed_symbols(run_cfg.active_table)

        exchange = table_cfgs[0].exchange if table_cfgs else "UNKNOWN"
        print(f"[DQ-{exchange}] DQ_RUN_ID = {dq_run_id}")
        print(f"[DQ-{exchange}] FAILED SYMBOL COUNT = {failed_symbols}")
        print(f"[DQ-{exchange}] archived_rows = {archived}")

        return dq_run_id

    def _run_table_checks(
        self,
        *,
        dq_run_id: int,
        run_cfg: DQRunConfig,
        cfg: DQTableConfig,
    ) -> None:
        last_trading_day = self._resolve_last_trading_day(run_cfg, cfg.exchange)
        expected_end_time = self._resolve_expected_end_time(cfg, last_trading_day)

        if "END_DATE_CHECK" in cfg.checks:
            self._insert_end_date_failures(
                dq_run_id=dq_run_id,
                run_cfg=run_cfg,
                cfg=cfg,
                last_trading_day=last_trading_day,
                expected_end_time=expected_end_time,
            )

        if "NULL_CHECK" in cfg.checks:
            self._insert_null_failures(
                dq_run_id=dq_run_id,
                run_cfg=run_cfg,
                cfg=cfg,
            )

        if "DUPLICATE_CHECK" in cfg.checks:
            self._insert_duplicate_failures(
                dq_run_id=dq_run_id,
                run_cfg=run_cfg,
                cfg=cfg,
            )

        if cfg.interval == "1min" and "BAR_CHECK" in cfg.checks:
            self._insert_bar_failures(
                dq_run_id=dq_run_id,
                run_cfg=run_cfg,
                cfg=cfg,
                last_trading_day=last_trading_day,
                expected_end_time=expected_end_time,
            )

    # ----------------------------------------------------------
    # Trading day helpers
    # ----------------------------------------------------------

    def _resolve_last_trading_day(self, run_cfg: DQRunConfig, exchange: str) -> date:
        """
        If specific_trading_calendar=False:
            use previous weekday excluding known_holidays.
        If True:
            read from raw.<exchange>_trading_calendar (or custom table) where IN_SCOPE=TRUE.
        """
        as_of = run_cfg.as_of_date or date.today()

        if run_cfg.specific_trading_calendar:
            cal_table = run_cfg.trading_calendar_table or f"{exchange.lower()}_trading_calendar"

            q = text(f"""
                SELECT MAX("DAYS")
                FROM {run_cfg.trading_calendar_schema}."{cal_table}"
                WHERE "IN_SCOPE" = TRUE
                  AND "DAYS" < :as_of;
            """)
            with self.repo.engine.begin() as conn:
                d = conn.execute(q, {"as_of": as_of}).scalar_one_or_none()

            if d is not None:
                return d

        holidays = set(run_cfg.known_holidays)
        d = as_of - timedelta(days=1)

        while True:
            if d.weekday() >= 5:
                d -= timedelta(days=1)
                continue
            if d in holidays:
                d -= timedelta(days=1)
                continue
            return d

    def _resolve_expected_end_time(self, cfg: DQTableConfig, last_trading_day: date) -> datetime:
        if cfg.interval == "daily":
            return datetime.combine(last_trading_day, time(0, 0))

        return datetime.combine(
            last_trading_day,
            time(cfg.expected_close_hour, cfg.expected_close_minute),
        )

    # ----------------------------------------------------------
    # Check 1: END_DATE_CHECK
    # ----------------------------------------------------------

    def _insert_end_date_failures(
        self,
        *,
        dq_run_id: int,
        run_cfg: DQRunConfig,
        cfg: DQTableConfig,
        last_trading_day: date,
        expected_end_time: datetime,
    ) -> None:
        active = run_cfg.active_table
        src = f'{cfg.schema_name}."{cfg.table_name}"'

        if cfg.interval == "daily":
            where_fail = 'DATE(lr.last_ts) < :last_trading_day'
            threshold = str(last_trading_day)
            failure_description = "Dataset end date is older than expected last trading day"
        else:
            acceptable_min_ts = expected_end_time - timedelta(minutes=cfg.end_tolerance_minutes)
            where_fail = 'lr.last_ts < :acceptable_min_ts'
            threshold = str(cfg.end_tolerance_minutes)
            failure_description = "Dataset end timestamp is earlier than allowed market close tolerance"

        q = text(f"""
            WITH last_rows AS (
                SELECT DISTINCT ON ("{cfg.symbol_col}")
                    "{cfg.symbol_col}" AS symbol,
                    "{cfg.ts_col}" AS last_ts,
                    "{cfg.row_id_col}" AS row_id
                FROM {src}
                WHERE "{cfg.symbol_col}" IS NOT NULL
                  AND "{cfg.ts_col}" IS NOT NULL
                ORDER BY "{cfg.symbol_col}", "{cfg.ts_col}" DESC
            )
            INSERT INTO logs."{active}" (
                "EXCHANGE",
                "SYMBOL",
                "SCHEMA_NAME",
                "TABLE_NAME",
                "ATTRIBUTE",
                "DQ_FAILURE_TYPE",
                "FAILURE_DESCRIPTION",
                "END_TIME_IN_DATASET",
                "EXPECTED_END_TIME",
                "LAST_TRADING_DAY",
                "FAILED_TRADING_DAY",
                "INTERVAL",
                "THRESHOLD",
                "SOURCE_ROW_COUNT",
                "STATUS",
                "SAMPLE_KEYS",
                "CREATED_AT",
                "DQ_RUN_ID"
            )
            SELECT
                :exchange,
                lr.symbol,
                :schema_name,
                :table_name,
                :attribute,
                'END_DATE_CHECK',
                :failure_description || ' | found=' || COALESCE(lr.last_ts::text, 'NULL')
                    || ' | expected=' || CAST(:expected_end_time AS text),
                lr.last_ts,
                :expected_end_time,
                :last_trading_day,
                NULL,
                :interval,
                :threshold,
                NULL,
                'FAILED',
                jsonb_build_object('row_id', lr.row_id),
                now(),
                :dq_run_id
            FROM last_rows lr
            WHERE {where_fail};
        """)

        params = {
            "exchange": cfg.exchange,
            "schema_name": cfg.schema_name,
            "table_name": cfg.table_name,
            "attribute": cfg.timestamp_col,
            "failure_description": failure_description,
            "expected_end_time": expected_end_time,
            "last_trading_day": last_trading_day,
            "interval": cfg.interval,
            "threshold": threshold,
            "dq_run_id": dq_run_id,
        }

        if cfg.interval == "1min":
            params["acceptable_min_ts"] = expected_end_time - timedelta(minutes=cfg.end_tolerance_minutes)

        with self.repo.engine.begin() as conn:
            conn.execute(q, params)

    # ----------------------------------------------------------
    # Check 2: NULL_CHECK
    # ----------------------------------------------------------

    def _insert_null_failures(
        self,
        *,
        dq_run_id: int,
        run_cfg: DQRunConfig,
        cfg: DQTableConfig,
    ) -> None:
        active = run_cfg.active_table
        src = f'{cfg.schema_name}."{cfg.table_name}"'

        q_types = text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = :schema_name
              AND table_name = :table_name
              AND column_name = ANY(:cols);
        """)

        cols = list(cfg.required_cols) + list(cfg.numeric_cols)

        with self.repo.engine.begin() as conn:
            rows = conn.execute(
                q_types,
                {
                    "schema_name": cfg.schema_name,
                    "table_name": cfg.table_name,
                    "cols": cols,
                },
            ).fetchall()

        dtype_map = {r[0]: r[1].lower() for r in rows}

        lateral_items: list[str] = []

        for col in cfg.required_cols:
            pred = f't."{col}" IS NULL OR btrim(t."{col}"::text) = \'\''
            lateral_items.append(
                f"('{col}', {pred}, COALESCE(t.\"{col}\"::text, 'NULL'))"
            )

        for col in cfg.numeric_cols:
            dtype = dtype_map.get(col, "")

            if dtype in {"smallint", "integer", "bigint", "numeric", "decimal", "real", "double precision"}:
                pred = f't."{col}" IS NULL OR t."{col}"::text = \'NaN\''
            else:
                pred = (
                    f't."{col}" IS NULL '
                    f'OR btrim(t."{col}"::text) = \'\' '
                    f'OR lower(btrim(t."{col}"::text)) = \'nan\' '
                    f'OR NOT (btrim(t."{col}"::text) ~ \'^[+-]?(?:\\d+(?:\\.\\d+)?|\\.\\d+)(?:[eE][+-]?\\d+)?$\')'
                )

            lateral_items.append(
                f"('{col}', {pred}, COALESCE(t.\"{col}\"::text, 'NULL'))"
            )

        lateral_sql = ",\n".join(lateral_items)

        q = text(f"""
            INSERT INTO logs."{active}" (
                "EXCHANGE",
                "SYMBOL",
                "SCHEMA_NAME",
                "TABLE_NAME",
                "ATTRIBUTE",
                "DQ_FAILURE_TYPE",
                "FAILURE_DESCRIPTION",
                "END_TIME_IN_DATASET",
                "EXPECTED_END_TIME",
                "LAST_TRADING_DAY",
                "FAILED_TRADING_DAY",
                "INTERVAL",
                "THRESHOLD",
                "SOURCE_ROW_COUNT",
                "STATUS",
                "SAMPLE_KEYS",
                "CREATED_AT",
                "DQ_RUN_ID"
            )
            SELECT
                :exchange,
                t."{cfg.symbol_col}" AS symbol,
                :schema_name,
                :table_name,
                v.attr,
                'NULL_CHECK',
                'Invalid value in ' || v.attr || ' | value=' || v.bad_value,
                NULL,
                NULL,
                NULL,
                NULL,
                :interval,
                NULL,
                NULL,
                'FAILED',
                jsonb_build_object('row_id', t."{cfg.row_id_col}"),
                now(),
                :dq_run_id
            FROM {src} t
            CROSS JOIN LATERAL (
                VALUES
                {lateral_sql}
            ) AS v(attr, is_fail, bad_value)
            WHERE v.is_fail;
        """)

        with self.repo.engine.begin() as conn:
            conn.execute(
                q,
                {
                    "exchange": cfg.exchange,
                    "schema_name": cfg.schema_name,
                    "table_name": cfg.table_name,
                    "interval": cfg.interval,
                    "dq_run_id": dq_run_id,
                },
            )

    # ----------------------------------------------------------
    # Check 3: DUPLICATE_CHECK
    # ----------------------------------------------------------

    def _insert_duplicate_failures(
        self,
        *,
        dq_run_id: int,
        run_cfg: DQRunConfig,
        cfg: DQTableConfig,
    ) -> None:
        active = run_cfg.active_table
        src = f'{cfg.schema_name}."{cfg.table_name}"'

        q = text(f"""
            WITH dup AS (
                SELECT
                    "{cfg.row_id_col}" AS row_id,
                    MIN("{cfg.symbol_col}") AS symbol,
                    COUNT(*)::bigint AS cnt
                FROM {src}
                WHERE "{cfg.row_id_col}" IS NOT NULL
                GROUP BY "{cfg.row_id_col}"
                HAVING COUNT(*) > 1
            )
            INSERT INTO logs."{active}" (
                "EXCHANGE",
                "SYMBOL",
                "SCHEMA_NAME",
                "TABLE_NAME",
                "ATTRIBUTE",
                "DQ_FAILURE_TYPE",
                "FAILURE_DESCRIPTION",
                "END_TIME_IN_DATASET",
                "EXPECTED_END_TIME",
                "LAST_TRADING_DAY",
                "FAILED_TRADING_DAY",
                "INTERVAL",
                "THRESHOLD",
                "SOURCE_ROW_COUNT",
                "STATUS",
                "SAMPLE_KEYS",
                "CREATED_AT",
                "DQ_RUN_ID"
            )
            SELECT
                :exchange,
                d.symbol,
                :schema_name,
                :table_name,
                :attribute,
                'DUPLICATE_CHECK',
                'Duplicate ROW_ID found | duplicate_count=' || d.cnt::text,
                NULL,
                NULL,
                NULL,
                NULL,
                :interval,
                NULL,
                d.cnt,
                'FAILED',
                jsonb_build_object('row_id', d.row_id),
                now(),
                :dq_run_id
            FROM dup d;
        """)

        with self.repo.engine.begin() as conn:
            conn.execute(
                q,
                {
                    "exchange": cfg.exchange,
                    "schema_name": cfg.schema_name,
                    "table_name": cfg.table_name,
                    "attribute": cfg.row_id_col,
                    "interval": cfg.interval,
                    "dq_run_id": dq_run_id,
                },
            )

    # ----------------------------------------------------------
    # Check 4: BAR_CHECK
    # ----------------------------------------------------------

    def _insert_bar_failures(
        self,
        *,
        dq_run_id: int,
        run_cfg: DQRunConfig,
        cfg: DQTableConfig,
        last_trading_day: date,
        expected_end_time: datetime,
    ) -> None:
        active = run_cfg.active_table
        src = f'{cfg.schema_name}."{cfg.table_name}"'

        # For each symbol, keep latest timestamp once, then join with daily counts.
        q = text(f"""
            WITH daily_counts AS (
                SELECT
                    "{cfg.symbol_col}" AS symbol,
                    DATE("{cfg.ts_col}") AS trading_day,
                    COUNT(*)::bigint AS cnt
                FROM {src}
                WHERE "{cfg.symbol_col}" IS NOT NULL
                  AND "{cfg.ts_col}" IS NOT NULL
                GROUP BY "{cfg.symbol_col}", DATE("{cfg.ts_col}")
            ),
            last_rows AS (
                SELECT DISTINCT ON ("{cfg.symbol_col}")
                    "{cfg.symbol_col}" AS symbol,
                    "{cfg.ts_col}" AS last_ts
                FROM {src}
                WHERE "{cfg.symbol_col}" IS NOT NULL
                  AND "{cfg.ts_col}" IS NOT NULL
                ORDER BY "{cfg.symbol_col}", "{cfg.ts_col}" DESC
            )
            INSERT INTO logs."{active}" (
                "EXCHANGE",
                "SYMBOL",
                "SCHEMA_NAME",
                "TABLE_NAME",
                "ATTRIBUTE",
                "DQ_FAILURE_TYPE",
                "FAILURE_DESCRIPTION",
                "END_TIME_IN_DATASET",
                "EXPECTED_END_TIME",
                "LAST_TRADING_DAY",
                "FAILED_TRADING_DAY",
                "INTERVAL",
                "THRESHOLD",
                "SOURCE_ROW_COUNT",
                "STATUS",
                "SAMPLE_KEYS",
                "CREATED_AT",
                "DQ_RUN_ID"
            )
            SELECT
                :exchange,
                d.symbol,
                :schema_name,
                :table_name,
                :attribute,
                'BAR_CHECK',
                'Insufficient intraday bars for symbol/day | row_count=' || d.cnt::text
                        || ' | threshold=' || CAST(:threshold AS text),
                lr.last_ts,
                :expected_end_time,
                :last_trading_day,
                d.trading_day,
                :interval,
                :threshold,
                d.cnt,
                'FAILED',
                jsonb_build_object(
                    'symbol', d.symbol,
                    'failed_trading_day', d.trading_day,
                    'row_count', d.cnt,
                    'threshold', :threshold_int
                ),
                now(),
                :dq_run_id
            FROM daily_counts d
            LEFT JOIN last_rows lr
              ON lr.symbol = d.symbol
            WHERE d.cnt < :threshold_int;
        """)

        with self.repo.engine.begin() as conn:
            conn.execute(
                q,
                {
                    "exchange": cfg.exchange,
                    "schema_name": cfg.schema_name,
                    "table_name": cfg.table_name,
                    "attribute": cfg.timestamp_col,
                    "expected_end_time": expected_end_time,
                    "last_trading_day": last_trading_day,
                    "interval": cfg.interval,
                    "threshold": str(cfg.bar_threshold),
                    "threshold_int": cfg.bar_threshold,
                    "dq_run_id": dq_run_id,
                },
            )