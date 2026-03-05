from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from app.infrastructure.database.repository import PostgresRepository


NUMERIC_TYPES = {
    "smallint",
    "integer",
    "bigint",
    "numeric",
    "decimal",
    "real",
    "double precision",
}


@dataclass(frozen=True)
class DQConfig:
    log_schema: str = "logs"
    log_table: str = "DQ_generic_check"
    job_name: str = "frvp_focus_dq"
    barcheck_min_rows_per_day: int = 360  # 6*60
    sample_limit: int = 10


class DQGenericService:
    def __init__(self, repo: PostgresRepository, config: DQConfig = DQConfig()):
        self.repo = repo
        self.cfg = config

    def run_for_table(
        self,
        *,
        run_id: uuid.UUID,
        exchange: str,
        schema: str,
        table: str,
        interval: str,  # "1min" or "daily"
        ts_col: str,    # "TS" recommended, fallback "TIMESTAMP"
        columns: List[str],
    ) -> None:
        """
        Runs DQ checks on a given table and logs FAIL rows into logs.DQ_generic_check.
        """
        dq_rows: List[Dict[str, Any]] = []

        # 1) Null checks (per column)
        dq_rows += self._check_is_null(
            run_id=run_id,
            exchange=exchange,
            schema=schema,
            table=table,
            ts_col=ts_col,
            columns=columns,
        )

        # 2) Numerical checks (column type-based, fast)
        dq_rows += self._check_is_numerical(
            run_id=run_id,
            exchange=exchange,
            schema=schema,
            table=table,
            numeric_columns=["OPEN", "LOW", "HIGH", "CLOSE", "VOLUME"],
        )

        # 3) BarCheck for 1min only
        if interval == "1min":
            dq_rows += self._check_barcount_per_symbol_day(
                run_id=run_id,
                exchange=exchange,
                schema=schema,
                table=table,
                ts_col=ts_col,
                min_rows=self.cfg.barcheck_min_rows_per_day,
            )

        # Write only FAIL rows
        if dq_rows:
            self.repo.bulk_insert_rows(self.cfg.log_schema, self.cfg.log_table, dq_rows)

    def truncate_logs(self) -> None:
        """
        Truncates the DQ log table each run (as requested).
        """
        self.repo.truncate_table(self.cfg.log_schema, self.cfg.log_table)

    # -------------------------
    # Checks
    # -------------------------

    def _check_is_null(
        self,
        *,
        run_id: uuid.UUID,
        exchange: str,
        schema: str,
        table: str,
        ts_col: str,
        columns: List[str],
    ) -> List[Dict[str, Any]]:
        """
        For each column, checks if there are NULLs.
        Logs FAIL if null_count > 0; includes sample ROW_IDs.
        """
        out: List[Dict[str, Any]] = []
        for col in columns:
            # Count NULL rows
            q_count = text(
                f"""
                SELECT COUNT(*)::bigint AS null_count
                FROM {schema}."{table}"
                WHERE "{col}" IS NULL;
                """
            )

            # Sample ROW_IDs for debug (jsonb array)
            q_sample = text(
                f"""
                SELECT COALESCE(jsonb_agg("ROW_ID"), '[]'::jsonb) AS sample_keys
                FROM (
                    SELECT "ROW_ID"
                    FROM {schema}."{table}"
                    WHERE "{col}" IS NULL
                    AND "ROW_ID" IS NOT NULL
                    LIMIT :lim
                ) s;
                """
            )

            with self.repo.engine.begin() as conn:
                null_count = conn.execute(q_count).scalar_one()
                if null_count and null_count > 0:
                    sample_keys = conn.execute(q_sample, {"lim": self.cfg.sample_limit}).scalar_one()

                    out.append(
                        self._dq_row(
                            run_id=run_id,
                            exchange=exchange,
                            schema=schema,
                            table=table,
                            attribute=col,
                            check_type="IsNull",
                            expectation=f"{col} must be NOT NULL",
                            passed=False,
                            status="FAIL",
                            sample_keys=sample_keys,
                        )
                    )

        return out

    def _check_is_numerical(
        self,
        *,
        run_id: uuid.UUID,
        exchange: str,
        schema: str,
        table: str,
        numeric_columns: List[str],
    ) -> List[Dict[str, Any]]:
        """
        Checks column data type is numeric via information_schema (fast).
        Logs FAIL if the column type is not numeric.
        """
        out: List[Dict[str, Any]] = []

        q_types = text(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = :schema
              AND table_name = :table
              AND column_name = ANY(:cols);
            """
        )

        with self.repo.engine.begin() as conn:
            rows = conn.execute(
                q_types,
                {
                    "schema": schema,
                    "table": table,
                    "cols": numeric_columns,
                },
            ).fetchall()

        types_by_col = {r[0]: r[1] for r in rows}

        for col in numeric_columns:
            dtype = types_by_col.get(col)
            if dtype is None:
                # Column missing is also a failure from a DQ perspective
                out.append(
                    self._dq_row(
                        run_id=run_id,
                        exchange=exchange,
                        schema=schema,
                        table=table,
                        attribute=col,
                        check_type="IsNumarical",
                        expectation="Column must exist and be numeric type",
                        passed=False,
                        status="FAIL",
                        sample_keys=None,
                    )
                )
                continue

            if dtype.lower() not in NUMERIC_TYPES:
                out.append(
                    self._dq_row(
                        run_id=run_id,
                        exchange=exchange,
                        schema=schema,
                        table=table,
                        attribute=col,
                        check_type="IsNumarical",
                        expectation=f"Type must be numeric. Found: {dtype}",
                        passed=False,
                        status="FAIL",
                        sample_keys=None,
                    )
                )

        return out

    def _check_barcount_per_symbol_day(
        self,
        *,
        run_id: uuid.UUID,
        exchange: str,
        schema: str,
        table: str,
        ts_col: str,
        min_rows: int,
    ) -> List[Dict[str, Any]]:
        """
        For interval=1min:
        For each symbol, finds the worst (lowest) daily bar count day.
        Logs a single FAIL row per symbol if any day is below threshold.
        This avoids duplicate violations under unique constraints.
        """
        out: List[Dict[str, Any]] = []

        ts_expr = f'"{ts_col}"'
        if ts_col.upper() != "TS":
            ts_expr = f'("{ts_col}")::timestamp'

        # Pick the worst day per symbol (minimum cnt), only if below threshold.
        # DISTINCT ON ensures one row per symbol.
        q = text(
            f"""
            WITH daily_counts AS (
                SELECT
                    "SYMBOL" AS symbol,
                    DATE({ts_expr}) AS d,
                    COUNT(*)::bigint AS cnt
                FROM {schema}."{table}"
                WHERE "SYMBOL" IS NOT NULL
                AND {ts_expr} IS NOT NULL
                GROUP BY "SYMBOL", DATE({ts_expr})
            ),
            worst_per_symbol AS (
                SELECT DISTINCT ON (symbol)
                    symbol, d, cnt
                FROM daily_counts
                WHERE cnt < :min_rows
                ORDER BY symbol, cnt ASC, d ASC
            )
            SELECT symbol, d, cnt
            FROM worst_per_symbol
            ORDER BY cnt ASC
            LIMIT :lim;
            """
        )

        with self.repo.engine.begin() as conn:
            bad = conn.execute(q, {"min_rows": min_rows, "lim": self.cfg.sample_limit}).fetchall()

        for symbol, d, cnt in bad:
            sample_json = {
                "symbol": symbol,
                "date": str(d),
                "row_count": int(cnt),
                "min_required": int(min_rows),
            }

            # Make CHECK_ID unique per symbol logically (optional but good).
            # Keeping it per symbol avoids collisions if table runs twice with same RUN_ID.
            check_id = f"DQ_{exchange}_{table}_BarCheck_TIMESTAMP_{symbol}"

            out.append(
                {
                    "CREATED_AT": datetime.utcnow(),
                    "JOB_NAME": self.cfg.job_name,
                    "RUN_ID": str(run_id),
                    "CHECK_ID": check_id,
                    "STATUS": "FAIL",
                    "PASSED": False,
                    "EXCHANGE": exchange,
                    "SYMBOL": symbol,
                    "SCHEMA_NAME": schema,
                    "TABLE_NAME": table,
                    "ATTRIBUTE": "TIMESTAMP",
                    "CHECK_TYPE": "BarCheck",
                    "EXPECTATION": f"Per symbol per day row_count must be >= {min_rows} for 1min data",
                    "SAMPLE_KEYS": sample_json,
                }
            )

        return out

    # -------------------------
    # Helpers
    # -------------------------

    def _dq_row(
        self,
        *,
        run_id: uuid.UUID,
        exchange: str,
        schema: str,
        table: str,
        attribute: str,
        check_type: str,
        expectation: str,
        passed: bool,
        status: str,
        sample_keys: Any,
        symbol: Optional[str] = None,
    ) -> Dict[str, Any]:
        return {
            "CREATED_AT": datetime.utcnow(),
            "JOB_NAME": self.cfg.job_name,
            "RUN_ID": str(run_id),
            "CHECK_ID": f"DQ_{exchange}_{table}_{check_type}_{attribute}",
            "STATUS": status,
            "PASSED": passed,
            "EXCHANGE": exchange,
            "SYMBOL": symbol or "*",
            "SCHEMA_NAME": schema,
            "TABLE_NAME": table,
            "ATTRIBUTE": attribute,
            "CHECK_TYPE": check_type,
            "EXPECTATION": expectation,
            "SAMPLE_KEYS": sample_keys,
        }