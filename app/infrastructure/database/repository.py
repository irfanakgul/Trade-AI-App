# app/infrastructure/database/repository.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple, Dict, Any
from sqlalchemy import text,bindparam
from sqlalchemy.engine import Engine
from datetime import datetime,timezone,timedelta
import json
import pandas as pd
from google.gg_read_write_update_func import fn_write_to_google


@dataclass(frozen=True)
class InScopeSymbol:
    symbol: str
    exchange: str


class PostgresRepository:
    def __init__(self, engine: Engine):
        self.engine = engine

    def get_in_scope_symbols(self, exchange: str, schema: str) -> List[str]:
        """
        Returns symbols where in_scope = true and EXCHANGE matches.
        """
        q = text(f"""
            SELECT "SYMBOL"
            FROM {schema}.scope_symbol_list
            WHERE in_scope = true
            AND "EXCHANGE" = :exchange
            ORDER BY "SYMBOL";
        """)
        with self.engine.connect() as conn:
            rows = conn.execute(q, {"exchange": exchange}).fetchall()

        return [r[0] for r in rows]

    def get_last_timestamp(
        self,
        symbol: str,
        schema: str,
        table: str,
        ts_column: str = "TIMESTAMP",
    ) -> Optional[datetime]:
        """
        Returns MAX(timestamp) casted to timestamp.
        Works even if TIMESTAMP is stored as text.
        """
        q = text(f"""
            SELECT MAX(("{ts_column}")::timestamp) AS max_ts
            FROM {schema}.{table}
            WHERE "SYMBOL" = :symbol;
        """)
        with self.engine.connect() as conn:
            row = conn.execute(q, {"symbol": symbol}).fetchone()
        return row[0] if row and row[0] else None
    

    def get_last_ts_typed(
        self,
        symbol: str,
        schema: str,
        table: str,
        ts_typed_col: str = "TS",
    ) -> Optional[datetime]:
        """
        Returns MAX(TS) for a typed timestamp column.
        Fast path: uses the typed column directly (no casting).
        """
        q = text(f"""
            SELECT MAX("{ts_typed_col}") AS max_ts
            FROM {schema}.{table}
            WHERE "SYMBOL" = :symbol;
        """)
        with self.engine.connect() as conn:
            row = conn.execute(q, {"symbol": symbol}).fetchone()
        return row[0] if row and row[0] else None

    def bulk_insert_on_conflict_do_nothing(
        self,
        schema: str,
        table: str,
        rows: Sequence[Dict[str, Any]],
        conflict_column: str = "ROW_ID",
    ) -> int:
        """
        Inserts rows using INSERT ... ON CONFLICT DO NOTHING.
        Returns the number of inserted rows (best-effort via rowcount).
        """
        if not rows:
            return 0

        cols = list(rows[0].keys())
        col_list = ", ".join([f'"{c}"' for c in cols])
        placeholders = ", ".join([f":{c}" for c in cols])

        stmt = text(f"""
            INSERT INTO {schema}.{table} ({col_list})
            VALUES ({placeholders})
            ON CONFLICT ("{conflict_column}") DO NOTHING;
        """)

        with self.engine.begin() as conn:
            res = conn.execute(stmt, rows)  # executemany
            # rowcount should reflect inserted rows for INSERT..DO NOTHING
            return int(res.rowcount) if res.rowcount is not None and res.rowcount >= 0 else 0
    
    def count_rows(self, schema: str, table: str) -> int:
        """
        Returns total row count of a table.
        """
        # Basic identifier safety
        if not schema.replace("_", "").isalnum():
            raise ValueError("Invalid schema name")
        if not table.replace("_", "").isalnum():
            raise ValueError("Invalid table name")

        q = text(f"SELECT COUNT(*) FROM {schema}.{table};")

        with self.engine.connect() as conn:
            result = conn.execute(q).scalar()

        return int(result)
    
    # func for error symbols. it will be retry in the end
    def log_ingestion_error(
        self,
        schema: str,
        table: str,
        job_name: str,
        symbol: str,
        exchange: str,
        error_type: str,
        error_message: str,
    ) -> None:
        q = text(f"""
            INSERT INTO {schema}.{table}
            (job_name, symbol, exchange, error_type, error_message)
            VALUES (:job_name, :symbol, :exchange, :error_type, :error_message);
        """)
        with self.engine.begin() as conn:
            conn.execute(
                q,
                {
                    "job_name": job_name,
                    "symbol": symbol,
                    "exchange": exchange,
                    "error_type": error_type,
                    "error_message": error_message,
                },
            )

    def trim_history_by_peak_or_lookback_ts(
        self,
        schema: str,
        table: str,
        symbol_col: str = "SYMBOL",
        ts_typed_col: str = "TS",
        high_col: str = "HIGH",
        lookback_days: int = 365,
        reference_days_ago: int = 1,
    ) -> int:
        """
        Fast trim using a typed timestamp column (e.g., TS) and an index on (SYMBOL, TS).
        """
        def _is_safe_ident(x: str) -> bool:
            return x.replace("_", "").isalnum()

        for ident in (schema, table, symbol_col, ts_typed_col, high_col):
            if not _is_safe_ident(ident):
                raise ValueError(f"Unsafe identifier: {ident}")

        q = text(f"""
            WITH max_high AS (
                SELECT "{symbol_col}" AS symbol, MAX("{high_col}"::double precision) AS mh
                FROM {schema}.{table}
                GROUP BY "{symbol_col}"
            ),
            peak AS (
                SELECT t."{symbol_col}" AS symbol, MIN(t."{ts_typed_col}") AS peak_ts
                FROM {schema}.{table} t
                JOIN max_high m
                ON t."{symbol_col}" = m.symbol
                AND t."{high_col}"::double precision = m.mh
                GROUP BY t."{symbol_col}"
            ),
            keep AS (
                SELECT
                    p.symbol,
                    CASE
                        WHEN p.peak_ts < (
                            (CURRENT_DATE - (CAST(:reference_days_ago AS int) * INTERVAL '1 day'))
                            - (CAST(:lookback_days AS int) * INTERVAL '1 day')
                        )::timestamp
                        THEN p.peak_ts
                        ELSE (
                            (CURRENT_DATE - (CAST(:reference_days_ago AS int) * INTERVAL '1 day'))
                            - (CAST(:lookback_days AS int) * INTERVAL '1 day')
                        )::timestamp
                    END AS keep_from
                FROM peak p
            )
            DELETE FROM {schema}.{table} t
            USING keep k
            WHERE t."{symbol_col}" = k.symbol
            AND t."{ts_typed_col}" < k.keep_from;
        """)

        with self.engine.begin() as conn:
            res = conn.execute(q, {"lookback_days": lookback_days, "reference_days_ago": reference_days_ago})
            return int(res.rowcount) if res.rowcount is not None and res.rowcount >= 0 else 0
        
    def upsert_ingestion_error(
        self,
        schema: str,
        table: str,
        job_name: str,
        symbol: str,
        exchange: str,
        error_type: str,
        error_message: str,
    ) -> None:
        """
        Upserts an active ingestion error. Requires a UNIQUE constraint on (job_name, exchange, symbol).
        """
        q = text(f"""
            INSERT INTO {schema}.{table}
                (job_name, exchange, symbol, error_type, error_message, updated_at)
            VALUES
                (:job_name, :exchange, :symbol, :error_type, :error_message, :updated_at)
            ON CONFLICT (job_name, exchange, symbol)
            DO UPDATE SET
                error_type = EXCLUDED.error_type,
                error_message = EXCLUDED.error_message,
                updated_at = EXCLUDED.updated_at;
        """)

        with self.engine.begin() as conn:
            conn.execute(
                q,
                {
                    "job_name": job_name,
                    "exchange": exchange,
                    "symbol": symbol,
                    "error_type": error_type,
                    "error_message": error_message,
                    "updated_at": datetime.now(timezone.utc).replace(tzinfo=None),
                },
            )


    def clear_ingestion_error(
        self,
        schema: str,
        table: str,
        job_name: str,
        symbol: str,
        exchange: str,
    ) -> None:
        """
        Clears an active ingestion error when a symbol succeeds later.
        """
        q = text(f"""
            DELETE FROM {schema}.{table}
            WHERE job_name = :job_name
            AND exchange = :exchange
            AND symbol = :symbol;
        """)

        with self.engine.begin() as conn:
            conn.execute(q, {"job_name": job_name, "exchange": exchange, "symbol": symbol})

    def get_active_error_symbols(
        self,
        schema: str,
        table: str,
        job_name: str,
        exchange: str,
    ) -> List[str]:
        """
        Returns symbols that still have an active ingestion error record.
        """
        q = text(f"""
            SELECT symbol
            FROM {schema}.{table}
            WHERE job_name = :job_name
            AND exchange = :exchange
            ORDER BY symbol;
        """)

        with self.engine.begin() as conn:
            rows = conn.execute(q, {"job_name": job_name, "exchange": exchange}).fetchall()

        return [r[0] for r in rows]
    
    # ============================================
    # FOCUS Dataset preperation for FRVP
    # ============================================

    def build_frvp_focus_dataset(
        self,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        ts_col: str,
        high_col: str,
        exchange: str,
        min_trading_days: int = 15,
    ) -> dict:
        """
        Creates a focus dataset table by:
          - Finding per-symbol earliest timestamp where HIGH is maximal (peak_ts)
          - Keeping rows from peak_ts onward
          - Excluding symbols whose distinct trading days between peak_ts and max_ts <= min_trading_days
        Table is dropped and recreated each run.

        Trading days are computed as COUNT(DISTINCT TS::date) using the data itself.
        Returns basic stats for printing.
        """
        src_fqn = f'{source_schema}.{source_table}'
        tgt_fqn = f'{target_schema}."{target_table}"'  # target table uses mixed-case safe quoting

        # Counts before
        q_before = text(f'''
            SELECT
                COUNT(*)::bigint AS rows,
                COUNT(DISTINCT "SYMBOL")::bigint AS symbols
            FROM {src_fqn};
        ''')

        # Drop target and rebuild
        q_drop = text(f'DROP TABLE IF EXISTS {tgt_fqn};')

        q_create = text(f"""
            CREATE TABLE {tgt_fqn} AS
            WITH base AS (
                SELECT
                    "SYMBOL" AS symbol,
                    "{ts_col}" AS ts,
                    "{high_col}"::double precision AS high
                FROM {src_fqn}
                WHERE "SYMBOL" IS NOT NULL
                  AND "{ts_col}" IS NOT NULL
                  AND "{high_col}" IS NOT NULL
            ),
            max_high AS (
                SELECT symbol, MAX(high) AS mh
                FROM base
                GROUP BY symbol
            ),
            peak AS (
                -- Earliest timestamp where HIGH equals the per-symbol maximum
                SELECT b.symbol, MIN(b.ts) AS peak_ts
                FROM base b
                JOIN max_high m
                  ON b.symbol = m.symbol
                 AND b.high = m.mh
                GROUP BY b.symbol
            ),
            max_ts AS (
                SELECT symbol, MAX(ts) AS end_ts
                FROM base
                GROUP BY symbol
            ),
            span AS (
                SELECT p.symbol, p.peak_ts, mt.end_ts
                FROM peak p
                JOIN max_ts mt ON mt.symbol = p.symbol
            ),
            trading_days AS (
                SELECT
                    b.symbol,
                    COUNT(DISTINCT (b.ts::date))::int AS td
                FROM base b
                JOIN span s ON s.symbol = b.symbol
                WHERE b.ts >= s.peak_ts AND b.ts <= s.end_ts
                GROUP BY b.symbol
            ),
            eligible AS (
                SELECT s.symbol, s.peak_ts
                FROM span s
                JOIN trading_days d ON d.symbol = s.symbol
                WHERE d.td > :min_trading_days
            )
            SELECT t.*
            FROM {src_fqn} t
            JOIN eligible e
              ON t."SYMBOL" = e.symbol
            WHERE t."{ts_col}" >= e.peak_ts;
        """)

        # Indexes for speed in downstream steps
        q_index_1 = text(f'CREATE INDEX IF NOT EXISTS ix_{target_table.lower()}_symbol_ts ON {tgt_fqn} ("SYMBOL", "{ts_col}");')
        q_index_2 = text(f'CREATE INDEX IF NOT EXISTS ix_{target_table.lower()}_row_id ON {tgt_fqn} ("ROW_ID");')

        q_after = text(f'''
            SELECT
                COUNT(*)::bigint AS rows,
                COUNT(DISTINCT "SYMBOL")::bigint AS symbols
            FROM {tgt_fqn};
        ''')

        with self.engine.begin() as conn:
            before = conn.execute(q_before).mappings().one()
            conn.execute(q_drop)
            conn.execute(q_create, {"min_trading_days": min_trading_days})
            conn.execute(q_index_1)
            conn.execute(q_index_2)
            after = conn.execute(q_after).mappings().one()

        return {
            "exchange": exchange,
            "source": f"{source_schema}.{source_table}",
            "target": f"{target_schema}.{target_table}",
            "before_rows": int(before["rows"]),
            "before_symbols": int(before["symbols"]),
            "after_rows": int(after["rows"]),
            "after_symbols": int(after["symbols"]),
        }

    def rebuild_frvp_focus_symbol_list(self) -> dict:
        """
        Rebuilds silver.FRVP_FOCUS_SYMBOL_LIST from the two focus datasets.
        Drops and recreates the table each run.
        """
        tgt = 'silver."FRVP_FOCUS_SYMBOL_LIST"'
        bist = 'silver."FRVP_BIST_FOCUS_DATASET"'
        usa = 'silver."FRVP_USA_FOCUS_DATASET"'

        q_drop = text(f'DROP TABLE IF EXISTS {tgt};')
        q_create = text(f"""
            CREATE TABLE {tgt} AS
            SELECT DISTINCT
                "SYMBOL" AS "SYMBOL",
                'BIST'::text AS "EXCHANGE",
                TRUE AS "IN_SCOPE"
            FROM {bist}
            UNION
            SELECT DISTINCT
                "SYMBOL" AS "SYMBOL",
                'USA'::text AS "EXCHANGE",
                TRUE AS "IN_SCOPE"
            FROM {usa};
        """)
        q_index = text(f'CREATE INDEX IF NOT EXISTS ix_frvp_focus_symbol_list ON {tgt} ("EXCHANGE", "SYMBOL");')
        q_count = text(f'SELECT COUNT(*)::bigint AS rows FROM {tgt};')

        with self.engine.begin() as conn:
            conn.execute(q_drop)
            conn.execute(q_create)
            conn.execute(q_index)
            rows = conn.execute(q_count).mappings().one()["rows"]

        return {"rows": int(rows)}
    
    
###############################################
# FRVP POC CALC
###############################################


    def get_frvp_focus_symbols(self, exchange: str) -> List[str]:
        q = text("""
            SELECT "SYMBOL"
            FROM silver."FRVP_FOCUS_SYMBOL_LIST"
            WHERE "IN_SCOPE" = true
              AND "EXCHANGE" = :exchange
            ORDER BY "SYMBOL";
        """)
        with self.engine.begin() as conn:
            rows = conn.execute(q, {"exchange": exchange}).fetchall()
        return [r[0] for r in rows]

    def get_symbol_max_ts(
        self,
        table: str,
        symbol: str,
        ts_col: str = "TS",
    ) -> Optional[datetime]:
        q = text(f"""
            SELECT MAX("{ts_col}") AS max_ts
            FROM silver."{table}"
            WHERE "SYMBOL" = :symbol;
        """)
        with self.engine.begin() as conn:
            row = conn.execute(q, {"symbol": symbol}).fetchone()
        return row[0] if row and row[0] is not None else None

    def fetch_symbol_ohlcv_between(
        self,
        table: str,
        symbol: str,
        start_ts: datetime,
        end_ts: datetime,
        ts_col: str = "TS",
        chunk_size: int = 50000,
    ) -> List[Dict[str, Any]]:
        """
        Streams rows in chunks to avoid huge memory spikes.
        Returns list of dicts; service converts to pandas DataFrame.
        """
        q = text(f"""
            SELECT
                "SYMBOL",
                "{ts_col}" AS "TS",
                "OPEN",
                "HIGH",
                "LOW",
                "CLOSE",
                "VOLUME",
                "ROW_ID"
            FROM silver."{table}"
            WHERE "SYMBOL" = :symbol
              AND "{ts_col}" >= :start_ts
              AND "{ts_col}" <= :end_ts
            ORDER BY "{ts_col}" ASC;
        """)

        out: List[Dict[str, Any]] = []
        with self.engine.begin() as conn:
            # Prevent hanging forever
            conn.execute(text("SET LOCAL statement_timeout = 300000"))

            result = conn.execution_options(stream_results=True).execute(
                q,
                {"symbol": symbol, "start_ts": start_ts, "end_ts": end_ts},
            ).mappings()

            while True:
                batch = result.fetchmany(chunk_size)
                if not batch:
                    break
                out.extend([dict(r) for r in batch])

        return out

    def delete_ind_frvp_scope(
        self,
        exchange: str,
        interval: str,
        periods: List[str],
    ) -> int:
        q = text("""
            DELETE FROM silver."IND_FRV_POC_PROFILE"
            WHERE "EXCHANGE" = :exchange
              AND "INTERVAL" = :interval
              AND "FRVP_PERIOD_TYPE" = ANY(:periods);
        """)
        with self.engine.begin() as conn:
            res = conn.execute(q, {"exchange": exchange, "interval": interval, "periods": periods})
            return int(res.rowcount or 0)

    def insert_ind_frvp_rows(self, rows: List[Dict[str, Any]]) -> int:
        """
        Inserts result rows. Assumes caller cleared scope or table has a suitable unique constraint.
        """
        if not rows:
            return 0

        q = text("""
            INSERT INTO silver."IND_FRV_POC_PROFILE" (
                "EXCHANGE","SYMBOL","INTERVAL","FRVP_PERIOD_TYPE",
                "BASED_PERIOD",
                "MIN_DATE","HIGHEST_DATE","HIGHEST_VALUE","MAX_DATE",
                "HIGHEST_ROW_ID","ROW_COUNT_AFTER_HIGHEST","DAY_COUNT_AFTER_HIGHEST",
                "CUTT_OFF_DATE",
                "POC","VAL","VAH",
                "RUNTIME",
                "LATEST_CLOSE_VALUE"
            )
            VALUES (
                :EXCHANGE,:SYMBOL,:INTERVAL,:FRVP_PERIOD_TYPE,
                :BASED_PERIOD,
                :MIN_DATE,:HIGHEST_DATE,:HIGHEST_VALUE,:MAX_DATE,
                :HIGHEST_ROW_ID,:ROW_COUNT_AFTER_HIGHEST,:DAY_COUNT_AFTER_HIGHEST,
                :CUTT_OFF_DATE,
                :POC,:VAL,:VAH,
                :RUNTIME,
                :LATEST_CLOSE_VALUE
            );
        """)

        # Ensure missing keys don't crash executemany and don't become "unbound"
        normalized_rows: List[Dict[str, Any]] = []
        for r in rows:
            rr = dict(r)
            rr.setdefault("BASED_PERIOD", None)
            rr.setdefault("LATEST_CLOSE_VALUE", None)
            normalized_rows.append(rr)

        with self.engine.begin() as conn:
            # NOTE: Use a literal integer, don't bind params here.
            conn.execute(text("SET LOCAL statement_timeout = 300000"))
            conn.execute(q, normalized_rows)

        return len(normalized_rows)

    def log_indicator_error(
        self,
        job_name: str,
        calc_group: str,
        calc_name: str,
        exchange: str,
        symbol: str,
        interval: Optional[str],
        frvp_period_type: Optional[str],
        error_type: str,
        error_message: str,
        error_stack: str,
    ) -> None:
        q = text("""
            INSERT INTO logs."INDICATOR_ERRORS" (
                "JOB_NAME","CALC_GROUP","CALC_NAME","EXCHANGE","SYMBOL",
                "INTERVAL","FRVP_PERIOD_TYPE",
                "ERROR_TYPE","ERROR_MESSAGE","ERROR_STACK"
            )
            VALUES (
                :job_name,:calc_group,:calc_name,:exchange,:symbol,
                :interval,:frvp_period_type,
                :error_type,:error_message,:error_stack
            );
        """)
        with self.engine.begin() as conn:
            conn.execute(q, {
                "job_name": job_name,
                "calc_group": calc_group,
                "calc_name": calc_name,
                "exchange": exchange,
                "symbol": symbol,
                "interval": interval,
                "frvp_period_type": frvp_period_type,
                "error_type": error_type,
                "error_message": error_message,
                "error_stack": error_stack,
            })

    def get_symbol_max_ts(self, table: str, symbol: str, ts_col: str = "TS") -> Optional[datetime]:
        q = text(f"""
            SELECT MAX("{ts_col}") AS max_ts
            FROM silver."{table}"
            WHERE "SYMBOL" = :symbol;
        """)
        with self.engine.begin() as conn:
            row = conn.execute(q, {"symbol": symbol}).fetchone()
        return row[0] if row and row[0] is not None else None

    def get_peak_ts_in_window(
        self,
        table: str,
        symbol: str,
        start_ts: datetime,
        end_ts: datetime,
        ts_col: str = "TS",
        high_col: str = "HIGH",
    ) -> Optional[datetime]:
        """
        Finds earliest timestamp where HIGH is maximum within [start_ts, end_ts].
        """
        q = text(f"""
            WITH w AS (
                SELECT "{ts_col}" AS ts, "{high_col}" AS high
                FROM silver."{table}"
                WHERE "SYMBOL" = :symbol
                  AND "{ts_col}" >= :start_ts
                  AND "{ts_col}" <= :end_ts
                  AND "{high_col}" IS NOT NULL
            ),
            mh AS (
                SELECT MAX(high) AS max_high FROM w
            )
            SELECT MIN(w.ts) AS peak_ts
            FROM w
            JOIN mh ON w.high = mh.max_high;
        """)
        with self.engine.begin() as conn:
            row = conn.execute(q, {"symbol": symbol, "start_ts": start_ts, "end_ts": end_ts}).fetchone()
        return row[0] if row and row[0] is not None else None

    def get_row_id_at_ts(
        self,
        table: str,
        symbol: str,
        ts_value: datetime,
        ts_col: str = "TS",
    ) -> Optional[str]:
        q = text(f"""
            SELECT "ROW_ID"
            FROM silver."{table}"
            WHERE "SYMBOL" = :symbol
              AND "{ts_col}" = :ts
            ORDER BY "{ts_col}" ASC
            LIMIT 1;
        """)
        with self.engine.begin() as conn:
            row = conn.execute(q, {"symbol": symbol, "ts": ts_value}).fetchone()
        return row[0] if row and row[0] is not None else None

    def fetch_ohlcv_between_no_rowid(
        self,
        table: str,
        symbol: str,
        start_ts: datetime,
        end_ts: datetime,
        ts_col: str = "TS",
        chunk_size: int = 100000,
    ) -> List[Dict[str, Any]]:
        """
        Fetches OHLCV between timestamps. Does NOT select ROW_ID to reduce transfer cost.
        """
        q = text(f"""
            SELECT
                "{ts_col}" AS "TS",
                "OPEN","HIGH","LOW","CLOSE","VOLUME"
            FROM silver."{table}"
            WHERE "SYMBOL" = :symbol
              AND "{ts_col}" >= :start_ts
              AND "{ts_col}" <= :end_ts
            ORDER BY "{ts_col}" ASC;
        """)
        out: List[Dict[str, Any]] = []
        with self.engine.begin() as conn:
            result = conn.execution_options(stream_results=True).execute(
                q,
                {"symbol": symbol, "start_ts": start_ts, "end_ts": end_ts},
            ).mappings()

            while True:
                batch = result.fetchmany(chunk_size)
                if not batch:
                    break
                # This is still dict conversion, but much smaller dataset now.
                out.extend([dict(r) for r in batch])

        return out
    
    def get_max_ts_for_symbol(
        self,
        schema: str,
        table: str,
        symbol: str,
        ts_col: str = "TS",
    ) -> Optional[datetime]:
        q = text(f"""
            SELECT MAX("{ts_col}") AS max_ts
            FROM {schema}."{table}"
            WHERE "SYMBOL" = :symbol
        """)
        with self.engine.begin() as conn:
            r = conn.execute(q, {"symbol": symbol}).scalar()
        return r
    
    # sync to bronze from raw. but if daily, then truncate and take last 2years. 
    def sync_archive_to_working(
        self,
        archive_schema: str,
        archive_table: str,
        working_schema: str,
        working_table: str,
        ts_col: str = "TS",
        safety_days: int = 1,
        interval: str = "",
        sync_start_date: str | None = None, #format: "2024-03-05"
    ) -> int:
        """
        Sync archive data into working table.

        Modes
        -----
        interval = "1min":
            Append-only sync. Uses working MAX(TS) as watermark and subtracts safety_days
            to avoid missing edge minutes. Inserts with ON CONFLICT DO NOTHING.

        interval = "daily":
            Full refresh for working table. Truncates working table first, then reloads data
            from archive where TS >= sync_start_date 00:00:00.
        """
        interval = interval.strip().lower()

        if interval not in {"1min", "daily"}:
            raise ValueError("interval must be either '1min' or 'daily'")

        cols = '"SYMBOL","TIMESTAMP","TS","OPEN","HIGH","LOW","CLOSE","VOLUME","SOURCE","ROW_ID"'

        get_max_q = text(f"""
            SELECT MAX("{ts_col}") AS max_ts
            FROM {working_schema}.{working_table};
        """)

        truncate_q = text(f"""
            TRUNCATE TABLE {working_schema}.{working_table};
        """)

        insert_incremental_q = text(f"""
            INSERT INTO {working_schema}.{working_table} ({cols})
            SELECT {cols}
            FROM {archive_schema}.{archive_table} a
            WHERE a."{ts_col}" >= :from_ts
            ON CONFLICT ("ROW_ID") DO NOTHING;
        """)

        insert_full_refresh_q = text(f"""
            INSERT INTO {working_schema}.{working_table} ({cols})
            SELECT {cols}
            FROM {archive_schema}.{archive_table} a
            WHERE a."{ts_col}" >= :from_ts;
        """)

        with self.engine.begin() as conn:
            if interval == "1min":
                max_ts = conn.execute(get_max_q).scalar()

                if max_ts is None:
                    from_ts = datetime(1900, 1, 1)
                else:
                    from_ts = max_ts - timedelta(days=safety_days)

                res = conn.execute(insert_incremental_q, {"from_ts": from_ts})
                return int(res.rowcount or 0)

            # interval == "daily"
            if not sync_start_date:
                raise ValueError("sync_start_date must be provided when interval='daily'")

            try:
                from_ts = datetime.strptime(sync_start_date, "%Y-%m-%d")
            except ValueError as exc:
                raise ValueError("sync_start_date must be in 'YYYY-MM-DD' format") from exc

            conn.execute(truncate_q)
            res = conn.execute(insert_full_refresh_q, {"from_ts": from_ts})
            return int(res.rowcount or 0)
    
    def get_high_at_ts(
        self,
        table: str,
        symbol: str,
        ts_value: datetime,
        ts_col: str = "TS",
        high_col: str = "HIGH",
        schema: str = "silver",
    ) -> Optional[float]:
        q = text(f"""
            SELECT "{high_col}"::double precision AS high
            FROM {schema}."{table}"
            WHERE "SYMBOL" = :symbol
            AND "{ts_col}" = :ts
            ORDER BY "{ts_col}" ASC
            LIMIT 1
        """)

        with self.engine.connect() as conn:
            row = conn.execute(q, {"symbol": symbol, "ts": ts_value}).fetchone()

        return float(row[0]) if row and row[0] is not None else None
        
    def get_latest_close_value(
        self,
        table: str,
        symbol: str,
        ts_col: str = "TS",
        close_col: str = "CLOSE",
        schema: str = "silver",
    ) -> Optional[float]:
        """
        Returns CLOSE value at the maximum timestamp for the given symbol.
        Uses a single indexed query.
        """
        q = text(f"""
            SELECT t."{close_col}"::double precision AS close
            FROM {schema}."{table}" t
            WHERE t."SYMBOL" = :symbol
            ORDER BY t."{ts_col}" DESC
            LIMIT 1;
        """)
        with self.engine.connect() as conn:
            row = conn.execute(q, {"symbol": symbol}).fetchone()
            return float(row[0]) if row and row[0] is not None else None
        
    # find min poc and calc if higher than close value
    def update_in_scope_for_ema_rsi(
        self,
        exchange: str,
        interval: str,
    ) -> Tuple[int, int]:
        """
        Sets IN_SCOPE_FOR_EMA_RSI per (EXCHANGE, SYMBOL) based on:
        latest_close_value > MIN(POC) over all periods.
        Updates all period rows for that symbol.

        Returns:
        (unique_symbol_count, unique_true_symbol_count)
        """
        exchange = exchange.upper().strip()

        # 1) Update flag for all rows in-scope
        q_update = text("""
            WITH agg AS (
                SELECT
                    "EXCHANGE",
                    "SYMBOL",
                    MIN("POC") AS min_poc,
                    MAX("LATEST_CLOSE_VALUE") AS latest_close
                FROM silver."IND_FRV_POC_PROFILE"
                WHERE "EXCHANGE" = :exchange
                AND "INTERVAL" = :interval
                AND "POC" IS NOT NULL
                AND "LATEST_CLOSE_VALUE" IS NOT NULL
                GROUP BY "EXCHANGE", "SYMBOL"
            )
            UPDATE silver."IND_FRV_POC_PROFILE" t
            SET "IN_SCOPE_FOR_EMA_RSI" = (a.latest_close > a.min_poc)
            FROM agg a
            WHERE t."EXCHANGE" = a."EXCHANGE"
            AND t."SYMBOL" = a."SYMBOL"
            AND t."INTERVAL" = :interval;
        """)

        # 2) Counts (unique symbols + unique true symbols)
        q_counts = text("""
            WITH agg AS (
                SELECT
                    "EXCHANGE",
                    "SYMBOL",
                    MIN("POC") AS min_poc,
                    MAX("LATEST_CLOSE_VALUE") AS latest_close
                FROM silver."IND_FRV_POC_PROFILE"
                WHERE "EXCHANGE" = :exchange
                AND "INTERVAL" = :interval
                AND "POC" IS NOT NULL
                AND "LATEST_CLOSE_VALUE" IS NOT NULL
                GROUP BY "EXCHANGE", "SYMBOL"
            )
            SELECT
                COUNT(*) AS total_symbols,
                SUM(CASE WHEN (latest_close > min_poc) THEN 1 ELSE 0 END) AS true_symbols
            FROM agg;
        """)

        with self.engine.begin() as conn:
            conn.execute(q_update, {"exchange": exchange, "interval": interval})
            row = conn.execute(q_counts, {"exchange": exchange, "interval": interval}).one()
            total_symbols = int(row[0] or 0)
            true_symbols = int(row[1] or 0)

        return total_symbols, true_symbols
    

    #daily chapter
    def get_active_error_symbols(
        self,
        schema: str,
        table: str,
        job_name: str,
        exchange: str,
    ) -> List[str]:
        """
        Returns distinct symbols that currently exist in ingestion error table
        for a given job+exchange.
        """
        q = text(f"""
            SELECT DISTINCT "symbol"
            FROM {schema}.{table}
            WHERE "job_name" = :job_name
              AND "exchange" = :exchange
            ORDER BY "symbol";
        """)
        with self.engine.connect() as conn:
            rows = conn.execute(q, {"job_name": job_name, "exchange": exchange}).fetchall()
        return [r[0] for r in rows] if rows else []

    def clear_ingestion_error(
        self,
        schema: str,
        table: str,
        job_name: str,
        exchange: str,
        symbol: str,
    ) -> None:
        """
        Deletes any existing error rows for (job_name, exchange, symbol).
        Idempotent: safe to call even if no rows exist.
        """
        q = text(f"""
            DELETE FROM {schema}.{table}
            WHERE "job_name" = :job_name
              AND "exchange" = :exchange
              AND "symbol" = :symbol;
        """)
        with self.engine.begin() as conn:
            conn.execute(q, {"job_name": job_name, "exchange": exchange, "symbol": symbol})

    ######## convert from daily from min bars   ########
    def ensure_converted_daily_table(self, schema: str, table: str) -> None:
        """
        Ensures the converted-daily table exists with the expected schema and PK.
        """
        ddl = f"""
        CREATE SCHEMA IF NOT EXISTS {schema};

        CREATE TABLE IF NOT EXISTS {schema}."{table}" (
            "EXCHANGE"       text NOT NULL,
            "SYMBOL"         text NOT NULL,
            "INTERVAL"       text NOT NULL,
            "TIMESTAMP"      timestamp NOT NULL,

            "MIN_DATE"       timestamp NULL,
            "MAX_DATE"       timestamp NULL,
            "HIGHEST_VALUE"  double precision NULL,
            "HIGHEST_DATE"   timestamp NULL,

            "OPEN"           double precision NULL,
            "HIGH"           double precision NULL,
            "LOW"            double precision NULL,
            "CLOSE"          double precision NULL,
            "VOLUME"         double precision NULL,

            "BAR_COUNT"      integer NOT NULL DEFAULT 0,

            CONSTRAINT "{table}_pk" PRIMARY KEY ("EXCHANGE","SYMBOL","TIMESTAMP")
        );
        """
        with self.engine.begin() as conn:
            conn.execute(text(ddl))

        # Helpful index for reads
        with self.engine.begin() as conn:
            conn.execute(text(f"""
                CREATE INDEX IF NOT EXISTS ix_{table}_symbol_ts
                ON {schema}."{table}" ("SYMBOL","TIMESTAMP");
            """))

    def truncate_table(self, schema: str, table: str) -> None:
        """
        Truncates a table (fast delete).
        """
        with self.engine.begin() as conn:
            conn.execute(text(f'TRUNCATE TABLE {schema}."{table}";'))

    def build_converted_daily_for_ema_rsi_scope(
        self,
        exchange: str,
        interval: str,  # "1min" or "daily"
        start_trading_days_back: int,
        source_schema: str,
        source_table: str,
        ts_col: str,
        high_col: str,
        target_schema: str,
        target_table: str,
    ) -> dict:
        exchange = exchange.upper().strip()
        interval = interval.lower().strip()

        # 1) Scope symbols
        q_syms = text("""
            SELECT DISTINCT "SYMBOL"
            FROM silver."IND_FRV_POC_PROFILE"
            WHERE "EXCHANGE" = :exchange
            AND "IN_SCOPE_FOR_EMA_RSI" = true
            ORDER BY "SYMBOL";
        """)

        with self.engine.begin() as conn:
            syms = [r[0] for r in conn.execute(q_syms, {"exchange": exchange}).fetchall()]

        before_symbols = len(syms)

        # Always truncate target
        with self.engine.begin() as conn:
            conn.execute(text(f'TRUNCATE TABLE {target_schema}."{target_table}";'))

        if not syms:
            return {
                "exchange": exchange,
                "before_symbols": 0,
                "after_symbols": 0,
                "after_rows": 0,
                "target": f'{target_schema}.{target_table}',
            }

        if interval == "daily":
            # Daily input: keep last N calendar days from each symbol's latest date
            q = text(f"""
                WITH scope AS (
                    SELECT UNNEST(CAST(:symbols AS text[])) AS symbol
                ),
                base AS (
                    SELECT
                        t."SYMBOL" AS symbol,
                        (t."{ts_col}")::timestamp AS ts,
                        date_trunc('day', (t."{ts_col}")::timestamp) AS day_ts,
                        t."OPEN"::double precision AS open,
                        t."HIGH"::double precision AS high,
                        t."LOW"::double precision AS low,
                        t."CLOSE"::double precision AS close,
                        t."VOLUME"::double precision AS volume
                    FROM {source_schema}."{source_table}" t
                    JOIN scope s ON s.symbol = t."SYMBOL"
                    WHERE t."{ts_col}" IS NOT NULL
                    AND t."OPEN" IS NOT NULL
                    AND t."HIGH" IS NOT NULL
                    AND t."LOW"  IS NOT NULL
                    AND t."CLOSE" IS NOT NULL
                ),
                symbol_day_bounds AS (
                    SELECT
                        symbol,
                        MAX(day_ts) AS max_day_ts,
                        (MAX(day_ts) - (:n_days * INTERVAL '1 day')) AS min_keep_day_ts
                    FROM base
                    GROUP BY symbol
                ),
                filtered AS (
                    SELECT b.*
                    FROM base b
                    JOIN symbol_day_bounds sb
                    ON sb.symbol = b.symbol
                    AND b.day_ts >= sb.min_keep_day_ts
                    AND b.day_ts <= sb.max_day_ts
                ),
                sym_stats AS (
                    SELECT
                        symbol,
                        MIN(ts) AS min_date,
                        MAX(ts) AS max_date,
                        MAX(high) AS highest_value
                    FROM filtered
                    GROUP BY symbol
                ),
                highest_date AS (
                    SELECT f.symbol, MIN(f.ts) AS highest_date
                    FROM filtered f
                    JOIN sym_stats s
                    ON s.symbol = f.symbol
                    AND f.high = s.highest_value
                    GROUP BY f.symbol
                )
                INSERT INTO {target_schema}."{target_table}" (
                    "EXCHANGE","SYMBOL","INTERVAL","TIMESTAMP",
                    "MIN_DATE","MAX_DATE","HIGHEST_VALUE","HIGHEST_DATE",
                    "OPEN","HIGH","LOW","CLOSE","VOLUME","BAR_COUNT"
                )
                SELECT
                    :exchange AS "EXCHANGE",
                    f.symbol AS "SYMBOL",
                    'DAILY' AS "INTERVAL",
                    f.day_ts AS "TIMESTAMP",
                    s.min_date AS "MIN_DATE",
                    s.max_date AS "MAX_DATE",
                    s.highest_value AS "HIGHEST_VALUE",
                    h.highest_date AS "HIGHEST_DATE",
                    f.open AS "OPEN",
                    f.high AS "HIGH",
                    f.low  AS "LOW",
                    f.close AS "CLOSE",
                    COALESCE(f.volume, 0.0) AS "VOLUME",
                    0 AS "BAR_COUNT"
                FROM filtered f
                JOIN sym_stats s ON s.symbol = f.symbol
                JOIN highest_date h ON h.symbol = f.symbol
                ORDER BY f.symbol, f.day_ts;
            """)

            params = {
                "exchange": exchange,
                "symbols": syms,
                "n_days": int(start_trading_days_back),
            }

        elif interval == "1min":
            # Minute input: aggregate to daily, but filter by last N calendar days
            q = text(f"""
                WITH scope AS (
                    SELECT UNNEST(CAST(:symbols AS text[])) AS symbol
                ),
                base AS (
                    SELECT
                        t."SYMBOL" AS symbol,
                        (t."{ts_col}")::timestamp AS ts,
                        date_trunc('day', (t."{ts_col}")::timestamp) AS day_ts,
                        t."OPEN"::double precision AS open,
                        t."HIGH"::double precision AS high,
                        t."LOW"::double precision AS low,
                        t."CLOSE"::double precision AS close,
                        COALESCE(t."VOLUME", 0)::double precision AS volume
                    FROM {source_schema}."{source_table}" t
                    JOIN scope s ON s.symbol = t."SYMBOL"
                    WHERE t."{ts_col}" IS NOT NULL
                    AND t."OPEN" IS NOT NULL
                    AND t."HIGH" IS NOT NULL
                    AND t."LOW"  IS NOT NULL
                    AND t."CLOSE" IS NOT NULL
                ),
                symbol_day_bounds AS (
                    SELECT
                        symbol,
                        MAX(day_ts) AS max_day_ts,
                        (MAX(day_ts) - (:n_days * INTERVAL '1 day')) AS min_keep_day_ts
                    FROM base
                    GROUP BY symbol
                ),
                filtered AS (
                    SELECT b.*
                    FROM base b
                    JOIN symbol_day_bounds sb
                    ON sb.symbol = b.symbol
                    AND b.day_ts >= sb.min_keep_day_ts
                    AND b.day_ts <= sb.max_day_ts
                ),
                sym_stats AS (
                    SELECT
                        symbol,
                        MIN(ts) AS min_date,
                        MAX(ts) AS max_date,
                        MAX(high) AS highest_value
                    FROM filtered
                    GROUP BY symbol
                ),
                highest_date AS (
                    SELECT f.symbol, MIN(f.ts) AS highest_date
                    FROM filtered f
                    JOIN sym_stats s
                    ON s.symbol = f.symbol
                    AND f.high = s.highest_value
                    GROUP BY f.symbol
                ),
                daily_agg AS (
                    SELECT
                        symbol,
                        day_ts,
                        MIN(ts) AS first_ts,
                        MAX(ts) AS last_ts,
                        MIN(low) AS low,
                        MAX(high) AS high,
                        SUM(volume) AS volume,
                        COUNT(*)::int AS bar_count
                    FROM filtered
                    GROUP BY symbol, day_ts
                ),
                daily_ohlc AS (
                    SELECT
                        a.symbol,
                        a.day_ts,
                        s.min_date,
                        s.max_date,
                        s.highest_value,
                        h.highest_date,
                        (SELECT f.open
                        FROM filtered f
                        WHERE f.symbol = a.symbol
                        AND f.day_ts = a.day_ts
                        AND f.ts = a.first_ts
                        LIMIT 1) AS open,
                        (SELECT f.close
                        FROM filtered f
                        WHERE f.symbol = a.symbol
                        AND f.day_ts = a.day_ts
                        AND f.ts = a.last_ts
                        LIMIT 1) AS close,
                        a.high,
                        a.low,
                        a.volume,
                        a.bar_count
                    FROM daily_agg a
                    JOIN sym_stats s ON s.symbol = a.symbol
                    JOIN highest_date h ON h.symbol = a.symbol
                )
                INSERT INTO {target_schema}."{target_table}" (
                    "EXCHANGE","SYMBOL","INTERVAL","TIMESTAMP",
                    "MIN_DATE","MAX_DATE","HIGHEST_VALUE","HIGHEST_DATE",
                    "OPEN","HIGH","LOW","CLOSE","VOLUME","BAR_COUNT"
                )
                SELECT
                    :exchange AS "EXCHANGE",
                    d.symbol AS "SYMBOL",
                    'CONVERTED_DAILY' AS "INTERVAL",
                    d.day_ts AS "TIMESTAMP",
                    d.min_date AS "MIN_DATE",
                    d.max_date AS "MAX_DATE",
                    d.highest_value AS "HIGHEST_VALUE",
                    d.highest_date AS "HIGHEST_DATE",
                    d.open AS "OPEN",
                    d.high AS "HIGH",
                    d.low AS "LOW",
                    d.close AS "CLOSE",
                    COALESCE(d.volume, 0.0) AS "VOLUME",
                    d.bar_count AS "BAR_COUNT"
                FROM daily_ohlc d
                ORDER BY d.symbol, d.day_ts;
            """)

            params = {
                "exchange": exchange,
                "symbols": syms,
                "n_days": int(start_trading_days_back),
            }

        else:
            raise ValueError(f"Unsupported interval: {interval} (use '1min' or 'daily')")

        # Execute insert
        with self.engine.begin() as conn:
            conn.execute(q, params)

            after_rows = conn.execute(
                text(f'SELECT COUNT(*) FROM {target_schema}."{target_table}";')
            ).scalar_one()

            after_symbols = conn.execute(
                text(f'SELECT COUNT(DISTINCT "SYMBOL") FROM {target_schema}."{target_table}";')
            ).scalar_one()

        return {
            "exchange": exchange,
            "before_symbols": before_symbols,
            "after_symbols": int(after_symbols),
            "after_rows": int(after_rows),
            "target": f'{target_schema}.{target_table}',
        }
    
    ####################### DATA QUALITY CHECK   ##############################
    def clear_dq_for_exchange(
        self,
        *,
        schema: str,
        table: str,
        exchange: str,
    ) -> int:
        """
        Deletes DQ log rows for a specific exchange only.
        Returns deleted row count (best-effort).
        """
        q = text(f'''
            DELETE FROM {schema}."{table}"
            WHERE "EXCHANGE" = :exchange;
        ''')
        with self.engine.begin() as conn:
            res = conn.execute(q, {"exchange": exchange})
            # rowcount is supported for DELETE in psycopg
            return int(res.rowcount or 0)

    def bulk_insert_rows(self, schema: str, table: str, rows: List[Dict[str, Any]]) -> int:
        """
        Bulk insert rows using executemany.

        Notes:
        - If a column is JSONB (e.g., SAMPLE_KEYS), pass it as a JSON string and CAST in SQL.
        """
        if not rows:
            return 0

        cols = list(rows[0].keys())

        # Build column list
        col_list = ", ".join([f'"{c}"' for c in cols])

        # Build values list with JSONB casting for SAMPLE_KEYS
        values_sql_parts: List[str] = []
        for c in cols:
            if c == "SAMPLE_KEYS":
                values_sql_parts.append("CAST(:SAMPLE_KEYS AS jsonb)")
            else:
                values_sql_parts.append(f":{c}")
        val_list = ", ".join(values_sql_parts)

        q = text(f'INSERT INTO {schema}."{table}" ({col_list}) VALUES ({val_list});')

        # Normalize JSON-ish values (dict/list) -> JSON string
        normalized: List[Dict[str, Any]] = []
        for r in rows:
            rr = dict(r)
            if "SAMPLE_KEYS" in rr and rr["SAMPLE_KEYS"] is not None:
                if isinstance(rr["SAMPLE_KEYS"], (dict, list)):
                    rr["SAMPLE_KEYS"] = json.dumps(rr["SAMPLE_KEYS"], ensure_ascii=False)
                else:
                    # If it's already a string (or jsonb from DB driver), keep as-is
                    rr["SAMPLE_KEYS"] = str(rr["SAMPLE_KEYS"])
            normalized.append(rr)

        with self.engine.begin() as conn:
            conn.execute(q, normalized)

        return len(normalized)
    
    def ensure_dq_status_column_on_poc_profile(self) -> None:
        """
        Ensures DQ_STATUS column exists on silver.FRVP_FOCUS_SYMBOL_LIST.
        """
        q = text("""
            ALTER TABLE silver."FRVP_FOCUS_SYMBOL_LIST"
            ADD COLUMN IF NOT EXISTS "DQ_STATUS" TEXT DEFAULT 'PASSED';
        """)
        with self.engine.begin() as conn:
            conn.execute(q)

    def apply_dq_to_poc_profile(self, reset_in_scope: bool = True) -> None:
        """
        Applies DQ results from logs.DQ_generic_check to silver.FRVP_FOCUS_SYMBOL_LIST.

        - FAILED symbols:
            IN_SCOPE_FOR_EMA_RSI = FALSE
            DQ_STATUS = FAILED_<CHECK_TYPE>|FAILED_<CHECK_TYPE>
        - PASSED symbols:
            DQ_STATUS = PASSED
        """

        self.ensure_dq_status_column_on_poc_profile()

        with self.engine.begin() as conn:

            # Reset DQ_STATUS
            conn.execute(text("""
                UPDATE silver."FRVP_FOCUS_SYMBOL_LIST"
                SET "DQ_STATUS" = 'PASSED';
            """))

            if reset_in_scope:
                conn.execute(text("""
                    UPDATE silver."FRVP_FOCUS_SYMBOL_LIST"
                    SET "IN_SCOPE" = TRUE;
                """))

            # Apply FAIL results
            conn.execute(text("""
                WITH failed AS (
                    SELECT
                        "SYMBOL" AS symbol,
                        string_agg(
                            DISTINCT ('FAILED_' || "CHECK_TYPE"),
                            '|' ORDER BY ('FAILED_' || "CHECK_TYPE")
                        ) AS dq_status
                    FROM logs."DQ_generic_check"
                    WHERE "STATUS" = 'FAIL'
                    GROUP BY "SYMBOL"
                )
                UPDATE silver."FRVP_FOCUS_SYMBOL_LIST" p
                SET "IN_SCOPE" = FALSE,
                    "DQ_STATUS" = f.dq_status
                FROM failed f
                WHERE p."SYMBOL" = f.symbol;
            """))

            # ---------------------------------------------------
            # Stats
            # ---------------------------------------------------

            failed_count_usa = conn.execute(text("""
                SELECT COUNT(DISTINCT "SYMBOL")
                FROM logs."DQ_generic_check"
                WHERE "STATUS"='FAIL' AND "EXCHANGE"='USA'
            """)).scalar()

            failed_count_bist = conn.execute(text("""
                SELECT COUNT(DISTINCT "SYMBOL")
                FROM logs."DQ_generic_check"
                WHERE "STATUS"='FAIL' AND "EXCHANGE"='BIST'
            """)).scalar()

            remaining_scope_usa = conn.execute(text("""
                SELECT COUNT(DISTINCT "SYMBOL")
                FROM silver."FRVP_FOCUS_SYMBOL_LIST"
                WHERE "EXCHANGE"= 'USA' AND "IN_SCOPE" = TRUE
            """)).scalar()

            remaining_scope_bist = conn.execute(text("""
                SELECT COUNT(DISTINCT "SYMBOL")
                FROM silver."FRVP_FOCUS_SYMBOL_LIST"
                WHERE "EXCHANGE"= 'BIST' AND "IN_SCOPE" = TRUE
            """)).scalar()

        print(f"[DQ] FAILED SYMBOL COUNT | USA = {failed_count_usa} | BIST = {failed_count_bist}")
        print(f"[DQ] After excluding failed DQ, USA UNIQUE SYMBOL COUNT = {remaining_scope_usa} | BIST UNIQUE SYMBOL COUNT = {remaining_scope_bist} ")

    ########### EMA CALC CHAPTER #########
    def get_ema_focus_symbols(self, exchange: str) -> list[str]:
        q = text("""
            SELECT DISTINCT "SYMBOL"
            FROM silver."IND_FRV_POC_PROFILE"
            WHERE "EXCHANGE" = :exchange
            AND "IN_SCOPE_FOR_EMA_RSI" = TRUE
            ORDER BY "SYMBOL";
        """)
        with self.engine.begin() as conn:
            rows = conn.execute(q, {"exchange": exchange}).fetchall()
        return [r[0] for r in rows]
    

    def delete_ind_ema_scope(self, exchange: str) -> int:
        q = text("""
            DELETE FROM silver."IND_EMA_FOCUS"
            WHERE "EXCHANGE" = :exchange;
        """)
        with self.engine.begin() as conn:
            res = conn.execute(q, {"exchange": exchange})
        return int(res.rowcount or 0)
    

    def fetch_last_n_days_close_for_symbols(
        self,
        schema: str,
        table: str,
        exchange: str,
        symbols: list[str],
        n_days: int,
        ts_col: str = "TIMESTAMP",
        close_col: str = "CLOSE",
    ) -> list[dict]:
        if not symbols:
            return []

        # expanding bind param (IN :symbols) için
        q = text(f"""
            WITH ranked AS (
                SELECT
                    "EXCHANGE" AS exchange,
                    "SYMBOL" AS symbol,
                    "{ts_col}" AS ts,
                    "{close_col}"::double precision AS close,
                    ROW_NUMBER() OVER (
                        PARTITION BY "SYMBOL"
                        ORDER BY "{ts_col}" DESC
                    ) AS rn
                FROM {schema}."{table}"
                WHERE "EXCHANGE" = :exchange
                AND "SYMBOL" IN :symbols
                AND "{ts_col}" IS NOT NULL
                AND "{close_col}" IS NOT NULL
            )
            SELECT exchange, symbol, ts, close
            FROM ranked
            WHERE rn <= :n_days
            ORDER BY symbol ASC, ts ASC;
        """).bindparams(bindparam("symbols", expanding=True))

        with self.engine.begin() as conn:
            rows = conn.execute(q, {"exchange": exchange, "symbols": symbols, "n_days": n_days}).fetchall()

        return [{"EXCHANGE": r[0], "SYMBOL": r[1], "TIMESTAMP": r[2], "CLOSE": r[3]} for r in rows]
    
    def insert_ind_ema_focus_rows(self, rows: list[dict]) -> int:
        if not rows:
            return 0

        q = text("""
            INSERT INTO silver."IND_EMA_FOCUS" (
                "EXCHANGE","SYMBOL","END_DATE",
                "EMA5","EMA20","EMA_STATUS","EMA_CROSS","DAYS_SINCE_CROSS",
                "CREATED_AT"
            )
            VALUES (
                :EXCHANGE,:SYMBOL,:END_DATE,
                :EMA5,:EMA20,:EMA_STATUS,:EMA_CROSS,:DAYS_SINCE_CROSS,
                :CREATED_AT
            );
        """)

        with self.engine.begin() as conn:
            conn.execute(q, rows)

        return len(rows)
    
    # delete last n days from table
    def delete_recent_days_by_last_ts(
        self,
        schema: str,
        table: str,
        ts_col: str = "TS",
        days_back: int = 1,
        ) -> None:
        """
        Deletes the latest trading day (or latest N calendar days based on the last TS date)
        from the given table for all symbols.

        Prints cleanup information.
        """

        if days_back < 1:
            raise ValueError("days_back must be >= 1")

        q_max_before = text(f'''
            SELECT MAX("{ts_col}")
            FROM {schema}.{table};
        ''')

        with self.engine.begin() as conn:

            max_ts_before = conn.execute(q_max_before).scalar()

            if max_ts_before is None:
                print(
                    f"[CLEANUP] {schema}.{table} | "
                    f"days_back={days_back} | "
                    f"table empty"
                )
                return

            last_day = max_ts_before.date()

            delete_from = last_day - timedelta(days=days_back - 1)
            delete_to = last_day + timedelta(days=1)

            q_delete = text(f'''
                DELETE FROM {schema}.{table}
                WHERE "{ts_col}" >= :delete_from
                AND "{ts_col}" < :delete_to
            ''')

            res = conn.execute(
                q_delete,
                {
                    "delete_from": delete_from,
                    "delete_to": delete_to,
                },
            )

            deleted_rows = res.rowcount

            q_max_after = text(f'''
                SELECT MAX("{ts_col}")
                FROM {schema}.{table};
            ''')

            max_ts_after = conn.execute(q_max_after).scalar()

        print(
            f"[CLEANUP] {schema}.{table} | "
            f"days_back={days_back} | "
            f"last_ts_before={max_ts_before} | "
            f"last_ts_after={max_ts_after} | "
            f"deleted_rows={deleted_rows}"
        )

    def rebuild_symbol_sample_dataset(
        self,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        symbols: Sequence[str],
        symbol_col: str = "SYMBOL",
        ts_col: str = "TS",
        trading_days_back: int = 30,
        truncate_target: bool = True,
    ) -> dict:
        """
        Rebuilds a sample dataset for the given symbols by taking the last N trading days
        per symbol from the source table and writing them into the target table.

        Assumptions:
        - source and target tables have the same column structure
        - ts_col is a typed timestamp column (recommended: TS)
        - trading day is calculated as DISTINCT ts_col::date per symbol

        Behavior:
        - Optionally truncates target table before insert
        - Rebuilds the dataset from scratch on every run

        Returns simple stats.
        """

        if trading_days_back < 1:
            raise ValueError("trading_days_back must be >= 1")

        def _is_safe_ident(x: str) -> bool:
            return x.replace("_", "").isalnum()

        for ident in (source_schema, source_table, target_schema, target_table, symbol_col, ts_col):
            if not _is_safe_ident(ident):
                raise ValueError(f"Unsafe identifier: {ident}")

        source_fqn = f'"{source_schema}"."{source_table}"'
        target_fqn = f'"{target_schema}"."{target_table}"'

        # Empty symbol list => just truncate target and exit
        if not symbols:
            with self.engine.begin() as conn:
                if truncate_target:
                    conn.execute(text(f"TRUNCATE TABLE {target_fqn};"))
            print(
                f"[SAMPLE] {target_fqn} rebuilt with 0 symbols | "
                f"source={source_fqn} | trading_days_back={trading_days_back} | inserted_rows=0"
            )
            return {
                "source": source_fqn,
                "target": target_fqn,
                "symbols_requested": 0,
                "symbols_inserted": 0,
                "inserted_rows": 0,
                "trading_days_back": trading_days_back,
            }

        q_before = text(f'''
            SELECT COUNT(*)::bigint AS rows
            FROM {target_fqn};
        ''')

        q_truncate = text(f"TRUNCATE TABLE {target_fqn};")

        q_insert = text(f"""
            WITH symbol_dates AS (
                SELECT
                    "{symbol_col}" AS symbol,
                    ("{ts_col}")::date AS trade_date
                FROM {source_fqn}
                WHERE "{symbol_col}" IN :symbols
                AND "{ts_col}" IS NOT NULL
                GROUP BY "{symbol_col}", ("{ts_col}")::date
            ),
            ranked_dates AS (
                SELECT
                    symbol,
                    trade_date,
                    DENSE_RANK() OVER (
                        PARTITION BY symbol
                        ORDER BY trade_date DESC
                    ) AS rn
                FROM symbol_dates
            ),
            eligible_ranges AS (
                SELECT
                    symbol,
                    MIN(trade_date) AS keep_from_date,
                    MAX(trade_date) AS keep_to_date
                FROM ranked_dates
                WHERE rn <= :trading_days_back
                GROUP BY symbol
            )
            INSERT INTO {target_fqn}
            SELECT s.*
            FROM {source_fqn} s
            JOIN eligible_ranges r
            ON s."{symbol_col}" = r.symbol
            WHERE (s."{ts_col}")::date >= r.keep_from_date
            AND (s."{ts_col}")::date <= r.keep_to_date
            ORDER BY s."{symbol_col}", s."{ts_col}";
        """).bindparams(bindparam("symbols", expanding=True))

        q_after = text(f'''
            SELECT
                COUNT(*)::bigint AS rows,
                COUNT(DISTINCT "{symbol_col}")::bigint AS symbols
            FROM {target_fqn};
        ''')

        with self.engine.begin() as conn:
            before_rows = conn.execute(q_before).scalar()

            if truncate_target:
                conn.execute(q_truncate)

            res = conn.execute(
                q_insert,
                {
                    "symbols": list(symbols),
                    "trading_days_back": trading_days_back,
                },
            )

            inserted_rows = int(res.rowcount) if res.rowcount is not None and res.rowcount >= 0 else 0
            after = conn.execute(q_after).mappings().one()

        print(
            f"[SAMPLE] {target_fqn} rebuilt | "
            f"source={source_fqn} | "
            f"symbols_requested={len(symbols)} | "
            f"symbols_inserted={int(after['symbols'])} | "
            f"trading_days_back={trading_days_back} | "
            f"rows_before={int(before_rows)} | "
            f"rows_after={int(after['rows'])} | "
            f"inserted_rows={inserted_rows}"
        )

    def fn_repo_write_to_google(
        self,
        schema: str,
        table: str,
        exchange: str,
        scope_col: str,
        cols: list[str] | None = None,
        sheet_name: str = "",
        replace_append: str = ''
    ):
        """
        Generic table reader.

        Parameters
        ----------
        schema : str
            Schema name
        table : str
            Table name
        exchange : str
            Exchange filter value
        scope_col : str
            Boolean scope column name
        cols : list[str] | None
            Column list. If empty or None -> SELECT *
        write_to_google : bool
            If True, writes result dataframe to Google Sheets
        sheet_name : str
            Target Google Sheet tab name

        Returns
        -------
        pandas.DataFrame
        """

        if cols and len(cols) > 0:
            col_list = ", ".join([f'"{c}"' for c in cols])
        else:
            col_list = "*"

        query = text(f"""
            SELECT {col_list}
            FROM "{schema}"."{table}"
            WHERE "{scope_col}" = TRUE
            AND "EXCHANGE" = :exchange
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql_query(query, conn, params={"exchange": exchange})

        if not sheet_name:
            raise ValueError("sheet_name must be provided when write_to_google=True")

        fn_write_to_google(
            df=df,
            sheet_name=sheet_name,
            replace_or_append=replace_append
        )

        print(f'[WRITE_GOOGLE] -{table}- has been saved into google sheets | {replace_append}')


    ###########################################
    # ANCHORED VWAP REPO CODES
    ###########################################

    # truncate vwap focus target
    def delete_ind_vwap_scope(
        self,
        schema: str,
        table: str,
        exchange: str,
    ) -> int:
        q = text(f'''
            DELETE FROM {schema}."{table}"
            WHERE "EXCHANGE" = :exchange
        ''')
        with self.engine.begin() as conn:
            res = conn.execute(q, {"exchange": exchange})
        return int(res.rowcount or 0)
    
    # vwap
    def fetch_vwap_focus_source_data(
        self,
        source_schema: str,
        source_table: str,
        exchange: str,
        lookback_month: int,
    ) -> List[Dict[str, Any]]:
        q = text(f'''
            WITH symbol_last_ts AS (
                SELECT
                    "SYMBOL",
                    MAX("TIMESTAMP") AS max_ts
                FROM {source_schema}."{source_table}"
                WHERE "EXCHANGE" = :exchange
                GROUP BY "SYMBOL"
            )
            SELECT
                s."EXCHANGE",
                s."SYMBOL",
                s."TIMESTAMP",
                s."HIGH",
                s."VOLUME"
            FROM {source_schema}."{source_table}" s
            JOIN symbol_last_ts l
            ON s."SYMBOL" = l."SYMBOL"
            WHERE s."EXCHANGE" = :exchange
            AND s."TIMESTAMP" >= (l.max_ts - make_interval(months => :lookback_month))
            AND s."TIMESTAMP" <= l.max_ts
            AND s."TIMESTAMP" IS NOT NULL
            AND s."HIGH" IS NOT NULL
            AND s."VOLUME" IS NOT NULL
            AND s."VOLUME" >= 0
            ORDER BY s."SYMBOL" ASC, s."TIMESTAMP" ASC
        ''')
        with self.engine.begin() as conn:
            rows = conn.execute(
                q,
                {
                    "exchange": exchange,
                    "lookback_month": lookback_month,
                }
            ).fetchall()

        return [dict(r._mapping) for r in rows]

    #insert vwap to sb
    def insert_ind_vwap_rows(
        self,
        target_schema: str,
        target_table: str,
        rows: List[Dict[str, Any]],
    ) -> int:
        if not rows:
            return 0

        q = text(f'''
            INSERT INTO {target_schema}."{target_table}" (
                "EXCHANGE",
                "SYMBOL",
                "START_TIME",
                "END_TIME",
                "HIGHEST_VALUE",
                "HIGHEST_TIMESTAMP",
                "VWAP",
                "AVG_VOLUME_10D",
                "AVG_VOLUME_20D",
                "AVG_VOLUME_30D",
                "CREATED_AT"
            )
            VALUES (
                :EXCHANGE,
                :SYMBOL,
                :START_TIME,
                :END_TIME,
                :HIGHEST_VALUE,
                :HIGHEST_TIMESTAMP,
                :VWAP,
                :AVG_VOLUME_10D,
                :AVG_VOLUME_20D,
                :AVG_VOLUME_30D,
                :CREATED_AT
            )
        ''')

        with self.engine.begin() as conn:
            conn.execute(q, rows)

        return len(rows)
    

    ### IND bar status identification
    def get_bar_status_focus_symbols(self, exchange: str) -> list[str]:
        q = text("""
            SELECT DISTINCT "SYMBOL"
            FROM silver."IND_FRV_POC_PROFILE"
            WHERE "EXCHANGE" = :exchange
            AND "IN_SCOPE_FOR_EMA_RSI" = TRUE
            ORDER BY "SYMBOL"
        """)
        with self.engine.begin() as conn:
            rows = conn.execute(q, {"exchange": exchange}).fetchall()
        return [r[0] for r in rows]
    
    def delete_ind_bar_status_scope(
        self,
        schema: str,
        table: str,
        exchange: str,
    ) -> int:
        q = text(f'''
            DELETE FROM {schema}."{table}"
            WHERE "EXCHANGE" = :exchange
        ''')
        with self.engine.begin() as conn:
            res = conn.execute(q, {"exchange": exchange})
        return int(res.rowcount or 0)


    def fetch_bar_status_source_rows(
        self,
        source_schema: str,
        source_table: str,
        exchange: str,
        symbols: list[str],
    ) -> list[dict]:
        if not symbols:
            return []

        q = text(f"""
            WITH base AS (
                SELECT
                    "SYMBOL",
                    "TIMESTAMP",
                    "OPEN",
                    "CLOSE"
                FROM {source_schema}."{source_table}"
                WHERE "SYMBOL" IN :symbols
                AND "TIMESTAMP" IS NOT NULL
                AND "OPEN" IS NOT NULL
                AND "CLOSE" IS NOT NULL
            ),
            last_day_per_symbol AS (
                SELECT
                    "SYMBOL",
                    MAX(DATE("TIMESTAMP")) AS last_day_date
                FROM base
                GROUP BY "SYMBOL"
            ),
            last_day_rows AS (
                SELECT b.*
                FROM base b
                JOIN last_day_per_symbol d
                ON b."SYMBOL" = d."SYMBOL"
                AND DATE(b."TIMESTAMP") = d.last_day_date
            ),
            ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY "SYMBOL"
                        ORDER BY "TIMESTAMP" ASC
                    ) AS rn_open,
                    ROW_NUMBER() OVER (
                        PARTITION BY "SYMBOL"
                        ORDER BY "TIMESTAMP" DESC
                    ) AS rn_close
                FROM last_day_rows
            ),
            opens AS (
                SELECT
                    "SYMBOL",
                    "TIMESTAMP" AS first_minute,
                    "OPEN"::double precision AS open_price
                FROM ranked
                WHERE rn_open = 1
            ),
            closes AS (
                SELECT
                    "SYMBOL",
                    "TIMESTAMP" AS last_minute,
                    "CLOSE"::double precision AS close_price
                FROM ranked
                WHERE rn_close = 1
            )
            SELECT
                o."SYMBOL",
                o.first_minute AS "FIRST_MINUTE",
                c.last_minute AS "LAST_MINUTE",
                o.open_price AS "OPEN_PRICE",
                c.close_price AS "CLOSE_PRICE"
            FROM opens o
            JOIN closes c
            ON o."SYMBOL" = c."SYMBOL"
            ORDER BY o."SYMBOL"
        """).bindparams(bindparam("symbols", expanding=True))

        with self.engine.begin() as conn:
            rows = conn.execute(q, {"symbols": symbols}).fetchall()

        out = []
        for r in rows:
            d = dict(r._mapping)
            d["EXCHANGE"] = exchange
            out.append(d)

        return out
    

    def insert_ind_bar_status_rows(
        self,
        target_schema: str,
        target_table: str,
        rows: List[Dict[str, Any]],
    ) -> int:
        if not rows:
            return 0

        q = text(f'''
            INSERT INTO {target_schema}."{target_table}" (
                "EXCHANGE",
                "SYMBOL",
                "FIRST_MINUTE",
                "LAST_MINUTE",
                "OPEN_PRICE",
                "CLOSE_PRICE",
                "DIFFER",
                "PERC",
                "BAR_STATUS",
                "CREATED_AT"
            )
            VALUES (
                :EXCHANGE,
                :SYMBOL,
                :FIRST_MINUTE,
                :LAST_MINUTE,
                :OPEN_PRICE,
                :CLOSE_PRICE,
                :DIFFER,
                :PERC,
                :BAR_STATUS,
                :CREATED_AT
            )
        ''')

        with self.engine.begin() as conn:
            conn.execute(q, rows)

        return len(rows)