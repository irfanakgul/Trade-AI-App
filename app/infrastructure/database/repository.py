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


    def get_cloned_focus_symbols(self, exchange: str) -> List[str]:
        q = text("""
            SELECT DISTINCT "SYMBOL"
            FROM silver."cloned_focus_symbol_list"
            WHERE "EXCHANGE" = :exchange
            AND "IN_SCOPE" IS TRUE
            AND "OUT_OF_SCOPE" IS FALSE
            ORDER BY "SYMBOL";
        """)
        with self.engine.begin() as conn:
            rows = conn.execute(q, {"exchange": exchange}).fetchall()
        return [r[0] for r in rows]

    def get_symbol_max_ts(
        self,
        schema:str,
        table: str,
        symbol: str,
        ts_col: str = "TS",
    ) -> Optional[datetime]:
        q = text(f"""
            SELECT MAX("{ts_col}") AS max_ts
            FROM {schema}."{table}"
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
        frvp_target_schema: str = '',
        frvp_target_table: str = ''
        
    ) -> int:
        
        q = text(f"""
            DELETE FROM {frvp_target_schema}."{frvp_target_table}"
            WHERE "EXCHANGE" = :exchange
        """)
        with self.engine.begin() as conn:
            res = conn.execute(q, {"exchange": exchange})
            return int(res.rowcount or 0)

    def insert_ind_frvp_rows(
        self,
        rows: List[Dict[str, Any]],
        schema: str = "",
        table: str = "",
    ) -> int:
        """
        Inserts result rows into target schema/table.
        """
        if not rows:
            return 0

        q = text(f"""
            INSERT INTO {schema}."{table}" (
                "EXCHANGE","SYMBOL","INTERVAL","FRVP_PERIOD_TYPE",
                "BASED_PERIOD",
                "MIN_DATE","HIGHEST_DATE","HIGHEST_VALUE","MAX_DATE",
                "HIGHEST_ROW_ID","ROW_COUNT_AFTER_HIGHEST","DAY_COUNT_AFTER_HIGHEST",
                "CUTT_OFF_DATE",
                "POC","VAL","VAH",
                "RUNTIME",
                "LATEST_CLOSE_VALUE",
                "IN_SCOPE_FOR_EMA_RSI"
            )
            VALUES (
                :EXCHANGE,:SYMBOL,:INTERVAL,:FRVP_PERIOD_TYPE,
                :BASED_PERIOD,
                :MIN_DATE,:HIGHEST_DATE,:HIGHEST_VALUE,:MAX_DATE,
                :HIGHEST_ROW_ID,:ROW_COUNT_AFTER_HIGHEST,:DAY_COUNT_AFTER_HIGHEST,
                :CUTT_OFF_DATE,
                :POC,:VAL,:VAH,
                :RUNTIME,
                :LATEST_CLOSE_VALUE,
                FALSE
            );
        """)

        # Ensure missing keys don't crash
        normalized_rows: List[Dict[str, Any]] = []
        for r in rows:
            rr = dict(r)
            rr.setdefault("BASED_PERIOD", None)
            rr.setdefault("LATEST_CLOSE_VALUE", None)
            rr.setdefault("IN_SCOPE_FOR_EMA_RSI", False)
            normalized_rows.append(rr)

        # 🔧 FIX: Loop yerine execute kullan
        with self.engine.begin() as conn:
            conn.execute(text("SET LOCAL statement_timeout = 300000"))
            
            # ✅ Her satır için execute çal (executemany yerine)
            for row in normalized_rows:
                conn.execute(q, row)

        return len(normalized_rows)
    
    def get_row_id_at_ts(
        self,
        schema: str,
        table: str,
        symbol: str,
        ts_value: datetime,
        ts_col: str = "TS",
        row_id_col: str = "ROW_ID",
    ) -> Optional[str]:
        """
        Gets ROW_ID at exact timestamp for a symbol.
        
        Parameters:
        -----------
        schema : str
            Schema name
        
        table : str
            Table name
        
        symbol : str
            Symbol to query
        
        ts_value : datetime
            Exact timestamp to find
        
        ts_col : str
            Timestamp column name (default: 'TS')
        
        row_id_col : str
            ROW_ID column name (default: 'ROW_ID')
        
        Returns:
        --------
        Optional[str] : ROW_ID value or None if not found
        """
        q = text(f"""
            SELECT "{row_id_col}"::text AS row_id
            FROM {schema}."{table}"
            WHERE "SYMBOL" = :symbol
            AND "{ts_col}" = :ts
            ORDER BY "{ts_col}" ASC
            LIMIT 1
        """)
    
        with self.engine.connect() as conn:
            row = conn.execute(q, {"symbol": symbol, "ts": ts_value}).fetchone()
    
        return str(row[0]) if row and row[0] is not None else None
    

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

    def get_latest_close_value(
        self,
        schema: str,          
        table: str,
        symbol: str,
        ts_col: str = "TS",
        close_col: str = "CLOSE",
    ) -> Optional[float]:
        q = text(f"""
            SELECT MAX("{ts_col}") AS max_ts
            FROM {schema}."{table}"
            WHERE "SYMBOL" = :symbol;
        """)
        with self.engine.begin() as conn:
            row = conn.execute(q, {"symbol": symbol}).fetchone()
        return row[0] if row and row[0] is not None else None

    def get_peak_ts_in_window(
        self,
        schema:str,
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
                FROM {schema}."{table}"
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
        schema:str,
        table: str,
        symbol: str,
        ts_value: datetime,
        ts_col: str = "TS",
    ) -> Optional[str]:
        q = text(f"""
            SELECT "ROW_ID"
            FROM {schema}."{table}"
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
        schema:str,
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
            FROM {schema}."{table}"
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

        if interval not in {"1min", "daily",'hourly'}:
            raise ValueError("interval must be either '1min', 'hourly' or 'daily'")

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
            if (interval == "1min") or (interval == "hourly"):
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
        schema:str,
        table: str,
        symbol: str,
        ts_value: datetime,
        ts_col: str = "TS",
        high_col: str = "HIGH",
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
        frvp_target_schema:str,
        frvp_target_table:str,
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
        q_update = text(f"""
            WITH agg AS (
                SELECT
                    "EXCHANGE",
                    "SYMBOL",
                    MIN("POC") AS min_poc,
                    MAX("LATEST_CLOSE_VALUE") AS latest_close
                FROM {frvp_target_schema}."{frvp_target_table}"
                WHERE "EXCHANGE" = :exchange
                AND "INTERVAL" = :interval
                AND "POC" IS NOT NULL
                AND "LATEST_CLOSE_VALUE" IS NOT NULL
                GROUP BY "EXCHANGE", "SYMBOL"
            )
            UPDATE {frvp_target_schema}."{frvp_target_table}" t
            SET "IN_SCOPE_FOR_EMA_RSI" = (a.latest_close > a.min_poc)
            FROM agg a
            WHERE t."EXCHANGE" = a."EXCHANGE"
            AND t."SYMBOL" = a."SYMBOL"
            AND t."INTERVAL" = :interval;
        """)

        # 2) Counts (unique symbols + unique true symbols)
        q_counts = text(f"""
            WITH agg AS (
                SELECT
                    "EXCHANGE",
                    "SYMBOL",
                    MIN("POC") AS min_poc,
                    MAX("LATEST_CLOSE_VALUE") AS latest_close
                FROM {frvp_target_schema}."{frvp_target_table}"
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


    def build_converted_daily_for_indicators(
        self,
        exchange: str,
        interval: str,  # "1min" or "daily" or "hourly"
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

        # 1) Count scope symbols directly from DB
        q_before = text("""
            SELECT COUNT(DISTINCT "SYMBOL")
            FROM silver."cloned_focus_symbol_list"
            WHERE "EXCHANGE" = :exchange
            AND "OUT_OF_SCOPE" = false
        """)

        with self.engine.begin() as conn:
            before_symbols = conn.execute(q_before, {"exchange": exchange}).scalar_one()

        # Always truncate target
        with self.engine.begin() as conn:
            conn.execute(text(f'TRUNCATE TABLE {target_schema}."{target_table}";'))

        if not before_symbols:
            return {
                "exchange": exchange,
                "before_symbols": 0,
                "after_symbols": 0,
                "after_rows": 0,
                "target": f"{target_schema}.{target_table}",
            }

        if interval not in ("daily", "1min", "hourly"):
            raise ValueError(f"Unsupported interval: {interval} (use '1min/hourly' or 'daily')")

        with self.engine.begin() as conn:
            # ----------------------------------------------------------
            # 2) Materialize filtered source once into a TEMP table
            #    Filter = last N calendar days from each symbol's latest date
            # ----------------------------------------------------------
            conn.execute(text("DROP TABLE IF EXISTS tmp_scope_symbols;"))
            conn.execute(text("DROP TABLE IF EXISTS tmp_filtered_src;"))
            conn.execute(text("DROP TABLE IF EXISTS tmp_sym_stats;"))
            conn.execute(text("DROP TABLE IF EXISTS tmp_highest_date;"))

            conn.execute(text("""
                CREATE TEMP TABLE tmp_scope_symbols ON COMMIT DROP AS
                SELECT DISTINCT "SYMBOL" AS symbol
                FROM silver."cloned_focus_symbol_list"
                WHERE "EXCHANGE" = :exchange
                AND "OUT_OF_SCOPE" = false;
            """), {"exchange": exchange})

            conn.execute(text("""
                CREATE INDEX idx_tmp_scope_symbols_symbol
                ON tmp_scope_symbols(symbol);
            """))

            conn.execute(text(f"""
                CREATE TEMP TABLE tmp_filtered_src ON COMMIT DROP AS
                WITH base AS (
                    SELECT
                        t."SYMBOL" AS symbol,
                        (t."{ts_col}")::timestamp AS ts,
                        date_trunc('day', (t."{ts_col}")::timestamp) AS day_ts,
                        t."OPEN"::double precision AS open,
                        t."{high_col}"::double precision AS high,
                        t."LOW"::double precision AS low,
                        t."CLOSE"::double precision AS close,
                        COALESCE(t."VOLUME", 0)::double precision AS volume
                    FROM {source_schema}."{source_table}" t
                    JOIN tmp_scope_symbols s
                    ON s.symbol = t."SYMBOL"
                    WHERE t."{ts_col}" IS NOT NULL
                    AND t."OPEN" IS NOT NULL
                    AND t."{high_col}" IS NOT NULL
                    AND t."LOW" IS NOT NULL
                    AND t."CLOSE" IS NOT NULL
                ),
                symbol_bounds AS (
                    SELECT
                        symbol,
                        MAX(ts) AS max_ts
                    FROM base
                    GROUP BY symbol
                )
                SELECT b.*
                FROM base b
                JOIN symbol_bounds sb
                ON sb.symbol = b.symbol
                WHERE b.ts >= (date_trunc('day', sb.max_ts) - (:n_days * INTERVAL '1 day'))
                AND b.ts <= sb.max_ts;
            """), {"n_days": int(start_trading_days_back)})

            conn.execute(text("""
                CREATE INDEX idx_tmp_filtered_src_symbol_day_ts
                ON tmp_filtered_src(symbol, day_ts, ts);
            """))
            conn.execute(text("""
                CREATE INDEX idx_tmp_filtered_src_symbol_high
                ON tmp_filtered_src(symbol, high);
            """))

            conn.execute(text("ANALYZE tmp_filtered_src;"))

            # ----------------------------------------------------------
            # 3) Materialize symbol-level stats once
            # ----------------------------------------------------------
            conn.execute(text("""
                CREATE TEMP TABLE tmp_sym_stats ON COMMIT DROP AS
                SELECT
                    symbol,
                    MIN(ts) AS min_date,
                    MAX(ts) AS max_date,
                    MAX(high) AS highest_value
                FROM tmp_filtered_src
                GROUP BY symbol;
            """))

            conn.execute(text("""
                CREATE INDEX idx_tmp_sym_stats_symbol
                ON tmp_sym_stats(symbol);
            """))

            conn.execute(text("""
                CREATE TEMP TABLE tmp_highest_date ON COMMIT DROP AS
                SELECT
                    f.symbol,
                    MIN(f.ts) AS highest_date
                FROM tmp_filtered_src f
                JOIN tmp_sym_stats s
                ON s.symbol = f.symbol
                AND f.high = s.highest_value
                GROUP BY f.symbol;
            """))

            conn.execute(text("""
                CREATE INDEX idx_tmp_highest_date_symbol
                ON tmp_highest_date(symbol);
            """))

            conn.execute(text("ANALYZE tmp_sym_stats;"))
            conn.execute(text("ANALYZE tmp_highest_date;"))

            # ----------------------------------------------------------
            # 4) Insert output
            # ----------------------------------------------------------
            if interval == "daily":
                conn.execute(text(f"""
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
                        f.low AS "LOW",
                        f.close AS "CLOSE",
                        COALESCE(f.volume, 0.0) AS "VOLUME",
                        0 AS "BAR_COUNT"
                    FROM tmp_filtered_src f
                    JOIN tmp_sym_stats s ON s.symbol = f.symbol
                    JOIN tmp_highest_date h ON h.symbol = f.symbol
                    ORDER BY f.symbol, f.day_ts;
                """), {"exchange": exchange})

            else:
                # 1min or hourly -> aggregate to daily
                conn.execute(text("""
                    DROP TABLE IF EXISTS tmp_open_rows;
                    DROP TABLE IF EXISTS tmp_close_rows;
                    DROP TABLE IF EXISTS tmp_daily_agg;
                """))

                # First open row of each day
                conn.execute(text("""
                    CREATE TEMP TABLE tmp_open_rows ON COMMIT DROP AS
                    SELECT DISTINCT ON (symbol, day_ts)
                        symbol,
                        day_ts,
                        open
                    FROM tmp_filtered_src
                    ORDER BY symbol, day_ts, ts ASC;
                """))

                # Last close row of each day
                conn.execute(text("""
                    CREATE TEMP TABLE tmp_close_rows ON COMMIT DROP AS
                    SELECT DISTINCT ON (symbol, day_ts)
                        symbol,
                        day_ts,
                        close
                    FROM tmp_filtered_src
                    ORDER BY symbol, day_ts, ts DESC;
                """))

                # Daily aggregate once
                conn.execute(text("""
                    CREATE TEMP TABLE tmp_daily_agg ON COMMIT DROP AS
                    SELECT
                        symbol,
                        day_ts,
                        MIN(low) AS low,
                        MAX(high) AS high,
                        SUM(volume) AS volume,
                        COUNT(*)::int AS bar_count
                    FROM tmp_filtered_src
                    GROUP BY symbol, day_ts;
                """))

                conn.execute(text("""
                    CREATE INDEX idx_tmp_open_rows_symbol_day
                    ON tmp_open_rows(symbol, day_ts);
                """))
                conn.execute(text("""
                    CREATE INDEX idx_tmp_close_rows_symbol_day
                    ON tmp_close_rows(symbol, day_ts);
                """))
                conn.execute(text("""
                    CREATE INDEX idx_tmp_daily_agg_symbol_day
                    ON tmp_daily_agg(symbol, day_ts);
                """))

                conn.execute(text("ANALYZE tmp_open_rows;"))
                conn.execute(text("ANALYZE tmp_close_rows;"))
                conn.execute(text("ANALYZE tmp_daily_agg;"))

                conn.execute(text(f"""
                    INSERT INTO {target_schema}."{target_table}" (
                        "EXCHANGE","SYMBOL","INTERVAL","TIMESTAMP",
                        "MIN_DATE","MAX_DATE","HIGHEST_VALUE","HIGHEST_DATE",
                        "OPEN","HIGH","LOW","CLOSE","VOLUME","BAR_COUNT"
                    )
                    SELECT
                        :exchange AS "EXCHANGE",
                        a.symbol AS "SYMBOL",
                        'CONVERTED_DAILY' AS "INTERVAL",
                        a.day_ts AS "TIMESTAMP",
                        s.min_date AS "MIN_DATE",
                        s.max_date AS "MAX_DATE",
                        s.highest_value AS "HIGHEST_VALUE",
                        h.highest_date AS "HIGHEST_DATE",
                        o.open AS "OPEN",
                        a.high AS "HIGH",
                        a.low AS "LOW",
                        c.close AS "CLOSE",
                        COALESCE(a.volume, 0.0) AS "VOLUME",
                        a.bar_count AS "BAR_COUNT"
                    FROM tmp_daily_agg a
                    JOIN tmp_open_rows o
                    ON o.symbol = a.symbol AND o.day_ts = a.day_ts
                    JOIN tmp_close_rows c
                    ON c.symbol = a.symbol AND c.day_ts = a.day_ts
                    JOIN tmp_sym_stats s
                    ON s.symbol = a.symbol
                    JOIN tmp_highest_date h
                    ON h.symbol = a.symbol
                    ORDER BY a.symbol, a.day_ts;
                """), {"exchange": exchange})

            after_rows = conn.execute(
                text(f'SELECT COUNT(*) FROM {target_schema}."{target_table}";')
            ).scalar_one()

            after_symbols = conn.execute(
                text(f'SELECT COUNT(DISTINCT "SYMBOL") FROM {target_schema}."{target_table}";')
            ).scalar_one()

        return {
            "exchange": exchange,
            "before_symbols": int(before_symbols),
            "after_symbols": int(after_symbols),
            "after_rows": int(after_rows),
            "target": f"{target_schema}.{target_table}",
        }

    ########### EMA CALC CHAPTER #########
    def get_ema_focus_symbols(self, exchange: str) -> list[str]:
        q = text("""
            SELECT DISTINCT "SYMBOL"
            FROM silver."IND_FRV_POC_PROFILE"
            WHERE "EXCHANGE" = :exchange
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
                "EXCHANGE","SYMBOL","TIMESTAMP","END_DATE",
                "EMA3","EMA5","EMA14","EMA20",
                "EMA_STATUS_5_20","EMA_CROSS_5_20",
                "EMA_STATUS_3_20","EMA_CROSS_3_20",
                "EMA_STATUS_3_14","EMA_CROSS_3_14",
                "DAYS_SINCE_CROSS_5_20","DAYS_SINCE_CROSS_3_20","DAYS_SINCE_CROSS_3_14",
                "CREATED_AT"
            )
            VALUES (
                :EXCHANGE,:SYMBOL,:TIMESTAMP,:END_DATE,
                :EMA3,:EMA5,:EMA14,:EMA20,
                :EMA_STATUS_5_20,:EMA_CROSS_5_20,
                :EMA_STATUS_3_20,:EMA_CROSS_3_20,
                :EMA_STATUS_3_14,:EMA_CROSS_3_14,
                :DAYS_SINCE_CROSS_5_20,:DAYS_SINCE_CROSS_3_20,:DAYS_SINCE_CROSS_3_14,
                :CREATED_AT
            );
        """)

        normalized_rows = []
        for r in rows:
            rr = dict(r)

            rr.setdefault("EMA3", None)
            rr.setdefault("EMA5", None)
            rr.setdefault("EMA14", None)
            rr.setdefault("EMA20", None)

            rr.setdefault("EMA_STATUS_5_20", None)
            rr.setdefault("EMA_CROSS_5_20", None)

            rr.setdefault("EMA_STATUS_3_20", None)
            rr.setdefault("EMA_CROSS_3_20", None)

            rr.setdefault("EMA_STATUS_3_14", None)
            rr.setdefault("EMA_CROSS_3_14", None)

            rr.setdefault("DAYS_SINCE_CROSS_5_20", None)
            rr.setdefault("DAYS_SINCE_CROSS_3_20", None)
            rr.setdefault("DAYS_SINCE_CROSS_3_14", None)

            normalized_rows.append(rr)

        with self.engine.begin() as conn:
            conn.execute(q, normalized_rows)

        return len(normalized_rows)
    
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

    # generic write to google
    def fn_repo_write_to_google_generic(
        self,
        schema: str,
        table: str,
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
        write_to_google : bool
            If True, writes result dataframe to Google Sheets
        sheet_name : str
            Target Google Sheet tab name

        Returns
        -------
        pandas.DataFrame
        """

        now_str = datetime.now().strftime('%d-%m-%Y %H:%M')

        query = text(f"""
            SELECT *
            FROM "{schema}"."{table}"
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql_query(query, conn)
            df['RUNTIME'] = now_str

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
    
    def fetch_vwap_source_data(
        self,
        source_schema: str,
        source_table: str,
        exchange: str,
    ) -> List[Dict[str, Any]]:
        q = text(f'''
            SELECT
                "EXCHANGE",
                "SYMBOL",
                "TIMESTAMP",
                "HIGH",
                "VOLUME"
            FROM {source_schema}."{source_table}"
            WHERE "EXCHANGE" = :exchange
            ORDER BY "SYMBOL" ASC, "TIMESTAMP" ASC
        ''')
        with self.engine.begin() as conn:
            rows = conn.execute(q, {"exchange": exchange}).fetchall()
        return [dict(r._mapping) for r in rows]

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
                "VWAP_PERIOD",
                "START_TIME",
                "END_TIME",
                "HIGHEST_VALUE",
                "HIGHEST_TIMESTAMP",
                "VWAP",
                "AVG_VOLUME_5D",
                "AVG_VOLUME_10D",
                "AVG_VOLUME_20D",
                "AVG_VOLUME_YESTERDAY",
                "AVG_VOLUME_LASTDAY",
                "AVG_VOL_STATUS",
                "CREATED_AT"
            )
            VALUES (
                :EXCHANGE,
                :SYMBOL,
                :VWAP_PERIOD,
                :START_TIME,
                :END_TIME,
                :HIGHEST_VALUE,
                :HIGHEST_TIMESTAMP,
                :VWAP,
                :AVG_VOLUME_5D,
                :AVG_VOLUME_10D,
                :AVG_VOLUME_20D,
                :AVG_VOLUME_YESTERDAY,
                :AVG_VOLUME_LASTDAY,
                :AVG_VOL_STATUS,
                :CREATED_AT
            )
        ''')
        with self.engine.begin() as conn:
            conn.execute(q, rows)
        return len(rows)
    

    ### IND bar status identification
    def get_bar_status_focus_symbols(self, exchange: str,focus_symbol_schema: str,focus_symbol_table: str) -> list[str]:
        q = text(f"""
            SELECT DISTINCT "SYMBOL"
            FROM {focus_symbol_schema}."{focus_symbol_table}"
            WHERE "EXCHANGE" = :exchange
            AND "OUT_OF_SCOPE" = False
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
    

    # RSI CALC
    def delete_ind_rsi_scope(self, exchange: str) -> int:
        q = text("""
            DELETE FROM silver."IND_RSI_FOCUS"
            WHERE "EXCHANGE" = :exchange;
        """)
        with self.engine.begin() as conn:
            res = conn.execute(q, {"exchange": exchange})
        return int(res.rowcount or 0)
    
    def insert_ind_rsi_focus_rows(self, rows: list[dict]) -> int:
        if not rows:
            return 0

        q = text("""
            INSERT INTO silver."IND_RSI_FOCUS" (
                "EXCHANGE","SYMBOL","END_DATE",
                "RSI","RSI_MA","RSI_STATUS","RSI_CROSS","RSI_CROSS_DAYS_AGO",
                "CREATED_AT"
            )
            VALUES (
                :EXCHANGE,:SYMBOL,:END_DATE,
                :RSI,:RSI_MA,:RSI_STATUS,:RSI_CROSS,:RSI_CROSS_DAYS_AGO,
                :CREATED_AT
            );
        """)

        with self.engine.begin() as conn:
            conn.execute(q, rows)

        return len(rows)
    

    # MFI CALC

    def delete_ind_mfi_scope(self, exchange: str) -> int:
        q = text("""
            DELETE FROM silver."IND_MFI_FOCUS"
            WHERE "EXCHANGE" = :exchange;
        """)
        with self.engine.begin() as conn:
            res = conn.execute(q, {"exchange": exchange})
        return int(res.rowcount or 0)
    
    def insert_ind_mfi_focus_rows(self, rows: list[dict]) -> int:
        if not rows:
            return 0

        q = text("""
            INSERT INTO silver."IND_MFI_FOCUS" (
                "EXCHANGE","SYMBOL","END_DATE",
                "MFI","MFI_YESTERDAY","MFI_12DAY_AVG","MFI_DIRECTION",
                "CREATED_AT"
            )
            VALUES (
                :EXCHANGE,:SYMBOL,:END_DATE,
                :MFI,:MFI_YESTERDAY,:MFI_12DAY_AVG,:MFI_DIRECTION,
                :CREATED_AT
            );
        """)

        normalized_rows = []
        for r in rows:
            rr = dict(r)
            rr.setdefault("MFI", None)
            rr.setdefault("MFI_YESTERDAY", None)
            rr.setdefault("MFI_12DAY_AVG", None)
            rr.setdefault("MFI_DIRECTION", None)
            normalized_rows.append(rr)

        with self.engine.begin() as conn:
            conn.execute(q, normalized_rows)

        return len(normalized_rows)
    
    def fetch_last_n_days_ohlcv_for_symbols(
        self,
        schema: str,
        table: str,
        exchange: str,
        symbols: list[str],
        n_days: int,
        ts_col: str = "TIMESTAMP",
        close_col: str = "CLOSE",
        volume_col: str = "VOLUME",
        high_col: str = "HIGH",
        low_col: str = "LOW",
    ) -> list[dict]:
        if not symbols:
            return []

        if not schema or not str(schema).strip():
            raise ValueError(f"fetch_last_n_days_ohlcv_for_symbols: schema is empty for table={table}")

        if isinstance(n_days, tuple):
            n_days = int(n_days[0])
        n_days = int(n_days)

        table_ref = f'{schema}."{table}"'

        q = text(f"""
            WITH ranked AS (
                SELECT
                    "EXCHANGE" AS exchange,
                    "SYMBOL" AS symbol,
                    "{ts_col}" AS ts,
                    "{close_col}"::double precision AS close,
                    "{volume_col}"::double precision AS volume,
                    "{high_col}"::double precision AS high,
                    "{low_col}"::double precision AS low,
                    ROW_NUMBER() OVER (
                        PARTITION BY "SYMBOL"
                        ORDER BY "{ts_col}" DESC
                    ) AS rn
                FROM {table_ref}
                WHERE "EXCHANGE" = :exchange
                AND "SYMBOL" IN :symbols
                AND "{ts_col}" IS NOT NULL
                AND "{close_col}" IS NOT NULL
                AND "{volume_col}" IS NOT NULL
            )
            SELECT exchange, symbol, ts, close, volume, high, low
            FROM ranked
            WHERE rn <= :n_days
            ORDER BY symbol ASC, ts ASC;
        """).bindparams(bindparam("symbols", expanding=True))

        with self.engine.begin() as conn:
            rows = conn.execute(
                q,
                {"exchange": exchange, "symbols": symbols, "n_days": n_days}
            ).fetchall()

        return [
            {
                "EXCHANGE": r[0],
                "SYMBOL": r[1],
                "TIMESTAMP": r[2],
                "CLOSE": r[3],
                "VOLUME": r[4],
                "HIGH": r[5],
                "LOW": r[6],
            }
            for r in rows
        ]
    

    ###########################################
    # MASTER COMBINED INDICATORS
    ###########################################

    def truncate_table(self, schema: str, table: str) -> None:
        q = text(f'TRUNCATE TABLE "{schema}"."{table}";')  
        with self.engine.begin() as conn:
            conn.execute(q)


    def build_master_combined_indicators(
        self,
        exchange: str,
        target_schema: str,
        target_table: str,
        log_schema: str,
        log_table: str,
        frvp_table: str,
        bs_table: str,
        end_dates_table: str,
        ema_table: str,
        rsi_table: str,
        mfi_table: str,
        vwap_table: str,
        pivot_table: str,
    ) -> Dict[str, int]:
        """
        Build master combined indicators table from silver indicator tables.
        - Target table is truncated before insert.
        - Log table is append-only.
        """
        exchange = exchange.upper().strip()

        # 1) truncate master target
        self.truncate_table(target_schema, target_table)

        insert_columns = [
            "EXCHANGE",
            "SYMBOL",

            "FRVP_INTERVAL",
            "FRVP_PERIOD_TYPE",
            "FRVP_HIGHEST_DATE",
            "FRVP_HIGHEST_VALUE",
            "FRVP_ROW_COUNT_AFTER_HIGHEST",
            "FRVP_DAY_COUNT_AFTER_HIGHEST",
            "FRVP_LATEST_CLOSE_VALUE",
            "FRVP_POC",
            "FRVP_VAL",
            "FRVP_VAH",

            "BS_OPEN_PRICE",
            "BS_CLOSE_PRICE",
            "BS_DIFFER",
            "BS_PERC",
            "BS_BAR_STATUS",

            "RAW_END_DATE",
            "BRONZE_END_DATE",
            "SILVER_END_DATE",
            "SILVER_CONVERTED_END_DATE",
            "EXPECTED_END_DATE",

            "EMA_TIMESTAMP",
            "EMA_END_DATE",
            "EMA3",
            "EMA5",
            "EMA14",
            "EMA20",
            "EMA_STATUS_5_20",
            "EMA_CROSS_5_20",
            "EMA_STATUS_3_20",
            "EMA_CROSS_3_20",
            "EMA_STATUS_3_14",
            "EMA_CROSS_3_14",
            "EMA_DAYS_SINCE_CROSS_5_20",
            "EMA_DAYS_SINCE_CROSS_3_20",
            "EMA_DAYS_SINCE_CROSS_3_14",

            "RSI",
            "RSI_MA",
            "RSI_STATUS",
            "RSI_CROSS",
            "RSI_CROSS_DAYS_AGO",

            "MFI",
            "MFI_YESTERDAY",
            "MFI_12DAY_AVG",
            "MFI_DIRECTION",

            "VWAP_HIGHEST_VALUE",
            "VWAP_HIGHEST_TIMESTAMP",
            "VWAP",
            "VOL_AVG_5DAY",
            "VOL_AVG_10DAY",
            "VOL_AVG_20DAY",
            "VOL_YESTERDAY",
            "VOL_LASTDAY",
            "VOL_STATUS",

            "PVT_START_DATE",
            "PVT_END_DATE",
            "PVT_YEAR",
            "PIVOT",
            "PVT_R1",
            "PVT_R2",
            "PVT_R3",
            "PVT_R4",
            "PVT_R5",
            "PVT_S1",
            "PVT_S2",
            "PVT_S3",
            "PVT_S4",
            "PVT_S5",
            "PVT_STATUS",

            "CREATED_AT",
            "CREATED_DAY",
        ]

        insert_columns_sql = ",\n                ".join(f'"{col}"' for col in insert_columns)

        select_sql = f"""
            SELECT
                frvp."EXCHANGE" AS "EXCHANGE",
                frvp."SYMBOL" AS "SYMBOL",

                frvp."INTERVAL" AS "FRVP_INTERVAL",
                frvp."FRVP_PERIOD_TYPE" AS "FRVP_PERIOD_TYPE",
                frvp."HIGHEST_DATE" AS "FRVP_HIGHEST_DATE",
                frvp."HIGHEST_VALUE" AS "FRVP_HIGHEST_VALUE",
                frvp."ROW_COUNT_AFTER_HIGHEST" AS "FRVP_ROW_COUNT_AFTER_HIGHEST",
                frvp."DAY_COUNT_AFTER_HIGHEST" AS "FRVP_DAY_COUNT_AFTER_HIGHEST",
                frvp."LATEST_CLOSE_VALUE" AS "FRVP_LATEST_CLOSE_VALUE",
                frvp."POC" AS "FRVP_POC",
                frvp."VAL" AS "FRVP_VAL",
                frvp."VAH" AS "FRVP_VAH",

                bs."OPEN_PRICE" AS "BS_OPEN_PRICE",
                bs."CLOSE_PRICE" AS "BS_CLOSE_PRICE",
                bs."DIFFER" AS "BS_DIFFER",
                bs."PERC" AS "BS_PERC",
                bs."BAR_STATUS" AS "BS_BAR_STATUS",

                ed."RAW_END_DATE" AS "RAW_END_DATE",
                ed."BRONZE_END_DATE" AS "BRONZE_END_DATE",
                ed."SILVER_END_DATE" AS "SILVER_END_DATE",
                ed."SILVER_CONVERTED_END_DATE" AS "SILVER_CONVERTED_END_DATE",
                ed."EXPECTED_END_DATE" AS "EXPECTED_END_DATE",

                ema."TIMESTAMP" AS "EMA_TIMESTAMP",
                ema."END_DATE" AS "EMA_END_DATE",
                ema."EMA3" AS "EMA3",
                ema."EMA5" AS "EMA5",
                ema."EMA14" AS "EMA14",
                ema."EMA20" AS "EMA20",
                ema."EMA_STATUS_5_20" AS "EMA_STATUS_5_20",
                ema."EMA_CROSS_5_20" AS "EMA_CROSS_5_20",
                ema."EMA_STATUS_3_20" AS "EMA_STATUS_3_20",
                ema."EMA_CROSS_3_20" AS "EMA_CROSS_3_20",
                ema."EMA_STATUS_3_14" AS "EMA_STATUS_3_14",
                ema."EMA_CROSS_3_14" AS "EMA_CROSS_3_14",
                ema."DAYS_SINCE_CROSS_5_20" AS "EMA_DAYS_SINCE_CROSS_5_20",
                ema."DAYS_SINCE_CROSS_3_20" AS "EMA_DAYS_SINCE_CROSS_3_20",
                ema."DAYS_SINCE_CROSS_3_14" AS "EMA_DAYS_SINCE_CROSS_3_14",

                rsi."RSI" AS "RSI",
                rsi."RSI_MA" AS "RSI_MA",
                rsi."RSI_STATUS" AS "RSI_STATUS",
                rsi."RSI_CROSS" AS "RSI_CROSS",
                rsi."RSI_CROSS_DAYS_AGO" AS "RSI_CROSS_DAYS_AGO",

                mfi."MFI" AS "MFI",
                mfi."MFI_YESTERDAY" AS "MFI_YESTERDAY",
                mfi."MFI_12DAY_AVG" AS "MFI_12DAY_AVG",
                mfi."MFI_DIRECTION" AS "MFI_DIRECTION",

                vwap."HIGHEST_VALUE" AS "VWAP_HIGHEST_VALUE",
                vwap."HIGHEST_TIMESTAMP" AS "VWAP_HIGHEST_TIMESTAMP",
                vwap."VWAP" AS "VWAP",
                vwap."AVG_VOLUME_5D" AS "VOL_AVG_5DAY",
                vwap."AVG_VOLUME_10D" AS "VOL_AVG_10DAY",
                vwap."AVG_VOLUME_20D" AS "VOL_AVG_20DAY",
                vwap."AVG_VOLUME_YESTERDAY" AS "VOL_YESTERDAY",
                vwap."AVG_VOLUME_LASTDAY" AS "VOL_LASTDAY",
                vwap."AVG_VOL_STATUS" AS "VOL_STATUS",

                pvt."PVT_START_DATE" AS "PVT_START_DATE",
                pvt."PVT_END_DATE" AS "PVT_END_DATE",
                pvt."PVT_YEAR" AS "PVT_YEAR",
                pvt."PIVOT" AS "PIVOT",
                pvt."PVT_R1" AS "PVT_R1",
                pvt."PVT_R2" AS "PVT_R2",
                pvt."PVT_R3" AS "PVT_R3",
                pvt."PVT_R4" AS "PVT_R4",
                pvt."PVT_R5" AS "PVT_R5",
                pvt."PVT_S1" AS "PVT_S1",
                pvt."PVT_S2" AS "PVT_S2",
                pvt."PVT_S3" AS "PVT_S3",
                pvt."PVT_S4" AS "PVT_S4",
                pvt."PVT_S5" AS "PVT_S5",
                pvt."STATUS" AS "PVT_STATUS",

                CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Amsterdam' AS "CREATED_AT",
                TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Amsterdam', 'DD-MM-YYYY') AS "CREATED_DAY"

            FROM silver."{frvp_table}" frvp

            LEFT JOIN silver."{bs_table}" bs
                ON frvp."EXCHANGE" = bs."EXCHANGE"
            AND frvp."SYMBOL" = bs."SYMBOL"

            LEFT JOIN silver."{end_dates_table}" ed
                ON frvp."EXCHANGE" = ed."EXCHANGE"
            AND frvp."SYMBOL" = ed."SYMBOL"

            LEFT JOIN silver."{ema_table}" ema
                ON frvp."EXCHANGE" = ema."EXCHANGE"
            AND frvp."SYMBOL" = ema."SYMBOL"

            LEFT JOIN silver."{rsi_table}" rsi
                ON frvp."EXCHANGE" = rsi."EXCHANGE"
            AND frvp."SYMBOL" = rsi."SYMBOL"

            LEFT JOIN silver."{mfi_table}" mfi
                ON frvp."EXCHANGE" = mfi."EXCHANGE"
            AND frvp."SYMBOL" = mfi."SYMBOL"

            LEFT JOIN silver."{vwap_table}" vwap
                ON frvp."EXCHANGE" = vwap."EXCHANGE"
            AND frvp."SYMBOL" = vwap."SYMBOL"
            AND frvp."FRVP_PERIOD_TYPE" = vwap."VWAP_PERIOD"

            LEFT JOIN silver."{pivot_table}" pvt
                ON frvp."EXCHANGE" = pvt."EXCHANGE"
            AND frvp."SYMBOL" = pvt."SYMBOL"

            WHERE frvp."EXCHANGE" = :exchange
        """

        insert_master_sql = f"""
            INSERT INTO {target_schema}."{target_table}" (
                    {insert_columns_sql}
            )
            {select_sql}
        """

        insert_log_sql = f"""
            INSERT INTO {log_schema}."{log_table}" (
                    {insert_columns_sql}
            )
            {select_sql}
        """

        count_sql = text(f'SELECT COUNT(*) FROM {target_schema}."{target_table}";')

        with self.engine.begin() as conn:
            conn.execute(text(insert_master_sql), {"exchange": exchange})
            master_count = conn.execute(count_sql).scalar() or 0
            conn.execute(text(insert_log_sql), {"exchange": exchange})

        return {
            "master_inserted_rows": int(master_count),
        }
    
    #########################################################
    # Data Quality Checks                                   #
    #########################################################

    def get_next_dq_run_id(self) -> int:
        q = text("SELECT nextval('logs.dq_run_id_seq');")
        with self.engine.begin() as conn:
            return int(conn.execute(q).scalar_one())


    def archive_dq_rows(self, active_table: str) -> int:
        """
        Appends all rows from logs.<active_table> into logs.log_dq_check.
        """
        q = text(f'''
            INSERT INTO logs."log_dq_check"
            SELECT * FROM logs."{active_table}";
        ''')
        with self.engine.begin() as conn:
            res = conn.execute(q)
            return int(res.rowcount or 0)

    def count_distinct_failed_symbols(self, active_table: str) -> int:
        q = text(f'''
            SELECT COUNT(DISTINCT "SYMBOL")
            FROM logs."{active_table}"
            WHERE "STATUS" = 'FAILED';
        ''')
        with self.engine.begin() as conn:
            return int(conn.execute(q).scalar_one() or 0)
        
    def get_dq_failed_symbols(self, exchange:str, table:str):
        q = text(f'''
            SELECT "SYMBOL"
            FROM logs."{table}"
            WHERE "EXCHANGE" = {exchange};
        ''')
        with self.engine.begin() as conn:
            return int(conn.execute(q).scalar_one() or 0)
        
    ########################################
    # NEW SYSTEM 6 EXCHANGE
    ########################################


    def get_in_scope_symbols_from_table(
        self,
        schema: str,
        table: str,
        exchange: str,
        symbol_col: str = "SYMBOL",
        exchange_col: str = "EXCHANGE",
        in_scope_col: str = "IN_SCOPE",
    ) -> list[str]:
        """
        Reads symbol list from a generic scope table.
        """

        q = text(f'''
            SELECT "{symbol_col}"
            FROM "{schema}"."{table}"
            WHERE "{exchange_col}" = :exchange
            AND "{in_scope_col}" = TRUE
            ORDER BY "{symbol_col}";
        ''')

        with self.engine.begin() as conn:
            rows = conn.execute(q, {"exchange": exchange}).fetchall()

        return [r[0] for r in rows]
    
    def update_focus_symbol_scope(
        self,
        exchange: str,
        compare_schema: str,
        compare_table: str,
        reason: str,
        main_symbol_schema: str,
        main_symbol_table: str,
        drop_and_recreate: bool = False,
    ) -> dict:
        """
        Manages OUT_OF_SCOPE and OOS_REASON flags on the cloned symbol list table.

        drop_and_recreate=True  → Drops entire table and recreates it from scratch 
                                (use only on first run of the day to reset)
        
        drop_and_recreate=False → NEVER drops table. Only updates OUT_OF_SCOPE and OOS_REASON columns
                                (safe to run multiple times per day)

        Elimination logic:
        - Symbols missing from compare table are marked OUT_OF_SCOPE = True with the given reason
        - Symbols already marked True are never overwritten (first elimination reason wins)
        - Symbol list never shrinks — only flags are updated
        """

        src_fqn = f'{main_symbol_schema}."{main_symbol_table}"'
        tgt_fqn = 'silver."cloned_focus_symbol_list"'
        cmp_fqn = f'{compare_schema}."{compare_table}"'

        with self.engine.begin() as conn:
            
            # ═══════════════════════════════════════════════════════════════════════════════
            # STEP 1: Table initialization (DROP and recreate OR create if missing)
            # ═══════════════════════════════════════════════════════════════════════════════
            
            if drop_and_recreate:
                # 🔴 DROP PATH: Completely remove and recreate table
                # Use case: First run of the day → reset all flags to FALSE
                
                q_drop = text(f'DROP TABLE IF EXISTS {tgt_fqn}')
                conn.execute(q_drop)
                
                q_create_fresh = text(f"""
                    CREATE TABLE {tgt_fqn} AS
                    SELECT
                        s.*,
                        FALSE::boolean AS "OUT_OF_SCOPE",
                        NULL::text     AS "OOS_REASON"
                    FROM {src_fqn} s
                """)
                conn.execute(q_create_fresh)
                
            else:
                # 🟢 SAFE PATH: Only create if table doesn't exist
                # Never drops or modifies existing table
                # Use case: Intraday runs → only update flags, never touch structure
                
                q_create_if_not_exists = text(f"""
                    CREATE TABLE IF NOT EXISTS {tgt_fqn} AS
                    SELECT
                        s.*,
                        FALSE::boolean AS "OUT_OF_SCOPE",
                        NULL::text     AS "OOS_REASON"
                    FROM {src_fqn} s
                """)
                conn.execute(q_create_if_not_exists)

            # ═══════════════════════════════════════════════════════════════════════════════
            # STEP 2: Update flag columns only (never touches table structure or other rows)
            # ═══════════════════════════════════════════════════════════════════════════════
            
            q_update = text(f"""
                UPDATE {tgt_fqn} t
                SET
                    "OUT_OF_SCOPE" = TRUE,
                    "OOS_REASON"   = :reason
                WHERE t."EXCHANGE" = :exchange
                AND t."OUT_OF_SCOPE" = FALSE
                AND NOT EXISTS (
                    SELECT 1
                    FROM {cmp_fqn} c
                    WHERE c."SYMBOL" = t."SYMBOL"
                )
            """)
            conn.execute(q_update, {"exchange": exchange, "reason": reason})

            # ═══════════════════════════════════════════════════════════════════════════════
            # STEP 3: Count results and gather statistics
            # ═══════════════════════════════════════════════════════════════════════════════
            
            q_counts = text(f"""
                SELECT
                    COUNT(*)                                                                    AS total,
                    COUNT(*) FILTER (WHERE "OUT_OF_SCOPE" = TRUE)                              AS eliminated,
                    COUNT(*) FILTER (WHERE "OUT_OF_SCOPE" = FALSE)                             AS remaining,
                    ARRAY_AGG("SYMBOL") FILTER (WHERE "OUT_OF_SCOPE" = TRUE AND "EXCHANGE" = :exchange) 
                        AS eliminated_symbols
                FROM {tgt_fqn}
                WHERE "EXCHANGE" = :exchange
            """)
            counts = conn.execute(q_counts, {"exchange": exchange}).mappings().one()

        # ═══════════════════════════════════════════════════════════════════════════════
        # Extract counts
        # ═══════════════════════════════════════════════════════════════════════════════
        
        total = int(counts["total"])
        eliminated = int(counts["eliminated"])
        remaining = int(counts["remaining"])
        eliminated_syms = counts["eliminated_symbols"] or []

        # ═══════════════════════════════════════════════════════════════════════════════
        # Log results
        # ═══════════════════════════════════════════════════════════════════════════════
        
        mode = "DROP & RECREATE" if drop_and_recreate else "UPDATE ONLY"
        
        print(
            f"[{exchange}] Symbol scope update | "
            f"Mode: {mode} | "
            f"Total: {total} | "
            f"Eliminated: {eliminated} | "
            f"Remaining: {remaining} | "
            f"Reason: '{reason}'"
        )

        return {
            "exchange": exchange,
            "source": f"{main_symbol_schema}.{main_symbol_table}",
            "target": "silver.cloned_focus_symbol_list",
            "compare": f"{compare_schema}.{compare_table}",
            "drop_and_recreate": drop_and_recreate,
            "mode": mode,
            "total": total,
            "eliminated": eliminated,
            "remaining": remaining,
            "eliminated_symbols": eliminated_syms,
            "reason": reason,
        }
    
    def update_focus_symbol_scope_filtered(
        self,
        exchange: str,
        compare_schema: str,
        compare_table: str,
        comp_col: str,
        comp_value: str,
        reason: str,
        main_symbol_schema: str,
        main_symbol_table: str,
        drop_and_recreate: bool = False,
    ) -> dict:
        """
        Manages OUT_OF_SCOPE and OOS_REASON flags on the cloned symbol list table
        with an ADDITIONAL FILTER on the comparison table.

        Parameters:
        -----------
        exchange : str
            Exchange code (e.g., 'BINANCE', 'EURONEXT')
        
        compare_schema : str
            Schema of comparison table (e.g., 'raw', 'silver')
        
        compare_table : str
            Table name to compare against
        
        comp_col : str
            Column name in comparison table to filter on (e.g., 'BAR_STATUS', 'DATA_QUALITY')
        
        comp_value : str
            Value to EXCLUDE/MARK AS BAD (e.g., 'RED' - symboller RED ise OUT_OF_SCOPE=TRUE)
        
        reason : str
            Elimination reason to store/append to OOS_REASON column
        
        main_symbol_schema : str
            Schema of main symbol list source
        
        main_symbol_table : str
            Table name of main symbol list source
        
        drop_and_recreate : bool
            If True: Drop entire table and recreate from scratch (first run of day)
            If False: Only update flags, never touch table structure (intraday runs)

        Logic:
        ------
        🔴 MARK BAD VALUES:
        1. Looks for symbols in compare_table WHERE comp_col = comp_value
        2. Symbols FOUND with this value are marked OUT_OF_SCOPE = True
        3. OOS_REASON is SET or APPENDED (if already has reason, append with ||)
        4. IN_SCOPE_FOR_EMA_RSI is NEVER touched - completely independent
        5. Already eliminated symbols can get multiple reasons appended
        6. Symbol list never shrinks — only flags are updated
        
        Example:
        --------
        # RED bar status olan symboller elenir (OUT_OF_SCOPE=TRUE)
        update_focus_symbol_scope_filtered(
            exchange='BINANCE',
            compare_schema='silver',
            compare_table='IND_BAR_STATUS',
            comp_col='BAR_STATUS',
            comp_value='RED',
            reason='bar status is red',
            main_symbol_schema='gold',
            main_symbol_table='symbol_master',
            drop_and_recreate=False
        )
        
        Çalıştıktan sonra:
        - RED olan symboller OUT_OF_SCOPE=TRUE, OOS_REASON="bar status is red"
        
        Eğer aynı sembol başka nedenle elenmek istenirse:
        - OOS_REASON="bar status is red || failed data quality check"
        """

        src_fqn = f'{main_symbol_schema}."{main_symbol_table}"'
        tgt_fqn = 'silver."cloned_focus_symbol_list"'
        cmp_fqn = f'{compare_schema}."{compare_table}"'

        with self.engine.begin() as conn:
            
            # ═══════════════════════════════════════════════════════════════════════════════
            # STEP 1: Table initialization (DROP and recreate OR create if missing)
            # ═══════════════════════════════════════════════════════════════════════════════
            
            if drop_and_recreate:
                # 🔴 DROP PATH: Completely remove and recreate table
                # Use case: First run of the day → reset all flags to FALSE
                
                q_drop = text(f'DROP TABLE IF EXISTS {tgt_fqn}')
                conn.execute(q_drop)
                
                q_create_fresh = text(f"""
                    CREATE TABLE {tgt_fqn} AS
                    SELECT
                        s.*,
                        FALSE::boolean AS "OUT_OF_SCOPE",
                        NULL::text     AS "OOS_REASON"
                    FROM {src_fqn} s
                """)
                conn.execute(q_create_fresh)
                
            else:
                # 🟢 SAFE PATH: Only create if table doesn't exist
                # Never drops or modifies existing table
                # Use case: Intraday runs → only update flags, never touch structure
                
                q_create_if_not_exists = text(f"""
                    CREATE TABLE IF NOT EXISTS {tgt_fqn} AS
                    SELECT
                        s.*,
                        FALSE::boolean AS "OUT_OF_SCOPE",
                        NULL::text     AS "OOS_REASON"
                    FROM {src_fqn} s
                """)
                conn.execute(q_create_if_not_exists)

            # ═══════════════════════════════════════════════════════════════════════════════
            # STEP 2: Update OUT_OF_SCOPE flag - Mark symbols with bad value
            # 
            # 🔧 LOGIC:
            # If symbol EXISTS in compare_table WITH comp_value → Mark OUT_OF_SCOPE=TRUE
            # OOS_REASON: If NULL/empty → SET reason, ELSE APPEND with ||
            # IN_SCOPE_FOR_EMA_RSI: NEVER TOUCHED (completely independent)
            # ═══════════════════════════════════════════════════════════════════════════════
            
            q_update = text(f"""
                UPDATE {tgt_fqn} t
                SET
                    "OUT_OF_SCOPE" = TRUE,
                    "OOS_REASON" = CASE 
                        WHEN t."OOS_REASON" IS NULL OR t."OOS_REASON" = ''
                        THEN :reason
                        ELSE t."OOS_REASON" || ' || ' || :reason
                    END
                WHERE t."EXCHANGE" = :exchange
                AND t."OUT_OF_SCOPE" = FALSE
                AND EXISTS (
                    SELECT 1
                    FROM {cmp_fqn} c
                    WHERE c."SYMBOL" = t."SYMBOL"
                    AND c."{comp_col}" != :comp_value
                )
            """)
            conn.execute(q_update, {
                "exchange": exchange, 
                "reason": reason,
                "comp_value": comp_value
            })

            # ═══════════════════════════════════════════════════════════════════════════════
            # STEP 3: Count results and gather statistics
            # ═══════════════════════════════════════════════════════════════════════════════
            
            q_counts = text(f"""
                SELECT
                    COUNT(*)                                                                    AS total,
                    COUNT(*) FILTER (WHERE "OUT_OF_SCOPE" = TRUE)                              AS eliminated,
                    COUNT(*) FILTER (WHERE "OUT_OF_SCOPE" = FALSE)                             AS remaining,
                    ARRAY_AGG("SYMBOL") FILTER (WHERE "OUT_OF_SCOPE" = TRUE AND "EXCHANGE" = :exchange) 
                        AS eliminated_symbols
                FROM {tgt_fqn}
                WHERE "EXCHANGE" = :exchange
            """)
            counts = conn.execute(q_counts, {"exchange": exchange}).mappings().one()

        # ═══════════════════════════════════════════════════════════════════════════════
        # Extract counts
        # ═══════════════════════════════════════════════════════════════════════════════
        
        total = int(counts["total"])
        eliminated = int(counts["eliminated"])
        remaining = int(counts["remaining"])
        eliminated_syms = counts["eliminated_symbols"] or []

        # ═══════════════════════════════════════════════════════════════════════════════
        # Log results
        # ═══════════════════════════════════════════════════════════════════════════════
        
        mode = "DROP & RECREATE" if drop_and_recreate else "UPDATE ONLY"
        
        print(
            f"[{exchange}] Symbol scope update (FILTERED) | "
            f"Mode: {mode} | "
            f"Filter: {comp_col}='{comp_value}' | "
            f"Total: {total} | "
            f"Eliminated: {eliminated} | "
            f"Remaining: {remaining} | "
            f"Reason: '{reason}'"
        )

        return {
            "exchange": exchange,
            "source": f"{main_symbol_schema}.{main_symbol_table}",
            "target": "silver.cloned_focus_symbol_list",
            "compare": f"{compare_schema}.{compare_table}",
            "compare_filter": {
                "column": comp_col,
                "value": comp_value,
                "meaning": "MARKS SYMBOLS WITH THIS VALUE AS OUT_OF_SCOPE (BAD)"
            },
            "drop_and_recreate": drop_and_recreate,
            "mode": mode,
            "total": total,
            "eliminated": eliminated,
            "remaining": remaining,
            "eliminated_symbols": eliminated_syms,
            "reason": reason,
            "note": "OOS_REASON values are APPENDED with ' || ' separator for multiple elimination reasons",
        }
    
    # ==============================
    # IND PIVOT CALC
    # ==============================

    def delete_indicator_scope_by_exchange(
        self,
        schema: str,
        table: str,
        exchange: str,
    ) -> int:
        q = text(f'''
            DELETE FROM {schema}."{table}"
            WHERE "EXCHANGE" = :exchange;
        ''')
        with self.engine.begin() as conn:
            res = conn.execute(q, {"exchange": exchange})
        return int(res.rowcount or 0)
    
    def fetch_last_n_days_ohlc_for_symbols(
        self,
        schema: str,
        table: str,
        exchange: str,
        symbols: list[str],
        n_days: int,
        ts_col: str = "TIMESTAMP",
        high_col: str = "HIGH",
        low_col: str = "LOW",
        close_col: str = "CLOSE",
    ) -> list[dict]:
        if not symbols:
            return []

        if isinstance(n_days, tuple):
            n_days = int(n_days[0])
        n_days = int(n_days)

        q = text(f"""
            WITH ranked AS (
                SELECT
                    "EXCHANGE" AS exchange,
                    "SYMBOL" AS symbol,
                    "{ts_col}" AS ts,
                    "{high_col}"::double precision AS high,
                    "{low_col}"::double precision AS low,
                    "{close_col}"::double precision AS close,
                    ROW_NUMBER() OVER (
                        PARTITION BY "SYMBOL"
                        ORDER BY "{ts_col}" DESC
                    ) AS rn
                FROM {schema}."{table}"
                WHERE "EXCHANGE" = :exchange
                AND "SYMBOL" IN :symbols
                AND "{ts_col}" IS NOT NULL
                AND "{high_col}" IS NOT NULL
                AND "{low_col}" IS NOT NULL
                AND "{close_col}" IS NOT NULL
            )
            SELECT exchange, symbol, ts, high, low, close
            FROM ranked
            WHERE rn <= :n_days
            ORDER BY symbol ASC, ts ASC;
        """).bindparams(bindparam("symbols", expanding=True))

        with self.engine.begin() as conn:
            rows = conn.execute(q, {
                "exchange": exchange,
                "symbols": symbols,
                "n_days": n_days,
            }).fetchall()

        return [
            {
                "EXCHANGE": r[0],
                "SYMBOL": r[1],
                "TIMESTAMP": r[2],
                "HIGH": r[3],
                "LOW": r[4],
                "CLOSE": r[5],
            }
            for r in rows
        ]
    
    def insert_ind_pivot_focus_rows(
        self,
        schema: str,
        table: str,
        rows: list[dict],
    ) -> int:
        if not rows:
            return 0

        q = text(f"""
            INSERT INTO {schema}."{table}" (
                "EXCHANGE","SYMBOL",
                "PVT_START_DATE","PVT_END_DATE","PVT_YEAR",
                "PIVOT",
                "PVT_R1","PVT_R2","PVT_R3","PVT_R4","PVT_R5",
                "PVT_S1","PVT_S2","PVT_S3","PVT_S4","PVT_S5",
                "STATUS","CREATED_AT"
            )
            VALUES (
                :EXCHANGE,:SYMBOL,
                :PVT_START_DATE,:PVT_END_DATE,:PVT_YEAR,
                :PIVOT,
                :PVT_R1,:PVT_R2,:PVT_R3,:PVT_R4,:PVT_R5,
                :PVT_S1,:PVT_S2,:PVT_S3,:PVT_S4,:PVT_S5,
                :STATUS,:CREATED_AT
            );
        """)

        normalized_rows = []
        for r in rows:
            rr = dict(r)
            rr.setdefault("PIVOT", None)
            rr.setdefault("PVT_R1", None)
            rr.setdefault("PVT_R2", None)
            rr.setdefault("PVT_R3", None)
            rr.setdefault("PVT_R4", None)
            rr.setdefault("PVT_R5", None)
            rr.setdefault("PVT_S1", None)
            rr.setdefault("PVT_S2", None)
            rr.setdefault("PVT_S3", None)
            rr.setdefault("PVT_S4", None)
            rr.setdefault("PVT_S5", None)
            rr.setdefault("STATUS", "FAILED")
            normalized_rows.append(rr)

        with self.engine.begin() as conn:
            conn.execute(q, normalized_rows)

        return len(normalized_rows)
    
    # ==============================
    # IND CALC END DATES FOR SOURCES
    # ==============================
    def delete_ind_end_dates_scope(self, exchange: str) -> int:
        q = text("""
            DELETE FROM silver."IND_END_DATES"
            WHERE "EXCHANGE" = :exchange;
        """)
        with self.engine.begin() as conn:
            res = conn.execute(q, {"exchange": exchange})
        return int(res.rowcount or 0)
    
    def fetch_symbol_end_dates(
        self,
        schema: str,
        table: str,
        symbols: List[str],
        ts_col: str,
    ) -> Dict[str, Optional[datetime]]:
        if not symbols:
            return {}

        q = text(f"""
            SELECT
                "SYMBOL" AS symbol,
                MAX("{ts_col}") AS end_ts
            FROM {schema}."{table}"
            WHERE "SYMBOL" IN :symbols
            GROUP BY "SYMBOL"
        """).bindparams(bindparam("symbols", expanding=True))

        with self.engine.begin() as conn:
            rows = conn.execute(q, {"symbols": symbols}).fetchall()

        result = {s: None for s in symbols}
        for r in rows:
            result[str(r[0])] = r[1]
        return result
    
    def insert_ind_end_dates_rows(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0

        q = text("""
            INSERT INTO silver."IND_END_DATES" (
                "EXCHANGE",
                "SYMBOL",
                "RAW_END_DATE",
                "BRONZE_END_DATE",
                "SILVER_END_DATE",
                "SILVER_CONVERTED_END_DATE",
                "EXPECTED_END_DATE",
                "CREATED_AT"
            )
            VALUES (
                :EXCHANGE,
                :SYMBOL,
                :RAW_END_DATE,
                :BRONZE_END_DATE,
                :SILVER_END_DATE,
                :SILVER_CONVERTED_END_DATE,
                :EXPECTED_END_DATE,
                :CREATED_AT
            );
        """)

        with self.engine.begin() as conn:
            conn.execute(q, rows)

        return len(rows)
    
    # ====================================================
    # CALC MASTER SCORE
    # ====================================================


    def get_table_as_dataframe(
        self,
        schema_name: str,
        table_name: str,
        exchange: str | None = None,
    ) -> pd.DataFrame:
        sql = f'SELECT * FROM {schema_name}."{table_name}"'
        params = {}

        if exchange is not None:
            sql += ' WHERE "EXCHANGE" = :exchange'
            params["exchange"] = exchange

        with self.engine.begin() as conn:
            return pd.read_sql(text(sql), conn, params=params)


    def truncate_table(
        self,
        schema_name: str,
        table_name: str,
    ) -> None:
        from sqlalchemy import text

        sql = text(f'TRUNCATE TABLE {schema_name}."{table_name}"')
        with self.engine.begin() as conn:
            conn.execute(sql)


    def insert_dataframe(
        self,
        df: pd.DataFrame,
        schema_name: str,
        table_name: str,
    ) -> None:
        if df.empty:
            return

        with self.engine.begin() as conn:
            df.to_sql(
                name=table_name,
                schema=schema_name,
                con=conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

    # ===========================================
    # RT WATCH
    # ===========================================

    def get_symbols_from_table(
        self,
        source_schema: str,
        source_table: str,
        exchange: str,
        symbol_col: str = "SYMBOL",
        exchange_col: str = "EXCHANGE",
        where_sql: str | None = None,
    ) -> List[str]:
        query = f'''
            SELECT DISTINCT "{symbol_col}"
            FROM {source_schema}."{source_table}"
            WHERE "{exchange_col}" = :exchange
        '''
        if where_sql:
            query += f"\n  AND ({where_sql})"

        query += f'\nORDER BY "{symbol_col}";'

        with self.engine.begin() as conn:
            rows = conn.execute(text(query), {"exchange": exchange}).fetchall()

        return [r[0] for r in rows if r[0]]

    def truncate_table(
        self,
        schema_name: str,
        table_name: str,
    ) -> None:
        q = text(f'TRUNCATE TABLE {schema_name}."{table_name}";')
        with self.engine.begin() as conn:
            conn.execute(q)

    def bulk_insert_watch_dataset(
        self,
        rows: List[Dict[str, Any]],
        target_schema: str,
        target_table: str,
    ) -> int:
        if not rows:
            return 0

        q = text(f'''
        INSERT INTO {target_schema}."{target_table}" (
            "EXCHANGE",
            "SYMBOL",
            "TIMESTAMP",
            "OPEN",
            "HIGH",
            "LOW",
            "CLOSE",
            "VOLUME",
            "ROW_ID",
            "SOURCE",
            "CREATED_AT"
        )
        VALUES (
            :EXCHANGE,
            :SYMBOL,
            :TIMESTAMP,
            :OPEN,
            :HIGH,
            :LOW,
            :CLOSE,
            :VOLUME,
            :ROW_ID,
            :SOURCE,
            :CREATED_AT
        )
        ON CONFLICT ("ROW_ID") DO NOTHING;
    ''')

        with self.engine.begin() as conn:
            conn.execute(q, rows)

        return len(rows)
    

    # ================================
    # calc buy signal
    # ================================

    def build_watch_signal_check_rows(
        self,
        exchange: str,
        input_schema: str,
        input_table: str,
        open_hour: int,
        open_minute: int,
        close_hour: int,
        close_minute: int,
    ) -> List[Dict[str, Any]]:
        q = text(f"""
            WITH last_day AS (
                SELECT MAX(("TIMESTAMP")::date) AS dt
                FROM {input_schema}."{input_table}"
                WHERE "EXCHANGE" = :exchange
            ),
            day_data AS (
                SELECT *
                FROM {input_schema}."{input_table}", last_day
                WHERE "EXCHANGE" = :exchange
                AND ("TIMESTAMP")::date = last_day.dt
            ),
            symbols AS (
                SELECT DISTINCT
                    d."EXCHANGE",
                    d."SYMBOL"
                FROM day_data d
            ),
            merged AS (
                SELECT
                    s."EXCHANGE" AS exchange,
                    s."SYMBOL" AS symbol,
                    ld.dt AS dt,

                    o.open_timestamp,
                    o.open_price,

                    c.close_timestamp,
                    c.close_price,

                    v.daily_volume

                FROM symbols s
                JOIN last_day ld ON TRUE

                -- OPEN: exact varsa kendisi, yoksa requested time'dan sonraki ilk row
                LEFT JOIN LATERAL (
                    SELECT
                        d."TIMESTAMP" AS open_timestamp,
                        d."OPEN"::double precision AS open_price
                    FROM day_data d
                    WHERE d."EXCHANGE" = s."EXCHANGE"
                    AND d."SYMBOL" = s."SYMBOL"
                    AND d."TIMESTAMP"::time >= make_time(:open_hour, :open_minute, 0)
                    ORDER BY d."TIMESTAMP" ASC
                    LIMIT 1
                ) o ON TRUE

                -- CLOSE: exact varsa kendisi, yoksa requested time'dan önceki son row
                LEFT JOIN LATERAL (
                    SELECT
                        d."TIMESTAMP" AS close_timestamp,
                        d."CLOSE"::double precision AS close_price
                    FROM day_data d
                    WHERE d."EXCHANGE" = s."EXCHANGE"
                    AND d."SYMBOL" = s."SYMBOL"
                    AND d."TIMESTAMP"::time <= make_time(:close_hour, :close_minute, 0)
                    ORDER BY d."TIMESTAMP" DESC
                    LIMIT 1
                ) c ON TRUE

                LEFT JOIN LATERAL (
                    SELECT
                        COALESCE(SUM(d."VOLUME"::double precision), 0) AS daily_volume
                    FROM day_data d
                    WHERE d."EXCHANGE" = s."EXCHANGE"
                    AND d."SYMBOL" = s."SYMBOL"
                    AND d."TIMESTAMP"::time >= make_time(:open_hour, :open_minute, 0)
                    AND d."TIMESTAMP"::time <= make_time(:close_hour, :close_minute, 0)
                ) v ON TRUE

                WHERE o.open_timestamp IS NOT NULL
                AND c.close_timestamp IS NOT NULL
            )
            SELECT
                exchange,
                symbol,
                TO_CHAR(dt, 'DD-MM-YYYY') AS date_str,
                open_timestamp,
                close_timestamp,
                LPAD(CAST(:open_hour AS text), 2, '0') || ':' || LPAD(CAST(:open_minute AS text), 2, '0') AS requested_open_time,
                LPAD(CAST(:close_hour AS text), 2, '0') || ':' || LPAD(CAST(:close_minute AS text), 2, '0') AS requested_close_time,
                open_price,
                close_price,
                (close_price - open_price) AS diff_o_c,
                daily_volume,
                CASE
                    WHEN (close_price - open_price) > 0 THEN 'BUY'
                    WHEN (close_price - open_price) < 0 THEN 'AVOID'
                    ELSE 'WATCH'
                END AS status,
                CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Amsterdam' AS created_at
            FROM merged
            ORDER BY symbol;
        """)

        with self.engine.begin() as conn:
            rows = conn.execute(
                q,
                {
                    "exchange": exchange,
                    "open_hour": int(open_hour),
                    "open_minute": int(open_minute),
                    "close_hour": int(close_hour),
                    "close_minute": int(close_minute),
                },
            ).fetchall()

        return [
        {
            "EXCHANGE": r[0],
            "SYMBOL": r[1],
            "DATE": r[2],
            "OPEN_TIMESTAMP": r[3],
            "CLOSE_TIMESTAMP": r[4],
            "REQUESTED_OPEN_TIME": r[5],
            "REQUESTED_CLOSE_TIME": r[6],
            "OPEN": float(r[7]) if r[7] is not None else None,
            "CLOSE": float(r[8]) if r[8] is not None else None,
            "DIFF_O_C": float(r[9]) if r[9] is not None else None,
            "DAILY_VOLUME": float(r[10]) if r[10] is not None else None,
            "STATUS": r[11],

            # 🔴 YENİLER → ŞU AN NULL
            "REALISED_CLOSE_TIMESTAMP": None,
            "REALISED_CLOSE_PRICE": None,
            "REALISED_PROFIT": None,

            "CREATED_AT": r[12],
        }
        for r in rows
    ]
    
    def insert_watch_signal_check_rows(
        self,
        schema: str,
        table: str,
        rows: List[Dict[str, Any]],
    ) -> int:
        if not rows:
            return 0

        q = text(f"""
        INSERT INTO {schema}."{table}" (
            "EXCHANGE",
            "SYMBOL",
            "DATE",
            "OPEN_TIMESTAMP",
            "CLOSE_TIMESTAMP",
            "REQUESTED_OPEN_TIME",
            "REQUESTED_CLOSE_TIME",
            "OPEN",
            "CLOSE",
            "DIFF_O_C",
            "DAILY_VOLUME",
            "STATUS",

            "REALISED_CLOSE_TIMESTAMP",
            "REALISED_CLOSE_PRICE",
            "REALISED_PROFIT",

            "CREATED_AT"
        )
        VALUES (
            :EXCHANGE,
            :SYMBOL,
            :DATE,
            :OPEN_TIMESTAMP,
            :CLOSE_TIMESTAMP,
            :REQUESTED_OPEN_TIME,
            :REQUESTED_CLOSE_TIME,
            :OPEN,
            :CLOSE,
            :DIFF_O_C,
            :DAILY_VOLUME,
            :STATUS,

            :REALISED_CLOSE_TIMESTAMP,
            :REALISED_CLOSE_PRICE,
            :REALISED_PROFIT,

            :CREATED_AT
        );
    """)

        with self.engine.begin() as conn:
            conn.execute(q, rows)
        return len(rows)
    

    # update realised profic
    def update_realised_close_fields_from_hourly_source(
        self,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        exchange: str | None = None,
        overwrite: bool = False,
    ) -> int:
        from sqlalchemy import text

        where_clauses = ["1=1"]
        params = {}

        if not overwrite:
            where_clauses.extend([
                't."REALISED_CLOSE_TIMESTAMP" IS NULL',
                't."REALISED_CLOSE_PRICE" IS NULL',
                't."REALISED_PROFIT" IS NULL',
            ])

        if exchange is not None:
            where_clauses.append('t."EXCHANGE" = :exchange')
            params["exchange"] = exchange

        where_sql = " AND ".join(where_clauses)

        sql = text(f"""
            WITH pending AS (
                SELECT
                    t."EXCHANGE",
                    t."SYMBOL",
                    t."DATE",
                    t."CLOSE" AS signal_close
                FROM {target_schema}."{target_table}" t
                WHERE {where_sql}
            ),
            matched AS (
                SELECT
                    p."EXCHANGE",
                    p."SYMBOL",
                    p."DATE",
                    p.signal_close,
                    s."TS" AS realised_close_timestamp,
                    s."CLOSE" AS realised_close_price,
                    ROW_NUMBER() OVER (
                        PARTITION BY p."EXCHANGE", p."SYMBOL", p."DATE"
                        ORDER BY s."TS" DESC
                    ) AS rn
                FROM pending p
                JOIN {source_schema}."{source_table}" s
                ON s."EXCHANGE" = p."EXCHANGE"
                AND s."SYMBOL" = p."SYMBOL"
                AND s."TS"::date = TO_DATE(p."DATE", 'DD-MM-YYYY')
            ),
            final_match AS (
                SELECT
                    "EXCHANGE",
                    "SYMBOL",
                    "DATE",
                    realised_close_timestamp,
                    realised_close_price,
                    CASE
                        WHEN signal_close IS NULL OR signal_close = 0 THEN NULL
                        ELSE ROUND((((realised_close_price - signal_close) / signal_close) * 100.0)::numeric, 4)
                    END AS realised_profit
                FROM matched
                WHERE rn = 1
            )
            UPDATE {target_schema}."{target_table}" t
            SET
                "REALISED_CLOSE_TIMESTAMP" = fm.realised_close_timestamp,
                "REALISED_CLOSE_PRICE" = fm.realised_close_price,
                "REALISED_PROFIT" = fm.realised_profit
            FROM final_match fm
            WHERE t."EXCHANGE" = fm."EXCHANGE"
            AND t."SYMBOL" = fm."SYMBOL"
            AND t."DATE" = fm."DATE"
        """)

        with self.engine.begin() as conn:
            result = conn.execute(sql, params)
            return result.rowcount if result.rowcount is not None else 0
        

# ======================
# final master combined
# ======================
    def build_master_final_combined(
        self,
        exchange: str,
        target_schema: str,
        target_table: str,
        log_schema: str,
        log_table: str,
        main_schema: str,
        main_table: str,
        score_schema: str,
        score_table: str,
        triage_schema: str,
        triage_table: str,
    ) -> Dict[str, int]:
        """
        Build final combined table from:
        1) main_table
        2) left join score_table on EXCHANGE, SYMBOL, FRVP_PERIOD_TYPE
        3) left join triage_table on EXCHANGE, SYMBOL

        - target table is truncated before insert
        - log table is append-only
        """

        exchange = exchange.upper().strip()

        # 1) truncate target
        self.truncate_table(target_schema, target_table)

        insert_columns = [
            "EXCHANGE",
            "SYMBOL",

            "FRVP_INTERVAL",
            "FRVP_PERIOD_TYPE",
            "FRVP_HIGHEST_DATE",
            "FRVP_HIGHEST_VALUE",
            "FRVP_ROW_COUNT_AFTER_HIGHEST",
            "FRVP_DAY_COUNT_AFTER_HIGHEST",
            "FRVP_LATEST_CLOSE_VALUE",
            "FRVP_POC",
            "FRVP_VAL",
            "FRVP_VAH",

            "BS_OPEN_PRICE",
            "BS_CLOSE_PRICE",
            "BS_DIFFER",
            "BS_PERC",
            "BS_BAR_STATUS",

            "RAW_END_DATE",
            "BRONZE_END_DATE",
            "SILVER_END_DATE",
            "SILVER_CONVERTED_END_DATE",
            "EXPECTED_END_DATE",

            "EMA_TIMESTAMP",
            "EMA_END_DATE",
            "EMA3",
            "EMA5",
            "EMA14",
            "EMA20",
            "EMA_STATUS_5_20",
            "EMA_CROSS_5_20",
            "EMA_STATUS_3_20",
            "EMA_CROSS_3_20",
            "EMA_STATUS_3_14",
            "EMA_CROSS_3_14",
            "EMA_DAYS_SINCE_CROSS_5_20",
            "EMA_DAYS_SINCE_CROSS_3_20",
            "EMA_DAYS_SINCE_CROSS_3_14",

            "RSI",
            "RSI_MA",
            "RSI_STATUS",
            "RSI_CROSS",
            "RSI_CROSS_DAYS_AGO",

            "MFI",
            "MFI_YESTERDAY",
            "MFI_12DAY_AVG",
            "MFI_DIRECTION",

            "VWAP_HIGHEST_VALUE",
            "VWAP_HIGHEST_TIMESTAMP",
            "VWAP",
            "VOL_AVG_5DAY",
            "VOL_AVG_10DAY",
            "VOL_AVG_20DAY",
            "VOL_YESTERDAY",
            "VOL_LASTDAY",
            "VOL_STATUS",

            "PVT_START_DATE",
            "PVT_END_DATE",
            "PVT_YEAR",
            "PIVOT",
            "PVT_R1",
            "PVT_R2",
            "PVT_R3",
            "PVT_R4",
            "PVT_R5",
            "PVT_S1",
            "PVT_S2",
            "PVT_S3",
            "PVT_S4",
            "PVT_S5",
            "PVT_STATUS",

            "POC_FRVP_STATUS",
            "VWAP_STATUS",
            "EMA_STATUS",
            "RSI_STATUS_TXT",
            "MFI_STATUS",
            "VOL_STATUS_TXT",
            "MASTER_SCORE",
            "TRIAGE_ENTRY_DAY",
            "TRIAGE_SCORE",
            "RANK",
            "VALID_CLUSTER_COUNT",
            "AVG_POC",
            "STOP_LOSS",
            "TARGET_PRICE",
            "STOP_LOSS_PERC",
            "TARGET_PERC",
            "CREATED_AT",
            "CREATED_DAY",
            "RUNTIME",
        ]

        insert_columns_sql = ",\n                ".join(f'"{col}"' for col in insert_columns)

        select_sql = f"""
            SELECT
                m."EXCHANGE" AS "EXCHANGE",
                m."SYMBOL" AS "SYMBOL",

                m."FRVP_INTERVAL" AS "FRVP_INTERVAL",
                m."FRVP_PERIOD_TYPE" AS "FRVP_PERIOD_TYPE",
                m."FRVP_HIGHEST_DATE" AS "FRVP_HIGHEST_DATE",
                m."FRVP_HIGHEST_VALUE" AS "FRVP_HIGHEST_VALUE",
                m."FRVP_ROW_COUNT_AFTER_HIGHEST" AS "FRVP_ROW_COUNT_AFTER_HIGHEST",
                m."FRVP_DAY_COUNT_AFTER_HIGHEST" AS "FRVP_DAY_COUNT_AFTER_HIGHEST",
                m."FRVP_LATEST_CLOSE_VALUE" AS "FRVP_LATEST_CLOSE_VALUE",
                m."FRVP_POC" AS "FRVP_POC",
                m."FRVP_VAL" AS "FRVP_VAL",
                m."FRVP_VAH" AS "FRVP_VAH",

                m."BS_OPEN_PRICE" AS "BS_OPEN_PRICE",
                m."BS_CLOSE_PRICE" AS "BS_CLOSE_PRICE",
                m."BS_DIFFER" AS "BS_DIFFER",
                m."BS_PERC" AS "BS_PERC",
                m."BS_BAR_STATUS" AS "BS_BAR_STATUS",

                m."RAW_END_DATE" AS "RAW_END_DATE",
                m."BRONZE_END_DATE" AS "BRONZE_END_DATE",
                m."SILVER_END_DATE" AS "SILVER_END_DATE",
                m."SILVER_CONVERTED_END_DATE" AS "SILVER_CONVERTED_END_DATE",
                m."EXPECTED_END_DATE" AS "EXPECTED_END_DATE",

                m."EMA_TIMESTAMP" AS "EMA_TIMESTAMP",
                m."EMA_END_DATE" AS "EMA_END_DATE",
                m."EMA3" AS "EMA3",
                m."EMA5" AS "EMA5",
                m."EMA14" AS "EMA14",
                m."EMA20" AS "EMA20",
                m."EMA_STATUS_5_20" AS "EMA_STATUS_5_20",
                m."EMA_CROSS_5_20" AS "EMA_CROSS_5_20",
                m."EMA_STATUS_3_20" AS "EMA_STATUS_3_20",
                m."EMA_CROSS_3_20" AS "EMA_CROSS_3_20",
                m."EMA_STATUS_3_14" AS "EMA_STATUS_3_14",
                m."EMA_CROSS_3_14" AS "EMA_CROSS_3_14",
                m."EMA_DAYS_SINCE_CROSS_5_20" AS "EMA_DAYS_SINCE_CROSS_5_20",
                m."EMA_DAYS_SINCE_CROSS_3_20" AS "EMA_DAYS_SINCE_CROSS_3_20",
                m."EMA_DAYS_SINCE_CROSS_3_14" AS "EMA_DAYS_SINCE_CROSS_3_14",

                m."RSI" AS "RSI",
                m."RSI_MA" AS "RSI_MA",
                m."RSI_STATUS" AS "RSI_STATUS",
                m."RSI_CROSS" AS "RSI_CROSS",
                m."RSI_CROSS_DAYS_AGO" AS "RSI_CROSS_DAYS_AGO",

                m."MFI" AS "MFI",
                m."MFI_YESTERDAY" AS "MFI_YESTERDAY",
                m."MFI_12DAY_AVG" AS "MFI_12DAY_AVG",
                m."MFI_DIRECTION" AS "MFI_DIRECTION",

                m."VWAP_HIGHEST_VALUE" AS "VWAP_HIGHEST_VALUE",
                m."VWAP_HIGHEST_TIMESTAMP" AS "VWAP_HIGHEST_TIMESTAMP",
                m."VWAP" AS "VWAP",
                m."VOL_AVG_5DAY" AS "VOL_AVG_5DAY",
                m."VOL_AVG_10DAY" AS "VOL_AVG_10DAY",
                m."VOL_AVG_20DAY" AS "VOL_AVG_20DAY",
                m."VOL_YESTERDAY" AS "VOL_YESTERDAY",
                m."VOL_LASTDAY" AS "VOL_LASTDAY",
                m."VOL_STATUS" AS "VOL_STATUS",

                m."PVT_START_DATE" AS "PVT_START_DATE",
                m."PVT_END_DATE" AS "PVT_END_DATE",
                m."PVT_YEAR" AS "PVT_YEAR",
                m."PIVOT" AS "PIVOT",
                m."PVT_R1" AS "PVT_R1",
                m."PVT_R2" AS "PVT_R2",
                m."PVT_R3" AS "PVT_R3",
                m."PVT_R4" AS "PVT_R4",
                m."PVT_R5" AS "PVT_R5",
                m."PVT_S1" AS "PVT_S1",
                m."PVT_S2" AS "PVT_S2",
                m."PVT_S3" AS "PVT_S3",
                m."PVT_S4" AS "PVT_S4",
                m."PVT_S5" AS "PVT_S5",
                m."PVT_STATUS" AS "PVT_STATUS",

                s."poc_frvp_status" AS "POC_FRVP_STATUS",
                s."vwap_status" AS "VWAP_STATUS",
                s."ema_status" AS "EMA_STATUS",
                s."rsi_status" AS "RSI_STATUS_TXT",
                s."mfi_status" AS "MFI_STATUS",
                s."vol_status" AS "VOL_STATUS_TXT",
                s."master_score" AS "MASTER_SCORE",
                e."TRIAGE_ENTRY_DAY" AS "TRIAGE_ENTRY_DAY",

                e."MASTER_SCORE" AS "TRIAGE_SCORE",
                e."RANK" AS "RANK",
                e."VALID_CLUSTER_COUNT" AS "VALID_CLUSTER_COUNT",
                e."AVG_POC" AS "AVG_POC",
                e."STOP_LOSS" AS "STOP_LOSS",
                e."TARGET_PRICE" AS "TARGET_PRICE",
                e."STOP_LOSS_PERC" AS "STOP_LOSS_PERC",
                e."TARGET_PERC" AS "TARGET_PERC",

                CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Amsterdam' AS "CREATED_AT",
                TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Amsterdam', 'DD-MM-YYYY') AS "CREATED_DAY",
                TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Amsterdam', 'DD-MM-YYYY HH24:MI') AS "RUNTIME"

            FROM {main_schema}."{main_table}" m

            LEFT JOIN {score_schema}."{score_table}" s
                ON m."EXCHANGE" = s."EXCHANGE"
            AND m."SYMBOL" = s."SYMBOL"
            AND m."FRVP_PERIOD_TYPE" = s."FRVP_PERIOD_TYPE"

            LEFT JOIN {triage_schema}."{triage_table}" e
                ON m."EXCHANGE" = e."EXCHANGE"
            AND m."SYMBOL" = e."SYMBOL"

            WHERE m."EXCHANGE" = :exchange
        """

        insert_target_sql = f"""
            INSERT INTO {target_schema}."{target_table}" (
                    {insert_columns_sql}
            )
            {select_sql}
        """

        insert_log_sql = f"""
            INSERT INTO {log_schema}."{log_table}" (
                    {insert_columns_sql}
            )
            {select_sql}
        """

        count_sql = text(f'SELECT COUNT(*) FROM {target_schema}."{target_table}";')

        with self.engine.begin() as conn:
            conn.execute(text(insert_target_sql), {"exchange": exchange})
            master_count = conn.execute(count_sql).scalar() or 0
            conn.execute(text(insert_log_sql), {"exchange": exchange})

        return {
            "master_inserted_rows": int(master_count),
        }