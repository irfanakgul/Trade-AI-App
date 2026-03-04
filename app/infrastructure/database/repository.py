# app/infrastructure/database/repository.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple, Dict, Any
from sqlalchemy import text
from sqlalchemy.engine import Engine
from datetime import datetime,timezone,timedelta

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
    
    # raw data handling
    def sync_archive_to_working(
        self,
        archive_schema: str,
        archive_table: str,
        working_schema: str,
        working_table: str,
        ts_col: str = "TS",
        safety_days: int = 1,
    ) -> int:
        """
        Append-only sync: moves only recent/new rows from archive into working using ON CONFLICT DO NOTHING.
        Uses working MAX(TS) as watermark and subtracts safety_days to avoid missing edge minutes.
        """
        cols = '"SYMBOL","TIMESTAMP","TS","OPEN","HIGH","LOW","CLOSE","VOLUME","SOURCE","ROW_ID"'

        get_max = text(f"""
            SELECT MAX("{ts_col}") AS max_ts
            FROM {working_schema}.{working_table};
        """)

        insert_q = text(f"""
            INSERT INTO {working_schema}.{working_table} ({cols})
            SELECT {cols}
            FROM {archive_schema}.{archive_table} a
            WHERE a."{ts_col}" >= :from_ts
            ON CONFLICT ("ROW_ID") DO NOTHING;
        """)

        with self.engine.begin() as conn:
            max_ts = conn.execute(get_max).scalar()

            if max_ts is None:
                from_ts = datetime(1900, 1, 1)
            else:
                from_ts = (max_ts - timedelta(days=safety_days))

            res = conn.execute(insert_q, {"from_ts": from_ts})
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