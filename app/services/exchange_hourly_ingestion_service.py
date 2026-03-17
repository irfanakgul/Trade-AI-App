from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional


@dataclass(frozen=True)
class ExchangeHourlyIngestionConfig:
    job_name: str
    exchange: str

    target_schema: str = "raw"
    target_table: str = ""

    last_ts_schema: str = "raw"
    last_ts_table: str = ""
    last_ts_column: str = "TS"

    error_schema: str = "logs"
    error_table: str = "ingestion_errors"

    safe_days_back: int = 1
    main_provider_retries: int = 2
    enable_fallback: bool = True
    max_concurrent_symbols: int = 4


class ExchangeHourlyIngestionService:
    def __init__(
        self,
        repo,
        main_provider,
        alternative_provider,
        cfg: ExchangeHourlyIngestionConfig,
    ):
        self.repo = repo
        self.main_provider = main_provider
        self.alternative_provider = alternative_provider
        self.cfg = cfg
        self._sem = asyncio.Semaphore(cfg.max_concurrent_symbols)

        self.summary = {
            "total_symbols": 0,
            "main_first_try_success": 0,
            "main_retry_success": 0,
            "fallback_success": 0,
            "final_fail": 0,
            "tvdata_requests": 0,
            "tvdata_success": 0,
            "yahoo_requests": 0,
            "yahoo_success": 0,
            "failed_symbols": [],
        }
        self._summary_lock = asyncio.Lock()

    def _provider_key(self, provider_name: str) -> str:
        p = provider_name.lower()
        if "tv" in p:
            return "tvdata"
        if "yahoo" in p:
            return "yahoo"
        return p

    def _print_summary(self) -> None:
        exchange = self.cfg.exchange

        print(f"\n[{exchange}-HOURLY] RUN SUMMARY")
        print(f"[{exchange}-HOURLY] total_symbols={self.summary['total_symbols']}")
        print(f"[{exchange}-HOURLY] main_first_try_success={self.summary['main_first_try_success']}")
        print(f"[{exchange}-HOURLY] main_retry_success={self.summary['main_retry_success']}")
        print(f"[{exchange}-HOURLY] fallback_success={self.summary['fallback_success']}")
        print(f"[{exchange}-HOURLY] final_fail={self.summary['final_fail']}")
        print(f"[{exchange}-HOURLY] tvdata_requests={self.summary['tvdata_requests']}")
        print(f"[{exchange}-HOURLY] tvdata_success={self.summary['tvdata_success']}")
        print(f"[{exchange}-HOURLY] yahoo_requests={self.summary['yahoo_requests']}")
        print(f"[{exchange}-HOURLY] yahoo_success={self.summary['yahoo_success']}")

        if self.summary["failed_symbols"]:
            print(
                f"[{exchange}-HOURLY] failed_symbols={len(self.summary['failed_symbols'])} "
                f"-> {self.summary['failed_symbols']}"
            )
        else:
            print(f"[{exchange}-HOURLY] failed_symbols=0 -> []")

    async def run(
        self,
        symbols: list[str],
        use_db_last_timestamp: bool = True,
        start_date: Optional[str] = None,
    ) -> None:
        if not symbols:
            print(f"[{self.cfg.exchange}-HOURLY] No symbols found.")
            return

        self.summary["total_symbols"] = len(symbols)

        tasks = [
            asyncio.create_task(
                self._process_symbol(
                    i + 1,
                    len(symbols),
                    s,
                    use_db_last_timestamp,
                    start_date,
                )
            )
            for i, s in enumerate(symbols)
        ]
        await asyncio.gather(*tasks)

        self._print_summary()

    async def _process_symbol(
        self,
        idx: int,
        total: int,
        symbol: str,
        use_db_last_timestamp: bool,
        start_date: Optional[str],
    ) -> None:
        async with self._sem:
            try:
                safe_start_dt = self._resolve_safe_start(
                    symbol=symbol,
                    use_db_last_timestamp=use_db_last_timestamp,
                    start_date=start_date,
                )

                main_name = self.main_provider.__class__.__name__.replace("HourlyProvider", "")
                alt_name = (
                    self.alternative_provider.__class__.__name__.replace("HourlyProvider", "")
                    if self.alternative_provider
                    else None
                )

                last_error = None
                main_df = None
                provider_key = self._provider_key(main_name)

                # --------------------------------------------------
                # MAIN PROVIDER
                # --------------------------------------------------
                for attempt in range(1 + self.cfg.main_provider_retries):
                    try:
                        async with self._summary_lock:
                            if provider_key == "tvdata":
                                self.summary["tvdata_requests"] += 1
                            elif provider_key == "yahoo":
                                self.summary["yahoo_requests"] += 1

                        main_df = await asyncio.to_thread(
                            self.main_provider.fetch,
                            self.cfg.exchange,
                            symbol,
                            safe_start_dt,
                        )

                        if main_df is None or main_df.empty:
                            raise ValueError("EMPTY DATAFRAME")

                        inserted_rows = self.repo.bulk_insert_on_conflict_do_nothing(
                            schema=self.cfg.target_schema,
                            table=self.cfg.target_table,
                            rows=main_df.to_dict(orient="records"),
                            conflict_column="ROW_ID",
                        )

                        self.repo.clear_ingestion_error(
                            schema=self.cfg.error_schema,
                            table=self.cfg.error_table,
                            job_name=self.cfg.job_name,
                            symbol=symbol,
                            exchange=self.cfg.exchange,
                        )

                        async with self._summary_lock:
                            if attempt == 0:
                                self.summary["main_first_try_success"] += 1
                            else:
                                self.summary["main_retry_success"] += 1

                            if provider_key == "tvdata":
                                self.summary["tvdata_success"] += 1
                            elif provider_key == "yahoo":
                                self.summary["yahoo_success"] += 1

                        status = "SUCCESS" if attempt == 0 else f"SUCCESS (retry {attempt})"

                        print(
                            f"[{self.cfg.exchange}-HOURLY {idx}/{total}] {symbol} | "
                            f"{main_name} | {status} | attempted={len(main_df)} inserted={inserted_rows}"
                        )
                        return

                    except Exception as e:
                        last_error = e
                        print(
                            f"[{self.cfg.exchange}-HOURLY {idx}/{total}] {symbol} | "
                            f"{main_name} | FAILED (attempt {attempt + 1}) | error={repr(e)}"
                        )

                # --------------------------------------------------
                # FALLBACK PROVIDER
                # --------------------------------------------------
                if self.cfg.enable_fallback and self.alternative_provider is not None:
                    try:
                        print(
                            f"[{self.cfg.exchange}-HOURLY {idx}/{total}] {symbol} | "
                            f"switching to fallback -> {alt_name}"
                        )

                        fallback_key = self._provider_key(alt_name)

                        async with self._summary_lock:
                            if fallback_key == "tvdata":
                                self.summary["tvdata_requests"] += 1
                            elif fallback_key == "yahoo":
                                self.summary["yahoo_requests"] += 1

                        fb_df = await asyncio.to_thread(
                            self.alternative_provider.fetch,
                            self.cfg.exchange,
                            symbol,
                            safe_start_dt,
                        )

                        if fb_df is None or fb_df.empty:
                            raise ValueError("EMPTY DATAFRAME")

                        inserted_rows = self.repo.bulk_insert_on_conflict_do_nothing(
                            schema=self.cfg.target_schema,
                            table=self.cfg.target_table,
                            rows=fb_df.to_dict(orient="records"),
                            conflict_column="ROW_ID",
                        )

                        self.repo.clear_ingestion_error(
                            schema=self.cfg.error_schema,
                            table=self.cfg.error_table,
                            job_name=self.cfg.job_name,
                            symbol=symbol,
                            exchange=self.cfg.exchange,
                        )

                        async with self._summary_lock:
                            self.summary["fallback_success"] += 1

                            if fallback_key == "tvdata":
                                self.summary["tvdata_success"] += 1
                            elif fallback_key == "yahoo":
                                self.summary["yahoo_success"] += 1

                        print(
                            f"[{self.cfg.exchange}-HOURLY {idx}/{total}] {symbol} | "
                            f"{alt_name} | SUCCESS (fallback) | attempted={len(fb_df)} inserted={inserted_rows}"
                        )
                        return

                    except Exception as fb_err:
                        print(
                            f"[{self.cfg.exchange}-HOURLY {idx}/{total}] {symbol} | "
                            f"{alt_name} | FAILED (fallback) | error={repr(fb_err)}"
                        )

                        self.repo.upsert_ingestion_error(
                            schema=self.cfg.error_schema,
                            table=self.cfg.error_table,
                            job_name=self.cfg.job_name,
                            symbol=symbol,
                            exchange=self.cfg.exchange,
                            error_type=type(fb_err).__name__,
                            error_message=str(fb_err),
                        )

                        async with self._summary_lock:
                            self.summary["final_fail"] += 1
                            self.summary["failed_symbols"].append(symbol)
                        return
                if self.cfg.enable_fallback and self.alternative_provider is None:
                    print(
                        f"[{self.cfg.exchange}-HOURLY {idx}/{total}] {symbol} | "
                        f"fallback configured but provider is not implemented yet"
                    )
                                # --------------------------------------------------
                # FINAL FAIL (NO FALLBACK / FALLBACK DISABLED)
                # --------------------------------------------------
                self.repo.upsert_ingestion_error(
                    schema=self.cfg.error_schema,
                    table=self.cfg.error_table,
                    job_name=self.cfg.job_name,
                    symbol=symbol,
                    exchange=self.cfg.exchange,
                    error_type=type(last_error).__name__ if last_error else "UnknownError",
                    error_message=str(last_error) if last_error else "Main provider failed",
                )

                async with self._summary_lock:
                    self.summary["final_fail"] += 1
                    self.summary["failed_symbols"].append(symbol)

                print(
                    f"[{self.cfg.exchange}-HOURLY {idx}/{total}] {symbol} | "
                    f"FINAL FAIL | error={repr(last_error)}"
                )

            except Exception as e:
                self.repo.upsert_ingestion_error(
                    schema=self.cfg.error_schema,
                    table=self.cfg.error_table,
                    job_name=self.cfg.job_name,
                    symbol=symbol,
                    exchange=self.cfg.exchange,
                    error_type=type(e).__name__,
                    error_message=str(e),
                )

                async with self._summary_lock:
                    self.summary["final_fail"] += 1
                    self.summary["failed_symbols"].append(symbol)

                print(
                    f"[{self.cfg.exchange}-HOURLY {idx}/{total}] {symbol} | "
                    f"CRITICAL FAIL | error={repr(e)}"
                )

    def _resolve_safe_start(
        self,
        symbol: str,
        use_db_last_timestamp: bool,
        start_date: Optional[str],
    ) -> datetime:
        if use_db_last_timestamp:
            last_ts = self.repo.get_last_timestamp(
                symbol=symbol,
                schema=self.cfg.last_ts_schema,
                table=self.cfg.last_ts_table,
                ts_column=self.cfg.last_ts_column,
            )
            if last_ts:
                return last_ts - timedelta(days=self.cfg.safe_days_back)

        if start_date:
            return datetime.strptime(start_date, "%Y-%m-%d")

        return datetime.now() - timedelta(days=self.cfg.safe_days_back + 7)