from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from app.infrastructure.database.repository import PostgresRepository
from app.services.ind_frv_poc_profile_service import IndFrvPocProfileService


@dataclass(frozen=True)
class IndicatorsFlags:
    # --------------------------------------------------
    # FRVP indicator calculation
    # --------------------------------------------------
    frvp: bool = True
    # If True → FRVP POC/VAL/VAH hesaplamasını çalıştırır.
    # False ise FRVP tamamen atlanır.

    truncate_scope: bool = True
    # FRVP output tablosundaki mevcut veriyi silip yeniden hesaplar.
    # True → tabloyu truncate edip yeniden üretir
    # False → mevcut veriyi bırakır (genelde test için)

    periods: List[str] = None
    # FRVP hesaplamasında kullanılacak period listesi.
    # Örn: ["2year", "1year", "6months", "4months"]

    cutt_off_date: Optional[str] = None
    # FRVP hesaplaması için opsiyonel tarih limiti.
    # None → tüm veri kullanılır
    # "2025-01-01" gibi verilirse o tarihten sonrası kullanılır.


    # --------------------------------------------------
    # Converted Daily dataset (EMA / RSI input dataset)
    # --------------------------------------------------
    build_converted_daily: bool = False
    # True → dakikalık veya günlük dataset'i
    # EMA/RSI hesaplamaları için günlük formata dönüştürür.

    converted_daily_input_schema: str = "silver"
    # Kaynak dataset'in bulunduğu schema.

    converted_daily_input_table: str = ""
    # Kaynak dataset tablosu.
    # Örn:
    # FRVP_USA_FOCUS_DATASET
    # FRVP_BIST_FOCUS_DATASET

    converted_daily_input_interval: str = "1min"
    # Kaynak dataset'in veri frekansı.
    # "1min"  → dakikalık veri
    # "daily" → zaten günlük veri

    converted_daily_output_schema: str = "silver"
    # Üretilecek converted daily dataset'in schema'sı.

    converted_daily_output_table: str = ""
    # Üretilecek converted daily dataset'in tablo adı.
    # Örn:
    # usa_focus_2e_indicators_converted_daily
    # bist_focus_2e_indicators_converted_daily

    converted_daily_start_trading_days_back: int = 30
    # Son kaç trading day kullanılacağını belirler.
    # Örn: 30 → son 30 işlem günü kullanılır.


def run_indicators_for_exchange(repo: PostgresRepository, exchange: str, flags: IndicatorsFlags) -> None:
    exchange = exchange.upper().strip()

    # Resolve periods once
    periods = flags.periods if flags.periods is not None else ["2year", "1year", "6months", "4months"]

    # ----------------------------------------------------------
    # 1) FRVP (optional)
    # ----------------------------------------------------------
    if flags.frvp:
        svc = IndFrvPocProfileService(repo=repo)

        print(
            f"\n[IND] FRVP POC/VAL/VAH started ({exchange})... "
            f"{datetime.now().strftime('%d-%m-%Y %H:%M')}\n"
        )
        svc.run(
            exchange=exchange,
            periods=periods,
            cutt_off_date=flags.cutt_off_date,
            is_truncate_scope=flags.truncate_scope,
        )
        print(
            f"\n[IND] FRVP POC/VAL/VAH ended ({exchange})... "
            f"{datetime.now().strftime('%d-%m-%Y %H:%M')}\n"
        )
    else:
        print(f"[IND] FRVP skipped for exchange={exchange}")

    # ----------------------------------------------------------
    # 2) Build "converted daily" dataset (optional)
    # ----------------------------------------------------------
    # Priority:
    # - If build_converted_daily=True -> use parametric inputs from flags
    # - Else if build_converted_daily_for_ema_rsi=True -> use legacy hardcoded mapping
    if flags.build_converted_daily:
        if not flags.converted_daily_input_table or not flags.converted_daily_output_table:
            raise ValueError("converted_daily_input_table and converted_daily_output_table must be set when build_converted_daily=True")

        stats = repo.build_converted_daily_for_ema_rsi_scope(
            exchange=exchange,
            interval=flags.converted_daily_input_interval,  # "1min" or "daily"
            start_trading_days_back=flags.converted_daily_start_trading_days_back,
            source_schema=flags.converted_daily_input_schema,
            source_table=flags.converted_daily_input_table,
            ts_col="TS",
            high_col="HIGH",
            target_schema=flags.converted_daily_output_schema,
            target_table=flags.converted_daily_output_table,
        )

        print(
            f'[IND] Converted-daily built. exchange={stats["exchange"]} '
            f'symbols: {stats["before_symbols"]} -> {stats["after_symbols"]}, '
            f'rows={stats["after_rows"]} target={stats["target"]}'
        )

    elif flags.build_converted_daily_for_ema_rsi:
        # Legacy mapping (works with your current focus tables)
        if exchange == "BIST":
            print(
                f"[IND] Converted-daily building... exchange={exchange} "
                f"source={flags.converted_daily_input_schema}.{flags.converted_daily_input_table} "
                f"interval={flags.converted_daily_input_interval} "
                f"days_back={flags.converted_daily_start_trading_days_back}",
                flush=True
            )
            stats = repo.build_converted_daily_for_ema_rsi_scope(
                exchange="BIST",
                interval="daily",
                start_trading_days_back=flags.converted_daily_trading_days_back,
                source_schema="silver",
                source_table="FRVP_BIST_FOCUS_DATASET",
                ts_col="TS",
                high_col="HIGH",
                target_schema="silver",
                target_table="bist_focus_2e_indicators_converted_daily",
            )
        elif exchange == "USA":
            print(
                f"[IND] Converted-daily building... exchange={exchange} "
                f"source={flags.converted_daily_input_schema}.{flags.converted_daily_input_table} "
                f"interval={flags.converted_daily_input_interval} "
                f"days_back={flags.converted_daily_start_trading_days_back}",
                flush=True
            )
            stats = repo.build_converted_daily_for_ema_rsi_scope(
                exchange="USA",
                interval="1min",
                start_trading_days_back=flags.converted_daily_trading_days_back,
                source_schema="silver",
                source_table="FRVP_USA_FOCUS_DATASET",
                ts_col="TS",
                high_col="HIGH",
                target_schema="silver",
                target_table="usa_focus_2e_indicators_converted_daily",
            )
        else:
            raise ValueError(f"Unsupported exchange for legacy converted-daily: {exchange}")

        print(
            f'[IND] Converted-daily built. exchange={stats["exchange"]} '
            f'symbols: {stats["before_symbols"]} -> {stats["after_symbols"]}, '
            f'rows={stats["after_rows"]} target={stats["target"]}'
        )
    else:
        print(f"[IND] Converted-daily step skipped for exchange={exchange}")