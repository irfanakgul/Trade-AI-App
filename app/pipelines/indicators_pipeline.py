from __future__ import annotations
import os
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from app.infrastructure.database.repository import PostgresRepository
from app.services.ind_frv_poc_profile_service import IndFrvPocProfileService
from app.services.ind_ema_focus_service import IndEmaFocusService
from app.services.ind_vwap_focus_service import IndVwapFocusService # type: ignore
from app.services.email_service import send_email



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

    periods: List[str] = ''  
    #check below in func
    # FRVP hesaplamasında kullanılacak period listesi.
    # Örn: ["2year", "1year", "6months", "4months"]

    cutt_off_date: Optional[str] = None
    # FRVP hesaplaması için opsiyonel tarih limiti.
    # None → tüm veri kullanılır
    # "2025-01-01" gibi verilirse o tarihten sonrası kullanılır.


    # --------------------------------------------------
    # Converted Daily dataset (EMA / RSI input dataset)
    # --------------------------------------------------
    build_converted_daily: bool = True
    # True → dakikalık veya günlük dataset'i
    # EMA/RSI hesaplamaları için günlük formata dönüştürür.


    converted_daily_input_schema: str = ""
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

    converted_daily_output_schema: str = ""
    # Üretilecek converted daily dataset'in schema'sı.

    converted_daily_output_table: str = ""
    # Üretilecek converted daily dataset'in tablo adı.
    # Örn:
    # usa_focus_2e_indicators_converted_daily
    # bist_focus_2e_indicators_converted_daily

    converted_daily_start_trading_days_back: int = 150
    # Son kaç trading day kullanılacağını belirler.
    # Örn: 30 → son 30 işlem günü kullanılır.

    auto_sample_run: bool = True # generate sample data on converted 1min dataset

    #ema flags
    ema_calc: bool = True
    ema_input_schema: str = ''
    ema_input_table: str = ''
    ema_lookback_days: int = 20 #default 20 day
    ema_is_truncate_scope: bool = True

    #VWAP falgs
    build_vwap_focus: bool = True
    vwap_source_schema: str = "silver"
    vwap_source_table: str = ""
    vwap_target_schema: str = "silver"
    vwap_target_table: str = "IND_VWAP_FOCUS"
    vwap_lookback_month: int = 4




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
            f"\n[IND-FRVP]  FRVP/POC/VAL/VAH started ({exchange})... "
            f"{datetime.now().strftime('%d-%m-%Y %H:%M')}\n"
        )
        svc.run(
            exchange=exchange,
            periods=["2year", "1year", "6months", "4months"],
            cutt_off_date=flags.cutt_off_date,
            is_truncate_scope=flags.truncate_scope,
        )
        print(
            f"\n[IND-FRVP] FRVP POC/VAL/VAH ended ({exchange})... "
            f"{datetime.now().strftime('%d-%m-%Y %H:%M')}\n"
        )

        #save result into google sheet 
        if exchange == 'BIST':
            sheet_name = "FRVP_BIST"
        elif exchange == 'USA':
            sheet_name = "FRVP_USA"

        
        repo.fn_repo_write_to_google(schema='silver',
            table='IND_FRV_POC_PROFILE',
            exchange=exchange,
            scope_col='IN_SCOPE_FOR_EMA_RSI',
            cols=None,
            sheet_name= sheet_name,
            replace_append = 'replace')
    else:
        print(f"⏭️[IND-FRVP] skipped for exchange={exchange}")
    
    
    # ----------------------------------------------------------
    # 2) Build "converted daily" dataset (optional)
    # ----------------------------------------------------------
    if flags.build_converted_daily:
        if not flags.converted_daily_input_table or not flags.converted_daily_output_table:
            raise ValueError(
                "converted_daily_input_table and converted_daily_output_table "
                "must be set when build_converted_daily=True"
            )

        print(
            f"[IND-CONVERT] Converted-daily building... exchange={exchange} "
            f"source={flags.converted_daily_input_schema}.{flags.converted_daily_input_table} "
            f"interval={flags.converted_daily_input_interval} "
            f"days_back={flags.converted_daily_start_trading_days_back}",
            flush=True
        )

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
            f'[IND-CONVERT] Converted-daily built. exchange={stats["exchange"]} '
            f'symbols: {stats["before_symbols"]} -> {stats["after_symbols"]}, '
            f'rows={stats["after_rows"]} target={stats["target"]}',
            flush=True
        )
    else:
        print(f"⏭️[IND-CONVERT] Converted-daily step skipped for exchange={exchange}", flush=True)

    if flags.auto_sample_run:
        if exchange == 'USA':  # takes ema exchange
            symbols = os.getenv("USA_SAMPLE_SYMBOLS", "")
            symbols = [s.strip() for s in symbols.split(",") if s.strip()]
            print(f'[SAMPLE-USA-1MIN-CONVERTED] | Sample symbols {len(symbols)} > {symbols}')

            repo.rebuild_symbol_sample_dataset(
                source_schema=flags.converted_daily_output_schema,
                source_table=flags.converted_daily_output_table,
                target_schema='test',
                target_table='sample_usa_daily_converted',
                symbols=symbols,
                symbol_col='SYMBOL',
                ts_col='TIMESTAMP',
                trading_days_back=30,
            )
        else:
            print(f'⏭️[SAMPLE-BIST-CONVERTED-1MIN] No data for BIST 1min converted yet! For now, only for USA')

    else:
            print(f'⏭️[SAMPLE-USA-CONVERTED-1MIN] SKIPPED!')


    # ----------------------------------------------------------
    # 3) EMA CALC CHAPTER
    # ----------------------------------------------------------

    if flags.ema_calc:
        svc = IndEmaFocusService(repo=repo)
        svc.run(
            exchange=exchange,
            input_schema=flags.ema_input_schema,
            input_table=flags.ema_input_table,
            lookback_days=flags.ema_lookback_days,          # parametrik
            is_truncate_scope=flags.ema_is_truncate_scope,    # exchange bazlı delete
        )
    else:
        print('⏭️[EMA] skipped!')

    # ----------------------------------------------------------
    # 4) VWAP CALC CHAPTER
    # ----------------------------------------------------------
    if flags.build_vwap_focus:
        svc = IndVwapFocusService(repo=repo)
        svc.run(
            exchange=exchange,
            source_schema=flags.vwap_source_schema,
            source_table=flags.vwap_source_table,
            target_schema=flags.vwap_target_schema,
            target_table=flags.vwap_target_table,
            lookback_month=flags.vwap_lookback_month,
            is_truncate_scope=True
        )
    else:
        print(f'⏭️[VWAP] skipped for exchange={exchange}')

    
    send_email(
        to_email=["1irfanakgul@gmail.com"],
        subject=f"INDICATORS-{exchange} RUN INFO",
        body=f"[NOTIFICATION] INDICATORS ({exchange}) has been calculated! \
            \nFRVP:{flags.frvp},\nConvert2Daily:{flags.build_converted_daily},\
            \nSample:{flags.auto_sample_run},\nEMA:{flags.ema_calc},\
            \nVWAP:{flags.build_vwap_focus}"
    )


