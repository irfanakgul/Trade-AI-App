from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional, List

import numpy as np
import pandas as pd

from app.infrastructure.database.repository import PostgresRepository
from app.services.telegram_bot_chat_service import telegram_send_message  # type: ignore
from app.infrastructure.database.db_connector import fn_write_cloud


# ============================================================
# CONFIG
# ============================================================

@dataclass
class MasterScoreWeights:
    poc_frvp: float = 0.62
    vwap: float = 0.02
    ema: float = 0.09
    rsi: float = 0.07
    mfi: float = 0.10
    volume: float = 0.10


@dataclass
class MasterScoreThresholds:
    buy_min: float = 70.0
    watch_min: float = 50.0
    poc_cluster_max_spread: float = 0.03
    triage_bonus_max_spread: float = 0.02
    vwap_near_threshold: float = 0.05
    vwap_far_threshold: float = 0.10
    poc_close_distance_limit: float = 0.20
    poc_val_distance_limit: float = 0.20
    poc_vah_near_threshold: float = 0.05
    poc_vah_far_threshold: float = 0.10
    rsi_cross_max_days: int = 14


@dataclass
class TradeLevelConfig:
    entry_markup_perc: float = 0.5
    stop_loss_perc: float = 3.0
    top_n: int = 10


@dataclass
class RankConfig:
    master_score_min: float = 50.0
    days_after_poc_max: int = 8
    fallback_rank: int = 99999


@dataclass
class TelegramConfig:
    send: bool = False
    title: str = "TOP 10"


@dataclass
class MasterScoreServiceConfig:
    exchange: str = ""
    input_schema: str = ""
    input_table: str = ""
    output_schema: str = ""
    output_table: str = ""
    days_after_poc_input_schema: str = ""
    days_after_poc_input_table: str = ""
    truncate_before_load: bool = True
    created_at: Optional[datetime] = None
    weights: MasterScoreWeights = field(default_factory=MasterScoreWeights)
    thresholds: MasterScoreThresholds = field(default_factory=MasterScoreThresholds)
    trade: TradeLevelConfig = field(default_factory=TradeLevelConfig)
    telegram: TelegramConfig = field(default_factory=TelegramConfig)
    rank: RankConfig = field(default_factory=RankConfig)


# ============================================================
# SERVICE
# ============================================================

class MasterScoreService:
    def __init__(
        self,
        repo: PostgresRepository,
        cfg: MasterScoreServiceConfig = MasterScoreServiceConfig(),
    ):
        self.repo = repo
        self.cfg = cfg

    def run(
        self,
        exchange: Optional[str] = None,
        input_schema: Optional[str] = None,
        input_table: Optional[str] = None,
        output_schema: Optional[str] = None,
        output_table: Optional[str] = None,
        is_truncate_scope: Optional[bool] = None,
        created_at: Optional[datetime] = None,
        top_n: Optional[int] = None,
        rank_master_score_min: Optional[float] = None,
        rank_days_after_poc_max: Optional[int] = None,
        rank_default_value: Optional[int] = None,
        entry_markup_perc: Optional[float] = None,
        stop_loss_perc: Optional[float] = None,
        buy_min: Optional[float] = None,
        watch_min: Optional[float] = None,
        poc_cluster_max_spread: Optional[float] = None,
        triage_bonus_max_spread: Optional[float] = None,
        vwap_near_threshold: Optional[float] = None,
        vwap_far_threshold: Optional[float] = None,
        poc_close_distance_limit: Optional[float] = None,
        poc_val_distance_limit: Optional[float] = None,
        poc_vah_near_threshold: Optional[float] = None,
        poc_vah_far_threshold: Optional[float] = None,
        rsi_cross_max_days: Optional[int] = None,
        weight_poc_frvp: Optional[float] = None,
        weight_vwap: Optional[float] = None,
        weight_ema: Optional[float] = None,
        weight_rsi: Optional[float] = None,
        weight_mfi: Optional[float] = None,
        weight_volume: Optional[float] = None,
        send_telegram: Optional[bool] = None,
        telegram_title: Optional[str] = None,
        log_schema: Optional[str] = None,
        log_table: Optional[str] = None,
        days_after_poc_input_schema: Optional[str] = None,
        days_after_poc_input_table: Optional[str] = None,
    ) -> pd.DataFrame:
        params = self._resolve_run_params(
            exchange=exchange,
            input_schema=input_schema,
            input_table=input_table,
            output_schema=output_schema,
            output_table=output_table,
            is_truncate_scope=is_truncate_scope,
            created_at=created_at,
            top_n=top_n,
            rank_master_score_min=rank_master_score_min,
            rank_days_after_poc_max=rank_days_after_poc_max,
            rank_default_value=rank_default_value,
            entry_markup_perc=entry_markup_perc,
            stop_loss_perc=stop_loss_perc,
            buy_min=buy_min,
            watch_min=watch_min,
            poc_cluster_max_spread=poc_cluster_max_spread,
            triage_bonus_max_spread=triage_bonus_max_spread,
            vwap_near_threshold=vwap_near_threshold,
            vwap_far_threshold=vwap_far_threshold,
            poc_close_distance_limit=poc_close_distance_limit,
            poc_val_distance_limit=poc_val_distance_limit,
            poc_vah_near_threshold=poc_vah_near_threshold,
            poc_vah_far_threshold=poc_vah_far_threshold,
            rsi_cross_max_days=rsi_cross_max_days,
            weight_poc_frvp=weight_poc_frvp,
            weight_vwap=weight_vwap,
            weight_ema=weight_ema,
            weight_rsi=weight_rsi,
            weight_mfi=weight_mfi,
            weight_volume=weight_volume,
            send_telegram=send_telegram,
            telegram_title=telegram_title,
            log_schema=log_schema,
            log_table=log_table,
            days_after_poc_input_schema=days_after_poc_input_schema,
            days_after_poc_input_table=days_after_poc_input_table,
        )

        df = self.repo.get_table_as_dataframe(
            schema_name=params["input_schema"],
            table_name=params["input_table"],
            exchange=params["exchange"],
        )

        if params["is_truncate_scope"]:
            self.repo.truncate_table(
                schema_name=params["output_schema"],
                table_name=params["output_table"],
            )

        if df.empty:
            print(f'[MASTER-SCORE] no data found for {params["exchange"]}.')
            return pd.DataFrame()

        df = self._prepare_numeric_columns(df)
        scored_df = self._calculate_scores(df, params)
        triage_df = self._calculate_triage_selection(scored_df, params)
        days_df = self._add_days_after_poc_column(triage_df, params)
        ranked_df = self._add_rank_column(days_df, params)
        output_df = self._format_output_columns(ranked_df)

        self.repo.insert_dataframe(
            df=output_df,
            schema_name=params["output_schema"],
            table_name=params["output_table"],
        )

        telegram_text = self._build_telegram_text(
            output_df,
            params["exchange"],
            params["top_n"],
        )
        print(telegram_text)

        if params["send_telegram"] and not output_df.empty:
            telegram_send_message(
                title=params["telegram_title"],
                text=telegram_text,
            )

        return output_df

    def _resolve_run_params(self, **kwargs) -> Dict:
        w = self.cfg.weights
        t = self.cfg.thresholds
        tr = self.cfg.trade
        tg = self.cfg.telegram
        rk = self.cfg.rank

        return {
            "exchange": kwargs["exchange"] if kwargs["exchange"] is not None else self.cfg.exchange,
            "input_schema": kwargs["input_schema"] if kwargs["input_schema"] is not None else self.cfg.input_schema,
            "input_table": kwargs["input_table"] if kwargs["input_table"] is not None else self.cfg.input_table,
            "output_schema": kwargs["output_schema"] if kwargs["output_schema"] is not None else self.cfg.output_schema,
            "output_table": kwargs["output_table"] if kwargs["output_table"] is not None else self.cfg.output_table,
            "is_truncate_scope": kwargs["is_truncate_scope"] if kwargs["is_truncate_scope"] is not None else self.cfg.truncate_before_load,
            "created_at": kwargs["created_at"] if kwargs["created_at"] is not None else (self.cfg.created_at or datetime.now()),

            "top_n": kwargs["top_n"] if kwargs["top_n"] is not None else tr.top_n,
            "rank_master_score_min": kwargs["rank_master_score_min"] if kwargs["rank_master_score_min"] is not None else rk.master_score_min,
            "rank_days_after_poc_max": kwargs["rank_days_after_poc_max"] if kwargs["rank_days_after_poc_max"] is not None else rk.days_after_poc_max,
            "rank_default_value": kwargs["rank_default_value"] if kwargs["rank_default_value"] is not None else rk.fallback_rank,
            "entry_markup_perc": kwargs["entry_markup_perc"] if kwargs["entry_markup_perc"] is not None else tr.entry_markup_perc,
            "stop_loss_perc": kwargs["stop_loss_perc"] if kwargs["stop_loss_perc"] is not None else tr.stop_loss_perc,

            "buy_min": kwargs["buy_min"] if kwargs["buy_min"] is not None else t.buy_min,
            "watch_min": kwargs["watch_min"] if kwargs["watch_min"] is not None else t.watch_min,
            "poc_cluster_max_spread": kwargs["poc_cluster_max_spread"] if kwargs["poc_cluster_max_spread"] is not None else t.poc_cluster_max_spread,
            "triage_bonus_max_spread": kwargs["triage_bonus_max_spread"] if kwargs["triage_bonus_max_spread"] is not None else t.triage_bonus_max_spread,
            "vwap_near_threshold": kwargs["vwap_near_threshold"] if kwargs["vwap_near_threshold"] is not None else t.vwap_near_threshold,
            "vwap_far_threshold": kwargs["vwap_far_threshold"] if kwargs["vwap_far_threshold"] is not None else t.vwap_far_threshold,
            "poc_close_distance_limit": kwargs["poc_close_distance_limit"] if kwargs["poc_close_distance_limit"] is not None else t.poc_close_distance_limit,
            "poc_val_distance_limit": kwargs["poc_val_distance_limit"] if kwargs["poc_val_distance_limit"] is not None else t.poc_val_distance_limit,
            "poc_vah_near_threshold": kwargs["poc_vah_near_threshold"] if kwargs["poc_vah_near_threshold"] is not None else t.poc_vah_near_threshold,
            "poc_vah_far_threshold": kwargs["poc_vah_far_threshold"] if kwargs["poc_vah_far_threshold"] is not None else t.poc_vah_far_threshold,
            "rsi_cross_max_days": kwargs["rsi_cross_max_days"] if kwargs["rsi_cross_max_days"] is not None else t.rsi_cross_max_days,

            "weight_poc_frvp": kwargs["weight_poc_frvp"] if kwargs["weight_poc_frvp"] is not None else w.poc_frvp,
            "weight_vwap": kwargs["weight_vwap"] if kwargs["weight_vwap"] is not None else w.vwap,
            "weight_ema": kwargs["weight_ema"] if kwargs["weight_ema"] is not None else w.ema,
            "weight_rsi": kwargs["weight_rsi"] if kwargs["weight_rsi"] is not None else w.rsi,
            "weight_mfi": kwargs["weight_mfi"] if kwargs["weight_mfi"] is not None else w.mfi,
            "weight_volume": kwargs["weight_volume"] if kwargs["weight_volume"] is not None else w.volume,

            "send_telegram": kwargs["send_telegram"] if kwargs["send_telegram"] is not None else tg.send,
            "telegram_title": kwargs["telegram_title"] if kwargs["telegram_title"] is not None else tg.title,

            "days_after_poc_input_schema": (
                kwargs["days_after_poc_input_schema"]
                if kwargs["days_after_poc_input_schema"] is not None
                else self.cfg.days_after_poc_input_schema
            ),
            "days_after_poc_input_table": (
                kwargs["days_after_poc_input_table"]
                if kwargs["days_after_poc_input_table"] is not None
                else self.cfg.days_after_poc_input_table
            ),
        }

    # ============================================================
    # HELPERS
    # ============================================================

    @staticmethod
    def _to_float(col: pd.Series) -> pd.Series:
        return (
            col.astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
            .astype(float)
        )

    def _prepare_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        numeric_cols = [
            "FRVP_LATEST_CLOSE_VALUE",
            "FRVP_POC",
            "FRVP_VAL",
            "FRVP_VAH",
            "VWAP",
            "BS_OPEN_PRICE",
            "BS_CLOSE_PRICE",
            "PIVOT",
            "R2",
            "MFI",
            "MFI_YESTERDAY",
            "MFI_12DAY_AVG",
            "RSI_MA",
            "RSI_CROSS_DAYS_AGO",
            "RSI_STATUS",
            "EMA_STATUS_5_20",
            "EMA_CROSS_5_20",
            "EMA_DAYS_SINCE_CROSS_5_20",
            "EMA_STATUS_3_20",
            "EMA_CROSS_3_20",
            "EMA_DAYS_SINCE_CROSS_3_20",
            "EMA_STATUS_3_14",
            "EMA_CROSS_3_14",
            "EMA_DAYS_SINCE_CROSS_3_14",
        ]

        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    # ============================================================
    # NEW SCORE FUNCTIONS
    # ============================================================

    def _calc_poc_cluster_bonus(self, df: pd.DataFrame) -> np.ndarray:
        def is_close(a, b, tol=0.02):
            return abs(a - b) / min(a, b) <= tol
    
        def cluster_bonus(g: pd.DataFrame):
           
            g_unique = g.drop_duplicates(subset=["FRVP_HIGHEST_DATE"]).copy()
            pocs = g_unique["FRVP_POC"].dropna().astype(float).values
            count = len(pocs)

            if count < 2:
                return 0

            # pairwise 
            close_matrix = [
                [is_close(pocs[i], pocs[j]) for j in range(count)]
                for i in range(count)
            ]

            close_counts = [sum(row) for row in close_matrix]
            max_cluster = max(close_counts)

            if count >= 4:
                if max_cluster >= 4:
                    return 15
                elif max_cluster == 3:
                    return 12
                elif max_cluster == 2:
                    return 10

            elif count == 3:
                if max_cluster == 3:
                    return 10
                elif max_cluster == 2:
                    return 8

            elif count == 2:
                if max_cluster == 2:
                    return 7

            return 0

        bonus_df = (
            df.groupby(["EXCHANGE", "SYMBOL"], group_keys=False)
            .apply(lambda g: pd.Series({"score7": cluster_bonus(g)}))
            .reset_index()
        )

        out = df.merge(bonus_df, on=["EXCHANGE", "SYMBOL"], how="left")
        return out["score7"].fillna(0).to_numpy()

    def _calc_mfi_new(self, df: pd.DataFrame) -> np.ndarray:
        mfi = pd.to_numeric(df["MFI"], errors="coerce").fillna(0)
        mfi_yesterday = pd.to_numeric(df["MFI_YESTERDAY"], errors="coerce").fillna(0)
        mfi_avg = pd.to_numeric(df["MFI_12DAY_AVG"], errors="coerce").fillna(0)

        base = (
            np.where(mfi > mfi_yesterday, 25, 0) +
            np.where(mfi > mfi_avg, 50, 0) +
            np.where(df["MFI_DIRECTION"] == "Upward", 25, 0)
        )

        scaled = (base / 100) * 70
        final = np.where(mfi > 80, scaled + 30, scaled)

        return np.clip(final, 0, 100)

    def _calc_rsi_new(self, df: pd.DataFrame, params: Dict) -> np.ndarray:
        rsi = self._to_float(df["RSI"])
        rsi_ma = pd.to_numeric(df["RSI_MA"], errors="coerce")

        score1 = np.where(rsi > rsi_ma, 80, 0)

        raw_days = df["RSI_CROSS_DAYS_AGO"]
        score2 = np.where(
            (df["RSI_STATUS"] == 1) & (raw_days <= 14),
            np.maximum(0, 15 - raw_days),
            0
        )

        score3 = np.where((rsi >= 30) & (rsi <= 70), 15, 0)

        return np.clip(score1 + score2 + score3, 0, 100)

    def _calc_ema_new(self, df: pd.DataFrame) -> np.ndarray:
        def ema_score(status, cross, days):
            status = pd.to_numeric(status, errors="coerce").fillna(0)
            cross = pd.to_numeric(cross, errors="coerce").fillna(0)
            days = pd.to_numeric(days, errors="coerce").fillna(0)

            return (
            np.where(status == 1, 2, 0) +
            np.where((status == 1) & (cross == 1) & (days < 3), 1, 0)
            )

        total = (
            ema_score(status_5_20, cross_5_20, days_5_20) +
            ema_score(status_3_20, cross_3_20, days_3_20) +
            ema_score(status_3_14, cross_3_14, days_3_14)
        )

        return np.clip((total / 9) * 100, 0, 100)

    def _calc_volume_new(self, df: pd.DataFrame) -> np.ndarray:
        vol_last = self._to_float(df["VOL_LASTDAY"])
        vol_yest = self._to_float(df["VOL_YESTERDAY"])
        vol_5 = self._to_float(df["VOL_AVG_5DAY"])
        vol_10 = self._to_float(df["VOL_AVG_10DAY"])
        vol_20 = self._to_float(df["VOL_AVG_20DAY"])

        return (
            np.where(vol_last > vol_yest, 25, 0) +
            np.where(vol_last > vol_5, 25, 0) +
            np.where(vol_5 > vol_10, 25, 0) +
            np.where(vol_5 > vol_20, 25, 0)
        )

    def _calc_vwap_new(self, df: pd.DataFrame, params: Dict) -> np.ndarray:
        close = pd.to_numeric(df["FRVP_LATEST_CLOSE_VALUE"], errors="coerce")
        vwap = pd.to_numeric(df["VWAP"], errors="coerce")

        dist = np.abs(close - vwap) / vwap

        return np.clip(
            np.where(
                dist <= params["vwap_near_threshold"],
                0,
                np.where(
                    dist >= params["vwap_far_threshold"],
                    100,
                    ((dist - params["vwap_near_threshold"]) / (params["vwap_far_threshold"] - params["vwap_near_threshold"])) * 100
                )
            ),
            0,
            100
        )

    def _calc_poc_frvp_new(self, df: pd.DataFrame) -> np.ndarray:
        close = pd.to_numeric(df["FRVP_LATEST_CLOSE_VALUE"], errors="coerce").astype(float)
        poc = pd.to_numeric(df["FRVP_POC"], errors="coerce").astype(float)
        val = pd.to_numeric(df["FRVP_VAL"], errors="coerce").astype(float)
        vah = pd.to_numeric(df["FRVP_VAH"], errors="coerce").astype(float)
        bs_open = pd.to_numeric(df["BS_OPEN_PRICE"], errors="coerce").astype(float)

        value_range = vah - val

        dist_close_poc = np.abs(close - poc)
        score1 = np.where(
            dist_close_poc <= (value_range * 0.05),
            10,
            np.where(
                dist_close_poc <= (value_range * 0.80),
                (1 - (dist_close_poc - value_range * 0.05) / (value_range * 0.75)) * 10,
                0
            )
        )
        score1 = np.clip(score1, 0, 10)

        score2 = self._calc_poc_cluster_bonus(df)

        score3 = np.where(df["BS_BAR_STATUS"] == "GREEN", 5, 0)

        score4 = np.where(
            (bs_open > poc) & (df["BS_BAR_STATUS"] == "GREEN"),
            5,
            0
        )

        dist_poc_val = np.abs(poc - val)
        score5 = np.where(
            dist_poc_val <= (value_range * 0.10),
            5,
            np.where(
                dist_poc_val <= (value_range * 0.40),
                (1 - (dist_poc_val - value_range * 0.10) / (value_range * 0.30)) * 5,
                0
            )
        )
        score5 = np.clip(score5, 0, 5)

        total = score1 + score2 + score3 + score4 + score5
        final_score = np.clip((total / 40) * 100, 0, 100)

        return final_score

    # ============================================================
    # TRADE LEVELS
    # ============================================================

    def _calculate_trade_levels(self, df: pd.DataFrame, params: Dict) -> pd.DataFrame:
        df = df.copy()

        close = df["FRVP_LATEST_CLOSE_VALUE"]
        df["entry_price"] = close * (1 + (params["entry_markup_perc"] / 100.0))

        pivot = df["PIVOT"] if "PIVOT" in df.columns else pd.Series(np.nan, index=df.index)
        r2 = df["R2"] if "R2" in df.columns else pd.Series(np.nan, index=df.index)

        def choose_target(row):
            c = row["FRVP_LATEST_CLOSE_VALUE"]

            if c < row["FRVP_POC"]:
                return row["VWAP"]
            elif c < row["VWAP"]:
                return row["VWAP"]
            elif c < row["FRVP_VAH"]:
                return row["FRVP_VAH"]
            elif pd.notna(pivot.loc[row.name]) and c < pivot.loc[row.name]:
                return pivot.loc[row.name]
            elif pd.notna(r2.loc[row.name]):
                return r2.loc[row.name]
            else:
                return row["FRVP_VAH"]

        df["target_price"] = df.apply(choose_target, axis=1)
        df["target_pct"] = ((df["target_price"] - df["entry_price"]) / df["entry_price"]) * 100
        df["risk_pct"] = ((df["entry_price"] - df["stop_loss"]) / df["entry_price"]) * 100
        df["rr_ratio"] = df["target_pct"] / df["risk_pct"]

        df["pivot_display"] = np.where(
            pivot.isna(),
            "N/A",
            pivot
        )

        return df

    # ============================================================
    # MAIN SCORE CALC
    # ============================================================

    def _calculate_scores(self, df: pd.DataFrame, params: Dict) -> pd.DataFrame:
        df = df.copy()

        df["poc_frvp_status"] = self._calc_poc_frvp_new(df)
        df["vwap_status"] = self._calc_vwap_new(df, params)
        df["ema_status"] = self._calc_ema_new(df)
        df["rsi_status"] = self._calc_rsi_new(df, params)
        df["mfi_status"] = self._calc_mfi_new(df)
        df["vol_status"] = self._calc_volume_new(df)

        df["master_score"] = (
            df["poc_frvp_status"] * params["weight_poc_frvp"] +
            df["vwap_status"] * params["weight_vwap"] +
            df["ema_status"] * params["weight_ema"] +
            df["rsi_status"] * params["weight_rsi"] +
            df["mfi_status"] * params["weight_mfi"] +
            df["vol_status"] * params["weight_volume"]
        )

        df["master_score"] = np.clip(df["master_score"], 0, 100)

        df["watchlist"] = np.where(
            df["master_score"] >= params["buy_min"], "BUY",
            np.where(df["master_score"] >= params["watch_min"], "WATCH", "AVOID")
        )

        df["entry_price"] = df["FRVP_LATEST_CLOSE_VALUE"] * (1 + (params["entry_markup_perc"] / 100.0))
        df["stop_loss"] = df["entry_price"] * (1 - (params["stop_loss_perc"] / 100.0))

        df = self._calculate_trade_levels(df, params)

        cols = [
            "EXCHANGE", "SYMBOL", "FRVP_INTERVAL", "FRVP_PERIOD_TYPE",
            "poc_frvp_status", "vwap_status", "ema_status", "rsi_status",
            "mfi_status", "vol_status", "master_score", "watchlist", "entry_price",
            "stop_loss", "target_price", "target_pct", "risk_pct", "rr_ratio",
            "pivot_display"
        ]
        df_all_status = df[cols].copy()
        df_all_status["RUNTIME"] = datetime.now().strftime("%Y-%m-%d %H:%M")

        exchange = params["exchange"]
        if exchange == "EURONEXT":
            name = "ams"
        elif exchange == "BINANCE":
            name = "crypto"
        else:
            name = exchange.lower()

        fn_write_cloud(df_all_status, "gold", f"{name}_ind_all_scores", "replace")
        return df

    # ============================================================
    # TRIAGE
    # ============================================================

    def _calculate_triage_selection(self, df: pd.DataFrame, params: Dict) -> pd.DataFrame:
        df_filtered = df[
            (df["BS_CLOSE_PRICE"] > df["FRVP_POC"])
        ].copy()

        if df_filtered.empty:
            return pd.DataFrame()

        grouped = df_filtered.groupby(["EXCHANGE", "SYMBOL"])
        results = []

        created_at = params["created_at"]
        created_day = created_at.strftime("%d-%m-%Y")

        for (exchange, symbol), g in grouped:
            g_unique = g.drop_duplicates(subset=["FRVP_HIGHEST_DATE"])
            count = len(g_unique)

            avg_score = g_unique["master_score"].mean()

            poc_counts = g["FRVP_POC"].value_counts()
            max_freq = poc_counts.max()
            dominant_pocs = poc_counts[poc_counts == max_freq].index
            avg_poc = np.mean(dominant_pocs)

            last_close = pd.to_numeric(g["BS_CLOSE_PRICE"], errors="coerce").iloc[-1]

            stop_loss_price = last_close * (1 - (params["stop_loss_perc"] / 100.0))
            avg_target = pd.to_numeric(g["target_price"], errors="coerce").mean()

            if pd.notna(last_close) and last_close != 0:
                target_pct = ((avg_target - last_close) / last_close) * 100
            else:
                target_pct = 0

            triage_day = pd.to_datetime(
                g["CREATED_DAY"].iloc[0],
                dayfirst=True
            ).strftime("%Y-%m-%d")

            results.append({
                "EXCHANGE": exchange,
                "SYMBOL": symbol,
                "TRIAGE_ENTRY_DAY": triage_day,
                "MASTER_SCORE": round(min(100, avg_score), 2),
                "VALID_CLUSTER_COUNT": count,
                "AVG_POC": f"{avg_poc:.2f}".replace(".", ","),
                "ENTRY_PRICE": f"{last_close:.2f}".replace(".", ","),
                "STOP_LOSS": f"{stop_loss_price:.2f}".replace(".", ","),
                "TARGET_PRICE": f"{avg_target:.2f}".replace(".", ","),
                "STOP_LOSS_PERC": f"-{params['stop_loss_perc']:.2f}%".replace(".", ","),
                "TARGET_PERC": f"{target_pct:.2f}%".replace(".", ","),
                "CREATED_DAY": created_day,
                "CREATED_AT": created_at,
            })

        result_df = pd.DataFrame(results)
        result_df = result_df.sort_values("MASTER_SCORE", ascending=False)

        return result_df

    def _add_rank_column(self, df: pd.DataFrame, params: Dict) -> pd.DataFrame:
        if df.empty:
            return df

        out = df.copy()
        out["RANK"] = params["rank_default_value"]

        master_score = pd.to_numeric(out["MASTER_SCORE"], errors="coerce")
        days_after_poc = pd.to_numeric(out["DAYS_AFTER_POC"], errors="coerce")

        eligible_mask = (
            (master_score >= params["rank_master_score_min"]) &
            (days_after_poc <= params["rank_days_after_poc_max"])
        )
        
        eligible_df = out.loc[eligible_mask].copy()
        eligible_df = eligible_df.sort_values("MASTER_SCORE", ascending=False).reset_index()
        eligible_df["RANK"] = range(1, len(eligible_df) + 1)

        out.loc[eligible_df["index"], "RANK"] = eligible_df["RANK"].values

        return out

    def _add_days_after_poc_column(self, df: pd.DataFrame, params: Dict) -> pd.DataFrame:
        if df.empty:
            return df

        out = df.copy()

        daily_df = self.repo.get_table_as_dataframe(
            schema_name=params["days_after_poc_input_schema"],
            table_name=params["days_after_poc_input_table"],
            exchange=params["exchange"],
        )

        if daily_df.empty:
            out["DAYS_AFTER_POC"] = 0
            return out

        daily_df = daily_df.copy()

        if "TIMESTAMP" not in daily_df.columns or "CLOSE" not in daily_df.columns or "SYMBOL" not in daily_df.columns:
            out["DAYS_AFTER_POC"] = 0
            return out

        daily_df["TIMESTAMP"] = pd.to_datetime(daily_df["TIMESTAMP"], errors="coerce")
        daily_df["CLOSE"] = pd.to_numeric(daily_df["CLOSE"], errors="coerce")

        daily_df = daily_df.dropna(subset=["TIMESTAMP", "CLOSE", "SYMBOL"]).copy()
        daily_df = daily_df.sort_values(["SYMBOL", "TIMESTAMP"], ascending=[True, False])

        out["AVG_POC_NUM"] = (
            out["AVG_POC"]
            .astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
            .astype(float)
        )

        symbol_groups = {
            symbol: grp["CLOSE"].tolist()
            for symbol, grp in daily_df.groupby("SYMBOL", sort=False)
        }

        days_after_list = []

        for row in out.itertuples(index=False):
            symbol = row.SYMBOL
            avg_poc = row.AVG_POC_NUM

            closes = symbol_groups.get(symbol, [])

            count_days = 0
            for close_val in closes:
                if pd.notna(close_val) and close_val >= avg_poc:
                    count_days += 1
                else:
                    break

            days_after_list.append(max(count_days - 1, 0))

        out["DAYS_AFTER_POC"] = days_after_list
        out = out.drop(columns=["AVG_POC_NUM"])

        return out

    # ============================================================
    # OUTPUT / TELEGRAM
    # ============================================================

    def _format_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        ordered_cols = [
            "EXCHANGE",
            "SYMBOL",
            "TRIAGE_ENTRY_DAY",
            "MASTER_SCORE",
            "RANK",
            "VALID_CLUSTER_COUNT",
            "AVG_POC",
            "DAYS_AFTER_POC",
            "ENTRY_PRICE",
            "STOP_LOSS",
            "TARGET_PRICE",
            "STOP_LOSS_PERC",
            "TARGET_PERC",
            "CREATED_DAY",
            "CREATED_AT",
        ]

        existing = [c for c in ordered_cols if c in df.columns]
        remaining = [c for c in df.columns if c not in existing]

        return df[existing + remaining]

    def _build_telegram_text(self, df: pd.DataFrame, exchange: str, top_n: int) -> str:
        if df.empty:
            return f"[MASTER-SCORE] {exchange} | No candidates found."

        lines: List[str] = []
        df_top_n = df.sort_values(by="RANK", ascending=True).head(top_n)

        for idx, row in enumerate(df_top_n.itertuples(index=False), start=1):
            lines.append(
                f"{idx}) {row.EXCHANGE}:{row.SYMBOL} | "
                f"MasterScore: {row.MASTER_SCORE} | "
                f"StopLoss: {row.STOP_LOSS} | "
            )

        return "\n".join(lines)