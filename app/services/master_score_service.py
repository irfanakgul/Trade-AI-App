from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional, List

import numpy as np
import pandas as pd

from app.infrastructure.database.repository import PostgresRepository
from app.services.telegram_bot_chat_service import telegram_send_message  # type: ignore


@dataclass
class MasterScoreWeights:
    poc_frvp: float = 0.65
    vwap: float = 0.02
    ema: float = 0.09
    rsi: float = 0.07
    mfi: float = 0.07
    volume: float = 0.10


@dataclass
class MasterScoreThresholds:
    buy_min: float = 70.0
    watch_min: float = 50.0
    poc_cluster_max_spread: float = 0.03
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
class TelegramConfig:
    send: bool = False
    title: str = "TOP 10"


@dataclass
class LogConfig:
    schema: str = "logs"
    table: str = "log_all_evaluation"
    append_only: bool = True


@dataclass
class MasterScoreServiceConfig:
    exchange: str = "BIST"
    input_schema: str = "gold"
    input_table: str = "bist_master_combined_indicators"
    output_schema: str = "gold"
    output_table: str = "bist_evaluation_master_score"
    truncate_before_load: bool = True
    created_at: Optional[datetime] = None
    weights: MasterScoreWeights = field(default_factory=MasterScoreWeights)
    thresholds: MasterScoreThresholds = field(default_factory=MasterScoreThresholds)
    trade: TradeLevelConfig = field(default_factory=TradeLevelConfig)
    telegram: TelegramConfig = field(default_factory=TelegramConfig)
    log: LogConfig = field(default_factory=LogConfig)


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
        entry_markup_perc: Optional[float] = None,
        stop_loss_perc: Optional[float] = None,
        buy_min: Optional[float] = None,
        watch_min: Optional[float] = None,
        poc_cluster_max_spread: Optional[float] = None,
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
            entry_markup_perc=entry_markup_perc,
            stop_loss_perc=stop_loss_perc,
            buy_min=buy_min,
            watch_min=watch_min,
            poc_cluster_max_spread=poc_cluster_max_spread,
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
        output_df = self._format_output_columns(triage_df)

        self.repo.insert_dataframe(
            df=output_df,
            schema_name=params["output_schema"],
            table_name=params["output_table"],
        )

        log_df = self._build_log_dataframe(output_df, params)
        if not log_df.empty:
            self.repo.insert_dataframe(
                df=log_df,
                schema_name=params["log_schema"],
                table_name=params["log_table"],
            )

        telegram_text = self._build_telegram_text(output_df, params["exchange"])
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
        lg = self.cfg.log

        return {
            "exchange": kwargs["exchange"] if kwargs["exchange"] is not None else self.cfg.exchange,
            "input_schema": kwargs["input_schema"] if kwargs["input_schema"] is not None else self.cfg.input_schema,
            "input_table": kwargs["input_table"] if kwargs["input_table"] is not None else self.cfg.input_table,
            "output_schema": kwargs["output_schema"] if kwargs["output_schema"] is not None else self.cfg.output_schema,
            "output_table": kwargs["output_table"] if kwargs["output_table"] is not None else self.cfg.output_table,
            "is_truncate_scope": kwargs["is_truncate_scope"] if kwargs["is_truncate_scope"] is not None else self.cfg.truncate_before_load,
            "created_at": kwargs["created_at"] if kwargs["created_at"] is not None else (self.cfg.created_at or datetime.now()),

            "top_n": kwargs["top_n"] if kwargs["top_n"] is not None else tr.top_n,
            "entry_markup_perc": kwargs["entry_markup_perc"] if kwargs["entry_markup_perc"] is not None else tr.entry_markup_perc,
            "stop_loss_perc": kwargs["stop_loss_perc"] if kwargs["stop_loss_perc"] is not None else tr.stop_loss_perc,

            "buy_min": kwargs["buy_min"] if kwargs["buy_min"] is not None else t.buy_min,
            "watch_min": kwargs["watch_min"] if kwargs["watch_min"] is not None else t.watch_min,
            "poc_cluster_max_spread": kwargs["poc_cluster_max_spread"] if kwargs["poc_cluster_max_spread"] is not None else t.poc_cluster_max_spread,
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

            "log_schema": kwargs["log_schema"] if kwargs["log_schema"] is not None else lg.schema,
            "log_table": kwargs["log_table"] if kwargs["log_table"] is not None else lg.table,
        }

    @staticmethod
    def _to_float(col: pd.Series) -> pd.Series:
        return (
            col.astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
            .replace({"nan": np.nan, "None": np.nan, "NaN": np.nan, "": np.nan})
            .astype(float)
        )

    def _prepare_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        direct_numeric_cols = [
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
            "EMA_DAYS_SINCE_CROSS_5_20",
            "EMA_DAYS_SINCE_CROSS_3_20",
            "EMA_DAYS_SINCE_CROSS_3_14",
            "RSI_STATUS",
            "EMA_STATUS_5_20",
            "EMA_CROSS_5_20",
            "EMA_STATUS_3_20",
            "EMA_CROSS_3_20",
            "EMA_STATUS_3_14",
            "EMA_CROSS_3_14",
        ]

        for col in direct_numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    def _calc_poc_cluster_score(self, df: pd.DataFrame, params: Dict) -> pd.Series:
        max_spread = params["poc_cluster_max_spread"]

        def cluster(group: pd.DataFrame):
            n = group["FRVP_HIGHEST_DATE"].nunique()
            vals = group["FRVP_POC"].values

            if n == 1:
                return 3

            spread = (vals.max() - vals.min()) / vals.min()

            if n >= 3:
                if spread == 0:
                    return 10
                elif spread <= max_spread:
                    return 3 + (1 - spread / max_spread) * 7
                else:
                    return 3

            if n == 2:
                if spread == 0:
                    return 7
                elif spread <= max_spread:
                    return 3 + (1 - spread / max_spread) * 4
                else:
                    return 3

            return 0

        cluster_scores = (
            df.groupby(["EXCHANGE", "SYMBOL"])
            .apply(cluster)
            .rename("cluster_score")
            .reset_index()
        )

        merged = df.merge(cluster_scores, on=["EXCHANGE", "SYMBOL"], how="left")
        return merged["cluster_score"]

    def _calc_mfi_new(self, df: pd.DataFrame) -> np.ndarray:
        return (
            np.where(df["MFI"] > df["MFI_YESTERDAY"], 25, 0) +
            np.where(df["MFI"] > df["MFI_12DAY_AVG"], 50, 0) +
            np.where(df["MFI_DIRECTION"] == "Upward", 25, 0)
        )

    def _calc_rsi_new(self, df: pd.DataFrame, params: Dict) -> np.ndarray:
        rsi = self._to_float(df["RSI"])
        rsi_ma = pd.to_numeric(df["RSI_MA"], errors="coerce")

        score1 = np.where(rsi > rsi_ma, 70, 0)

        days = df["RSI_CROSS_DAYS_AGO"]
        score2 = np.where(
            (df["RSI_STATUS"] == 1) & (days <= params["rsi_cross_max_days"]),
            np.maximum(0, 15 - days),
            0
        )

        score3 = np.where((rsi >= 30) & (rsi <= 70), 15, 0)

        return np.clip(score1 + score2 + score3, 0, 100)

    def _calc_ema_new(self, df: pd.DataFrame) -> np.ndarray:
        def ema_score(status, cross, days):
            return np.where(
                (status == 1) & (cross == 1),
                np.maximum(0, 3 - (days / 10)),
                0
            )

        total = (
            ema_score(df["EMA_STATUS_5_20"], df["EMA_CROSS_5_20"], df["EMA_DAYS_SINCE_CROSS_5_20"]) +
            ema_score(df["EMA_STATUS_3_20"], df["EMA_CROSS_3_20"], df["EMA_DAYS_SINCE_CROSS_3_20"]) +
            ema_score(df["EMA_STATUS_3_14"], df["EMA_CROSS_3_14"], df["EMA_DAYS_SINCE_CROSS_3_14"])
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
        close = df["FRVP_LATEST_CLOSE_VALUE"]
        vwap = df["VWAP"]

        dist = np.abs(close - vwap) / vwap

        near_threshold = params["vwap_near_threshold"]
        far_threshold = params["vwap_far_threshold"]

        return np.clip(
            np.where(
                dist <= near_threshold, 0,
                np.where(
                    dist >= far_threshold, 100,
                    ((dist - near_threshold) / (far_threshold - near_threshold)) * 100
                )
            ),
            0, 100
        )

    def _calc_poc_frvp_new(self, df: pd.DataFrame, params: Dict) -> np.ndarray:
        close = df["FRVP_LATEST_CLOSE_VALUE"]
        poc = df["FRVP_POC"]
        val = df["FRVP_VAL"]
        vah = df["FRVP_VAH"]

        score1 = np.clip(
            (1 - ((close - poc) / poc) / params["poc_close_distance_limit"]) * 5,
            0, 5
        )

        score2 = self._calc_poc_cluster_score(df, params)

        score3 = np.where(df["BS_BAR_STATUS"] == "GREEN", 5, 0)

        score4 = np.where(
            (df["BS_OPEN_PRICE"] > poc) &
            (df["BS_BAR_STATUS"] == "GREEN"),
            5,
            0
        )

        score5 = np.clip(
            (1 - np.abs(poc - val) / val / params["poc_val_distance_limit"]) * 5,
            0, 5
        )

        dist_vah = np.abs(poc - vah) / vah
        score6 = np.where(
            dist_vah >= params["poc_vah_far_threshold"], 5,
            np.where(
                dist_vah <= params["poc_vah_near_threshold"], 0,
                (
                    (dist_vah - params["poc_vah_near_threshold"])
                    / (params["poc_vah_far_threshold"] - params["poc_vah_near_threshold"])
                ) * 5
            )
        )

        total = score1 + score2 + score3 + score4 + score5 + score6

        return np.clip((total / 35) * 100, 0, 100)

    def _calculate_trade_levels(self, df: pd.DataFrame, params: Dict) -> pd.DataFrame:
        df = df.copy()

        close = df["FRVP_LATEST_CLOSE_VALUE"]
        df["entry_price"] = close * (1 + (params["entry_markup_perc"] / 100.0))

        pivot = df["PIVOT"] if "PIVOT" in df.columns else pd.Series(np.nan, index=df.index)
        r2 = df["R2"] if "R2" in df.columns else pd.Series(np.nan, index=df.index)

        df["stop_loss"] = df["entry_price"] * (1 - (params["stop_loss_perc"] / 100.0))

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
        df["rr_ratio"] = np.where(df["risk_pct"] != 0, df["target_pct"] / df["risk_pct"], 0)

        df["pivot_display"] = np.where(pivot.isna(), "N/A", pivot)

        return df

    def _calculate_scores(self, df: pd.DataFrame, params: Dict) -> pd.DataFrame:
        df = df.copy()

        df["poc_frvp_status"] = self._calc_poc_frvp_new(df, params)
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

        df = self._calculate_trade_levels(df, params)

        return df

    def _calculate_triage_selection(self, df: pd.DataFrame, params: Dict) -> pd.DataFrame:
        df_filtered = df[
            (df["BS_CLOSE_PRICE"] > df["FRVP_POC"]) &
            (df["BS_BAR_STATUS"] == "GREEN")
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

            pocs = g_unique["FRVP_POC"].values
            if len(pocs) > 1:
                spread = (max(pocs) - min(pocs)) / min(pocs)
            else:
                spread = 999

            if spread <= 0.02:
                if count >= 4:
                    final_score = avg_score * 1.20
                elif count == 3:
                    final_score = avg_score * 1.10
                elif count == 2:
                    final_score = avg_score * 1.05
                else:
                    final_score = avg_score
            else:
                final_score = avg_score

            poc_counts = g["FRVP_POC"].value_counts()
            max_freq = poc_counts.max()
            dominant_pocs = poc_counts[poc_counts == max_freq].index
            avg_poc = np.mean(dominant_pocs)

            avg_entry = g["entry_price"].mean()
            avg_stop = g["stop_loss"].mean()
            avg_target = g["target_price"].mean()

            if avg_entry != 0:
                risk_pct = ((avg_entry - avg_stop) / avg_entry) * 100
                target_pct = ((avg_target - avg_entry) / avg_entry) * 100
            else:
                risk_pct = 0
                target_pct = 0

            triage_day = pd.to_datetime(
                g["CREATED_DAY"].iloc[0],
                dayfirst=True
            ).strftime("%Y-%m-%d") if "CREATED_DAY" in g.columns else created_at.strftime("%Y-%m-%d")

            results.append({
                "EXCHANGE": exchange,
                "SYMBOL": symbol,
                "TRIAGE_ENTRY_DAY": triage_day,
                "MASTER_SCORE": round(min(100, final_score), 2),
                "VALID_CLUSTER_COUNT": count,
                "AVG_POC": f"{avg_poc:.2f}".replace(".", ","),
                "ENTRY_PRICE": f"{avg_entry:.2f}".replace(".", ","),
                "STOP_LOSS": f"{avg_stop:.2f}".replace(".", ","),
                "TARGET_PRICE": f"{avg_target:.2f}".replace(".", ","),
                "STOP_LOSS_PERC": f"-{risk_pct:.2f}%".replace(".", ","),
                "TARGET_PERC": f"+{target_pct:.2f}%".replace(".", ","),
                "CREATED_DAY": created_day,
                "CREATED_AT": created_at,
            })

        result_df = pd.DataFrame(results)
        result_df = result_df.sort_values("MASTER_SCORE", ascending=False)

        return result_df.head(params["top_n"])

    def _format_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        ordered_cols = [
            "EXCHANGE",
            "SYMBOL",
            "TRIAGE_ENTRY_DAY",
            "MASTER_SCORE",
            "VALID_CLUSTER_COUNT",
            "AVG_POC",
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

    def _build_log_dataframe(self, df: pd.DataFrame, params: Dict) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()

        log_df = df.copy()
        log_df["SOURCE_TABLE"] = f'{params["input_schema"]}.{params["input_table"]}'
        log_df["TARGET_TABLE"] = f'{params["output_schema"]}.{params["output_table"]}'
        log_df["LOG_CREATED_AT"] = params["created_at"]

        ordered_cols = [
            "EXCHANGE",
            "SYMBOL",
            "TRIAGE_ENTRY_DAY",
            "MASTER_SCORE",
            "VALID_CLUSTER_COUNT",
            "AVG_POC",
            "ENTRY_PRICE",
            "STOP_LOSS",
            "TARGET_PRICE",
            "STOP_LOSS_PERC",
            "TARGET_PERC",
            "CREATED_DAY",
            "CREATED_AT",
            "SOURCE_TABLE",
            "TARGET_TABLE",
            "LOG_CREATED_AT",
        ]

        existing = [c for c in ordered_cols if c in log_df.columns]
        remaining = [c for c in log_df.columns if c not in existing]
        return log_df[existing + remaining]

    def _build_telegram_text(self, df: pd.DataFrame, exchange: str) -> str:
        if df.empty:
            return f"[MASTER-SCORE] {exchange} | No candidates found."

        lines: List[str] = []

        for idx, row in enumerate(df.itertuples(index=False), start=1):
            stop_pct_num = str(row.STOP_LOSS_PERC).replace("%", "")
            target_pct_num = str(row.TARGET_PERC).replace("%", "")

            lines.append(
                f"{idx}) {row.SYMBOL} | "
                f"MasterScore: {row.MASTER_SCORE} | "
                f"StopLoss: {row.STOP_LOSS} "
            )

        return "\n".join(lines)