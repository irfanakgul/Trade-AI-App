from __future__ import annotations

import numpy as np
import pandas as pd


def detect_tick_size(df: pd.DataFrame) -> float:
    prices = pd.concat([df["OPEN"], df["HIGH"], df["LOW"], df["CLOSE"]], ignore_index=True)
    uniq = np.sort(prices.dropna().unique())
    if uniq.size < 2:
        return 0.01
    diffs = np.diff(uniq)
    diffs = diffs[diffs > 0]
    if diffs.size == 0:
        return 0.01
    return float(np.round(diffs.min(), 6))


def calculate_tv_frvp_v2_fast(df: pd.DataFrame, value_area_pct: float = 68, row_size: int = 1) -> dict:
    """
    Same TradingView-compatible logic as calculate_tv_frvp_v2, but optimized with numpy arrays.
    Do NOT change business logic; only data structure/ops are optimized.
    """
    df = df.copy()

    # Ensure numeric
    for c in ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["HIGH", "LOW", "VOLUME"])
    if df.empty:
        raise ValueError("Empty dataframe after cleaning.")

    tick = detect_tick_size(df)
    price_step = tick * row_size
    if not np.isfinite(price_step) or price_step <= 0:
        raise ValueError("Invalid price_step computed.")

    price_min = np.floor(df["LOW"].min() / price_step) * price_step
    price_max = np.ceil(df["HIGH"].max() / price_step) * price_step

    n_bins = int(np.floor((price_max - price_min) / price_step)) + 1
    if n_bins <= 1:
        poc = float(np.round(price_min, 6))
        return {"POC": poc, "VAL": poc, "VAH": poc}

    hist = np.zeros(n_bins, dtype=np.float64)

    lows = df["LOW"].to_numpy(np.float64)
    highs = df["HIGH"].to_numpy(np.float64)
    vols = df["VOLUME"].to_numpy(np.float64)

    for low_v, high_v, vol in zip(lows, highs, vols):
        low = np.floor(low_v / price_step) * price_step
        high = np.ceil(high_v / price_step) * price_step

        low_idx = int(np.round((low - price_min) / price_step))
        high_idx = int(np.round((high - price_min) / price_step))

        if high_idx < low_idx:
            continue

        if low_idx < 0:
            low_idx = 0
        if high_idx >= n_bins:
            high_idx = n_bins - 1

        length = high_idx - low_idx + 1
        if length <= 0:
            continue

        weights = np.linspace(1, 2, length, dtype=np.float64)
        weights /= weights.sum()

        hist[low_idx : high_idx + 1] += vol * weights

    max_volume = hist.max()
    poc_candidates = np.where(hist == max_volume)[0]
    poc_idx = int(poc_candidates[len(poc_candidates) // 2])
    poc_price = price_min + poc_idx * price_step
    poc = float(np.round(poc_price, 6))

    total_volume = hist.sum()
    target_volume = total_volume * (value_area_pct / 100.0)

    included = {poc_idx}
    cum_volume = hist[poc_idx]

    lower = poc_idx - 1
    upper = poc_idx + 1

    while cum_volume < target_volume:
        vol_down = hist[lower] if lower >= 0 else -1
        vol_up = hist[upper] if upper < n_bins else -1

        if vol_up > vol_down:
            cum_volume += vol_up
            included.add(upper)
            upper += 1

        elif vol_down > vol_up:
            cum_volume += vol_down
            included.add(lower)
            lower -= 1

        else:
            if lower >= 0 and upper < n_bins:
                dist_down = abs((price_min + lower * price_step) - poc_price)
                dist_up = abs((price_min + upper * price_step) - poc_price)

                if dist_up <= dist_down:
                    cum_volume += vol_up
                    included.add(upper)
                    upper += 1
                else:
                    cum_volume += vol_down
                    included.add(lower)
                    lower -= 1

            elif upper < n_bins:
                cum_volume += vol_up
                included.add(upper)
                upper += 1

            elif lower >= 0:
                cum_volume += vol_down
                included.add(lower)
                lower -= 1
            else:
                break

    val_idx = int(min(included))
    vah_idx = int(max(included))

    VAL = float(np.round(price_min + val_idx * price_step, 6))
    VAH = float(np.round(price_min + vah_idx * price_step, 6))

    return {"POC": poc, "VAL": VAL, "VAH": VAH}