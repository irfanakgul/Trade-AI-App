from __future__ import annotations

import numpy as np
import pandas as pd

def detect_tick_size(df: pd.DataFrame) -> float:
    prices = pd.concat([df["OPEN"], df["HIGH"], df["LOW"], df["CLOSE"]]).dropna()
    if prices.empty:
        raise ValueError("Cannot detect tick size: prices are empty.")

    diffs = np.diff(np.sort(prices.unique()))
    diffs = diffs[diffs > 0]

    if diffs.size == 0:
        raise ValueError("Cannot detect tick size: no positive price differences.")

    return float(np.round(diffs.min(), 6))


def calculate_tv_frvp_v2(df: pd.DataFrame, value_area_pct: float = 68, row_size: int = 1) -> dict:
    """
    TradingView-compatible FRVP calculation.
    MATH LOGIC IS THE SAME. This patch only prevents float-index KeyErrors and invalid steps.
    Expected columns: OPEN, HIGH, LOW, CLOSE, VOLUME
    """
    df = df.copy()

    tick = detect_tick_size(df)
    price_step = tick * row_size

    if not np.isfinite(price_step) or price_step <= 0:
        raise ValueError(f"Invalid price_step computed: {price_step}")

    price_min = np.floor(df["LOW"].min() / price_step) * price_step
    price_max = np.ceil(df["HIGH"].max() / price_step) * price_step

    if not np.isfinite(price_min) or not np.isfinite(price_max) or price_max < price_min:
        raise ValueError("Invalid price range computed for histogram.")

    price_levels = np.arange(price_min, price_max + price_step, price_step)
    price_levels = np.round(price_levels, 6)

    hist = pd.Series(0.0, index=price_levels)

    for _, row in df.iterrows():
        low = np.floor(row["LOW"] / price_step) * price_step
        high = np.ceil(row["HIGH"] / price_step) * price_step

        levels = np.arange(low, high + price_step, price_step)
        levels = np.round(levels, 6)

        if len(levels) == 0:
            continue

        volume = row["VOLUME"]

        weights = np.linspace(1, 2, len(levels))
        weights = weights / weights.sum()

        # Ensure histogram contains all required levels (prevents KeyError due to float rounding)
        if not np.isin(levels, hist.index).all():
            hist = hist.reindex(hist.index.union(levels), fill_value=0.0)

        hist.loc[levels] += volume * weights

    max_volume = hist.max()
    poc_candidates = hist[hist == max_volume].index.values
    poc = float(poc_candidates[len(poc_candidates) // 2])

    total_volume = hist.sum()
    target_volume = total_volume * (value_area_pct / 100)

    sorted_prices = hist.sort_index().index.values
    poc_pos = np.where(sorted_prices == poc)[0][0]

    included = {poc}
    cum_volume = hist.loc[poc]

    lower = poc_pos - 1
    upper = poc_pos + 1

    while cum_volume < target_volume:
        vol_down = hist.iloc[lower] if lower >= 0 else -1
        vol_up = hist.iloc[upper] if upper < len(hist) else -1

        if vol_up > vol_down:
            cum_volume += vol_up
            included.add(sorted_prices[upper])
            upper += 1
        elif vol_down > vol_up:
            cum_volume += vol_down
            included.add(sorted_prices[lower])
            lower -= 1
        else:
            if lower >= 0 and upper < len(hist):
                dist_down = abs(sorted_prices[lower] - poc)
                dist_up = abs(sorted_prices[upper] - poc)

                if dist_up <= dist_down:
                    cum_volume += vol_up
                    included.add(sorted_prices[upper])
                    upper += 1
                else:
                    cum_volume += vol_down
                    included.add(sorted_prices[lower])
                    lower -= 1
            elif upper < len(hist):
                cum_volume += vol_up
                included.add(sorted_prices[upper])
                upper += 1
            elif lower >= 0:
                cum_volume += vol_down
                included.add(sorted_prices[lower])
                lower -= 1
            else:
                break

    VAL = float(min(included))
    VAH = float(max(included))

    return {"POC": round(poc, 6), "VAL": round(VAL, 6), "VAH": round(VAH, 6)}