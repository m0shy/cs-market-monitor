from __future__ import annotations

import math
from collections import deque

from .storage import Storage


def _ema(values: list[float], period: int) -> list[float | None]:
    result: list[float | None] = []
    alpha = 2 / (period + 1)
    current = None
    for value in values:
        current = value if current is None else alpha * value + (1 - alpha) * current
        result.append(current)
    return result


def _rolling(values: list[float], period: int) -> tuple[list[float | None], list[float | None]]:
    means: list[float | None] = []
    deviations: list[float | None] = []
    window: deque[float] = deque(maxlen=period)
    for value in values:
        window.append(value)
        if len(window) < period:
            means.append(None)
            deviations.append(None)
            continue
        mean = sum(window) / period
        variance = sum((point - mean) ** 2 for point in window) / period
        means.append(mean)
        deviations.append(math.sqrt(variance))
    return means, deviations


def calculate_all(storage: Storage) -> int:
    with storage.connect() as conn:
        groups = conn.execute(
            """SELECT DISTINCT source,item_id,metric,platform FROM series
            WHERE close IS NOT NULL AND item_id <> '' AND dataset <> 'derived_kline_1d'
            ORDER BY source,item_id,metric,platform"""
        ).fetchall()
    total = 0
    for group in groups:
        source, item_id, metric, platform = group
        with storage.connect() as conn:
            points = conn.execute(
                """SELECT ts,close FROM series WHERE source=? AND item_id=? AND metric=? AND platform=?
                AND close IS NOT NULL ORDER BY ts""", (source, item_id, metric, platform)
            ).fetchall()
        if len(points) < 2:
            continue
        values = [float(row["close"]) for row in points]
        sma7, _ = _rolling(values, 7)
        sma30, std30 = _rolling(values, 30)
        ema12 = _ema(values, 12)
        ema26 = _ema(values, 26)
        macd = [a - b for a, b in zip(ema12, ema26)]
        signal = _ema(macd, 9)
        gains = [0.0]
        losses = [0.0]
        for previous, current in zip(values, values[1:]):
            change = current - previous
            gains.append(max(change, 0))
            losses.append(max(-change, 0))
        avg_gain, _ = _rolling(gains, 14)
        avg_loss, _ = _rolling(losses, 14)
        rows = []
        for i, point in enumerate(points):
            rsi = None
            if avg_gain[i] is not None and avg_loss[i] is not None:
                rsi = 100.0 if avg_loss[i] == 0 else 100 - (100 / (1 + avg_gain[i] / avg_loss[i]))
            ret = None if i == 0 or values[i - 1] == 0 else values[i] / values[i - 1] - 1
            mid, dev = sma30[i], std30[i]
            rows.append((source, item_id, metric, platform, point["ts"], sma7[i], sma30[i], ema12[i],
                         ema26[i], macd[i], signal[i], rsi, mid,
                         mid + 2 * dev if mid is not None and dev is not None else None,
                         mid - 2 * dev if mid is not None and dev is not None else None,
                         ret, dev / mid if mid not in (None, 0) and dev is not None else None))
        with storage.connect() as conn:
            conn.executemany(
                """INSERT INTO indicators(source,item_id,metric,platform,ts,sma7,sma30,ema12,ema26,macd,
                macd_signal,rsi14,bollinger_mid,bollinger_upper,bollinger_lower,return_1d,volatility30)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO UPDATE SET sma7=excluded.sma7,
                sma30=excluded.sma30,ema12=excluded.ema12,ema26=excluded.ema26,macd=excluded.macd,
                macd_signal=excluded.macd_signal,rsi14=excluded.rsi14,bollinger_mid=excluded.bollinger_mid,
                bollinger_upper=excluded.bollinger_upper,bollinger_lower=excluded.bollinger_lower,
                return_1d=excluded.return_1d,volatility30=excluded.volatility30""", rows)
        total += len(rows)
    return total


def derive_daily_klines(storage: Storage) -> int:
    """Aggregate captured price points into auditable daily OHLC rows."""
    with storage.connect() as conn:
        groups = conn.execute(
            """SELECT DISTINCT source,item_id,metric,platform FROM series
            WHERE close IS NOT NULL AND item_id <> '' AND metric IN ('sell_price','buy_price')"""
        ).fetchall()
    total = 0
    for source, item_id, metric, platform in groups:
        with storage.connect() as conn:
            points = conn.execute(
                """SELECT ts,close,volume FROM series WHERE source=? AND item_id=? AND metric=? AND platform=?
                AND close IS NOT NULL ORDER BY ts""", (source, item_id, metric, platform)
            ).fetchall()
        days: dict[str, list] = {}
        for point in points:
            days.setdefault(str(point["ts"])[:10], []).append(point)
        rows = []
        for day, values in days.items():
            prices = [float(value["close"]) for value in values]
            volumes = [float(value["volume"]) for value in values if value["volume"] is not None]
            rows.append({
                "source": source, "dataset": "derived_kline_1d", "item_id": item_id,
                "metric": f"{metric}_ohlc", "platform": platform, "ts": day,
                "open": prices[0], "high": max(prices), "low": min(prices), "close": prices[-1],
                "volume": sum(volumes) if volumes else None,
                "extra": {"points": len(values), "derived": True},
            })
        total += storage.upsert_series(rows)
    if total:
        with storage.connect() as conn:
            summary = conn.execute(
                "SELECT COUNT(*),MIN(ts),MAX(ts) FROM series WHERE dataset='derived_kline_1d'"
            ).fetchone()
        storage.set_coverage("derived", "daily_kline", "ok", summary[0], summary[1], summary[2],
                             "OHLC aggregated from captured intraday/daily price points")
    return total
