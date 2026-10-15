# -*- coding: utf-8 -*-
"""指标相关性实测与数据源依赖矩阵。

用途：组合打分之前必须实测指标之间的相关性——同一数据源家族内的指标
共享同一份底层数据，高相关是预期现象（家族一票制：整族只投一票）；
不同家族之间 |ρ|>0.8 才算真正的重复计数，需要合并投票权。

数据源家族（依赖矩阵；组合特征按其主导数据源族归属）：
  价差族: spd_fb
  库存族: supply_growth_7d, dd_30d
  挂单量族: dsr, obi_d
  成交族: turn_7d, val_turn_7d, amihud_7d, miss_rate_7d, ret_7d
  波动族: vol_30d, tail_ratio
  组合层: liq_contraction, absorption, distribution, crash_pressure,
          zero_vol_rise, bottom_seq, ano_score, rel_strength

运行：python tools/indicator_correlation.py [--freq 1d] [--top 15]
输出：族内/跨族 Spearman 相关（基于排序秩的相关系数）高相关对清单
（|ρ| 降序）+ 跨族重复计数判定
"""
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from csgo_monitor.config import load_settings  # noqa: E402
from csgo_monitor.storage import Storage  # noqa: E402

FAMILIES = {
    "价差族": ["spd_fb"],
    "库存族": ["supply_growth_7d"],
    "挂单量族": ["dsr", "obi_d"],
    "成交族": ["turn_7d", "val_turn_7d", "amihud_7d", "miss_rate_7d", "ret_7d"],
    "波动族": ["vol_30d", "tail_ratio"],
    # 组合特征按其主导数据源族归属（依赖矩阵规则：与成分同票）
    "成交族合成": ["liq_contraction", "crash_pressure", "ano_score",
                 "bottom_seq"],
    "跨族组合": ["absorption", "distribution", "zero_vol_rise"],
}
FEATURE_TO_FAMILY = {f: fam for fam, fs in FAMILIES.items() for f in fs}


def spearman(xs: list[float], ys: list[float]) -> float | None:
    n = len(xs)
    if n < 10:
        return None

    def rank(v: list[float]) -> list[float]:
        order = sorted(range(n), key=lambda i: v[i])
        r = [0.0] * n
        i = 0
        while i < n:
            j = i
            while j + 1 < n and v[order[j + 1]] == v[order[i]]:
                j += 1
            avg = (i + j) / 2 + 1
            for k in range(i, j + 1):
                r[order[k]] = avg
            i = j + 1
        return r

    rx, ry = rank(xs), rank(ys)
    mx, my = sum(rx) / n, sum(ry) / n
    cov = sum((rx[i] - mx) * (ry[i] - my) for i in range(n))
    vx = sum((r - mx) ** 2 for r in rx)
    vy = sum((r - my) ** 2 for r in ry)
    if vx <= 0 or vy <= 0:
        return None
    return cov / (vx * vy) ** 0.5


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--freq", default="1d")
    parser.add_argument("--top", type=int, default=15)
    args = parser.parse_args()

    settings = load_settings(str(ROOT / "config.yaml"))
    storage = Storage(settings.database, settings.output_dir / "raw",
                      settings.control_database)
    names = [f for fs in FAMILIES.values() for f in fs]
    frame = storage.read_feature_frame(names, freq=args.freq)
    if not frame:
        print("feature_store 为空——先运行: python -m csgo_monitor features")
        return 1

    # 最新截面：每饰品取最新 ts
    latest_ts: dict[str, str] = {}
    for (item, ts) in frame:
        if item not in latest_ts or ts > latest_ts[item]:
            latest_ts[item] = ts
    cross = {(i, t): v for (i, t), v in frame.items() if latest_ts.get(i) == t}
    print(f"截面: {len(cross)} 件饰品 × {len(names)} 特征")

    # 两两 Spearman
    values: dict[str, list[float]] = defaultdict(list)
    for feats in cross.values():
        for n in names:
            v = feats.get(n)
            if v is not None:
                values[n].append(v)
    available = [n for n in names if len(values[n]) >= 30]
    print("可用特征:", ", ".join(
        f"{n}({len(values[n])})" for n in available))

    results = []
    for i in range(len(available)):
        for j in range(i + 1, len(available)):
            a, b = available[i], available[j]
            xs = [feats[a] for feats in cross.values()
                  if feats.get(a) is not None and feats.get(b) is not None]
            ys = [feats[b] for feats in cross.values()
                  if feats.get(a) is not None and feats.get(b) is not None]
            rho = spearman(xs, ys)
            if rho is not None:
                same = FEATURE_TO_FAMILY[a] == FEATURE_TO_FAMILY[b]
                results.append((abs(rho), rho, a, b, same))

    results.sort(reverse=True)
    print("\n== 高相关对（|ρ|>0.6，降序）==")
    dup_cross = 0
    for absr, rho, a, b, same in results:
        if absr <= 0.6:
            break
        tag = "族内(预期)" if same else "★跨族(疑似重复计数)"
        if not same and absr > 0.8:
            dup_cross += 1
        print(f"  ρ={rho:+.3f}  {a:16s} × {b:16s}  [{tag}]")

    print("\n== 判定 ==")
    heavy = [r for r in results if not r[4] and r[0] > 0.8]
    if heavy:
        print(f"跨族 |ρ|>0.8 共 {len(heavy)} 对——按依赖矩阵判定需合并投票权：")
        for _, rho, a, b, _ in heavy:
            print(f"  - {a} × {b} (ρ={rho:+.3f})")
    else:
        print("无跨族 |ρ|>0.8 的重复计数对——当前特征集按家族一票制可用。")
    top = results[:args.top]
    print(f"\nTop-{args.top} 相关对均值 |ρ| = "
          f"{sum(r[0] for r in top)/max(1,len(top)):.3f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
