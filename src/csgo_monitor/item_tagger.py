# -*- coding: utf-8 -*-
"""饰品自动标签引擎：品类/磨损/稀有度/StatTrak/纪念品/规则暴露度。

基于名称模式匹配与原始数据字段，为全部饰品建立结构化标签体系。
标签存入 item_tags 表，供后续分析（品类配置评分、KOL 推荐后验等）使用。

运行：python -m csgo_monitor tag-items
"""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any

from .storage import Storage

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 标签规则（名称模式匹配）
# ---------------------------------------------------------------------------

KNIFE_PATTERNS = [
    r"(?:^|\s|/)(?:Knife|Karambit|M9 Bayonet|Bayonet|Butterfly|Navaja|Stiletto|"
    r"Talon|Skeleton|Paracord|Classic|Flip|Huntsman|Bowie|Shadow Daggers|Ursus|"
    r"Nomad|Survival|Freehand|Gut|Flip Knife|Kukri)(?:\s|\||\(|$)",
]
GLOVE_PATTERNS = [
    r"(?:^|\s|/)(?:Gloves|Hand Wraps|Bloodhound Gloves|Sport Gloves|"
    r"Driver Gloves|Specialist Gloves|Hydra Gloves|Broken Fang Gloves|"
    r"Moto Gloves|Wraps)(?:\s|\||\(|$)",
]
CASE_PATTERNS = [
    r"(?:Case|Container|Package|Prisma|Clutch|Danger Zone|Revolution|"
    r"Dreams & Nightmares|Recoil|Kilowatt)(?:\s|$)",
    r"(?:箱|盒子)$",
]
AGENT_PATTERNS = [
    r"(?:^|\|)(?:Agent|探员|圆桌|特工)(?:\s|\||$)",
]
STICKER_PREFIX = "Sticker |"
PATCH_PREFIX = "Patch |"
MUSIC_KIT = "Music Kit"

WEAR_MAP = {
    "Factory New": "fn",
    "Minimal Wear": "mw",
    "Field-Tested": "ft",
    "Well-Worn": "ww",
    "Battle-Scarred": "bs",
}

STATTRAK = "StatTrak™"
SOUVENIR = "Souvenir"

# 武器名（用于 weapon 品类标签）
WEAPON_NAMES = [
    "AK-47", "M4A4", "M4A1-S", "AWP", "Desert Eagle", "USP-S", "Glock-18",
    "MP9", "MAC-10", "MP7", "UMP-45", "P90", "Galil AR", "FAMAS", "SG 553",
    "AUG", "SSG 08", "SCAR-20", "G3SG1", "Nova", "XM1014", "MAG-7", "M249",
    "Negev", "P2000", "P250", "Five-SeveN", "Tec-9", "CZ75-Auto", "Dual Berettas",
    "R8 Revolver", "Zeus x27", "MP5-SD", "Knife", "Gloves",
]

RARITY_MAP = {
    "covert": "covert",
    "classified": "classified",
    "restricted": "restricted",
    "milspec": "milspec",
    "industrial": "industrial",
    "consumer": "consumer",
    "base": "base_grade",
    "exotic": "exotic",
    "extraordinary": "extraordinary",
    "superior": "superior",
    "distinguished": "distinguished",
}


def _matches(name: str, patterns: list[str]) -> bool:
    return any(re.search(p, name, re.IGNORECASE) for p in patterns)


def _extract_tags(name: str, raw_json: str | None = None) -> list[tuple[str, str, float]]:
    """从名称和原始数据提取标签列表 [(tag_type, tag_value, confidence)]。"""
    tags: list[tuple[str, str, float]] = []

    # 品类
    if name.startswith(STICKER_PREFIX):
        tags.append(("category", "sticker", 1.0))
    elif name.startswith(PATCH_PREFIX):
        tags.append(("category", "patch", 1.0))
    elif MUSIC_KIT in name:
        tags.append(("category", "music_kit", 1.0))
    elif _matches(name, KNIFE_PATTERNS):
        tags.append(("category", "knife", 0.95))
    elif _matches(name, GLOVE_PATTERNS):
        tags.append(("category", "glove", 0.95))
    elif _matches(name, CASE_PATTERNS):
        tags.append(("category", "case", 0.90))
    elif _matches(name, AGENT_PATTERNS):
        tags.append(("category", "agent", 0.90))
    else:
        for w in WEAPON_NAMES:
            if w in name:
                tags.append(("category", "weapon", 0.90))
                tags.append(("weapon_family", w.lower().replace(" ", "_"), 1.0))
                break
        else:
            tags.append(("category", "other", 0.50))

    # 磨损档位
    for full, short in WEAR_MAP.items():
        if f"({full})" in name:
            tags.append(("wear", short, 1.0))
            break

    # StatTrak / 纪念品
    if STATTRAK in name:
        tags.append(("special", "stattrak", 1.0))
    if SOUVENIR in name:
        tags.append(("special", "souvenir", 1.0))

    # 从 raw_json 提取稀有度
    if raw_json:
        try:
            import json
            raw = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
            if isinstance(raw, dict):
                rarity = str(raw.get("rarityName") or
                            raw.get("rarity") or "").lower().strip()
                if rarity in RARITY_MAP:
                    tags.append(("rarity", RARITY_MAP[rarity], 1.0))
                quality = str(raw.get("qualityName") or "").lower()
                if "违禁" in quality or "contraband" in quality:
                    tags.append(("rarity", "contraband", 1.0))
        except (ValueError, TypeError):
            pass

    # 规则暴露度（基于品类的机制映射）
    cat = next((v for t, v, _ in tags if t == "category"), "")
    if cat in ("covert", "classified") or any(
            v in ("covert", "classified") for t, v, _ in tags if t == "rarity"):
        tags.append(("rule_exposure", "tradeup_material", 1.0))
    if cat == "knife":
        tags.append(("rule_exposure", "knife_product", 1.0))
    if cat == "sticker":
        tags.append(("rule_exposure", "capsule_dependent", 0.80))

    return tags


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------

def run_tag_items(storage: Storage, *, batch_size: int = 5000) -> dict[str, Any]:
    """为全部 siteb 饰品建立自动标签。"""
    now = datetime.now(timezone.utc).isoformat()

    # 确保 item_tags 表存在
    with storage.connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS item_tags (
                item_id TEXT NOT NULL,
                tag_type TEXT NOT NULL,
                tag_value TEXT NOT NULL,
                confidence REAL DEFAULT 1.0,
                source TEXT DEFAULT 'auto',
                created_at TEXT NOT NULL,
                PRIMARY KEY(item_id, tag_type, tag_value)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_item_tags_type_value
            ON item_tags(tag_type, tag_value)
        """)
        conn.commit()

    # 分批读取 items 并打标签
    total_tagged = 0
    total_rows = 0
    with storage.read_connect() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("""
            SELECT item_id, market_hash_name, raw_json
            FROM items WHERE source='siteb'
              AND market_hash_name IS NOT NULL AND market_hash_name != ''
            ORDER BY item_id
        """)
        batch: list[tuple[str, str, str | None]] = []
        for row in cursor:
            batch.append((str(row["item_id"]), str(row["market_hash_name"]),
                         row["raw_json"]))
            if len(batch) >= batch_size:
                total_tagged += _tag_batch(storage, batch, now)
                total_rows += len(batch)
                batch = []
        if batch:
            total_tagged += _tag_batch(storage, batch, now)
            total_rows += len(batch)

    log.info("item tagging: %d items -> %d tags", total_rows, total_tagged)
    return {
        "status": "ok",
        "error_class": "",
        "metrics": {"items_tagged": total_rows, "tags_created": total_tagged},
        "details": {},
    }


def _tag_batch(storage: Storage, batch: list[tuple[str, str, str | None]],
               now: str) -> int:
    """为一批饰品写入标签。"""
    rows = []
    for item_id, name, raw_json in batch:
        for tag_type, tag_value, confidence in _extract_tags(name, raw_json):
            rows.append({
                "item_id": item_id,
                "tag_type": tag_type,
                "tag_value": tag_value,
                "confidence": confidence,
                "source": "auto",
                "created_at": now,
            })
    if not rows:
        return 0
    # 分块插入
    written = 0
    chunk = 2000
    for i in range(0, len(rows), chunk):
        chunk_rows = rows[i:i + chunk]

        def _write(chunk_rows=chunk_rows):
            with storage.connect() as conn:
                conn.executemany(
                    """INSERT OR IGNORE INTO item_tags
                    (item_id, tag_type, tag_value, confidence, source, created_at)
                    VALUES(?,?,?,?,?,?)""",
                    [(r["item_id"], r["tag_type"], r["tag_value"],
                      r["confidence"], r["source"], r["created_at"])
                     for r in chunk_rows],
                )
            return len(chunk_rows)

        from .storage import _with_sqlite_retries
        _with_sqlite_retries(_write)
        written += len(chunk_rows)
    return written
