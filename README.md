# cs-market-monitor

[![CI](https://github.com/m0shy/cs-market-monitor/actions/workflows/ci.yml/badge.svg)](https://github.com/m0shy/cs-market-monitor/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

[简体中文](README.md) · [English](README_ENG.md)

CS2 饰品市场的数据工程框架：采集调度、时序存储、指标计算、可视化仪表盘。为**单机长期无人值守运行**设计——自研 supervisor 支持任务分片（lane）、租约、安静时段、心跳与崩溃恢复。

> **开源版边界说明**：本仓库只发布框架层。所有针对具体站点的采集适配器（API / 页面 / WebSocket 抓取）与代理配置出于合规考虑已移除，保留同名桩模块（调用即抛 `NotImplementedError`，见 `src/csgo_monitor/` 下 `siteb_*.py`、`sitea_api.py`、`browser.py`、`page_harvest.py` 的文件头说明）。仓库**不含任何采集数据**、密钥与代理配置。

## 这套框架实际跑过什么

在作者的机器上以这套框架完成了长期采集运行，规模供参考（数据本身不随仓库发布）：

- 17.3 万个饰品条目、**2.22 亿个时序数据点**、224 万条原始响应
- 覆盖 16 个价格平台的 K 线 / 挂单 / 成交 / 流通盘
- SQLite 单库长期运行，含 WAL、pit 快照、对账（reconcile）与恢复（recover）机制

> 条目口径说明：17.3 万为 16 个平台归并存储后的饰品条目总量（含跨平台重复）；`src/csgo_monitor/supervisor.py` 注释中的"约 4.4 万"为 行情站B 单站目录条目数，两者口径不同。

## 架构

```
supervisor            # lane 分片 / 租约 / 安静时段 / 心跳 / 恢复
   │
adapters (stubbed)    # 站点适配器——开源版为桩，保留接口边界
   │
normalize / ingest / cross_validate / incremental
   │
storage               # SQLite：时序点表、K线、排行快照、大盘指数
   │
features / indicators / fees / skin_indicators / sentiment_cycle
   │
web                   # FastAPI + Jinja2 + ECharts 仪表盘（:8800）
```

## 快速开始

```bash
pip install -e .
cp config.example.yaml config.yaml    # 按注释填入你自己的数据源配置
python -m csgo_monitor --help         # CLI：采集调度 / 导出 / 对账 / 报告
python -m csgo_monitor web            # 仪表盘 http://127.0.0.1:8800
pytest tests/                         # 99 passed, 2 skipped（跳过项与开源版移除的站点适配器耦合）
```

## 目录导览

| 路径 | 内容 |
|---|---|
| `src/csgo_monitor/supervisor.py` | 调度核心：lane、租约、安静时段 |
| `src/csgo_monitor/storage.py` | SQLite 存储层 |
| `src/csgo_monitor/features.py` `indicators.py` `fees.py` | 指标与费用计算 |
| `src/csgo_monitor/cli.py` | 1800+ 行统一 CLI |
| `web/` | FastAPI + ECharts 仪表盘（routes / templates / static） |
| `tools/` | 离线研究脚本（指标相关性矩阵等） |
| `tests/` | 9 个测试文件（核心、特性、费用、HTTP、对账等） |
| `docs/cs-market-indicators-v2.md` | 指标体系设计文档 |
| `docs/web-renovation.md` | Web 工作台改造说明 |
| `docs/security.md` | 采集侧安全加固记录（已脱敏） |

## License

MIT
