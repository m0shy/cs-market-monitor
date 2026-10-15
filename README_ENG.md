# cs-market-monitor

A data-engineering framework for CS2 skin markets: lane/lease task supervision, SQLite time-series storage (220M+ points collected in production), indicator computation, and a FastAPI + ECharts dashboard.

> **Open-source boundary**: this repository ships the framework layer only. Site-specific collectors are stubbed (they raise `NotImplementedError`) for compliance reasons; no data, keys, or proxy configs are included.

## Quick start

```bash
pip install -e . pytest
cp config.example.yaml config.yaml   # fill in your own data sources
python -m csgo_monitor --help        # CLI: scheduling / export / reconcile
python -m csgo_monitor web           # dashboard at http://127.0.0.1:8800
python -m pytest tests/ -q           # 99 passed, 2 skipped
```

## Layout

| Path | Contents |
|---|---|
| `src/csgo_monitor/supervisor.py` | Task supervision: lanes, leases, quiet hours |
| `src/csgo_monitor/storage.py` | SQLite storage layer |
| `src/csgo_monitor/features.py` `indicators.py` `fees.py` | Indicator & fee computation |
| `web/` | FastAPI + ECharts dashboard |
| `docs/cs-market-indicators-v2.md` | Indicator research (Chinese) |

Docs are written in Chinese; an English translation is welcome as a PR.

## License

MIT — for research and educational use; verify the terms of any data source you connect.
