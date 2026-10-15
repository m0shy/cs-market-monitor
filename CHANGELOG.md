# Changelog

本项目的全部显著变更记录在此。格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 [SemVer](https://semver.org/lang/zh-CN/)。

## [0.1.0] - 2026-10-15

### Added
- 首个开源版本（框架层）：lane/租约任务调度、SQLite 时序存储、指标与费用计算、FastAPI + ECharts 仪表盘。
- 站点适配器以桩形式保留接口边界（合规考虑），数据与密钥不随仓库分发。
- 测试 99 项（另有 2 项与已移除适配器耦合，标记跳过）。
