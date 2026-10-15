# 贡献指南

## 开发环境

```bash
pip install -e . pytest
```

## 运行测试

```bash
python -m pytest tests/ -q
```

开源版的站点适配器是桩（调用即抛 NotImplementedError）；与适配器耦合的测试标记了 skip。为框架层（调度/存储/指标/Web）贡献时，请保持测试全绿并新增对应用例。

## 约定

- 不引入任何真实密钥与采集数据（`config.yaml`、`data/` 已被 .gitignore 排除）；
- 指标类改动请在 docs/cs-market-indicators-v2.md 补充依据；
- PR 描述写清动机与验证方式。
