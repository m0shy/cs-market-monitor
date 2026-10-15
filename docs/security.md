# 安全与 IP 泄露防护

> 更新日期：2026-10-15。本文件记录采集器的泄露面、我已实施的防护与验证方法。

## 1. 泄露面清单

| 向量 | 风险 | 状态 |
| --- | --- | --- |
| 直连 egress IP | 所有 HTTP/HTTPS 请求直接使用本机公网 IP，可被站点日志关联 | 已支持代理（配置后全部流量走代理） |
| WebRTC STUN | 页面 JS 可调用 `RTCPeerConnection` + STUN（浏览器探测自身 NAT 映射地址的协议），srflx 候选（经 NAT 映射后对外呈现的地址）会暴露 NAT 后的真实公网 IP（与出口 IP 一致），即使走代理也可能绕过 | 已修复：`--force-webrtc-ip-handling-policy=disable_non_proxied_udp` 无条件启用 |
| `navigator.webdriver` | 自动化标记，增大风控概率 | 已伪造为 `false` |
| `navigator.languages` | 早期版本占位符替换写错脚本，页面调用会抛 `ReferenceError: __NAVIGATOR_LANGUAGES__ is not defined` | 已修复并单测覆盖 |
| 指纹一致性 | UA / 语言 / 时区 / 插件 / WebGL / 并发数 这类环境特征（合称浏览器指纹）不匹配会暴露自动化 | 已随机化并保持 Chromium 引擎一致 |
| 代理环境变量 | `requests` 默认读取系统代理，可能意外走代理或泄露 | `PoliteClient` 设 `trust_env=False`，仅使用显式配置 |

## 2. 一键安全自检

```powershell
python -m csgo_monitor security-check                 # 直连基线
python -m csgo_monitor --proxy http://127.0.0.1:7890 security-check
```

输出为一份 JSON 检查报告，包含：

- `fingerprint`：`navigator.webdriver`、`languages`、`plugins`、`hardwareConcurrency`、`deviceMemory`
- `webrtc_candidates`：页面内 `RTCPeerConnection` + Google STUN 收集到的全部 ICE 候选（建立连接时枚举出的可达地址）
- `egress`：直连 / 代理出口 IP（`api.ipify.org` + Cloudflare trace 双源探测）
- `leaks`：发现的问题列表；为空则退出码 0，有泄露退出码 1

判定规则：

- 候选地址为 `*.local`（mDNS 混淆的本地主机名）或非 IPv4 → 安全
- 出现内网 IPv4（10.x / 172.16-31.x / 192.168.x 等）→ 泄露
- 直连模式下出现公网 IPv4 候选 → 泄露
- 配置代理后直连 IP 与代理 IP 相同 → 泄露（代理未生效）

> 说明：本文的实测记录与加固措施出自完整部署版（含站点适配器）；开源版中适配器为桩、相关自检在 `browser_launch` 处会终止，实测数字不可在开源版直接复现。

## 3. 2026-08-09 实测结果

- 修复前：直连模式下 STUN 返回的 srflx 候选是一个公网 IPv4 地址（与出口 IP 一致，即真实公网 IP 泄露）。
- 修复后：启用 `disable_non_proxied_udp` 后不再收集到任何 ICE 候选，`leaks: []`，三组检查全部 `pass`。
- 无效代理（`http://127.0.0.1:59999`）下：浏览器正常启动、指纹正常、代理出口探测报 `error`（连接失败），无泄露、退出码 0。
- 指纹检查：`webdriver=false`，语言列表去重后形如 `["zh-CN","zh","en-US","en"]`，插件数 / 并发数 / 内存均为常见值，与 Chromium 引擎保持一致。

## 4. 注意事项

- WebRTC 防护策略为“禁用非代理 UDP”：无代理时 STUN 无法收集中继候选；
  配置代理后 WebRTC 流量也被限制在代理内，不会绕过代理直连。
- 不要授予 `camera` / `microphone` 权限（会强制 Chromium 关闭 mDNS 混淆，暴露真实主机 IP）。
- 站点侧的风控（如 行情站B `errorCode 106/108`、行情站A `/405` 验证页——两者是本框架采集的公开行情站点，开源版已移除对应适配器）仍会存在；
  代理只能改变出口 IP，不能保证绕过环境风控。

## 5. 代理网关（开源版说明）

开源版不含任何个人代理配置。如需代理，请在 `config.yaml` 的 `proxy` 段自行配置。

本机没有现成代理订阅时，可自建本地网关掩盖出口 IP：

crawler -> 本地代理网关（开源内核，仅监听 127.0.0.1）-> 认证出口 -> 目标站点

- 网关选型：任选成熟开源代理内核，配置只落在本地 `data/proxy/config.yaml`；
  建议启用 fake-ip DNS（网关先用假 IP 应答域名解析、真实连接时再做映射，避免 DNS 请求绕开代理外泄）防泄露，并配置出口自动测速、故障自动切换。
- 认证出口：优先使用带认证的专属出口节点，稳定性与带宽优于公共资源；
  代理链路仅用于公开市场数据采集，绝不在其中传输任何账号令牌。
- 验证：`security-check` 会对比直连 IP 与代理出口 IP，全部 pass、`leaks` 为空
  即掩蔽生效；若目标站出现风控拦截，先重启网关再重试。
- 已有稳定订阅时，把订阅地址加入网关配置的 proxy-providers 并重启网关即可。
