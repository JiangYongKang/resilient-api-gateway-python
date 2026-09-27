# 本地多上游聚合网关（Resilient Aggregate Gateway）

一个 FastAPI 网关：客户端只调用 **一个入口** `GET /aggregate`，网关在统一的
时间预算内**并发**请求多个本地 mock 上游，合并结果返回。具备部分失败容错、
按上游隔离的熔断器、超时预算与并发负载控制。**不依赖任何外部网络或真实服务**，
所有上游均为本机 `127.0.0.1` 上的 mock HTTP 服务。

## 目录结构

```
gateway/
  config.py          # 配置：上游列表、总预算、单上游超时、并发、熔断参数
  models.py          # 响应模型与失败原因枚举 FailReason / 熔断状态 CircuitState
  breaker.py         # 熔断器（CLOSED/OPEN/HALF_OPEN）与按上游隔离的注册表
  upstream.py        # httpx 调用封装：异常/状态码 -> 可区分失败原因
  aggregator.py      # 聚合核心：总预算、信号量并发控制、结果合并、日志
  mock_upstream.py   # mock 上游 ASGI 应用（ok/slow/error/malformed/not_found）
  mock_server.py     # 后台线程启动真实 uvicorn mock 服务
  app.py             # FastAPI 应用工厂（/aggregate /circuits /health）
main.py              # 一键启动 3 个 mock 上游 + 网关（端口 8000）
tests/               # 16 个测试：部分失败 / 熔断恢复 / 超时预算 / 并发突发
```

## 快速开始

需要 Python 3.14+ 与 [uv](https://docs.astral.sh/uv/)。

```bash
uv sync --group dev          # 安装依赖（fastapi/httpx + pytest）
uv run python main.py        # 启动 3 个本地 mock 上游 + 网关
```

另开终端：

```bash
curl -s http://127.0.0.1:8000/aggregate | python3 -m json.tool
curl -s http://127.0.0.1:8000/circuits  | python3 -m json.tool
```

`main.py` 默认起三个上游：`users`（正常）、`orders`（慢但成功，0.2s）、
`payments`（持续 HTTP 500，用于观察部分失败与熔断：连续失败达到阈值后
`/aggregate` 中它的 `reason` 从 `http_error` 变为 `circuit_open`，
`degraded=true`）。

## 聚合与部分失败语义

- 每次聚合对配置中的**每个上游**都返回一条结果，顺序固定为配置顺序
  （与完成先后无关），**绝不静默丢弃**任何上游。
- 成功：`ok=true`，`data` 为上游真实数据；失败：`ok=false`，`data=null`，
  并给出可区分的 `reason` 与人类可读的 `error`。**绝不用伪造数据补齐**。
- 至少一个上游成功 → HTTP **200**（部分成功）；全部失败 → HTTP **503**，
  但响应体结构不变，失败原因完整保留。
- 响应顶层还有 `success` / `all_failed` / `timed_out` / `total_ms` 汇总字段。

失败原因（`FailReason`）：

| reason | 含义 | 是否计入连续失败 |
|---|---|---|
| `timeout` | 超过该上游自身超时 | 是 |
| `budget_exceeded` | 被统一总预算掐断 / 排队后预算耗尽（未完成） | 是 |
| `http_error` | 上游返回 5xx | 是 |
| `upstream_error` | 连接失败 / 网络错误 / 其他 4xx | 是 |
| `bad_response` | 非 JSON 或数据结构畸形（缺对象型 `data`） | 是 |
| `upstream_not_found` | 上游返回 404（视为业务性失败，上游健康） | **否** |
| `circuit_open` | 熔断 OPEN，快速失败（降级，未真正发请求） | — |
| `circuit_rejected` | HALF_OPEN 期间唯一探测在飞，其他请求被拒（降级） | — |
| `internal_error` | 网关内部异常（不炸掉聚合，单独标记） | 否 |

`degraded=true` 明确标识“因熔断而没有真正请求上游”的降级结果，与真实请求失败
区分；5xx/404/畸形/超时之间的 `reason` 彼此可区分。

## 熔断与降级规则

每个上游一个独立熔断器（`CircuitBreakerRegistry` 按名字隔离，**互不影响**）：

```
CLOSED ── 连续失败计数 ≥ failure_threshold ──▶ OPEN
OPEN   ── 冷却 recovery_timeout 到期，放行 1 次探测 ──▶ HALF_OPEN
HALF_OPEN ── 探测成功 ──▶ CLOSED（计数清零，恢复）
HALF_OPEN ── 探测失败 ──▶ OPEN（冷却重新计时）
HALF_OPEN 期间其余并发请求 ──▶ 直接拒绝 reason=circuit_rejected
```

- **偶发的单次、两次失败不会熔断**；一次成功会把连续失败计数清零。
- OPEN 期间请求**快速失败**（不建立到上游的连接），`reason=circuit_open`、
  `degraded=true`，与真实上游报错明显可区分。
- 冷却后进入 HALF_OPEN，**同一时刻只允许一次探测**，保证恢复行为确定；
  探测期间其他并发请求得到 `circuit_rejected`。
- 404 等业务失败不增加失败计数（上游进程是健康的）。
- 熔断状态变化通过 `gateway.breaker` logger 输出，例如：
  `[breaker] upstream=orders CLOSED -> OPEN (failure_count=3 reached threshold 3)`。
- 可随时 `GET /circuits` 查看所有上游的状态、连续失败计数与探测标记。

## 超时预算与并发控制

- **统一总预算** `total_timeout`：每个上游的实际等待时间为
  `min(自身 timeout, 总预算剩余)`；信号量排队也在剩余预算内等待。
  预算耗尽时未完成的上游记 `budget_exceeded`，任何慢上游组合都不能把整体
  拖到预算之外（另有一个很小的外层兜底取消，标记 `timed_out`）。
- **单上游超时**：`UpstreamConfig.timeout`（默认取 `default_upstream_timeout`），
  超时记 `timeout`，与被总预算掐断的 `budget_exceeded` 可区分。
- **并发负载控制**：`max_concurrent_calls` 信号量 + httpx 连接上限，
  限制同一时刻在飞的上游调用数；突发请求在预算内排队，不丢结果。
- **稳定性**：结果按配置顺序返回，成功只使用真实数据，失败显式标记，
  相同输入下结论确定、可重复（测试中连续两轮全绿、突发响应签名唯一）。

### 配置项（均可通过环境变量覆盖）

| 项 | 环境变量 | 默认 | 说明 |
|---|---|---|---|
| 总时间预算（秒） | `GW_TOTAL_TIMEOUT` | 2.0 | 整个 `/aggregate` 的硬上限 |
| 单上游超时（秒） | `GW_UPSTREAM_TIMEOUT` | 1.0 | 每个上游调用的超时 |
| 全局并发上限 | `GW_MAX_CONCURRENCY` | 8 | 同时在飞的上游 HTTP 调用数 |
| 熔断失败阈值 | `GW_BREAKER_THRESHOLD` | 3 | 连续失败达到该值才 OPEN |
| 熔断冷却（秒） | `GW_BREAKER_RECOVERY` | 5.0 | OPEN 后多久允许半开探测 |

代码中也可用 `gateway.config.build_config(...)` 显式传参（测试即用该方式，
把冷却时间缩短到 0.3s 以加快验证）。

## Mock 上游的可控行为

每个 mock 上游暴露：

- `GET /data`：按当前模式响应。模式：`ok`（200 正常 JSON）、`slow`
  （`delay` 秒后 200）、`error`（500）、`malformed`（200 但 body 是坏 JSON）、
  `not_found`（404）。
- `POST /control`：运行时切换 `{mode, delay, payload, sequence}`；
  `sequence` 可让前 N 次调用按序列返回不同模式（用于制造“失败两次后恢复”等
  确定性场景）。
- `GET /control`：查看当前模式与**服务端观测到的并发峰值** `in_flight_peak`
  （并发测试据此断言信号量生效）。

## 本地验证（测试）

```bash
uv run pytest                      # 全部 16 个测试
uv run pytest -s                  # -s 可看到每个上游结果与熔断状态变化日志
uv run pytest tests/test_circuit_breaker.py   # 只跑熔断类
uv run pytest tests/test_timeout.py           # 只跑超时预算类
```

四类场景：

1. **部分失败** `tests/test_partial_failure.py`：成功/500/畸形混合仍 200 返回
   部分结果；全失败 503；404 不熔断；失败 `data` 恒为 null、原因可区分、顺序稳定。
2. **熔断触发与恢复** `tests/test_circuit_breaker.py`：单次/两次失败不熔断；
   达阈值 OPEN 并快速失败（`circuit_open`）；各上游隔离；冷却后半开探测成功
   恢复 / 失败重开；半开期间唯一探测，其余 `circuit_rejected`。
3. **超时预算** `tests/test_timeout.py`：单上游自身超时；全慢场景下整体耗时
   的硬上限断言；`timeout` 与 `budget_exceeded` 区分；两次运行结论一致。
4. **并发突发** `tests/test_concurrency.py`：突发 5~12 个并发聚合，服务端观测
   并发峰值不超过上限；突发响应结果签名唯一、确定可重复；串行限流不丢结果；
   熔断后突发 10 个请求对故障上游全部稳定降级。

所有上游/网络交互都发生在 `127.0.0.1`，无外部网络依赖。
