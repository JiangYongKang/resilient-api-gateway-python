# Resilient API Gateway (Python / FastAPI)

面向本地多个上游服务的聚合网关：客户端只调用一个入口 `GET /aggregate`，
网关并发请求所有上游、合并结果后返回。上游由本地 mock 服务代替，
不依赖任何外部网络或真实服务。

## 运行

```bash
uv sync
# 终端 1：启动本地 mock 上游（alpha:9001 beta:9002 gamma:9003）
uv run python -m gateway.upstreams
# 终端 2：启动网关（:8000）
uv run python main.py
```

验证：

```bash
curl -s http://127.0.0.1:8000/aggregate | jq
curl -s http://127.0.0.1:8000/breakers | jq
# 手动制造故障：mode 可取 ok | slow | error | malformed
curl -X POST http://127.0.0.1:9002/upstream/beta/mode \
  -H 'content-type: application/json' -d '{"mode":"error"}'
```

## 聚合与部分失败语义

`GET /aggregate` 的响应为每个上游给出独立结果，整体请求永不因单个
上游失败而失败：

```json
{
  "results": {
    "alpha": {"name": "alpha", "status": "ok", "data": {...}, "reason": null, "latency_ms": 1.2},
    "beta":  {"name": "beta",  "status": "error", "data": null, "reason": "upstream returned HTTP 503", ...}
  },
  "succeeded": ["alpha"],
  "failed": ["beta"]
}
```

- 失败的上游**显式标记**在 `failed` 中并附带 `reason`，不会静默丢弃，
  也不会用伪造数据补齐（失败时 `data` 恒为 `null`）。
- `status` 取值可区分失败原因：
  - `ok`：成功，`data` 为上游真实返回的 `payload`；
  - `timeout`：单上游超时或超出整体时间预算被取消；
  - `error`：上游返回 5xx 或连接错误；
  - `malformed`：响应体不是合法 JSON 或缺少约定的 `payload` 字段；
  - `circuit_open`：熔断器打开，快速失败（未真正发请求）。

## 熔断与降级规则

每个上游有**相互独立**的熔断器（`gateway/breaker.py`），状态机：

- **closed**：正常放行，统计*连续*失败次数（成功即清零，
  偶发单次失败不会触发熔断）；
- **open**：连续失败达到 `breaker_failure_threshold` 后打开，
  期间请求直接快速失败，返回 `circuit_open` 及可区分的降级原因；
- **half-open**：冷却 `breaker_cooldown_s` 秒后允许一个探测请求；
  探测成功则关闭熔断器恢复服务，探测失败则重新打开并重新计时冷却。

## 超时预算与并发控制

- **单请求超时** `request_timeout_s`：单个上游调用的最长等待；
- **总时间预算** `total_budget_s`：一次聚合请求的最长耗时。到预算时
  仍未完成的上游被取消并标记为 `timeout`（原因注明超出预算），
  慢上游不会把整体响应拖到预算之外；
- **并发控制** `max_concurrent_per_upstream`：每个上游一个信号量，
  限制并发请求下对该上游的同时在途请求数，避免突发流量打满上游。

## 配置项（环境变量）

| 环境变量 | 默认 | 含义 |
| --- | --- | --- |
| `GATEWAY_UPSTREAMS` | 三个本地 mock | 逗号分隔的 `name=url` 列表 |
| `GATEWAY_REQUEST_TIMEOUT_S` | `2.0` | 单上游请求超时（秒） |
| `GATEWAY_TOTAL_BUDGET_S` | `3.0` | 聚合请求总时间预算（秒） |
| `GATEWAY_BREAKER_FAILURE_THRESHOLD` | `3` | 熔断连续失败阈值 |
| `GATEWAY_BREAKER_COOLDOWN_S` | `5.0` | 熔断冷却时间（秒） |
| `GATEWAY_MAX_CONCURRENT_PER_UPSTREAM` | `5` | 单上游最大并发在途请求 |

## 本地验证（测试）

```bash
uv run pytest -q        # 全部测试
uv run pytest -q -s     # 查看每个上游的结果与熔断状态变化日志
```

测试通过 `httpx.ASGITransport` 在进程内挂载 mock 上游，不占用端口、
不访问外部网络，结果确定可重复：

- `tests/test_partial_failure.py`：全部成功、部分失败（error/malformed
  区分原因）、总预算内取消慢上游、单请求超时分类；
- `tests/test_circuit_breaker.py`：连续失败达阈值后熔断并快速失败、
  各上游熔断状态隔离、单次失败不熔断、冷却后半开探测恢复、
  探测失败重新打开；
- `tests/test_concurrency.py`：20 并发突发下结果稳定一致、每上游
  并发在途数不超过上限、相同条件重复请求结果确定。
