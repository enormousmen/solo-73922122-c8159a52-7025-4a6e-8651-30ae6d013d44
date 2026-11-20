# restart-permit-api

存储集群值班员用的**分批重启许可 API**。FastAPI + SQLite，单容器即可运行。

核心规则：

- 服务启动时读取**固定的分片投票副本清单**（`manifest.json`）。
- 节点探活报告携带**采样时间、健康状态、启动标识**；每个节点以**采样时间最新**的一份为准。
- 值班员提交**申请号 + 拟重启的一批节点**，满足以下条件才批准整批：
  1. 本批每个节点最新探活均健康；
  2. 本批节点未被任何**未结束许可**占用；
  3. 扣除 *本批节点 + 未结束许可占用节点 + 健康未知/异常节点* 后，本批涉及的**每个分片**剩余可用投票副本均**过半**；否则拒绝并列出受影响分片。
- **候补**：申请时带 `enqueue_if_rejected: true`，因探活、占用或票数不足未获批的批次按**申请号入队**（HTTP 202 待批）；候补**不占用节点**，实际发证后才计入占用。
- **探活上报或许可结单时**，按**入队顺序**检查候补：仍不符合条件的批次**跳过**（留在队列），每发一张证就把该批节点计入占用再判定下一批；触发事件与由此发出的许可**在同一事务中生效，任何写入失败整次回滚**。
- **并发申请**在写事务（`BEGIN IMMEDIATE`）中串行判定，按实际生效顺序使用余量，守住多数票。
- **同号同批重发**返回原许可或待批状态（HTTP 200）；同号不同批返回 409。
- 许可、候补队列（含入队顺序）与探活记录落盘 SQLite，**服务重启后保留**。
- **结束许可**要求本批每个节点的最新探活：在获批后采集、健康、启动标识与获批时不同。

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 服务监听 http://localhost:8000 ，交互文档 http://localhost:8000/docs
# 候补获批演示：./scripts/demo_waitlist.sh
```

- SQLite 数据在命名卷 `permit-data`（容器内 `/data/permits.db`），重启不丢。
- 分片清单 `./manifest.json` 只读挂载进容器；修改后需 `docker compose restart` 生效。

本地开发（不用 Docker）：

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
DB_PATH=./data/permits.db MANIFEST_PATH=./manifest.json \
  uvicorn app.main:app --host 0.0.0.0 --port 8000
```

运行测试：

```bash
pytest tests/ -q
```

## 分片清单格式

```json
{
  "shards": {
    "shard-a": ["n1", "n2", "n3"],
    "shard-b": ["n2", "n3", "n4"],
    "shard-c": ["n3", "n4", "n5"]
  }
}
```

每个分片列出全部投票副本节点；过半线 = `floor(票数/2)+1`（3 票需 2 可用）。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/healthz` | 服务健康检查 |
| GET | `/manifest` | 查看分片清单与各分片过半线 |
| POST | `/liveness` | 批量上报探活（201；响应含本次补发的候补许可 `issued_from_waitlist`） |
| GET | `/liveness` | 全部节点最新探活 |
| GET | `/liveness/{node}` | 单节点最新探活 |
| POST | `/permits` | 申请许可（201 新批 / 202 入队候补 / 200 幂等重发 / 409 拒绝或换批冲突） |
| GET | `/permits?status=open\|finished` | 许可列表 |
| GET | `/permits/{request_id}` | 按申请号查询：已发证返回许可，在队返回待批状态，都没有返回 404 |
| POST | `/permits/{request_id}/finish` | 结束许可（200 / 409 条件不满足；响应含补发的候补许可） |
| GET | `/waitlist` | 候补队列（按入队顺序） |

## 调用示例

### 1. 上报探活

```bash
curl -X POST http://localhost:8000/liveness \
  -H 'Content-Type: application/json' -d '{
    "reports": [
      {"node_id":"n1","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"},
      {"node_id":"n2","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"},
      {"node_id":"n3","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"},
      {"node_id":"n4","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"},
      {"node_id":"n5","sampled_at":"2026-11-19T10:00:00Z","healthy":true,"boot_id":"boot-1"}
    ]}'
# => {"accepted":5}
```

### 2. 申请许可

```bash
curl -X POST http://localhost:8000/permits \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-001","nodes":["n1"]}'
# 201 =>
# {"request_id":"req-001","nodes":["n1"],"status":"open",
#  "approved_at":"...","finished_at":null,
#  "boot_ids_at_approval":{"n1":"boot-1"}}
```

同号同批重发，返回原许可（200，`approved_at` 不变）；同号不同批返回 409。

### 3. 多数票不足被拒（列出受影响分片）

`req-001` 已占用 n1 时再申请 n2（同属 shard-a，3 票最多扣 1 票）：

```bash
curl -X POST http://localhost:8000/permits \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-002","nodes":["n2"]}'
# 409 =>
# {"detail":{"ok":false,"error":"permit_rejected",
#   "affected_shards":[{"shard":"shard-a","voters":3,"required_available":2,
#     "available":1,"available_nodes":["n3"],
#     "excluded":{"n1":"occupied_by:req-001","n2":"in_batch"}}]}}
```

其它拒绝类别：`health_unknown_nodes`（健康未知）、`unhealthy_nodes`（异常）、
`occupied_nodes`（被未结束许可占用）。清单外节点一律 422。

### 4. 节点重启后上报新探活，结束许可

```bash
# 节点重启完成：采样时间晚于获批时间、健康、启动标识已变
curl -X POST http://localhost:8000/liveness \
  -H 'Content-Type: application/json' -d '{
    "reports":[{"node_id":"n1","sampled_at":"2026-11-19T10:20:00Z",
                "healthy":true,"boot_id":"boot-2"}]}'

curl -X POST http://localhost:8000/permits/req-001/finish
# 200 => {"request_id":"req-001",...,"status":"finished","finished_at":"..."}
```

结束条件不满足时 409 并给出逐节点原因，例如：

```json
{"detail":{"error":"finish_conditions_not_met",
           "nodes":{"n1":"boot_id_unchanged"}}}
```

可能原因：`no_liveness_report`、`not_sampled_after_approval`、`unhealthy`、`boot_id_unchanged`。
许可结束后节点被释放，可被后续申请占用。

## 候补流程演示（候补获批）

完整可运行脚本见 `scripts/demo_waitlist.sh`（服务启动后执行 `./scripts/demo_waitlist.sh`）。
以下为核心 API 请求。

### 1. 申请时选择候补：未获批则入队（202 待批）

`req-001` 已持有 n1，再为 n1 提交申请并带 `enqueue_if_rejected`：

```bash
curl -X POST http://localhost:8000/permits \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-002","nodes":["n1"],"enqueue_if_rejected":true}'
# 202 =>
# {"request_id":"req-002","nodes":["n1"],"status":"pending",
#  "enqueued_at":"...","position":1,
#  "rejection":{"ok":false,"error":"permit_rejected",
#    "occupied_nodes":[{"node":"n1","permit":"req-001"}]}}
```

候补**不占用节点**：其它不冲突的申请照常获批。按申请号查询或同号同批重发，
都返回待批状态（同号换批返回 409）：

```bash
curl http://localhost:8000/permits/req-002
# => {"request_id":"req-002","nodes":["n1"],"status":"pending","position":1,...}

curl http://localhost:8000/waitlist
# => {"waitlist":[{"request_id":"req-002","nodes":["n1"],"status":"pending","position":1,...}]}
```

### 2. 结单触发补发：按入队顺序发证

n1 重启完成上报新探活后，`req-001` 结单；响应中的 `issued_from_waitlist`
是本次按入队顺序补发的许可（每发一张，该批节点立即计入占用再判下一批）：

```bash
curl -X POST http://localhost:8000/permits/req-001/finish
# 200 =>
# {"request_id":"req-001",...,"status":"finished",
#  "issued_from_waitlist":[
#    {"request_id":"req-002","nodes":["n1"],"status":"open",
#     "approved_at":"...","boot_ids_at_approval":{"n1":"boot-2"}}]}

curl http://localhost:8000/permits/req-002
# => {"request_id":"req-002","status":"open",...}   # 候补已获批
```

### 3. 探活上报触发补发：仍不符合条件的批次被跳过

n4 上报异常时 `req-004`（节点 n4）入队；n4 恢复健康的上报到达时，
同一响应里带回补发的许可（上报与发证在同一事务中生效）：

```bash
curl -X POST http://localhost:8000/liveness \
  -H 'Content-Type: application/json' -d '{
    "reports":[{"node_id":"n4","sampled_at":"2026-11-19T10:30:00Z",
                "healthy":true,"boot_id":"boot-1"}]}'
# 201 =>
# {"accepted":1,
#  "issued_from_waitlist":[
#    {"request_id":"req-004","nodes":["n4"],"status":"open",...}]}
```

若上报后某候补仍不满足条件（节点仍异常 / 仍被占用 / 票数仍不足），
它会被跳过并保留在队列中，后续符合条件的候补照常获批。

## 设计说明

- **幂等**：`request_id` 是许可表主键，也是候补队列的唯一键。节点集合规范化（去重排序）后比较，
  同号同批重放直接返回原许可或待批状态；同号不同批 409。未选择候补的被拒申请不落库，
  整改后可用同号重试。
- **候补队列**：入队即落盘 `waitlist` 表，自增 `id` 即入队顺序（`position`），重启后保留。
  候补不占用节点；只有发证时节点才计入占用。只在**探活上报**与**许可结单**两个
  状态变化点按入队顺序重估：逐批判定，仍不满足的跳过，每批准一批就把其节点计入占用
  再判下一批（`logic.plan_waitlist` 纯函数实现，便于单测）。
- **原子性**：`POST /liveness` 与 `POST /permits/{id}/finish` 都在同一写事务中完成
  "落触发事件 + 补发候补许可"，事务可重入、任一层失败整次回滚，
  不会出现"上报已存但许可没发"或反过来的中间态。
- **并发**：所有写路径走单连接 + 可重入锁 + `BEGIN IMMEDIATE` 事务，
  "检查多数票 → 写入许可/入队"是原子的；并发申请按事务实际生效顺序串行判定，不会超批。
- **持久化**：许可、候补与全部探活报告存 SQLite（WAL 模式），最新探活按
  `max(sampled_at)` 实时计算，服务重启后状态完整恢复。
- **结束判定**：批准时快照各节点 `boot_id`；结束时要求最新报告
  `sampled_at > approved_at` 且健康且 `boot_id` 不同。

## 已知限制 / 未完成项

- 单 worker 运行（SQLite 单写者）；如需多副本需换外部数据库。
- 未做认证鉴权，假定部署在受信内网；如需可前置网关或加 token。
- 探活无"过期"概念：只要最新一份健康即视为健康，不判断采样时间新旧。
- 结束许可比较的是节点上报的采样时钟与服务端获批时钟，依赖各节点时钟基本同步（NTP）。
- 许可无过期/自动回收机制；值班员需显式 finish。
- 候补无取消接口、无超时/过期；只在探活上报与许可结单时重估，不随时间自动触发。
  如需取消，目前只能换申请号重新申请（旧候补会一直在队，直到条件满足被发证）。
