#!/usr/bin/env bash
# 演示"候补获批"的完整流程（需服务已启动，默认 http://localhost:8000）。
#
# 场景一：req-002 因节点被占用入队，req-001 结单后按入队顺序补发。
# 场景二：req-004 因探活异常入队，节点恢复健康的上报到达时补发。
set -euo pipefail

BASE=${BASE:-http://localhost:8000}
NOW=$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)
LATER=$(date -u -d '+5 minutes' +%Y-%m-%dT%H:%M:%S.%NZ)
LATER2=$(date -u -d '+10 minutes' +%Y-%m-%dT%H:%M:%S.%NZ)

step() { printf '\n=== %s ===\n' "$1"; }

step "0. 全部节点上报健康（boot-1）"
curl -sS -X POST "$BASE/liveness" -H 'Content-Type: application/json' -d "{
  \"reports\": [
    {\"node_id\":\"n1\",\"sampled_at\":\"$NOW\",\"healthy\":true,\"boot_id\":\"boot-1\"},
    {\"node_id\":\"n2\",\"sampled_at\":\"$NOW\",\"healthy\":true,\"boot_id\":\"boot-1\"},
    {\"node_id\":\"n3\",\"sampled_at\":\"$NOW\",\"healthy\":true,\"boot_id\":\"boot-1\"},
    {\"node_id\":\"n4\",\"sampled_at\":\"$NOW\",\"healthy\":true,\"boot_id\":\"boot-1\"},
    {\"node_id\":\"n5\",\"sampled_at\":\"$NOW\",\"healthy\":true,\"boot_id\":\"boot-1\"}
  ]}"
echo

step "1. req-001 申请 [n1] -> 201 立即发证"
curl -sS -X POST "$BASE/permits" -H 'Content-Type: application/json' \
  -d '{"request_id":"req-001","nodes":["n1"]}'
echo

step "2. req-002 申请 [n1] 并选择候补 -> 202 待批（n1 被占用，入队不占用节点）"
curl -sS -X POST "$BASE/permits" -H 'Content-Type: application/json' \
  -d '{"request_id":"req-002","nodes":["n1"],"enqueue_if_rejected":true}'
echo

step "3. 按申请号查询 req-002 -> 待批状态；查看候补队列"
curl -sS "$BASE/permits/req-002"
echo
curl -sS "$BASE/waitlist"
echo

step "4. n1 重启完成上报新探活（boot-2，采样时间晚于获批）"
curl -sS -X POST "$BASE/liveness" -H 'Content-Type: application/json' -d "{
  \"reports\":[{\"node_id\":\"n1\",\"sampled_at\":\"$LATER\",\"healthy\":true,\"boot_id\":\"boot-2\"}]}"
echo

step "5. req-001 结单 -> 响应中 issued_from_waitlist 包含补发给 req-002 的许可"
curl -sS -X POST "$BASE/permits/req-001/finish"
echo

step "6. 再查 req-002 -> 已获批（open）；同号同批重发 -> 200 返回原许可"
curl -sS "$BASE/permits/req-002"
echo
curl -sS -X POST "$BASE/permits" -H 'Content-Type: application/json' \
  -d '{"request_id":"req-002","nodes":["n1"],"enqueue_if_rejected":true}'
echo

step "7. n4 上报异常，req-004 申请 [n4] 选择候补 -> 202 入队"
curl -sS -X POST "$BASE/liveness" -H 'Content-Type: application/json' -d "{
  \"reports\":[{\"node_id\":\"n4\",\"sampled_at\":\"$LATER\",\"healthy\":false,\"boot_id\":\"boot-1\"}]}"
echo
curl -sS -X POST "$BASE/permits" -H 'Content-Type: application/json' \
  -d '{"request_id":"req-004","nodes":["n4"],"enqueue_if_rejected":true}'
echo

step "8. n4 恢复健康的上报到达 -> 响应中 issued_from_waitlist 包含补发给 req-004 的许可"
curl -sS -X POST "$BASE/liveness" -H 'Content-Type: application/json' -d "{
  \"reports\":[{\"node_id\":\"n4\",\"sampled_at\":\"$LATER2\",\"healthy\":true,\"boot_id\":\"boot-1\"}]}"
echo

step "9. 最终状态：许可列表与候补队列"
curl -sS "$BASE/permits"
echo
curl -sS "$BASE/waitlist"
echo
