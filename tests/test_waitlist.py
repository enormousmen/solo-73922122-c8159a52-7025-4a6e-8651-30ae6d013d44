"""候补（waitlist）流程自测：入队、待批视图、顺序发放、跳过、原子回滚、持久化、并发。"""
from __future__ import annotations

import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import create_app  # noqa: E402

MANIFEST = {
    "shards": {
        "shard-a": ["n1", "n2", "n3"],
        "shard-b": ["n2", "n3", "n4"],
        "shard-c": ["n3", "n4", "n5"],
    }
}
ALL_NODES = ["n1", "n2", "n3", "n4", "n5"]
T0 = datetime.now(timezone.utc) - timedelta(minutes=30)  # 初始探活采样时间


@pytest.fixture()
def make_client(tmp_path):
    """工厂：同一 db 路径可反复建 app，模拟服务重启。"""
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(MANIFEST))
    db_path = tmp_path / "permits.db"
    clients = []

    def _make():
        app = create_app(db_path=str(db_path), manifest_path=str(manifest_path))
        c = TestClient(app)
        clients.append(c)
        return c

    yield _make
    for c in clients:
        c.close()


@pytest.fixture()
def client(make_client):
    return make_client()


def report(client, node, healthy=True, boot="boot-1", sampled_at=None):
    sampled_at = sampled_at or datetime.now(timezone.utc)
    return client.post(
        "/liveness",
        json={
            "reports": [
                {
                    "node_id": node,
                    "sampled_at": sampled_at.isoformat(),
                    "healthy": healthy,
                    "boot_id": boot,
                }
            ]
        },
    )


def seed_all_healthy(client, boot="boot-1", sampled_at=T0):
    for n in ALL_NODES:
        r = report(client, n, healthy=True, boot=boot, sampled_at=sampled_at)
        assert r.status_code == 201, r.text


def request_permit(client, request_id, nodes, enqueue=False):
    return client.post(
        "/permits",
        json={
            "request_id": request_id,
            "nodes": nodes,
            "enqueue_if_rejected": enqueue,
        },
    )


def later(minutes=10):
    return datetime.now(timezone.utc) + timedelta(minutes=minutes)


# ---------- 入队与待批视图 ----------


def test_enqueue_when_rejected_and_pending_views(client):
    seed_all_healthy(client)
    assert request_permit(client, "req-1", ["n1"]).status_code == 201

    # n1 被占用，选择候补 -> 202 待批
    r = request_permit(client, "req-2", ["n1"], enqueue=True)
    assert r.status_code == 202, r.text
    body = r.json()
    assert body["status"] == "pending"
    assert body["position"] == 1
    assert body["rejection"]["occupied_nodes"] == [{"node": "n1", "permit": "req-1"}]

    # 按申请号查询 -> 待批状态
    got = client.get("/permits/req-2").json()
    assert got["status"] == "pending" and got["nodes"] == ["n1"]

    # 队列按入队顺序可见
    wl = client.get("/waitlist").json()["waitlist"]
    assert [e["request_id"] for e in wl] == ["req-2"]

    # 候补不占用节点：不冲突的申请照常获批
    assert request_permit(client, "req-3", ["n5"]).status_code == 201


def test_enqueue_unknown_nodes_still_422(client):
    seed_all_healthy(client)
    r = request_permit(client, "req-x", ["n1", "ghost"], enqueue=True)
    assert r.status_code == 422
    assert client.get("/permits/req-x").status_code == 404  # 未入队


def test_pending_idempotent_replay_and_conflict(client):
    seed_all_healthy(client)
    assert request_permit(client, "req-1", ["n1"]).status_code == 201
    r = request_permit(client, "req-2", ["n1"], enqueue=True)
    assert r.status_code == 202
    enqueued_at = r.json()["enqueued_at"]

    # 同号同批重发 -> 200 返回待批状态（enqueued_at 不变）
    replay = request_permit(client, "req-2", ["n1"], enqueue=True)
    assert replay.status_code == 200
    assert replay.json()["status"] == "pending"
    assert replay.json()["enqueued_at"] == enqueued_at
    # 节点顺序/重复不影响"同批"判定
    assert request_permit(client, "req-2", ["n1", "n1"]).status_code == 200

    # 同号换批 -> 409
    conflict = request_permit(client, "req-2", ["n2"], enqueue=True)
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["error"] == "request_id_conflict"
    assert conflict.json()["detail"]["existing_nodes"] == ["n1"]


def test_no_flag_rejection_stays_409_and_not_queued(client):
    seed_all_healthy(client)
    assert request_permit(client, "req-1", ["n1"]).status_code == 201
    r = request_permit(client, "req-2", ["n1"])  # 未选择候补
    assert r.status_code == 409
    assert client.get("/permits/req-2").status_code == 404
    assert client.get("/waitlist").json()["waitlist"] == []


# ---------- 触发发放：结单 ----------


def test_finish_issues_waitlist_in_order_and_accounts_occupancy(client):
    seed_all_healthy(client)
    assert request_permit(client, "req-1", ["n1"]).status_code == 201
    assert request_permit(client, "req-2", ["n1"], enqueue=True).status_code == 202
    # n1 被占用时申请 n2：shard-a 余票不足 -> 入队
    assert request_permit(client, "req-3", ["n2"], enqueue=True).status_code == 202

    # n1 重启完成，结单 req-1
    report(client, "n1", healthy=True, boot="boot-2", sampled_at=later())
    r = client.post("/permits/req-1/finish")
    assert r.status_code == 200, r.text
    issued = r.json()["issued_from_waitlist"]
    # 按入队顺序：req-2 先获批；其节点 n1 立即计入占用，
    # req-3 (n2) 因 shard-a 余票仍不足被跳过、留在队列
    assert [p["request_id"] for p in issued] == ["req-2"]
    assert issued[0]["status"] == "open"
    assert issued[0]["boot_ids_at_approval"] == {"n1": "boot-2"}

    assert client.get("/permits/req-2").json()["status"] == "open"
    assert client.get("/permits/req-3").json()["status"] == "pending"
    assert [e["request_id"] for e in client.get("/waitlist").json()["waitlist"]] == ["req-3"]

    # req-2 结单后，req-3 才获批
    report(client, "n1", healthy=True, boot="boot-3", sampled_at=later(20))
    r = client.post("/permits/req-2/finish")
    assert [p["request_id"] for p in r.json()["issued_from_waitlist"]] == ["req-3"]
    assert client.get("/waitlist").json()["waitlist"] == []


def test_replay_after_waitlist_approval_returns_permit(client):
    seed_all_healthy(client)
    assert request_permit(client, "req-1", ["n1"]).status_code == 201
    assert request_permit(client, "req-2", ["n1"], enqueue=True).status_code == 202
    report(client, "n1", healthy=True, boot="boot-2", sampled_at=later())
    client.post("/permits/req-1/finish")

    # 候补已获批：同号同批重发 -> 200 返回原许可
    replay = request_permit(client, "req-2", ["n1"], enqueue=True)
    assert replay.status_code == 200
    assert replay.json()["status"] == "open"
    assert replay.json()["approved_at"] == client.get("/permits/req-2").json()["approved_at"]
    # 同号换批 -> 409
    assert request_permit(client, "req-2", ["n2"], enqueue=True).status_code == 409


# ---------- 触发发放：探活上报 ----------


def test_liveness_report_issues_waitlist_and_skips_uneligible(client):
    seed_all_healthy(client)
    t1 = later()
    report(client, "n1", healthy=False, boot="boot-1", sampled_at=t1)
    report(client, "n5", healthy=False, boot="boot-1", sampled_at=t1)

    # n1、n5 异常 -> 均入队（req-a 在前）
    assert request_permit(client, "req-a", ["n1"], enqueue=True).status_code == 202
    assert request_permit(client, "req-b", ["n5"], enqueue=True).status_code == 202

    # 只有 n5 恢复健康：req-a 仍不符合被跳过，req-b 获批
    r = report(client, "n5", healthy=True, boot="boot-1", sampled_at=later(20))
    assert r.status_code == 201
    assert [p["request_id"] for p in r.json()["issued_from_waitlist"]] == ["req-b"]
    assert client.get("/permits/req-a").json()["status"] == "pending"
    assert client.get("/permits/req-b").json()["status"] == "open"

    # n1 恢复健康后 req-a 获批
    r = report(client, "n1", healthy=True, boot="boot-1", sampled_at=later(30))
    assert [p["request_id"] for p in r.json()["issued_from_waitlist"]] == ["req-a"]
    assert client.get("/waitlist").json()["waitlist"] == []


# ---------- 原子性 ----------


def test_report_and_issued_permits_rollback_together(client, monkeypatch):
    seed_all_healthy(client)
    report(client, "n2", healthy=False, boot="boot-1", sampled_at=later())
    assert request_permit(client, "req-1", ["n2"], enqueue=True).status_code == 202

    # 模拟发证写入失败：整次（探活上报 + 发出的许可）回滚
    store = client.app.state.store

    def boom(*args, **kwargs):
        raise RuntimeError("simulated write failure")

    monkeypatch.setattr(store, "insert_permit", boom)
    with pytest.raises(RuntimeError):
        report(client, "n2", healthy=True, boot="boot-1", sampled_at=later(20))

    # 上报被回滚：n2 仍是不健康的旧报告；候补仍在队列；没有发出许可
    assert client.get("/liveness/n2").json()["healthy"] is False
    assert client.get("/permits/req-1").json()["status"] == "pending"
    assert client.get("/permits", params={"status": "open"}).json()["permits"] == []

    # 故障消除后重试：上报与发证一同生效
    monkeypatch.undo()
    r = report(client, "n2", healthy=True, boot="boot-1", sampled_at=later(30))
    assert r.status_code == 201
    assert [p["request_id"] for p in r.json()["issued_from_waitlist"]] == ["req-1"]
    assert client.get("/permits/req-1").json()["status"] == "open"


# ---------- 持久化 ----------


def test_waitlist_and_order_survive_restart(make_client):
    c1 = make_client()
    seed_all_healthy(c1)
    assert request_permit(c1, "req-1", ["n1"]).status_code == 201
    assert request_permit(c1, "req-2", ["n1"], enqueue=True).status_code == 202
    assert request_permit(c1, "req-3", ["n2"], enqueue=True).status_code == 202
    c1.close()

    # 模拟服务重启：候补及其顺序保留
    c2 = make_client()
    wl = c2.get("/waitlist").json()["waitlist"]
    assert [e["request_id"] for e in wl] == ["req-2", "req-3"]
    assert [e["position"] for e in wl] == [1, 2]
    assert c2.get("/permits/req-2").json()["status"] == "pending"

    # 重启后结单仍按入队顺序补发
    report(c2, "n1", healthy=True, boot="boot-2", sampled_at=later())
    r = c2.post("/permits/req-1/finish")
    assert [p["request_id"] for p in r.json()["issued_from_waitlist"]] == ["req-2"]
    assert [e["request_id"] for e in c2.get("/waitlist").json()["waitlist"]] == ["req-3"]


# ---------- 并发 ----------


def test_concurrent_requests_enqueue_and_drain_in_effective_order(client):
    seed_all_healthy(client)
    # 8 个并发申请，交替抢 n1 / n2（同属 shard-a，3 票最多扣 1 票），全部选择候补
    reqs = [(f"c-{i}", ["n1" if i % 2 == 0 else "n2"]) for i in range(8)]

    def submit(req):
        rid, nodes = req
        return request_permit(client, rid, nodes, enqueue=True).status_code

    with ThreadPoolExecutor(max_workers=8) as pool:
        codes = list(pool.map(submit, reqs))

    # 按实际生效顺序使用余量：恰好 1 个获批，其余 7 个入队
    assert codes.count(201) == 1, codes
    assert codes.count(202) == 7, codes
    opens = client.get("/permits", params={"status": "open"}).json()["permits"]
    assert len(opens) == 1
    assert len(client.get("/waitlist").json()["waitlist"]) == 7

    # 结单后按入队顺序补发：仍只有 1 张新证（余量只够一张）
    held = opens[0]
    report(client, held["nodes"][0], healthy=True, boot="boot-2", sampled_at=later())
    r = client.post(f"/permits/{held['request_id']}/finish")
    assert r.status_code == 200, r.text
    assert len(r.json()["issued_from_waitlist"]) == 1
    assert len(client.get("/permits", params={"status": "open"}).json()["permits"]) == 1
    assert len(client.get("/waitlist").json()["waitlist"]) == 6
