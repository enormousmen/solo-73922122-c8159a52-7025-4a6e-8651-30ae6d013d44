"""FastAPI 应用：分批重启许可 API（含候补队列）。"""
from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException, Query, Response

from .logic import evaluate_finish, evaluate_permit_request, plan_waitlist
from .manifest import load_manifest
from .models import PermitRecord, WaitlistEntry, now_utc, to_iso
from .schemas import LivenessBatchIn, PermitRequestIn
from .store import Store


def create_app(db_path: str, manifest_path: str) -> FastAPI:
    manifest = load_manifest(manifest_path)
    store = Store(db_path)

    app = FastAPI(title="restart-permit-api", version="1.1.0")
    app.state.store = store
    app.state.manifest = manifest

    def permit_view(p: PermitRecord) -> dict:
        return {
            "request_id": p.request_id,
            "nodes": list(p.nodes),
            "status": p.status,
            "approved_at": to_iso(p.approved_at),
            "finished_at": to_iso(p.finished_at) if p.finished_at else None,
            "boot_ids_at_approval": p.boot_ids_at_approval,
        }

    def waitlist_view(e: WaitlistEntry, rejection: dict | None = None) -> dict:
        view = {
            "request_id": e.request_id,
            "nodes": list(e.nodes),
            "status": "pending",
            "enqueued_at": to_iso(e.enqueued_at),
            "position": e.position,
        }
        if rejection is not None:
            view["rejection"] = rejection  # 入队时的未获批原因
        return view

    def process_waitlist() -> list[PermitRecord]:
        """按入队顺序尝试为候补发证，返回本次发出的许可。

        须在写事务内调用：发出的许可与触发它的上报/结单一同提交，
        任一写入失败则整次回滚。每发一张，该批节点立即计入占用，
        再判定下一批；仍不符合条件的批次跳过并保留在队列中。
        """
        latest = store.latest_reports()
        approved = plan_waitlist(
            manifest, latest, store.open_permits(), store.list_waitlist()
        )
        issued: list[PermitRecord] = []
        for entry in approved:
            record = PermitRecord(
                request_id=entry.request_id,
                nodes=entry.nodes,
                status="open",
                approved_at=now_utc(),
                finished_at=None,
                boot_ids_at_approval={n: latest[n].boot_id for n in entry.nodes},
            )
            store.insert_permit(record)
            store.remove_waitlist(entry.request_id)
            issued.append(record)
        return issued

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.get("/manifest")
    def get_manifest():
        return {
            "shards": {
                name: {
                    "voters": list(voters),
                    "majority_needed": manifest.majority_needed(name),
                }
                for name, voters in sorted(manifest.shards.items())
            }
        }

    # ---- 探活报告 ----

    @app.post("/liveness", status_code=201)
    def post_liveness(batch: LivenessBatchIn):
        unknown = sorted({r.node_id for r in batch.reports} - manifest.nodes)
        if unknown:
            raise HTTPException(
                status_code=422,
                detail={"error": "unknown_nodes", "unknown_nodes": unknown},
            )
        with store.transaction():  # 上报与由此发出的候补许可一同生效
            store.add_reports(batch.reports)
            issued = process_waitlist()
        return {
            "accepted": len(batch.reports),
            "issued_from_waitlist": [permit_view(p) for p in issued],
        }

    @app.get("/liveness")
    def get_all_liveness():
        latest = store.latest_reports()
        return {
            "nodes": {
                n: {
                    "sampled_at": to_iso(r.sampled_at),
                    "healthy": r.healthy,
                    "boot_id": r.boot_id,
                }
                for n, r in sorted(latest.items())
            },
            "nodes_without_reports": sorted(manifest.nodes - latest.keys()),
        }

    @app.get("/liveness/{node_id}")
    def get_node_liveness(node_id: str):
        rep = store.latest_reports().get(node_id)
        if rep is None:
            raise HTTPException(
                status_code=404,
                detail={"error": "no_liveness_report", "node": node_id},
            )
        return {
            "node_id": node_id,
            "sampled_at": to_iso(rep.sampled_at),
            "healthy": rep.healthy,
            "boot_id": rep.boot_id,
        }

    # ---- 许可 ----

    @app.post("/permits", status_code=201)
    def request_permit(req: PermitRequestIn, response: Response):
        nodes = sorted(set(req.nodes))  # 规范化：去重排序
        unknown = [n for n in nodes if n not in manifest.nodes]
        if unknown:
            raise HTTPException(
                status_code=422,
                detail={"error": "unknown_nodes", "unknown_nodes": unknown},
            )
        with store.transaction():  # 并发申请在此串行，守住多数票
            existing = store.get_permit(req.request_id)
            if existing is not None:
                if list(existing.nodes) == nodes:
                    # 同号同批重发 -> 返回原许可
                    response.status_code = 200
                    return permit_view(existing)
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error": "request_id_conflict",
                        "message": "request_id 已存在且节点集合不同",
                        "existing_nodes": list(existing.nodes),
                    },
                )
            queued = store.get_waitlist(req.request_id)
            if queued is not None:
                if list(queued.nodes) == nodes:
                    # 同号同批重发 -> 返回待批状态
                    response.status_code = 200
                    return waitlist_view(queued)
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error": "request_id_conflict",
                        "message": "request_id 已在候补队列且节点集合不同",
                        "existing_nodes": list(queued.nodes),
                    },
                )
            latest = store.latest_reports()
            ok, detail = evaluate_permit_request(
                manifest, nodes, latest, store.open_permits()
            )
            if not ok:
                if req.enqueue_if_rejected:
                    # 未获批 -> 按申请号入队候补（不占用任何节点）
                    entry = store.insert_waitlist(
                        WaitlistEntry(
                            request_id=req.request_id,
                            nodes=tuple(nodes),
                            enqueued_at=now_utc(),
                            position=0,  # 由插入时的自增序号取代
                        )
                    )
                    response.status_code = 202
                    return waitlist_view(entry, rejection=detail)
                raise HTTPException(status_code=409, detail=detail)
            record = PermitRecord(
                request_id=req.request_id,
                nodes=tuple(nodes),
                status="open",
                approved_at=now_utc(),
                finished_at=None,
                boot_ids_at_approval={n: latest[n].boot_id for n in nodes},
            )
            store.insert_permit(record)
            return permit_view(record)

    @app.get("/permits")
    def list_permits(status: str | None = Query(default=None, pattern="^(open|finished)$")):
        return {"permits": [permit_view(p) for p in store.list_permits(status)]}

    @app.get("/permits/{request_id}")
    def get_permit(request_id: str):
        p = store.get_permit(request_id)
        if p is not None:
            return permit_view(p)
        queued = store.get_waitlist(request_id)
        if queued is not None:
            return waitlist_view(queued)
        raise HTTPException(
            status_code=404,
            detail={"error": "permit_not_found", "request_id": request_id},
        )

    @app.post("/permits/{request_id}/finish")
    def finish_permit(request_id: str):
        with store.transaction():  # 结单与由此发出的候补许可一同生效
            p = store.get_permit(request_id)
            if p is None:
                raise HTTPException(
                    status_code=404,
                    detail={"error": "permit_not_found", "request_id": request_id},
                )
            if p.status == "finished":
                raise HTTPException(
                    status_code=409,
                    detail={"error": "permit_already_finished", "request_id": request_id},
                )
            problems = evaluate_finish(p, store.latest_reports())
            if problems:
                raise HTTPException(
                    status_code=409,
                    detail={"error": "finish_conditions_not_met", "nodes": problems},
                )
            store.mark_finished(request_id, now_utc())
            issued = process_waitlist()  # 结单释放节点，按入队顺序补发
            view = permit_view(store.get_permit(request_id))
            view["issued_from_waitlist"] = [permit_view(x) for x in issued]
            return view

    # ---- 候补队列 ----

    @app.get("/waitlist")
    def list_waitlist():
        return {"waitlist": [waitlist_view(e) for e in store.list_waitlist()]}

    return app


def create_app_from_env() -> FastAPI:
    return create_app(
        db_path=os.environ.get("DB_PATH", "./data/permits.db"),
        manifest_path=os.environ.get("MANIFEST_PATH", "./manifest.json"),
    )


app = create_app_from_env()
