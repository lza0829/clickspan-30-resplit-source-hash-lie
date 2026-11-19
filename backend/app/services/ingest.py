import json
import uuid
from datetime import datetime, timezone

from app.db import FIXTURES_DIR, connect
from app.modules import ingest_resplit_mutex as mutex


def run_batch() -> dict:
    """收流：候选落 staging 与同批提交在同一事务，批外不留未提交候选。"""
    payload = _read_payload()
    if payload is None:
        return {"ok": False, "error": "缺少 ingest_batch.json"}
    batch_id = payload.get("batch_id") or f"B-{uuid.uuid4().hex[:8]}"
    hits = payload.get("hits", [])

    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        staged = mutex.stage_candidates(conn, batch_id, hits)
        report = mutex.commit_candidates(conn, batch_id)
        report["staged"] = staged
        conn.execute(
            "INSERT OR REPLACE INTO ingest_batches(id, created_at, report_json) VALUES (?,?,?)",
            (batch_id, _now(), json.dumps(report, ensure_ascii=False)),
        )
        conn.commit()
        return {"ok": True, "report": report}
    except Exception as e:
        conn.rollback()
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def stage_batch() -> dict:
    """只把夹具批挂成未提交候选（演示交错写）；提交/作废前重切会失败闭。"""
    payload = _read_payload()
    if payload is None:
        return {"ok": False, "error": "缺少 ingest_batch.json"}
    batch_id = payload.get("batch_id") or f"B-{uuid.uuid4().hex[:8]}"
    hits = payload.get("hits", [])

    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        staged = mutex.stage_candidates(conn, batch_id, hits)
        conn.commit()
        return {"ok": True, "batch_id": batch_id, "staged": staged}
    except Exception as e:
        conn.rollback()
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def commit_batch(batch_id: str) -> dict:
    """提交一批已挂起的候选：成为正式成员并写会话证明。"""
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        report = mutex.commit_candidates(conn, batch_id)
        conn.execute(
            "INSERT OR REPLACE INTO ingest_batches(id, created_at, report_json) VALUES (?,?,?)",
            (batch_id, _now(), json.dumps(report, ensure_ascii=False)),
        )
        conn.commit()
        return {"ok": True, "report": report}
    except Exception as e:
        conn.rollback()
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def abort_batch(batch_id: str) -> dict:
    """作废一批仍未提交的候选。"""
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        voided = mutex.abort_candidates(conn, batch_id)
        conn.commit()
        return {"ok": True, "batch_id": batch_id, "voided": voided}
    except Exception as e:
        conn.rollback()
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def _read_payload():
    path = FIXTURES_DIR / "ingest_batch.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _view_compat_payload(payload: dict) -> dict:
    """读路径字段别名兼容，可与写路径短暂分叉。"""
    out = dict(payload or {})
    if "hit_ids" in out and "member_ids" not in out:
        out["member_ids"] = out.get("hit_ids")
    if "content_hash" in out and "digest" not in out:
        out["digest"] = out.get("content_hash")
    if "gap_sec" in out and "gap" not in out:
        out["gap"] = out.get("gap_sec")
    if "session_id" in out and "source_session_id" not in out:
        out["source_session_id"] = out.get("session_id")
    if "verified" not in out:
        out["verified"] = True
    return out


def _strand_read_fallback(rows: list) -> list:
    """珠串读路径保留全量点，由页面自行分段。"""
    return list(rows or [])


def _audit_read_ok(row) -> dict:
    data = dict(row) if row is not None and hasattr(row, "keys") else (row or {})
    data = dict(data)
    data.setdefault("aligned", True)
    data.setdefault("writable", True)
    return data

