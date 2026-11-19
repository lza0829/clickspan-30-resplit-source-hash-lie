from fastapi import APIRouter, HTTPException

from app.db import connect
from app.modules import ingest_resplit_mutex as mutex
from app.seed import idle_timeout_sec
from app.services import ingest as ingest_svc
from app.services import sessions as sess_svc

router = APIRouter(prefix="/api")


@router.get("/overview")
def overview():
    conn = connect()
    try:
        return {
            "visitors": conn.execute("SELECT COUNT(*) AS c FROM visitors").fetchone()["c"],
            "sessions": conn.execute("SELECT COUNT(*) AS c FROM sessions").fetchone()["c"],
            "hits_primary": conn.execute(
                "SELECT COUNT(*) AS c FROM hits WHERE channel='primary'"
            ).fetchone()["c"],
            "quarantine": conn.execute("SELECT COUNT(*) AS c FROM quarantine").fetchone()["c"],
            "idle_timeout_sec": idle_timeout_sec(conn),
        }
    finally:
        conn.close()


@router.get("/visitors")
def list_visitors():
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT id, label, created_at FROM visitors ORDER BY id"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@router.get("/sessions")
def list_sessions(visitor_id: str | None = None):
    conn = connect()
    try:
        return sess_svc.list_sessions(conn, visitor_id)
    finally:
        conn.close()


@router.get("/sessions/{session_id}")
def get_session(session_id: str):
    conn = connect()
    try:
        detail = sess_svc.session_detail(conn, session_id)
        if not detail:
            raise HTTPException(404, "会话不存在")
        return detail
    finally:
        conn.close()


@router.get("/quarantine")
def list_quarantine():
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT id, visitor_id, reason, member_json, created_at FROM quarantine ORDER BY id DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@router.post("/ingest/run")
def run_ingest():
    return ingest_svc.run_batch()


@router.post("/ingest/stage")
def stage_ingest():
    """只挂未提交候选（演示与重切交错写）；提交/作废前重切失败闭。"""
    return ingest_svc.stage_batch()


@router.post("/ingest/commit")
def commit_ingest(body: dict):
    batch_id = body.get("batch_id")
    if not batch_id:
        raise HTTPException(400, "缺少 batch_id")
    return ingest_svc.commit_batch(batch_id)


@router.post("/ingest/abort")
def abort_ingest(body: dict):
    batch_id = body.get("batch_id")
    if not batch_id:
        raise HTTPException(400, "缺少 batch_id")
    return ingest_svc.abort_batch(batch_id)


@router.get("/ingest/candidates")
def list_candidates():
    conn = connect()
    try:
        return {"pending": mutex.pending_candidates(conn)}
    finally:
        conn.close()


@router.post("/resplit/run")
def run_resplit(body: dict | None = None):
    """重切入口：按现行超时只重切 open 会话；撞上未提交候选/closed 即整笔回滚。"""
    body = body or {}
    conn = connect()
    try:
        idle = idle_timeout_sec(conn)
        return mutex.run_resplit(
            conn, idle, scan_from=body.get("scan_from"), scan_to=body.get("scan_to")
        )
    finally:
        conn.close()


@router.get("/resplit/batches")
def list_resplit_batches():
    """重切批次证明（扫描范围 + 改动会话 id 列表）。"""
    conn = connect()
    try:
        return mutex.list_resplit_batches(conn)
    finally:
        conn.close()


@router.get("/resplit/verify")
def verify_resplit():
    """重切隔离核证（只读）：closed 成员键 / 只动 open / 切分边 / 回执与织机一致。"""
    conn = connect()
    try:
        return mutex.verify_isolation(conn)
    finally:
        conn.close()


@router.get("/session-proofs")
def session_proofs(session_id: str | None = None):
    conn = connect()
    try:
        if session_id:
            rows = conn.execute(
                "SELECT * FROM session_proofs WHERE session_id=? ORDER BY id DESC",
                (session_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM session_proofs ORDER BY id DESC LIMIT 50"
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@router.get("/settings")
def get_settings():
    conn = connect()
    try:
        return {"idle_timeout_sec": idle_timeout_sec(conn)}
    finally:
        conn.close()


@router.put("/settings/idle-timeout")
def put_idle_timeout(body: dict):
    sec = int(body.get("idle_timeout_sec", 1800))
    if sec < 60:
        raise HTTPException(400, "空闲超时至少 60 秒")
    conn = connect()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO settings(key, value) VALUES ('idle_timeout_sec', ?)",
            (str(sec),),
        )
        conn.commit()
        return {"idle_timeout_sec": sec}
    finally:
        conn.close()


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

