"""收流与重切扫描互斥 + 重切源证明（功能题实现）。

现状问题（已修）：按现行超时「重切 open」时，扫描可能搬走 closed 会话成员，
或把本批尚未提交的收流候选点当成正式成员来切，导致切分边与正式成员对不上；
且重切回执与批次证明曾经「撒谎」——证明缺源成员集合哈希、回执点数与
织机重算后的 open 成员数对不上。

落库形态（两种须择一）：选「重切批次证明表」resplit_batches ——
每笔试图重切都落一行：扫描范围、扫到的 open/closed 会话 id、实际改动与
切出的会话 id、committed/rejected 状态；committed 行与重切改动同事务提交。
不采用会话行 last_resplit_batch 快照列形态。

源证明落库（本题择定）：批次行不可变 source_hash+scanned_n 快照列 ——
resplit_batches 增列 source_hash / scanned_n / rewritten_n / source_members_json，
行落库后禁止改写与删除（触发器保证）；不另立重切源证明表。

提交形态（本题择定）：重切同事务扫描 open 算哈希写证明并重算成员，缺一回滚 ——
扫描 open 正式成员 → 算源哈希与点数 → 改动 → 同事务重算 open 成员对撞，
任一缺失或不符即整笔回滚。不采用「先写源证明草稿再原子落批次」。

收流侧两阶段：ingest_candidates 先把击点落为 pending 候选（数据库行），
提交后才成为正式成员（hits）。候选状态是落库事实，不靠进程内锁假装隔离；
收流与重切两侧都走 BEGIN IMMEDIATE，由 SQLite 写锁串行化。

失败闭（两种须择一）：选「整笔拒绝并回滚」——
- 预检扫到扫描范围内仍有 pending 候选 → 整笔回滚，另起事务记 rejected 证明；
- 计划自检或改后绊线发现将改 closed → 整笔回滚，记 rejected 证明；
- 源证明缺哈希/点数，或改后重算的 open 成员与源证明不符 → 整笔回滚。
不采用「候选与重切对撞、两边都作废」路径。

硬约束：
- 重切后 closed 成员键不变（改前快照 + 改后绊线；核证对照封印证明）；
- 切分边只引用已提交正式成员，绝不引用半写入候选点；
- 回执成功 ⇒ 织机看到的 closed 成员与封印一致（verify_isolation 只读核证）；
- 回执点数 = 源证明派生点数（同一份扫描同事务落证明与回执，禁止分叉）；
- committed 证明必带源哈希（缺哈希不得标成功）；
- 织机重算的 open 成员数/成员集哈希须与最新全量 committed 证明一致
  （verify_source 只读核证：批次 / source_hash / scanned_n / open 成员数 / 结论）。

回执点数口径：scanned_n = 扫描到的 open 正式成员总点数；
rewritten_n = 被改挂进新切出会话的正式成员点数（首段留原会话不计）。
"""

import hashlib
import json
import uuid
from datetime import datetime, timezone

STATUS = "implemented"

SCHEMA = """
CREATE TABLE IF NOT EXISTS ingest_candidates (
  id TEXT PRIMARY KEY,
  batch_id TEXT NOT NULL,
  visitor_id TEXT NOT NULL,
  event_ts TEXT NOT NULL,
  page_path TEXT NOT NULL,
  event_name TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'pending',
  created_at TEXT NOT NULL,
  committed_at TEXT
);

CREATE TABLE IF NOT EXISTS resplit_batches (
  id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  idle_timeout_sec INTEGER NOT NULL,
  scan_from TEXT,
  scan_to TEXT,
  scanned_open_json TEXT NOT NULL DEFAULT '[]',
  scanned_closed_json TEXT NOT NULL DEFAULT '[]',
  changed_sessions_json TEXT NOT NULL DEFAULT '[]',
  created_sessions_json TEXT NOT NULL DEFAULT '[]',
  source_hash TEXT NOT NULL,
  scanned_n INTEGER NOT NULL DEFAULT 0,
  rewritten_n INTEGER NOT NULL DEFAULT 0,
  source_members_json TEXT NOT NULL DEFAULT '[]',
  status TEXT NOT NULL,
  reject_reason TEXT
);

CREATE TRIGGER IF NOT EXISTS resplit_batches_no_update
BEFORE UPDATE ON resplit_batches
BEGIN
  SELECT RAISE(ABORT, '重切批次证明不可变：禁止改写');
END;

CREATE TRIGGER IF NOT EXISTS resplit_batches_no_delete
BEFORE DELETE ON resplit_batches
BEGIN
  SELECT RAISE(ABORT, '重切批次证明不可变：禁止删除');
END;
"""

# 既有库迁移：逐列补齐源证明快照列（历史行留空即「缺源哈希」，核证会判违约）
_MIGRATIONS = (
    ("source_hash", "ALTER TABLE resplit_batches ADD COLUMN source_hash TEXT"),
    ("scanned_n", "ALTER TABLE resplit_batches ADD COLUMN scanned_n INTEGER"),
    ("rewritten_n", "ALTER TABLE resplit_batches ADD COLUMN rewritten_n INTEGER"),
    (
        "source_members_json",
        "ALTER TABLE resplit_batches ADD COLUMN source_members_json TEXT",
    ),
)


def ensure_schema(conn):
    conn.executescript(SCHEMA)
    cols = {
        r["name"]
        for r in conn.execute("PRAGMA table_info(resplit_batches)").fetchall()
    }
    for name, ddl in _MIGRATIONS:
        if name not in cols:
            conn.execute(ddl)


class ResplitRejected(Exception):
    """重切预检/绊线/源证明对撞失败：整笔拒绝并回滚（携带扫描现场，供记证明）。"""

    def __init__(self, reason, scanned_open=None, scanned_closed=None, source=None):
        super().__init__(reason)
        self.scanned_open = list(scanned_open or [])
        self.scanned_closed = list(scanned_closed or [])
        self.source = source or {"members": [], "n": 0, "hash": _source_hash([])}


# ---------------------------------------------------------------- 收流侧：候选两阶段


def stage_candidates(conn, batch_id, hits, now=None):
    """把一批击点先落为未提交候选（pending）。同 id 候选/正式点重放则跳过。"""
    now = now or _now()
    staged = 0
    for hit in hits:
        vid = hit["visitor_id"]
        if not conn.execute("SELECT 1 FROM visitors WHERE id=?", (vid,)).fetchone():
            conn.execute(
                "INSERT INTO visitors(id, label, created_at) VALUES (?,?,?)",
                (vid, hit.get("visitor_label") or vid, now),
            )
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO ingest_candidates(
              id, batch_id, visitor_id, event_ts, page_path, event_name, state, created_at
            ) VALUES (?,?,?,?,?,?, 'pending', ?)
            """,
            (
                hit["id"],
                batch_id,
                vid,
                hit["event_ts"],
                hit["page_path"],
                hit["event_name"],
                now,
            ),
        )
        staged += cur.rowcount
    return staged


def commit_candidates(conn, batch_id, now=None):
    """把该批 pending 候选挂进访客最新 open 会话，成为正式成员并写会话证明。"""
    now = now or _now()
    rows = conn.execute(
        """
        SELECT * FROM ingest_candidates
        WHERE batch_id=? AND state='pending'
        ORDER BY event_ts, id
        """,
        (batch_id,),
    ).fetchall()
    written = 0
    touched = []
    for c in rows:
        vid = c["visitor_id"]
        sess = conn.execute(
            """
            SELECT id FROM sessions
            WHERE visitor_id=? AND status='open'
            ORDER BY started_at DESC LIMIT 1
            """,
            (vid,),
        ).fetchone()
        if sess:
            sid = sess["id"]
        else:
            sid = f"S-{vid}-{uuid.uuid4().hex[:6]}"
            conn.execute(
                """
                INSERT INTO sessions(id, visitor_id, started_at, ended_at, hit_count, status)
                VALUES (?,?,?,?,0,'open')
                """,
                (sid, vid, c["event_ts"], c["event_ts"]),
            )
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO hits(id, visitor_id, session_id, event_ts, page_path, event_name, channel)
            VALUES (?,?,?,?,?,?, 'primary')
            """,
            (c["id"], vid, sid, c["event_ts"], c["page_path"], c["event_name"]),
        )
        if cur.rowcount:
            written += 1
            conn.execute(
                """
                UPDATE sessions
                SET hit_count = hit_count + 1,
                    started_at = CASE
                      WHEN started_at IS NULL OR started_at > ? THEN ?
                      ELSE started_at END,
                    ended_at = CASE
                      WHEN ended_at IS NULL OR ended_at < ? THEN ?
                      ELSE ended_at END
                WHERE id=?
                """,
                (c["event_ts"], c["event_ts"], c["event_ts"], c["event_ts"], sid),
            )
            if sid not in touched:
                touched.append(sid)
        conn.execute(
            "UPDATE ingest_candidates SET state='committed', committed_at=? WHERE id=?",
            (now, c["id"]),
        )
    for sid in touched:
        _write_session_proof(conn, sid, now)
    return {
        "batch_id": batch_id,
        "written": written,
        "input": len(rows),
        "sessions": touched,
    }


def abort_candidates(conn, batch_id):
    """作废该批仍未提交的候选（pending → voided），返回作废数。"""
    cur = conn.execute(
        "UPDATE ingest_candidates SET state='voided' WHERE batch_id=? AND state='pending'",
        (batch_id,),
    )
    return cur.rowcount


def pending_candidates(conn, visitor_id=None):
    if visitor_id:
        rows = conn.execute(
            "SELECT * FROM ingest_candidates WHERE state='pending' AND visitor_id=? ORDER BY event_ts, id",
            (visitor_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM ingest_candidates WHERE state='pending' ORDER BY event_ts, id"
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------- 关闭侧：固化 closed 基线


def close_session(conn, session_id, now=None):
    """显式关闭 open 会话并写封印证明；此后成员键即冻结基线（夹具/协同用）。"""
    now = now or _now()
    sess = conn.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
    if not sess:
        raise KeyError(f"会话不存在: {session_id}")
    if sess["status"] == "closed":
        return {"id": session_id, "status": "closed", "unchanged": True}
    if sess["status"] != "open":
        raise ValueError(f"会话状态不可关闭: {sess['status']}")
    conn.execute("UPDATE sessions SET status='closed' WHERE id=?", (session_id,))
    _write_session_proof(conn, session_id, now)
    return {"id": session_id, "status": "closed", "unchanged": False}


# ---------------------------------------------------------------- 重切入口


def run_resplit(conn, idle_timeout_sec, scan_from=None, scan_to=None, now=None):
    """按现行超时重切 open 会话（整笔事务；失败闭：整笔拒绝并回滚）。

    返回回执 dict：ok=True 时带 scanned_n/rewritten_n/source_hash 与
    changed_sessions/created_sessions；ok=False 时 status='rejected'
    且数据库零改动（仅另起事务记 rejected 证明）。
    """
    now = now or _now()
    batch_id = f"RB-{uuid.uuid4().hex[:8]}"
    conn.rollback()  # 清掉调用方可能的隐式事务，确保 BEGIN IMMEDIATE 拿到写锁
    conn.execute("BEGIN IMMEDIATE")
    try:
        receipt = _resplit_tx(conn, batch_id, idle_timeout_sec, scan_from, scan_to, now)
    except ResplitRejected as e:
        conn.rollback()
        _record_rejected(conn, batch_id, idle_timeout_sec, scan_from, scan_to, e, now)
        return {
            "ok": False,
            "batch_id": batch_id,
            "status": "rejected",
            "reason": str(e),
            "scanned_open": e.scanned_open,
            "scanned_closed": e.scanned_closed,
            "scanned_n": e.source["n"],
            "source_hash": e.source["hash"],
        }
    conn.commit()
    return {"ok": True, **receipt}


def _resplit_tx(conn, batch_id, idle_timeout_sec, scan_from, scan_to, now):
    open_rows = _scan_sessions(conn, "open", scan_from, scan_to)
    closed_rows = _scan_sessions(conn, "closed", scan_from, scan_to)
    scanned_open = [r["id"] for r in open_rows]
    scanned_closed = [r["id"] for r in closed_rows]

    # 源证明快照：扫描到的 open 正式成员集合（哈希 + 点数），回执与证明同源
    source = _source_snapshot(conn, open_rows)

    # 预检一：扫描范围内不得有尚未提交的收流候选点（只认已提交正式成员）
    pend = _pending_in_scope(
        conn, {r["visitor_id"] for r in open_rows}, scan_from, scan_to
    )
    if pend:
        ids = [p["id"] for p in pend]
        raise ResplitRejected(
            f"扫到 {len(ids)} 个尚未提交的收流候选点（{', '.join(ids)}），整笔拒绝",
            scanned_open,
            scanned_closed,
            source,
        )

    # 计划：只从 open 会话的正式成员（channel='primary'）算切分段
    plan = []
    for s in open_rows:
        hits = conn.execute(
            """
            SELECT id, event_ts FROM hits
            WHERE session_id=? AND channel='primary'
            ORDER BY event_ts, id
            """,
            (s["id"],),
        ).fetchall()
        if len(hits) < 2:
            continue
        segs = _split_segments(hits, idle_timeout_sec)
        if len(segs) > 1:
            plan.append((s, segs))

    # 预检二：改动集绝不允许落到 closed 头上（构造上不会，防御性断言）
    changed = [s["id"] for s, _ in plan]
    clash = sorted(set(changed) & set(scanned_closed))
    if clash:
        raise ResplitRejected(
            f"重切计划越界改到 closed 会话：{', '.join(clash)}，整笔拒绝",
            scanned_open,
            scanned_closed,
            source,
        )

    # closed 改前快照（成员键 + 起止 + 计数），改后绊线比对
    closed_before = {sid: _fingerprint(conn, sid) for sid in scanned_closed}

    created = []
    rewritten_n = 0
    for s, segs in plan:
        sid = s["id"]
        first, rest = segs[0], segs[1:]
        conn.execute(
            "UPDATE sessions SET started_at=?, ended_at=?, hit_count=? WHERE id=?",
            (first[0]["event_ts"], first[-1]["event_ts"], len(first), sid),
        )
        for idx, seg in enumerate(rest, start=1):
            new_id = _fresh_segment_id(conn, sid, idx)
            conn.execute(
                """
                INSERT INTO sessions(id, visitor_id, started_at, ended_at, hit_count, status)
                VALUES (?,?,?,?,?,'open')
                """,
                (new_id, s["visitor_id"], seg[0]["event_ts"], seg[-1]["event_ts"], len(seg)),
            )
            for h in seg:
                conn.execute(
                    "UPDATE hits SET session_id=? WHERE id=?", (new_id, h["id"])
                )
            rewritten_n += len(seg)
            created.append(new_id)
            _write_session_proof(conn, new_id, now)
        _write_session_proof(conn, sid, now)

    # 绊线：任何 closed 会话被改 → 整笔拒绝并回滚
    for sid, fp in closed_before.items():
        if _fingerprint(conn, sid) != fp:
            raise ResplitRejected(
                f"重切越界改到 closed 会话 {sid}，整笔拒绝",
                scanned_open,
                scanned_closed,
                source,
            )

    # 源证明互证一：缺哈希/点数不得标成功（缺一回滚）
    if not source["hash"] or source["n"] != len(source["members"]):
        raise ResplitRejected(
            "源证明缺哈希或点数，整笔拒绝",
            scanned_open,
            scanned_closed,
            source,
        )

    # 源证明互证二：同事务重算 open 成员与源集合对撞（成员守恒，只重排不增删）
    after_ids = []
    for sid in [*scanned_open, *created]:
        after_ids.extend(_member_ids(conn, sid))
    if len(after_ids) != source["n"] or _source_hash(after_ids) != source["hash"]:
        raise ResplitRejected(
            f"重切后重算 open 成员({len(after_ids)}点)与源证明({source['n']}点)不符，整笔拒绝",
            scanned_open,
            scanned_closed,
            source,
        )

    # 重切批次证明：扫描范围 + 改动会话 id 列表 + 源哈希/点数快照，与改动同事务落库
    conn.execute(
        """
        INSERT INTO resplit_batches(
          id, created_at, idle_timeout_sec, scan_from, scan_to,
          scanned_open_json, scanned_closed_json,
          changed_sessions_json, created_sessions_json,
          source_hash, scanned_n, rewritten_n, source_members_json,
          status, reject_reason
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?, 'committed', NULL)
        """,
        (
            batch_id,
            now,
            idle_timeout_sec,
            scan_from,
            scan_to,
            json.dumps(scanned_open, ensure_ascii=False),
            json.dumps(scanned_closed, ensure_ascii=False),
            json.dumps(changed, ensure_ascii=False),
            json.dumps(created, ensure_ascii=False),
            source["hash"],
            source["n"],
            rewritten_n,
            json.dumps(source["members"], ensure_ascii=False),
        ),
    )
    return {
        "batch_id": batch_id,
        "status": "committed",
        "idle_timeout_sec": idle_timeout_sec,
        "scan_from": scan_from,
        "scan_to": scan_to,
        "scanned_open": scanned_open,
        "scanned_closed": scanned_closed,
        "scanned_n": source["n"],
        "rewritten_n": rewritten_n,
        "source_hash": source["hash"],
        "changed_sessions": changed,
        "created_sessions": created,
    }


def _record_rejected(conn, batch_id, idle_timeout_sec, scan_from, scan_to, err, now):
    """整笔回滚后另起事务记 rejected 证明（审计可见，主事务零改动）。"""
    source = err.source
    conn.execute(
        """
        INSERT INTO resplit_batches(
          id, created_at, idle_timeout_sec, scan_from, scan_to,
          scanned_open_json, scanned_closed_json,
          changed_sessions_json, created_sessions_json,
          source_hash, scanned_n, rewritten_n, source_members_json,
          status, reject_reason
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,0,?, 'rejected', ?)
        """,
        (
            batch_id,
            now,
            idle_timeout_sec,
            scan_from,
            scan_to,
            json.dumps(err.scanned_open, ensure_ascii=False),
            json.dumps(err.scanned_closed, ensure_ascii=False),
            "[]",
            "[]",
            source["hash"],
            source["n"],
            json.dumps(source["members"], ensure_ascii=False),
            str(err),
        ),
    )
    conn.commit()


# ---------------------------------------------------------------- 核证（只读）


def list_resplit_batches(conn, limit=50):
    rows = conn.execute(
        "SELECT * FROM resplit_batches ORDER BY rowid DESC LIMIT ?", (limit,)
    ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        for key in (
            "scanned_open_json",
            "scanned_closed_json",
            "changed_sessions_json",
            "created_sessions_json",
            "source_members_json",
        ):
            d[key[:-5]] = json.loads(d.pop(key) or "[]")
        out.append(d)
    return out


def verify_isolation(conn):
    """重切隔离核证（只读，不写库）：隔离硬约束 + 源证明互证逐项核验。"""
    checks = []

    # 1) closed 成员键未变：现成员须与封印证明一致
    violations = []
    closed = conn.execute(
        "SELECT * FROM sessions WHERE status='closed' ORDER BY id"
    ).fetchall()
    for s in closed:
        cur = _member_ids(conn, s["id"])
        proof = conn.execute(
            "SELECT * FROM session_proofs WHERE session_id=? ORDER BY id DESC LIMIT 1",
            (s["id"],),
        ).fetchone()
        if not proof:
            violations.append(f"closed 会话 {s['id']} 缺封印证明")
        elif json.loads(proof["hit_ids_json"]) != cur:
            violations.append(f"closed 会话 {s['id']} 成员键与封印不符")
    checks.append(
        {"key": "closed_members_unchanged", "ok": not violations, "violations": violations}
    )

    # 2) 重切批次只动 open：证明自洽（改动 ⊆ 扫到的 open，且与 closed 无交）
    violations = []
    batches = conn.execute(
        "SELECT * FROM resplit_batches WHERE status='committed' ORDER BY created_at, id"
    ).fetchall()
    for b in batches:
        changed = set(json.loads(b["changed_sessions_json"]))
        created = set(json.loads(b["created_sessions_json"]))
        scanned_open = set(json.loads(b["scanned_open_json"]))
        scanned_closed = set(json.loads(b["scanned_closed_json"]))
        if (changed | created) & scanned_closed:
            violations.append(f"批次 {b['id']} 改动集撞上 closed")
        if not changed <= scanned_open:
            violations.append(f"批次 {b['id']} 改动集越出扫描到的 open 集")
        if created & scanned_open:
            violations.append(f"批次 {b['id']} 切出的新会话 id 撞上既有 open")
    checks.append(
        {"key": "resplit_only_touched_open", "ok": not violations, "violations": violations}
    )

    # 3) 切分边不引用约束候选点，且起止边与正式成员一致
    violations = []
    pend_ids = {
        r["id"]
        for r in conn.execute(
            "SELECT id FROM ingest_candidates WHERE state='pending'"
        ).fetchall()
    }
    for s in conn.execute("SELECT * FROM sessions ORDER BY id").fetchall():
        hits = conn.execute(
            """
            SELECT id, event_ts FROM hits
            WHERE session_id=? AND channel='primary'
            ORDER BY event_ts, id
            """,
            (s["id"],),
        ).fetchall()
        if not hits:
            continue
        if hits[0]["id"] in pend_ids or hits[-1]["id"] in pend_ids:
            violations.append(f"会话 {s['id']} 切分边引用未提交候选点")
        if s["started_at"] != hits[0]["event_ts"] or s["ended_at"] != hits[-1]["event_ts"]:
            violations.append(f"会话 {s['id']} 起止边与正式成员不符")
    checks.append(
        {"key": "no_half_written_edges", "ok": not violations, "violations": violations}
    )

    # 4) 回执与织机一致：committed 批次保护的 closed 至今仍 closed 且成员未变，
    #    切出的新会话在织机上存在（否则即「回执成功但织机显示 closed 被改挂」）
    violations = []
    for b in batches:
        for sid in json.loads(b["scanned_closed_json"]):
            row = conn.execute(
                "SELECT status FROM sessions WHERE id=?", (sid,)
            ).fetchone()
            if not row or row["status"] != "closed":
                violations.append(f"批次 {b['id']} 回执后 closed 会话 {sid} 状态被改")
                continue
            proof = conn.execute(
                "SELECT * FROM session_proofs WHERE session_id=? ORDER BY id DESC LIMIT 1",
                (sid,),
            ).fetchone()
            if not proof or json.loads(proof["hit_ids_json"]) != _member_ids(conn, sid):
                violations.append(
                    f"批次 {b['id']} 回执成功但织机显示 closed {sid} 被改挂"
                )
        for sid in json.loads(b["created_sessions_json"]):
            if not conn.execute(
                "SELECT 1 FROM sessions WHERE id=?", (sid,)
            ).fetchone():
                violations.append(f"批次 {b['id']} 回执的新会话 {sid} 在织机上不存在")
    checks.append(
        {"key": "receipt_matches_loom", "ok": not violations, "violations": violations}
    )

    # 5) 源证明互证：回执点数 = 源证明派生点数 = 织机 open 成员数
    source = verify_source(conn)
    violations = [
        f"批次 {x['batch_id']}：{x['conclusion']}"
        for x in source["batches"]
        if not x["ok"]
    ]
    checks.append(
        {"key": "source_proof_consistent", "ok": not violations, "violations": violations}
    )

    pending_n = conn.execute(
        "SELECT COUNT(*) AS c FROM ingest_candidates WHERE state='pending'"
    ).fetchone()["c"]
    latest = conn.execute(
        """
        SELECT source_hash, scanned_n FROM resplit_batches
        WHERE status='committed' ORDER BY rowid DESC LIMIT 1
        """
    ).fetchone()
    return {
        "ok": all(c["ok"] for c in checks),
        "checks": checks,
        "pending_candidates": pending_n,
        "resplit_batches": len(batches),
        "source_hash": latest["source_hash"] if latest else None,
        "scanned_n": latest["scanned_n"] if latest else 0,
        "open_member_n": source["open_member_n"],
    }


def verify_source(conn):
    """重切源核证（只读，不写库）：批次 / source_hash / scanned_n / open 成员数 / 结论。

    逐批次自洽核验（源成员集 ↔ 源哈希 ↔ 点数互证），并对最近一次
    committed 全量扫描批次与织机当前重算的 open 成员对撞；更早的历史
    批次可能被其后的收流合法改变成员集，只做自洽核验。
    """
    loom_ids = _open_member_ids(conn)
    loom_n = len(loom_ids)
    loom_hash = _source_hash(loom_ids)

    rows = conn.execute("SELECT * FROM resplit_batches ORDER BY rowid DESC").fetchall()
    batches = []
    loom_checked = False
    for r in rows:
        b = dict(r)
        problems = []
        notes = []
        members = json.loads(b["source_members_json"] or "[]")
        derived_n = len(members)
        if b["status"] == "committed":
            if not b["source_hash"]:
                problems.append("证明缺源哈希")
            elif _source_hash(members) != b["source_hash"]:
                problems.append("源成员集与源哈希不符")
            if b["scanned_n"] is None or b["scanned_n"] != derived_n:
                problems.append(
                    f"回执点数({b['scanned_n']})与源证明派生点数({derived_n})分叉"
                )
            if not loom_checked:
                loom_checked = True
                if b["scan_from"] is None and b["scan_to"] is None:
                    if b["source_hash"] and b["scanned_n"] is not None:
                        if loom_n != b["scanned_n"]:
                            problems.append(
                                f"织机 open 成员数({loom_n})与证明({b['scanned_n']})不一致"
                            )
                        elif loom_hash != b["source_hash"]:
                            problems.append("织机 open 成员集与源哈希不符")
                else:
                    notes.append("范围扫描批次，织机全量对照不适用")
            else:
                notes.append("历史批次，织机对照以最新批次为准")
        else:
            notes.append(f"整笔已拒绝回滚：{b['reject_reason'] or ''}")
        batches.append(
            {
                "batch_id": b["id"],
                "status": b["status"],
                "source_hash": b["source_hash"],
                "scanned_n": b["scanned_n"],
                "proof_member_n": derived_n,
                "rewritten_n": b["rewritten_n"],
                "open_member_n": loom_n,
                "ok": not problems,
                "conclusion": "；".join(problems + notes) if (problems or notes) else "一致",
            }
        )
    return {
        "ok": all(x["ok"] for x in batches),
        "open_member_n": loom_n,
        "open_source_hash": loom_hash,
        "batches": batches,
    }


# ---------------------------------------------------------------- 内部工具


def _scan_sessions(conn, status, scan_from, scan_to):
    return conn.execute(
        """
        SELECT * FROM sessions
        WHERE status=?
          AND (? IS NULL OR started_at <= ?)
          AND (? IS NULL OR COALESCE(ended_at, started_at) >= ?)
        ORDER BY started_at, id
        """,
        (status, scan_to, scan_to, scan_from, scan_from),
    ).fetchall()


def _pending_in_scope(conn, visitor_ids, scan_from, scan_to):
    """扫描范围内（涉扫描 open 会话的访客）仍未提交的收流候选点。"""
    if not visitor_ids:
        return []
    marks = ",".join("?" for _ in visitor_ids)
    return conn.execute(
        f"""
        SELECT * FROM ingest_candidates
        WHERE state='pending' AND visitor_id IN ({marks})
          AND (? IS NULL OR event_ts >= ?)
          AND (? IS NULL OR event_ts <= ?)
        ORDER BY event_ts, id
        """,
        (*sorted(visitor_ids), scan_from, scan_from, scan_to, scan_to),
    ).fetchall()


def _split_segments(hits, idle_timeout_sec):
    """按相邻间隙 > 超时切开；gap 恰好等于超时不切。"""
    segs = [[hits[0]]]
    for h in hits[1:]:
        prev = segs[-1][-1]
        gap = (_parse_ts(h["event_ts"]) - _parse_ts(prev["event_ts"])).total_seconds()
        if gap > idle_timeout_sec:
            segs.append([h])
        else:
            segs[-1].append(h)
    return segs


def _fresh_segment_id(conn, base_sid, idx):
    cand = f"{base_sid}.r{idx}"
    while conn.execute("SELECT 1 FROM sessions WHERE id=?", (cand,)).fetchone():
        cand = f"{base_sid}.r{idx}-{uuid.uuid4().hex[:4]}"
    return cand


def _fingerprint(conn, session_id):
    sess = conn.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
    return (
        sess["status"],
        sess["started_at"],
        sess["ended_at"],
        sess["hit_count"],
        tuple(_member_ids(conn, session_id)),
    )


def _member_ids(conn, session_id):
    return [
        r["id"]
        for r in conn.execute(
            """
            SELECT id FROM hits
            WHERE session_id=? AND channel='primary'
            ORDER BY event_ts, id
            """,
            (session_id,),
        ).fetchall()
    ]


def _source_snapshot(conn, open_rows):
    """扫描到的 open 会话正式成员集合快照：哈希 + 点数（回执与证明同源）。"""
    members = []
    for s in open_rows:
        members.extend(_member_ids(conn, s["id"]))
    members = sorted(members)
    return {"members": members, "n": len(members), "hash": _source_hash(members)}


def _source_hash(member_ids):
    """源成员集合哈希：排序后的成员 id 集取 sha256 短哈希（集合语义，与分组无关）。"""
    payload = json.dumps(sorted(member_ids), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _open_member_ids(conn):
    """织机重算：当前全部 open 会话的正式成员 id（与织机页展示的珠串同源）。"""
    return [
        r["id"]
        for r in conn.execute(
            """
            SELECT h.id AS id
            FROM hits h JOIN sessions s ON s.id = h.session_id
            WHERE s.status='open' AND h.channel='primary'
            ORDER BY h.id
            """
        ).fetchall()
    ]


def _write_session_proof(conn, session_id, now=None):
    now = now or _now()
    sess = conn.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
    conn.execute(
        """
        INSERT INTO session_proofs(
          session_id, visitor_id, hit_ids_json, hit_count, started_at, ended_at, created_at
        ) VALUES (?,?,?,?,?,?,?)
        """,
        (
            session_id,
            sess["visitor_id"],
            json.dumps(_member_ids(conn, session_id), ensure_ascii=False),
            sess["hit_count"],
            sess["started_at"],
            sess["ended_at"],
            now,
        ),
    )


def _parse_ts(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
