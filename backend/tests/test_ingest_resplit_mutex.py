"""ingest_resplit_mutex 夹具测试（本地 SQLite）：

- closed 与 open 并存时，重切只动 open；
- 收流候选未提交时触发重切应失败闭（整笔拒绝并回滚）；
- 核证只读接口能抓到「回执成功但织机显示 closed 被改挂」；
- 重切源证明与回执点数互证：回执 n = 证明派生 n = open 成员数。
"""

import json
import sqlite3
from pathlib import Path

import pytest

from app import db, seed
from app.modules import ingest_resplit_mutex as mutex
from app.services import ingest as ingest_svc

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "resplit_mutex.json"


@pytest.fixture()
def conn(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "test.db")
    c = db.connect()
    c.executescript(seed.SCHEMA)
    mutex.ensure_schema(c)
    c.execute("INSERT INTO settings(key, value) VALUES ('idle_timeout_sec', '1800')")
    c.commit()
    yield c
    c.close()


def _insert_session(conn, spec, status):
    hits = sorted(spec["hits"], key=lambda h: (h["event_ts"], h["id"]))
    conn.execute(
        "INSERT INTO sessions(id, visitor_id, started_at, ended_at, hit_count, status)"
        " VALUES (?,?,?,?,?,?)",
        (
            spec["id"],
            spec["visitor_id"],
            hits[0]["event_ts"],
            hits[-1]["event_ts"],
            len(hits),
            status,
        ),
    )
    for h in hits:
        conn.execute(
            "INSERT INTO hits(id, visitor_id, session_id, event_ts, page_path, event_name, channel)"
            " VALUES (?,?,?,?,?,?,'primary')",
            (h["id"], spec["visitor_id"], spec["id"], h["event_ts"], h["page_path"], h["event_name"]),
        )
    conn.execute(
        "INSERT INTO session_proofs(session_id, visitor_id, hit_ids_json, hit_count,"
        " started_at, ended_at, created_at) VALUES (?,?,?,?,?,?,datetime('now'))",
        (
            spec["id"],
            spec["visitor_id"],
            json.dumps([h["id"] for h in hits]),
            len(hits),
            hits[0]["event_ts"],
            hits[-1]["event_ts"],
        ),
    )


def load_fixture(conn):
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    for v in data["visitors"]:
        conn.execute(
            "INSERT INTO visitors(id, label, created_at) VALUES (?,?,?)",
            (v["id"], v["label"], v["created_at"]),
        )
    for spec in data.get("closed_sessions", []):
        _insert_session(conn, spec, "open")
        mutex.close_session(conn, spec["id"])  # 关闭并写封印证明
    for spec in data.get("open_sessions", []):
        _insert_session(conn, spec, "open")
    conn.commit()
    return data


def member_ids(conn, sid):
    return [
        r["id"]
        for r in conn.execute(
            "SELECT id FROM hits WHERE session_id=? ORDER BY event_ts, id", (sid,)
        ).fetchall()
    ]


def session_row(conn, sid):
    r = conn.execute("SELECT * FROM sessions WHERE id=?", (sid,)).fetchone()
    return dict(r) if r else None


class TestResplitOnlyTouchesOpen:
    """closed 与 open 并存：重切只动 open。"""

    def test_closed_untouched_open_split(self, conn):
        load_fixture(conn)
        closed_before = session_row(conn, "S-V-2001-0")

        receipt = mutex.run_resplit(conn, 1800)

        assert receipt["ok"], receipt
        assert receipt["changed_sessions"] == ["S-V-2001-1"]
        assert receipt["created_sessions"] == ["S-V-2001-1.r1"]
        assert receipt["scanned_open"] == ["S-V-2001-1", "S-V-2002-1"]
        assert receipt["scanned_closed"] == ["S-V-2001-0"]

        # closed 行与成员键原封不动
        assert session_row(conn, "S-V-2001-0") == closed_before
        assert member_ids(conn, "S-V-2001-0") == ["HC1", "HC2"]

        # open 按超时切开：首段留原会话，尾段进新会话，均保持 open
        assert member_ids(conn, "S-V-2001-1") == ["HO1", "HO2"]
        assert member_ids(conn, "S-V-2001-1.r1") == ["HO3", "HO4"]
        head = session_row(conn, "S-V-2001-1")
        tail = session_row(conn, "S-V-2001-1.r1")
        assert (
            head["started_at"],
            head["ended_at"],
            head["hit_count"],
            head["status"],
        ) == ("2026-11-18T09:00:00Z", "2026-11-18T09:03:00Z", 2, "open")
        assert (
            tail["started_at"],
            tail["ended_at"],
            tail["hit_count"],
            tail["status"],
        ) == ("2026-11-18T10:30:00Z", "2026-11-18T10:31:00Z", 2, "open")

        # 间隙未超窗的 open 不动
        assert member_ids(conn, "S-V-2002-1") == ["HP1", "HP2"]

        # 批次证明落库：扫描范围 + 改动会话 id 列表
        batches = mutex.list_resplit_batches(conn)
        assert len(batches) == 1
        assert batches[0]["status"] == "committed"
        assert batches[0]["changed_sessions"] == ["S-V-2001-1"]
        assert batches[0]["created_sessions"] == ["S-V-2001-1.r1"]
        assert batches[0]["scanned_closed"] == ["S-V-2001-0"]

        # 核证通过；同超时再切幂等（零改动）
        assert mutex.verify_isolation(conn)["ok"]
        again = mutex.run_resplit(conn, 1800)
        assert again["ok"] and again["changed_sessions"] == []
        assert again["created_sessions"] == []

    def test_resplit_respects_current_timeout(self, conn):
        load_fixture(conn)
        receipt = mutex.run_resplit(conn, 7200)
        assert receipt["ok"] and receipt["changed_sessions"] == []
        assert member_ids(conn, "S-V-2001-1") == ["HO1", "HO2", "HO3", "HO4"]


class TestPendingCandidateFailsClosed:
    """收流候选未提交时触发重切：失败闭，整笔拒绝并回滚。"""

    def test_pending_candidate_blocks_resplit(self, conn):
        data = load_fixture(conn)
        spec = data["staged_batch"]
        staged = mutex.stage_candidates(conn, spec["batch_id"], spec["hits"])
        conn.commit()
        assert staged == 1

        receipt = mutex.run_resplit(conn, 1800)

        assert not receipt["ok"]
        assert receipt["status"] == "rejected"
        assert "未提交" in receipt["reason"] and "HS1" in receipt["reason"]

        # 整笔回滚：open 未切、无新会话、closed 未动
        assert member_ids(conn, "S-V-2001-1") == ["HO1", "HO2", "HO3", "HO4"]
        assert session_row(conn, "S-V-2001-1.r1") is None
        assert member_ids(conn, "S-V-2001-0") == ["HC1", "HC2"]

        # rejected 证明落库（审计可见），无 committed 批次
        batches = mutex.list_resplit_batches(conn)
        assert [b["status"] for b in batches] == ["rejected"]
        assert batches[0]["changed_sessions"] == []
        assert batches[0]["created_sessions"] == []

        # 提交候选后互斥解除：候选成正式成员，重切放行
        report = mutex.commit_candidates(conn, spec["batch_id"])
        conn.commit()
        assert report["written"] == 1
        released = mutex.run_resplit(conn, 1800)
        assert released["ok"], released
        assert mutex.verify_isolation(conn)["ok"]

    def test_abort_releases_mutex(self, conn):
        data = load_fixture(conn)
        spec = data["staged_batch"]
        mutex.stage_candidates(conn, spec["batch_id"], spec["hits"])
        conn.commit()

        assert mutex.abort_candidates(conn, spec["batch_id"]) == 1
        conn.commit()

        receipt = mutex.run_resplit(conn, 1800)
        assert receipt["ok"], receipt
        # 作废候选不成为正式成员
        assert "HS1" not in member_ids(conn, "S-V-2001-1")
        assert "HS1" not in member_ids(conn, "S-V-2001-1.r1")


class TestVerifyDetectsClosedTamper:
    """核证只读接口：抓到「回执成功但织机显示 closed 被改挂」。"""

    def test_closed_rehang_detected(self, conn):
        load_fixture(conn)
        receipt = mutex.run_resplit(conn, 1800)
        assert receipt["ok"]

        # 模拟越界写：把 closed 成员挂到别的会话
        conn.execute("UPDATE hits SET session_id='S-V-2001-1' WHERE id='HC2'")
        conn.commit()

        result = mutex.verify_isolation(conn)
        assert not result["ok"]
        checks = {c["key"]: c for c in result["checks"]}
        assert not checks["closed_members_unchanged"]["ok"]
        assert any(
            "S-V-2001-0" in v for v in checks["closed_members_unchanged"]["violations"]
        )
        assert not checks["receipt_matches_loom"]["ok"]
        assert any("被改挂" in v for v in checks["receipt_matches_loom"]["violations"])


class TestIngestServiceTwoPhase:
    """收流服务两阶段与重切互斥（服务层走自己的连接）。"""

    def _write_payload(self, tmp_path, monkeypatch, payload):
        (tmp_path / "ingest_batch.json").write_text(
            json.dumps(payload, ensure_ascii=False), encoding="utf-8"
        )
        monkeypatch.setattr(ingest_svc, "FIXTURES_DIR", tmp_path)

    def test_stage_blocks_resplit_until_commit(self, conn, tmp_path, monkeypatch):
        load_fixture(conn)
        self._write_payload(
            tmp_path,
            monkeypatch,
            {
                "batch_id": "B-svc-001",
                "hits": [
                    {
                        "id": "HX1",
                        "visitor_id": "V-2001",
                        "event_ts": "2026-11-18T12:00:00Z",
                        "page_path": "/pay",
                        "event_name": "click",
                    }
                ],
            },
        )

        staged = ingest_svc.stage_batch()
        assert staged["ok"] and staged["staged"] == 1

        blocked = mutex.run_resplit(conn, 1800)
        assert not blocked["ok"] and blocked["status"] == "rejected"

        committed = ingest_svc.commit_batch("B-svc-001")
        assert committed["ok"] and committed["report"]["written"] == 1

        released = mutex.run_resplit(conn, 1800)
        assert released["ok"], released
        assert mutex.verify_isolation(conn)["ok"]

    def test_run_batch_leaves_no_pending(self, conn, tmp_path, monkeypatch):
        load_fixture(conn)
        self._write_payload(
            tmp_path,
            monkeypatch,
            {
                "batch_id": "B-svc-002",
                "hits": [
                    {
                        "id": "HY1",
                        "visitor_id": "V-2002",
                        "event_ts": "2026-11-18T12:10:00Z",
                        "page_path": "/cart",
                        "event_name": "view",
                    },
                    {
                        "id": "HY2",
                        "visitor_id": "V-2002",
                        "event_ts": "2026-11-18T12:11:00Z",
                        "page_path": "/cart",
                        "event_name": "click",
                    },
                ],
            },
        )

        res = ingest_svc.run_batch()
        assert res["ok"] and res["report"]["written"] == 2
        assert mutex.pending_candidates(conn) == []
        assert mutex.run_resplit(conn, 1800)["ok"]


class TestSourceProofReceipt:
    """重切源证明与回执点数互证：回执 n = 证明派生 n = open 成员数。"""

    def test_receipt_proof_loom_counts_agree(self, conn):
        load_fixture(conn)
        receipt = mutex.run_resplit(conn, 1800)
        assert receipt["ok"], receipt

        # 回执点数与源哈希（夹具：6 个 open 正式成员，尾段改写 2 点）
        assert receipt["scanned_n"] == 6
        assert receipt["rewritten_n"] == 2
        assert receipt["source_hash"]

        # 批次证明快照列：哈希与点数和回执同源落库
        batch = mutex.list_resplit_batches(conn)[0]
        assert batch["status"] == "committed"
        assert batch["source_hash"] == receipt["source_hash"]
        assert batch["scanned_n"] == receipt["scanned_n"]
        assert batch["rewritten_n"] == receipt["rewritten_n"]
        proof_derived_n = len(batch["source_members"])
        assert proof_derived_n == batch["scanned_n"]

        # 织机重算 open 成员（与织机页珠串同源）
        loom_ids = mutex._open_member_ids(conn)
        assert len(loom_ids) == receipt["scanned_n"]
        assert mutex._source_hash(loom_ids) == receipt["source_hash"]

        # 主验收：回执 n = 证明派生 n = open 成员数
        assert receipt["scanned_n"] == proof_derived_n == len(loom_ids)

        # 只读源核证：批次 / source_hash / scanned_n / open 成员数 / 结论
        report = mutex.verify_source(conn)
        assert report["ok"], report
        row = report["batches"][0]
        assert row["batch_id"] == receipt["batch_id"]
        assert row["source_hash"] == receipt["source_hash"]
        assert row["scanned_n"] == row["proof_member_n"] == row["open_member_n"] == 6
        assert row["ok"] and row["conclusion"] == "一致"

        # 隔离核证同样带上真实源字段（不再是硬编码谎报）
        iso = mutex.verify_isolation(conn)
        assert iso["ok"]
        assert iso["source_hash"] == receipt["source_hash"]
        assert iso["scanned_n"] == 6 and iso["open_member_n"] == 6
        checks = {c["key"]: c for c in iso["checks"]}
        assert checks["source_proof_consistent"]["ok"]

    def test_committed_proof_always_has_source_hash(self, conn):
        load_fixture(conn)
        for _ in range(2):  # 二次重切幂等，仍逐笔固化源证明
            receipt = mutex.run_resplit(conn, 1800)
            assert receipt["ok"], receipt
        for b in mutex.list_resplit_batches(conn):
            assert b["source_hash"], f"批次 {b['id']} 缺源哈希"
            assert b["scanned_n"] == len(b["source_members"])

    def test_batch_proof_row_immutable(self, conn):
        load_fixture(conn)
        assert mutex.run_resplit(conn, 1800)["ok"]
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("UPDATE resplit_batches SET source_hash='lie'")
        conn.rollback()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("DELETE FROM resplit_batches")
        conn.rollback()
        # 触发器拦下改写后，证明原样仍在
        batch = mutex.list_resplit_batches(conn)[0]
        assert batch["source_hash"] and batch["scanned_n"] == 6

    def test_loom_drift_detected(self, conn):
        load_fixture(conn)
        receipt = mutex.run_resplit(conn, 1800)
        assert receipt["ok"]

        # 绕过重切直接给 open 会话加成员 → 织机与源证明分叉
        conn.execute(
            "INSERT INTO hits(id, visitor_id, session_id, event_ts, page_path,"
            " event_name, channel) VALUES ('HX9','V-2002','S-V-2002-1',"
            " '2026-11-18T12:00:00Z','/x','view','primary')"
        )
        conn.commit()

        report = mutex.verify_source(conn)
        assert not report["ok"]
        row = report["batches"][0]
        assert row["scanned_n"] == 6 and row["open_member_n"] == 7
        assert "织机" in row["conclusion"]

        iso = mutex.verify_isolation(conn)
        assert not iso["ok"]
        checks = {c["key"]: c for c in iso["checks"]}
        assert not checks["source_proof_consistent"]["ok"]


class TestHttpWiring:
    """HTTP 冒烟：重切入口 / 只读核证 / 收流候选两阶段接线。"""

    def test_resplit_verify_and_mutex_over_http(self):
        from fastapi.testclient import TestClient

        from app.main import app

        with TestClient(app) as client:
            r = client.post("/api/resplit/run", json={})
            assert r.status_code == 200 and r.json()["ok"], r.text

            v = client.get("/api/resplit/verify")
            assert v.status_code == 200 and v.json()["ok"], v.text

            s = client.post("/api/ingest/stage")
            assert s.status_code == 200 and s.json()["ok"]
            batch_id = s.json()["batch_id"]

            blocked = client.post("/api/resplit/run", json={})
            assert blocked.status_code == 200
            assert blocked.json()["status"] == "rejected"

            a = client.post("/api/ingest/abort", json={"batch_id": batch_id})
            assert a.status_code == 200 and a.json()["voided"] >= 1

            released = client.post("/api/resplit/run", json={})
            assert released.status_code == 200 and released.json()["ok"]

            batches = client.get("/api/resplit/batches")
            assert batches.status_code == 200
            statuses = {b["status"] for b in batches.json()}
            assert "committed" in statuses and "rejected" in statuses

    def test_source_verify_over_http(self):
        from fastapi.testclient import TestClient

        from app.main import app

        with TestClient(app) as client:
            r = client.post("/api/resplit/run", json={})
            assert r.status_code == 200 and r.json()["ok"], r.text
            receipt = r.json()
            assert receipt["source_hash"] and receipt["scanned_n"] >= 0

            v = client.get("/api/resplit/source-verify")
            assert v.status_code == 200, v.text
            body = v.json()
            assert body["ok"], body
            assert body["batches"], "应有批次源核证行"
            latest = body["batches"][0]
            assert latest["batch_id"] == receipt["batch_id"]
            assert latest["source_hash"] == receipt["source_hash"]
            assert (
                latest["scanned_n"]
                == latest["proof_member_n"]
                == latest["open_member_n"]
                == receipt["scanned_n"]
            )
