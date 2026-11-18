"""序号28链路测试：状态压缩 → 检查点快照 → 回放恢复的完整链路。

覆盖题位要求：
  - 压缩产生可验证的状态版本与压缩清单
  - 并发修复只应用一次（幂等键 / 指纹 / 配置互斥锁）
  - 压缩后的状态键和检查点必须属于同一版本
  - 恢复过程只能使用活动键集合，回放验证键数量
  - 重复压缩不能删除上一个检查点仍引用的状态
  - 两种修复策略：defer 延迟等待回放完成 / archive 归档历史后继续
  - 两种修复顺序产生不同的恢复结果版本，页面最终只确定一个当前版本
  - 事务中断后回到修复前版本；显式回滚同样回到修复前版本
  - 其他模块同步更新修复台账
  - 修复后详情接口返回完整结果
"""
from __future__ import annotations

import time

import pytest

from app.routers.state_chain import (
    compact_state,
    create_checkpoint,
    replay_from_checkpoint,
    rollback_version,
    state_chain_detail,
    version_manifest,
    CompactPayload,
    CheckpointPayload,
    ReplayPayload,
    RollbackPayload,
)
from app.services import service_for
from app.store import store


# ---------------------------------------------------------------------------
# 测试辅助
# ---------------------------------------------------------------------------

_seq = 0


def _next_id() -> int:
    return max(
        (int(r.get("id", 0)) for r in store.rows("state_store")
         if str(r.get("id", "")).isdigit()),
        default=0,
    ) + 1


def _make_config(config_name: str, pattern: str) -> None:
    store.rows("state_store").append({
        "id": _next_id(),
        "配置名称": config_name,
        "后端类型": "memory",
        "状态键模式": pattern,
        "TTL时长": "3600",
        "检查点间隔": "30",
        "增量模式": "full",
        "清理策略": "none",
        "status": "已生效",
        "pending": False,
        "abnormal": False,
        "_timestamp": 1,
    })


def _make_duplicate(pattern: str, ts: float) -> int:
    nid = _next_id()
    store.rows("state_store").append({
        "id": nid,
        "配置名称": f"dup_{nid}",
        "后端类型": "memory",
        "状态键模式": pattern,
        "TTL时长": "3600",
        "检查点间隔": "30",
        "增量模式": "full",
        "清理策略": "none",
        "status": "已生效",
        "pending": False,
        "abnormal": False,
        "_timestamp": ts,
    })
    return nid


def _fresh_config(tag: str, dup_timestamps: list[float]) -> tuple[str, str]:
    """生成独立配置与重复键，返回 (配置名, 状态键模式)。

    模式带全局序号，避免子串匹配在不同用例间互相命中。
    """
    global _seq
    _seq += 1
    name = f"t28_{tag}_{_seq}"
    pattern = f"t28p{_seq}:*"
    _make_config(name, pattern)
    for ts in dup_timestamps:
        _make_duplicate(pattern, ts)
    return name, pattern


def _rows_with_pattern(pattern: str) -> list[dict]:
    return [
        r for r in store.rows("state_store")
        if str(r.get("状态键模式", "")) == pattern
    ]


@pytest.fixture()
def state_svc():
    return service_for("state_store")


@pytest.fixture()
def cp_svc():
    return service_for("checkpoint_mgr")


@pytest.fixture()
def replay_svc():
    return service_for("replay_engine")


# ---------------------------------------------------------------------------
# 1. 压缩产生可验证的状态版本与压缩清单
# ---------------------------------------------------------------------------

class TestCompactionVersion:
    def test_compact_creates_verifiable_version(self, state_svc):
        """压缩重复键后生成状态版本，清单记录压缩前后键数量。"""
        cfg, _ = _fresh_config("ver", [100, 200, 300])
        result = state_svc.compact_duplicate_states(cfg)
        assert result["ok"] is True
        assert result["applied_once"] is True
        assert result["version_id"]
        manifest = result["manifest"]
        assert manifest["key_count_before"] == 4  # 配置行 + 3 重复键
        assert manifest["key_count_after"] == 1
        assert manifest["compacted_count"] == 3

    def test_compact_marks_old_states_inactive(self, state_svc):
        """压缩后旧状态标记停用并归属新版本。"""
        cfg, pattern = _fresh_config("mark", [100, 200])
        result = state_svc.compact_duplicate_states(cfg)
        version_id = result["version_id"]
        compacted = [r for r in _rows_with_pattern(pattern) if r.get("_compacted")]
        assert len(compacted) == 2
        for r in compacted:
            assert r["status"] == "已停用"
            assert r["_compacted_in_version"] == version_id

    def test_single_current_version_per_config(self, state_svc):
        """页面上最终确定一个当前状态版本。"""
        cfg, pattern = _fresh_config("current", [100, 200])
        v1 = state_svc.compact_duplicate_states(cfg)["version_id"]
        _make_duplicate(pattern, 300)
        v2 = state_svc.compact_duplicate_states(cfg)["version_id"]
        currents = [
            v for v in store.state_versions_for(cfg)
            if v.get("status") == "current"
        ]
        assert len(currents) == 1
        assert currents[0]["version_id"] == v2
        assert store._state_versions[v1]["status"] == "superseded"


# ---------------------------------------------------------------------------
# 2. 并发修复只应用一次（幂等）
# ---------------------------------------------------------------------------

class TestIdempotentCompaction:
    def test_same_idempotency_key_applies_once(self, state_svc):
        """同一幂等键的并发重试只生效一次。"""
        cfg, _ = _fresh_config("idem", [100, 200])
        r1 = state_svc.compact_duplicate_states(cfg, idempotency_key="fix28")
        r2 = state_svc.compact_duplicate_states(cfg, idempotency_key="fix28")
        assert r1["applied_once"] is True
        assert r2["applied_once"] is False
        assert r2["idempotent_hit"] is True
        assert r2["version_id"] == r1["version_id"]
        # 版本数不增加
        assert len(store.state_versions_for(cfg)) == 1

    def test_same_fingerprint_noop(self, state_svc):
        """状态未变化时重复压缩是幂等空操作。"""
        cfg, _ = _fresh_config("finger", [100, 200])
        r1 = state_svc.compact_duplicate_states(cfg)
        r2 = state_svc.compact_duplicate_states(cfg)
        assert r2["idempotent_hit"] is True
        assert r2["version_id"] == r1["version_id"]

    def test_concurrent_lock_conflict(self, state_svc):
        """压缩进行中，不同幂等键的请求被拒绝。"""
        cfg, _ = _fresh_config("lock", [100, 200])
        store._compaction_locks[cfg] = "holder-a"
        try:
            result = state_svc.compact_duplicate_states(cfg, idempotency_key="holder-b")
            assert result["ok"] is False
            assert result["conflict"] is True
            # 同一持有者的重试不被拒绝为冲突
            retry = state_svc.compact_duplicate_states(cfg, idempotency_key="holder-a")
            assert retry["ok"] is True
            assert retry["applied_once"] is False
        finally:
            store._compaction_locks.pop(cfg, None)


# ---------------------------------------------------------------------------
# 3. 检查点读取压缩清单生成快照，回放验证键数量
# ---------------------------------------------------------------------------

class TestCheckpointReplayChain:
    def test_checkpoint_reads_manifest(self, state_svc, cp_svc):
        """检查点模块读取状态压缩清单生成快照，版本一致。"""
        cfg, _ = _fresh_config("chain", [100, 200, 300])
        compact = state_svc.compact_duplicate_states(cfg)
        cp = cp_svc.create_checkpoint_from_manifest(cfg)
        assert cp["ok"] is True
        assert cp["state_version"] == compact["version_id"]
        assert cp["key_count"] == compact["manifest"]["key_count_after"]

    def test_checkpoint_requires_version(self, cp_svc):
        """没有当前版本时不能生成快照。"""
        cfg, _ = _fresh_config("nover", [])
        result = cp_svc.create_checkpoint_from_manifest(cfg)
        assert result["ok"] is False

    def test_replay_restores_and_verifies_key_count(
        self, state_svc, cp_svc, replay_svc,
    ):
        """回放引擎依据快照恢复状态数据并验证键数量。"""
        cfg, _ = _fresh_config("replay", [100, 200])
        compact = state_svc.compact_duplicate_states(cfg)
        cp = cp_svc.create_checkpoint_from_manifest(cfg)
        replay = replay_svc.restore_state_data(cp["checkpoint_id"])
        assert replay["ok"] is True
        verification = replay["verification"]
        assert verification["status"] == "passed"
        assert verification["expected_key_count"] == compact["manifest"]["key_count_after"]
        assert verification["restored_key_count"] == verification["expected_key_count"]
        assert verification["missing_keys"] == []

    def test_restore_uses_only_active_keys(self, state_svc, cp_svc, replay_svc):
        """恢复过程只能使用活动键集合：压缩停用的键不进入快照。"""
        cfg, _ = _fresh_config("active", [100, 200, 300])
        compact = state_svc.compact_duplicate_states(cfg)
        compacted_ids = set()
        for group in compact["manifest"]["groups"]:
            compacted_ids.update(group["compacted_state_ids"])
        cp = cp_svc.create_checkpoint_from_manifest(cfg)
        snapshot = next(
            r for r in store.rows("checkpoint_mgr")
            if r.get("checkpoint_id") == cp["checkpoint_id"]
        )
        referenced = set(snapshot["_referenced_state_keys"])
        assert referenced.isdisjoint(compacted_ids)
        replay = replay_svc.restore_state_data(cp["checkpoint_id"])
        assert replay["ok"] is True

    def test_replay_rejects_rolled_back_version(self, state_svc, cp_svc, replay_svc):
        """已回滚的版本不能用于恢复。"""
        cfg, _ = _fresh_config("rbver", [100, 200])
        compact = state_svc.compact_duplicate_states(cfg)
        cp = cp_svc.create_checkpoint_from_manifest(cfg)
        state_svc.rollback_state_version(compact["version_id"])
        result = replay_svc.restore_state_data(cp["checkpoint_id"])
        assert result["ok"] is False
        assert "回滚" in result["error"]


# ---------------------------------------------------------------------------
# 4. 检查点仍引用旧状态时的两种修复策略
# ---------------------------------------------------------------------------

class TestCheckpointReferencedProtection:
    def _setup_referenced(self, state_svc, cp_svc, tag: str):
        """第一次压缩 + 检查点 → 新重复键到达，使检查点引用旧状态。"""
        cfg, pattern = _fresh_config(tag, [100, 200])
        v1 = state_svc.compact_duplicate_states(cfg)["version_id"]
        cp1 = cp_svc.create_checkpoint_from_manifest(cfg)
        _make_duplicate(pattern, 300)
        _make_duplicate(pattern, 400)
        return cfg, pattern, v1, cp1

    def test_defer_delays_compaction(self, state_svc, cp_svc):
        """defer：检查点仍引用旧状态时延迟压缩等待回放完成。"""
        cfg, _, v1, _ = self._setup_referenced(state_svc, cp_svc, "defer")
        result = state_svc.compact_duplicate_states(cfg, strategy="defer")
        assert result["ok"] is False
        assert result["deferred"] is True
        # 当前版本不变
        assert store.current_state_version(cfg)["version_id"] == v1

    def test_archive_copies_to_history_then_compacts(self, state_svc, cp_svc):
        """archive：复制旧状态到历史区域后继续压缩。"""
        cfg, _, _, _ = self._setup_referenced(state_svc, cp_svc, "arch")
        result = state_svc.compact_duplicate_states(cfg, strategy="archive")
        assert result["ok"] is True
        assert result["archived"] >= 1
        version = store._state_versions[result["version_id"]]
        assert len(version["history_archive"]) >= 1
        # 旧状态数据在历史区域完整保留
        archived = version["history_archive"][0]["snapshot"]
        assert archived["id"] is not None
        assert store.current_state_version(cfg)["version_id"] == result["version_id"]

    def test_repeated_compaction_keeps_referenced_state(self, state_svc, cp_svc):
        """重复压缩不能删除上一个检查点仍引用的状态。"""
        cfg, _, _, cp1 = self._setup_referenced(state_svc, cp_svc, "keep")
        referenced = set()
        for r in store.rows("checkpoint_mgr"):
            if r.get("checkpoint_id") == cp1["checkpoint_id"]:
                referenced = set(r.get("_referenced_state_keys", []))
        assert referenced
        result = state_svc.compact_duplicate_states(cfg, strategy="archive")
        assert result["ok"] is True
        # 被引用的旧状态虽然停用，但数据在历史区域可恢复
        version = store._state_versions[result["version_id"]]
        archived_ids = {
            str(item["snapshot"]["id"]) for item in version["history_archive"]
        }
        assert referenced & archived_ids or referenced <= {
            str(k) for g in result["manifest"]["groups"] for k in g["active_keys"]
        } | archived_ids

    def test_two_repair_orders_produce_different_result_versions(
        self, state_svc, cp_svc, replay_svc,
    ):
        """两种修复顺序产生恢复状态数据的不同结果版本。"""
        # 顺序一：先回放旧检查点，再压缩 → 恢复数据属于 v1
        cfg_a, _, v1_a, cp1_a = self._setup_referenced(state_svc, cp_svc, "orderA")
        replay_a = replay_svc.restore_state_data(cp1_a["checkpoint_id"])
        assert replay_a["state_version"] == v1_a
        state_svc.compact_duplicate_states(cfg_a, strategy="defer")  # 仍延迟

        # 顺序二：先归档压缩，再从新检查点回放 → 恢复数据属于 v2
        cfg_b, _, v1_b, _ = self._setup_referenced(state_svc, cp_svc, "orderB")
        compact_b = state_svc.compact_duplicate_states(cfg_b, strategy="archive")
        cp2_b = cp_svc.create_checkpoint_from_manifest(cfg_b)
        replay_b = replay_svc.restore_state_data(cp2_b["checkpoint_id"])
        assert replay_b["state_version"] == compact_b["version_id"]
        assert replay_b["state_version"] != v1_b

        # 两种顺序的结果版本不同，但各自链路自洽
        assert replay_a["state_version"] != replay_b["state_version"]
        assert replay_a["verification"]["all_passed"] is True
        assert replay_b["verification"]["all_passed"] is True


# ---------------------------------------------------------------------------
# 5. 事务中断与回滚
# ---------------------------------------------------------------------------

class TestTransactionRollback:
    def test_interrupted_transaction_restores_pre_fix_version(
        self, state_svc, monkeypatch,
    ):
        """事务中断后回到修复前版本。"""
        cfg, pattern = _fresh_config("tx", [100, 200, 300])
        before = {r["id"]: r["status"] for r in _rows_with_pattern(pattern)}

        def boom(*args, **kwargs):
            raise RuntimeError("模拟事务中断")

        monkeypatch.setattr(store, "append_fix_ledger", boom)
        result = state_svc.compact_duplicate_states(cfg)
        monkeypatch.undo()

        assert result["ok"] is False
        assert result["rolled_back"] is True
        assert store._state_versions[result["version_id"]]["status"] == "rolled_back"
        after = {r["id"]: r["status"] for r in _rows_with_pattern(pattern)}
        assert after == before  # 全部回到修复前状态
        assert store.current_state_version(cfg) is None

    def test_explicit_rollback_restores_rows(self, state_svc):
        """显式回滚：撤销压缩，活动键恢复，版本状态正确流转。"""
        cfg, pattern = _fresh_config("manual", [100, 200])
        v1 = state_svc.compact_duplicate_states(cfg)["version_id"]
        _make_duplicate(pattern, 300)
        v2 = state_svc.compact_duplicate_states(cfg)["version_id"]

        result = state_svc.rollback_state_version(v2)
        assert result["ok"] is True
        assert result["restored_version"] == v1
        assert store._state_versions[v2]["status"] == "rolled_back"
        assert store.current_state_version(cfg)["version_id"] == v1
        # v2 压缩停用的键已恢复
        active = [r for r in _rows_with_pattern(pattern) if r.get("status") == "已生效"]
        assert len(active) == 2  # v1 压缩后剩 1 + 新到的 1

    def test_rollback_rejects_non_current_version(self, state_svc):
        """非当前版本不能回滚。"""
        cfg, pattern = _fresh_config("noncur", [100, 200])
        v1 = state_svc.compact_duplicate_states(cfg)["version_id"]
        _make_duplicate(pattern, 300)
        state_svc.compact_duplicate_states(cfg)
        result = state_svc.rollback_state_version(v1)
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# 6. 修复台账跨模块同步
# ---------------------------------------------------------------------------

class TestFixLedgerSync:
    def test_ledger_synced_across_modules(self, state_svc, cp_svc, replay_svc):
        """压缩→检查点→回放全链路动作写入台账，其他模块同步更新。"""
        cfg, _ = _fresh_config("ledger", [100, 200])
        compact = state_svc.compact_duplicate_states(cfg)
        version_id = compact["version_id"]
        cp = cp_svc.create_checkpoint_from_manifest(cfg)
        replay_svc.restore_state_data(cp["checkpoint_id"])

        entries = [
            e for e in store._fix_ledger if e.get("version_id") == version_id
        ]
        actions = {e["action"] for e in entries}
        assert {"compact", "checkpoint", "replay"} <= actions
        compact_entry = next(e for e in entries if e["action"] == "compact")
        # 其他模块同步更新修复台账
        assert "checkpoint_mgr" in compact_entry["synced_modules"]
        assert "replay_engine" in compact_entry["synced_modules"]
        assert "restore_plan" in compact_entry["synced_modules"]


# ---------------------------------------------------------------------------
# 7. 详情接口返回完整结果
# ---------------------------------------------------------------------------

class TestDetailApi:
    def test_detail_returns_complete_result(self, state_svc, cp_svc, replay_svc):
        """序号28详情载入：活动键、版本、检查点、回放历史、台账齐全。"""
        cfg, _ = _fresh_config("detail", [100, 200])
        compact = state_svc.compact_duplicate_states(cfg)
        cp = cp_svc.create_checkpoint_from_manifest(cfg)
        replay_svc.restore_state_data(cp["checkpoint_id"])

        detail = state_chain_detail(config=cfg)
        assert detail["ok"] is True
        assert detail["ticket"] == "序号28"
        assert detail["current_version"]["version_id"] == compact["version_id"]
        assert len(detail["active_keys"]) == 1
        assert detail["active_keys"][0]["count"] == 1
        assert len(detail["checkpoints"]) == 1
        assert detail["checkpoints"][0]["is_current_version"] is True
        assert len(detail["replays"]) == 1
        assert detail["replays"][0]["restored_key_count"] == 1
        assert detail["stats"]["versions"] >= 1
        assert any(e["action"] == "compact" for e in detail["ledger"])

    def test_detail_empty_config_still_complete(self):
        """无数据的配置也返回完整结构（修复后接口返回完整结果）。"""
        detail = state_chain_detail(config="不存在的配置")
        assert detail["ok"] is True
        assert detail["current_version"] is None
        assert detail["versions"] == []
        assert detail["active_keys"] == []
        assert detail["checkpoints"] == []
        assert detail["replays"] == []

    def test_api_chain_end_to_end(self):
        """通过路由函数走完整链路：压缩→检查点→回放→核对清单。"""
        cfg, _ = _fresh_config("api", [100, 200, 300])
        compact = compact_state(CompactPayload(
            config_name=cfg, strategy="archive", idempotency_key="api-fix28",
        ))
        assert compact["ok"] is True
        cp = create_checkpoint(CheckpointPayload(config_name=cfg))
        assert cp["ok"] is True
        replay = replay_from_checkpoint(ReplayPayload(
            checkpoint_id=cp["checkpoint_id"], job_name="api回放",
        ))
        assert replay["ok"] is True
        # 按检查点版本核对状态压缩清单和状态恢复数据
        manifest = version_manifest(compact["version_id"])
        assert manifest["ok"] is True
        assert manifest["version"]["manifest"]["key_count_after"] == 1
        assert len(manifest["checkpoints"]) == 1
        assert len(manifest["replays"]) == 1
        assert manifest["replays"][0]["restored_key_count"] == 1

    def test_api_rollback_endpoint(self):
        """回滚接口：事务回到修复前版本。"""
        cfg, _ = _fresh_config("apirb", [100, 200])
        compact = compact_state(CompactPayload(config_name=cfg))
        result = rollback_version(RollbackPayload(version_id=compact["version_id"]))
        assert result["ok"] is True
        assert result["rolled_back"] is True
        detail = state_chain_detail(config=cfg)
        assert detail["current_version"] is None
        assert detail["versions"][0]["status"] == "rolled_back"
