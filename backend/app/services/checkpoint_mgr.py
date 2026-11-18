"""检查点领域服务：管理流式任务的检查点策略。

数据流角色: replay_engine → checkpoint_mgr（消费者+生产者）→ restore_plan（消费者）
增强能力: 检查点创建/快照/恢复/对齐、状态校验、外部化存储路径管理、多检查点一致性验证

序号28修复：检查点模块读取状态压缩清单生成快照，
快照与压缩后的状态键属于同一版本，恢复过程只能使用活动键集合。
"""
from __future__ import annotations

import copy
import hashlib
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class CheckpointMgrService(BaseModuleService):
    MODULE = "checkpoint_mgr"
    ENTITY = "检查点配置"
    LIST_FIELDS = ["配置名称", "间隔时间", "超时时长", "最大并发", "对齐策略", "保留数量", "外部化路径"]
    REQUIRED_FIELDS = ["配置名称", "间隔时间", "超时时长"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用检查点": "草稿", "停用检查点": "已启用"}
    NEGATIVE_ACTIONS = ["停用检查点"]

    # ---- 序号28：读取状态压缩清单生成快照 ----

    def create_checkpoint_from_manifest(self, config_name: str) -> dict[str, Any]:
        """读取状态压缩清单生成检查点快照。

        快照只包含当前版本的活动键集合，且与压缩版本号一致：
        压缩后的状态键和检查点必须属于同一版本。
        """
        version = store.current_state_version(config_name)
        if version is None:
            return {
                "ok": False,
                "error": f"配置「{config_name}」没有当前状态版本，请先执行压缩",
            }

        # 依据压缩清单收集活动键集合（恢复过程只能使用活动键集合）
        operator_states: dict[str, Any] = {}
        referenced_keys: list[str] = []
        for group in version["manifest"].get("groups", []):
            if group.get("deferred"):
                continue
            active_keys = [str(k) for k in group.get("active_keys", [])]
            if not active_keys:
                continue
            referenced_keys.extend(active_keys)
            operator_states[group["key_pattern"]] = {
                "state_keys": active_keys,
                "active_state_id": group.get("active_state_id"),
                "version_id": version["version_id"],
            }

        cp_id = hashlib.md5(
            f"{config_name}_{version['version_id']}_{time.time()}".encode()
        ).hexdigest()[:12]
        int_id = max(
            (int(r.get("id", 0)) for r in store.rows(self.MODULE)
             if str(r.get("id", "")).isdigit()),
            default=0,
        ) + 1

        snapshot = {
            "id": int_id,
            "配置名称": config_name,
            "间隔时间": "30",
            "超时时长": "60",
            "最大并发": "3",
            "对齐策略": "exactly_once",
            "保留数量": 5,
            "外部化路径": "/data/checkpoints",
            "status": "已启用",
            "pending": False,
            "abnormal": False,
            "checkpoint_id": cp_id,
            "operator_states": operator_states,
            "snapshot_ts": time.time(),
            "_created_at": time.time(),
            # 序号28：快照与压缩状态键同属一个版本
            "state_version": version["version_id"],
            "key_count": len(referenced_keys),
            "_referenced_state_keys": referenced_keys,
            "_state_config": config_name,
        }
        store.rows(self.MODULE).append(snapshot)

        # 其他模块同步更新修复台账
        store.append_fix_ledger(
            action="checkpoint",
            module=self.MODULE,
            version_id=version["version_id"],
            summary=(
                f"检查点 {cp_id} 依据压缩清单生成快照，"
                f"活动键 {len(referenced_keys)} 个"
            ),
            detail={"checkpoint_id": cp_id, "key_count": len(referenced_keys)},
        )
        store.sync_ledger_modules(version["version_id"], self.MODULE)

        return {
            "ok": True,
            "checkpoint_id": cp_id,
            "config": config_name,
            "state_version": version["version_id"],
            "key_count": len(referenced_keys),
            "operators": list(operator_states.keys()),
        }

    def list_checkpoints_with_version(
        self, config_name: str | None = None,
    ) -> list[dict[str, Any]]:
        """列出检查点及版本信息，供详情面板核对压缩前后的键数量。"""
        result = []
        for r in store.rows(self.MODULE):
            if not r.get("checkpoint_id"):
                continue
            if config_name and r.get("配置名称") != config_name:
                continue
            version_id = r.get("state_version")
            version = store._state_versions.get(version_id) if version_id else None
            current = store.current_state_version(r.get("配置名称", ""))
            result.append({
                "checkpoint_id": r.get("checkpoint_id"),
                "config": r.get("配置名称", ""),
                "state_version": version_id,
                "key_count": r.get("key_count")
                    if r.get("key_count") is not None
                    else len(r.get("operator_states", {})),
                "is_current_version": bool(
                    current and version_id == current.get("version_id")
                ),
                "version_status": version.get("status") if version else None,
                "snapshot_ts": r.get("snapshot_ts", r.get("timestamp", 0)),
                "status": r.get("status", ""),
            })
        result.sort(key=lambda x: x["snapshot_ts"])
        return result

    # ---- 跨模块数据管理 ----

    def create_checkpoint(
        self, config_name: str, operator_states: dict[str, Any],
    ) -> dict[str, Any]:
        """创建算子状态检查点快照。

        为指定的检查点配置生成带有状态快照的检查点记录，
        存储到 store 中供 replay_engine 消费。
        """
        config = None
        for r in store.rows(self.MODULE):
            if r.get("配置名称") == config_name:
                config = r
                break
        if config is None:
            return {"ok": False, "error": f"检查点配置「{config_name}」不存在"}
        if config.get("status") != "已启用":
            return {"ok": False, "error": f"配置「{config_name}」未启用"}

        try:
            interval = float(config.get("间隔时间", "30"))
        except (ValueError, TypeError):
            interval = 30.0

        cp_id = hashlib.md5(
            f"{config_name}_{time.time()}".encode()
        ).hexdigest()[:12]

        snapshot = {
            "id": cp_id,
            "配置名称": config_name,
            "间隔时间": str(interval),
            "超时时长": config.get("超时时长", "60"),
            "最大并发": config.get("最大并发", "3"),
            "对齐策略": config.get("对齐策略", "exactly_once"),
            "保留数量": int(config.get("保留数量", 5)),
            "外部化路径": config.get("外部化路径", "/data/checkpoints"),
            "status": "已启用",
            "pending": False,
            "abnormal": False,
            "checkpoint_id": cp_id,
            "operator_states": copy.deepcopy(operator_states),
            "snapshot_ts": time.time(),
            "_created_at": time.time(),
        }
        store.rows(self.MODULE).append(snapshot)

        # 清理旧检查点（保留最近 N 个）
        self._prune_old_checkpoints(config_name)

        return {
            "ok": True,
            "checkpoint_id": cp_id,
            "config": config_name,
            "operators": list(operator_states.keys()),
            "interval": interval,
        }

    def _prune_old_checkpoints(self, config_name: str) -> None:
        """清理超过保留数量的旧检查点。"""
        config = None
        for r in store.rows(self.MODULE):
            if r.get("配置名称") == config_name:
                config = r
                break
        max_keep = int((config or {}).get("保留数量", 5))

        snapshots = [
            r for r in store.rows(self.MODULE)
            if r.get("配置名称") == config_name and r.get("checkpoint_id")
        ]
        snapshots.sort(key=lambda s: s.get("snapshot_ts", 0), reverse=True)
        for s in snapshots[max_keep:]:
            s["status"] = "已停用"
            s["_pruned"] = True

    def restore_state(self, checkpoint_id: str) -> dict[str, Any]:
        """从检查点恢复算子状态，验证完整性后返回。"""
        snapshot = None
        for r in store.rows(self.MODULE):
            if r.get("checkpoint_id") == checkpoint_id:
                snapshot = r
                break
        if snapshot is None:
            return {"ok": False, "error": f"检查点「{checkpoint_id}」不存在"}

        states = snapshot.get("operator_states", {})
        # 验证状态完整性
        validation = self._validate_state_structure(states)

        return {
            "ok": validation["valid"],
            "checkpoint_id": checkpoint_id,
            "config": snapshot.get("配置名称", ""),
            "snapshot_ts": snapshot.get("snapshot_ts", 0),
            "operator_count": len(states),
            "operators": list(states.keys()),
            "states": states,
            "validation": validation,
        }

    @staticmethod
    def _validate_state_structure(states: dict[str, Any]) -> dict[str, Any]:
        """验证状态数据结构完整性。"""
        issues = []
        if not isinstance(states, dict):
            return {"valid": False, "issues": ["状态结构不是 dict 类型"]}
        for op_name, op_state in states.items():
            if op_state is None:
                issues.append(f"算子「{op_name}」状态为 None")
            elif isinstance(op_state, dict) and len(op_state) == 0:
                issues.append(f"算子「{op_name}」状态为空字典")
        return {"valid": len(issues) == 0, "issues": issues}

    def verify_multi_checkpoint_consistency(
        self, config_name: str,
    ) -> dict[str, Any]:
        """验证同一配置下多个检查点之间的状态一致性。"""
        snapshots = [
            r for r in store.rows(self.MODULE)
            if r.get("配置名称") == config_name and r.get("checkpoint_id")
        ]
        snapshots.sort(key=lambda s: s.get("snapshot_ts", 0))

        if len(snapshots) < 2:
            return {"ok": True, "consistent": True, "message": "检查点不足2个，无需比较"}

        baseline = snapshots[0].get("operator_states", {})
        inconsistencies = []
        for i, s in enumerate(snapshots[1:], 1):
            current = s.get("operator_states", {})
            for op, state in current.items():
                if op not in baseline:
                    inconsistencies.append(f"检查点{i}: 新增算子「{op}」")
                elif state != baseline.get(op):
                    inconsistencies.append(f"检查点{i}: 算子「{op}」状态变更")

        return {
            "ok": True,
            "config": config_name,
            "snapshot_count": len(snapshots),
            "consistent": len(inconsistencies) == 0,
            "inconsistencies": inconsistencies,
        }

    def list_checkpoints_for_replay(self) -> list[dict[str, Any]]:
        """列出可用于回放引擎消费的检查点。"""
        available = [
            r for r in store.rows(self.MODULE)
            if r.get("checkpoint_id") and r.get("status") == "已启用"
        ]
        result = []
        for cp in available:
            states = cp.get("operator_states", {})
            result.append({
                "checkpoint_id": cp.get("checkpoint_id"),
                "config": cp.get("配置名称", ""),
                "operator_count": len(states),
                "operators": list(states.keys()),
                "snapshot_ts": cp.get("snapshot_ts", 0),
                "external_path": cp.get("外部化路径", ""),
            })
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["total_snapshots"] = sum(1 for r in rows if r.get("checkpoint_id"))
        base["available_snapshots"] = sum(
            1 for r in rows
            if r.get("checkpoint_id") and r.get("status") == "已启用"
        )
        base["pruned_snapshots"] = sum(1 for r in rows if r.get("_pruned"))
        return base