"""状态存储领域服务：管理流式任务的有状态存储配置。

数据流角色: calc_operator → state_store（消费者+生产者）→ 下游多模块
增强能力: 状态TTL清理、增量/全量模式切换、状态键去重、状态查询、跨模块状态索引

序号28修复：压缩重复键后生成可验证的状态版本与压缩清单，
检查点模块读取清单生成快照，回放引擎依据快照恢复状态数据并验证键数量。
- 并发修复只应用一次（幂等键 + 指纹去重 + 配置级互斥锁）
- 压缩后的状态键和检查点必须属于同一版本，恢复只能使用活动键集合
- 重复压缩不能删除上一个检查点仍引用的状态：
  策略 defer=延迟压缩等待回放完成；策略 archive=复制旧状态到历史区域后继续压缩
- 事务中断后回到修复前版本（行级快照回滚）
"""
from __future__ import annotations

import copy
import hashlib
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 序号28：两种修复策略
STRATEGY_DEFER = "defer"      # 检查点仍引用旧状态时：延迟压缩等待回放完成
STRATEGY_ARCHIVE = "archive"  # 检查点仍引用旧状态时：复制旧状态到历史区域后继续压缩


class StateStoreService(BaseModuleService):
    MODULE = "state_store"
    ENTITY = "状态配置"
    LIST_FIELDS = ["配置名称", "后端类型", "状态键模式", "TTL时长", "检查点间隔", "增量模式", "清理策略"]
    REQUIRED_FIELDS = ["配置名称", "后端类型", "状态键模式"]
    STATUS_ORDER = ["草稿", "已生效", "已停用"]
    ACTION_RULES = {"启用配置": "草稿", "停用配置": "已生效"}
    NEGATIVE_ACTIONS = ["停用配置"]

    # ---- 跨模块数据管理 ----

    def _find_config(self, config_name: str) -> dict[str, Any] | None:
        for r in store.rows(self.MODULE):
            if r.get("配置名称") == config_name:
                return r
        return None

    def _group_entries(self, pattern: str) -> dict[str, list[dict[str, Any]]]:
        """按状态键模式分组，返回该配置命中的全部状态条目。"""
        states_grouped: dict[str, list[dict[str, Any]]] = {}
        for r in store.rows(self.MODULE):
            if r.get("状态键模式") and pattern in str(r.get("状态键模式", "")):
                key = r.get("状态键模式", "__default__")
                states_grouped.setdefault(key, []).append(r)
        return states_grouped

    @staticmethod
    def _active_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [e for e in entries if e.get("status") == "已生效" and not e.get("_compacted")]

    def _compression_fingerprint(
        self, config_name: str, states_grouped: dict[str, list[dict[str, Any]]],
    ) -> str:
        """压缩指纹：配置 + 各组活动条目集合。重试同一状态时指纹不变 → 幂等。"""
        parts = [config_name]
        for key in sorted(states_grouped):
            active_ids = sorted(
                str(e.get("id")) for e in self._active_entries(states_grouped[key])
            )
            parts.append(f"{key}:[{','.join(active_ids)}]")
        return hashlib.md5("|".join(parts).encode()).hexdigest()[:16]

    def _checkpoint_referenced_keys(self, config_name: str) -> set[str]:
        """收集检查点仍引用的状态键（重复压缩不能删除这些状态）。"""
        referenced: set[str] = set()
        for r in store.rows("checkpoint_mgr"):
            if not r.get("checkpoint_id") or r.get("status") != "已启用":
                continue
            if r.get("配置名称") != config_name and r.get("_state_config") != config_name:
                continue
            for key in r.get("_referenced_state_keys", []) or []:
                referenced.add(str(key))
            states = r.get("operator_states", {})
            if isinstance(states, dict):
                for op_state in states.values():
                    if isinstance(op_state, dict):
                        for key in op_state.get("state_keys", []) or []:
                            referenced.add(str(key))
        return referenced

    @staticmethod
    def _safe_int_id(entry: dict[str, Any]) -> int:
        """id 可能是非数字字符串（历史数据），排序时安全转换。"""
        raw = entry.get("id", 0)
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    def compact_duplicate_states(
        self,
        config_name: str,
        strategy: str = STRATEGY_ARCHIVE,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """压缩重复状态：按状态键模式去重，保留最新版本，并生成可验证的状态版本。

        序号28修复要点：
        - 幂等：同一配置同一状态指纹只应用一次，并发重试返回首次结果
        - 版本：每次压缩产生新的状态版本与压缩清单，旧版本标记 superseded
        - 保护：检查点仍引用的状态键，defer 策略延迟压缩，archive 策略归档后继续
        - 回滚：事务中断自动回到修复前版本
        """
        config = self._find_config(config_name)
        if config is None:
            return {"ok": False, "error": f"状态配置「{config_name}」不存在"}
        if strategy not in (STRATEGY_DEFER, STRATEGY_ARCHIVE):
            return {"ok": False, "error": f"未知压缩策略「{strategy}」"}
        pattern = config.get("状态键模式", "")
        if not pattern:
            return {"ok": False, "error": f"状态配置「{config_name}」缺少状态键模式"}

        idem_key = idempotency_key or f"auto-{config_name}"
        states_grouped = self._group_entries(pattern)
        fingerprint = self._compression_fingerprint(config_name, states_grouped)

        # ---- 幂等：已应用过相同指纹的压缩，直接返回当前版本 ----
        current = store.current_state_version(config_name)
        if current and current.get("fingerprint") == fingerprint:
            return {
                "ok": True,
                "applied_once": False,
                "idempotent_hit": True,
                "config": config_name,
                "version_id": current["version_id"],
                "manifest": current["manifest"],
                "message": "相同压缩已应用过，并发重试只生效一次",
            }

        # ---- 并发互斥：同一配置同时只允许一个压缩事务 ----
        holder = store._compaction_locks.get(config_name)
        if holder is not None:
            if holder == idem_key:
                # 同一请求的并发重试：首次已生效，直接返回
                current = store.current_state_version(config_name)
                return {
                    "ok": True,
                    "applied_once": False,
                    "idempotent_hit": True,
                    "config": config_name,
                    "version_id": current["version_id"] if current else None,
                    "manifest": current["manifest"] if current else None,
                    "message": "并发重试已被首次压缩覆盖，只生效一次",
                }
            return {
                "ok": False,
                "conflict": True,
                "error": f"配置「{config_name}」正在压缩中，请稍后重试",
            }

        store._compaction_locks[config_name] = idem_key
        try:
            return self._apply_compaction(
                config_name, pattern, strategy, fingerprint, states_grouped,
            )
        finally:
            store._compaction_locks.pop(config_name, None)

    def _apply_compaction(
        self,
        config_name: str,
        pattern: str,
        strategy: str,
        fingerprint: str,
        states_grouped: dict[str, list[dict[str, Any]]],
    ) -> dict[str, Any]:
        """压缩事务主体：失败时回滚到修复前版本。"""
        referenced = self._checkpoint_referenced_keys(config_name)
        pre_version = store.current_state_version(config_name)

        # ---- 事务前快照：受影响行的深拷贝，用于中断回滚 ----
        affected: list[dict[str, Any]] = [
            e for entries in states_grouped.values() for e in entries
        ]
        row_backup = {id(e): copy.deepcopy(e) for e in affected}

        version_id = store.next_state_version_id()
        version_record: dict[str, Any] = {
            "version_id": version_id,
            "seq": store._version_seq,
            "ticket": "序号28",
            "config_name": config_name,
            "status": "current",
            "strategy": strategy,
            "created_at": time.time(),
            "fingerprint": fingerprint,
            "pre_version_id": pre_version["version_id"] if pre_version else None,
            "manifest": {},
            "history_archive": [],
            "_row_backup": row_backup,
        }

        try:
            compacted = 0
            archived = 0
            deferred_groups: list[str] = []
            manifest_groups: list[dict[str, Any]] = []

            for key, entries in sorted(states_grouped.items()):
                active = self._active_entries(entries)
                if len(active) <= 1:
                    if active:
                        manifest_groups.append({
                            "key_pattern": key,
                            "active_state_id": active[0].get("id"),
                            "active_keys": [str(active[0].get("id"))],
                            "compacted_state_ids": [],
                            "protected": False,
                        })
                    continue

                # 保留最新的，其余为待压缩旧状态
                active.sort(
                    key=lambda e: (e.get("_timestamp", 0) or 0, self._safe_int_id(e)),
                    reverse=True,
                )
                keeper, duplicates = active[0], active[1:]
                dup_ids = [str(e.get("id")) for e in duplicates]

                # 检查点仍引用旧状态 → 按策略处理
                blocked = [d for d in duplicates if str(d.get("id")) in referenced]
                if blocked and strategy == STRATEGY_DEFER:
                    # 延迟压缩等待回放完成：本组不压缩
                    deferred_groups.append(key)
                    manifest_groups.append({
                        "key_pattern": key,
                        "active_state_id": None,
                        "active_keys": [str(e.get("id")) for e in active],
                        "compacted_state_ids": [],
                        "protected": True,
                        "deferred": True,
                    })
                    continue
                if blocked and strategy == STRATEGY_ARCHIVE:
                    # 复制旧状态到历史区域后继续压缩
                    for d in blocked:
                        version_record["history_archive"].append({
                            "archived_at": time.time(),
                            "reason": "检查点仍引用，复制到历史区域后继续压缩",
                            "snapshot": copy.deepcopy(d),
                        })
                        archived += 1

                for e in duplicates:
                    e["status"] = "已停用"
                    e["_compacted"] = True
                    e["_compacted_in_version"] = version_id
                    compacted += 1
                manifest_groups.append({
                    "key_pattern": key,
                    "active_state_id": keeper.get("id"),
                    "active_keys": [str(keeper.get("id"))],
                    "compacted_state_ids": dup_ids,
                    "protected": bool(blocked),
                })

            if deferred_groups and len(deferred_groups) == len(manifest_groups):
                # 全部组都被延迟：压缩整体推迟，不产生新版本
                store._version_seq -= 1  # 回收版本号，保持版本序列连续
                return {
                    "ok": False,
                    "deferred": True,
                    "config": config_name,
                    "deferred_groups": deferred_groups,
                    "message": "检查点仍引用旧状态，延迟压缩等待回放完成",
                }

            key_count_before = sum(
                len(self._active_entries(entries)) for entries in states_grouped.values()
            ) + compacted  # 压缩前活动键 = 当前活动 + 刚被压缩的
            key_count_after = key_count_before - compacted

            version_record["manifest"] = {
                "groups": manifest_groups,
                "key_count_before": key_count_before,
                "key_count_after": key_count_after,
                "compacted_count": compacted,
                "archived_count": archived,
                "deferred_groups": deferred_groups,
            }

            # 页面上最终确定一个当前状态版本：旧版本降为 superseded
            if pre_version is not None:
                pre_version["status"] = "superseded"
            # 指纹记录压缩后的状态：并发重试计算同一指纹 → 幂等命中
            version_record["fingerprint"] = self._compression_fingerprint(
                config_name, states_grouped,
            )
            store._state_versions[version_id] = version_record

            # 修复台账：压缩动作登记，供其他模块同步
            store.append_fix_ledger(
                action="compact",
                module=self.MODULE,
                version_id=version_id,
                summary=(
                    f"配置「{config_name}」压缩 {compacted} 个重复键，"
                    f"活动键 {key_count_before}→{key_count_after}"
                ),
                detail={
                    "strategy": strategy,
                    "compacted": compacted,
                    "archived": archived,
                    "deferred_groups": deferred_groups,
                },
            )

            return {
                "ok": True,
                "applied_once": True,
                "config": config_name,
                "pattern": pattern,
                "strategy": strategy,
                "version_id": version_id,
                "groups_found": len(states_grouped),
                "compacted": compacted,
                "archived": archived,
                "deferred_groups": deferred_groups,
                "remaining_active": key_count_after,
                "manifest": version_record["manifest"],
            }
        except Exception as exc:  # 事务中断 → 回到修复前版本
            for e in affected:
                backup = row_backup.get(id(e))
                if backup is not None:
                    e.clear()
                    e.update(backup)
            version_record["status"] = "rolled_back"
            store._state_versions[version_id] = version_record
            if pre_version is not None:
                pre_version["status"] = "current"
            try:
                store.append_fix_ledger(
                    action="rollback",
                    module=self.MODULE,
                    version_id=version_id,
                    summary=f"配置「{config_name}」压缩事务中断，已回到修复前版本",
                    detail={"error": str(exc)},
                )
            except Exception:
                pass  # 台账写入失败不影响回滚本身
            return {
                "ok": False,
                "rolled_back": True,
                "config": config_name,
                "version_id": version_id,
                "restored_version": pre_version["version_id"] if pre_version else None,
                "error": f"压缩事务中断已回滚：{exc}",
            }

    def rollback_state_version(self, version_id: str) -> dict[str, Any]:
        """显式回滚：撤销指定版本的压缩，回到修复前版本。"""
        version = store._state_versions.get(version_id)
        if version is None:
            return {"ok": False, "error": f"状态版本「{version_id}」不存在"}
        if version.get("status") != "current":
            return {"ok": False, "error": f"版本「{version_id}」不是当前版本，不能回滚"}

        # 恢复事务前快照
        for backup in (version.get("_row_backup") or {}).values():
            target = None
            for r in store.rows(self.MODULE):
                if r.get("id") == backup.get("id") and r.get("配置名称") == backup.get("配置名称"):
                    target = r
                    break
            if target is not None:
                target.clear()
                target.update(copy.deepcopy(backup))

        version["status"] = "rolled_back"
        version["rolled_back_at"] = time.time()
        pre_id = version.get("pre_version_id")
        if pre_id and pre_id in store._state_versions:
            store._state_versions[pre_id]["status"] = "current"

        store.append_fix_ledger(
            action="rollback",
            module=self.MODULE,
            version_id=version_id,
            summary=f"版本「{version_id}」已回滚，回到修复前版本 {pre_id or '（无）'}",
        )
        return {
            "ok": True,
            "rolled_back": True,
            "version_id": version_id,
            "restored_version": pre_id,
            "message": "已回到修复前版本",
        }

    def list_active_state_keys(self, config_name: str | None = None) -> list[dict[str, Any]]:
        """列出活动状态键（责任审核人在状态页面查看活动键）。

        只统计当前版本下的活动键集合：已生效且未被压缩的条目。
        """
        allowed_ids: set[int] | None = None
        if config_name:
            config = self._find_config(config_name)
            if config is None:
                return []
            grouped = self._group_entries(config.get("状态键模式", ""))
            allowed_ids = {id(e) for entries in grouped.values() for e in entries}
        key_stats: dict[str, dict[str, Any]] = {}
        for r in store.rows(self.MODULE):
            if r.get("status") != "已生效" or r.get("_compacted"):
                continue
            if r.get("_restored_state_key"):
                continue  # 回放恢复的数据在回放历史中核对，不计入版本活动键
            if allowed_ids is not None and id(r) not in allowed_ids:
                continue
            pattern = r.get("状态键模式", "__unknown__")
            if pattern not in key_stats:
                key_stats[pattern] = {
                    "pattern": pattern,
                    "count": 0,
                    "latest_ts": 0,
                    "state_ids": [],
                }
            key_stats[pattern]["count"] += 1
            key_stats[pattern]["state_ids"].append(r.get("id"))
            ts = r.get("_timestamp", 0)
            if isinstance(ts, (int, float)) and ts > key_stats[pattern]["latest_ts"]:
                key_stats[pattern]["latest_ts"] = ts
        return sorted(key_stats.values(), key=lambda x: x["count"], reverse=True)

    def apply_ttl_cleanup(self, config_name: str) -> dict[str, Any]:
        """根据TTL策略清理过期状态条目。"""
        config = None
        for r in store.rows(self.MODULE):
            if r.get("配置名称") == config_name:
                config = r
                break
        if config is None:
            return {"ok": False, "error": f"状态配置「{config_name}」不存在"}

        try:
            ttl = int(config.get("TTL时长", "3600"))
        except (ValueError, TypeError):
            ttl = 3600

        now = time.time()
        cleaned = 0
        for r in store.rows(self.MODULE):
            ts = r.get("_timestamp", 0)
            if isinstance(ts, (int, float)) and now - ts > ttl:
                r["status"] = "已停用"
                r["_cleaned"] = True
                r["_cleaned_at"] = now
                cleaned += 1

        return {
            "ok": True,
            "config": config_name,
            "ttl": ttl,
            "cleaned": cleaned,
            "timestamp": now,
        }

    def query_state_by_operator(
        self, operator_name: str, group_key: str | None = None,
    ) -> dict[str, Any]:
        """按算子名称查询关联状态，支持分组过滤。"""
        matches = []
        for r in store.rows(self.MODULE):
            if r.get("_operator") == operator_name:
                if group_key and r.get("_group") != group_key:
                    continue
                matches.append({
                    "state_id": r.get("id"),
                    "config": r.get("配置名称", ""),
                    "operator": r.get("_operator", ""),
                    "group": r.get("_group", ""),
                    "aggregates": r.get("_aggregates", {}),
                    "timestamp": r.get("_timestamp", 0),
                    "status": r.get("status", ""),
                })
        return {
            "ok": True,
            "operator": operator_name,
            "group_filter": group_key,
            "count": len(matches),
            "states": matches,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["active_states"] = sum(1 for r in rows if r.get("status") == "已生效")
        base["compacted_states"] = sum(1 for r in rows if r.get("_compacted"))
        base["cleaned_states"] = sum(1 for r in rows if r.get("_cleaned"))
        base["unique_key_patterns"] = len(set(
            r.get("状态键模式", "") for r in rows
        ))
        base["state_versions"] = len(store._state_versions)
        base["current_versions"] = sum(
            1 for v in store._state_versions.values() if v.get("status") == "current"
        )
        return base