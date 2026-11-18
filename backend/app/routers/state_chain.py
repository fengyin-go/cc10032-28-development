"""序号28链路接口：状态压缩 → 检查点快照 → 回放恢复的完整链路。

修复后接口返回完整结果：详情面板一次载入活动键、状态版本、
检查点历史、回放历史与修复台账，可按检查点版本核对状态压缩清单
和状态恢复数据。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from app.services import service_for
from app.store import store

router = APIRouter(prefix="/api/state-chain", tags=["序号28状态链路"])

TICKET = "序号28"


class CompactPayload(BaseModel):
    config_name: str
    strategy: str = "archive"  # defer=延迟等待回放完成 / archive=归档历史后继续
    idempotency_key: str | None = None


class CheckpointPayload(BaseModel):
    config_name: str


class ReplayPayload(BaseModel):
    checkpoint_id: str
    job_name: str | None = None


class RollbackPayload(BaseModel):
    version_id: str


def _state_service():
    return service_for("state_store")


def _checkpoint_service():
    return service_for("checkpoint_mgr")


def _replay_service():
    return service_for("replay_engine")


def _version_public(version: dict[str, Any]) -> dict[str, Any]:
    """版本记录对外视图：去掉内部回滚快照等大字段。"""
    return {
        "version_id": version.get("version_id"),
        "seq": version.get("seq"),
        "config_name": version.get("config_name"),
        "status": version.get("status"),
        "strategy": version.get("strategy"),
        "created_at": version.get("created_at"),
        "pre_version_id": version.get("pre_version_id"),
        "manifest": version.get("manifest", {}),
        "archived_count": len(version.get("history_archive", [])),
    }


@router.get("/detail")
def state_chain_detail(config: str | None = Query(default=None)) -> dict[str, Any]:
    """序号28详情：活动键、版本、检查点与回放历史、修复台账。"""
    state_svc = _state_service()
    cp_svc = _checkpoint_service()
    replay_svc = _replay_service()

    versions = store.state_versions_for(config)
    current = store.current_state_version(config) if config else None
    if current is None and config is None:
        currents = [
            v for v in versions if v.get("status") == "current"
        ]
        current = currents[-1] if currents else None

    return {
        "ok": True,
        "ticket": TICKET,
        "config": config,
        "current_version": _version_public(current) if current else None,
        "versions": [_version_public(v) for v in versions],
        "active_keys": state_svc.list_active_state_keys(config),
        "checkpoints": cp_svc.list_checkpoints_with_version(config),
        "replays": replay_svc.list_replay_restore_history(config),
        "ledger": list(reversed(store._fix_ledger[-50:])),
        "stats": {
            "versions": len(versions),
            "rolled_back": sum(1 for v in versions if v.get("status") == "rolled_back"),
            "checkpoints": len(cp_svc.list_checkpoints_with_version(config)),
            "replays": len(replay_svc.list_replay_restore_history(config)),
            "ledger_entries": len(store._fix_ledger),
        },
    }


@router.post("/compact")
def compact_state(payload: CompactPayload) -> dict[str, Any]:
    """触发状态压缩：幂等，并发重试只生效一次。"""
    result = _state_service().compact_duplicate_states(
        payload.config_name,
        strategy=payload.strategy,
        idempotency_key=payload.idempotency_key,
    )
    return {"ticket": TICKET, **result}


@router.post("/checkpoint")
def create_checkpoint(payload: CheckpointPayload) -> dict[str, Any]:
    """检查点模块读取状态压缩清单生成快照。"""
    result = _checkpoint_service().create_checkpoint_from_manifest(payload.config_name)
    return {"ticket": TICKET, **result}


@router.post("/replay")
def replay_from_checkpoint(payload: ReplayPayload) -> dict[str, Any]:
    """回放引擎依据快照恢复状态数据并验证键数量。"""
    result = _replay_service().restore_state_data(
        payload.checkpoint_id, job_name=payload.job_name,
    )
    return {"ticket": TICKET, **result}


@router.post("/rollback")
def rollback_version(payload: RollbackPayload) -> dict[str, Any]:
    """事务中断/人工回滚：回到修复前版本。"""
    result = _state_service().rollback_state_version(payload.version_id)
    return {"ticket": TICKET, **result}


@router.get("/versions/{version_id}/manifest")
def version_manifest(version_id: str) -> dict[str, Any]:
    """按检查点版本核对状态压缩清单和状态恢复数据。"""
    version = store._state_versions.get(version_id)
    if version is None:
        return {"ok": False, "error": f"状态版本「{version_id}」不存在"}
    checkpoints = [
        cp for cp in _checkpoint_service().list_checkpoints_with_version()
        if cp.get("state_version") == version_id
    ]
    replays = [
        r for r in _replay_service().list_replay_restore_history()
        if r.get("state_version") == version_id
    ]
    return {
        "ok": True,
        "ticket": TICKET,
        "version": _version_public(version),
        "history_archive": version.get("history_archive", []),
        "checkpoints": checkpoints,
        "replays": replays,
    }


@router.get("/ledger")
def fix_ledger() -> dict[str, Any]:
    """修复台账：序号28全链路动作记录，含各模块同步状态。"""
    return {
        "ok": True,
        "ticket": TICKET,
        "total": len(store._fix_ledger),
        "entries": list(reversed(store._fix_ledger)),
    }
