import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---- 序号28：状态压缩 → 检查点 → 回放链路 ----

export type ManifestGroup = {
  key_pattern: string
  active_state_id: number | string | null
  active_keys: string[]
  compacted_state_ids: string[]
  protected: boolean
  deferred?: boolean
}

export type StateVersionView = {
  version_id: string
  seq: number
  config_name: string
  status: string
  strategy: string
  created_at: number
  pre_version_id: string | null
  manifest: {
    groups: ManifestGroup[]
    key_count_before: number
    key_count_after: number
    compacted_count: number
    archived_count: number
    deferred_groups: string[]
  }
  archived_count: number
}

export type CheckpointHistoryItem = {
  checkpoint_id: string
  config: string
  state_version: string | null
  key_count: number
  is_current_version: boolean
  version_status: string | null
  snapshot_ts: number
  status: string
}

export type ReplayHistoryItem = {
  task_id: number
  任务名称: string
  源检查点: string
  state_version: string | null
  expected_key_count: number | null
  restored_key_count: number | null
  结果验证: string
  status: string
  created_at: number
}

export type LedgerEntry = {
  ledger_id: string
  ticket: string
  action: string
  module: string
  version_id: string | null
  summary: string
  synced_modules: string[]
  ts: number
}

export type StateChainDetail = {
  ok: boolean
  ticket: string
  config: string | null
  current_version: StateVersionView | null
  versions: StateVersionView[]
  active_keys: { pattern: string; count: number; latest_ts: number; state_ids: (string | number)[] }[]
  checkpoints: CheckpointHistoryItem[]
  replays: ReplayHistoryItem[]
  ledger: LedgerEntry[]
  stats: Record<string, number>
}

export type ChainActionResult = {
  ok: boolean
  ticket?: string
  version_id?: string
  checkpoint_id?: string
  state_version?: string
  key_count?: number
  compacted?: number
  archived?: number
  deferred?: boolean
  conflict?: boolean
  rolled_back?: boolean
  idempotent_hit?: boolean
  applied_once?: boolean
  restored_version?: string | null
  manifest?: StateVersionView['manifest']
  verification?: {
    status: string
    all_passed: boolean
    expected_key_count: number
    restored_key_count: number
    missing_keys: string[]
    residual_keys: string[]
  }
  message?: string
  error?: string
}

export async function loadStateChainDetail(config?: string): Promise<StateChainDetail> {
  const query = config ? `?config=${encodeURIComponent(config)}` : ''
  const response = await request(`/api/state-chain/detail${query}`)
  if (!response.ok) throw new Error('序号28详情读取失败')
  return await response.json()
}

export async function compactStateChain(
  configName: string,
  strategy: string,
  idempotencyKey: string,
): Promise<ChainActionResult> {
  const response = await request('/api/state-chain/compact', {
    method: 'POST',
    body: JSON.stringify({
      config_name: configName,
      strategy,
      idempotency_key: idempotencyKey,
    }),
  })
  return await response.json()
}

export async function createManifestCheckpoint(configName: string): Promise<ChainActionResult> {
  const response = await request('/api/state-chain/checkpoint', {
    method: 'POST',
    body: JSON.stringify({ config_name: configName }),
  })
  return await response.json()
}

export async function replayCheckpoint(
  checkpointId: string,
  jobName?: string,
): Promise<ChainActionResult> {
  const response = await request('/api/state-chain/replay', {
    method: 'POST',
    body: JSON.stringify({ checkpoint_id: checkpointId, job_name: jobName }),
  })
  return await response.json()
}

export async function rollbackStateVersion(versionId: string): Promise<ChainActionResult> {
  const response = await request('/api/state-chain/rollback', {
    method: 'POST',
    body: JSON.stringify({ version_id: versionId }),
  })
  return await response.json()
}
