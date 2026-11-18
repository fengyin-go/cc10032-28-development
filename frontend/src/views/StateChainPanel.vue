<template>
  <section class="chain-panel" data-testid="state-chain-panel">
    <header class="chain-head">
      <div>
        <h3>序号28 · 状态压缩 → 检查点 → 回放链路</h3>
        <p class="chain-desc">
          压缩重复键生成可验证的状态版本，检查点读取压缩清单生成快照，回放引擎依据快照恢复状态数据并验证键数量。
        </p>
      </div>
      <div class="chain-version" data-testid="current-version">
        <span class="stat-label">当前状态版本</span>
        <strong v-if="detail?.current_version" class="version-badge">
          {{ detail.current_version.version_id }}
        </strong>
        <strong v-else class="version-badge none">未生成</strong>
      </div>
    </header>

    <form class="chain-toolbar" @submit.prevent="runCompact">
      <label class="toolbar-item">
        <span>状态配置</span>
        <select v-model="selectedConfig" data-testid="chain-config">
          <option v-for="name in configNames" :key="name" :value="name">{{ name }}</option>
        </select>
      </label>
      <label class="toolbar-item">
        <span>修复策略</span>
        <select v-model="strategy" data-testid="chain-strategy">
          <option value="archive">归档历史后继续压缩</option>
          <option value="defer">延迟压缩等待回放完成</option>
        </select>
      </label>
      <button class="btn primary" data-testid="chain-compact" type="submit" :disabled="busy">
        触发压缩
      </button>
      <button class="btn" data-testid="chain-checkpoint" type="button" :disabled="busy" @click="runCheckpoint">
        生成检查点
      </button>
      <button
        class="btn ghost"
        data-testid="chain-rollback"
        type="button"
        :disabled="busy || !detail?.current_version"
        @click="runRollback"
      >
        回滚当前版本
      </button>
      <button class="btn ghost" data-testid="chain-refresh" type="button" :disabled="busy" @click="reload">
        刷新
      </button>
    </form>

    <p v-if="actionMessage" class="chain-message" data-testid="chain-message">{{ actionMessage }}</p>
    <p v-if="errorMessage" class="error-text" data-testid="chain-error">{{ errorMessage }}</p>

    <div class="chain-grid">
      <article class="chain-card">
        <h4>活动状态键</h4>
        <table class="data-table">
          <thead>
            <tr><th>状态键模式</th><th>活动键数</th><th>状态ID</th></tr>
          </thead>
          <tbody>
            <tr v-for="item in detail?.active_keys ?? []" :key="item.pattern">
              <td>{{ item.pattern }}</td>
              <td>{{ item.count }}</td>
              <td>{{ item.state_ids.join(', ') }}</td>
            </tr>
            <tr v-if="!detail?.active_keys?.length">
              <td colspan="3" class="empty-state">当前版本下暂无活动键</td>
            </tr>
          </tbody>
        </table>
      </article>

      <article class="chain-card">
        <h4>状态版本（点击展开压缩清单）</h4>
        <table class="data-table">
          <thead>
            <tr><th>版本</th><th>状态</th><th>策略</th><th>键数量(前→后)</th><th>归档</th></tr>
          </thead>
          <tbody>
            <template v-for="v in detail?.versions ?? []" :key="v.version_id">
              <tr
                :class="{ 'row-current': v.status === 'current' }"
                :data-testid="`version-${v.version_id}`"
                @click="toggleManifest(v.version_id)"
              >
                <td>{{ v.version_id }}</td>
                <td>{{ versionStatusText(v.status) }}</td>
                <td>{{ v.strategy }}</td>
                <td>{{ v.manifest.key_count_before }} → {{ v.manifest.key_count_after }}</td>
                <td>{{ v.archived_count }}</td>
              </tr>
              <tr v-if="expandedVersion === v.version_id" class="manifest-row">
                <td colspan="5">
                  <table class="data-table manifest-table">
                    <thead>
                      <tr><th>键模式</th><th>活动键</th><th>已压缩键</th><th>保护</th></tr>
                    </thead>
                    <tbody>
                      <tr v-for="g in v.manifest.groups" :key="g.key_pattern">
                        <td>{{ g.key_pattern }}</td>
                        <td>{{ g.active_keys.join(', ') || '—' }}</td>
                        <td>{{ g.compacted_state_ids.join(', ') || '—' }}</td>
                        <td>{{ g.deferred ? '已延迟' : g.protected ? '受保护' : '—' }}</td>
                      </tr>
                    </tbody>
                  </table>
                </td>
              </tr>
            </template>
            <tr v-if="!detail?.versions?.length">
              <td colspan="5" class="empty-state">暂无状态版本，请先触发压缩</td>
            </tr>
          </tbody>
        </table>
      </article>

      <article class="chain-card">
        <h4>检查点历史</h4>
        <table class="data-table">
          <thead>
            <tr><th>检查点</th><th>状态版本</th><th>键数量</th><th>当前版本</th><th>操作</th></tr>
          </thead>
          <tbody>
            <tr v-for="cp in detail?.checkpoints ?? []" :key="cp.checkpoint_id">
              <td>{{ cp.checkpoint_id }}</td>
              <td>{{ cp.state_version ?? '—' }}</td>
              <td>{{ cp.key_count }}</td>
              <td>{{ cp.is_current_version ? '是' : '否' }}</td>
              <td>
                <button
                  class="link"
                  type="button"
                  :data-testid="`replay-${cp.checkpoint_id}`"
                  :disabled="busy"
                  @click="runReplay(cp.checkpoint_id)"
                >
                  创建回放
                </button>
              </td>
            </tr>
            <tr v-if="!detail?.checkpoints?.length">
              <td colspan="5" class="empty-state">暂无检查点快照</td>
            </tr>
          </tbody>
        </table>
      </article>

      <article class="chain-card">
        <h4>回放历史</h4>
        <table class="data-table">
          <thead>
            <tr><th>任务</th><th>源检查点</th><th>状态版本</th><th>恢复键数/期望</th><th>验证</th></tr>
          </thead>
          <tbody>
            <tr v-for="r in detail?.replays ?? []" :key="r.task_id">
              <td>{{ r.任务名称 }}</td>
              <td>{{ r.源检查点 }}</td>
              <td>{{ r.state_version ?? '—' }}</td>
              <td>{{ r.restored_key_count ?? '—' }}/{{ r.expected_key_count ?? '—' }}</td>
              <td :class="r.结果验证 === 'passed' ? 'ok-text' : 'error-text'">{{ r.结果验证 }}</td>
            </tr>
            <tr v-if="!detail?.replays?.length">
              <td colspan="5" class="empty-state">暂无回放任务</td>
            </tr>
          </tbody>
        </table>
      </article>

      <article class="chain-card ledger-card">
        <h4>修复台账（各模块同步）</h4>
        <table class="data-table">
          <thead>
            <tr><th>动作</th><th>模块</th><th>版本</th><th>摘要</th><th>已同步模块</th></tr>
          </thead>
          <tbody>
            <tr v-for="e in detail?.ledger ?? []" :key="e.ledger_id">
              <td>{{ e.action }}</td>
              <td>{{ e.module }}</td>
              <td>{{ e.version_id ?? '—' }}</td>
              <td>{{ e.summary }}</td>
              <td>{{ e.synced_modules.join('、') }}</td>
            </tr>
            <tr v-if="!detail?.ledger?.length">
              <td colspan="5" class="empty-state">暂无台账记录</td>
            </tr>
          </tbody>
        </table>
      </article>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  compactStateChain,
  createManifestCheckpoint,
  listEntries,
  loadStateChainDetail,
  replayCheckpoint,
  rollbackStateVersion,
} from '@/api/backend-service'
import type { StateChainDetail } from '@/api/backend-service'

const detail = ref<StateChainDetail | null>(null)
const configNames = ref<string[]>([])
const selectedConfig = ref('')
const strategy = ref('archive')
const busy = ref(false)
const actionMessage = ref('')
const errorMessage = ref('')
const expandedVersion = ref('')

// 每次压缩使用固定幂等键：并发重试只生效一次
const idempotencyKey = computed(() => `fix28-${selectedConfig.value}`)

function versionStatusText(status: string): string {
  const mapping: Record<string, string> = {
    current: '当前版本',
    superseded: '已被取代',
    rolled_back: '已回滚',
  }
  return mapping[status] ?? status
}

function toggleManifest(versionId: string) {
  expandedVersion.value = expandedVersion.value === versionId ? '' : versionId
}

async function reload() {
  errorMessage.value = ''
  try {
    detail.value = await loadStateChainDetail(selectedConfig.value || undefined)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '序号28详情读取失败'
  }
}

async function loadConfigs() {
  try {
    const payload = await listEntries('state_store', {})
    const names = payload.items
      .map((row) => String(row['配置名称'] ?? ''))
      .filter((name) => name.length > 0)
    configNames.value = [...new Set(names)]
    if (!selectedConfig.value && configNames.value.length) {
      selectedConfig.value = configNames.value[0]
    }
  } catch {
    configNames.value = []
  }
}

function report(result: { ok: boolean; message?: string; error?: string }, okText: string) {
  if (result.ok) {
    actionMessage.value = result.message ?? okText
    errorMessage.value = ''
  } else {
    errorMessage.value = result.error ?? result.message ?? '操作失败'
    actionMessage.value = ''
  }
}

async function runCompact() {
  if (!selectedConfig.value) return
  busy.value = true
  actionMessage.value = ''
  errorMessage.value = ''
  try {
    const result = await compactStateChain(selectedConfig.value, strategy.value, idempotencyKey.value)
    if (result.deferred) {
      actionMessage.value = result.message ?? result.error ?? '检查点仍引用旧状态，延迟压缩等待回放完成'
    } else if (result.idempotent_hit) {
      actionMessage.value = result.message ?? '相同压缩已应用过，并发重试只生效一次'
    } else {
      report(result, `压缩完成，版本 ${result.version_id ?? ''}`)
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '压缩失败'
  } finally {
    busy.value = false
  }
}

async function runCheckpoint() {
  if (!selectedConfig.value) return
  busy.value = true
  actionMessage.value = ''
  errorMessage.value = ''
  try {
    const result = await createManifestCheckpoint(selectedConfig.value)
    report(result, `检查点 ${result.checkpoint_id ?? ''} 已生成`)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '生成检查点失败'
  } finally {
    busy.value = false
  }
}

async function runReplay(checkpointId: string) {
  busy.value = true
  actionMessage.value = ''
  errorMessage.value = ''
  try {
    const result = await replayCheckpoint(checkpointId, `回放_${checkpointId}`)
    if (result.verification) {
      const v = result.verification
      report(
        result,
        `回放完成：恢复 ${v.restored_key_count}/${v.expected_key_count} 个活动键，验证${v.status === 'passed' ? '通过' : '失败'}`,
      )
    } else {
      report(result, '回放完成')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回放失败'
  } finally {
    busy.value = false
  }
}

async function runRollback() {
  const versionId = detail.value?.current_version?.version_id
  if (!versionId) return
  busy.value = true
  actionMessage.value = ''
  errorMessage.value = ''
  try {
    const result = await rollbackStateVersion(versionId)
    report(result, `版本 ${versionId} 已回滚`)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回滚失败'
  } finally {
    busy.value = false
  }
}

onMounted(async () => {
  await loadConfigs()
  await reload()
})
</script>

<style scoped>
.chain-panel {
  margin-top: 20px;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: #fff;
  padding: 16px;
}
.chain-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 12px;
}
.chain-head h3 {
  margin: 0 0 4px;
  font-size: 15px;
}
.chain-desc {
  margin: 0;
  color: var(--muted);
  font-size: 12px;
}
.chain-version {
  text-align: right;
}
.version-badge {
  display: inline-block;
  padding: 4px 10px;
  border-radius: 6px;
  background: #e8f0fe;
  color: var(--brand);
  font-size: 14px;
}
.version-badge.none {
  background: #f1f5f9;
  color: var(--muted);
}
.chain-toolbar {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  align-items: flex-end;
  margin: 12px 0;
}
.toolbar-item span {
  display: block;
  font-size: 12px;
  color: var(--muted);
  margin-bottom: 4px;
}
.toolbar-item select {
  padding: 6px 10px;
  border: 1px solid var(--border);
  border-radius: 6px;
  font-size: 13px;
  min-width: 180px;
}
.chain-message {
  color: #15803d;
  font-size: 13px;
  margin: 4px 0;
}
.chain-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
}
.chain-card {
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 12px;
  overflow: auto;
}
.chain-card h4 {
  margin: 0 0 8px;
  font-size: 13px;
}
.ledger-card {
  grid-column: 1 / -1;
}
.row-current {
  background: #f0f7ff;
}
.chain-card tbody tr {
  cursor: pointer;
}
.manifest-row td {
  background: #fafbfc;
  cursor: default;
}
.manifest-table {
  font-size: 12px;
}
.ok-text {
  color: #15803d;
}
</style>
