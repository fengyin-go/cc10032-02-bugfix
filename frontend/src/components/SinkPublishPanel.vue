<template>
  <section class="batch-panel" data-testid="sink-publish-panel">
    <header class="batch-head">
      <div>
        <h3>结果落地发布台账</h3>
        <p class="page-desc">
          按批次号读取同一窗口指标结果数据；发现断档时保持当前发布版本不变，并发重试只接收一组版本。
        </p>
      </div>
      <div class="page-actions">
        <label class="batch-source">
          <span>源端</span>
          <select v-model="selectedSource" data-testid="publish-source-select" @change="reload">
            <option v-for="name in sourceOptions" :key="name" :value="name">{{ name }}</option>
          </select>
        </label>
        <label class="batch-source">
          <span>合并方式</span>
          <select v-model="mergeMode" data-testid="publish-merge-mode">
            <option value="source_offset">按源端位点排序</option>
            <option value="event_time">按事件时间合并同分组</option>
          </select>
        </label>
        <button class="btn" data-testid="publish-ledger" type="button" @click="buildLedger">生成整组台账</button>
        <button class="btn primary" data-testid="publish-submit" type="button" @click="submitPublish">
          发布已封存批次
        </button>
      </div>
    </header>

    <p v-if="message" class="batch-message" data-testid="publish-message">{{ message }}</p>

    <p class="batch-hint">
      缺失批次：
      <strong data-testid="publish-missing">
        {{ state.missing_batch_seqs.length ? state.missing_batch_seqs.join(',') : '无' }}
      </strong>
      <span class="batch-gap-note">缺失批次不会被新结果掩盖，发布版本保持不变。</span>
    </p>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">当前发布版本</span>
        <strong class="stat-value" data-testid="publish-version">v{{ state.state.published_version }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">发布状态</span>
        <strong class="stat-value">{{ state.state.status }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">版本数</span>
        <strong class="stat-value">{{ state.versions.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">被断档拦截</span>
        <strong class="stat-value">{{ blockedAttempts }}</strong>
      </article>
    </div>

    <table class="data-table">
      <thead>
        <tr>
          <th>版本</th>
          <th>批次序号</th>
          <th>合并方式</th>
          <th>组级结果</th>
          <th>逐条结果</th>
          <th>状态</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="version in state.versions" :key="version.version" :data-testid="`publish-row-${version.version}`">
          <td>{{ version.version_label }}</td>
          <td>{{ version.batch_seqs.join(',') }}</td>
          <td>{{ version.merge_mode }}</td>
          <td>{{ version.group_result_count }}</td>
          <td>{{ version.row_result_count }}</td>
          <td>{{ version.status }}</td>
          <td>
            <button class="link" type="button" :data-testid="`publish-detail-${version.version}`" @click="openVersion(version.version)">
              查看逐条明细
            </button>
          </td>
        </tr>
        <tr v-if="!state.versions.length">
          <td colspan="7" class="empty-state">还没有发布版本。</td>
        </tr>
      </tbody>
    </table>

    <section v-if="detail.ok" data-testid="publish-detail" class="publish-detail">
      <h4>发布版本 v{{ detail.version }} 明细（成功 {{ detail.success_count }} / 失败 {{ detail.failed_count }}）</h4>
      <table class="data-table">
        <thead>
          <tr>
            <th>类型</th>
            <th>批次</th>
            <th>窗口</th>
            <th>分组键</th>
            <th>版本</th>
            <th>结果</th>
            <th>错误</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="item in [...detail.group_details, ...detail.row_details]"
            :key="`${item.item_kind}-${item.id}`"
            :data-testid="`publish-detail-row-${item.id}`"
          >
            <td>{{ item.item_kind === 'row' ? '逐条' : '组级' }}</td>
            <td>{{ item.batch_id }}</td>
            <td>{{ item.window_key }}</td>
            <td>{{ item.group_key }}</td>
            <td>{{ item.group_version ?? item.row_version ?? '—' }}</td>
            <td :class="item.ok ? 'ok-text' : 'error-text'">{{ item.ok ? '成功' : '失败' }}</td>
            <td>{{ item.error ?? '—' }}</td>
          </tr>
        </tbody>
      </table>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'

import { batchFlowApi } from '@/api/batch-flow'
import type { PublishDetail, PublishState } from '@/api/batch-flow'

const state = reactive<PublishState>({
  ok: true,
  source: '',
  state: { published_version: 0, status: '未发布', last_batch_ids: [] },
  versions: [],
  attempts: [],
  pending_batch_seqs: [],
  missing_batch_seqs: [],
})
const detail = reactive<PublishDetail>({
  ok: false,
  version: 0,
  group_details: [],
  row_details: [],
  success_count: 0,
  failed_count: 0,
})
const sourceOptions = ref<string[]>([])
const selectedSource = ref('')
const mergeMode = ref('source_offset')
const message = ref('')

const blockedAttempts = computed(
  () => state.attempts.filter((attempt) => attempt.status === 'blocked_by_gap').length,
)

async function loadSources() {
  try {
    const response = await fetch('/api/stream_source?size=200')
    const payload = await response.json()
    sourceOptions.value = (payload.items ?? [])
      .map((row: Record<string, unknown>) => String(row['源端名称'] ?? ''))
      .filter((name: string) => name)
    if (!selectedSource.value && sourceOptions.value.length) {
      selectedSource.value = sourceOptions.value[0]
    }
  } catch {
    sourceOptions.value = []
  }
}

async function reload() {
  if (!selectedSource.value) return
  message.value = ''
  try {
    Object.assign(state, await batchFlowApi.publishState(selectedSource.value))
  } catch (error) {
    message.value = error instanceof Error ? error.message : '发布状态读取失败'
  }
}

async function buildLedger() {
  try {
    const result = await batchFlowApi.ledger(selectedSource.value)
    message.value = result.ok
      ? `已生成整组台账 ${result.ledger_id}${result.idempotent ? '（命中已有台账）' : ''}`
      : String(result.error ?? '台账生成失败')
  } catch (error) {
    message.value = error instanceof Error ? error.message : '台账生成失败'
  }
}

async function submitPublish() {
  try {
    const result = await batchFlowApi.publish(selectedSource.value, [], mergeMode.value)
    message.value = result.ok
      ? `已发布版本 v${result.accepted_version}${result.idempotent ? '（并发重试命中同一版本）' : ''}`
      : String(result.error ?? '发布失败')
    await reload()
  } catch (error) {
    message.value = error instanceof Error ? error.message : '发布失败'
  }
}

async function openVersion(version: number) {
  try {
    const payload = await batchFlowApi.publishDetail(selectedSource.value, version)
    if (!payload.ok) {
      message.value = payload.error ?? '发布明细读取失败'
      return
    }
    Object.assign(detail, payload)
  } catch (error) {
    message.value = error instanceof Error ? error.message : '发布明细读取失败'
  }
}

onMounted(async () => {
  await loadSources()
  await reload()
})
</script>

<style scoped>
.batch-panel { margin-top: 20px; }
.batch-head { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: flex-start; gap: 12px; }
.batch-head h3 { margin: 0; font-size: 15px; }
.batch-source { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--muted); }
.batch-source select { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; }
.batch-message { font-size: 13px; color: var(--brand); }
.batch-hint { font-size: 12px; color: var(--muted); margin: 0 0 8px; }
.batch-gap-note { margin-left: 8px; }
.publish-detail { margin-top: 18px; }
.ok-text { color: #15803d; }
</style>
