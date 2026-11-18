<template>
  <section class="batch-panel" data-testid="batch-result-panel">
    <header class="batch-head">
      <div>
        <h3>窗口指标结果（按批次）</h3>
        <p class="page-desc">
          对照每个批次连续且唯一的窗口指标结果数据；组级与逐条结果各自形成版本。
        </p>
      </div>
      <div class="page-actions">
        <label class="batch-source">
          <span>源端</span>
          <select v-model="selectedSource" data-testid="result-source-select" @change="reload">
            <option v-for="name in sourceOptions" :key="name" :value="name">{{ name }}</option>
          </select>
        </label>
        <label class="batch-source">
          <span>批次</span>
          <select v-model="selectedBatch" data-testid="result-batch-select" @change="reload">
            <option value="">全部批次</option>
            <option v-for="batch in results.batches" :key="batch.batch_id" :value="batch.batch_id">
              批次 {{ batch.batch_seq }}
            </option>
          </select>
        </label>
      </div>
    </header>

    <p v-if="message" class="batch-message" data-testid="result-message">{{ message }}</p>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">组级结果</span>
        <strong class="stat-value">{{ results.group_total }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">逐条结果</span>
        <strong class="stat-value">{{ results.row_total }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">连续批次</span>
        <strong class="stat-value">{{ continuousBatches }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">唯一窗口</span>
        <strong class="stat-value">{{ uniqueWindows }}</strong>
      </article>
    </div>

    <table class="data-table summary-table">
      <thead>
        <tr>
          <th>批次</th>
          <th>状态</th>
          <th>窗口数</th>
          <th>组级结果</th>
          <th>逐条结果</th>
          <th>组级版本</th>
          <th>逐条版本</th>
          <th>回执</th>
          <th>发布版本</th>
          <th>选择</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="batch in results.batches"
          :key="batch.batch_id"
          :data-testid="`result-batch-${batch.batch_seq}`"
        >
          <td>批次 {{ batch.batch_seq }}</td>
          <td>{{ batch.status }}</td>
          <td>{{ batch.window_count }}</td>
          <td>{{ batch.group_results }}</td>
          <td>{{ batch.row_results }}</td>
          <td>{{ batch.group_versions.join(' / ') || '—' }}</td>
          <td>{{ batch.row_versions.join(' / ') || '—' }}</td>
          <td><code v-if="batch.receipt_id">{{ batch.receipt_id }}</code><span v-else>—</span></td>
          <td>{{ batch.publish_version ? `v${batch.publish_version}` : '—' }}</td>
          <td>
            <button
              class="link"
              type="button"
              :data-testid="`result-select-${batch.batch_seq}`"
              @click="toggleBatch(batch.batch_id)"
            >
              {{ selectedBatchIds.includes(batch.batch_id) ? '取消' : '选择' }}
            </button>
          </td>
        </tr>
      </tbody>
    </table>

    <div class="batch-actions">
      <span>已选 {{ selectedBatchIds.length }} 个批次（{{ selectedGroupIds.length }} 条组级结果）</span>
      <button class="btn primary" data-testid="result-publish" type="button" @click="publishSelected">
        提交结果落地
      </button>
    </div>

    <h4 class="group-title">组级窗口指标结果</h4>
    <table class="data-table">
      <thead>
        <tr>
          <th>选择</th>
          <th>批次</th>
          <th>窗口</th>
          <th>分组键</th>
          <th>组级版本</th>
          <th>条数</th>
          <th>聚合值</th>
          <th>状态</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in visibleGroups" :key="item.id" :data-testid="`group-result-${item.id}`">
          <td>
            <input
              v-model="selectedGroupIds"
              type="checkbox"
              :value="item.id"
              :data-testid="`group-check-${item.id}`"
            />
          </td>
          <td>{{ item.batch_seq }}</td>
          <td>{{ item.window_key }}</td>
          <td>{{ item.group_key }}</td>
          <td>{{ item.result_version_label }}</td>
          <td>{{ item.row_count }}</td>
          <td>{{ formatAggregates(item.aggregates) }}</td>
          <td>{{ item.sealed ? '已封存' : '已计算' }}</td>
        </tr>
        <tr v-if="!visibleGroups.length">
          <td colspan="8" class="empty-state">暂无组级窗口指标结果。</td>
        </tr>
      </tbody>
    </table>

    <h4 class="group-title">逐条窗口指标结果</h4>
    <table class="data-table">
      <thead>
        <tr>
          <th>批次</th>
          <th>窗口</th>
          <th>分组键</th>
          <th>事件</th>
          <th>指标值</th>
          <th>逐条版本</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in visibleRows" :key="item.id" :data-testid="`row-result-${item.id}`">
          <td>{{ item.batch_seq }}</td>
          <td>{{ item.window_key }}</td>
          <td>{{ item.group_key }}</td>
          <td><code>{{ item.event_id }}</code></td>
          <td>{{ item.amount }}</td>
          <td>{{ item.result_version_label }}</td>
        </tr>
        <tr v-if="!visibleRows.length">
          <td colspan="6" class="empty-state">暂无逐条窗口指标结果。</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'

import { batchFlowApi } from '@/api/batch-flow'
import type { ResultsResult } from '@/api/batch-flow'

const results = reactive<ResultsResult>({
  ok: true,
  source: '',
  batches: [],
  group_results: [],
  row_results: [],
  group_total: 0,
  row_total: 0,
})
const sourceOptions = ref<string[]>([])
const selectedSource = ref('')
const selectedBatch = ref('')
const selectedBatchIds = ref<string[]>([])
const selectedGroupIds = ref<number[]>([])
const message = ref('')

const visibleGroups = computed(() =>
  selectedBatch.value
    ? results.group_results.filter((item) => item.batch_id === selectedBatch.value)
    : results.group_results,
)
const visibleRows = computed(() =>
  selectedBatch.value
    ? results.row_results.filter((item) => item.batch_id === selectedBatch.value)
    : results.row_results,
)
const continuousBatches = computed(() => {
  const seqs = results.batches.map((batch) => batch.batch_seq).sort((a, b) => a - b)
  if (!seqs.length) return 0
  return seqs[seqs.length - 1] - seqs[0] + 1 === seqs.length ? seqs.length : `${seqs.length}/${seqs[seqs.length - 1]}`
})
const uniqueWindows = computed(
  () => new Set(results.group_results.map((item) => item.window_key)).size,
)

function formatAggregates(aggregates: Record<string, number>): string {
  return Object.entries(aggregates)
    .map(([key, value]) => `${key}=${value}`)
    .join('，')
}

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
    const payload = await batchFlowApi.results(selectedSource.value, selectedBatch.value || undefined)
    if (!payload.ok) {
      message.value = payload.error ?? '批次结果读取失败'
      return
    }
    Object.assign(results, payload)
  } catch (error) {
    message.value = error instanceof Error ? error.message : '批次结果读取失败'
  }
}

function toggleBatch(batchId: string) {
  const index = selectedBatchIds.value.indexOf(batchId)
  if (index >= 0) {
    selectedBatchIds.value.splice(index, 1)
  } else {
    selectedBatchIds.value.push(batchId)
  }
}

async function publishSelected() {
  message.value = ''
  try {
    const payload = await batchFlowApi.publish(
      selectedSource.value,
      selectedBatchIds.value,
      'source_offset',
      selectedGroupIds.value,
    )
    if (!payload.ok) {
      message.value = String(payload.error ?? '结果落地失败')
      return
    }
    message.value = `已发布版本 v${payload.accepted_version}：组级 ${payload.group_count} 条，逐条 ${payload.row_count} 条`
    selectedGroupIds.value = []
    await reload()
  } catch (error) {
    message.value = error instanceof Error ? error.message : '结果落地失败'
  }
}

onMounted(async () => {
  await loadSources()
  await reload()
})
</script>

<style scoped>
.batch-panel { margin-top: 20px; }
.batch-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }
.batch-head h3 { margin: 0; font-size: 15px; }
.batch-source { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--muted); }
.batch-source select { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; }
.batch-message { font-size: 13px; color: var(--brand); }
.batch-actions { display: flex; justify-content: space-between; align-items: center; margin: 10px 0; font-size: 13px; }
.group-title { font-size: 14px; margin: 18px 0 8px; }
</style>
