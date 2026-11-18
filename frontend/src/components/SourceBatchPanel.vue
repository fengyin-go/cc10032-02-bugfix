<template>
  <section class="batch-panel" data-testid="source-batch-panel">
    <header class="batch-head">
      <div>
        <h3>源端批次工作台</h3>
        <p class="page-desc">
          按批次查看处理状态、完成回执与发布版本；较早批次未完成时新批次不会越过它写入。
        </p>
      </div>
      <div class="page-actions">
        <label class="batch-source">
          <span>源端</span>
          <select v-model="selectedSource" data-testid="batch-source-select" @change="reload">
            <option v-for="name in sourceOptions" :key="name" :value="name">{{ name }}</option>
          </select>
        </label>
        <button class="btn" data-testid="batch-produce" type="button" @click="produceBatch">生产新批次</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">批次数</span>
        <strong class="stat-value">{{ workbench.batch_count }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">已封存</span>
        <strong class="stat-value">{{ workbench.sealed_count }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">已发布</span>
        <strong class="stat-value">{{ workbench.published_count }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">缺失批次</span>
        <strong class="stat-value" data-testid="batch-missing">
          {{ workbench.missing_batch_seqs.length ? workbench.missing_batch_seqs.join(',') : '无' }}
        </strong>
      </article>
    </div>

    <p v-if="message" class="batch-message" data-testid="batch-message">{{ message }}</p>

    <p class="batch-hint">
      缺失批次：
      <strong data-testid="batch-missing-seq">
        {{ workbench.missing_batch_seqs.length ? workbench.missing_batch_seqs.join(',') : '无' }}
      </strong>
    </p>

    <table class="data-table">
      <thead>
        <tr>
          <th>批次序号</th>
          <th>批次号</th>
          <th>源位点</th>
          <th>事件数</th>
          <th>窗口数</th>
          <th>组级结果</th>
          <th>逐条结果</th>
          <th>处理状态</th>
          <th>完成回执</th>
          <th>发布版本</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="batch in workbench.batches"
          :key="batch.batch_id"
          :data-testid="`batch-row-${batch.batch_seq}`"
          :data-status="batch.status"
        >
          <td>{{ batch.batch_seq }}</td>
          <td><code>{{ batch.batch_id }}</code></td>
          <td>{{ batch.source_offset ? batch.source_offset.toFixed(3) : '—' }}</td>
          <td>{{ batch.event_count }}</td>
          <td>{{ batch.window_count }}</td>
          <td>{{ batch.group_result_count }}</td>
          <td>{{ batch.row_result_count }}</td>
          <td>
            <span :class="['batch-state', batch.sealed ? 'done' : 'open']">{{ batch.status }}</span>
          </td>
          <td><code v-if="batch.receipt_id">{{ batch.receipt_id }}</code><span v-else>—</span></td>
          <td>{{ batch.publish_version ? `v${batch.publish_version}` : '—' }}</td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`batch-compute-${batch.batch_seq}`" @click="computeBatch(batch)">
              计算指标
            </button>
            <button class="link" type="button" :data-testid="`batch-seal-${batch.batch_seq}`" @click="sealBatch(batch)">
              封存结果
            </button>
          </td>
        </tr>
        <tr v-if="!workbench.batches.length">
          <td colspan="11" class="empty-state">该源端还没有生产批次，可先点击“生产新批次”。</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'

import { batchFlowApi } from '@/api/batch-flow'
import type { BatchItem, WorkbenchResult } from '@/api/batch-flow'

const props = defineProps<{ moduleKey: string }>()
void props

const workbench = reactive<WorkbenchResult>({
  ok: true,
  source: '',
  batches: [],
  batch_count: 0,
  missing_batch_seqs: [],
  sealed_count: 0,
  published_count: 0,
})
const sourceOptions = ref<string[]>([])
const selectedSource = ref('')
const message = ref('')

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

function applyResult(result: WorkbenchResult) {
  if (!result.ok) {
    message.value = result.error ?? '批次读取失败'
    return
  }
  Object.assign(workbench, result)
}

async function reload() {
  if (!selectedSource.value) return
  message.value = ''
  try {
    applyResult(await batchFlowApi.workbench(selectedSource.value))
  } catch (error) {
    message.value = error instanceof Error ? error.message : '批次读取失败'
  }
}

async function produceBatch() {
  try {
    const result = await batchFlowApi.produce(selectedSource.value)
    message.value = result.ok ? `已生产批次 ${result.batch_id}` : String(result.error ?? '生产失败')
    await reload()
  } catch (error) {
    message.value = error instanceof Error ? error.message : '生产失败'
  }
}

async function computeBatch(batch: BatchItem) {
  try {
    const result = await batchFlowApi.compute(batch.batch_id)
    message.value = result.ok
      ? `批次 ${batch.batch_seq} 已计算 ${result.group_results} 组 / ${result.row_results} 条`
      : String(result.error ?? '计算失败')
    await reload()
  } catch (error) {
    message.value = error instanceof Error ? error.message : '计算失败'
  }
}

async function sealBatch(batch: BatchItem) {
  try {
    const result = await batchFlowApi.seal(batch.batch_id)
    message.value = result.ok
      ? `批次 ${batch.batch_seq} 已封存，回执 ${result.receipt_id}`
      : String(result.error ?? '封存失败')
    await reload()
  } catch (error) {
    message.value = error instanceof Error ? error.message : '封存失败'
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
.batch-state.done { color: #15803d; }
.batch-state.open { color: #b45309; }
.batch-message { font-size: 13px; color: var(--brand); }
.batch-hint { font-size: 12px; color: var(--muted); margin: 0 0 8px; }
:deep(code) { font-size: 12px; }
</style>
