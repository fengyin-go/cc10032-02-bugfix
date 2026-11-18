/** 批次链路接口：源端工作台、窗口指标结果、批次台账与结果发布。 */
import { request } from './client'

export type BatchItem = {
  batch_id: string
  batch_seq: number
  source_offset: number
  event_count: number
  status: string
  window_count: number
  group_result_count: number
  row_result_count: number
  sealed: boolean
  receipt_id: string | null
  publish_version: number | null
  missing_result: boolean
}

export type WorkbenchResult = {
  ok: boolean
  error?: string
  source: string
  batches: BatchItem[]
  batch_count: number
  missing_batch_seqs: number[]
  sealed_count: number
  published_count: number
}

export type GroupResult = {
  id: number
  batch_id: string
  batch_seq: number
  window_key: string
  group_key: string
  aggregates: Record<string, number>
  row_count: number
  result_version_label: string
  sealed?: boolean
  receipt_id?: string
}

export type RowResult = {
  id: number
  batch_id: string
  batch_seq: number
  window_key: string
  group_key: string
  event_id: string
  amount: number
  result_version_label: string
}

export type BatchSummary = {
  batch_id: string
  batch_seq: number
  status: string
  window_count: number
  group_results: number
  row_results: number
  group_versions: string[]
  row_versions: string[]
  unique_windows: string[]
  receipt_id: string | null
  publish_version: number | null
}

export type ResultsResult = {
  ok: boolean
  error?: string
  source: string
  batches: BatchSummary[]
  group_results: GroupResult[]
  row_results: RowResult[]
  group_total: number
  row_total: number
}

export type PublishState = {
  ok: boolean
  source: string
  state: { published_version: number; status: string; last_batch_ids: string[] }
  versions: {
    version: number
    version_label: string
    batch_ids: string[]
    batch_seqs: number[]
    merge_mode: string
    group_result_count: number
    row_result_count: number
    status: string
  }[]
  attempts: {
    signature: string
    batch_ids: string[]
    accepted_version: number | null
    status: string
    missing_batch_seqs?: number[]
  }[]
  pending_batch_seqs: number[]
  missing_batch_seqs: number[]
}

export type PublishDetailRow = {
  id: number
  item_kind: 'group' | 'row'
  batch_id: string
  window_key: string
  group_key: string
  group_version: string | null
  row_version: string | null
  event_id: string | null
  ok: boolean
  status: string
  error: string | null
  merged: boolean
}

export type PublishDetail = {
  ok: boolean
  error?: string
  version: number
  meta?: Record<string, unknown>
  group_details: PublishDetailRow[]
  row_details: PublishDetailRow[]
  success_count: number
  failed_count: number
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) throw new Error('批次链路读取失败')
  return (await response.json()) as T
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  if (!response.ok) throw new Error('批次链路操作失败')
  return (await response.json()) as T
}

export const batchFlowApi = {
  workbench: (source: string) =>
    getJson<WorkbenchResult>(`/api/batch-flow/workbench?source=${encodeURIComponent(source)}`),

  results: (source: string, batchId?: string) =>
    getJson<ResultsResult>(
      `/api/batch-flow/results?source=${encodeURIComponent(source)}` +
        (batchId ? `&batch_id=${encodeURIComponent(batchId)}` : ''),
    ),

  publishState: (source: string) =>
    getJson<PublishState>(`/api/batch-flow/publish-state?source=${encodeURIComponent(source)}`),

  publishDetail: (source: string, version: number) =>
    getJson<PublishDetail>(
      `/api/batch-flow/publish-version/${version}?source=${encodeURIComponent(source)}`,
    ),

  diagnose: (source: string) =>
    getJson<Record<string, unknown>>(
      `/api/batch-flow/diagnose?source=${encodeURIComponent(source)}`),

  produce: (source: string, batchSize = 20) =>
    postJson<Record<string, unknown>>('/api/batch-flow/produce', {
      source,
      batch_size: batchSize,
    }),

  windows: (source: string) =>
    postJson<Record<string, unknown>>('/api/batch-flow/windows', { source }),

  compute: (batchId: string) =>
    postJson<Record<string, unknown>>('/api/batch-flow/compute', { batch_id: batchId }),

  seal: (batchId: string) =>
    postJson<Record<string, unknown>>('/api/batch-flow/seal', { batch_id: batchId }),

  ledger: (source: string) =>
    postJson<Record<string, unknown>>('/api/batch-flow/ledger', { source }),

  publish: (
    source: string,
    batchIds: string[] = [],
    mergeMode = 'source_offset',
    resultIds: number[] = [],
  ) =>
    postJson<Record<string, unknown>>('/api/batch-flow/publish', {
      source,
      batch_ids: batchIds,
      result_ids: resultIds.length ? resultIds : null,
      merge_mode: mergeMode,
    }),
}
