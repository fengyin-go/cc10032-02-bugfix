"""批次链路接口：源端工作台、窗口指标结果、批次台账和结果发布。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from app.services import batch_flow
from app.services.sink_output import SinkOutputService

router = APIRouter(prefix="/api/batch-flow", tags=["批次链路"])


class ProducePayload(BaseModel):
    source: str
    batch_size: int = Field(default=20, ge=1, le=500)


class WindowsPayload(BaseModel):
    source: str
    batch_id: str | None = None
    policy: str | None = None


class ComputePayload(BaseModel):
    batch_id: str
    index_name: str | None = None


class SealPayload(BaseModel):
    batch_id: str


class LedgerPayload(BaseModel):
    source: str


class PublishPayload(BaseModel):
    source: str | None = None
    batch_ids: list[str] | None = None
    result_ids: list[int] | None = None
    merge_mode: str = "source_offset"
    sink: str = "结果落地样例2"


@router.get("/workbench")
def workbench(source: str = Query(...)) -> dict[str, Any]:
    """源端工作台：查看每个批次的处理状态与缺失批次。"""
    return batch_flow.batch_workbench(source)


@router.get("/results")
def results(
    source: str = Query(...),
    batch_id: str | None = Query(default=None),
    kind: str = Query(default="both"),
) -> dict[str, Any]:
    """批量结果页：按批次对照连续且唯一的窗口指标结果数据。"""
    return batch_flow.list_batch_results(source, batch_id=batch_id, kind=kind)


@router.get("/diagnose")
def diagnose(source: str = Query(...)) -> dict[str, Any]:
    """序号2 的失败诊断：源端调用链与结果落地数据流共同定位。"""
    return batch_flow.diagnose_batch_flow(source)


@router.get("/publish-state")
def publish_state(source: str = Query(...)) -> dict[str, Any]:
    """结果落地页：当前发布版本、最近一次发布批次与缺失批次。"""
    state = batch_flow._publish_state(source)
    batches = batch_flow._source_batches(source)
    published_ids = {
        str(b.get("batch_id")) for b in batch_flow._rows(batch_flow.PUBLISH_BATCH_MODULE)
        if b.get("source") == source
    }
    pending_seqs = sorted(
        batch_flow._batch_seq(b) for b in batches
        if str(b.get("batch_id")) not in published_ids
        and batch_flow._receipt_for(str(b.get("batch_id", ""))) is not None
    )
    missing_seqs = (
        batch_flow._detect_gap(
            source,
            [str(b.get("batch_id")) for b in batches],
        )
        if batches else []
    )
    versions = [
        v for v in batch_flow._rows(batch_flow.PUBLISH_VERSION_MODULE)
        if v.get("source") == source
    ]
    batch_publications = [
        b for b in batch_flow._rows(batch_flow.PUBLISH_BATCH_MODULE)
        if b.get("source") == source
    ]
    attempts = [
        a for a in batch_flow._rows(batch_flow.PUBLISH_ATTEMPT_MODULE)
        if a.get("source") == source
    ]
    return {
        "ok": True,
        "source": source,
        "state": state,
        "versions": versions,
        "batch_publications": batch_publications,
        "pending_batch_seqs": pending_seqs,
        "missing_batch_seqs": missing_seqs,
        "attempts": [
            {k: v for k, v in a.items() if k != "detail"} for a in attempts
        ],
    }


@router.get("/publish-version/{version}")
def publish_version(version: int, source: str = Query(...)) -> dict[str, Any]:
    """2 号详情页：逐条显示某发布版本的组级 / 逐条结果。"""
    meta = next(
        (v for v in batch_flow._rows(batch_flow.PUBLISH_VERSION_MODULE)
         if v.get("source") == source and int(v.get("version", 0)) == version),
        None,
    )
    if meta is None:
        return {"ok": False, "error": f"发布版本 v{version} 不存在"}
    detail = [
        d for d in batch_flow._rows(batch_flow.PUBLISH_DETAIL_MODULE)
        if d.get("source") == source and int(d.get("version", 0)) == version
    ]
    return {
        "ok": True,
        "version": version,
        "meta": meta,
        "group_details": [d for d in detail if d.get("item_kind") == "group"],
        "row_details": [d for d in detail if d.get("item_kind") == "row"],
        "success_count": sum(1 for d in detail if d.get("ok")),
        "failed_count": sum(1 for d in detail if not d.get("ok")),
    }


@router.post("/produce")
def produce(payload: ProducePayload) -> dict[str, Any]:
    """生产一个新的源端批次。"""
    return batch_flow.produce_batch(payload.source, payload.batch_size)


@router.post("/windows")
def consume_windows(payload: WindowsPayload) -> dict[str, Any]:
    """按源端批次生成窗口定义（旧批次不会被新批次覆盖）。"""
    from app.services.window_config import WindowConfigService

    return WindowConfigService().consume_source_batch(
        payload.source,
        payload.policy,
        batch_id=payload.batch_id,
    )


@router.post("/compute")
def compute(payload: ComputePayload) -> dict[str, Any]:
    """计算批次窗口指标结果（组级 + 逐条）。"""
    return batch_flow.compute_batch_metrics(
        payload.batch_id, index_name=payload.index_name)


@router.post("/seal")
def seal(payload: SealPayload) -> dict[str, Any]:
    """封存批次结果并生成唯一成功回执。"""
    return batch_flow.seal_batch(payload.batch_id)


@router.post("/ledger")
def ledger(payload: LedgerPayload) -> dict[str, Any]:
    """生成整组处理台账（同一进度只保留一份）。"""
    return batch_flow.build_batch_ledger(payload.source)


@router.post("/publish")
def publish(payload: PublishPayload) -> dict[str, Any]:
    """结果落地：按批次号读取窗口指标结果并整组发布。"""
    return SinkOutputService().submit_window_results(
        payload.sink,
        result_ids=payload.result_ids,
        source_name=payload.source,
        batch_ids=payload.batch_ids,
        merge_mode=payload.merge_mode,
    )
