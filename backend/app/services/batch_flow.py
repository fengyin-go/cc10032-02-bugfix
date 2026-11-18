"""按源端批次处理窗口定义、窗口指标结果、封存回执、处理台账和结果发布。

数据流：

    stream_source（生产批次，带 batch_seq / source_offset）
        -> window_config（按批次生成窗口定义，不覆盖旧批次）
        -> window_metric_result（组级结果 + 逐条结果，各自形成版本）
        -> batch_completion_receipt（每个批次唯一的一份成功回执）
        -> sink_output（按批次号读取结果，断档不发布，整组事务写入）

约定：
  * 同一批次重复执行只会命中原有结果和回执（幂等），并发请求由 BATCH_LOCK 串行化。
  * 较早批次未封存时，新批次的指标结果不能越序写入。
  * 结果发布按源位点排序，也可以按业务事件时间合并同一分组。
  * 发布发现批次断档时保持当前发布版本不变。
  * 发布、台账写入使用整组事务，异常时回滚本次新增的全部数据。
"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterable

from app.store import store

DEFINITION_MODULE = "window_config"
METRIC_MODULE = "window_metric_result"
ROW_MODULE = "window_metric_row_result"
RECEIPT_MODULE = "batch_completion_receipt"
BATCH_STATE_MODULE = "source_batch_state"
LEDGER_MODULE = "batch_ledger"
LEDGER_DETAIL_MODULE = "batch_ledger_detail"
PUBLISH_STATE_MODULE = "sink_publish_state"
PUBLISH_VERSION_MODULE = "sink_publish_version"
PUBLISH_DETAIL_MODULE = "sink_publish_detail"
PUBLISH_BATCH_MODULE = "sink_publish_batch"
PUBLISH_ATTEMPT_MODULE = "sink_publish_attempt"

# 批次处理、版本分配和发布共用一把可重入锁：同一进程内保证“检查 + 写入”原子。
BATCH_LOCK = threading.RLock()

DEFAULT_GROUP_KEYS = ["user_id", "region"]
DEFAULT_AGG_FUNCS = ["count", "sum", "avg", "max", "min"]
TERMINAL_BATCH_STATE = "已封存"


# ---------------------------------------------------------------------------
# 通用工具
# ---------------------------------------------------------------------------

def _rows(module: str) -> list[dict[str, Any]]:
    return store.rows(module)


def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in _rows(module)), default=0) + 1


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _batch_seq(batch: dict[str, Any], fallback: int = 0) -> int:
    try:
        return int(batch.get("batch_seq") or fallback)
    except (TypeError, ValueError):
        return fallback


def _source_offset(batch: dict[str, Any]) -> float:
    return _to_float(batch.get("source_offset"), _to_float(batch.get("produced_at"), 0.0))


def _content_hash(payload: Any) -> str:
    raw = repr(payload).encode("utf-8", "replace")
    return hashlib.sha1(raw).hexdigest()[:16]


@contextmanager
def _atomic_tables(*module_names: str):
    """整组事务：出现异常时恢复这些表的原快照，避免半组数据落库。"""
    for name in module_names:
        _rows(name)
    snapshots = {name: copy.deepcopy(_rows(name)) for name in module_names}
    try:
        yield
    except Exception:
        for name, rows in snapshots.items():
            store._tables[name] = rows
        raise


def _source_batches(source_name: str) -> list[dict[str, Any]]:
    batches = [
        r for r in _rows("stream_source")
        if isinstance(r.get("batch_id"), str) and r.get("source") == source_name
    ]
    for index, batch in enumerate(batches, 1):
        if not batch.get("batch_seq"):
            batch["batch_seq"] = index
    return sorted(batches, key=lambda b: _batch_seq(b))


def find_batch(batch_id: str) -> dict[str, Any] | None:
    for row in _rows("stream_source"):
        if row.get("batch_id") == batch_id:
            return row
    return None


def _batch_state(batch: dict[str, Any]) -> dict[str, Any]:
    batch_id = str(batch.get("batch_id", ""))
    for row in _rows(BATCH_STATE_MODULE):
        if row.get("batch_id") == batch_id:
            return row
    return {
        "batch_id": batch_id,
        "source": batch.get("source", ""),
        "batch_seq": _batch_seq(batch),
        "status": "已生产",
    }


def _touch_batch_state(batch: dict[str, Any], status: str, **fields: Any) -> dict[str, Any]:
    state = _batch_state(batch)
    existing = state.get("batch_id") and any(
        r.get("batch_id") == state["batch_id"] for r in _rows(BATCH_STATE_MODULE)
    )
    state.update({
        "source": batch.get("source", ""),
        "batch_seq": _batch_seq(batch),
        "status": status,
        "updated_at": time.time(),
    })
    state.update(fields)
    if not existing:
        state["id"] = _next_id(BATCH_STATE_MODULE)
        _rows(BATCH_STATE_MODULE).append(state)
    return state


def _window_rows(batch_id: str) -> list[dict[str, Any]]:
    return [r for r in _rows(DEFINITION_MODULE) if r.get("batch_id") == batch_id]


def _receipt_for(batch_id: str) -> dict[str, Any] | None:
    for row in _rows(RECEIPT_MODULE):
        if row.get("batch_id") == batch_id:
            return row
    return None


def _metric_rows(batch_id: str) -> list[dict[str, Any]]:
    return [r for r in _rows(METRIC_MODULE) if r.get("batch_id") == batch_id]


def _row_rows(batch_id: str) -> list[dict[str, Any]]:
    return [r for r in _rows(ROW_MODULE) if r.get("batch_id") == batch_id]


def _policy_row(policy_name: str) -> dict[str, Any] | None:
    for row in _rows(DEFINITION_MODULE):
        if row.get("策略名称") == policy_name and not row.get("batch_id"):
            return row
    return None


def _index_row(index_name: str | None) -> dict[str, Any] | None:
    if not index_name:
        return None
    for row in _rows("sliding_index"):
        if row.get("指标名称") == index_name and row.get("id", 0) < 100:
            return row
    return None


def _split_terms(value: Any) -> list[str]:
    if not value:
        return []
    text = str(value)
    for sep in ("，", ";", "、", "|", "/"):
        text = text.replace(sep, ",")
    return [part.strip() for part in text.split(",") if part.strip()]


# ---------------------------------------------------------------------------
# 1. 源端批次读取
# ---------------------------------------------------------------------------

def produce_batch(source_name: str, batch_size: int = 20) -> dict[str, Any]:
    """生产一个新源端批次；批次序号与源位点在锁内分配，避免并发重号。"""
    with BATCH_LOCK:
        source = None
        for row in _rows("stream_source"):
            if row.get("源端名称") == source_name and not row.get("batch_id"):
                source = row
                break
        if source is None:
            return {"ok": False, "error": f"源端「{source_name}」不存在"}
        if source.get("status") == "异常断连":
            return {"ok": False, "error": f"源端「{source_name}」异常断连, 无法生产数据"}
        if source.get("status") == "已下线":
            return {"ok": False, "error": f"源端「{source_name}」已下线"}

        batch_id = hashlib.md5(
            f"{source_name}_{time.time()}_{len(_source_batches(source_name))}".encode()
        ).hexdigest()[:12]
        base_ts = time.time()
        events = []
        for i in range(batch_size):
            events.append({
                "event_id": f"{batch_id}_{i:04d}",
                "topic": source.get("Topic名称", ""),
                "format": source.get("数据格式", "JSON"),
                "payload": {
                    "user_id": f"u{i % 5}",
                    "value": round(10.0 + i * 3.7 + (hash(str(i)) % 50), 2),
                    "amount": round(10.0 + i * 3.7 + (hash(str(i)) % 50), 2),
                    "region": f"region_{i % 4}",
                    "device": f"device_{i % 3}",
                },
                "event_time": base_ts + i * 1.5,
                "source": source_name,
            })

        source_batches = _source_batches(source_name)
        seq = max((_batch_seq(b) for b in source_batches), default=0) + 1
        source_batch = {
            "batch_id": batch_id,
            "batch_seq": seq,
            "source": source_name,
            "event_count": len(events),
            "events": events,
            "produced_at": time.time(),
            "source_offset": max(
                (e["event_time"] for e in events), default=base_ts),
            "topic": source.get("Topic名称", ""),
            "deserializer": source.get("反序列化", "DEFAULT"),
            "consumer_group": source.get("消费者组", "default"),
        }
        _rows("stream_source").append(source_batch)
        _touch_batch_state(source_batch, "已生产")
        return {
            "ok": True,
            "batch_id": batch_id,
            "batch_seq": seq,
            "event_count": len(events),
            "source_offset": source_batch["source_offset"],
        }


def batch_workbench(source_name: str) -> dict[str, Any]:
    """源端工作台：按批次列出处理状态、回执、发布版本和缺失批次。"""
    if not any(r.get("源端名称") == source_name for r in _rows("stream_source")):
        return {"ok": False, "error": f"源端「{source_name}」不存在"}
    batches = _source_batches(source_name)
    published = {
        str(r.get("batch_id")): int(r.get("version", 0))
        for r in _rows(PUBLISH_BATCH_MODULE)
        if r.get("source") == source_name
    }
    items: list[dict[str, Any]] = []
    for batch in batches:
        batch_id = str(batch.get("batch_id", ""))
        state = _batch_state(batch)
        windows = _window_rows(batch_id)
        group_count = len(_metric_rows(batch_id))
        row_count = len(_row_rows(batch_id))
        receipt = _receipt_for(batch_id)
        version = published.get(batch_id)
        if version:
            status = "已发布"
        elif receipt:
            status = "已封存"
        elif group_count:
            status = "指标已计算"
        elif windows:
            status = "窗口已生成"
        else:
            status = str(state.get("status") or "已生产")
        items.append({
            "batch_id": batch_id,
            "batch_seq": _batch_seq(batch),
            "source_offset": _source_offset(batch),
            "event_count": len(batch.get("events") or []),
            "produced_at": batch.get("produced_at"),
            "status": status,
            "window_count": sum(len(w.get("window_definitions") or []) for w in windows),
            "group_result_count": group_count,
            "row_result_count": row_count,
            "sealed": receipt is not None,
            "receipt_id": receipt.get("receipt_id") if receipt else None,
            "publish_version": version,
            "missing_result": bool(windows and not group_count and not receipt),
        })
    seqs = [_batch_seq(b) for b in batches]
    missing_seqs = [
        seq for seq in range(1, max(seqs, default=0) + 1) if seq not in seqs
    ]
    ledgers = [
        r for r in _rows(LEDGER_MODULE) if r.get("source") == source_name
    ]
    latest_ledger = ledgers[-1] if ledgers else None
    return {
        "ok": True,
        "source": source_name,
        "batches": items,
        "batch_count": len(items),
        "missing_batch_seqs": missing_seqs,
        "latest_ledger": latest_ledger,
        "sealed_count": sum(1 for i in items if i["sealed"]),
        "published_count": sum(1 for i in items if i["publish_version"]),
    }


# ---------------------------------------------------------------------------
# 2. 按批次生成窗口定义
# ---------------------------------------------------------------------------

def _policy_params(policy_name: str) -> dict[str, Any] | None:
    policy = _policy_row(policy_name)
    if policy is None:
        return None
    wtype = str(policy.get("窗口类型", "sliding"))
    wsize = _to_float(policy.get("窗口大小"), 60.0)
    slide = _to_float(policy.get("滑动步长"), wsize)
    if wsize <= 0:
        wsize = 60.0
    if slide <= 0 or slide > wsize:
        slide = wsize
    return {
        "policy": policy,
        "window_type": wtype,
        "window_size": wsize,
        "slide": slide if wtype != "tumbling" else wsize,
        "allowed_lateness": _to_float(policy.get("水位延迟"), 10.0),
    }


def _build_window_definitions(
    batch: dict[str, Any], params: dict[str, Any],
) -> list[dict[str, Any]]:
    events = sorted(
        batch.get("events") or [], key=lambda e: _to_float(e.get("event_time"))
    )
    if not events:
        return []
    first_ts = _to_float(events[0].get("event_time"))
    last_ts = _to_float(events[-1].get("event_time"))
    wsize = params["window_size"]
    step = params["slide"]
    windows: list[dict[str, Any]] = []
    window_start = first_ts
    while window_start <= last_ts:
        window_end = window_start + wsize
        window_events = [
            e for e in events
            if window_start <= _to_float(e.get("event_time")) < window_end
        ]
        if window_events:
            windows.append({
                "window_key": f"w_{int(window_start)}_{int(window_end)}",
                "window_start": window_start,
                "window_end": window_end,
                "event_count": len(window_events),
                "event_ids": [str(e.get("event_id", "")) for e in window_events],
                "policy": params["policy"].get("策略名称", ""),
                "source": batch.get("source", ""),
                "batch_id": batch.get("batch_id", ""),
                "batch_seq": _batch_seq(batch),
                "source_offset": _source_offset(batch),
            })
        window_start += step
    return windows


def consume_batch_windows(
    source_name: str,
    batch_id: str | None = None,
    policy_name: str | None = None,
) -> dict[str, Any]:
    """按源端批次读取数据并生成窗口定义；同一批次重复调用返回原定义。"""
    with BATCH_LOCK:
        batches = _source_batches(source_name)
        if not batches:
            return {"ok": False, "error": f"未找到源端「{source_name}」的生产批次"}

        if batch_id:
            batch = next(
                (b for b in batches if b.get("batch_id") == batch_id), None)
            if batch is None:
                return {"ok": False, "error": f"批次「{batch_id}」不属于源端「{source_name}」"}
            targets = [batch]
        else:
            # 修复点：逐个批次读取，不再只取 produced_at 最大的那一个批次。
            targets = [
                b for b in batches
                if not any(
                    w.get("batch_id") == b.get("batch_id")
                    for w in _rows(DEFINITION_MODULE)
                )
            ]
            if not targets:
                targets = batches[-1:]

        policy_name = policy_name or f"window_from_{source_name}"
        params = _policy_params(policy_name)
        if params is None:
            policy_name = str((batches[0].get("policy_name") or "窗口配置样例2"))
            params = _policy_params(policy_name)
        if params is None:
            # 没有显式策略时用批次默认参数，保持链路可用。
            params = {
                "policy": {"策略名称": policy_name},
                "window_type": "sliding",
                "window_size": 60.0,
                "slide": 30.0,
                "allowed_lateness": 10.0,
            }

        results: list[dict[str, Any]] = []
        for target in targets:
            bid = str(target.get("batch_id", ""))
            existing = _window_rows(bid)
            if existing:
                results.append({
                    "batch_id": bid,
                    "batch_seq": _batch_seq(target),
                    "windows_generated": sum(
                        len(w.get("window_definitions") or []) for w in existing),
                    "reused": True,
                })
                continue

            windows = _build_window_definitions(target, params)
            definition = {
                "id": _next_id(DEFINITION_MODULE),
                "策略名称": policy_name,
                "窗口类型": params["window_type"],
                "窗口大小": str(params["window_size"]),
                "滑动步长": str(params["slide"]),
                "水位延迟": str(params["allowed_lateness"]),
                "触发策略": params["policy"].get("触发策略", "watermark"),
                "延迟处理": params["policy"].get("延迟处理", "bypass"),
                "status": "已生效",
                "pending": True,
                "abnormal": False,
                "batch_id": bid,
                "batch_seq": _batch_seq(target),
                "source": source_name,
                "source_offset": _source_offset(target),
                "events_consumed": len(target.get("events") or []),
                "window_definitions": windows,
                "produced_at": time.time(),
            }
            _rows(DEFINITION_MODULE).append(definition)
            _touch_batch_state(target, "窗口已生成", window_count=len(windows))
            results.append({
                "batch_id": bid,
                "batch_seq": _batch_seq(target),
                "windows_generated": len(windows),
                "reused": False,
            })

        return {
            "ok": True,
            "source": source_name,
            "policy": policy_name,
            "batches": results,
            "windows_generated": sum(r["windows_generated"] for r in results),
            "reused": all(r["reused"] for r in results),
        }


# ---------------------------------------------------------------------------
# 3. 窗口指标计算（组级 + 逐条，各自形成版本）
# ---------------------------------------------------------------------------

def _aggregate(values: list[float]) -> dict[str, float]:
    if not values:
        return {"count": 0.0, "sum": 0.0, "avg": 0.0, "max": 0.0, "min": 0.0}
    return {
        "count": float(len(values)),
        "sum": float(sum(values)),
        "avg": float(sum(values) / len(values)),
        "max": float(max(values)),
        "min": float(min(values)),
    }


def _group_payload(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload")
    return payload if isinstance(payload, dict) else {}


def _event_amount(event: dict[str, Any]) -> float:
    payload = _group_payload(event)
    return _to_float(payload.get("amount", payload.get("value")), 0.0)


def _existing_group_versions(window_key: str, group_key: str) -> int:
    return sum(
        1 for r in _rows(METRIC_MODULE)
        if r.get("window_key") == window_key and r.get("group_key") == group_key
    )


def _existing_row_versions(window_key: str, group_key: str) -> int:
    return sum(
        1 for r in _rows(ROW_MODULE)
        if r.get("window_key") == window_key and r.get("group_key") == group_key
    )


def _batch_order_guard(batch: dict[str, Any]) -> str | None:
    """较早批次未封存时，新批次不得越过它写入指标结果。"""
    seq = _batch_seq(batch)
    for earlier in _source_batches(str(batch.get("source", ""))):
        earlier_seq = _batch_seq(earlier)
        if earlier_seq >= seq:
            continue
        if _receipt_for(str(earlier.get("batch_id", ""))) is None:
            return (
                f"批次 {seq} 不能越过未封存的批次 {earlier_seq}"
                f"（{earlier.get('batch_id')}）写入指标结果"
            )
    return None


def compute_batch_metrics(
    batch_id: str,
    group_keys: Iterable[str] | None = None,
    agg_funcs: Iterable[str] | None = None,
    index_name: str | None = None,
) -> dict[str, Any]:
    """计算某批次的窗口指标：保留组级结果和逐条结果，重复执行幂等。"""
    batch = find_batch(batch_id)
    if batch is None:
        return {"ok": False, "error": f"批次「{batch_id}」不存在"}

    with BATCH_LOCK:
        existing = _metric_rows(batch_id)
        if existing:
            return {
                "ok": True,
                "batch_id": batch_id,
                "batch_seq": _batch_seq(batch),
                "group_results": len(existing),
                "row_results": len(_row_rows(batch_id)),
                "versions": sorted({r.get("result_version_label") for r in existing}),
                "idempotent": True,
            }

        guard = _batch_order_guard(batch)
        if guard:
            return {"ok": False, "error": guard, "batch_id": batch_id}

        definitions = _window_rows(batch_id)
        if not definitions:
            return {"ok": False, "error": f"批次「{batch_id}」尚未生成窗口定义"}

        index_row = _index_row(index_name)
        keys = list(group_keys or [])
        if not keys and index_row is not None:
            keys = _split_terms(index_row.get("分组维度"))
        keys = keys or DEFAULT_GROUP_KEYS
        funcs = list(agg_funcs or [])
        if not funcs and index_row is not None:
            raw_func = str(index_row.get("聚合函数", "count")).lower()
            funcs = [raw_func]
        funcs = funcs or DEFAULT_AGG_FUNCS

        events_by_id = {
            str(e.get("event_id", "")): e for e in batch.get("events") or []
        }
        group_created = 0
        row_created = 0
        group_versions: list[int] = []
        row_versions: list[int] = []

        for definition in definitions:
            for window in definition.get("window_definitions") or []:
                window_key = str(window.get("window_key", ""))
                event_ids = window.get("event_ids") or []
                grouped: dict[str, list[dict[str, Any]]] = {}
                for event_id in event_ids:
                    event = events_by_id.get(str(event_id))
                    if event is None:
                        continue
                    values = [
                        str(_group_payload(event).get(k, "")) for k in keys
                    ]
                    grouped.setdefault("|".join(values) or "__all__", []).append(event)

                for group_key, group_events in sorted(grouped.items()):
                    amounts = [_event_amount(e) for e in group_events]
                    aggregates = _aggregate(amounts)
                    aggregate_view = {name: aggregates[name] for name in funcs if name in aggregates}

                    group_version = _existing_group_versions(window_key, group_key) + 1
                    max_event_time = max(
                        (_to_float(e.get("event_time")) for e in group_events),
                        default=0.0,
                    )
                    _rows(METRIC_MODULE).append({
                        "id": _next_id(METRIC_MODULE),
                        "batch_id": batch_id,
                        "batch_seq": _batch_seq(batch),
                        "source": batch.get("source", ""),
                        "source_offset": _source_offset(batch),
                        "policy": definition.get("策略名称", ""),
                        "index": index_name or "",
                        "window_key": window_key,
                        "window_start": window.get("window_start"),
                        "window_end": window.get("window_end"),
                        "group_key": group_key,
                        "group_values": dict(zip(keys, group_key.split("|"))),
                        "aggregates": aggregate_view,
                        "row_count": len(group_events),
                        "max_event_time": max_event_time,
                        "result_kind": "group",
                        "result_version": group_version,
                        "result_version_label": f"grp_v{group_version}",
                        "created_at": time.time(),
                    })
                    group_created += 1
                    group_versions.append(group_version)

                    for event in sorted(group_events, key=lambda e: _to_float(e.get("event_time"))):
                        row_version = _existing_row_versions(window_key, group_key) + 1
                        _rows(ROW_MODULE).append({
                            "id": _next_id(ROW_MODULE),
                            "batch_id": batch_id,
                            "batch_seq": _batch_seq(batch),
                            "source": batch.get("source", ""),
                            "source_offset": _source_offset(batch),
                            "window_key": window_key,
                            "group_key": group_key,
                            "group_values": dict(zip(keys, group_key.split("|"))),
                            "event_id": str(event.get("event_id", "")),
                            "event_time": _to_float(event.get("event_time")),
                            "amount": _event_amount(event),
                            "payload": _group_payload(event),
                            "result_kind": "row",
                            "result_version": row_version,
                            "result_version_label": f"row_v{row_version}",
                            "created_at": time.time(),
                        })
                        row_created += 1
                        row_versions.append(row_version)

        if not group_created:
            return {
                "ok": False,
                "error": f"批次「{batch_id}」窗口内没有可计算的事件",
                "batch_id": batch_id,
            }

        _touch_batch_state(
            batch, "指标已计算",
            group_result_count=group_created,
            row_result_count=row_created,
        )
        return {
            "ok": True,
            "batch_id": batch_id,
            "batch_seq": _batch_seq(batch),
            "group_results": group_created,
            "row_results": row_created,
            "group_versions": sorted(group_versions),
            "row_versions": sorted(set(row_versions)),
            "idempotent": False,
        }


# ---------------------------------------------------------------------------
# 4. 封存与完成回执（每批次唯一）
# ---------------------------------------------------------------------------

def seal_batch(batch_id: str) -> dict[str, Any]:
    """封存批次结果并生成唯一成功回执；重复封存返回同一份回执。"""
    batch = find_batch(batch_id)
    if batch is None:
        return {"ok": False, "error": f"批次「{batch_id}」不存在"}

    with BATCH_LOCK:
        existing = _receipt_for(batch_id)
        if existing is not None:
            return {
                "ok": True,
                "batch_id": batch_id,
                "batch_seq": _batch_seq(batch),
                "receipt_id": existing.get("receipt_id"),
                "receipt": existing,
                "idempotent": True,
            }

        metrics = _metric_rows(batch_id)
        if not metrics:
            return {"ok": False, "error": f"批次「{batch_id}」尚未计算窗口指标结果"}

        guard = _batch_order_guard(batch)
        if guard:
            return {"ok": False, "error": guard, "batch_id": batch_id}

        group_versions = sorted({int(r.get("result_version", 0)) for r in metrics})
        row_results = _row_rows(batch_id)
        row_versions = sorted({int(r.get("result_version", 0)) for r in row_results})
        receipt_id = hashlib.md5(
            f"receipt_{batch_id}".encode()
        ).hexdigest()[:12]
        receipt = {
            "id": _next_id(RECEIPT_MODULE),
            "receipt_id": receipt_id,
            "batch_id": batch_id,
            "batch_seq": _batch_seq(batch),
            "source": batch.get("source", ""),
            "source_offset": _source_offset(batch),
            "status": "succeeded",
            "result": "success",
            "window_count": sum(
                len(w.get("window_definitions") or []) for w in _window_rows(batch_id)
            ),
            "group_result_count": len(metrics),
            "row_result_count": len(row_results),
            "group_version": f"grp_v{group_versions[-1]}" if group_versions else "",
            "row_version": f"row_v{row_versions[-1]}" if row_versions else "",
            "group_results_digest": _content_hash(
                [(r.get("window_key"), r.get("group_key"), r.get("aggregates")) for r in metrics]
            ),
            "sealed_at": time.time(),
        }
        _rows(RECEIPT_MODULE).append(receipt)
        for row in metrics:
            row["sealed"] = True
            row["receipt_id"] = receipt_id
        for row in row_results:
            row["sealed"] = True
            row["receipt_id"] = receipt_id
        _touch_batch_state(
            batch, TERMINAL_BATCH_STATE,
            receipt_id=receipt_id,
            group_result_count=len(metrics),
            row_result_count=len(row_results),
        )
        return {
            "ok": True,
            "batch_id": batch_id,
            "batch_seq": _batch_seq(batch),
            "receipt_id": receipt_id,
            "receipt": receipt,
            "idempotent": False,
        }


# ---------------------------------------------------------------------------
# 5. 批次清单 / 台账（同一进度只生成一份整组台账）
# ---------------------------------------------------------------------------

def _source_ledger_entries(source_name: str) -> list[dict[str, Any]]:
    return [
        r for r in _rows(LEDGER_MODULE) if r.get("source") == source_name
    ]


def build_batch_ledger(source_name: str) -> dict[str, Any]:
    """为源端生成整组处理台账；重复调用命中同一份台账。"""
    with BATCH_LOCK:
        batches = _source_batches(source_name)
        if not batches:
            return {"ok": False, "error": f"源端「{source_name}」没有生产批次"}

        receipts = {
            str(r.get("batch_id")): r for r in _rows(RECEIPT_MODULE)
            if r.get("source") == source_name
        }
        signature = _content_hash([
            (b.get("batch_id"), _batch_state(b).get("status"))
            for b in batches
        ])
        for ledger in _source_ledger_entries(source_name):
            if ledger.get("signature") == signature:
                return {
                    "ok": True,
                    "source": source_name,
                    "ledger_id": ledger.get("ledger_id"),
                    "ledger": ledger,
                    "idempotent": True,
                }

        details = []
        for batch in batches:
            batch_id = str(batch.get("batch_id", ""))
            receipt = receipts.get(batch_id)
            details.append({
                "batch_id": batch_id,
                "batch_seq": _batch_seq(batch),
                "source_offset": _source_offset(batch),
                "window_count": sum(
                    len(w.get("window_definitions") or [])
                    for w in _window_rows(batch_id)
                ),
                "group_result_count": len(_metric_rows(batch_id)),
                "row_result_count": len(_row_rows(batch_id)),
                "sealed": receipt is not None,
                "receipt_id": receipt.get("receipt_id") if receipt else None,
                "receipt_status": receipt.get("status") if receipt else "pending",
                "status": _batch_state(batch).get("status", ""),
            })

        ledger_id = hashlib.md5(
            f"ledger_{source_name}_{signature}".encode()
        ).hexdigest()[:12]
        ledger = {
            "id": _next_id(LEDGER_MODULE),
            "ledger_id": ledger_id,
            "source": source_name,
            "signature": signature,
            "batch_count": len(details),
            "sealed_count": sum(1 for d in details if d["sealed"]),
            "missing_receipts": [
                d["batch_id"] for d in details if not d["sealed"]
            ],
            "status": (
                "已完成" if all(d["sealed"] for d in details) else "处理中"
            ),
            "created_at": time.time(),
            "details": details,
        }
        _rows(LEDGER_MODULE).append(ledger)
        return {
            "ok": True,
            "source": source_name,
            "ledger_id": ledger_id,
            "ledger": ledger,
            "idempotent": False,
        }


def list_batch_summaries(source_name: str) -> list[dict[str, Any]]:
    """批量结果页：按批次列出连续且唯一的窗口指标结果数据。"""
    batches = _source_batches(source_name)
    published = {
        str(r.get("batch_id")): r
        for r in _rows(PUBLISH_BATCH_MODULE) if r.get("source") == source_name
    }
    summaries = []
    for batch in batches:
        batch_id = str(batch.get("batch_id", ""))
        metrics = _metric_rows(batch_id)
        row_results = _row_rows(batch_id)
        receipt = _receipt_for(batch_id)
        publish = published.get(batch_id)
        group_versions = sorted({int(r.get("result_version", 0)) for r in metrics})
        row_versions = sorted({int(r.get("result_version", 0)) for r in row_results})
        summaries.append({
            "batch_id": batch_id,
            "batch_seq": _batch_seq(batch),
            "source_offset": _source_offset(batch),
            "status": _batch_state(batch).get("status", ""),
            "window_count": sum(
                len(w.get("window_definitions") or []) for w in _window_rows(batch_id)
            ),
            "group_results": len(metrics),
            "row_results": len(row_results),
            "group_versions": [f"grp_v{v}" for v in group_versions],
            "row_versions": [f"row_v{v}" for v in row_versions],
            "unique_windows": sorted({r.get("window_key") for r in metrics}),
            "receipt_id": receipt.get("receipt_id") if receipt else None,
            "receipt_status": receipt.get("status") if receipt else "pending",
            "publish_version": int(publish.get("version", 0)) if publish else None,
        })
    return summaries


def list_batch_results(
    source_name: str,
    batch_id: str | None = None,
    kind: str | None = None,
) -> dict[str, Any]:
    """按批次返回窗口指标结果；kind 取 group / row / both。"""
    batches = _source_batches(source_name)
    if batch_id and not any(b.get("batch_id") == batch_id for b in batches):
        return {"ok": False, "error": f"批次「{batch_id}」不属于源端「{source_name}」"}
    target_ids = (
        [batch_id] if batch_id else [str(b.get("batch_id", "")) for b in batches]
    )
    kind = kind or "both"
    groups: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    if kind in ("group", "both"):
        groups = [
            r for r in _rows(METRIC_MODULE) if r.get("batch_id") in target_ids
        ]
    if kind in ("row", "both"):
        rows = [
            r for r in _rows(ROW_MODULE) if r.get("batch_id") in target_ids
        ]
    groups.sort(key=lambda r: (_batch_seq({"batch_seq": r.get("batch_seq")}),
                               str(r.get("window_key")), str(r.get("group_key"))))
    rows.sort(key=lambda r: (_batch_seq({"batch_seq": r.get("batch_seq")}),
                             str(r.get("window_key")), str(r.get("group_key")),
                             _to_float(r.get("event_time"))))
    return {
        "ok": True,
        "source": source_name,
        "batch_id": batch_id,
        "batches": list_batch_summaries(source_name),
        "group_results": groups,
        "row_results": rows,
        "group_total": len(groups),
        "row_total": len(rows),
    }


# ---------------------------------------------------------------------------
# 6. 结果发布（按批次号读取、断档保护、整组事务）
# ---------------------------------------------------------------------------

def _publish_state(source_name: str) -> dict[str, Any]:
    for row in _rows(PUBLISH_STATE_MODULE):
        if row.get("source") == source_name:
            return row
    state = {
        "id": _next_id(PUBLISH_STATE_MODULE),
        "source": source_name,
        "published_version": 0,
        "last_publish_at": None,
        "last_batch_ids": [],
        "status": "未发布",
    }
    _rows(PUBLISH_STATE_MODULE).append(state)
    return state


def _publish_signature(source_name: str, batch_ids: list[str], merge_mode: str) -> str:
    return _content_hash(
        (source_name, tuple(sorted(batch_ids)), merge_mode)
    )


def _batches_to_publish(source_name: str, batch_ids: list[str] | None) -> list[str]:
    batches = _source_batches(source_name)
    if batch_ids:
        selected = [b for b in batches if b.get("batch_id") in set(batch_ids)]
        return [str(b.get("batch_id")) for b in selected]
    return [
        str(b.get("batch_id")) for b in batches
        if _receipt_for(str(b.get("batch_id", ""))) is not None
    ]


def _detect_gap(source_name: str, batch_ids: list[str]) -> list[int]:
    """发布前按批次号检查断档：缺少的批次序号列表。

    已经发布过的批次视为在位，因此单批次增量发布不会因为历史批次被判断档。
    """
    batches = _source_batches(source_name)
    seq_by_id = {str(b.get("batch_id")): _batch_seq(b) for b in batches}
    published_seqs = {
        seq_by_id.get(str(p.get("batch_id")), 0)
        for p in _rows(PUBLISH_BATCH_MODULE)
        if p.get("source") == source_name
    }
    present = sorted(
        {seq_by_id.get(bid, 0) for bid in batch_ids} | published_seqs - {0}
    )
    if not present:
        return []
    missing = [seq for seq in range(1, present[-1] + 1) if seq not in present]
    return missing


def _merge_group_results(
    metrics: list[dict[str, Any]],
    row_results: list[dict[str, Any]],
    merge_mode: str,
) -> list[dict[str, Any]]:
    """把多个源端批次的同窗口同分组结果整理为发布明细。"""
    ordered_metrics = sorted(
        metrics,
        key=lambda r: (
            _to_float(r.get("source_offset")) if merge_mode == "source_offset"
            else _to_float(r.get("max_event_time")),
            _batch_seq(r),
        ),
    )
    if merge_mode != "event_time":
        return [dict(item, merged=False) for item in ordered_metrics]

    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for item in ordered_metrics:
        key = (str(item.get("window_key")), str(item.get("group_key")))
        current = merged.get(key)
        if current is None:
            merged[key] = dict(item, merged=True, merged_batch_ids=[item.get("batch_id")])
            continue
        events = [
            r for r in row_results
            if r.get("window_key") == key[0] and r.get("group_key") == key[1]
        ]
        amounts = [_to_float(r.get("amount")) for r in events]
        agg = _aggregate(amounts) if amounts else item.get("aggregates", {})
        current["aggregates"] = {k: agg[k] for k in agg}
        current["row_count"] = len(events)
        current["max_event_time"] = max(
            _to_float(current.get("max_event_time")),
            _to_float(item.get("max_event_time")),
        )
        current["merged_batch_ids"] = [
            *current.get("merged_batch_ids", []), item.get("batch_id")
        ]
        current["result_version_label"] = (
            f"{current.get('result_version_label')}+{item.get('result_version_label')}"
        )
    return list(merged.values())


def publish_batch_results(
    source_name: str,
    batch_ids: list[str] | None = None,
    merge_mode: str = "source_offset",
    sink_name: str | None = None,
) -> dict[str, Any]:
    """按批次号读取同一窗口指标结果并发布；断档时保持当前发布版本不变。

    merge_mode:
      * source_offset：不同批次同一分组按源端位点先后排序后发布。
      * event_time：按业务事件时间合并同窗口同分组，只保留一份结果。
    """
    if merge_mode not in ("source_offset", "event_time"):
        return {"ok": False, "error": f"未知合并方式「{merge_mode}」"}

    with BATCH_LOCK:
        state = _publish_state(source_name)
        current_version = int(state.get("published_version", 0))
        targets = _batches_to_publish(source_name, batch_ids)
        if not targets:
            return {
                "ok": False,
                "error": "没有已封存的批次可供发布",
                "published_version": current_version,
            }

        signature = _publish_signature(source_name, targets, merge_mode)
        for attempt in _rows(PUBLISH_ATTEMPT_MODULE):
            if (
                attempt.get("source") == source_name
                and attempt.get("signature") == signature
                and attempt.get("accepted_version")
            ):
                # 并发重试只接收一组版本。
                return {
                    "ok": True,
                    "source": source_name,
                    "accepted_version": attempt.get("accepted_version"),
                    "published_version": current_version,
                    "batch_ids": targets,
                    "merge_mode": merge_mode,
                    "idempotent": True,
                    "detail": attempt.get("detail", []),
                }

        missing = _detect_gap(source_name, targets)
        if missing:
            attempt = {
                "id": _next_id(PUBLISH_ATTEMPT_MODULE),
                "source": source_name,
                "signature": signature,
                "batch_ids": targets,
                "merge_mode": merge_mode,
                "accepted_version": None,
                "status": "blocked_by_gap",
                "missing_batch_seqs": missing,
                "previous_version": current_version,
                "created_at": time.time(),
            }
            _rows(PUBLISH_ATTEMPT_MODULE).append(attempt)
            return {
                "ok": False,
                "error": f"批次断档：缺少批次序号 {missing}，保持发布版本 v{current_version} 不变",
                "missing_batch_seqs": missing,
                "published_version": current_version,
                "batch_ids": targets,
            }

        unsealed = [
            bid for bid in targets if _receipt_for(bid) is None
        ]
        if unsealed:
            return {
                "ok": False,
                "error": f"批次 {unsealed} 尚未封存，不能发布",
                "published_version": current_version,
            }

        metrics = [
            r for r in _rows(METRIC_MODULE) if r.get("batch_id") in set(targets)
        ]
        row_results = [
            r for r in _rows(ROW_MODULE) if r.get("batch_id") in set(targets)
        ]
        if not metrics:
            return {
                "ok": False,
                "error": "所选批次没有窗口指标结果数据",
                "published_version": current_version,
            }

        next_version = current_version + 1
        merged_groups = _merge_group_results(metrics, row_results, merge_mode)
        source_offsets = [
            _source_offset(b) for b in _source_batches(source_name)
            if b.get("batch_id") in set(targets)
        ]

        try:
            with _atomic_tables(
                PUBLISH_VERSION_MODULE,
                PUBLISH_DETAIL_MODULE,
                PUBLISH_BATCH_MODULE,
                PUBLISH_ATTEMPT_MODULE,
                PUBLISH_STATE_MODULE,
                "sink_output",
            ):
                _rows(PUBLISH_VERSION_MODULE).append({
                    "id": _next_id(PUBLISH_VERSION_MODULE),
                    "source": source_name,
                    "version": next_version,
                    "version_label": f"pub_v{next_version}",
                    "batch_ids": targets,
                    "batch_seqs": sorted(
                        _batch_seq(b) for b in _source_batches(source_name)
                        if b.get("batch_id") in set(targets)
                    ),
                    "merge_mode": merge_mode,
                    "source_offset": max(source_offsets, default=0.0),
                    "group_version_label": f"grp_v{next_version}",
                    "row_version_label": f"row_v{next_version}",
                    "group_result_count": len(merged_groups),
                    "row_result_count": len(row_results),
                    "status": "已发布",
                    "created_at": time.time(),
                })

                detail: list[dict[str, Any]] = []
                detail_id = _next_id(PUBLISH_DETAIL_MODULE)
                failures = 0
                for item in merged_groups:
                    write_ok = bool(item.get("group_key"))
                    if not write_ok:
                        failures += 1
                    detail.append({
                        "id": detail_id,
                        "version": next_version,
                        "source": source_name,
                        "item_kind": "group",
                        "item_id": item.get("id"),
                        "batch_id": item.get("batch_id"),
                        "batch_ids": item.get("merged_batch_ids", [item.get("batch_id")]),
                        "window_key": item.get("window_key"),
                        "group_key": item.get("group_key"),
                        "aggregates": item.get("aggregates", {}),
                        "group_version": item.get("result_version_label"),
                        "row_version": None,
                        "merged": bool(item.get("merged")),
                        "ok": write_ok,
                        "status": "success" if write_ok else "failed",
                        "error": None if write_ok else "分组键为空",
                    })
                    detail_id += 1
                for row in row_results:
                    detail.append({
                        "id": detail_id,
                        "version": next_version,
                        "source": source_name,
                        "item_kind": "row",
                        "item_id": row.get("id"),
                        "batch_id": row.get("batch_id"),
                        "batch_ids": [row.get("batch_id")],
                        "window_key": row.get("window_key"),
                        "group_key": row.get("group_key"),
                        "event_id": row.get("event_id"),
                        "amount": row.get("amount"),
                        "group_version": None,
                        "row_version": row.get("result_version_label"),
                        "merged": False,
                        "ok": True,
                        "status": "success",
                        "error": None,
                    })
                    detail_id += 1
                _rows(PUBLISH_DETAIL_MODULE).extend(detail)

                for bid in targets:
                    _rows(PUBLISH_BATCH_MODULE).append({
                        "id": _next_id(PUBLISH_BATCH_MODULE),
                        "source": source_name,
                        "batch_id": bid,
                        "version": next_version,
                        "created_at": time.time(),
                    })

                _rows(PUBLISH_ATTEMPT_MODULE).append({
                    "id": _next_id(PUBLISH_ATTEMPT_MODULE),
                    "source": source_name,
                    "signature": signature,
                    "batch_ids": targets,
                    "merge_mode": merge_mode,
                    "accepted_version": next_version,
                    "status": "published",
                    "previous_version": current_version,
                    "created_at": time.time(),
                    "detail": detail,
                })

                state.update({
                    "published_version": next_version,
                    "last_publish_at": time.time(),
                    "last_batch_ids": targets,
                    "status": "已发布" if failures == 0 else "部分失败",
                })

                # 结果同时落到已有的 sink_output 记录表，保持既有查询口径。
                for item in merged_groups:
                    _rows("sink_output").append({
                        "id": _next_id("sink_output"),                        "sink": sink_name or f"batch_{source_name}",
                        "配置名称": sink_name or f"batch_{source_name}",
                        "window_key": item.get("window_key"),
                        "group_key": item.get("group_key"),
                        "aggregates": item.get("aggregates", {}),
                        "row_count": item.get("row_count", 0),
                        "batch_id": item.get("batch_id"),
                        "batch_ids": item.get("merged_batch_ids", [item.get("batch_id")]),
                        "publish_version": next_version,
                        "group_version": item.get("result_version_label"),
                        "row_version": f"row_v{next_version}",
                        "write_status": "success",
                        "status": "已生效",
                        "written_at": time.time(),
                    })
        except Exception as error:  # 整组回滚后向上报告
            return {
                "ok": False,
                "error": f"发布事务失败已回滚整组：{error}",
                "published_version": current_version,
            }

        return {
            "ok": True,
            "source": source_name,
            "accepted_version": next_version,
            "published_version": next_version,
            "previous_version": current_version,
            "batch_ids": targets,
            "merge_mode": merge_mode,
            "group_count": len(merged_groups),
            "row_count": len(row_results),
            "failed_count": failures,
            "idempotent": False,
            "detail": detail,
        }


# ---------------------------------------------------------------------------
# 7. 失败诊断：源端调用链 + 结果落地数据流
# ---------------------------------------------------------------------------

def diagnose_batch_flow(source_name: str) -> dict[str, Any]:
    """同时核对源端处理链和结果落地数据流，定位共同导致失败的环节。"""
    batches = _source_batches(source_name)
    if not batches:
        return {"ok": False, "error": f"源端「{source_name}」没有生产批次"}

    source_chain: list[dict[str, Any]] = []
    for batch in batches:
        batch_id = str(batch.get("batch_id", ""))
        receipt = _receipt_for(batch_id)
        issues = []
        if not _window_rows(batch_id):
            issues.append("窗口定义缺失")
        elif not _metric_rows(batch_id):
            issues.append("窗口指标结果缺失")
        if receipt is None:
            issues.append("完成回执缺失")
        source_chain.append({
            "batch_id": batch_id,
            "batch_seq": _batch_seq(batch),
            "window_count": sum(
                len(w.get("window_definitions") or []) for w in _window_rows(batch_id)
            ),
            "group_results": len(_metric_rows(batch_id)),
            "row_results": len(_row_rows(batch_id)),
            "receipt": receipt.get("receipt_id") if receipt else None,
            "issues": issues,
            "ok": not issues,
        })

    sink_chain: list[dict[str, Any]] = []
    unpublished = [
        b for b in batches
        if not any(
            p.get("batch_id") == b.get("batch_id")
            for p in _rows(PUBLISH_BATCH_MODULE)
        )
    ]
    if unpublished:
        pending_ids = [str(b.get("batch_id")) for b in unpublished]
        missing = _detect_gap(source_name, pending_ids + [
            str(p.get("batch_id")) for p in _rows(PUBLISH_BATCH_MODULE)
            if p.get("source") == source_name
        ])
        unsealed = [
            bid for bid in pending_ids
            if _receipt_for(bid) is None
        ]
        sink_chain.append({
            "stage": "publish",
            "pending_batches": pending_ids,
            "missing_batch_seqs": missing,
            "unsealed_batches": unsealed,
            "blocked": bool(missing or unsealed),
            "message": (
                f"结果落地存在批次断档 {missing}" if missing
                else f"批次 {unsealed} 未封存，结果落地无法继续"
                if unsealed
                else "存在已封存但未发布的批次"
            ),
        })
    blocked_attempts = [
        a for a in _rows(PUBLISH_ATTEMPT_MODULE)
        if a.get("source") == source_name and a.get("status") == "blocked_by_gap"
    ]
    state = next(
        (r for r in _rows(PUBLISH_STATE_MODULE) if r.get("source") == source_name),
        {"published_version": 0, "status": "未发布"},
    )
    last_publish_at = _to_float(state.get("last_publish_at"), 0.0)
    # 只统计最近一次成功发布之后仍被拦截的尝试，历史拦截不代表当前异常。
    pending_blocked = [
        a for a in blocked_attempts
        if _to_float(a.get("created_at"), 0.0) > last_publish_at
    ]
    source_failed = any(not item["ok"] for item in source_chain)
    sink_failed = bool(
        (sink_chain and any(s.get("blocked") for s in sink_chain))
        or pending_blocked
    )
    if source_failed and sink_failed:
        conclusion = "源端链路与结果落地同时异常，先补齐缺失批次再发布"
    elif source_failed:
        conclusion = "源端链路存在未封存或缺结果批次"
    elif sink_failed:
        conclusion = "结果落地存在断档，当前发布版本保持不变"
    else:
        conclusion = "源端链路与结果落地均正常"
    return {
        "ok": True,
        "source": source_name,
        "source_chain": source_chain,
        "sink_chain": sink_chain,
        "blocked_attempts": len(pending_blocked),
        "published_version": int(state.get("published_version", 0)),
        "source_failed": source_failed,
        "sink_failed": sink_failed,
        "transaction_boundary": "按发布批次整组提交，任一条失败回滚整组",
        "conclusion": conclusion,
    }
