"""窗口配置领域服务：定义流式计算的时间窗口策略。

数据流角色: stream_source → window_config（消费者+生产者）→ stream_job（消费者）
增强能力: 窗口参数校验、重叠检测、策略绑定、水位推算、消费上游事件批次
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class WindowConfigService(BaseModuleService):
    MODULE = "window_config"
    ENTITY = "窗口策略"
    LIST_FIELDS = ["策略名称", "窗口类型", "窗口大小", "滑动步长", "水位延迟", "触发策略", "延迟处理"]
    REQUIRED_FIELDS = ["策略名称", "窗口类型", "窗口大小"]
    STATUS_ORDER = ["草稿", "已生效", "已停用"]
    ACTION_RULES = {"启用策略": "草稿", "停用策略": "已生效"}
    NEGATIVE_ACTIONS = ["停用策略"]

    VALID_WINDOW_TYPES = {"tumbling", "sliding", "session", "global"}

    # ---- 跨模块数据消费与生产 ----

    def validate_window_params(
        self, window_type: str, window_size: str, slide_step: str | None = None,
    ) -> dict[str, Any]:
        """校验窗口参数合法性：类型、大小范围、步长≤窗口大小。"""
        issues = []
        wtype = window_type.lower()
        if wtype not in self.VALID_WINDOW_TYPES:
            issues.append(f"窗口类型「{window_type}」无效，支持: {self.VALID_WINDOW_TYPES}")
        try:
            wsize_sec = float(window_size)
            if wsize_sec <= 0:
                issues.append("窗口大小必须 > 0")
            if wsize_sec > 86400:
                issues.append("窗口大小不能超过 24 小时")
        except (ValueError, TypeError):
            issues.append("窗口大小必须为数字")
        if slide_step is not None and wtype in ("sliding",):
            try:
                step_sec = float(slide_step)
                if step_sec <= 0:
                    issues.append("滑动步长必须 > 0")
                elif step_sec > float(window_size):
                    issues.append("滑动步长不能超过窗口大小")
            except (ValueError, TypeError):
                issues.append("滑动步长必须为数字")
        return {"ok": len(issues) == 0, "type": window_type, "size": window_size, "issues": issues}

    def detect_overlapping_policies(
        self, new_policy: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """检测新窗口策略是否与已有策略重叠或冲突。"""
        new_type = new_policy.get("窗口类型", "")
        new_size = new_policy.get("窗口大小", "")
        overlaps = []
        for r in store.rows(self.MODULE):
            if r.get("id", 0) == new_policy.get("id"):
                continue
            if r.get("status") == "已停用":
                continue
            if r.get("窗口类型") == new_type and r.get("窗口大小") == new_size:
                overlaps.append({
                    "conflicting_policy": r.get("策略名称", ""),
                    "conflict_id": r.get("id"),
                    "reason": f"相同类型({new_type})和大{new_size}",
                })
        return overlaps

    def resolve_policy(self, policy_name: str | None) -> dict[str, Any] | None:
        """定位窗口策略：优先按名称，其次取第一条已生效策略。"""
        policies = [r for r in store.rows(self.MODULE) if not r.get("batch_id")]
        if policy_name:
            named = next(
                (r for r in policies if r.get("策略名称") == policy_name), None)
            if named is not None:
                return named
        return next(
            (r for r in policies if r.get("status") == "已生效"),
            policies[0] if policies else None,
        )

    def consume_source_batch(
        self, source_name: str, policy_name: str | None = None,
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        """消费上游 stream_source 产生的数据批次，按窗口策略生成窗口定义。

        逐批次读取源端的生产批次（不再只取最新批次），按策略参数计算窗口边界
        和触发时间，把窗口定义写入 store 供 stream_job 消费。同一批次重复消费
        会返回已有定义，避免覆盖旧批次结果。
        """
        from app.services.batch_flow import consume_batch_windows

        # 1. 确认策略已生效
        policy = self.resolve_policy(policy_name)
        if policy is None:
            return {"ok": False, "error": f"窗口策略「{policy_name}」不存在"}
        if policy.get("status") != "已生效":
            return {"ok": False, "error": f"策略「{policy['策略名称']}」未生效"}
        policy_name = str(policy.get("策略名称"))

        # 2-4. 逐批次计算窗口并写入窗口定义
        result = consume_batch_windows(source_name, batch_id=batch_id,
                                       policy_name=policy_name)
        if not result.get("ok"):
            return result
        batches = result.get("batches", [])
        consumed = 0
        for definition in store.rows(self.MODULE):
            if definition.get("batch_id") in {b["batch_id"] for b in batches}:
                consumed += int(definition.get("events_consumed") or 0)
        return {
            "ok": True,
            "policy": result.get("policy", policy_name),
            "source": source_name,
            "events_consumed": consumed,
            "windows_generated": result.get("windows_generated", 0),
            "batch_id": batches[-1]["batch_id"] if batches else "",
            "batches": batches,
            "reused": result.get("reused", False),
        }

    def compute_window_metrics(
        self, batch_id: str, index_name: str | None = None,
    ) -> dict[str, Any]:
        """按批次计算窗口指标，保留组级和逐条窗口指标结果数据。"""
        from app.services.batch_flow import compute_batch_metrics

        return compute_batch_metrics(batch_id, index_name=index_name)

    def seal_batch_results(self, batch_id: str) -> dict[str, Any]:
        """封存批次结果并写入该批次唯一的成功回执。"""
        from app.services.batch_flow import seal_batch

        return seal_batch(batch_id)

    def calculate_watermark(
        self, policy_name: str, events_processed: int = 0, event_time_max: float = 0,
    ) -> dict[str, Any]:
        """根据策略计算当前水位线。"""
        policy = None
        for r in store.rows(self.MODULE):
            if r.get("策略名称") == policy_name:
                policy = r
                break
        try:
            allowed_late = float((policy or {}).get("水位延迟", "10"))
        except (ValueError, TypeError):
            allowed_late = 10.0
        watermarks = []
        # 收集窗口计算结果中的水位
        for r in store.rows(self.MODULE):
            if r.get("策略名称") == policy_name:
                windows = r.get("window_definitions", [])
                if windows:
                    last_end = max(w.get("window_end", 0) for w in windows)
                    watermarks.append({
                        "policy": policy_name,
                        "watermark": last_end - allowed_late,
                        "window_end": last_end,
                        "allowed_lateness": allowed_late,
                    })
        return {
            "ok": True,
            "policy": policy_name,
            "watermarks": watermarks,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        batch_defs = [r for r in rows if r.get("batch_id")]
        base["active_policies"] = sum(
            1 for r in rows if r.get("status") == "已生效" and not r.get("batch_id"))
        base["total_window_defs"] = sum(
            len(r.get("window_definitions") or []) for r in batch_defs
        )
        base["batch_window_defs"] = len(batch_defs)
        base["covered_batches"] = sorted({
            r.get("batch_id") for r in batch_defs if r.get("batch_id")
        })
        return base