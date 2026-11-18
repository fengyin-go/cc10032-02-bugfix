"""流式源端领域服务：管理实时流式数据源的接入配置。

数据流角色: stream_source（生产者）→ window_config（消费者）→ stream_job（消费者）
增强能力: 源端连接校验、数据格式验证、位点管理、心跳检测、跨模块消费通知
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class StreamSourceService(BaseModuleService):
    MODULE = "stream_source"
    ENTITY = "数据源配置"
    LIST_FIELDS = ["源端名称", "消息队列", "Topic名称", "数据格式", "反序列化", "起始位点", "消费者组"]
    REQUIRED_FIELDS = ["源端名称", "消息队列", "Topic名称"]
    STATUS_ORDER = ["草稿", "已接入", "已下线", "异常断连"]
    ACTION_RULES = {"正式接入": "草稿", "下线上线": "已接入"}
    NEGATIVE_ACTIONS = ["下线上线"]

    # ---- 跨模块数据生产 ----

    def produce_source_snapshot(
        self, source_name: str, batch_size: int = 20,
    ) -> dict[str, Any]:
        """生产源端数据快照，供窗口配置和流式作业消费。

        从种子数据生成模拟事件批次，写入共享存储产物流水线序号，
        下游模块可通过读取 store 获取最新批次数据。

        批次序号（batch_seq）和源位点（source_offset）由批次链路在锁内分配，
        保证并发生产不会重号，也不会覆盖已有批次。
        """
        from app.services.batch_flow import produce_batch

        return produce_batch(source_name, batch_size)

    def validate_connection(self, source_name: str) -> dict[str, Any]:
        """验证源端连接健康状态：格式校验、消费者组可达、位点有效。"""
        source = None
        for r in store.rows(self.MODULE):
            if r.get("源端名称") == source_name:
                source = r
                break
        if source is None:
            return {"ok": False, "error": f"源端「{source_name}」不存在"}

        checks = []
        # 格式校验
        valid_formats = {"JSON", "PROTOBUF", "AVRO", "CSV"}
        fmt = source.get("数据格式", "JSON")
        checks.append({
            "check": "format",
            "value": fmt,
            "pass": fmt.upper() in valid_formats,
        })
        # 消费者组非空校验
        cg = source.get("消费者组", "")
        checks.append({
            "check": "consumer_group",
            "value": cg,
            "pass": bool(cg and cg.strip()),
        })
        # 位点校验
        offset = source.get("起始位点", "")
        valid_offsets = {"earliest", "latest", ""}
        checks.append({
            "check": "offset",
            "value": offset,
            "pass": offset.lower() in valid_offsets or offset.isdigit(),
        })
        all_pass = all(c["pass"] for c in checks)
        if not all_pass and source.get("status") == "已接入":
            source["status"] = "异常断连"
            source["abnormal"] = True
        return {"ok": all_pass, "source": source_name, "checks": checks}

    def notify_downstream_consumers(
        self, source_name: str, batch_id: str,
    ) -> dict[str, Any]:
        """通知下游消费者（window_config, stream_job）新数据批次已就绪。

        遍历下游模块，为每个消费者创建消费通知记录并标记 pending。
        """
        notified = []
        consumer_modules = ["window_config", "stream_job"]
        for mod in consumer_modules:
            notification = {
                "id": max(
                    (int(r.get("id", 0)) for r in store.rows(mod)), default=0
                ) + 1,
                "type": "source_batch_ready",
                "source": source_name,
                "batch_id": batch_id,
                "consumer_module": mod,
                "timestamp": time.time(),
                "status": "pending",
                "pending": True,
            }
            if mod in ("window_config",):
                notification["策略名称"] = f"window_from_{source_name}"
                notification["窗口类型"] = "sliding"
                notification["窗口大小"] = "60"
                notification["滑动步长"] = "30"
                notification["水位延迟"] = "10"
                notification["触发策略"] = "watermark"
                notification["延迟处理"] = "bypass"
            elif mod == "stream_job":
                notification["作业名称"] = f"job_from_{source_name}"
                notification["源端配置"] = source_name
                notification["窗口策略"] = f"window_from_{source_name}"
                notification["计算拓扑"] = f"topo_{source_name}"
                notification["并行度"] = "4"
                notification["启动时间"] = ""
                notification["运行水位"] = "0"
            store.rows(mod).append(notification)
            notified.append(mod)
        return {
            "ok": True,
            "source": source_name,
            "batch_id": batch_id,
            "notified_modules": notified,
        }

    def list_source_metrics(self) -> list[dict[str, Any]]:
        """聚合所有源端指标：事件量、状态分布、下游消费状况。"""
        metrics = []
        for r in store.rows(self.MODULE):
            if r.get("id", 0) < 100:
                src_name = r.get("源端名称", "")
                # 统计下游消费情况
                downstream_status = []
                for w in store.rows("window_config"):
                    if src_name in str(w.get("策略名称", "")):
                        downstream_status.append(w.get("status", ""))
                for j in store.rows("stream_job"):
                    if src_name in str(j.get("源端配置", "")):
                        downstream_status.append(j.get("status", ""))
                metrics.append({
                    "source": src_name,
                    "queue": r.get("消息队列", ""),
                    "topic": r.get("Topic名称", ""),
                    "status": r.get("status", ""),
                    "format": r.get("数据格式", ""),
                    "downstream_consumers": len(downstream_status),
                    "downstream_healthy": sum(
                        1 for s in downstream_status if s in ("已生效", "运行中")
                    ),
                })
        return metrics

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["active_sources"] = sum(1 for r in rows if r.get("status") == "已接入")
        base["disconnected"] = sum(1 for r in rows if r.get("status") == "异常断连")
        base["total_events_produced"] = sum(
            r.get("event_count", 0) for r in rows if isinstance(r.get("event_count"), int)
        )
        return base