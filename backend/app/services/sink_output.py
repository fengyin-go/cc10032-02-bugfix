"""结果落地领域服务：管理流式计算结果的输出落地配置。

数据流角色: state_store / calc_operator → sink_output（消费者+生产者）
增强能力: 批量写入调度、分区策略计算、写入模式切换、异常重试、输出完整性校验
"""
from __future__ import annotations

import copy
import hashlib
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SinkOutputService(BaseModuleService):
    MODULE = "sink_output"
    ENTITY = "输出配置"
    LIST_FIELDS = ["配置名称", "输出类型", "目标地址", "写入模式", "分区策略", "格式转换", "批量大小"]
    REQUIRED_FIELDS = ["配置名称", "输出类型", "目标地址"]
    STATUS_ORDER = ["草稿", "已生效", "已停用", "写入异常"]
    ACTION_RULES = {"启用输出": "草稿", "停用输出": "已生效"}
    NEGATIVE_ACTIONS = ["停用输出"]

    # ---- 跨模块数据消费与生产 ----

    def batch_write_results(
        self, sink_name: str, results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """将计算结果批量写入目标存储，支持分区和格式转换。

        整组提交：中间出现异常时回滚本次写入的全部记录，不留下半组结果。
        """
        sink = None
        for r in store.rows(self.MODULE):
            if r.get("配置名称") == sink_name:
                sink = r
                break
        if sink is None:
            return {"ok": False, "error": f"输出配置「{sink_name}」不存在"}
        if sink.get("status") == "已停用":
            return {"ok": False, "error": f"输出配置「{sink_name}」已停用"}

        try:
            int(sink.get("批量大小", "100"))
        except (ValueError, TypeError):
            pass

        partition_strategy = sink.get("分区策略", "round_robin")
        write_mode = sink.get("写入模式", "append")
        output_type = sink.get("输出类型", "database")
        format_type = sink.get("格式转换", "json")

        snapshot = copy.deepcopy(store.rows(self.MODULE))
        written = 0
        failed = 0
        try:
            for i, item in enumerate(results):
                partition_key = self._calc_partition(
                    item, partition_strategy, i,
                )
                converted = self._convert_format(item, format_type)
                record = {
                    "id": max(
                        (int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0
                    ) + 1,
                    "sink": sink_name,
                    "output_type": output_type,
                    "partition": partition_key,
                    "format": format_type,
                    "mode": write_mode,
                    "data": converted,
                    "written_at": time.time(),
                    "status": "已生效",
                }
                # 模拟写入成功/失败
                if self._simulate_write(partition_key):
                    record["write_status"] = "success"
                    store.rows(self.MODULE).append(record)
                    written += 1
                else:
                    record["write_status"] = "failed"
                    record["异常标记"] = "写入异常"
                    store.rows(self.MODULE).append(record)
                    failed += 1
        except Exception as error:
            store._tables[self.MODULE] = snapshot
            return {
                "ok": False,
                "error": f"批量写入失败已回滚整组：{error}",
                "sink": sink_name,
                "total": len(results),
            }

        if failed > 0:
            sink["status"] = "写入异常"
            sink["pending"] = True

        return {
            "ok": True,
            "sink": sink_name,
            "total": len(results),
            "written": written,
            "failed": failed,
            "partition_strategy": partition_strategy,
        }

    def submit_window_results(
        self,
        sink_name: str,
        result_ids: list[int] | None = None,
        source_name: str | None = None,
        batch_ids: list[str] | None = None,
        merge_mode: str = "source_offset",
    ) -> dict[str, Any]:
        """结果落地页批量提交：按批次号读取同一窗口指标结果并整组发布。

        * 一次提交保留整组明细，逐条返回成功/失败。
        * 批次断档时保持当前发布版本不变。
        * 并发重试命中同一批次组合时只接收一组版本。
        """
        from app.services import batch_flow

        if result_ids:
            selected_batches: list[str] = []
            for rid in result_ids:
                for module in (batch_flow.METRIC_MODULE, batch_flow.ROW_MODULE):
                    row = next(
                        (r for r in store.rows(module) if int(r.get("id", 0)) == int(rid)),
                        None,
                    )
                    if row is not None:
                        batch_id = str(row.get("batch_id", ""))
                        if batch_id and batch_id not in selected_batches:
                            selected_batches.append(batch_id)
                        source_name = source_name or row.get("source")
                        break
            if not selected_batches:
                return {"ok": False, "error": "所选窗口指标结果数据不存在"}
            batch_ids = selected_batches

        if not source_name:
            return {"ok": False, "error": "缺少源端名称或窗口指标结果数据"}

        result = batch_flow.publish_batch_results(
            source_name,
            batch_ids=batch_ids,
            merge_mode=merge_mode,
            sink_name=sink_name,
        )
        if result.get("ok"):
            sink = next(
                (r for r in store.rows(self.MODULE) if r.get("配置名称") == sink_name),
                None,
            )
            if sink is not None:
                sink["pending"] = False
                if sink.get("status") in ("草稿", "写入异常"):
                    sink["status"] = "已生效"
        return result

    @staticmethod
    def _calc_partition(
        item: dict[str, Any], strategy: str, index: int,
    ) -> str:
        """计算数据分区键。"""
        if strategy == "hash_key":
            raw = str(item.get("key", index))
            return f"pt_{hashlib.md5(raw.encode()).hexdigest()[:4]}"
        elif strategy == "time_range":
            ts = item.get("timestamp", time.time())
            return time.strftime("dt=%Y%m%d%H", time.localtime(ts))
        elif strategy == "source_based":
            return f"src_{item.get('source', 'default')}"
        else:  # round_robin
            return f"pt_{index % 8:02d}"

    @staticmethod
    def _convert_format(
        item: dict[str, Any], fmt: str,
    ) -> dict[str, Any]:
        """格式转换：JSON/CSV/Parquet 模拟。"""
        if fmt == "csv":
            return {"format": "csv", "raw": str(item)}
        elif fmt == "parquet":
            return {"format": "parquet", "columns": list(item.keys()), "rows": 1}
        return {"format": "json", "payload": item}

    @staticmethod
    def _simulate_write(partition: str) -> bool:
        """模拟写入成功（绝大部分成功，偶发失败模拟异常）。"""
        return hash(partition) % 20 != 0

    def verify_output_integrity(self, sink_name: str) -> dict[str, Any]:
        """验证指定 sink 下的输出记录完整性和写入成功率。"""
        records = [
            r for r in store.rows(self.MODULE)
            if r.get("sink") == sink_name
        ]
        total = len(records)
        success = sum(1 for r in records if r.get("write_status") == "success")
        failed = total - success
        partitions = set(r.get("partition", "") for r in records)
        return {
            "ok": True,
            "sink": sink_name,
            "total": total,
            "success": success,
            "failed": failed,
            "success_rate": success / max(total, 1),
            "partitions": len(partitions),
            "needs_retry": failed > 0,
        }

    def retry_failed_writes(self, sink_name: str) -> dict[str, Any]:
        """重试失败的写入记录。"""
        records = [
            r for r in store.rows(self.MODULE)
            if r.get("sink") == sink_name and r.get("write_status") == "failed"
        ]
        retried = 0
        for record in records:
            if self._simulate_write(record.get("partition", "")):
                record["write_status"] = "success"
                retried += 1
        return {
            "ok": True,
            "sink": sink_name,
            "total_failed": len(records),
            "retried_success": retried,
        }

    def list_sink_destinations(self) -> list[dict[str, Any]]:
        """列出所有活跃的输出目标和状态。"""
        configs = [
            r for r in store.rows(self.MODULE)
            if r.get("id", 0) < 100
        ]
        result = []
        for c in configs:
            name = c.get("配置名称", "")
            records_count = sum(
                1 for r in store.rows(self.MODULE)
                if r.get("sink") == name
            )
            result.append({
                "sink": name,
                "type": c.get("输出类型", ""),
                "target": c.get("目标地址", ""),
                "status": c.get("status", ""),
                "records": records_count,
            })
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        from app.services import batch_flow

        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["total_records"] = sum(
            1 for r in rows if r.get("write_status")
        )
        base["failed_records"] = sum(
            1 for r in rows if r.get("write_status") == "failed"
        )
        base["publish_versions"] = len(store.rows(batch_flow.PUBLISH_VERSION_MODULE))
        base["publish_details"] = len(store.rows(batch_flow.PUBLISH_DETAIL_MODULE))
        base["blocked_attempts"] = sum(
            1 for r in store.rows(batch_flow.PUBLISH_ATTEMPT_MODULE)
            if r.get("status") == "blocked_by_gap"
        )
        return base