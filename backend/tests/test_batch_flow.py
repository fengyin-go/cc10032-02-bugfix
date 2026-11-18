"""批次链路业务测试：按批次窗口定义、指标版本、回执、台账与结果发布。

覆盖：
  * 逐批次读取窗口定义，旧批次不会被新批次结果覆盖
  * 分组键重复出现时保留多份结果，不再直接覆盖
  * 并发只产生一组结果，重复执行幂等
  * 完成回执与批次一一对应，旧批次未完成时新批次不能越序写入
  * 结果落地按批次号读取，断档保持发布版本不变
  * 按源位点排序 / 按事件时间合并两种取舍
  * 发布台账整组事务，异常回滚
"""
from __future__ import annotations

import threading

import pytest

from app.services import batch_flow
from app.services.batch_flow import (
    BATCH_LOCK,
    batch_workbench,
    build_batch_ledger,
    compute_batch_metrics,
    consume_batch_windows,
    diagnose_batch_flow,
    list_batch_results,
    produce_batch,
    publish_batch_results,
    seal_batch,
)
from app.store import store


@pytest.fixture()
def source_name(request) -> str:
    """每个用例使用独立源端行，避免共享 store 互相影响。"""
    name = f"批次源_{request.node.name[:24]}"
    store.rows("stream_source").append({
        "id": 900,
        "源端名称": name,
        "消息队列": "kafka",
        "Topic名称": f"topic_{name}",
        "数据格式": "JSON",
        "反序列化": "DEFAULT",
        "起始位点": "latest",
        "消费者组": "cg",
        "status": "已接入",
        "pending": False,
        "abnormal": False,
    })
    yield name


def _produce_two(source_name: str, batch_size: int = 12) -> list[str]:
    first = produce_batch(source_name, batch_size)
    second = produce_batch(source_name, batch_size)
    assert first["batch_seq"] == 1
    assert second["batch_seq"] == 2
    consume_batch_windows(source_name)
    return [first["batch_id"], second["batch_id"]]


# ---------------------------------------------------------------------------
# 按批次读取窗口定义
# ---------------------------------------------------------------------------

class TestBatchWindowDefinitions:
    def test_older_batch_windows_not_overwritten(self, source_name):
        """旧批次的窗口定义在生成新批次后仍然存在。"""
        batch_ids = _produce_two(source_name)
        definitions = [
            r for r in store.rows("window_config")
            if r.get("batch_id") in batch_ids
        ]
        assert {r["batch_id"] for r in definitions} == set(batch_ids)
        for definition in definitions:
            assert definition["window_definitions"], "每个批次都应生成窗口"
            assert definition["events_consumed"] > 0

    def test_consume_is_idempotent(self, source_name):
        """同一批次重复消费窗口返回原定义，不新增记录。"""
        batch_ids = _produce_two(source_name)
        before = len([r for r in store.rows("window_config") if r.get("batch_id")])
        result = consume_batch_windows(source_name, batch_id=batch_ids[0])
        after = len([r for r in store.rows("window_config") if r.get("batch_id")])
        assert result["batches"][0]["reused"] is True
        assert after == before

    def test_window_definitions_cover_all_batches(self, source_name):
        """窗口列表不应漏掉较早批次的一段指标。"""
        batch_ids = _produce_two(source_name)
        summary = list_batch_results(source_name)
        covered = {
            item["batch_id"] for item in summary["batches"]
            if item["window_count"] > 0
        }
        assert covered == set(batch_ids)


# ---------------------------------------------------------------------------
# 窗口指标结果版本
# ---------------------------------------------------------------------------

class TestMetricResults:
    def test_group_and_row_results_have_versions(self, source_name):
        """组级与逐条结果分别形成版本。"""
        batch_ids = _produce_two(source_name)
        result = compute_batch_metrics(batch_ids[0])
        assert result["ok"] is True
        groups = [
            r for r in store.rows(batch_flow.METRIC_MODULE)
            if r.get("batch_id") == batch_ids[0]
        ]
        rows = [
            r for r in store.rows(batch_flow.ROW_MODULE)
            if r.get("batch_id") == batch_ids[0]
        ]
        assert groups and rows
        assert all(r["result_kind"] == "group" for r in groups)
        assert all(r["result_kind"] == "row" for r in rows)
        assert all(r["result_version_label"].startswith("grp_v") for r in groups)
        assert all(r["result_version_label"].startswith("row_v") for r in rows)

    def test_duplicate_group_key_keeps_both_results(self, source_name):
        """同一窗口分组键在各批次重复出现时保留两份结果。"""
        batch_ids = _produce_two(source_name)
        compute_batch_metrics(batch_ids[0])
        seal_batch(batch_ids[0])
        compute_batch_metrics(batch_ids[1])
        first_keys = {
            (r["window_key"], r["group_key"])
            for r in store.rows(batch_flow.METRIC_MODULE)
            if r.get("batch_id") == batch_ids[0]
        }
        second = [
            r for r in store.rows(batch_flow.METRIC_MODULE)
            if r.get("batch_id") == batch_ids[1]
        ]
        assert first_keys & {(r["window_key"], r["group_key"]) for r in second}
        for key in first_keys:
            versions = [
                r["result_version"]
                for r in store.rows(batch_flow.METRIC_MODULE)
                if (r["window_key"], r["group_key"]) == key
            ]
            assert len(versions) == len(set(versions)), "同一分组应保留唯一版本序列"

    def test_compute_idempotent(self, source_name):
        """重复计算同一批次只保留一份结果。"""
        batch_ids = _produce_two(source_name)
        first = compute_batch_metrics(batch_ids[0])
        second = compute_batch_metrics(batch_ids[0])
        assert second["idempotent"] is True
        assert second["group_results"] == first["group_results"]

    def test_concurrent_compute_single_group(self, source_name):
        """序号2 的并发操作只产生一组结果。"""
        batch_ids = _produce_two(source_name)
        outcomes: list[dict] = []

        def work():
            outcomes.append(compute_batch_metrics(batch_ids[0]))

        threads = [threading.Thread(target=work) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        created = [o for o in outcomes if o.get("ok") and not o.get("idempotent")]
        assert len(created) == 1
        assert all(o.get("ok") for o in outcomes)
        groups = [
            r for r in store.rows(batch_flow.METRIC_MODULE)
            if r.get("batch_id") == batch_ids[0]
        ]
        assert len(groups) == created[0]["group_results"]


# ---------------------------------------------------------------------------
# 完成回执与批次顺序
# ---------------------------------------------------------------------------

class TestReceiptsAndOrdering:
    def test_receipt_matches_batch(self, source_name):
        """每个批次的分组结果只与该批次的完成回执对应。"""
        batch_ids = _produce_two(source_name)
        compute_batch_metrics(batch_ids[0])
        receipt = seal_batch(batch_ids[0])
        assert receipt["ok"] is True
        stored = [
            r for r in store.rows(batch_flow.RECEIPT_MODULE)
            if r.get("batch_id") == batch_ids[0]
        ]
        assert len(stored) == 1
        assert stored[0]["receipt_id"] == receipt["receipt_id"]
        assert stored[0]["status"] == "succeeded"
        groups = [
            r for r in store.rows(batch_flow.METRIC_MODULE)
            if r.get("batch_id") == batch_ids[0]
        ]
        assert all(g["receipt_id"] == receipt["receipt_id"] for g in groups)

    def test_duplicate_seal_keeps_single_receipt(self, source_name):
        """重复执行同一批次只能保留一份成功回执。"""
        batch_ids = _produce_two(source_name)
        compute_batch_metrics(batch_ids[0])
        first = seal_batch(batch_ids[0])
        second = seal_batch(batch_ids[0])
        assert second["idempotent"] is True
        assert second["receipt_id"] == first["receipt_id"]
        assert len([
            r for r in store.rows(batch_flow.RECEIPT_MODULE)
            if r.get("batch_id") == batch_ids[0]
        ]) == 1

    def test_new_batch_cannot_skip_unfinished_batch(self, source_name):
        """较早批次未完成时新批次不得越过它写入。"""
        batch_ids = _produce_two(source_name)
        compute_batch_metrics(batch_ids[0])
        blocked = compute_batch_metrics(batch_ids[1])
        assert blocked["ok"] is False
        assert "不能越过" in blocked["error"]

        seal_batch(batch_ids[0])
        allowed = compute_batch_metrics(batch_ids[1])
        assert allowed["ok"] is True

    def test_concurrent_seal_single_receipt(self, source_name):
        """并发封存同一批次只生成一份回执。"""
        batch_ids = _produce_two(source_name)
        compute_batch_metrics(batch_ids[0])
        receipts: list[dict] = []

        def work():
            receipts.append(seal_batch(batch_ids[0]))

        threads = [threading.Thread(target=work) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert len({r["receipt_id"] for r in receipts}) == 1
        assert len([
            r for r in store.rows(batch_flow.RECEIPT_MODULE)
            if r.get("batch_id") == batch_ids[0]
        ]) == 1


# ---------------------------------------------------------------------------
# 结果落地与发布
# ---------------------------------------------------------------------------

def _prepare_sealed(source_name: str) -> list[str]:
    batch_ids = _produce_two(source_name)
    compute_batch_metrics(batch_ids[0])
    seal_batch(batch_ids[0])
    compute_batch_metrics(batch_ids[1])
    seal_batch(batch_ids[1])
    return batch_ids


class TestPublish:
    def test_publish_by_batch_order(self, source_name):
        """两个批次同时完成时按源端位点决定发布顺序。"""
        batch_ids = _prepare_sealed(source_name)
        result = publish_batch_results(source_name)
        assert result["ok"] is True
        assert result["published_version"] == 1
        details = [
            d for d in store.rows(batch_flow.PUBLISH_DETAIL_MODULE)
            if d.get("version") == 1 and d.get("item_kind") == "group"
        ]
        offsets = [
            next(
                b["source_offset"] for b in batch_flow._source_batches(source_name)
                if b["batch_id"] == d["batch_id"]
            )
            for d in details
        ]
        assert offsets == sorted(offsets), "发布明细应按源端位点先后排列"

    def test_event_time_merge_single_group(self, source_name):
        """按事件时间合并同一分组，详情页保留连续且唯一的结果版本。"""
        batch_ids = _prepare_sealed(source_name)
        result = publish_batch_results(source_name, merge_mode="event_time")
        assert result["ok"] is True
        groups = [d for d in result["detail"] if d["item_kind"] == "group"]
        keys = [(d["window_key"], d["group_key"]) for d in groups]
        assert len(keys) == len(set(keys)), "合并后同一分组只保留一份结果"
        assert all(len(d["batch_ids"]) == 2 for d in groups)

    def test_gap_keeps_publish_version(self, source_name):
        """发现断档时保持当前发布版本不变。"""
        batch_ids = _prepare_sealed(source_name)
        third = produce_batch(source_name, 12)["batch_id"]
        consume_batch_windows(source_name)
        compute_batch_metrics(third)
        seal_batch(third)

        first = publish_batch_results(source_name, batch_ids=[batch_ids[0]])
        assert first["ok"] is True
        version = first["published_version"]

        blocked = publish_batch_results(source_name, batch_ids=[third])
        assert blocked["ok"] is False
        assert blocked["missing_batch_seqs"] == [2]
        assert blocked["published_version"] == version
        state = next(
            r for r in store.rows(batch_flow.PUBLISH_STATE_MODULE)
            if r.get("source") == source_name
        )
        assert state["published_version"] == version

    def test_concurrent_publish_single_version(self, source_name):
        """并发重试只接收一组版本。"""
        _prepare_sealed(source_name)
        results: list[dict] = []

        def work():
            results.append(publish_batch_results(source_name))

        threads = [threading.Thread(target=work) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        accepted = {r["accepted_version"] for r in results if r.get("ok")}
        assert len(accepted) == 1
        versions = [
            v for v in store.rows(batch_flow.PUBLISH_VERSION_MODULE)
            if v.get("source") == source_name
        ]
        assert len(versions) == 1

    def test_publish_rejects_unsealed_batch(self, source_name):
        """未封存批次不参与发布。"""
        batch_ids = _produce_two(source_name)
        compute_batch_metrics(batch_ids[0])
        result = publish_batch_results(source_name, batch_ids=[batch_ids[0]])
        assert result["ok"] is False
        assert result["published_version"] == 0

    def test_viewer_keeps_gap_visible(self, source_name):
        """结果落地页面能确认缺失批次不会被新结果掩盖。"""
        batch_ids = _prepare_sealed(source_name)
        publish_batch_results(source_name, batch_ids=[batch_ids[0]])
        workbench = batch_workbench(source_name)
        published = [b for b in workbench["batches"] if b["publish_version"]]
        assert [b["batch_id"] for b in published] == [batch_ids[0]]
        later = next(b for b in workbench["batches"] if b["batch_id"] == batch_ids[1])
        assert later["publish_version"] is None


class TestTransactions:
    def test_publish_rolls_back_whole_group(self, source_name, monkeypatch):
        """序号2 事务异常后回滚整组。"""
        _prepare_sealed(source_name)
        before_details = len(store.rows(batch_flow.PUBLISH_DETAIL_MODULE))
        before_versions = len(store.rows(batch_flow.PUBLISH_VERSION_MODULE))

        original = batch_flow._rows

        def exploding_rows(module: str):
            if module == "sink_output":
                raise RuntimeError("sink write failed")
            return original(module)

        monkeypatch.setattr(batch_flow, "_rows", exploding_rows)
        result = publish_batch_results(source_name)
        monkeypatch.undo()

        assert result["ok"] is False
        assert "回滚" in result["error"]
        assert result["published_version"] == 0
        assert len(store.rows(batch_flow.PUBLISH_DETAIL_MODULE)) == before_details
        assert len(store.rows(batch_flow.PUBLISH_VERSION_MODULE)) == before_versions
        state = next(
            r for r in store.rows(batch_flow.PUBLISH_STATE_MODULE)
            if r.get("source") == source_name
        )
        assert state["published_version"] == 0

    def test_ledger_single_group(self, source_name):
        """序号2 由另一个入口生成整组处理台账，重复调用只记一次。"""
        _prepare_sealed(source_name)
        first = build_batch_ledger(source_name)
        second = build_batch_ledger(source_name)
        assert first["ok"] is True
        assert second["idempotent"] is True
        assert first["ledger_id"] == second["ledger_id"]
        assert len([
            r for r in store.rows(batch_flow.LEDGER_MODULE)
            if r.get("source") == source_name
        ]) == 1
        assert first["ledger"]["sealed_count"] == 2


class TestDiagnosis:
    def test_diagnose_links_source_and_sink(self, source_name):
        """序号2 的流式源端与结果落地两处异常共同导致失败。"""
        batch_ids = _prepare_sealed(source_name)
        blocked = publish_batch_results(source_name, batch_ids=[batch_ids[1]])
        assert blocked["ok"] is False
        report = diagnose_batch_flow(source_name)
        assert report["ok"] is True
        assert report["source_failed"] is False
        assert report["sink_failed"] is True
        assert report["blocked_attempts"] == 1
        assert report["published_version"] == 0

    def test_diagnose_missing_source_chain(self, source_name):
        """未计算指标的批次会同时出现在源端链路异常里。"""
        _produce_two(source_name)
        report = diagnose_batch_flow(source_name)
        assert report["source_failed"] is True
        assert report["sink_failed"] is True
        assert "同时异常" in report["conclusion"]


class TestSinkSubmission:
    def test_submit_selected_results_keeps_whole_group(self, source_name):
        """序号2 批量选择多条窗口指标结果数据，一次提交保留整组结果。"""
        from app.services.sink_output import SinkOutputService

        batch_ids = _prepare_sealed(source_name)
        groups = [
            r for r in store.rows(batch_flow.METRIC_MODULE)
            if r.get("batch_id") in batch_ids
        ]
        picked = [groups[0]["id"], groups[-1]["id"]]
        result = SinkOutputService().submit_window_results(
            "结果落地样例2", result_ids=picked,
        )
        assert result["ok"] is True
        assert set(result["batch_ids"]) == set(batch_ids)
        assert result["group_count"] == len(groups)
        detail = result["detail"]
        assert len(detail) == len(groups) + len([
            r for r in store.rows(batch_flow.ROW_MODULE)
            if r.get("batch_id") in batch_ids
        ])

    def test_submit_result_ids_only_uses_their_batches(self, source_name):
        """只选中一个批次的结果时，只发布该批次。"""
        from app.services.sink_output import SinkOutputService

        batch_ids = _prepare_sealed(source_name)
        groups = [
            r for r in store.rows(batch_flow.METRIC_MODULE)
            if r.get("batch_id") == batch_ids[0]
        ]
        result = SinkOutputService().submit_window_results(
            "结果落地样例2", result_ids=[groups[0]["id"]],
        )
        assert result["ok"] is True
        assert result["batch_ids"] == [batch_ids[0]]

    def test_concurrent_submit_retry_single_version(self, source_name):
        """序号2 并发重试只接收一组版本。"""
        from app.services.sink_output import SinkOutputService

        batch_ids = _prepare_sealed(source_name)
        outputs: list[dict] = []

        def work():
            outputs.append(SinkOutputService().submit_window_results(
                "结果落地样例2", source_name=source_name,
            ))

        threads = [threading.Thread(target=work) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        accepted = {o["accepted_version"] for o in outputs if o.get("ok")}
        assert len(accepted) == 1
        assert len([
            v for v in store.rows(batch_flow.PUBLISH_VERSION_MODULE)
            if v.get("source") == source_name
        ]) == 1

    def test_sink_records_written(self, source_name):
        """发布结果同时写入 sink_output，保持既有查询口径。"""
        from app.services.sink_output import SinkOutputService

        _prepare_sealed(source_name)
        result = SinkOutputService().submit_window_results(
            "结果落地样例2", source_name=source_name,
        )
        assert result["ok"] is True
        written = [
            r for r in store.rows("sink_output")
            if r.get("sink") == "结果落地样例2"
            and r.get("publish_version") == result["accepted_version"]
            and r.get("batch_id") in result["batch_ids"]
        ]
        assert len(written) == result["group_count"]
