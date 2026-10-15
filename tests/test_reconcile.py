import pytest
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from csgo_monitor.pipelines import build_pipeline_status
from csgo_monitor.reconcile import reconcile_control_state
from csgo_monitor.storage import Storage


def settings() -> SimpleNamespace:
    return SimpleNamespace(
        request_delay=0,
        siteb_api_key="test-key",
        siteb_open_base="https://open.siteb.com",
        siteb_open_delay=0.55,
        timeout=1,
    )


class ReconcileTests(unittest.TestCase):
    def test_sitea_series_evidence_is_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            storage = Storage(
                root / "market.sqlite3",
                root / "raw",
                root / "control.sqlite3",
            )
            storage.upsert_items(
                "sitea",
                [{"item_id": "1", "name": "Test Item"}],
            )
            storage.upsert_series([
                {
                    "source": "sitea",
                    "dataset": "item_snapshot",
                    "item_id": "1",
                    "metric": "sell_price",
                    "platform": "all",
                    "ts": "2026-08-30T00:00:00+00:00",
                    "close": 1,
                },
                {
                    "source": "sitea",
                    "dataset": "good_statistic",
                    "item_id": "1",
                    "metric": "turnover",
                    "platform": "all",
                    "ts": "2026-08-30T00:00:00+00:00",
                    "close": 1,
                },
            ])
            storage.record_failure(
                "sitea_api_detail_p0",
                "1good",
                "temporary",
            )
            storage.record_failure(
                "sitea_api_detail_p0",
                "1statistic",
                "temporary",
            )

            first = reconcile_control_state(
                storage,
                settings(),
                apply=True,
                sections=("sitea",),
            )
            second = reconcile_control_state(
                storage,
                settings(),
                apply=True,
                sections=("sitea",),
            )

            self.assertEqual(
                first["pipelines"]["sitea_detail"]["completed"],
                1,
            )
            self.assertEqual(
                second["sitea"]["sitea_api_detail_p0"]["promoted"],
                0,
            )
            self.assertEqual(
                storage.get_state("sitea_api_detail_p0"),
                "1",
            )
            self.assertEqual(
                storage.failure_summary("sitea_api_detail_p0"),
                (0, 0),
            )
            for slice_id in ("1:good", "1:statistic", "1"):
                row = storage.task_slice_status(
                    "sitea_api_detail_p0",
                    slice_id,
                )
                self.assertEqual(row["status"], "done")

    def test_sitea_capture_evidence_reads_query_identity(self):
        from csgo_monitor.reconcile import SITEA_EMPTY_CONFIRM_ROUNDS

        def save_statistic(storage, item_id):
            storage.save_capture(
                "sitea",
                "GET",
                f"https://sitea.com/proxies/api/v1/info/good/statistic?id={item_id}",
                200,
                b'{"code":200,"data":{}}',
                "application/json",
                {},
            )

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            storage = Storage(
                root / "market.sqlite3",
                root / "raw",
                root / "control.sqlite3",
            )
            storage.upsert_items(
                "sitea",
                [{"item_id": "7", "name": "Test Item"}],
            )
            storage.save_capture(
                "sitea",
                "GET",
                "https://sitea.com/proxies/api/v1/info/good?id=7",
                200,
                b'{"code":200,"data":{"id":7}}',
                "application/json",
                {},
            )
            save_statistic(storage, "7")

            result = reconcile_control_state(
                storage,
                settings(),
                apply=True,
                sections=("sitea",),
            )

            # good 模块有真实数据 → 立即落定；statistic 的 200 空载荷
            # 可能是瞬时软故障，只挂起为 deferred（采集端可重试），不
            # 直接升 done（D10）
            self.assertEqual(
                result["pipelines"]["sitea_detail"]["completed"],
                0,
            )
            self.assertEqual(
                storage.task_slice_status(
                    "sitea_api_detail_p0", "7:good",
                )["status"],
                "done",
            )
            deferred = storage.task_slice_status(
                "sitea_api_detail_p0", "7:statistic",
            )
            self.assertEqual(deferred["status"], "deferred")
            self.assertEqual(deferred["outcome"], "empty_ok")

            storage.upsert_items(
                "sitea",
                [{"item_id": "8", "name": "Second Item"}],
            )
            storage.save_capture(
                "sitea",
                "GET",
                "https://sitea.com/proxies/api/v1/info/good?id=8",
                200,
                b'{"code":200,"data":{"id":8}}',
                "application/json",
                {},
            )
            save_statistic(storage, "8")

            refreshed = reconcile_control_state(
                storage,
                settings(),
                apply=True,
                sections=("sitea",),
            )

            self.assertEqual(
                refreshed["pipelines"]["sitea_detail"]["completed"],
                0,
            )
            self.assertEqual(
                storage.task_slice_status(
                    "sitea_api_detail_p1", "8:good",
                )["status"],
                "done",
            )
            self.assertEqual(
                storage.task_slice_status(
                    "sitea_api_detail_p1", "8:statistic",
                )["status"],
                "deferred",
            )

            # 跨轮次反复观察仍为空，达到阈值后才确认 empty_ok → done
            for _ in range(SITEA_EMPTY_CONFIRM_ROUNDS - 2):
                save_statistic(storage, "7")
                reconcile_control_state(
                    storage,
                    settings(),
                    apply=True,
                    sections=("sitea",),
                )
                self.assertEqual(
                    storage.task_slice_status(
                        "sitea_api_detail_p0", "7:statistic",
                    )["status"],
                    "deferred",
                )
            save_statistic(storage, "7")
            confirmed = reconcile_control_state(
                storage,
                settings(),
                apply=True,
                sections=("sitea",),
            )
            self.assertEqual(
                storage.task_slice_status(
                    "sitea_api_detail_p0", "7:statistic",
                )["status"],
                "done",
            )
            self.assertEqual(
                confirmed["pipelines"]["sitea_detail"]["completed"],
                1,
            )

    @pytest.mark.skip(reason="coupled to site adapters removed in open-source build")
    def test_history_flags_promote_numeric_modules_to_catalog_parent(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            storage = Storage(
                root / "market.sqlite3",
                root / "raw",
                root / "control.sqlite3",
            )
            storage.upsert_items("siteb", [
                {
                    "item_id": "open:test",
                    "name": "Test Item",
                    "market_hash_name": "Test Item",
                },
                {
                    "item_id": "123",
                    "name": "Test Item",
                    "market_hash_name": "Test Item",
                },
            ])
            storage.upsert_catalog_items(
                "siteb",
                ["open:test"],
                universe="siteb_open_api",
            )
            from csgo_monitor.siteb_api import HISTORY_RANGE_SCHEMA
            storage.set_state(
                "full_hist:123",
                "1",
                {
                    "schema": HISTORY_RANGE_SCHEMA,
                    "typeDay": 7,
                    "range": {"span_days": 366},
                },
            )
            storage.set_state(
                "full_kline:123",
                "1",
                {"schema": HISTORY_RANGE_SCHEMA, "exhausted": True},
            )
            storage.set_state(
                "full_comp:123",
                "1",
                {"schema": HISTORY_RANGE_SCHEMA, "typeDay": 7},
            )
            storage.record_failure(
                "siteb_api_histories_b",
                "open:test",
                "temporary",
            )

            result = reconcile_control_state(
                storage,
                settings(),
                apply=True,
                sections=("siteb-history",),
            )

            pipeline = result["pipelines"]["siteb_site_history"]
            self.assertEqual(pipeline["completed"], 1)
            self.assertEqual(
                pipeline["module_done"],
                {
                    "lookup": 1,
                    "history": 1,
                    "kline": 1,
                    "comparison": 1,
                },
            )
            self.assertEqual(
                storage.task_slice_status(
                    "siteb_api_histories_b",
                    "open:test",
                )["status"],
                "done",
            )
            self.assertEqual(
                storage.failure_summary("siteb_api_histories_b"),
                (0, 0),
            )

    def test_pipeline_summary_maps_numeric_module_aliases(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            storage = Storage(
                root / "market.sqlite3",
                root / "raw",
                root / "control.sqlite3",
            )
            storage.upsert_items("siteb", [
                {
                    "item_id": "open:test",
                    "name": "Test Item",
                    "market_hash_name": "Test Item",
                },
                {
                    "item_id": "123",
                    "name": "Test Item",
                    "market_hash_name": "Test Item",
                },
            ])
            storage.upsert_catalog_items(
                "siteb",
                ["open:test"],
                universe="siteb_open_api",
            )
            storage.seed_task_slices(
                "siteb_api_extended",
                (
                    (f"123:{module}", "ok")
                    for module in (
                        "sale_wear",
                        "weapon_trace",
                        "item_changes",
                        "wear_rank",
                        "trade_statistics",
                        "trade_today",
                        "trade_overview",
                        "trade_list",
                    )
                ),
            )

            snapshot = build_pipeline_status(storage)

            pipeline = snapshot["pipelines"]["siteb_extended"]
            self.assertEqual(pipeline["attempted"], 1)
            self.assertEqual(pipeline["completed"], 1)
            self.assertTrue(
                all(value == 1 for value in pipeline["module_done"].values())
            )

    def test_pipeline_status_marks_terminal_remainder(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            storage = Storage(
                root / "market.sqlite3",
                root / "raw",
                root / "control.sqlite3",
            )
            storage.upsert_items("sitea", [
                {"item_id": "1", "name": "Complete"},
                {"item_id": "2", "name": "Limited"},
            ])
            storage.seed_task_slices(
                "sitea_api_detail_p0",
                (
                    ("1:good", "ok"),
                    ("1:statistic", "ok"),
                    ("1", "ok"),
                    ("2:statistic", "ok"),
                ),
            )
            storage.record_failure(
                "sitea_api_detail_p0",
                "2good",
                "payload contract requires review",
                error_class="protocol",
                failure_status="protocol_ticket",
            )

            snapshot = build_pipeline_status(storage)

            pipeline = snapshot["pipelines"]["sitea_detail"]
            self.assertEqual(pipeline["remaining"], 1)
            self.assertEqual(pipeline["status"], "limited")

    def test_terminal_done_module_is_not_counted_as_complete(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            storage = Storage(
                root / "market.sqlite3", root / "raw",
                root / "control.sqlite3",
            )
            storage.upsert_items("sitea", [{"item_id": "1", "name": "Test"}])
            storage.seed_task_slices(
                "sitea_api_detail_p0",
                [("1:good", "ok"), ("1:statistic", "ok")],
            )
            storage.update_task_slice(
                "sitea_api_detail_p0", "1:statistic",
                "done", outcome="authorization",
            )
            pipeline = build_pipeline_status(storage)["pipelines"]["sitea_detail"]
            self.assertEqual(pipeline["completed"], 0)
            self.assertEqual(pipeline["module_done"]["statistic"], 0)
            self.assertEqual(pipeline["outcomes"]["authorization"], 1)

    def test_open_kline_legacy_evidence_is_refresh_due(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            storage = Storage(
                root / "market.sqlite3", root / "raw",
                root / "control.sqlite3",
            )
            storage.upsert_items("siteb", [
                {"item_id": "open:1", "name": "Test",
                 "market_hash_name": "Test"},
            ])
            storage.upsert_catalog_items(
                "siteb", ["open:1"],
                universe="siteb_open_api",
            )
            storage.seed_task_slices(
                "siteb_open_histories_kline",
                [("open:1:kline", "legacy_data")],
            )
            status = build_pipeline_status(storage)["pipelines"]["siteb_open_kline"]
            self.assertEqual(status["completed"], 1)
            self.assertEqual(status["refresh_due"], 1)
            self.assertEqual(status["status"], "stale")


if __name__ == "__main__":
    unittest.main()
