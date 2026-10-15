import json
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from csgo_monitor.supervisor import (
    TaskSpec,
    TaskSupervisor,
    default_task_specs,
)
from csgo_monitor.storage import Storage


def settings(root: Path) -> SimpleNamespace:
    return SimpleNamespace(
        project_dir=Path(__file__).resolve().parent.parent,
        output_dir=root,
        supervisor_max_parallel=3,
        supervisor_heartbeat_seconds=5,
        supervisor_lease_seconds=30,
        supervisor_lane_limits={
            "siteb_site_fast": 1,
            "siteb_site_bulk": 1,
            "siteb_open": 1,
            "sitea": 1,
            "public_pages": 1,
            "maintenance": 1,
            "local": 1,
        },
        runtime_routes={},
    )


class SupervisorContractTests(unittest.TestCase):
    def test_shared_public_pause_prevents_route_probe_and_respects_retry_time(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            storage = Storage(root / "market.sqlite3", root / "raw", root / "control.sqlite3")
            supervisor = TaskSupervisor(settings(root), storage, root / "config.yaml", tasks=())
            spec = TaskSpec("sitea-public", "sitea_public", ("discover",), 60)
            storage.set_state("sitea_public_pause_until", str(time.time() + 7200), {
                "error_class": "rate_limit",
            })
            with patch.object(supervisor, "_select_route") as select:
                supervisor._launch(spec)
            select.assert_not_called()
            self.assertGreater(supervisor._next_delay(spec, "deferred", "rate_limit"), 7100)
            self.assertGreater(supervisor.next_due[spec.task_id], time.monotonic() + 7100)
            unrelated = TaskSpec("open", "siteb_open_api", ("incremental",), 60)
            self.assertEqual(supervisor._shared_source_pause(unrelated), (0, ""))

    def test_audit_pause_is_deferred_with_long_backoff(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            storage = Storage(root / "market.sqlite3", root / "raw", root / "control.sqlite3")
            supervisor = TaskSupervisor(settings(root), storage, root / "config.yaml", tasks=())
            spec = TaskSpec("siteb-public", "siteb_public_pages", ("discover",), 60)
            self.assertGreaterEqual(
                supervisor._next_delay(spec, "deferred", "audit"),
                1800,
            )

    def test_default_tasks_include_services_and_all_lanes(self):
        specs = default_task_specs()
        by_id = {spec.task_id: spec for spec in specs}

        self.assertEqual(by_id["web"].mode, "service")
        self.assertFalse(by_id["web"].worker_slot)
        self.assertEqual(by_id["siteb-incremental"].mode, "service")
        self.assertFalse(by_id["siteb-incremental"].worker_slot)
        self.assertEqual(by_id["runtime-report"].interval_seconds, 1800)
        self.assertEqual(by_id["runtime-report"].lane, "local")
        self.assertEqual(by_id["siteb-kline"].interval_seconds, 21600)
        self.assertIn(
            "--refresh-kline-hours", by_id["siteb-kline"].arguments,
        )
        self.assertFalse(by_id["runtime-report"].worker_slot)
        for task_id in ("siteb-public-pages", "sitea-public-pages"):
            self.assertEqual(by_id[task_id].lane, "public_pages")
            self.assertFalse(by_id[task_id].worker_slot)
            self.assertEqual(by_id[task_id].catchup_interval, 120)
        self.assertIn(
            "--reconcile",
            by_id["runtime-maintenance"].arguments,
        )
        self.assertEqual(
            {spec.lane for spec in specs if spec.mode == "scheduled"},
            {
                "siteb_site_fast",
                "siteb_site_bulk",
                "siteb_open",
                "sitea",
                "public_pages",
                "page_harvest",
                "maintenance",
                "local",
            },
        )

    def test_result_exit_codes_map_to_task_states(self):
        classify = TaskSupervisor._classify_result

        self.assertEqual(classify(0, None, "", timed_out=False)[0], "ok")
        self.assertEqual(
            classify(3, None, "", timed_out=False)[0],
            "partial",
        )
        self.assertEqual(
            classify(75, None, "", timed_out=False)[0],
            "deferred",
        )
        self.assertEqual(
            classify(1, None, "parse failed", timed_out=False)[0],
            "failed",
        )
        self.assertEqual(
            classify(
                3,
                {"status": "partial", "error_class": "parse"},
                "",
                timed_out=False,
            ),
            ("partial", "parse"),
        )

    def test_worker_slots_and_lane_limits_are_independent(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            storage = Storage(
                root / "market.sqlite3",
                root / "raw",
                root / "control.sqlite3",
            )
            supervisor = TaskSupervisor(
                settings(root),
                storage,
                root / "config.yaml",
                tasks=(),
            )
            service = TaskSpec(
                "incremental",
                "local",
                ("incremental",),
                0,
                mode="service",
                worker_slot=True,
            )
            bulk = TaskSpec(
                "bulk",
                "local",
                ("maintenance",),
                60,
                lane="siteb_site_bulk",
            )
            fast = TaskSpec(
                "fast",
                "local",
                ("maintenance",),
                60,
                lane="siteb_site_fast",
            )
            supervisor.active["incremental"] = SimpleNamespace(spec=service)

            self.assertTrue(supervisor._capacity_available(bulk))
            supervisor.active["bulk"] = SimpleNamespace(spec=bulk)
            self.assertFalse(supervisor._capacity_available(bulk))
            self.assertTrue(supervisor._capacity_available(fast))

    def test_local_report_runs_when_worker_slots_are_full(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            storage = Storage(
                root / "market.sqlite3",
                root / "raw",
                root / "control.sqlite3",
            )
            supervisor = TaskSupervisor(
                settings(root),
                storage,
                root / "config.yaml",
                tasks=(),
            )
            worker = TaskSpec(
                "worker",
                "local",
                ("maintenance",),
                60,
                lane="siteb_site_bulk",
            )
            for index in range(3):
                supervisor.active[f"worker-{index}"] = SimpleNamespace(
                    spec=worker
                )
            report = TaskSpec(
                "report",
                "local",
                ("report",),
                60,
                lane="local",
                worker_slot=False,
                priority=100,
            )

            self.assertEqual(supervisor._capacity_reason(report), "")

    def test_blocked_reason_tracks_lane_capacity(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            storage = Storage(
                root / "market.sqlite3",
                root / "raw",
                root / "control.sqlite3",
            )
            waiting = TaskSpec(
                "waiting",
                "local",
                ("maintenance",),
                60,
                lane="sitea",
            )
            active = TaskSpec(
                "active",
                "local",
                ("maintenance",),
                60,
                lane="sitea",
            )
            supervisor = TaskSupervisor(
                settings(root),
                storage,
                root / "config.yaml",
                tasks=(waiting,),
            )
            supervisor.active["active"] = SimpleNamespace(spec=active)
            supervisor.next_due["waiting"] = 0

            with patch.object(supervisor, "_launch") as launch:
                supervisor._launch_due((waiting,))

            launch.assert_not_called()
            self.assertEqual(
                supervisor.blocked_reasons["waiting"],
                "lane:sitea",
            )

    def test_runtime_recovery_recognizes_legacy_uvicorn_web(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            storage = Storage(
                root / "market.sqlite3",
                root / "raw",
                root / "control.sqlite3",
            )
            supervisor = TaskSupervisor(
                settings(root),
                storage,
                root / "config.yaml",
                tasks=default_task_specs(),
            )

            self.assertTrue(supervisor._managed_command(
                '"python.exe" -m uvicorn web.app:app '
                '--host 127.0.0.1 --port 8800',
                default_task_specs(),
            ))
            self.assertFalse(supervisor._managed_command(
                '"python.exe" -m uvicorn web.app:app '
                '--host 127.0.0.1 --port 9900',
                default_task_specs(),
            ))

    def test_single_run_records_result_and_releases_lease(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            output = root / "output"
            config_path = root / "config.yaml"
            config_path.write_text(
                "crawler:\n"
                f"  output_dir: \"{output.as_posix()}\"\n",
                encoding="utf-8",
            )
            storage = Storage(
                output / "market.sqlite3",
                output / "raw",
                output / "control.sqlite3",
            )
            spec = TaskSpec(
                "maintenance-test",
                "local",
                (
                    "maintenance",
                    "--reclassify-limit",
                    "0",
                    "--no-checkpoint",
                ),
                3600,
                lane="maintenance",
                timeout_seconds=30,
            )
            supervisor = TaskSupervisor(
                settings(output),
                storage,
                config_path,
                python=sys.executable,
                tasks=(spec,),
            )

            with patch.object(
                supervisor,
                "_process_snapshot",
                return_value={},
            ):
                exit_code = supervisor.run(
                    once=True,
                    selected_tasks=("maintenance-test",),
                )

            self.assertEqual(exit_code, 0)
            with storage.control_connect() as conn:
                run = conn.execute(
                    """SELECT status,result_json FROM task_runs
                    WHERE task_id='maintenance-test'"""
                ).fetchone()
                lease = conn.execute(
                    """SELECT status FROM task_leases
                    WHERE task_id='maintenance-test'"""
                ).fetchone()
            self.assertEqual(run["status"], "ok")
            self.assertEqual(lease["status"], "ok")
            result = json.loads(run["result_json"])
            self.assertTrue(Path(result["result_path"]).exists())


if __name__ == "__main__":
    unittest.main()


class SupervisorAgingTests(unittest.TestCase):
    """反饥饿（aging）：久未派发的任务优先级获得加成，避免低优先级
    bulk 任务在共享每日配额的池子里被永远轮空。"""

    def _spec(self, task_id, priority):
        from csgo_monitor.supervisor import TaskSpec

        return TaskSpec(task_id, "local", (task_id,), 60, priority=priority)

    def test_starved_low_priority_task_outranks_fresh_high_priority(self):
        from csgo_monitor.supervisor import TaskSupervisor

        specs = (self._spec("bulk-low", 60), self._spec("fast-high", 80))
        supervisor = TaskSupervisor.__new__(TaskSupervisor)
        now = time.monotonic()
        supervisor.last_dispatch = {
            "bulk-low": now - 20 * 3600,
            "fast-high": now,
        }
        self.assertGreater(
            TaskSupervisor._effective_priority(supervisor, specs[0], now),
            TaskSupervisor._effective_priority(supervisor, specs[1], now),
        )
        # 老化加成封顶 120（60h × 2/h）：local 车道存在 priority=100 的
        # 调度任务（runtime-report），封顶必须让最低基线任务（14）老化
        # 后也能反超，否则 daily-export 永久饿死（M1）
        supervisor.last_dispatch["bulk-low"] = now - 100 * 3600
        self.assertAlmostEqual(
            TaskSupervisor._effective_priority(supervisor, specs[0], now),
            specs[0].priority + 120,
        )
        # local 车道拓扑：runtime-report 基线 100 对 daily-export 基线 14
        low = self._spec("daily-export", 14)
        high = self._spec("runtime-report", 100)
        supervisor.last_dispatch = {
            "daily-export": now - 60 * 3600,
            "runtime-report": now,
        }
        self.assertGreater(
            TaskSupervisor._effective_priority(supervisor, low, now),
            TaskSupervisor._effective_priority(supervisor, high, now),
        )

    def test_last_dispatch_seeded_from_task_runs(self):
        from csgo_monitor.supervisor import TaskSupervisor

        specs = (self._spec("fresh", 80), self._spec("stale", 60))
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            storage = Storage(root / "db.sqlite3", root / "raw")
            recent = datetime.now(timezone.utc)
            storage.start_task_run("stale", "direct")
            storage.start_task_run("fresh", "direct")
            with storage.control_connect() as conn:
                conn.execute(
                    "UPDATE task_runs SET finished_at=? WHERE task_id=?",
                    ((recent - timedelta(days=2)).isoformat(), "stale"),
                )
                conn.execute(
                    "UPDATE task_runs SET finished_at=? WHERE task_id=?",
                    ((recent - timedelta(hours=1)).isoformat(), "fresh"),
                )
            seeded = TaskSupervisor.__new__(TaskSupervisor)
            seeded.storage = storage
            seeded.tasks = specs
            seeded.last_dispatch = {s.task_id: time.monotonic() for s in specs}
            seeded._seed_last_dispatch()
            now = time.monotonic()
            age_stale = (now - seeded.last_dispatch["stale"]) / 3600
            age_fresh = (now - seeded.last_dispatch["fresh"]) / 3600
            self.assertGreater(age_stale, 40)
            self.assertLess(age_fresh, 3)
