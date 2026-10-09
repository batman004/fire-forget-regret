"""End to end: real vendor server, real SIGKILL, real resume."""

import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.request
from pathlib import Path

from demo import ui, vendors

ROOT = Path(__file__).resolve().parents[1]


class DemoTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        vendors.ui.vendor_log = lambda *a, **k: None  # keep test output quiet
        cls.vendors = vendors.Vendors("flaky", seed=7, latency=(0.0, 0.005), hang=0.5)
        cls.server = vendors.VendorServer(("127.0.0.1", 0), cls.vendors)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.url = f"http://127.0.0.1:{cls.server.server_address[1]}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self):
        self.vendors.set_paused(False)
        self.vendors.set_llm_stopped(False)
        self.vendors.reset()
        self.tmp = tempfile.TemporaryDirectory()
        self.env = {**os.environ, "VENDOR_URL": self.url, "DEMO_STATE_DIR": self.tmp.name,
                    "NO_COLOR": "1", "VENDOR_TIMEOUT": "0.3"}

    def tearDown(self):
        self.tmp.cleanup()

    def demo(self, *args) -> subprocess.CompletedProcess:
        return subprocess.run([sys.executable, "-m", "demo", *args], cwd=ROOT, env=self.env,
                              capture_output=True, text=True, timeout=60)

    def ledger(self) -> dict:
        with urllib.request.urlopen(self.url + "/ledger") as r:
            return json.load(r)["orders"]

    def charges(self) -> dict[str, int]:
        return {o: e["charges"] for o, e in self.ledger().items()}

    def test_crash_then_resume_charges_everyone_once(self):
        crashed = self.demo("run", "--orders", "6", "--crash-at", "order-0004:reserve_inventory")
        self.assertEqual(crashed.returncode, -9, crashed.stderr)
        self.assertNotIn("order-0005", self.charges())

        status = self.demo("status")
        self.assertIn("killed during reserve_inventory", status.stdout)

        resumed = self.demo("run", "--orders", "6")
        self.assertEqual(resumed.returncode, 0, resumed.stdout + resumed.stderr)
        self.assertIn("already fulfilled", resumed.stdout)
        self.assertEqual(self.charges(), {f"order-000{i}": 1 for i in range(1, 7)})

    def test_naive_rerun_charges_again(self):
        self.demo("naive", "--orders", "4", "--crash-at", "order-0003:reserve_inventory")
        self.demo("naive", "--orders", "4")
        charges = self.charges()
        self.assertGreaterEqual(charges["order-0001"], 2)
        self.assertGreaterEqual(charges["order-0002"], 2)

    def test_crash_after_effect_needs_idempotency_key(self):
        args = ("run", "--orders", "2", "--crash-at", "order-0002:capture_payment:after")
        self.demo(*args, "--no-idempotency")
        self.demo("run", "--orders", "2", "--no-idempotency")
        self.assertEqual(self.charges()["order-0002"], 2)

        self.demo("reset")
        self.demo(*args)
        self.demo("run", "--orders", "2")
        self.assertEqual(self.charges()["order-0002"], 1)
        self.assertEqual(self.ledger()["order-0002"]["replays"], 1)

    def test_concurrent_run_fulfils_every_order_once(self):
        result = self.demo("run", "--orders", "40", "--concurrency", "8")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(set(self.charges().values()), {1})
        self.assertEqual(len(self.charges()), 40)

    def test_pause_holds_a_call_while_admin_stays_up(self):
        self.vendors.set_paused(True)
        try:
            with urllib.request.urlopen(self.url + "/admin/control", timeout=1) as response:
                self.assertTrue(json.load(response)["paused"])
            box: dict = {}

            def call():
                request = urllib.request.Request(
                    self.url + "/fraud/check",
                    data=json.dumps({"order_id": "order-pause"}).encode(),
                    headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(request, timeout=3) as response:
                    box["status"] = response.status

            thread = threading.Thread(target=call)
            thread.start()
            thread.join(0.4)
            self.assertNotIn("status", box)
            self.vendors.set_paused(False)
            thread.join(2)
            self.assertEqual(box.get("status"), 200)
        finally:
            self.vendors.set_paused(False)

    def test_control_can_stop_llm_calls(self):
        stopped = self.demo("control", "llm", "stop")
        self.assertEqual(stopped.returncode, 0, stopped.stdout + stopped.stderr)
        self.assertIn("STOPPED", stopped.stdout)
        request = urllib.request.Request(
            self.url + "/llm/eu-west-1/complete",
            data=json.dumps({"task": "summarise", "event": {"id": "evt-x"}}).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(request, timeout=1)
        self.assertEqual(ctx.exception.code, 503)
        self.assertEqual(self.vendors.security.regions["eu-west-1"]["calls"], 0)
        intel = urllib.request.Request(
            self.url + "/intel/lookup",
            data=json.dumps({"event_id": "evt-x", "ip": "1.2.3.4"}).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(intel, timeout=1) as response:
            self.assertEqual(response.status, 200)

    def test_board_page_and_snapshot(self):
        from demo.board import snapshot_db
        from stitch import Store

        with urllib.request.urlopen(self.url + "/", timeout=1) as response:
            page = response.read().decode()
            self.assertEqual(response.headers.get("Cache-Control"), "no-store")
        self.assertIn("Pause network", page)

        with urllib.request.urlopen(self.url + "/admin/board", timeout=1) as response:
            body = json.load(response)
        self.assertFalse(body["control"]["paused"])
        self.assertIn("runs", body)
        self.assertIn("eu-west-1", body["llm"]["regions"])

        path = Path(self.tmp.name) / "checkpoints.db"
        store = Store(path)
        store.open_run("triage:demo", "triage", {"events": 2})
        store.set_item("triage:demo", "evt-0001", "running",
                       input=json.dumps({"id": "evt-0001", "type": "port_scan", "asset": "vpn-gateway-01"}))
        store.step_started("triage:demo", "evt-0001", "enrich", 0)
        store.close()
        snap = snapshot_db(path)
        item = snap["runs"][0]["items"][0]
        self.assertEqual(item["label"], "Port scan · vpn-gateway-01")
        self.assertEqual(item["steps"]["enrich"]["status"], "running")
        self.assertEqual([col["id"] for col in snap["runs"][0]["columns"]],
                         ["enrich", "summarise", "classify", "publish"])


class UiTest(unittest.TestCase):
    def test_ledger_flags_duplicates(self):
        lines = []
        original = ui.say
        ui.say = lambda *parts: lines.append(" ".join(map(str, parts)))
        try:
            ui.render_ledger({"chaos": "calm", "injected_failures": 0, "orders": {
                "order-0001": {"charges": 2, "charged_total": 20.0, "reservations": 1,
                               "labels": 1, "emails": 2, "replays": 0}}})
        finally:
            ui.say = original
        self.assertTrue(any("1 customers charged more than once (€10.00 too much)" in l for l in lines))


if __name__ == "__main__":
    unittest.main()


class TriageTest(unittest.TestCase):
    """The LLM story: credits run out, events degrade, a new region heals them."""

    @classmethod
    def setUpClass(cls):
        vendors.ui.vendor_log = lambda *a, **k: None
        cls.vendors = vendors.Vendors("calm", seed=7, latency=(0.0, 0.005), hang=0.5)
        cls.server = vendors.VendorServer(("127.0.0.1", 0), cls.vendors)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.url = f"http://127.0.0.1:{cls.server.server_address[1]}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self):
        self.vendors.set_paused(False)
        self.vendors.set_llm_stopped(False)
        self.vendors.reset()
        self.tmp = tempfile.TemporaryDirectory()
        self.env = {**os.environ, "VENDOR_URL": self.url, "DEMO_STATE_DIR": self.tmp.name,
                    "NO_COLOR": "1"}

    def tearDown(self):
        self.tmp.cleanup()

    def demo(self, *args) -> subprocess.CompletedProcess:
        return subprocess.run([sys.executable, "-m", "demo", *args], cwd=ROOT, env=self.env,
                              capture_output=True, text=True, timeout=60)

    def dashboard(self) -> dict[str, dict]:
        with urllib.request.urlopen(self.url + "/ui/events") as r:
            return {e["id"]: e for e in json.load(r)["events"]}

    def test_credits_run_out_then_new_region_heals(self):
        self.demo("llm", "topup", "eu-west-1", "500")
        run = self.demo("triage", "--events", "6")
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        shown = self.dashboard()
        self.assertEqual(len(shown), 6)  # nothing is stale, even during the outage
        degraded = [e for e in shown.values() if e["source"] == "rule-based"]
        self.assertTrue(0 < len(degraded) < 6, [e["source"] for e in shown.values()])

        self.demo("providers", "add", "us-east-1")
        healed = self.demo("heal")
        self.assertEqual(healed.returncode, 0, healed.stdout + healed.stderr)
        shown = self.dashboard()
        self.assertEqual({e["source"] for e in shown.values()}, {"llm"})
        self.assertEqual(max(e["version"] for e in shown.values()), 2)  # upserted, not duplicated

        trace = self.demo("trace", degraded[0]["id"])
        for expected in ("breaker OPEN", "used its fallback", "heal: redoing", "us-east-1"):
            self.assertIn(expected, trace.stdout)

    def test_crash_during_llm_step_resumes_without_repeating_work(self):
        crashed = self.demo("triage", "--events", "5", "--crash-at", "evt-0003:classify")
        self.assertEqual(crashed.returncode, -9)
        resumed = self.demo("triage", "--events", "5", "--crash-at", "evt-0003:classify")
        self.assertEqual(resumed.returncode, 0, resumed.stdout + resumed.stderr)
        self.assertIn("already done", resumed.stdout)
        self.assertEqual(len(self.dashboard()), 5)
        spent = self.vendors.security.regions["eu-west-1"]["calls"]
        self.assertEqual(spent, 10)  # 5 events x 2 LLM calls: summarise was not paid for twice

    def test_park_policy_waits_instead_of_publishing_a_fallback(self):
        self.demo("llm", "topup", "eu-west-1", "300")
        run = self.demo("triage", "--events", "4", "--on-outage", "park", "--no-wait")
        self.assertIn("parked", run.stdout)
        shown = self.dashboard()
        self.assertTrue(all(e["source"] == "llm" for e in shown.values()))  # no placeholders
        self.assertLess(len(shown), 4)
        self.demo("llm", "topup", "eu-west-1", "5000")
        resumed = self.demo("resume", "--no-wait")
        self.assertEqual(resumed.returncode, 0, resumed.stdout + resumed.stderr)

    def test_live_command_runs_the_pipeline(self):
        result = self.demo("live", "--events", "1")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("break it by hand", result.stdout)
        self.assertEqual(len(self.dashboard()), 1)
