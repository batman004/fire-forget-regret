"""The opening hook must behave the same on every supported Python."""

import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def act(n: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, "-m", "hook", n], cwd=ROOT,
                          capture_output=True, text=True, timeout=30)


class HookTest(unittest.TestCase):
    def test_act1_task_is_cancelled_silently(self):
        r = act("1")
        self.assertIn("sending welcome email", r.stdout)
        self.assertNotIn("✓ welcome email sent", r.stdout)
        self.assertEqual(r.stderr, "")

    def test_act2_task_is_garbage_collected(self):
        r = act("2")
        self.assertIn("Task was destroyed but it is pending!", r.stderr)
        self.assertIn("nobody is waiting for it", r.stdout)
        self.assertNotIn("delivered", r.stdout)

    def test_act3_task_survives(self):
        r = act("3")
        self.assertIn("✓ welcome email delivered", r.stdout)
        self.assertNotIn("destroyed", r.stderr)


class QuickstartTest(unittest.TestCase):
    """examples/quickstart.py is the first thing a reader runs: keep it honest."""

    def run_example(self, tmp, die=None) -> subprocess.CompletedProcess:
        import os
        env = {**os.environ, "QUICKSTART_DB": f"{tmp}/q.db", "QUICKSTART_MARKER": f"{tmp}/died"}
        env.pop("DIE", None)
        if die:
            env["DIE"] = die
        return subprocess.run([sys.executable, "examples/quickstart.py"], cwd=ROOT, env=env,
                              capture_output=True, text=True, timeout=30)

    def test_kill_then_same_command_resumes(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            first = self.run_example(tmp, die="job-2")
            self.assertEqual(first.returncode, -9)
            second = self.run_example(tmp, die="job-2")
            self.assertEqual(second.returncode, 0, second.stderr)
            for repeated in ("fetch     job-1", "transform job-1", "fetch     job-2"):
                self.assertNotIn(repeated, second.stdout)  # checkpointed, not redone
            for expected in ("transform job-2", "load      job-2", "fetch     job-3"):
                self.assertIn(expected, second.stdout)
            self.assertIn("job-3: loaded 6 rows", second.stdout)


if __name__ == "__main__":
    unittest.main()
