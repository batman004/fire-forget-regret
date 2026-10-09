import os
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class RecordScriptTest(unittest.TestCase):
    """The backup recording must replay the demo the same way every time."""

    def replay(self, *sections: str) -> str:
        cmd = [sys.executable, "scripts/record_demo.py", "--fast", "--chaos", "calm"]
        for s in sections:
            cmd += ["--section", s]
        r = subprocess.run(cmd, cwd=ROOT, env={**os.environ, "NO_COLOR": "1"},
                           capture_output=True, text=True, timeout=120)
        self.assertEqual(r.returncode, 0, r.stderr)
        return r.stdout

    def test_triage_story_replays(self):
        out = self.replay("triage")
        self.assertIn("breaker open on eu-west-1", out)
        self.assertIn("4 rule-based fallback", out)
        self.assertIn("8 analysed by the LLM · 0 rule-based fallback", out)
        # the typed command sits on its own line, not glued to the output
        self.assertIn("$ python -m demo providers add us-east-1 --priority 2\n", out)

    def test_orders_story_replays(self):
        out = self.replay("orders")
        self.assertIn("charged more than once", out)               # naive: the regret
        self.assertIn("every order charged exactly once · 8 orders", out)  # checkpointed
        self.assertIn("order-0002", out)


if __name__ == "__main__":
    unittest.main()
