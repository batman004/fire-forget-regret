import asyncio
import contextlib
import io
import json
import os
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

os.environ["TALK_RETRY_SECONDS"] = "0.001"

try:
    from talk import breach_pipeline as plain_pipeline
    from talk import breach_pipeline_durable as durable_pipeline
except ImportError as error:
    raise unittest.SkipTest(f"needs the demo dependencies: {error}")

ARTICLE_COUNT = 4
FEED_XML = (
    '<rss><channel>'
    + "".join(
        f"<item><title>Breach number {number}</title><link>https://news.example/a{number}</link>"
        f"<description>summary {number}</description></item>"
        for number in range(1, ARTICLE_COUNT + 1)
    )
    + "</channel></rss>"
).encode()
ARTICLE_PAGE = ("<html><p>" + "The company confirmed a breach of customer data. " * 5 + "</p></html>").encode()
CLAUDE_REPLY = json.dumps({"content": [{"text": '{"severity": "high", "summary": "short summary"}'}]}).encode()


class ProcessKilled(BaseException):
    pass


class FakeInternet:
    def __init__(self):
        self.offline = False
        self.claude_refuses = False
        self.kill_on_claude_call = None
        self.claude_requests = 0
        self.downloads = []

    def __call__(self, url, headers=None, body=None):
        if self.offline:
            raise urllib.error.URLError("no internet")
        if url == plain_pipeline.FEED_URL:
            return FEED_XML
        if url == plain_pipeline.CLAUDE_URL:
            return self._claude_answer(url)
        self.downloads.append(url)
        return ARTICLE_PAGE

    def _claude_answer(self, url):
        self.claude_requests += 1
        if self.claude_requests == self.kill_on_claude_call:
            raise ProcessKilled()
        if self.claude_refuses:
            raise urllib.error.HTTPError(url, 429, "limit reached", {}, None)
        return CLAUDE_REPLY


class TalkPipelineTestCase(unittest.TestCase):
    module = plain_pipeline

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        output = Path(self.directory.name)
        self.internet = FakeInternet()
        self.patches = [
            mock.patch.object(self.module, "http_get", self.internet),
            mock.patch.object(self.module, "OUTPUT_DIRECTORY", output),
            mock.patch.object(self.module, "PUBLISHED_FILE", output / "published.jsonl"),
            mock.patch.object(self.module, "CLAUDE_CALLS_FILE", output / "claude_calls.log"),
            mock.patch.object(self.module, "HEADLINES_FILE", output / "headlines.json"),
            mock.patch.object(self.module, "ARTICLES_PER_RUN", ARTICLE_COUNT),
            mock.patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}),
        ]
        if hasattr(self.module, "CHECKPOINT_FILE"):
            self.patches.append(mock.patch.object(self.module, "CHECKPOINT_FILE", output / "checkpoints.db"))
        for patch in self.patches:
            patch.start()

    def tearDown(self):
        for patch in self.patches:
            patch.stop()
        self.directory.cleanup()

    def run_quietly(self, coroutine):
        with contextlib.redirect_stdout(io.StringIO()):
            return asyncio.run(coroutine)

    def published(self) -> list[dict]:
        return [json.loads(line) for line in self.module.read_lines(self.module.PUBLISHED_FILE)]

    def paid_claude_calls(self) -> int:
        return len(self.module.read_lines(self.module.CLAUDE_CALLS_FILE))


class PlainPipelineTest(TalkPipelineTestCase):
    def test_a_crash_makes_the_rerun_repeat_work_and_publish_duplicates(self):
        self.internet.kill_on_claude_call = 3
        with self.assertRaises(BaseException):
            self.run_quietly(self.module.run_pipeline())
        self.assertEqual(len(self.published()), 2)
        self.run_quietly(self.module.run_pipeline())
        published = self.published()
        self.assertEqual((len(published), len({record["id"] for record in published})), (ARTICLE_COUNT + 2, ARTICLE_COUNT))
        self.assertEqual(self.paid_claude_calls(), ARTICLE_COUNT + 2)

    def test_claude_refusing_stops_the_whole_run(self):
        self.internet.claude_refuses = True
        with self.assertRaises(ConnectionError):
            self.run_quietly(self.module.run_pipeline())
        self.assertEqual(self.published(), [])


class DurablePipelineTest(TalkPipelineTestCase):
    module = durable_pipeline

    def test_a_crash_resumes_without_repeating_work_or_duplicating(self):
        self.internet.kill_on_claude_call = 3
        with self.assertRaises(BaseException):
            self.run_quietly(self.module.run_pipeline())
        self.run_quietly(self.module.run_pipeline())
        published = self.published()
        self.assertEqual((len(published), len({record["id"] for record in published})), (ARTICLE_COUNT, ARTICLE_COUNT))
        self.assertEqual(self.paid_claude_calls(), ARTICLE_COUNT)
        self.assertEqual(self.internet.downloads.count("https://news.example/a3"), 1)

    def test_claude_refusing_publishes_flagged_fallbacks_and_heal_replaces_them(self):
        self.internet.claude_refuses = True
        self.run_quietly(self.module.run_pipeline())
        self.assertEqual({record["source"] for record in self.published()}, {"fallback"})
        self.assertEqual(self.paid_claude_calls(), 0)
        self.internet.claude_refuses = False
        self.run_quietly(self.module.heal_pipeline())
        latest = {record["id"]: record for record in self.published()}
        self.assertEqual({record.get("source") for record in latest.values()}, {None})
        self.assertEqual(self.paid_claude_calls(), ARTICLE_COUNT)
        self.assertEqual(len(self.internet.downloads), ARTICLE_COUNT)

    def test_an_internet_outage_stops_articles_cleanly_and_a_rerun_finishes_them(self):
        self.run_quietly(self.module.fetch_headlines())
        self.internet.offline = True
        self.run_quietly(self.module.run_pipeline())
        self.assertEqual(self.published(), [])
        self.internet.offline = False
        self.run_quietly(self.module.run_pipeline())
        self.assertEqual(len({record["id"] for record in self.published()}), ARTICLE_COUNT)
        self.assertEqual(self.paid_claude_calls(), ARTICLE_COUNT)

    def test_the_two_scripts_differ_only_by_the_durability_lines(self):
        plain = set(Path(plain_pipeline.__file__).read_text().splitlines())
        durable = set(Path(durable_pipeline.__file__).read_text().splitlines())
        removed = sorted(line for line in plain - durable if line.strip())
        self.assertEqual(removed, ['        await process_article(article)', '        print("usage: python -m talk.breach_pipeline [run|report|reset]")', '    for article in await fetch_headlines():', '    logger.success("finished")'])


if __name__ == "__main__":
    unittest.main()
