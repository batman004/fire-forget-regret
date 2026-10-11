from loguru import logger
from stitch import Reporter


class TalkReporter(Reporter):
    def on_item_skipped(self, item):
        logger.info("[{}] already finished, skipped", item)

    def on_step_skipped(self, item, step):
        logger.info("[{}] {}: already done, skipped", item, step)

    def on_step_retry(self, item, step, attempt, attempts, delay, error):
        logger.warning("[{}] {} failed ({}); retry {}/{} in {:.0f}s", item, step, error, attempt + 1, attempts, delay)

    def on_step_degraded(self, item, step, reason):
        logger.warning("[{}] {} gave up ({}); using the fallback", item, step, reason)

    def on_step_failed(self, item, step, error):
        logger.error("[{}] {} failed for good: {}", item, step, error)

    def on_item_failed(self, item, error):
        logger.error("[{}] stopped: {}. Run again to continue from here.", item, error)
