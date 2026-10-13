from loguru import logger
from stitch import Reporter

MAXIMUM_ERROR_CHARACTERS = 160


def shorten(text: str) -> str:
    return text if len(text) <= MAXIMUM_ERROR_CHARACTERS else text[:MAXIMUM_ERROR_CHARACTERS - 3] + "..."


class TalkReporter(Reporter):
    def on_item_skipped(self, item):
        logger.info("[{}] already finished, skipped", item)

    def on_step_skipped(self, item, step):
        logger.info("[{}] {}: already done, skipped", item, step)

    def on_step_retry(self, item, step, attempt, attempts, delay, error):
        logger.warning("[{}] {} failed ({}); retry {}/{} in {:.0f}s", item, step, shorten(error), attempt + 1, attempts, delay)

    def on_step_degraded(self, item, step, reason):
        logger.warning("[{}] {} gave up ({}); using the fallback", item, step, shorten(reason))

    def on_step_failed(self, item, step, error):
        logger.error("[{}] {} failed for good: {}", item, step, shorten(error))

    def on_item_failed(self, item, error):
        logger.error("[{}] stopped: {}. Run again to continue from here.", item, shorten(error))

    def on_critical_failure(self, item, step, error):
        logger.critical("[{}] {} is critical and failed for good: {}", item, step, shorten(error))

    def on_run_halted(self, step):
        logger.critical("run halted. It starts again only when the {} resource is healthy.", step)

    def on_run_blocked(self, step, reason):
        logger.error("still halted: the {} resource is not healthy ({})", step, shorten(reason))

    def on_run_recovered(self, step):
        logger.success("the {} resource is healthy again, resuming the run", step)
