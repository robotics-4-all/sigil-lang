import json
import os
import signal
import sys
import time
import logging

from pipelines.valid_readings import ValidReadingsPipeline


def _configure_logging():
    """Configure logging with optional JSON structured output.

    Set LOG_FORMAT=json for JSON-line output (machine-readable).
    Set LOG_LEVEL to DEBUG, INFO, WARNING, ERROR, or CRITICAL.
    Defaults: LOG_FORMAT=text, LOG_LEVEL=INFO.
    """
    log_format = os.environ.get("LOG_FORMAT", "text").lower()
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()

    root = logging.getLogger()
    root.setLevel(getattr(logging, log_level, logging.INFO))

    handler = logging.StreamHandler(sys.stderr)

    if log_format == "json":

        class _JsonFormatter(logging.Formatter):
            def format(self, record):
                return json.dumps(
                    {
                        "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
                        "level": record.levelname,
                        "logger": record.name,
                        "msg": record.getMessage(),
                    },
                    default=str,
                )

        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")
        )

    root.addHandler(handler)


_configure_logging()
logger = logging.getLogger("sigil.app")

_shutdown_in_progress = False


def main():
    global _shutdown_in_progress

    pipeline_instances = {
        "valid_readings": ValidReadingsPipeline(),
    }


    def shutdown(sig, frame):
        global _shutdown_in_progress
        if _shutdown_in_progress:
            logger.warning("Forced shutdown")
            sys.exit(1)
        _shutdown_in_progress = True
        sig_name = signal.Signals(sig).name
        logger.info("Received %s — draining pipelines ...", sig_name)
        for name, p in pipeline_instances.items():
            try:
                p.drain()
                logger.info("Pipeline '%s' drained", name)
            except Exception as exc:
                logger.error("Error draining pipeline '%s': %s", name, exc)
        for name, p in pipeline_instances.items():
            try:
                p.stop()
            except Exception as exc:
                logger.error("Error stopping pipeline '%s': %s", name, exc)
        logger.info("All pipelines stopped")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    for p in pipeline_instances.values():
        p.start()

    logger.info("All pipelines started — press Ctrl+C to stop")

    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
