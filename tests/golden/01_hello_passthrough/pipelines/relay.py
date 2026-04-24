from datetime import datetime
import json
import math
import re
import time
import threading
import logging
import statistics
import random
from collections import defaultdict

from commlib.transports.mqtt import (
    ConnectionParameters as SourceConnParams,
    Subscriber,
)
from commlib.transports.mqtt import (
    ConnectionParameters as SinkConnParams,
    Publisher,
)

logger = logging.getLogger("sigil.pipeline.relay")

_MAX_PUBLISH_RETRIES = 3
_PUBLISH_RETRY_BASE = 0.1


def _get_nested(d, *keys):
    for k in keys:
        if isinstance(d, dict):
            d = d.get(k)
        else:
            return None
    return d


def _coalesce(value, default):
    return value if value is not None else default


class RelayPipeline:

    def __init__(self):
        self.name = "relay"
        self._running = False
        self._lock = threading.Lock()
        self._downstream_listeners = []

        source_conn = SourceConnParams(
            host="localhost",
            port=1883,
        )

        self._subscriber = Subscriber(
            topic="sensors/temperature",
            on_message=self._on_message,
            conn_params=source_conn,
        )


        sink_conn = SinkConnParams(
            host="localhost",
            port=1883,
        )

        self._publisher = Publisher(
            topic="mirror/temperature",
            conn_params=sink_conn,
        )

    def start(self):
        self._running = True
        self._subscriber.run()
        self._publisher.run()
        time.sleep(0.3)
        logger.info("Pipeline '%s' started", self.name)

    def stop(self, timeout=5):
        self._running = False
        _endpoints = []
        _endpoints.append(self._subscriber)
        _endpoints.append(self._publisher)
        for ep in _endpoints:
            t = threading.Thread(target=ep.stop, daemon=True)
            t.start()
            t.join(timeout=timeout)
        logger.info("Pipeline '%s' stopped", self.name)

    def drain(self):
        pass

    def _on_message(self, msg):
        if not isinstance(msg, dict):
            try:
                msg = json.loads(msg) if isinstance(msg, (str, bytes)) else msg
            except (json.JSONDecodeError, TypeError, ValueError) as exc:
                logger.warning("Invalid message (not JSON dict): %s", exc)
                return
            if not isinstance(msg, dict):
                logger.warning("Invalid message type: %s", type(msg).__name__)
                return











        self._process_immediate(msg)





    def _process_immediate(self, msg):
        try:
            result = msg

            result["_timestamp"] = time.time()
            result["_pipeline"] = self.name

            self._publish_message(result)

        except Exception as exc:
            logger.error(
                "DLQ: failed to process message: %s — %s", exc, msg,
            )


    def _publish_with_retry(self, publisher, payload):
        """Publish with exponential backoff retry."""
        for attempt in range(_MAX_PUBLISH_RETRIES):
            try:
                publisher.publish(payload)
                return
            except Exception as exc:
                if attempt < _MAX_PUBLISH_RETRIES - 1:
                    delay = _PUBLISH_RETRY_BASE * (2 ** attempt) + random.uniform(0, 0.05)
                    logger.warning(
                        "Publish failed (attempt %d/%d): %s — retrying in %.2fs",
                        attempt + 1, _MAX_PUBLISH_RETRIES, exc, delay,
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        "Publish failed after %d attempts: %s", _MAX_PUBLISH_RETRIES, exc,
                    )

    def _notify_downstream(self, msg):
        for listener in self._downstream_listeners:
            listener(msg)


    def _publish_message(self, msg):
        self._publish_with_retry(self._publisher, msg)
        self._notify_downstream(msg)
