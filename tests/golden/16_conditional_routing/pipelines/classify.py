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
from commlib.transports.redis import (
    ConnectionParameters as Route1ConnParams,
    Publisher as Route1Publisher,
)
from commlib.transports.mqtt import (
    ConnectionParameters as Route2ConnParams,
    Publisher as Route2Publisher,
)
from commlib.transports.mqtt import (
    ConnectionParameters as RouteDefaultConnParams,
    Publisher as RouteDefaultPublisher,
)

logger = logging.getLogger("sigil.pipeline.classify")

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


class ClassifyPipeline:

    def __init__(self):
        self.name = "classify"
        self._running = False
        self._lock = threading.Lock()
        self._downstream_listeners = []

        source_conn = SourceConnParams(
            host="localhost",
            port=1883,
        )

        self._subscriber = Subscriber(
            topic="sensors/all",
            on_message=self._on_message,
            conn_params=source_conn,
        )


        self._route_pub_1 = Route1Publisher(
            topic="priority.high",
            conn_params=Route1ConnParams(
                host="redis.local", port=6379,
            ),
        )
        self._route_pub_2 = Route2Publisher(
            topic="priority/medium",
            conn_params=Route2ConnParams(
                host="localhost", port=1883,
            ),
        )
        self._route_pub_default = RouteDefaultPublisher(
            topic="priority/low",
            conn_params=RouteDefaultConnParams(
                host="localhost", port=1883,
            ),
        )

    def start(self):
        self._running = True
        self._subscriber.run()
        self._route_pub_1.run()
        self._route_pub_2.run()
        self._route_pub_default.run()
        time.sleep(0.3)
        self._publish_message({
            "status": 'router_online',
        })
        logger.info("Pipeline '%s' started", self.name)

    def stop(self, timeout=5):
        self._running = False
        self._publish_message({
            "status": 'router_offline',
        })
        _endpoints = []
        _endpoints.append(self._subscriber)
        _endpoints.append(self._route_pub_1)
        _endpoints.append(self._route_pub_2)
        _endpoints.append(self._route_pub_default)
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







        if not ((_get_nested(msg, 'value') is not None)):
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
        if _get_nested(msg, 'value') > 90.0:
            self._publish_with_retry(self._route_pub_1, msg)
        elif _get_nested(msg, 'value') > 50.0:
            self._publish_with_retry(self._route_pub_2, msg)
        else:
            self._publish_with_retry(self._route_pub_default, msg)
        self._notify_downstream(msg)
