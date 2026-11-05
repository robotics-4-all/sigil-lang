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

logger = logging.getLogger("sigil.pipeline.fleet_summary")

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


class FleetSummaryPipeline:

    def __init__(self):
        self.name = "fleet_summary"
        self._running = False
        self._lock = threading.Lock()
        self._downstream_listeners = []
        self._window_data = defaultdict(list)
        self._window_stop_event = threading.Event()
        self._window_thread = None
        self._window_size = 300
        self._MAX_WINDOW_BUFFER = 10000

        source_conn = SourceConnParams(
            host="fleet.example.com",
            port=1883,
            ssl=True,
            username="analytics",
            password="s3cur3-t0k3n",
        )

        self._subscriber = Subscriber(
            topic="trucks/+/telemetry",
            on_message=self._on_message,
            conn_params=source_conn,
        )


        sink_conn = SinkConnParams(
            host="fleet.example.com",
            port=1883,
            ssl=True,
            username="analytics",
            password="s3cur3-t0k3n",
        )

        self._publisher = Publisher(
            topic="analytics/fleet/regional",
            conn_params=sink_conn,
        )

    def start(self):
        self._running = True
        self._subscriber.run()
        self._publisher.run()
        time.sleep(0.3)
        self._schedule_window_flush()
        logger.info("Pipeline '%s' started", self.name)

    def stop(self, timeout=5):
        self._running = False
        self._window_stop_event.set()
        if self._window_thread:
            self._window_thread.join(timeout=timeout)
        _endpoints = []
        _endpoints.append(self._subscriber)
        _endpoints.append(self._publisher)
        for ep in _endpoints:
            t = threading.Thread(target=ep.stop, daemon=True)
            t.start()
            t.join(timeout=timeout)
        logger.info("Pipeline '%s' stopped", self.name)

    def drain(self):
        self._flush_window()
        logger.debug("Window buffer drained")

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







        if not (_get_nested(msg, 'speed_kmh') >= 5.0):
            return




        group_key = "|".join([
            str(_get_nested(msg, 'region')),
        ])
        with self._lock:
            self._window_data[group_key].append(msg)
            _buf = self._window_data[group_key]
            if len(_buf) > self._MAX_WINDOW_BUFFER:
                _dropped = len(_buf) - self._MAX_WINDOW_BUFFER
                del _buf[:_dropped]
                logger.warning(
                    "Backpressure: dropped %d oldest messages for group '%s'",
                    _dropped, group_key,
                )

    def _schedule_window_flush(self):
        if self._window_thread and self._window_thread.is_alive():
            return
        interval = self._window_size
        self._window_stop_event.clear()
        self._window_thread = threading.Thread(
            target=self._window_flush_loop, args=(interval,), daemon=True
        )
        self._window_thread.start()

    def _window_flush_loop(self, interval):
        while not self._window_stop_event.wait(timeout=interval):
            if not self._running:
                break
            self._flush_window()

    def _flush_window(self):
        with self._lock:
            snapshot = dict(self._window_data)
            self._window_data.clear()

        for group_key, messages in snapshot.items():
            if not messages:
                continue
            self._process_window(group_key, messages)

    def _process_window(self, group_key, messages):
        try:
            aggregated = {}
            _vals_avg_speed = [
                v for v in (_get_nested(m, 'speed_kmh') for m in messages)
                if v is not None and isinstance(v, (int, float))
            ]
            aggregated["avg_speed"] = (
                statistics.mean(_vals_avg_speed)
                if _vals_avg_speed else 0.0
            )
            _vals_top_speed = [
                v for v in (_get_nested(m, 'speed_kmh') for m in messages)
                if v is not None and isinstance(v, (int, float))
            ]
            aggregated["top_speed"] = (
                max(_vals_top_speed)
                if _vals_top_speed else 0.0
            )
            _vals_total_fuel = [
                v for v in (_get_nested(m, 'fuel_rate') for m in messages)
                if v is not None and isinstance(v, (int, float))
            ]
            aggregated["total_fuel"] = sum(_vals_total_fuel)
            aggregated["active_trucks"] = len(messages)
            _vals_speed_stddev = [
                v for v in (_get_nested(m, 'speed_kmh') for m in messages)
                if v is not None and isinstance(v, (int, float))
            ]
            aggregated["speed_stddev"] = (
                statistics.stdev(_vals_speed_stddev)
                if len(_vals_speed_stddev) > 1 else 0.0
            )
            aggregated["first_report"] = _get_nested(
                messages[0], 'speed_kmh'
            )
            aggregated["last_report"] = _get_nested(
                messages[-1], 'speed_kmh'
            )



            result = {}
            result["avg_speed"] = aggregated['avg_speed']
            result["top_speed"] = aggregated['top_speed']
            result["fuel_used"] = aggregated['total_fuel'] / 60.0 * 5.0
            result["active_trucks"] = aggregated['active_trucks']
            result["speed_spread"] = aggregated['speed_stddev']

            result["_group"] = group_key
            result["_timestamp"] = time.time()
            result["_pipeline"] = self.name

            self._publish_message(result)
            logger.debug("Published aggregate for group '%s'", group_key)

        except Exception as exc:
            logger.error(
                "DLQ: failed to process message: %s — %s", exc, messages,
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
