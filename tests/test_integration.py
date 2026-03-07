import importlib
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid

import pytest

from sigil.language import parse_string
from sigil.codegen import generate

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
AMQP_HOST = os.environ.get("AMQP_HOST", "localhost")
KAFKA_HOST = os.environ.get("KAFKA_HOST", "localhost")


def _mqtt_available() -> bool:
    try:
        from commlib.transports.mqtt import ConnectionParameters, Publisher

        conn = ConnectionParameters(host=MQTT_HOST, port=1883)
        pub = Publisher(topic="test/ping", conn_params=conn)
        pub.run()
        pub.publish({"ping": 1})
        pub.stop()
        return True
    except Exception:
        return False


def _redis_available() -> bool:
    try:
        import redis

        r = redis.Redis(host=REDIS_HOST, port=6379, socket_connect_timeout=2)
        r.ping()
        r.close()
        return True
    except Exception:
        return False


def _amqp_available() -> bool:
    try:
        import socket

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        s.connect((AMQP_HOST, 5672))
        s.close()
        import commlib.transports.amqp  # noqa: F401 — verify extras installed

        return True
    except Exception:
        return False


def _kafka_available() -> bool:
    try:
        import socket

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        s.connect((KAFKA_HOST, 9092))
        s.close()
        import commlib.transports.kafka  # noqa: F401 — verify extras installed

        return True
    except Exception:
        return False


mqtt_available = pytest.mark.skipif(
    not _mqtt_available(), reason=f"MQTT broker not available on {MQTT_HOST}:1883"
)
redis_available = pytest.mark.skipif(
    not _redis_available(), reason=f"Redis not available on {REDIS_HOST}:6379"
)
amqp_available = pytest.mark.skipif(
    not _amqp_available(), reason=f"AMQP broker not available on {AMQP_HOST}:5672"
)
kafka_available = pytest.mark.skipif(
    not _kafka_available(), reason=f"Kafka not available on {KAFKA_HOST}:9092"
)


def _uid() -> str:
    """Short unique id for topic isolation between tests."""
    return uuid.uuid4().hex[:8]


def _generate_and_import(dsl_src: str, pipeline_name: str):
    """
    Parse DSL source, generate code into a temp dir,
    dynamically import the pipeline module, and return
    (pipeline_instance, tmpdir_path).

    Caller is responsible for cleanup (tmpdir is NOT deleted
    automatically so the generated code remains importable).
    """
    model = parse_string(dsl_src)
    tmpdir = tempfile.mkdtemp(prefix="aggr_integ_")
    generate(model, tmpdir)

    sys.path.insert(0, tmpdir)

    # Force fresh import (avoid cached stale modules)
    mod_name = f"pipelines.{pipeline_name}"
    if mod_name in sys.modules:
        del sys.modules[mod_name]
    if "pipelines" in sys.modules:
        del sys.modules["pipelines"]

    mod = importlib.import_module(mod_name)
    class_name = "".join(part.capitalize() for part in pipeline_name.split("_"))
    cls = getattr(mod, f"{class_name}Pipeline")
    return cls(), tmpdir


def _cleanup_import(tmpdir: str):
    """Remove tmpdir from sys.path."""
    if tmpdir in sys.path:
        sys.path.remove(tmpdir)


def _make_mqtt_pub_sub(source_topic: str, sink_topic: str):
    """
    Create commlib-py MQTT Publisher (to source) and Subscriber (from sink).
    Returns (publisher, subscriber, received_list).
    """
    from commlib.transports.mqtt import (
        ConnectionParameters,
        Publisher,
        Subscriber,
    )

    conn = ConnectionParameters(host=MQTT_HOST, port=1883)
    received = []

    def on_msg(msg):
        received.append(msg)

    pub = Publisher(topic=source_topic, conn_params=conn)
    sub = Subscriber(topic=sink_topic, on_message=on_msg, conn_params=conn)
    return pub, sub, received


def _make_redis_sub(sink_topic: str):
    """
    Create a Redis subscriber via direct redis-py (not commlib)
    to avoid the commlib Redis stop() hang.
    Returns (subscriber_thread, received_list, stop_event).
    """
    import redis
    import json
    import threading

    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    received = []
    stop_event = threading.Event()

    def listener():
        ps = r.pubsub()
        ps.subscribe(sink_topic)
        while not stop_event.is_set():
            msg = ps.get_message(ignore_subscribe_messages=True, timeout=0.1)
            if msg and msg["type"] == "message":
                try:
                    received.append(json.loads(msg["data"]))
                except (json.JSONDecodeError, TypeError):
                    pass
        ps.unsubscribe()
        ps.close()

    t = threading.Thread(target=listener, daemon=True)
    t.start()
    return t, received, stop_event


def _make_redis_pub(source_topic: str):
    """
    Create a Redis publisher via direct redis-py.
    Returns a publish(msg_dict) callable and a close() callable.
    """
    import redis
    import json

    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

    def publish(msg_dict):
        r.publish(source_topic, json.dumps(msg_dict))

    def close():
        r.close()

    return publish, close


def _make_amqp_pub_sub(source_topic: str, sink_topic: str):
    """
    Create commlib-py AMQP Publisher (to source) and Subscriber (from sink).
    Returns (publisher, subscriber, received_list).
    """
    from commlib.transports.amqp import (
        ConnectionParameters,
        Publisher,
        Subscriber,
    )

    conn = ConnectionParameters(host=AMQP_HOST, port=5672)
    received = []

    def on_msg(msg):
        received.append(msg)

    pub = Publisher(topic=source_topic, conn_params=conn)
    sub = Subscriber(topic=sink_topic, on_message=on_msg, conn_params=conn)
    return pub, sub, received


def _run_redis_subprocess(
    tmpdir: str,
    runner_script: str,
    result_file: str,
    timeout: float = 15,
) -> list:
    """
    Run a Redis integration test in a subprocess to work around
    commlib-py Redis transport's stop() hang.

    Returns the list of received messages parsed from result_file.
    Raises AssertionError if the subprocess produces no output.
    """
    proc = subprocess.Popen(
        [sys.executable, "-c", runner_script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )

    try:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if os.path.exists(result_file + ".done"):
                break
            time.sleep(0.3)

        stderr_output = ""
        if proc.poll() is not None:
            _, stderr_bytes = proc.communicate(timeout=2)
            stderr_output = stderr_bytes.decode("utf-8", errors="replace")

        assert os.path.exists(result_file), (
            f"Pipeline produced no output.\nSubprocess stderr:\n{stderr_output}"
        )

        with open(result_file) as f:
            return json.load(f)

    finally:
        proc.kill()
        proc.wait(timeout=5)


def _redis_runner_preamble(tmpdir: str, alarm_secs: int = 15) -> str:
    """Return the common preamble for Redis subprocess runner scripts."""
    return f"""\
import sys, time, json, os, signal, logging
sys.path.insert(0, "{tmpdir}")

# Suppress commlib-py internal logging to work around broken format strings
# in commlib-py 0.13.1 Kafka transport (crashes subscriber/publisher threads).
logging.disable(logging.CRITICAL)

signal.signal(signal.SIGALRM, lambda s, f: os._exit(0))
signal.alarm({alarm_secs})
"""


def _kafka_preamble(topics: list[str]) -> str:
    """Patch commlib-py 0.13.1 Kafka transport and pre-create topics.

    commlib-py 0.13.1 has several bugs affecting Kafka integration:
    1. run_forever raises on UNKNOWN_TOPIC_OR_PART instead of retrying
    2. _on_assign / _on_delivery have broken logging format strings
    3. auto.offset.reset is "end" — consumers miss messages on new topics
    4. producer.flush() is never called — messages may not be delivered
    Pre-creating topics and patching these avoids all issues.  Each
    subscriber also gets a unique group.id to prevent consumer-group
    conflicts between consecutive test runs.
    """
    topics_str = repr(topics)
    return f"""\
import uuid as _uuid
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import commlib.transports.kafka as _kafka_mod

_admin = AdminClient({{"bootstrap.servers": "kafka:9092"}})
_futures = _admin.create_topics([NewTopic(t, num_partitions=1, replication_factor=1) for t in {topics_str}])
for t, f in _futures.items():
    try:
        f.result()
    except Exception:
        pass
time.sleep(1)

_orig_create_conf = _kafka_mod.Subscriber._create_kafka_conf
def _patched_create_conf(self):
    _orig_create_conf(self)
    self._kafka_cfg["auto.offset.reset"] = "earliest"
    self._kafka_cfg["group.id"] = self._kafka_cfg.get("group.id", "main") + "-" + _uuid.uuid4().hex[:8]
_kafka_mod.Subscriber._create_kafka_conf = _patched_create_conf

_orig_run_forever = _kafka_mod.Subscriber.run_forever
def _patched_run_forever(self):
    self._consumer = self._transport.create_consumer(self._kafka_cfg)
    try:
        self._consumer.subscribe([self._topic], on_assign=lambda c, p: None)
        while True:
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() in (KafkaError._PARTITION_EOF,
                                          KafkaError.UNKNOWN_TOPIC_OR_PART):
                    continue
                raise KafkaException(msg.error())
            else:
                self._on_message(msg)
    finally:
        self._consumer.close()
_kafka_mod.Subscriber.run_forever = _patched_run_forever

_orig_publish_data = _kafka_mod.KafkaTransport.publish_data
def _patched_publish_data(self, producer, data, topic, key="", on_delivery=None):
    _orig_publish_data(self, producer, data, topic, key, on_delivery)
    producer.flush(timeout=5)
_kafka_mod.KafkaTransport.publish_data = _patched_publish_data
"""


def _redis_runner_epilogue(result_file: str) -> str:
    """Return the common epilogue for Redis subprocess runner scripts."""
    return f"""\
# Write final results
with open("{result_file}", "w") as f:
    json.dump(received, f)

# Signal done
with open("{result_file}.done", "w") as f:
    f.write("ok")

time.sleep(30)
"""


@mqtt_available
class TestMQTTPassthrough:
    """Source → Sink with no processing stages (MQTT)."""

    def test_message_passes_through(self):
        uid = _uid()
        src_topic = f"test/{uid}/in"
        snk_topic = f"test/{uid}/out"

        dsl = f"""
        broker b
            transport: mqtt
            host: "{MQTT_HOST}"
            port: 1883
        end
        pipeline passthrough
            source from b :: "{src_topic}"
            |> sink to b :: "{snk_topic}"
        end
        """

        pipeline, tmpdir = _generate_and_import(dsl, "passthrough")
        pub, sub, received = _make_mqtt_pub_sub(src_topic, snk_topic)

        try:
            sub.run()
            time.sleep(0.3)
            pipeline.start()
            time.sleep(0.3)
            pub.run()
            time.sleep(0.3)

            pub.publish({"sensor": "t1", "value": 23.5})
            pub.publish({"sensor": "t2", "value": 18.0})

            deadline = time.time() + 5
            while len(received) < 2 and time.time() < deadline:
                time.sleep(0.1)

            assert len(received) >= 2, f"Expected 2 messages, got {len(received)}"

            values = [m["value"] for m in received]
            assert 23.5 in values
            assert 18.0 in values

            for m in received:
                assert "_pipeline" in m
                assert m["_pipeline"] == "passthrough"
                assert "_timestamp" in m

        finally:
            pipeline.stop()
            sub.stop()
            pub.stop()
            _cleanup_import(tmpdir)


@mqtt_available
class TestMQTTFilter:
    """Filter stage: only matching messages pass through."""

    def test_filter_passes_matching_rejects_nonmatching(self):
        uid = _uid()
        src_topic = f"test/{uid}/in"
        snk_topic = f"test/{uid}/out"

        dsl = f"""
        broker b
            transport: mqtt
            host: "{MQTT_HOST}"
            port: 1883
        end
        pipeline filtered
            source from b :: "{src_topic}"
            |> filter msg.value > 20
            |> sink to b :: "{snk_topic}"
        end
        """

        pipeline, tmpdir = _generate_and_import(dsl, "filtered")
        pub, sub, received = _make_mqtt_pub_sub(src_topic, snk_topic)

        try:
            sub.run()
            time.sleep(0.3)
            pipeline.start()
            time.sleep(0.3)
            pub.run()
            time.sleep(0.3)

            pub.publish({"value": 5})  # filtered out (not > 20)
            pub.publish({"value": 25})  # passes
            pub.publish({"value": 10})  # filtered out
            pub.publish({"value": 50})  # passes
            pub.publish({"value": 15})  # filtered out

            deadline = time.time() + 5
            while len(received) < 2 and time.time() < deadline:
                time.sleep(0.1)

            time.sleep(1.0)  # extra wait to confirm no false positives

            assert len(received) == 2, (
                f"Expected 2 messages, got {len(received)}: {received}"
            )

            values = sorted([m["value"] for m in received])
            assert values == [25, 50]

        finally:
            pipeline.stop()
            sub.stop()
            pub.stop()
            _cleanup_import(tmpdir)


@mqtt_available
class TestMQTTWindowAggregate:
    """Tumbling window + aggregate: collect messages, flush aggregated result."""

    def test_tumbling_window_aggregation(self):
        uid = _uid()
        src_topic = f"test/{uid}/in"
        snk_topic = f"test/{uid}/out"

        dsl = f"""
        broker b
            transport: mqtt
            host: "{MQTT_HOST}"
            port: 1883
        end
        pipeline windowed
            source from b :: "{src_topic}"
            |> window tumbling(1 s)
            |> aggregate {{
                avg_val = avg(msg.value)
                total   = sum(msg.value)
                n       = count()
            }}
            |> sink to b :: "{snk_topic}"
        end
        """

        pipeline, tmpdir = _generate_and_import(dsl, "windowed")
        pub, sub, received = _make_mqtt_pub_sub(src_topic, snk_topic)

        try:
            sub.run()
            time.sleep(0.3)
            pipeline.start()
            time.sleep(0.3)
            pub.run()
            time.sleep(0.3)

            for v in [10, 20, 30, 40]:
                pub.publish({"value": v})
                time.sleep(0.05)

            deadline = time.time() + 5
            while len(received) < 1 and time.time() < deadline:
                time.sleep(0.1)

            assert len(received) >= 1, (
                f"Expected at least 1 aggregate, got {len(received)}"
            )

            agg = received[0]
            assert agg["avg_val"] == pytest.approx(25.0)
            assert agg["total"] == 100
            assert agg["n"] == 4
            assert agg["_pipeline"] == "windowed"
            assert "_group" in agg

        finally:
            pipeline.stop()
            sub.stop()
            pub.stop()
            _cleanup_import(tmpdir)


@mqtt_available
class TestMQTTMapStage:
    """Map stage: field renaming in the output."""

    def test_map_renames_fields(self):
        uid = _uid()
        src_topic = f"test/{uid}/in"
        snk_topic = f"test/{uid}/out"

        dsl = f"""
        broker b
            transport: mqtt
            host: "{MQTT_HOST}"
            port: 1883
        end
        pipeline mapped
            source from b :: "{src_topic}"
            |> window tumbling(1 s)
            |> aggregate {{
                avg_val = avg(msg.value)
                n       = count()
            }}
            |> map {{
                average = avg_val;
                samples = n
            }}
            |> sink to b :: "{snk_topic}"
        end
        """

        pipeline, tmpdir = _generate_and_import(dsl, "mapped")
        pub, sub, received = _make_mqtt_pub_sub(src_topic, snk_topic)

        try:
            sub.run()
            time.sleep(0.3)
            pipeline.start()
            time.sleep(0.3)
            pub.run()
            time.sleep(0.3)

            for v in [10, 20, 30]:
                pub.publish({"value": v})
                time.sleep(0.05)

            deadline = time.time() + 5
            while len(received) < 1 and time.time() < deadline:
                time.sleep(0.1)

            assert len(received) >= 1, (
                f"Expected at least 1 result, got {len(received)}"
            )

            result = received[0]
            assert "average" in result
            assert "samples" in result
            assert result["average"] == pytest.approx(20.0)
            assert result["samples"] == 3
            assert "avg_val" not in result
            assert "n" not in result

        finally:
            pipeline.stop()
            sub.stop()
            pub.stop()
            _cleanup_import(tmpdir)


@mqtt_available
class TestMQTTFilterAndAggregate:
    """Combined filter + window + aggregate: filter first, then aggregate."""

    def test_filter_before_aggregation(self):
        uid = _uid()
        src_topic = f"test/{uid}/in"
        snk_topic = f"test/{uid}/out"

        dsl = f"""
        broker b
            transport: mqtt
            host: "{MQTT_HOST}"
            port: 1883
        end
        pipeline combo
            source from b :: "{src_topic}"
            |> filter msg.value > 10
            |> window tumbling(1 s)
            |> aggregate {{
                total = sum(msg.value)
                n     = count()
            }}
            |> sink to b :: "{snk_topic}"
        end
        """

        pipeline, tmpdir = _generate_and_import(dsl, "combo")
        pub, sub, received = _make_mqtt_pub_sub(src_topic, snk_topic)

        try:
            sub.run()
            time.sleep(0.3)
            pipeline.start()
            time.sleep(0.3)
            pub.run()
            time.sleep(0.3)

            for v in [5, 15, 10, 25, 35]:  # only 15, 25, 35 pass filter (> 10)
                pub.publish({"value": v})
                time.sleep(0.05)

            deadline = time.time() + 5
            while len(received) < 1 and time.time() < deadline:
                time.sleep(0.1)

            assert len(received) >= 1
            agg = received[0]
            assert agg["total"] == 75
            assert agg["n"] == 3

        finally:
            pipeline.stop()
            sub.stop()
            pub.stop()
            _cleanup_import(tmpdir)


@mqtt_available
class TestMQTTDedup:
    """Deduplicate stage: only unique messages (by key) pass within TTL."""

    def test_dedup_drops_duplicates_within_ttl(self):
        uid = _uid()
        src_topic = f"test/{uid}/in"
        snk_topic = f"test/{uid}/out"

        dsl = f"""
        broker b
            transport: mqtt
            host: "{MQTT_HOST}"
            port: 1883
        end
        pipeline deduped
            source from b :: "{src_topic}"
            |> deduplicate msg.event_id within 5 s
            |> sink to b :: "{snk_topic}"
        end
        """

        pipeline, tmpdir = _generate_and_import(dsl, "deduped")
        pub, sub, received = _make_mqtt_pub_sub(src_topic, snk_topic)

        try:
            sub.run()
            time.sleep(0.3)
            pipeline.start()
            time.sleep(0.3)
            pub.run()
            time.sleep(0.3)

            # Send 3 messages: two with same event_id, one unique
            pub.publish({"event_id": "aaa", "value": 1})
            time.sleep(0.1)
            pub.publish({"event_id": "aaa", "value": 2})  # duplicate — dropped
            time.sleep(0.1)
            pub.publish({"event_id": "bbb", "value": 3})  # unique — passes

            deadline = time.time() + 5
            while len(received) < 2 and time.time() < deadline:
                time.sleep(0.1)

            time.sleep(1.0)  # extra wait to confirm no false positives

            assert len(received) == 2, (
                f"Expected 2 messages (dedup should drop 1), got {len(received)}: {received}"
            )

            values = sorted([m["value"] for m in received])
            assert values == [1, 3], f"Expected values [1, 3], got {values}"

        finally:
            pipeline.stop()
            sub.stop()
            pub.stop()
            _cleanup_import(tmpdir)

    def test_dedup_allows_after_ttl_expires(self):
        """After TTL expires, same key should pass again."""
        uid = _uid()
        src_topic = f"test/{uid}/in"
        snk_topic = f"test/{uid}/out"

        dsl = f"""
        broker b
            transport: mqtt
            host: "{MQTT_HOST}"
            port: 1883
        end
        pipeline dedup_ttl
            source from b :: "{src_topic}"
            |> deduplicate msg.event_id within 1 s
            |> sink to b :: "{snk_topic}"
        end
        """

        pipeline, tmpdir = _generate_and_import(dsl, "dedup_ttl")
        pub, sub, received = _make_mqtt_pub_sub(src_topic, snk_topic)

        try:
            sub.run()
            time.sleep(0.3)
            pipeline.start()
            time.sleep(0.3)
            pub.run()
            time.sleep(0.3)

            # First message passes
            pub.publish({"event_id": "x", "value": 10})

            deadline = time.time() + 3
            while len(received) < 1 and time.time() < deadline:
                time.sleep(0.1)

            assert len(received) == 1, f"Expected 1 message, got {len(received)}"

            # Wait for TTL to expire (1s TTL + margin)
            time.sleep(1.5)

            # Same key should now pass again
            pub.publish({"event_id": "x", "value": 20})

            deadline = time.time() + 3
            while len(received) < 2 and time.time() < deadline:
                time.sleep(0.1)

            assert len(received) == 2, (
                f"Expected 2 messages after TTL expiry, got {len(received)}"
            )
            assert received[1]["value"] == 20

        finally:
            pipeline.stop()
            sub.stop()
            pub.stop()
            _cleanup_import(tmpdir)


@mqtt_available
class TestMQTTHaving:
    """Having stage: post-aggregate filter on windowed results."""

    def test_having_filters_aggregated_results(self):
        uid = _uid()
        src_topic = f"test/{uid}/in"
        snk_topic = f"test/{uid}/out"

        dsl = f"""
        broker b
            transport: mqtt
            host: "{MQTT_HOST}"
            port: 1883
        end
        pipeline having_test
            source from b :: "{src_topic}"
            |> window tumbling(1 s)
            |> aggregate {{
                total = sum(msg.value)
                n     = count()
            }}
            |> having n > 2
            |> sink to b :: "{snk_topic}"
        end
        """

        pipeline, tmpdir = _generate_and_import(dsl, "having_test")
        pub, sub, received = _make_mqtt_pub_sub(src_topic, snk_topic)

        try:
            sub.run()
            time.sleep(0.3)
            pipeline.start()
            time.sleep(0.3)
            pub.run()
            time.sleep(0.3)

            # Send 4 messages in one window — count=4 > 2, so having passes
            for v in [10, 20, 30, 40]:
                pub.publish({"value": v})
                time.sleep(0.05)

            deadline = time.time() + 5
            while len(received) < 1 and time.time() < deadline:
                time.sleep(0.1)

            assert len(received) >= 1, (
                f"Expected at least 1 aggregate (n=4 > 2), got {len(received)}"
            )

            agg = received[0]
            assert agg["n"] == 4
            assert agg["total"] == 100
            assert agg["_pipeline"] == "having_test"

        finally:
            pipeline.stop()
            sub.stop()
            pub.stop()
            _cleanup_import(tmpdir)

    def test_having_suppresses_when_condition_false(self):
        """When having condition is false, no output should be published."""
        uid = _uid()
        src_topic = f"test/{uid}/in"
        snk_topic = f"test/{uid}/out"

        dsl = f"""
        broker b
            transport: mqtt
            host: "{MQTT_HOST}"
            port: 1883
        end
        pipeline having_suppress
            source from b :: "{src_topic}"
            |> window tumbling(1 s)
            |> aggregate {{
                n = count()
            }}
            |> having n > 100
            |> sink to b :: "{snk_topic}"
        end
        """

        pipeline, tmpdir = _generate_and_import(dsl, "having_suppress")
        pub, sub, received = _make_mqtt_pub_sub(src_topic, snk_topic)

        try:
            sub.run()
            time.sleep(0.3)
            pipeline.start()
            time.sleep(0.3)
            pub.run()
            time.sleep(0.3)

            # Send only 3 messages — count=3, having n > 100 is false
            for v in [1, 2, 3]:
                pub.publish({"value": v})
                time.sleep(0.05)

            # Wait for window to flush + extra time
            time.sleep(3.0)

            assert len(received) == 0, (
                f"Expected 0 messages (having n>100 should suppress), got {len(received)}: {received}"
            )

        finally:
            pipeline.stop()
            sub.stop()
            pub.stop()
            _cleanup_import(tmpdir)


@mqtt_available
class TestMQTTConst:
    """Const definitions: constants inlined in filter expressions."""

    def test_const_in_filter(self):
        uid = _uid()
        src_topic = f"test/{uid}/in"
        snk_topic = f"test/{uid}/out"

        dsl = f"""
        broker b
            transport: mqtt
            host: "{MQTT_HOST}"
            port: 1883
        end
        const THRESHOLD = 20
        pipeline const_filter
            source from b :: "{src_topic}"
            |> filter msg.value > THRESHOLD
            |> sink to b :: "{snk_topic}"
        end
        """

        pipeline, tmpdir = _generate_and_import(dsl, "const_filter")
        pub, sub, received = _make_mqtt_pub_sub(src_topic, snk_topic)

        try:
            sub.run()
            time.sleep(0.3)
            pipeline.start()
            time.sleep(0.3)
            pub.run()
            time.sleep(0.3)

            pub.publish({"value": 10})  # below THRESHOLD — filtered
            pub.publish({"value": 25})  # above THRESHOLD — passes
            pub.publish({"value": 15})  # below — filtered
            pub.publish({"value": 50})  # above — passes

            deadline = time.time() + 5
            while len(received) < 2 and time.time() < deadline:
                time.sleep(0.1)

            time.sleep(1.0)  # extra wait to confirm no false positives

            assert len(received) == 2, (
                f"Expected 2 messages (const THRESHOLD=20), got {len(received)}: {received}"
            )

            values = sorted([m["value"] for m in received])
            assert values == [25, 50]

        finally:
            pipeline.stop()
            sub.stop()
            pub.stop()
            _cleanup_import(tmpdir)


@mqtt_available
class TestMQTTLetBindings:
    """Let-bindings in map stage: computed intermediate values."""

    def test_let_binding_computes_ratio(self):
        uid = _uid()
        src_topic = f"test/{uid}/in"
        snk_topic = f"test/{uid}/out"

        dsl = f"""
        broker b
            transport: mqtt
            host: "{MQTT_HOST}"
            port: 1883
        end
        pipeline let_test
            source from b :: "{src_topic}"
            |> window tumbling(1 s)
            |> aggregate {{
                avg_val = avg(msg.value)
                total   = sum(msg.value)
            }}
            |> map {{
                let ratio = avg_val / total;
                result = ratio
            }}
            |> sink to b :: "{snk_topic}"
        end
        """

        pipeline, tmpdir = _generate_and_import(dsl, "let_test")
        pub, sub, received = _make_mqtt_pub_sub(src_topic, snk_topic)

        try:
            sub.run()
            time.sleep(0.3)
            pipeline.start()
            time.sleep(0.3)
            pub.run()
            time.sleep(0.3)

            # Send values: [10, 20, 30] → avg=20, total=60, ratio=20/60=0.333...
            for v in [10, 20, 30]:
                pub.publish({"value": v})
                time.sleep(0.05)

            deadline = time.time() + 5
            while len(received) < 1 and time.time() < deadline:
                time.sleep(0.1)

            assert len(received) >= 1, (
                f"Expected at least 1 result, got {len(received)}"
            )

            result = received[0]
            assert "result" in result, f"Missing 'result' key in output: {result}"
            assert result["result"] == pytest.approx(20.0 / 60.0, rel=1e-3)
            # Map replaces aggregated keys — only mapped fields + metadata
            assert "avg_val" not in result
            assert "total" not in result
            assert "_pipeline" in result
            assert result["_pipeline"] == "let_test"

        finally:
            pipeline.stop()
            sub.stop()
            pub.stop()
            _cleanup_import(tmpdir)


@mqtt_available
class TestMQTTMultiPipeline:
    """Two pipelines in same model with different filters on same source."""

    def test_two_pipelines_independent_output(self):
        uid = _uid()
        src_topic = f"test/{uid}/in"
        snk_topic_high = f"test/{uid}/high"
        snk_topic_low = f"test/{uid}/low"

        dsl = f"""
        broker b
            transport: mqtt
            host: "{MQTT_HOST}"
            port: 1883
        end
        pipeline high_values
            source from b :: "{src_topic}"
            |> filter msg.value >= 50
            |> sink to b :: "{snk_topic_high}"
        end
        pipeline low_values
            source from b :: "{src_topic}"
            |> filter msg.value < 50
            |> sink to b :: "{snk_topic_low}"
        end
        """

        model = parse_string(dsl)
        tmpdir = tempfile.mkdtemp(prefix="aggr_integ_multi_")
        generate(model, tmpdir)

        sys.path.insert(0, tmpdir)

        # Force fresh imports
        for mod_name in ["pipelines", "pipelines.high_values", "pipelines.low_values"]:
            if mod_name in sys.modules:
                del sys.modules[mod_name]

        mod_high = importlib.import_module("pipelines.high_values")
        mod_low = importlib.import_module("pipelines.low_values")
        pipeline_high = mod_high.HighValuesPipeline()
        pipeline_low = mod_low.LowValuesPipeline()

        from commlib.transports.mqtt import (
            ConnectionParameters,
            Publisher,
            Subscriber,
        )

        conn = ConnectionParameters(host=MQTT_HOST, port=1883)
        received_high = []
        received_low = []

        sub_high = Subscriber(
            topic=snk_topic_high,
            on_message=lambda msg: received_high.append(msg),
            conn_params=conn,
        )
        sub_low = Subscriber(
            topic=snk_topic_low,
            on_message=lambda msg: received_low.append(msg),
            conn_params=conn,
        )
        pub = Publisher(topic=src_topic, conn_params=conn)

        try:
            sub_high.run()
            sub_low.run()
            time.sleep(0.3)
            pipeline_high.start()
            pipeline_low.start()
            time.sleep(0.3)
            pub.run()
            time.sleep(0.3)

            pub.publish({"value": 10})  # low
            pub.publish({"value": 75})  # high
            pub.publish({"value": 30})  # low
            pub.publish({"value": 90})  # high
            pub.publish({"value": 50})  # high (>= 50)

            deadline = time.time() + 5
            while (
                len(received_high) < 3 or len(received_low) < 2
            ) and time.time() < deadline:
                time.sleep(0.1)

            time.sleep(1.0)  # extra wait for stragglers

            assert len(received_high) == 3, (
                f"Expected 3 high messages, got {len(received_high)}: {received_high}"
            )
            assert len(received_low) == 2, (
                f"Expected 2 low messages, got {len(received_low)}: {received_low}"
            )

            high_values = sorted([m["value"] for m in received_high])
            low_values = sorted([m["value"] for m in received_low])
            assert high_values == [50, 75, 90]
            assert low_values == [10, 30]

            # Verify pipeline names in metadata
            for m in received_high:
                assert m["_pipeline"] == "high_values"
            for m in received_low:
                assert m["_pipeline"] == "low_values"

        finally:
            pipeline_high.stop()
            pipeline_low.stop()
            sub_high.stop()
            sub_low.stop()
            pub.stop()
            _cleanup_import(tmpdir)


@redis_available
class TestRedisPassthrough:
    """Source → Sink with no processing stages (Redis transport).

    Uses subprocess isolation because commlib-py Redis transport's
    stop() method hangs. The generated pipeline runs in a subprocess
    that is killed after verification.
    """

    def test_message_passes_through_redis(self):
        uid = _uid()
        src_topic = f"test.{uid}.in"
        snk_topic = f"test.{uid}.out"

        dsl = f"""
        broker b
            transport: redis
            host: "{REDIS_HOST}"
            port: 6379
        end
        pipeline redis_pass
            source from b :: "{src_topic}"
            |> sink to b :: "{snk_topic}"
        end
        """

        model = parse_string(dsl)
        tmpdir = tempfile.mkdtemp(prefix="aggr_integ_redis_")
        generate(model, tmpdir)

        result_file = os.path.join(tmpdir, "results.json")

        runner_script = (
            _redis_runner_preamble(tmpdir)
            + f"""
from commlib.transports.redis import ConnectionParameters, Subscriber as RSub, Publisher as RPub

conn = ConnectionParameters(host="{REDIS_HOST}", port=6379)

import importlib
mod = importlib.import_module("pipelines.redis_pass")
pipeline = mod.RedisPassPipeline()

received = []
def on_sink(msg):
    received.append(msg)
    with open("{result_file}", "w") as f:
        json.dump(received, f)

sink_sub = RSub(topic="{snk_topic}", on_message=on_sink, conn_params=conn)
sink_sub.run()
time.sleep(0.5)

pipeline.start()
time.sleep(0.5)

src_pub = RPub(topic="{src_topic}", conn_params=conn)
src_pub.run()
time.sleep(0.3)

src_pub.publish({{"sensor": "t1", "value": 42}})
src_pub.publish({{"sensor": "t2", "value": 99}})
time.sleep(3)

"""
            + _redis_runner_epilogue(result_file)
        )

        received = _run_redis_subprocess(tmpdir, runner_script, result_file)

        assert len(received) >= 2, f"Expected 2 messages, got {len(received)}"
        values = [m["value"] for m in received]
        assert 42 in values
        assert 99 in values

    def test_redis_filter(self):
        uid = _uid()
        src_topic = f"test.{uid}.in"
        snk_topic = f"test.{uid}.out"

        dsl = f"""
        broker b
            transport: redis
            host: "{REDIS_HOST}"
            port: 6379
        end
        pipeline redis_filt
            source from b :: "{src_topic}"
            |> filter msg.value > 50
            |> sink to b :: "{snk_topic}"
        end
        """

        model = parse_string(dsl)
        tmpdir = tempfile.mkdtemp(prefix="aggr_integ_redis_filt_")
        generate(model, tmpdir)

        result_file = os.path.join(tmpdir, "results.json")

        runner_script = (
            _redis_runner_preamble(tmpdir)
            + f"""
from commlib.transports.redis import ConnectionParameters, Subscriber as RSub, Publisher as RPub

conn = ConnectionParameters(host="{REDIS_HOST}", port=6379)

import importlib
mod = importlib.import_module("pipelines.redis_filt")
pipeline = mod.RedisFiltPipeline()

received = []
def on_sink(msg):
    received.append(msg)
    with open("{result_file}", "w") as f:
        json.dump(received, f)

sink_sub = RSub(topic="{snk_topic}", on_message=on_sink, conn_params=conn)
sink_sub.run()
time.sleep(0.5)

pipeline.start()
time.sleep(0.5)

src_pub = RPub(topic="{src_topic}", conn_params=conn)
src_pub.run()
time.sleep(0.3)

src_pub.publish({{"sensor": "s1", "value": 30}})
src_pub.publish({{"sensor": "s2", "value": 80}})
src_pub.publish({{"sensor": "s3", "value": 10}})
src_pub.publish({{"sensor": "s4", "value": 99}})
time.sleep(3)

"""
            + _redis_runner_epilogue(result_file)
        )

        received = _run_redis_subprocess(tmpdir, runner_script, result_file)

        assert len(received) >= 2, f"Expected 2 filtered messages, got {len(received)}"
        values = [m["value"] for m in received]
        assert 80 in values
        assert 99 in values
        assert 30 not in values
        assert 10 not in values


@amqp_available
class TestAMQPPassthrough:
    def test_message_passes_through_amqp(self):
        uid = _uid()
        src_topic = f"test.{uid}.in"
        snk_topic = f"test.{uid}.out"

        dsl = f"""
        broker b
            transport: amqp
            host: "{AMQP_HOST}"
            port: 5672
        end
        pipeline amqp_pass
            source from b :: "{src_topic}"
            |> sink to b :: "{snk_topic}"
        end
        """

        pipeline, tmpdir = _generate_and_import(dsl, "amqp_pass")
        pub, sub, received = _make_amqp_pub_sub(src_topic, snk_topic)

        try:
            sub.run()
            time.sleep(0.5)
            pipeline.start()
            time.sleep(0.5)
            pub.run()
            time.sleep(0.3)

            pub.publish({"sensor": "a1", "value": 42})
            pub.publish({"sensor": "a2", "value": 99})

            deadline = time.time() + 5
            while len(received) < 2 and time.time() < deadline:
                time.sleep(0.1)

            assert len(received) >= 2, f"Expected 2 messages, got {len(received)}"
            values = [m["value"] for m in received]
            assert 42 in values
            assert 99 in values

        finally:
            pipeline.stop()
            sub.stop()
            pub.stop()
            _cleanup_import(tmpdir)


@amqp_available
class TestAMQPFilter:
    def test_filter_passes_matching_rejects_nonmatching_amqp(self):
        uid = _uid()
        src_topic = f"test.{uid}.in"
        snk_topic = f"test.{uid}.out"

        dsl = f"""
        broker b
            transport: amqp
            host: "{AMQP_HOST}"
            port: 5672
        end
        pipeline amqp_filt
            source from b :: "{src_topic}"
            |> filter msg.value > 50
            |> sink to b :: "{snk_topic}"
        end
        """

        pipeline, tmpdir = _generate_and_import(dsl, "amqp_filt")
        pub, sub, received = _make_amqp_pub_sub(src_topic, snk_topic)

        try:
            sub.run()
            time.sleep(0.5)
            pipeline.start()
            time.sleep(0.5)
            pub.run()
            time.sleep(0.3)

            pub.publish({"sensor": "s1", "value": 30})
            pub.publish({"sensor": "s2", "value": 80})
            pub.publish({"sensor": "s3", "value": 10})
            pub.publish({"sensor": "s4", "value": 99})

            deadline = time.time() + 5
            while len(received) < 2 and time.time() < deadline:
                time.sleep(0.1)

            time.sleep(1.0)

            assert len(received) == 2, (
                f"Expected 2 messages, got {len(received)}: {received}"
            )

            values = sorted([m["value"] for m in received])
            assert values == [80, 99]

        finally:
            pipeline.stop()
            sub.stop()
            pub.stop()
            _cleanup_import(tmpdir)


@mqtt_available
@redis_available
class TestCrossTransportMQTTToRedis:
    """Source from MQTT → Sink to Redis (cross-transport pipeline)."""

    def test_mqtt_source_redis_sink(self):
        uid = _uid()
        src_topic = f"test/{uid}/in"
        snk_topic = f"test.{uid}.out"

        dsl = f"""
        broker mqtt_broker
            transport: mqtt
            host: "{MQTT_HOST}"
            port: 1883
        end
        broker redis_broker
            transport: redis
            host: "{REDIS_HOST}"
            port: 6379
        end
        pipeline bridge
            source from mqtt_broker :: "{src_topic}"
            |> sink to redis_broker :: "{snk_topic}"
        end
        """

        model = parse_string(dsl)
        tmpdir = tempfile.mkdtemp(prefix="aggr_integ_cross_")
        generate(model, tmpdir)

        result_file = os.path.join(tmpdir, "results.json")

        runner_script = (
            _redis_runner_preamble(tmpdir)
            + f"""
import importlib
mod = importlib.import_module("pipelines.bridge")
pipeline = mod.BridgePipeline()

import redis, threading
r = redis.Redis(host="{REDIS_HOST}", port=6379, decode_responses=True)
received = []

def listener():
    ps = r.pubsub()
    ps.subscribe("{snk_topic}")
    while True:
        msg = ps.get_message(ignore_subscribe_messages=True, timeout=0.1)
        if msg and msg["type"] == "message":
            try:
                received.append(json.loads(msg["data"]))
                with open("{result_file}", "w") as f:
                    json.dump(received, f)
            except Exception:
                pass

t = threading.Thread(target=listener, daemon=True)
t.start()
time.sleep(0.5)

pipeline.start()
time.sleep(0.5)

from commlib.transports.mqtt import ConnectionParameters, Publisher
mqtt_conn = ConnectionParameters(host="{MQTT_HOST}", port=1883)
pub = Publisher(topic="{src_topic}", conn_params=mqtt_conn)
pub.run()
time.sleep(0.3)

pub.publish({{"sensor": "cross", "value": 77}})
pub.publish({{"sensor": "cross", "value": 88}})
time.sleep(3)

pub.stop()

"""
            + _redis_runner_epilogue(result_file)
        )

        received = _run_redis_subprocess(tmpdir, runner_script, result_file)

        assert len(received) >= 2, f"Expected 2 messages, got {len(received)}"
        values = [m["value"] for m in received]
        assert 77 in values
        assert 88 in values

        for m in received:
            assert m["_pipeline"] == "bridge"


@kafka_available
class TestKafkaPassthrough:
    def test_message_passes_through_kafka(self):
        uid = _uid()
        src_topic = f"test.{uid}.in"
        snk_topic = f"test.{uid}.out"
        group_id = f"test-group-{uid}"

        dsl = f"""
        broker b
            transport: kafka
            host: "{KAFKA_HOST}"
            port: 9092
        end
        pipeline kafka_pass
            source from b :: "{src_topic}"
            |> sink to b :: "{snk_topic}"
        end
        """

        model = parse_string(dsl)
        tmpdir = tempfile.mkdtemp(prefix="aggr_integ_kafka_")
        generate(model, tmpdir)

        result_file = os.path.join(tmpdir, "results.json")

        runner_script = (
            _redis_runner_preamble(tmpdir, alarm_secs=25)
            + _kafka_preamble([src_topic, snk_topic])
            + f"""
from commlib.transports.kafka import ConnectionParameters, Subscriber as KSub, Publisher as KPub

conn = ConnectionParameters(host="{KAFKA_HOST}", port=9092)

import importlib
mod = importlib.import_module("pipelines.kafka_pass")
pipeline = mod.KafkaPassPipeline()

received = []
def on_sink(msg):
    received.append(msg)
    with open("{result_file}", "w") as f:
        json.dump(received, f)

sink_conn = ConnectionParameters(host="{KAFKA_HOST}", port=9092, group="{group_id}")
sink_sub = KSub(topic="{snk_topic}", on_message=on_sink, conn_params=sink_conn)
sink_sub.run()
time.sleep(1.0)

pipeline.start()
time.sleep(1.0)

src_pub = KPub(topic="{src_topic}", conn_params=conn)
src_pub.run()
time.sleep(0.5)

src_pub.publish({{"sensor": "k1", "value": 42}})
src_pub.publish({{"sensor": "k2", "value": 99}})
time.sleep(5)

"""
            + _redis_runner_epilogue(result_file)
        )

        received = _run_redis_subprocess(tmpdir, runner_script, result_file, timeout=20)

        assert len(received) >= 2, f"Expected 2 messages, got {len(received)}"
        values = [m["value"] for m in received]
        assert 42 in values
        assert 99 in values


@kafka_available
class TestKafkaFilter:
    def test_filter_passes_matching_rejects_nonmatching_kafka(self):
        uid = _uid()
        src_topic = f"test.{uid}.in"
        snk_topic = f"test.{uid}.out"
        group_id = f"test-group-{uid}"

        dsl = f"""
        broker b
            transport: kafka
            host: "{KAFKA_HOST}"
            port: 9092
        end
        pipeline kafka_filt
            source from b :: "{src_topic}"
            |> filter msg.value > 50
            |> sink to b :: "{snk_topic}"
        end
        """

        model = parse_string(dsl)
        tmpdir = tempfile.mkdtemp(prefix="aggr_integ_kafka_filt_")
        generate(model, tmpdir)

        result_file = os.path.join(tmpdir, "results.json")

        runner_script = (
            _redis_runner_preamble(tmpdir, alarm_secs=30)
            + _kafka_preamble([src_topic, snk_topic])
            + f"""
from commlib.transports.kafka import ConnectionParameters, Subscriber as KSub, Publisher as KPub

conn = ConnectionParameters(host="{KAFKA_HOST}", port=9092)

import importlib
mod = importlib.import_module("pipelines.kafka_filt")
pipeline = mod.KafkaFiltPipeline()

received = []
def on_sink(msg):
    received.append(msg)
    with open("{result_file}", "w") as f:
        json.dump(received, f)

sink_conn = ConnectionParameters(host="{KAFKA_HOST}", port=9092, group="{group_id}")
sink_sub = KSub(topic="{snk_topic}", on_message=on_sink, conn_params=sink_conn)
sink_sub.run()
time.sleep(2.0)

pipeline.start()
time.sleep(3.0)

src_pub = KPub(topic="{src_topic}", conn_params=conn)
src_pub.run()
time.sleep(1.0)

src_pub.publish({{"sensor": "s1", "value": 30}})
src_pub.publish({{"sensor": "s2", "value": 80}})
src_pub.publish({{"sensor": "s3", "value": 10}})
src_pub.publish({{"sensor": "s4", "value": 99}})
time.sleep(10)

"""
            + _redis_runner_epilogue(result_file)
        )

        received = _run_redis_subprocess(tmpdir, runner_script, result_file, timeout=30)

        assert len(received) >= 2, f"Expected 2 filtered messages, got {len(received)}"
        values = [m["value"] for m in received]
        assert 80 in values
        assert 99 in values
        assert 30 not in values
        assert 10 not in values


# ---------------------------------------------------------------------------
# New stage integration tests (flatten, distinct, throttle, sample, retry,
# multi-window) — all MQTT-based.
# ---------------------------------------------------------------------------


@mqtt_available
class TestMQTTFlatten:
    """Flatten stage: expand array field into individual messages."""

    def test_flatten_expands_array(self):
        uid = _uid()
        src_topic = f"test/{uid}/in"
        snk_topic = f"test/{uid}/out"

        dsl = f"""
        broker b
            transport: mqtt
            host: "{MQTT_HOST}"
            port: 1883
        end
        pipeline flat
            source from b :: "{src_topic}"
            |> flatten msg.items
            |> sink to b :: "{snk_topic}"
        end
        """

        pipeline, tmpdir = _generate_and_import(dsl, "flat")
        pub, sub, received = _make_mqtt_pub_sub(src_topic, snk_topic)

        try:
            sub.run()
            time.sleep(0.3)
            pipeline.start()
            time.sleep(0.3)
            pub.run()
            time.sleep(0.3)

            # Each array element becomes a separate message
            pub.publish({"sensor": "s1", "items": [{"x": 1}, {"x": 2}, {"x": 3}]})

            deadline = time.time() + 5
            while len(received) < 3 and time.time() < deadline:
                time.sleep(0.1)

            assert len(received) == 3, (
                f"Expected 3 flattened messages, got {len(received)}: {received}"
            )

            x_values = sorted([m["x"] for m in received])
            assert x_values == [1, 2, 3]

            for m in received:
                assert "_pipeline" in m
                assert m["_pipeline"] == "flat"
                assert "_timestamp" in m

        finally:
            pipeline.stop()
            sub.stop()
            pub.stop()
            _cleanup_import(tmpdir)

    def test_flatten_non_array_passes_through(self):
        """When field is not an array, message passes through unchanged."""
        uid = _uid()
        src_topic = f"test/{uid}/in"
        snk_topic = f"test/{uid}/out"

        dsl = f"""
        broker b
            transport: mqtt
            host: "{MQTT_HOST}"
            port: 1883
        end
        pipeline flat_pass
            source from b :: "{src_topic}"
            |> flatten msg.items
            |> sink to b :: "{snk_topic}"
        end
        """

        pipeline, tmpdir = _generate_and_import(dsl, "flat_pass")
        pub, sub, received = _make_mqtt_pub_sub(src_topic, snk_topic)

        try:
            sub.run()
            time.sleep(0.3)
            pipeline.start()
            time.sleep(0.3)
            pub.run()
            time.sleep(0.3)

            # items is not an array — original message passes through
            pub.publish({"sensor": "s1", "items": "not_an_array", "value": 42})

            deadline = time.time() + 5
            while len(received) < 1 and time.time() < deadline:
                time.sleep(0.1)

            assert len(received) == 1, (
                f"Expected 1 passthrough message, got {len(received)}: {received}"
            )
            assert received[0]["value"] == 42

        finally:
            pipeline.stop()
            sub.stop()
            pub.stop()
            _cleanup_import(tmpdir)


@mqtt_available
class TestMQTTDistinct:
    """Distinct stage: drop duplicate values by field within TTL."""

    def test_distinct_drops_duplicates_within_ttl(self):
        uid = _uid()
        src_topic = f"test/{uid}/in"
        snk_topic = f"test/{uid}/out"

        dsl = f"""
        broker b
            transport: mqtt
            host: "{MQTT_HOST}"
            port: 1883
        end
        pipeline dist
            source from b :: "{src_topic}"
            |> distinct msg.sensor_id within 3 s
            |> sink to b :: "{snk_topic}"
        end
        """

        pipeline, tmpdir = _generate_and_import(dsl, "dist")
        pub, sub, received = _make_mqtt_pub_sub(src_topic, snk_topic)

        try:
            sub.run()
            time.sleep(0.3)
            pipeline.start()
            time.sleep(0.3)
            pub.run()
            time.sleep(0.3)

            pub.publish({"sensor_id": "A", "value": 1})  # passes (first A)
            time.sleep(0.1)
            pub.publish({"sensor_id": "A", "value": 2})  # dropped (duplicate A)
            time.sleep(0.1)
            pub.publish({"sensor_id": "B", "value": 3})  # passes (first B)
            time.sleep(0.1)
            pub.publish({"sensor_id": "C", "value": 4})  # passes (first C)

            deadline = time.time() + 5
            while len(received) < 3 and time.time() < deadline:
                time.sleep(0.1)

            time.sleep(1.0)  # extra wait to confirm no false positives

            assert len(received) == 3, (
                f"Expected 3 messages (distinct drops 1 duplicate), got {len(received)}: {received}"
            )

            ids = sorted([m["sensor_id"] for m in received])
            assert ids == ["A", "B", "C"]

            # The A message should have value 1 (the first one)
            a_msg = [m for m in received if m["sensor_id"] == "A"][0]
            assert a_msg["value"] == 1

        finally:
            pipeline.stop()
            sub.stop()
            pub.stop()
            _cleanup_import(tmpdir)


@mqtt_available
class TestMQTTThrottle:
    """Throttle stage: token-bucket rate limiting."""

    def test_throttle_limits_throughput(self):
        uid = _uid()
        src_topic = f"test/{uid}/in"
        snk_topic = f"test/{uid}/out"

        dsl = f"""
        broker b
            transport: mqtt
            host: "{MQTT_HOST}"
            port: 1883
        end
        pipeline throttled
            source from b :: "{src_topic}"
            |> throttle 2 per 1 s
            |> sink to b :: "{snk_topic}"
        end
        """

        pipeline, tmpdir = _generate_and_import(dsl, "throttled")
        pub, sub, received = _make_mqtt_pub_sub(src_topic, snk_topic)

        try:
            sub.run()
            time.sleep(0.3)
            pipeline.start()
            time.sleep(0.3)
            pub.run()
            time.sleep(0.3)

            # Send 10 messages rapidly — only ~2 should pass per second
            for i in range(10):
                pub.publish({"value": i})
                time.sleep(0.02)

            # Wait for messages to arrive
            time.sleep(2.0)

            # Token bucket starts with `limit` tokens, so first burst allows
            # ~2 messages, then refills at 2/s. Over 2s total we'd expect ~4-6.
            # Use wide tolerance for timing variability.
            assert len(received) <= 8, (
                f"Expected throttle to limit output, but got {len(received)} messages"
            )
            assert len(received) >= 1, (
                f"Expected at least 1 message to pass throttle, got {len(received)}"
            )

        finally:
            pipeline.stop()
            sub.stop()
            pub.stop()
            _cleanup_import(tmpdir)


@mqtt_available
class TestMQTTSample:
    """Sample stage: probabilistic message filtering."""

    def test_sample_filters_probabilistically(self):
        uid = _uid()
        src_topic = f"test/{uid}/in"
        snk_topic = f"test/{uid}/out"

        dsl = f"""
        broker b
            transport: mqtt
            host: "{MQTT_HOST}"
            port: 1883
        end
        pipeline sampled
            source from b :: "{src_topic}"
            |> sample 50%
            |> sink to b :: "{snk_topic}"
        end
        """

        pipeline, tmpdir = _generate_and_import(dsl, "sampled")
        pub, sub, received = _make_mqtt_pub_sub(src_topic, snk_topic)

        try:
            sub.run()
            time.sleep(0.3)
            pipeline.start()
            time.sleep(0.3)
            pub.run()
            time.sleep(0.3)

            # Send 200 messages to get statistically meaningful results
            for i in range(200):
                pub.publish({"value": i})
                time.sleep(0.01)

            # Wait for all messages to be processed
            time.sleep(3.0)

            # 50% sample with wide tolerance for randomness (20%-80% of 200)
            assert len(received) >= 40, (
                f"Sample 50%: expected at least 40/200 messages, got {len(received)}"
            )
            assert len(received) <= 160, (
                f"Sample 50%: expected at most 160/200 messages, got {len(received)}"
            )

        finally:
            pipeline.stop()
            sub.stop()
            pub.stop()
            _cleanup_import(tmpdir)


@mqtt_available
class TestMQTTRetryPolicy:
    """Retry policy: verify config doesn't break normal operation."""

    def test_retry_policy_normal_operation(self):
        uid = _uid()
        src_topic = f"test/{uid}/in"
        snk_topic = f"test/{uid}/out"

        dsl = f"""
        broker b
            transport: mqtt
            host: "{MQTT_HOST}"
            port: 1883
        end
        pipeline retry_test
            source from b :: "{src_topic}"
            |> filter msg.value > 10
            |> sink to b :: "{snk_topic}"
            retry: 3 with backoff 200 ms
        end
        """

        pipeline, tmpdir = _generate_and_import(dsl, "retry_test")
        pub, sub, received = _make_mqtt_pub_sub(src_topic, snk_topic)

        try:
            sub.run()
            time.sleep(0.3)
            pipeline.start()
            time.sleep(0.3)
            pub.run()
            time.sleep(0.3)

            pub.publish({"value": 5})  # filtered out
            pub.publish({"value": 25})  # passes
            pub.publish({"value": 50})  # passes

            deadline = time.time() + 5
            while len(received) < 2 and time.time() < deadline:
                time.sleep(0.1)

            time.sleep(1.0)

            assert len(received) == 2, (
                f"Expected 2 messages with retry policy active, got {len(received)}: {received}"
            )

            values = sorted([m["value"] for m in received])
            assert values == [25, 50]

            for m in received:
                assert m["_pipeline"] == "retry_test"

        finally:
            pipeline.stop()
            sub.stop()
            pub.stop()
            _cleanup_import(tmpdir)


@mqtt_available
class TestMQTTMultiWindow:
    """Named multi-window two windows with different sizes produce separate outputs."""

    def test_multi_window_produces_tagged_results(self):
        uid = _uid()
        src_topic = f"test/{uid}/in"
        snk_topic = f"test/{uid}/out"

        dsl = f"""
        broker b
            transport: mqtt
            host: "{MQTT_HOST}"
            port: 1883
        end
        pipeline multi_win
            source from b :: "{src_topic}"
            |> window fast tumbling(1 s)
            |> window slow tumbling(3 s)
            |> aggregate {{
                fast_n = count() over fast
                slow_n = count() over slow
            }}
            |> sink to b :: "{snk_topic}"
        end
        """

        pipeline, tmpdir = _generate_and_import(dsl, "multi_win")
        pub, sub, received = _make_mqtt_pub_sub(src_topic, snk_topic)

        try:
            sub.run()
            time.sleep(0.3)
            pipeline.start()
            time.sleep(0.3)
            pub.run()
            time.sleep(0.3)

            # Send messages over ~2 seconds so fast window flushes at least once
            for i in range(8):
                pub.publish({"value": i})
                time.sleep(0.2)

            # Wait for both windows to flush (slow=3s + margin)
            deadline = time.time() + 6
            while len(received) < 2 and time.time() < deadline:
                time.sleep(0.2)

            assert len(received) >= 2, (
                f"Expected at least 2 window results (fast + slow), got {len(received)}: {received}"
            )

            # Verify _window field is present and has expected values
            window_names = set(m["_window"] for m in received)
            assert "fast" in window_names, (
                f"Expected 'fast' window results, got windows: {window_names}"
            )

            # Fast window results should have fast_n (count from fast window)
            fast_results = [m for m in received if m["_window"] == "fast"]
            assert len(fast_results) >= 1, "Expected at least 1 fast window result"
            for r in fast_results:
                assert "fast_n" in r, f"fast window result missing fast_n: {r}"
                assert r["fast_n"] > 0

            for m in received:
                assert m["_pipeline"] == "multi_win"

        finally:
            pipeline.stop()
            sub.stop()
            pub.stop()
            _cleanup_import(tmpdir)
