<p align="center">
  <img src=".github/assets/sigil_logo.png" alt="Sigil" width="180" />
</p>

<h1 align="center">Sigil</h1>

<p align="center">
  A domain-specific language for building stream-processing aggregation pipelines<br>that compile to standalone Python applications.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-3.10+-blue.svg" alt="Python 3.10+" />
  <img src="https://img.shields.io/badge/license-Apache--2.0-green.svg" alt="License Apache-2.0" />
  <img src="https://img.shields.io/badge/built%20with-textX-orange.svg" alt="Built with textX" />
</p>

<p align="center">
  <img src=".github/assets/sigil_landing.png" alt="Sigil — Stream Processing Pipelines" width="100%" />
</p>

## Features

- Declarative pipeline syntax with pipe operators (`|>`)
- Multi-transport support (MQTT, Redis, AMQP, Kafka) via commlib-py
- Windowed aggregation (tumbling/sliding) with named multi-window
- 17 processing stages: filter, predict, deduplicate, rate limit, flatten, distinct, throttle, sample, group_by, window, aggregate, accumulate, having, map, join, route
- ML inference via `predict` stage (ONNX, scikit-learn, PyTorch, TFLite, custom runtimes)
- Schema declarations with strict/lenient validation
- Error handling with Dead Letter Queue (DLQ) routing
- Pipeline chaining (source from pipe)
- Lifecycle hooks (@initially, @finally)
- Retry policies with exponential backoff
- Constants and environment variable injection
- Conditional routing (fan-out)
- DOT and Mermaid pipeline visualization
- Production-ready generated code: JSON logging, graceful shutdown, message validation

## Quick Start

Install the Sigil compiler:

```bash
pip install -e .
```

Create a simple pipeline in `hello.sigil`:

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline relay
    source from local :: "sensors/temperature"
    |> sink to local :: "mirror/temperature"
end
```

Validate and generate the Python application:

```bash
sigil check hello.sigil
sigil generate hello.sigil -o generated/
```

Run the generated application:

```bash
python generated/main.py
```

## Syntax Overview

### Basic Filter Pipeline
```sigil
pipeline temperature_filter
    source from edge :: "sensors/+/temperature"
    |> filter msg.value > -40 and msg.value < 85
    |> sink to edge :: "filtered/temperature"
end
```

### Window + Aggregate Pipeline
```sigil
pipeline fleet_summary
    source from fleet :: "trucks/+/telemetry"
    |> filter msg.speed_kmh >= 5
    |> group_by msg.region
    |> window tumbling(5 m)
    |> aggregate {
        avg_speed = avg(msg.speed_kmh)
        top_speed = max(msg.speed_kmh)
        active_trucks = count()
    }
    |> sink to fleet :: "analytics/fleet/regional"
end
```

### Stream Join
```sigil
pipeline enrich_orders
    source from local :: "orders/new"
    |> join local :: "customers/profiles"
        on msg.customer_id == other.id
        within 30 s
        type: inner
    end
    |> map {
        order_id = msg.order_id;
        customer_name = other.name ?? "unknown";
        amount = msg.amount
    }
    |> sink to local :: "orders/enriched"
end
```

### ML Inference
```sigil
model anomaly_detector
    source: "models/anomaly.onnx"
    runtime: onnx
    features: [msg.temperature, msg.humidity, msg.pressure]
    returns: [anomaly_score, is_anomaly]
end

pipeline sensor_monitoring
    source from edge :: "sensors/+/data"
    |> filter msg.temperature is not null
    |> predict anomaly_detector
    |> map {
        device_id = msg.device_id;
        score = msg.anomaly_score;
        temperature = msg.temperature
    }
    |> sink to cloud :: "alerts/anomaly"
end
```

### Conditional Routing
```sigil
pipeline classify
    source from local :: "sensors/all"
    |> filter msg.value is not null
    |> route
        when msg.value > 90 => sink to alerts :: "priority.high"
        when msg.value > 50 => sink to local :: "priority/medium"
        default => sink to local :: "priority/low"
    end
end
```

## CLI Reference

| Command | Description | Flags |
|---------|-------------|-------|
| `sigil check` | Validate syntax and semantics | `FILE` |
| `sigil generate` | Compile to Python application | `FILE`, `-o` (output dir) |
| `sigil explain` | Human-readable pipeline description | `FILE` |
| `sigil visualize` | Generate pipeline diagrams | `FILE`, `--format` (dot/mermaid) |

## Pipeline Stages

Sigil enforces a fixed stage order: `source` → `join` → `deduplicate` → `filter`/`predict` → `rate` → `flatten` → `distinct` → `throttle` → `sample` → `group_by` → `window` → `aggregate` → `accumulate` → `having` → `map` → `sink`.

| Stage | Description | Syntax Snippet |
|-------|-------------|----------------|
| `source` | Define message entry point | `source from broker :: "topic"` |
| `join` | Enrich stream with another stream | `|> join broker :: "topic" on ... end` |
| `deduplicate` | Drop duplicates by key within TTL | `|> deduplicate msg.id within 5 s` |
| `filter` | Drop messages by condition | `|> filter msg.val > 10` |
| `predict` | ML model inference | `|> predict my_model` |
| `rate` | Limit message frequency | `|> rate 100 per 1 s` |
| `flatten` | Expand array field into messages | `|> flatten msg.items` |
| `distinct` | Drop duplicate values within TTL | `|> distinct msg.val within 1 m` |
| `throttle` | Token-bucket rate limiting | `|> throttle 10 per 1 s` |
| `sample` | Probabilistic sampling | `|> sample 10 %` |
| `group_by` | Partition for aggregation | `|> group_by msg.region` |
| `window` | Define aggregation buffer | `|> window tumbling(1 m)` |
| `aggregate` | Perform window calculations | `|> aggregate { total = sum(msg.v) }` |
| `accumulate` | Running accumulators across windows | `|> accumulate { s = ema(msg.v, 0.3) }` |
| `having` | Filter aggregation results | `|> having total > 100` |
| `map` | Transform output structure | `|> map { val = total }` |
| `sink` | Define message destination | `|> sink to broker :: "topic"` |

## Docker

The project includes a `docker-compose.yml` that provisions the full development stack:

| Service | Image | Port |
|---------|-------|------|
| MQTT | EMQX | 1883 |
| Redis | redis:7-alpine | 6379 |
| AMQP | rabbitmq:3-management-alpine | 5672 |
| Kafka | apache/kafka (KRaft) | 9092 |

### Makefile Targets

```bash
make build              # Build Docker image
make up                 # Start all services
make test               # Run full test suite in Docker
make test-unit          # Unit tests only
make test-integration   # Integration tests (real brokers)
make shell              # Open bash in app container
make logs               # Tail service logs
make check FILE=X       # Validate a .sigil file
make generate FILE=X    # Generate code (OUT=dir)
make validate-examples  # Validate all examples/*.sigil files
make clean              # Remove containers, images, caches
```

## Examples

- 01_hello_passthrough: Basic source to sink relay
- 02_filtering: Using filter stage with boolean logic
- 03_tumbling_window: Fixed-size non-overlapping windows
- 04_sliding_window: Overlapping windows with step interval
- 05_group_by: Partitioning streams for aggregation
- 06_map_transform: Reshaping output messages and let bindings
- 07_full_pipeline: Comprehensive example with multiple stages
- 08_multi_transport: Bridging MQTT, Redis, and AMQP
- 09_constants_and_env: Using constants and environment variables
- 10_case_and_having: Conditional expressions and aggregate filtering
- 11_dedup_and_temporal: Deduplication and old/changed expressions
- 12_quantifiers: Using forall and exists in predicates
- 13_lifecycle_hooks: Publishing on pipeline start and stop
- 14_sink_format_batch: Output serialization and batching
- 15_pipeline_chaining: Connecting pipelines via in-memory pipes
- 16_conditional_routing: Fan-out to multiple destinations
- 17_annotations_and_explain: Using @warn/@error and explain CLI
- 18_rate_limiting: Using rate and throttle stages
- 19_accumulate: State-based accumulation across messages
- 20_fmt_and_types: String interpolation and type coercion
- 21_schema_declarations: Strict and lenient schema validation
- 22_stream_join: Inner and left joins between streams
- 23_on_error_dlq: Error handling and Dead Letter Queues
- 24_new_stages: Flatten, distinct, and sample stages
- 25_retry_and_multi_window: Publish retries and named windows
- 26_ml_predict: ML inference with ONNX anomaly detection
- 27_ml_sklearn_classifier: scikit-learn ticket classification with routing
- 28_ml_torch_quality: PyTorch quality scoring with windowed aggregation
- 29_ml_tflite_edge: TFLite predictive maintenance on edge gateways
- 30_ml_custom_runtime: Custom Python module for financial signal detection
- 31_user_functions: User-defined functions inlined at call sites
- 32_match_and_if: Match expressions and if-then-else in pipelines
- 33_pipeline_let_bindings: Pipeline-level let bindings for shared values
- 34_no_sink_hub: Hub pipeline without a sink (route-only output)
- 35_imports: Multi-file imports and shared broker/constant definitions
- 36_amqp_kafka_transports: AMQP and Kafka transport configurations
- 37_left_join: Left join with optional enrichment from a second stream
- 38_scalar_funcs_and_exists: Scalar functions and collection exists() predicate
- 39_select_stage: Field projection and renaming with the select stage

## Documentation

Full language details are available in the [Language Reference](docs/language-reference.md).

## Architecture

Sigil uses a compiler pipeline that transforms high-level DSL definitions into production-ready Python code. The process involves parsing the textX grammar into an AST, performing multi-pass semantic validation (checking stage order, type compatibility, and reference integrity), and finally emitting code using Jinja2 templates. The generated applications are built on top of `commlib-py` for robust, transport-agnostic communication.

## Testing

The project maintains a comprehensive test suite with over 737 unit tests and 26 integration tests.

```bash
pytest tests/             # Run all tests
make test-unit            # Run unit tests (grammar, semantics, codegen)
make test-integration     # Run integration tests (requires Docker)
```

## License

Apache 2.0
