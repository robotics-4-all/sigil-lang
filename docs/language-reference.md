# Sigil Language Reference

Sigil is a domain-specific language (DSL) designed for building stream-processing aggregation pipelines. It allows you to define how messages from various message brokers are consumed, transformed, aggregated, and routed to destinations.

Sigil compiles into standalone Python applications that use the `commlib-py` library for unified communication across different message transports.

## Overview

Sigil focuses on defining data flow through a fixed sequence of processing stages. A Sigil program typically consists of broker definitions, constants, and one or more pipelines.

### Key Features
- **Declarative Pipeline Syntax**: Define what happens to your data, not how to implement the message handling.
- **Multi-Transport Support**: Seamlessly connect to MQTT, Redis, AMQP, and Kafka brokers.
- **Windowed Aggregation**: Perform calculations like averages, sums, and counts over time-based windows.
- **Conditional Routing**: Fan out messages to different destinations based on their content.
- **Pipeline Chaining**: Connect multiple pipelines together for complex processing logic.
- **Lifecycle Hooks**: Execute actions when a pipeline starts or stops.

### Compilation Target
Sigil files (`.sigil`) are compiled into Python code. The generated applications are self-contained and handle broker connections, message serialization, and pipeline logic automatically.

## Quick Start

A minimal Sigil program requires at least one broker and one pipeline with a source and a sink.

```sigil
BROKER local
    HOST: "localhost"
    PORT: 1883
;

PIPELINE hello_world
    SOURCE FROM local :: "input/topic"
    |> SINK TO local :: "output/topic"
;
```

### CLI Commands

The `sigil` command-line tool provides several subcommands for working with Sigil files:

- **Check**: Validate the syntax and semantics of a Sigil file.
  ```bash
  sigil check my_pipeline.sigil
  ```
- **Generate**: Compile a Sigil file into a Python application.
  ```bash
  sigil generate my_pipeline.sigil -o ./output_dir
  ```
- **Explain**: Generate a human-readable description of the pipeline logic.
  ```bash
  sigil explain my_pipeline.sigil
  ```
- **Visualize**: Generate a DOT or Mermaid diagram of the pipeline.
  ```bash
  sigil visualize my_pipeline.sigil --format dot
  sigil visualize my_pipeline.sigil --format mermaid
  ```

## Brokers

Brokers define the connection parameters for message transports.

### Syntax
```sigil
BROKER NAME
    TRANSPORT: MQTT | REDIS | AMQP | KAFKA
    HOST: "hostname"
    PORT: 1234
    SSL: true | false
    AUTH: {
        USERNAME: "user"
        PASSWORD: "pass"
    }
;
```

### Fields
- **TRANSPORT**: The message transport protocol. Defaults to `MQTT`. Supported values: `MQTT`, `REDIS`, `AMQP`, `KAFKA`.
- **HOST**: The broker hostname or IP address (string).
- **PORT**: The port number (integer).
- **SSL**: (Optional) Enable SSL/TLS connection. Defaults to `false`.
- **AUTH**: (Optional) Authentication credentials.

Multiple brokers can be defined in a single file, allowing pipelines to bridge different transports.

## Constants

Constants allow you to define reusable values that are inlined at compile time.

### Syntax
```sigil
CONST NAME = value
```

### Supported Types
- **Integer**: `42`
- **Float**: `3.14`
- **String**: `"hello"`
- **Boolean**: `true`, `false`

> **Note**: The Sigil parser treats all numbers as floats internally. `const X = 42` will be treated as `42.0`.

## Schema Declarations

Schemas define typed message shapes for compile-time field validation. When a schema is bound to a pipeline source, the compiler checks that all `msg.X` references exist in the schema and have compatible types.

### Syntax
```sigil
SCHEMA <name> [STRICT|LENIENT]
    <field_name>: <type>
    <field_name>: <type>?     // optional field
    ...
;
```

### Field Types

| Type | Description | Example |
|------|-------------|---------|
| `int` | Integer | `count: int` |
| `float` | Floating point | `temp: float` |
| `str` | String | `name: str` |
| `bool` | Boolean | `active: bool` |
| `{ ... }` | Nested object | `location: { lat: float, lon: float }` |
| `[type]` | List | `tags: [str]` |
| `type?` | Optional (may be null/absent) | `altitude: float?` |

### Strictness

- **`strict`** (default): Unknown fields in `msg.X` references produce **errors**.
- **`lenient`**: Unknown fields produce **warnings** (allows dynamic/extra fields).

### Schema Binding

Bind a schema to a pipeline source:

```sigil
PIPELINE my_pipeline
    SOURCE FROM broker :: "topic" SCHEMA: SensorReading
    ...
;
```

### Type Compatibility

The compiler issues warnings for type mismatches:
- `sum(msg.name)` where `name: str` → warning (sum expects numeric)
- `lower(msg.temp)` where `temp: float` → warning (lower expects string)

### Example

```sigil
SCHEMA SensorReading STRICT
    temp: float
    humidity: float
    sensor_id: str
    location: {
        lat: float,
        lon: float,
        altitude: float?
    }
    tags: [str]
;
```


PIPELINE monitor
    SOURCE FROM local :: "sensors/data" SCHEMA: SensorReading
    |> FILTER msg.temp > 30.0
    |> FILTER msg.location.lat > 35.0   // nested access validated
    |> SINK TO local :: "alerts"
;
```

## Pipelines

A pipeline defines the flow of data from a source through various processing stages to a sink or route.

### Syntax
```sigil
PIPELINE NAME
    [lifecycle_hooks]
    SOURCE FROM BROKER :: "topic"
    |> [join]
    |> [stages]
    |> SINK TO BROKER :: "topic" | |> ROUTE ... ;
    [lifecycle_hooks]
;
```

### Fixed Stage Order
Sigil enforces a strict order for processing stages. While most stages are optional, they must appear in this sequence if present:

1. **SOURCE** (Required)
2. **JOIN**
3. **DEDUPLICATE**
4. **FILTER**
5. **RATE**
6. **FLATTEN**
7. **DISTINCT**
8. **THROTTLE**
9. **SAMPLE**
10. **GROUP_BY**
11. **WINDOW** (Multiple named windows allowed)
12. **AGGREGATE**
13. **ACCUMULATE**
14. **HAVING**
15. **MAP**
16. **SINK** or **ROUTE** (Required)

Each stage type can appear at most once in a pipeline, with the exception of the **WINDOW** stage which can appear multiple times if each instance is named.

## Source

The `SOURCE` stage defines where the pipeline receives messages.

### Broker Source
Subscribes to a topic on a defined broker.
```sigil
SOURCE FROM local :: "sensors/+/data" QOS: 1 FORMAT: json
```
- **broker**: Reference to a defined broker name.
- **topic**: The topic to subscribe to. Supports wildcards depending on the transport.
- **qos**: (Optional) Quality of Service level (0, 1, or 2).
- **format**: (Optional) Message serialization format (`JSON`, `CSV`, or `RAW`). Defaults to `JSON`.

### Pipe Source
Receives messages directly from another pipeline in the same application.
```sigil
SOURCE FROM PIPE upstream_pipeline_name
```
This enables pipeline chaining without needing an intermediate broker.

## Join

The `JOIN` stage allows you to enrich the primary message stream with data from another stream (the "joined" stream). It buffers messages from both streams and matches them based on a common key.

### Syntax
```sigil
|> JOIN BROKER :: "topic"
    ON msg.LEFT_KEY == other.RIGHT_KEY
    WITHIN DURATION
    [TYPE: INNER | LEFT]
;
```

- **broker**: Reference to a defined broker name for the joined stream.
- **topic**: The topic for the joined stream.
- **on**: The key matching condition. The left side must reference the primary message (`msg.field`), and the right side must reference the joined message (`other.field`).
- **within**: The time window (e.g., `30s`, `1m`) to buffer messages for matching.
- **type**: (Optional) The join type. Defaults to `inner`.

### Join Types
- **INNER**: Only emits a message when a match is found in both streams within the time window. If a primary message arrives and no match is found yet, it is buffered and retried when a message arrives on the joined stream.
- **LEFT**: Always emits the primary message. If a match is found in the joined stream within the window, the fields from `other` are populated. Otherwise, they are `null`.

### Referencing Joined Fields
Downstream stages (like `FILTER`, `AGGREGATE`, or `MAP`) can access fields from the joined message using the `other` prefix (e.g., `other.name`).

### Null Handling (Left Join)
When using a `LEFT` join, joined fields may be `null`. Use the null coalesce operator (`??`) to provide default values:
```sigil
|> MAP {
    customer_name = other.name ?? "Guest"
}
```

### Restrictions
- The `JOIN` stage cannot be used in pipelines that use a `PIPE` source.
- The left key in the `ON` clause must start with `msg.`.
- The right key in the `ON` clause must start with `other.`.

### Example
```sigil
PIPELINE enrich_orders
    SOURCE FROM local :: "orders/new"
    |> JOIN local :: "customers/profiles"
        ON msg.customer_id == other.id
        WITHIN 30 S
        TYPE: INNER
    ;
    |> FILTER other.active == true
    |> MAP {
        order_id = msg.order_id;
        customer_name = other.name ?? "unknown";
        amount = msg.amount
    }
    |> SINK TO local :: "orders/enriched"
;
```

## Sink

The `SINK` stage defines the primary destination for processed messages.

### Basic Sink
```sigil
|> SINK TO local :: "output/topic" QOS: 1 FORMAT: json
```

### Batching
Sinks can buffer messages and send them in batches.
```sigil
|> SINK TO local :: "output/batch" BATCH: {
    SIZE: 100
    TIMEOUT: 10S
}
```
- **SIZE**: Maximum number of messages in a batch.
- **TIMEOUT**: Maximum time to wait before sending a partial batch. Supports duration units (`MS`, `S`, `M`, `H`, `D`).

## Route (Conditional Routing)

The `ROUTE` stage allows you to send messages to different destinations based on conditions. A pipeline can have either a `SINK` or a `ROUTE`, but not both.

### Syntax
```sigil
|> ROUTE
    WHEN msg.value > 100 => SINK TO alerts :: "critical"
    WHEN msg.value > 50 => SINK TO local :: "warning"
    DEFAULT => SINK TO local :: "normal"
;
```
Conditions are evaluated in order. The first matching arm is used. The `DEFAULT` arm is highly recommended to handle messages that don't match any condition.

## Stages

### DEDUPLICATE
Drops duplicate messages based on a specific field within a sliding time window.

```sigil
|> DEDUPLICATE msg.event_id WITHIN 5M
```
- **key**: Must be a message field path starting with `msg.`.
- **within**: The time window for tracking duplicates.

### FILTER
Drops messages that do not satisfy a boolean expression.

```sigil
|> FILTER msg.status == "active" and msg.value > 0
```

### FLATTEN

Expands an array field into individual messages. Each element of the array
becomes a separate message flowing through the rest of the pipeline.

**Syntax:**
|> FLATTEN msg.<field>

**Example:**
|> FLATTEN msg.items

If `msg = {"items": [{"name": "a"}, {"name": "b"}]}`, the flatten stage
produces two messages: `{"name": "a"}` and `{"name": "b"}`.

If the field is not a list, the original message passes through unchanged.

### DISTINCT

Deduplicates messages by a field value within a time window. Messages with
the same field value within the TTL window are dropped.

**Syntax:**
|> DISTINCT msg.<field> WITHIN <duration>

**Example:**
|> DISTINCT msg.event_id WITHIN 10 M

Unlike `DEDUPLICATE` (which uses string key matching), `DISTINCT` compares
field values for equality and automatically expires entries after the TTL.

### THROTTLE

Rate-limits messages with configurable overflow behavior. Unlike `RATE`
(which always drops excess messages), THROTTLE can buffer overflow messages
and process them when capacity becomes available.

**Syntax:**
|> THROTTLE <limit> PER <duration> [OVERFLOW: DROP|BUFFER] [BUFFER: <size>]

**Examples:**
|> THROTTLE 100 PER 1 S                        // drop excess (default)
|> THROTTLE 10 PER 1 S OVERFLOW: BUFFER         // buffer excess (default 1000)
|> THROTTLE 10 PER 1 S OVERFLOW: BUFFER BUFFER: 500  // buffer with size limit

When overflow is `BUFFER`, excess messages are queued (up to buffer size)
and automatically drained when rate tokens become available. When the buffer
is full, additional messages are dropped.

### SAMPLE

Probabilistically samples a percentage of messages. Each message has an
independent chance of being kept.

**Syntax:**
|> SAMPLE <percentage> %

**Example:**
|> SAMPLE 10 %

The percentage must be between 1 and 100. `SAMPLE 100 %` keeps all
messages. `SAMPLE 1 %` keeps approximately 1 in 100 messages.

### GROUP_BY
Partitions messages into groups for windowed aggregation.

```sigil
|> GROUP_BY msg.sensor_id, msg.region
```
Requires a `WINDOW` stage to follow.

### WINDOW
Defines the time-based buffer for aggregation.

#### Named Multi-Window
Pipelines can define multiple windows to perform aggregations over different time horizons simultaneously. When using multiple windows, each must be given a unique name.

```sigil
|> WINDOW SHORT TUMBLING(1M)
|> WINDOW LONG TUMBLING(5M)
```

When multiple windows are defined:
- All windows must be named.
- All aggregation assignments in the `AGGREGATE` stage must use the `OVER` keyword to specify which window they apply to.
- Each window flushes independently at its own interval.
- The output message includes a `_WINDOW` field containing the name of the window that triggered the flush.

If only a single window is used, naming is optional and the `OVER` keyword is not required in the `AGGREGATE` stage.

#### Tumbling Window
Fixed-size, non-overlapping windows.

```sigil
|> WINDOW TUMBLING(1H)
```

#### Sliding Window
Overlapping windows that advance by a specified step.

```sigil
|> WINDOW SLIDING(1H, STEP = 15M)
```
The `STEP` must be less than or equal to the window size.

### AGGREGATE
Performs calculations over the messages in the current window.

```sigil
|> AGGREGATE {
    avg_val = avg(msg.value),
    max_val = max(msg.value),
    total   = sum(msg.count),
    samples = count()
}
```

#### The `OVER` Clause
When multiple windows are defined, use the `OVER` keyword to associate an aggregation with a specific window.

```sigil
|> AGGREGATE {
    avg_1m = avg(msg.value) OVER short,
    avg_5m = avg(msg.value) OVER long
}
```

#### Type Annotations
Aggregate assignments can include optional type annotations to ensure the result is of a specific type.

- **Syntax**: `name: type = func(field)`
- **Supported Types**: `int`, `float`, `str`, `bool`

If a type annotation is provided, the result of the aggregation function is coerced using the corresponding Python type constructor (e.g., `float(value)`). If omitted, the default type returned by the function is used.

**Examples**:
- `total: float = sum(msg.value)`: Ensures the result is always a float.
- `n: int = count()`: Explicitly marks the count as an integer.

#### Supported Functions
- `avg(field)`: Arithmetic mean.
- `sum(field)`: Total sum.
- `min(field)`: Minimum value.
- `max(field)`: Maximum value.
- `count()`: Number of messages in the window (takes no arguments).
- `stddev(field)`: Standard deviation.
- `first(field)`: First value received in the window.
- `last(field)`: Last value received in the window.
- `percentile(field, q)`: Approximate q-th percentile (0–100) using a sorted list. Returns `None` if the window is empty. Example: `p95 = percentile(msg.value, 95)`.
- `distinct_count(field)`: Exact count of distinct non-null values (Python `set` cardinality). Example: `n_devices = distinct_count(msg.device_id)`.

Requires a `window` stage.

### HAVING
Filters the results of an aggregation.

```sigil
|> HAVING avg_val > 10 and samples >= 5
```
In the `HAVING` stage, bare identifiers (like `avg_val`) resolve to the names defined in the `AGGREGATE` stage.

### MAP
Transforms the output message structure.

```sigil
|> MAP {
    let temp_c = (avg_val - 32) * 5 / 9;
    
    sensor_id = msg.sensor_id;
    status    = if temp_c > 30 then "hot" else "normal";
    value     = temp_c
}
```
- **let bindings**: Define temporary variables for use within the map block. Use `,` to separate multiple bindings.
- **assignments**: Define the fields of the output message. Use `,` to separate multiple assignments.

Bare identifiers resolve to aggregation names.

### SELECT
Projects and optionally renames fields from the current message. Use `SELECT` as a lightweight alternative to `MAP` when you only need to pick a subset of fields — no expressions, no let bindings.

```sigil
|> SELECT [field1, alias: source_field, ...]
```

**Two forms:**
- `name` — keep the field as-is (pass-through, same name in output):
  ```sigil
  |> SELECT [device_id, ts, battery_mv]
  ```
- `alias: source` — rename `source` to `alias` in the output message:
  ```sigil
  |> SELECT [id: device_id, temp: temperature_celsius, ts: ts]
  ```

Both forms can be mixed freely in a single `SELECT` stage:
```sigil
|> SELECT [
    id: device_id,
    ts,
    temp: temperature_celsius,
    battery_mv
]
```

**Notes:**
- `select` runs at the same phase as `map` (after aggregation). Only one of `select` or `map` should be used per pipeline.
- Fields not listed in `select` are dropped from the output message.
- Field values are taken directly from the incoming message — no expression evaluation.
- When used after an `aggregate` stage, `select` picks from the aggregated result object, not from individual messages.

## Expressions

Sigil supports a rich expression language used in filters, mappings, and conditions.

### Precedence (low to high)
1. `or`
2. `and`
3. `==`, `!=`, `>`, `<`, `>=`, `<=`, `is null`, `is not null`, `in`, `not in`
4. `+`, `-`
5. `*`, `/`, `%`
6. `??` (Null coalesce)
7. Unary `-`, `not`
8. Atoms (literals, field refs, function calls, etc.)

### Literals
- **Integer**: `123`
- **Float**: `123.45`
- **String**: `"text"`
- **Boolean**: `true`, `false`
- **Null check**: `expr is null` or `expr is not null`

### Field References
- `msg.path.to.field`: Access fields in the incoming message.
- `bare_id`: Resolves to an aggregation name in `having` and `map` stages, or a `let` binding in `map`.

### Operators
- **Arithmetic**: `+`, `-`, `*`, `/`, `%`
- **Comparison**: `==`, `!=`, `>`, `<`, `>=`, `<=`
- **Logical**: `and`, `or`, `not`
- **Null Coalesce**: `a ?? b` (returns `a` if not null, otherwise `b`)
- **Membership**: `expr in [1, 2, 3]` or `expr not in ["a", "b"]`

### Conditional Expressions
- **If-Then-Else**: `if condition then expr1 else expr2`
- **Case**:
  ```sigil
  case {
      when msg.type == 1 => "A"
      when msg.type == 2 => "B"
      else => "Unknown"
  }
  ```

### Functions
- `abs(x)`: Absolute value.
- `round(x, n)`: Round `x` to `n` decimal places.
- `len(x)`: Length of a string or collection.
- `lower(x)` / `upper(x)`: String casing.
- `floor(x)` / `ceil(x)`: Rounding to nearest integer.
- `fmt(pattern, ...)`: String interpolation.

#### String Interpolation: `fmt()`
The `fmt()` function provides a way to construct strings by interpolating values into a pattern. It uses Python's `str.format()` syntax, where `{}` placeholders are replaced by positional arguments.

- **Syntax**: `fmt(pattern, arg1, arg2, ...)`
- **Context**: Works in any expression context (filter, map, having, etc.).

**Examples**:
- `fmt("Temperature: {}°C", msg.temp)` → `"Temperature: 23.5°C"`
- `fmt("{} at {}", msg.sensor_id, msg.location)` → `"sensor_1 at room_3"`

### Environment Variables
Access system environment variables at runtime.
```sigil
env("API_KEY")
env("TIMEOUT", 30) // With default value
```

### Temporal Expressions
Valid only in `filter` and `having` stages.
- `old(msg.field)`: The value of the field from the previous message.
- `changed(msg.field)`: Returns `true` if the field value is different from the previous message.

### Quantifiers
- **forall**: `forall(m => m > 0)`
  - In `having`: Checks if the predicate is true for all messages in the window.
- **exists**: `exists(x in msg.tags => x == "critical")`
  - Checks if at least one element in a collection satisfies the predicate.

## Lifecycle Hooks

Hooks allow you to publish messages when a pipeline starts or stops.

```sigil
PIPELINE my_pipe
    @INITIALLY { PUBLISH { event: "started", version: 1.0 } }
    
    // ... pipeline logic ...
    
    @FINALLY { PUBLISH { event: "stopped" } }
;
```

Or to silently discard failed messages:

```sigil
    ON_ERROR: DROP
```

### DLQ Envelope

When `ON_ERROR: SINK TO ...` is used, failed messages are wrapped in an error envelope:

```json
{
    "_error": "division by zero",
    "_error_type": "ZeroDivisionError",
    "_pipeline": "sensor_pipeline",
    "_timestamp": 1709123456.789,
    "_original": { ... the original message or batch ... }
}
```

### Cross-Transport DLQ

The DLQ can use a different broker/transport than the main pipeline:

```sigil
PIPELINE payments
    SOURCE FROM mqtt_broker :: "payments/incoming"
    |> SINK TO mqtt_broker :: "payments/processed"
    ON_ERROR: SINK TO redis_broker :: "dlq:payments"
;
```

## Retry Policy

The `retry` clause configures the publish retry behavior for a pipeline's sink or route. This ensures reliable delivery when the destination broker is temporarily unavailable.

### Syntax

```sigil
PIPELINE my_pipeline
    SOURCE FROM broker :: "topic"
    |> SINK TO broker :: "output"
    RETRY: 5 WITH BACKOFF 500 MS
;
```

- **MAX_RETRIES**: The maximum number of retry attempts (must be ≥ 1).
- **BACKOFF**: The base duration for exponential backoff (must be positive).

### Default Behavior

If no `RETRY` policy is specified, the default is **3 retries with a 100ms base backoff**.

### Exponential Backoff

The retry mechanism uses exponential backoff to avoid overwhelming the broker. The delay between attempts increases according to the formula: `base_backoff * (2 ^ attempt_number)`.

## Stage Annotations

Annotations can be used to emit diagnostic messages during the `sigil check` process.

```sigil
|> @WARN("This filter might be too restrictive")
FILTER msg.value > 1000

|> @ERROR("Deprecated stage usage")
DEDUPLICATE msg.id WITHIN 1S
```

## Imports

You can split your Sigil definitions across multiple files.

```sigil
IMPORT "common_brokers.sigil"
IMPORT "constants.sigil"
```
Importing a file merges its brokers, constants, and pipelines into the current model. Circular imports are automatically detected and prevented.

## Pipeline Chaining

Pipelines can be connected by setting the `source` of one pipeline to the name of another.

```sigil
PIPELINE p1
    SOURCE FROM b :: "in"
    |> SINK TO b :: "out"
;

PIPELINE p2
    SOURCE FROM PIPE p1
    |> SINK TO b :: "final"
;
```
Messages emitted by `p1` (via its sink or route) are automatically fed into `p2`.

## Comments

- **Line comments**: `// this is a comment`
- **Block comments**: `/* this is a multi-line comment */`

## Reserved Words

The following keywords are reserved and cannot be used as identifiers (broker names, pipeline names, etc.):

Expression keywords (lowercase):
`if`, `then`, `else`, `let`, `in`, `is`, `null`, `const`, `env`, `case`, `when`, `having`, `old`, `changed`, `forall`, `exists`, `initially`, `finally`, `publish`, `default`, `warn`, `error`, `abs`, `round`, `len`, `lower`, `upper`, `floor`, `ceil`, `fmt`

Structural keywords (UPPERCASE):
`BROKER`, `PIPELINE`, `SOURCE`, `SINK`, `FILTER`, `MAP`, `JOIN`, `ROUTE`, `AGGREGATE`, `ACCUMULATE`, `HAVING`, `WINDOW`, `GROUP_BY`, `DEDUPLICATE`, `RATE`, `FLATTEN`, `DISTINCT`, `THROTTLE`, `SAMPLE`, `MODEL`, `SCHEMA`, `PREDICT`, `DEBUG`, `TEE`, `FANOUT`, `ENRICH`

Structural clause keywords:
`FROM`, `TO`, `PIPE`, `ON`, `OTHER`, `INNER`, `LEFT`, `TYPE`, `WITH`, `OVER`, `WITHIN`, `BATCH`, `RETRY`, `BACKOFF`, `ON_ERROR`

Annotation keywords:
`@WARN`, `@ERROR`, `@INITIALLY`, `@FINALLY`, `PUBLISH`

Type keywords:
`int`, `float`, `str`, `bool`

Transport values:
`MQTT`, `REDIS`, `AMQP`, `KAFKA`

Action values:
`DROP`, `WARN`, `ERROR`

Runtime values:
`ONNX`, `SKLEARN`, `TORCH`, `TFLITE`, `CUSTOM`

Strictness values:
`STRICT`, `LENIENT`
