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
broker local
    host: "localhost"
    port: 1883
end

pipeline hello_world
    source from local :: "input/topic"

    |> sink to local :: "output/topic"
end
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
broker NAME
    transport: mqtt | redis | amqp | kafka
    host: "hostname"
    port: 1234
    ssl: true | false
    auth: {
        username: "user"
        password: "pass"
    }
end
```

### Fields
- **transport**: The message transport protocol. Defaults to `mqtt`. Supported values: `mqtt`, `redis`, `amqp`, `kafka`.
- **host**: The broker hostname or IP address (string).
- **port**: The port number (integer).
- **ssl**: (Optional) Enable SSL/TLS connection. Defaults to `false`.
- **auth**: (Optional) Authentication credentials.

Multiple brokers can be defined in a single file, allowing pipelines to bridge different transports.

## Constants

Constants allow you to define reusable values that are inlined at compile time.

### Syntax
```sigil
const NAME = value
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
schema <name> [strict|lenient]
    <field_name>: <type>
    <field_name>: <type>?     // optional field
    ...
end
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
pipeline my_pipeline
    source from broker :: "topic" schema: SensorReading
    ...
end
```

### Type Compatibility

The compiler issues warnings for type mismatches:
- `sum(msg.name)` where `name: str` → warning (sum expects numeric)
- `lower(msg.temp)` where `temp: float` → warning (lower expects string)

### Example

```sigil
schema SensorReading strict
    temp: float
    humidity: float
    sensor_id: str
    location: {
        lat: float,
        lon: float,
        altitude: float?
    }
    tags: [str]
end

pipeline monitor
    source from local :: "sensors/data" schema: SensorReading
    |> filter: msg.temp > 30.0
    |> filter: msg.location.lat > 35.0   // nested access validated
    |> sink to local :: "alerts"
end
```

## Pipelines

A pipeline defines the flow of data from a source through various processing stages to a sink or route.

### Syntax
```sigil
pipeline NAME
    [lifecycle_hooks]
    source from BROKER :: "topic"
    |> [join]
    |> [stages]
    |> sink to BROKER :: "topic" | |> route ... end
    [lifecycle_hooks]
end
```

### Fixed Stage Order
Sigil enforces a strict order for processing stages. While most stages are optional, they must appear in this sequence if present:

1. **source** (Required)
2. **join**
3. **deduplicate**
4. **filter**
5. **rate**
6. **flatten**
7. **distinct**
8. **throttle**
9. **sample**
10. **group_by**
11. **window** (Multiple named windows allowed)
12. **aggregate**
13. **accumulate**
14. **having**
15. **map**
16. **sink** or **route** (Required)

Each stage type can appear at most once in a pipeline, with the exception of the **window** stage which can appear multiple times if each instance is named.

## Source

The `source` stage defines where the pipeline receives messages.

### Broker Source
Subscribes to a topic on a defined broker.
```sigil
source from local :: "sensors/+/data" qos: 1 format: json
```
- **broker**: Reference to a defined broker name.
- **topic**: The topic to subscribe to. Supports wildcards depending on the transport.
- **qos**: (Optional) Quality of Service level (0, 1, or 2).
- **format**: (Optional) Message serialization format (`json`, `csv`, or `raw`). Defaults to `json`.

### Pipe Source
Receives messages directly from another pipeline in the same application.
```sigil
source from pipe upstream_pipeline_name
```
This enables pipeline chaining without needing an intermediate broker.

## Join

The `join` stage allows you to enrich the primary message stream with data from another stream (the "joined" stream). It buffers messages from both streams and matches them based on a common key.

### Syntax
```sigil
|> join BROKER :: "topic"
    on msg.LEFT_KEY == other.RIGHT_KEY
    within DURATION
    [type: inner | left]
end
```

- **broker**: Reference to a defined broker name for the joined stream.
- **topic**: The topic for the joined stream.
- **on**: The key matching condition. The left side must reference the primary message (`msg.field`), and the right side must reference the joined message (`other.field`).
- **within**: The time window (e.g., `30s`, `1m`) to buffer messages for matching.
- **type**: (Optional) The join type. Defaults to `inner`.

### Join Types
- **inner**: Only emits a message when a match is found in both streams within the time window. If a primary message arrives and no match is found yet, it is buffered and retried when a message arrives on the joined stream.
- **left**: Always emits the primary message. If a match is found in the joined stream within the window, the fields from `other` are populated. Otherwise, they are `null`.

### Referencing Joined Fields
Downstream stages (like `filter`, `aggregate`, or `map`) can access fields from the joined message using the `other` prefix (e.g., `other.name`).

### Null Handling (Left Join)
When using a `left` join, joined fields may be `null`. Use the null coalesce operator (`??`) to provide default values:
```sigil
|> map: {
    customer_name = other.name ?? "Guest"
}
```

### Restrictions
- The `join` stage cannot be used in pipelines that use a `pipe` source.
- The left key in the `on` clause must start with `msg.`.
- The right key in the `on` clause must start with `other.`.

### Example
```sigil
pipeline enrich_orders
    source from local :: "orders/new"
    |> join local :: "customers/profiles"
        on msg.customer_id == other.id
        within 30 s
        type: inner
    end
    |> filter: other.active == true
    |> map: {
        order_id = msg.order_id;
        customer_name = other.name ?? "unknown";
        amount = msg.amount
    }
    |> sink to local :: "orders/enriched"
end
```

## Sink

The `sink` stage defines the primary destination for processed messages.

### Basic Sink
```sigil
|> sink to local :: "output/topic" qos: 1 format: json
```

### Batching
Sinks can buffer messages and send them in batches.
```sigil
|> sink to local :: "output/batch" batch: {
    size: 100
    timeout: 10s
}
```
- **size**: Maximum number of messages in a batch.
- **timeout**: Maximum time to wait before sending a partial batch. Supports duration units (`ms`, `s`, `m`, `h`, `d`).

## Route (Conditional Routing)

The `route` stage allows you to send messages to different destinations based on conditions. A pipeline can have either a `sink` or a `route`, but not both.

### Syntax
```sigil
|> route
    when msg.value > 100 => sink to alerts :: "critical"
    when msg.value > 50 => sink to local :: "warning"
    default => sink to local :: "normal"
end
```
Conditions are evaluated in order. The first matching arm is used. The `default` arm is highly recommended to handle messages that don't match any condition.

## Stages

### deduplicate
Drops duplicate messages based on a specific field within a sliding time window.

```sigil
|> deduplicate: msg.event_id within 5m
```
- **key**: Must be a message field path starting with `msg.`.
- **within**: The time window for tracking duplicates.

### filter
Drops messages that do not satisfy a boolean expression.

```sigil
|> filter: msg.status == "active" and msg.value > 0
```

### flatten

Expands an array field into individual messages. Each element of the array
becomes a separate message flowing through the rest of the pipeline.

**Syntax:**
|> flatten: msg.<field>

**Example:**
|> flatten: msg.items

If `msg = {"items": [{"name": "a"}, {"name": "b"}]}`, the flatten stage
produces two messages: `{"name": "a"}` and `{"name": "b"}`.

If the field is not a list, the original message passes through unchanged.

### distinct

Deduplicates messages by a field value within a time window. Messages with
the same field value within the TTL window are dropped.

**Syntax:**
|> distinct: msg.<field> within <duration>

**Example:**
|> distinct: msg.event_id within 10 m

Unlike `deduplicate` (which uses string key matching), `distinct` compares
field values for equality and automatically expires entries after the TTL.

### throttle

Rate-limits messages with configurable overflow behavior. Unlike `rate`
(which always drops excess messages), throttle can buffer overflow messages
and process them when capacity becomes available.

**Syntax:**
|> throttle: <limit> per <duration> [overflow: drop|buffer] [buffer: <size>]

**Examples:**
|> throttle: 100 per 1 s                        // drop excess (default)
|> throttle: 10 per 1 s overflow: buffer         // buffer excess (default 1000)
|> throttle: 10 per 1 s overflow: buffer buffer: 500  // buffer with size limit

When overflow is `buffer`, excess messages are queued (up to buffer size)
and automatically drained when rate tokens become available. When the buffer
is full, additional messages are dropped.

### sample

Probabilistically samples a percentage of messages. Each message has an
independent chance of being kept.

**Syntax:**
|> sample: <percentage> %

**Example:**
|> sample: 10 %

The percentage must be between 1 and 100. `sample: 100 %` keeps all
messages. `sample: 1 %` keeps approximately 1 in 100 messages.

### group_by
Partitions messages into groups for windowed aggregation.

```sigil
|> group_by: msg.sensor_id, msg.region
```
Requires a `window` stage to follow.

### window
Defines the time-based buffer for aggregation.

#### Named Multi-Window
Pipelines can define multiple windows to perform aggregations over different time horizons simultaneously. When using multiple windows, each must be given a unique name.

```sigil
|> window short: tumbling(1m)
|> window long: tumbling(5m)
```

When multiple windows are defined:
- All windows must be named.
- All aggregation assignments in the `aggregate` stage must use the `over` keyword to specify which window they apply to.
- Each window flushes independently at its own interval.
- The output message includes a `_window` field containing the name of the window that triggered the flush.

If only a single window is used, naming is optional and the `over` keyword is not required in the `aggregate` stage.

#### Tumbling Window
Fixed-size, non-overlapping windows.
```sigil
|> window: tumbling(1h)
```

#### Sliding Window
Overlapping windows that advance by a specified step.
```sigil
|> window: sliding(1h, step = 15m)
```
The `step` must be less than or equal to the window size.

### aggregate
Performs calculations over the messages in the current window.

```sigil
|> aggregate: {
    avg_val = avg(msg.value)
    max_val = max(msg.value)
    total   = sum(msg.count)
    samples = count()
}
```

#### The `over` Clause
When multiple windows are defined, use the `over` keyword to associate an aggregation with a specific window.

```sigil
|> aggregate: {
    avg_1m = avg(msg.value) over short
    avg_5m = avg(msg.value) over long
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

Requires a `window` stage.

### having
Filters the results of an aggregation.

```sigil
|> having: avg_val > 10 and samples >= 5
```
In the `having` stage, bare identifiers (like `avg_val`) resolve to the names defined in the `aggregate` stage.

### map
Transforms the output message structure.

```sigil
|> map: {
    let temp_c = (avg_val - 32) * 5 / 9;
    
    sensor_id = msg.sensor_id;
    status    = if temp_c > 30 then "hot" else "normal";
    value     = temp_c
}
```
- **let bindings**: Define temporary variables for use within the map block. Use `;` to separate multiple bindings.
- **assignments**: Define the fields of the output message. Use `;` to separate multiple assignments.

Bare identifiers resolve to aggregation names.

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
pipeline my_pipe
    @initially {
        publish { event: "started", version: 1.0 }
    }
    
    // ... pipeline logic ...
    
    @finally {
        publish { event: "stopped" }
    }
end

## Error Handling (on_error)

The `on_error` clause configures how a pipeline handles processing failures. Without `on_error`, failures are logged at ERROR level and the message is lost.

### Syntax

```sigil
pipeline my_pipeline
    source from broker :: "topic"
    |> ...stages...
    |> sink to broker :: "output"
    on_error: sink to broker :: "errors/topic"   // route to DLQ
end
```

Or to silently discard failed messages:

```sigil
    on_error: drop
```

### DLQ Envelope

When `on_error: sink to ...` is used, failed messages are wrapped in an error envelope:

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
pipeline payments
    source from mqtt_broker :: "payments/incoming"
    |> sink to mqtt_broker :: "payments/processed"
    on_error: sink to redis_broker :: "dlq:payments"
end
```

## Retry Policy

The `retry` clause configures the publish retry behavior for a pipeline's sink or route. This ensures reliable delivery when the destination broker is temporarily unavailable.

### Syntax

```sigil
pipeline my_pipeline
    source from broker :: "topic"
    |> sink to broker :: "output"
    retry: 5 with backoff 500 ms
end
```

- **max_retries**: The maximum number of retry attempts (must be ≥ 1).
- **backoff**: The base duration for exponential backoff (must be positive).

### Default Behavior

If no `retry` policy is specified, the default is **3 retries with a 100ms base backoff**.

### Exponential Backoff

The retry mechanism uses exponential backoff to avoid overwhelming the broker. The delay between attempts increases according to the formula: `base_backoff * (2 ^ attempt_number)`.

## Stage Annotations



Annotations can be used to emit diagnostic messages during the `sigil check` process.

```sigil
|> @warn("This filter might be too restrictive")
filter: msg.value > 1000

|> @error("Deprecated stage usage")
deduplicate: msg.id within 1s
```

## Imports

You can split your Sigil definitions across multiple files.

```sigil
import "common_brokers.sigil"
import "constants.sigil"
```
Importing a file merges its brokers, constants, and pipelines into the current model. Circular imports are automatically detected and prevented.

## Pipeline Chaining

Pipelines can be connected by setting the `source` of one pipeline to the name of another.

```sigil
pipeline p1
    source from b :: "in"
    |> sink to b :: "out"
end

pipeline p2
    source from pipe p1
    |> sink to b :: "final"
end
```
Messages emitted by `p1` (via its sink or route) are automatically fed into `p2`.

## Comments

- **Line comments**: `// this is a comment`
- **Block comments**: `/* this is a multi-line comment */`

## Reserved Words

The following keywords are reserved and cannot be used as identifiers (broker names, pipeline names, etc.):

`if`, `then`, `else`, `let`, `in`, `is`, `null`, `const`, `env`, `case`, `when`, `having`, `deduplicate`, `within`, `old`, `changed`, `forall`, `exists`, `initially`, `finally`, `publish`, `batch`, `import`, `route`, `default`, `warn`, `error`, `abs`, `round`, `len`, `lower`, `upper`, `floor`, `ceil`, `fmt`, `int`, `float`, `str`, `bool`, `end`, `from`, `to`, `pipe`, `source`, `sink`, `join`, `on`, `other`, `inner`, `left`, `type`, `schema`, `strict`, `lenient`, `on_error`, `flatten`, `distinct`, `throttle`, `sample`, `overflow`, `buffer`, `retry`, `backoff`, `with`, `over`
