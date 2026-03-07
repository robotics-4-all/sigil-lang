# Sigil Examples Walkthrough

Sigil comes with a set of examples that show how to build data pipelines. These files live in the `examples/` directory. They're organized to help you learn the language step by step. You'll start with simple relays and move toward complex systems with joins and multi-window logic.

To check if an example is valid, run:
`sigil check examples/01_hello_passthrough.sigil`

To generate a Python application from it:
`sigil generate examples/01_hello_passthrough.sigil -o ./output`

To see a human-readable explanation of what the pipeline does:
`sigil explain examples/01_hello_passthrough.sigil`

To visualize the pipeline as a diagram:
`sigil visualize examples/01_hello_passthrough.sigil --format mermaid`

---

## Tier 1: Fundamentals (01-06). Core concepts.

### 01_hello_passthrough.sigil
**New Concepts:**
- Broker definitions
- Pipeline structure
- Source and sink stages

This example shows the simplest possible pipeline. It connects to a local MQTT broker and moves messages from one topic to another without changing them.

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

**Key Takeaways:**
Every Sigil file needs at least one broker and one pipeline. The `source` and `sink` stages are the only ones you must include.

### 02_filtering.sigil
**New Concepts:**
- Filter stage
- Boolean expressions
- QoS and format options

Here we add a filter to drop readings that fall outside a specific range. It also shows how to set MQTT QoS levels and specify that the input is JSON.

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline valid_readings
    source from local :: "sensors/temperature" qos: 1 format: json
    |> filter msg.value > -40 and msg.value < 85
    |> sink to local :: "validated/temperature" qos: 1
end
```

**Key Takeaways:**
Filters use standard boolean logic. You can access message fields using the `msg.` prefix.

### 03_tumbling_window.sigil
**New Concepts:**
- Tumbling windows
- Aggregate stage
- Aggregation functions (avg, min, max, count)
- Topic wildcards

This pipeline groups messages into 30-second blocks. It calculates the average, minimum, and maximum values for each block. The `+` wildcard in the source topic lets it listen to all sensors.

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline temperature_summary
    source from local :: "sensors/+/temperature" qos: 1 format: json
    |> window tumbling(30 s)
    |> aggregate {
        avg_temp = avg(msg.value)
        min_temp = min(msg.value)
        max_temp = max(msg.value)
        readings = count()
    }
    |> sink to local :: "analytics/temperature" qos: 1
end
```

**Key Takeaways:**
Tumbling windows don't overlap. When a window ends, Sigil runs the aggregations and sends the result to the next stage.

### 04_sliding_window.sigil
**New Concepts:**
- Sliding windows
- Window step
- Standard deviation (stddev)

Sliding windows overlap. This example looks at the last 10 seconds of vibration data every 2 seconds. It's useful for detecting sudden changes or anomalies using standard deviation.

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline vibration_monitor
    source from local :: "machines/+/vibration" qos: 2 format: json
    |> window sliding(10 s, step = 2 s)
    |> aggregate {
        avg_rms    = avg(msg.rms)
        peak_rms   = max(msg.rms)
        deviation  = stddev(msg.rms)
        samples    = count()
    }
    |> sink to local :: "analytics/vibration" qos: 1
end
```

**Key Takeaways:**
Use sliding windows when you need a moving average or frequent updates on recent history.

### 05_group_by.sigil
**New Concepts:**
- Grouping data
- Per-group aggregation

This pipeline handles data from multiple factory zones. By using `group_by`, it creates separate windows for each zone. The aggregations run independently for each group.

```sigil
broker factory
    host: "localhost"
    port: 1883
end

pipeline zone_temperature
    source from factory :: "factory/sensors/temperature" format: json
    |> group_by msg.zone
    |> window tumbling(1 m)
    |> aggregate {
        avg_temp = avg(msg.value)
        max_temp = max(msg.value)
        readings = count()
    }
    |> sink to factory :: "analytics/zone_temperature"
end
```

**Key Takeaways:**
Grouping lets you process many streams in a single pipeline. Each unique value in the `group_by` field gets its own state.

### 06_map_transform.sigil
**New Concepts:**
- Map stage
- Field renaming
- Derived calculations
- Semicolon separators

The map stage lets you reshape the output. In this energy report, we rename fields and calculate the cost in Euros by multiplying the total kWh by a rate.

```sigil
broker meters
    host: "localhost"
    port: 1883
end

pipeline energy_report
    source from meters :: "meters/+/energy" format: json
    |> window tumbling(1 h)
    |> aggregate {
        total_kwh = sum(msg.kwh)
        peak_kw   = max(msg.kw)
        readings  = count()
    }
    |> map {
        energy      = total_kwh;
        peak_demand = peak_kw;
        cost_eur    = total_kwh * 0.12;
        samples     = readings
    }
    |> sink to meters :: "reports/energy/hourly"
end
```

**Key Takeaways:**
Use the map stage to prepare data for external systems. It's the last chance to change the message structure before it hits the sink.

---

## Tier 2: Intermediate (07-14). Full pipelines, multi-transport, expressions.

### 07_full_pipeline.sigil
**New Concepts:**
- SSL and Authentication
- Complex aggregation sets
- First and Last functions

This example shows a production-ready fleet telemetry pipeline. It uses SSL and username/password auth for the broker. It also uses almost every available stage to process vehicle data.

```sigil
broker fleet_mqtt
    host: "mqtt.fleet.com"
    port: 8883
    transport: mqtt
    auth: {
        username: "admin"
        password: env("MQTT_PASS")
    }
    ssl: true
end

pipeline fleet_telemetry
    source from fleet_mqtt :: "fleet/+/telemetry"
    |> filter msg.speed > 0
    |> group_by msg.vehicle_id
    |> window tumbling(5 m)
    |> aggregate {
        avg_speed = avg(msg.speed)
        max_speed = max(msg.speed)
        total_dist = sum(msg.distance)
        msg_count = count()
        speed_std = stddev(msg.speed)
        first_loc = first(msg.location)
        last_loc = last(msg.location)
    }
    |> map {
        vehicle = msg.vehicle_id;
        stats = {
            avg: avg_speed,
            max: max_speed,
            std: speed_std
        };
        distance = total_dist;
        count = msg_count;
        start = first_loc;
        end = last_loc
    }
    |> sink to fleet_mqtt :: "analytics/fleet"
end
```

**Key Takeaways:**
Sigil handles complex nested objects in the map stage. You can use `env()` to keep secrets out of your code.

### 08_multi_transport.sigil
**New Concepts:**
- Multiple brokers
- Cross-transport bridging
- Redis transport

You aren't limited to one broker or one protocol. This example bridges raw data from MQTT to Redis and also sends a processed summary to a different Redis stream.

```sigil
broker edge_mqtt
    host: "localhost"
    port: 1883
end

broker cloud_redis
    host: "redis.cloud.com"
    port: 6379
    transport: redis
end

pipeline raw_bridge
    source from edge_mqtt :: "sensors/raw"
    |> sink to cloud_redis :: "raw_stream"
end

pipeline smart_summary
    source from edge_mqtt :: "sensors/data"
    |> window sliding(10 s, step = 2 s)
    |> aggregate {
        val = avg(msg.value)
    }
    |> sink to cloud_redis :: "summary_stream"
end
```

**Key Takeaways:**
Sigil makes it easy to move data between different types of message brokers. The compiler handles the protocol differences for you.

### 09_constants_and_env.sigil
**New Concepts:**
- Global constants
- Environment variables
- Default values

Constants help you avoid magic numbers. Environment variables let you change behavior without recompiling. This example uses both to set thresholds and identify the deployment environment.

```sigil
const TEMP_THRESHOLD = 80
const ALERT_TOPIC = "alerts/high_temp"

broker local
    host: "localhost"
    port: 1883
end

pipeline monitor
    source from local :: "sensors/temp"
    |> filter msg.value > TEMP_THRESHOLD
    |> map {
        val = msg.value;
        env = env("DEPLOY_ENV", "development")
    }
    |> sink to local :: ALERT_TOPIC
end
```

**Key Takeaways:**
Constants are inlined when you generate the code. Environment variables are checked when the application starts.

### 10_case_and_having.sigil
**New Concepts:**
- Case expressions
- Having stage
- Let bindings in map

The `case` expression works like a switch statement. The `having` stage filters the results of aggregations. This error rate monitor only sends alerts if the total number of requests is high enough.

```sigil
const MIN_REQUESTS = 100

broker local
    host: "localhost"
    port: 1883
end

pipeline error_monitor
    source from local :: "api/logs"
    |> window tumbling(1 m)
    |> aggregate {
        total = count()
        errors = count(msg.status >= 500)
    }
    |> having total >= MIN_REQUESTS
    |> map {
        let rate = (errors / total) * 100;
        error_rate = rate;
        severity = case {
            when rate > 10 => "critical"
            when rate > 5  => "warning"
            else           => "info"
        }
    }
    |> sink to local :: "alerts/errors"
end
```

**Key Takeaways:**
Use `having` to filter window results based on aggregate values. Use `let` in the map stage to store temporary calculations.

### 11_dedup_and_temporal.sigil
**New Concepts:**
- Deduplication
- Temporal functions (old, changed)

Sometimes sensors send duplicate data or you only care when a value changes. This example drops duplicate event IDs and uses `changed()` to detect status flips.

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline sensor_cleaner
    source from local :: "sensors/raw"
    |> deduplicate msg.event_id within 60 s
    |> filter changed(msg.status) or msg.value > 100
    |> map {
        current = msg.value;
        previous = old(msg.value);
        status = msg.status
    }
    |> sink to local :: "sensors/clean"
end
```

**Key Takeaways:**
`deduplicate` keeps a cache of IDs to prevent processing the same message twice. `old()` and `changed()` let you compare the current message to the previous one.

### 12_quantifiers.sigil
**New Concepts:**
- exists() quantifier
- forall() quantifier

Quantifiers let you check conditions across lists or window buffers. This example checks if any tag in a list is an alarm and ensures all values in a window are positive.

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline quality_check
    source from local :: "iot/data"
    |> filter exists(t in msg.tags => t == "alarm")
    |> window tumbling(1 m)
    |> aggregate {
        readings = count()
    }
    |> having forall(m => m.value > 0)
    |> sink to local :: "iot/quality"
end
```

**Key Takeaways:**
`exists` is great for searching through arrays in a single message. `forall` is useful in the `having` stage to validate every message that went into a window.

### 13_lifecycle_hooks.sigil
**New Concepts:**
- @initially hook
- @finally hook
- Startup/Shutdown publishing

Lifecycle hooks let you run actions when the pipeline starts or stops. This is perfect for sending "online" or "offline" status messages.

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline status_reporter
    @initially {
        publish { status: "online", host: env("HOSTNAME", "unknown") } to local :: "system/status"
    }
    @finally {
        publish { status: "offline" } to local :: "system/status"
    }
    source from local :: "sensors/temp"
    |> sink to local :: "archive/temp"
end
```

**Key Takeaways:**
Hooks run exactly once. They're useful for setting up state or notifying other systems that your pipeline is active.

### 14_sink_format_batch.sigil
**New Concepts:**
- Output formatting (json, csv)
- Batching sinks

You can control how data is written to the sink. This example shows how to batch messages together to improve performance and how to export data as CSV.

```sigil
broker local
    host: "localhost"
    port: 1883
end

broker storage
    host: "localhost"
    port: 6379
    transport: redis
end

pipeline json_batcher
    source from local :: "events/+"
    |> sink to storage :: "event_stream"
       batch: { size: 100, timeout: 10 s }
end

pipeline csv_exporter
    source from local :: "telemetry/+"
    |> sink to local :: "export/telemetry"
       format: csv
end
```

**Key Takeaways:**
Batching reduces the number of network calls. CSV format is useful for logging data that needs to be opened in spreadsheets.

---

## Tier 3: Advanced (15-21). Chaining, routing, temporal, schema.

### 15_pipeline_chaining.sigil
**New Concepts:**
- Pipe sources
- In-memory DAGs

Pipelines can feed into each other without going through a broker. This creates an in-memory Directed Acyclic Graph (DAG), which is faster and keeps intermediate data private.

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline clean
    source from local :: "raw/data"
    |> filter msg.valid == true
    |> sink to local :: "internal/clean"
end

pipeline enrich
    source from pipe clean
    |> map {
        val = msg.value;
        origin = "enriched";
        value = msg.value
    }
    |> sink to local :: "final/data"
end
```

**Key Takeaways:**
Use `source from pipe` to chain logic. It's more efficient than publishing to a topic just to read it back in another pipeline.

### 16_conditional_routing.sigil
**New Concepts:**
- Route stage
- Conditional fan-out

Instead of a single sink, you can use a `route` block to send data to different places based on conditions. This example routes messages to different brokers depending on their priority.

```sigil
broker mqtt_edge
    host: "localhost"
    port: 1883
end

broker redis_cloud
    host: "redis.cloud.com"
    port: 6379
    transport: redis
end

pipeline router
    source from mqtt_edge :: "ingress/data"
    |> route:
        when msg.priority == "high" => sink to redis_cloud :: "priority_queue"
        when msg.priority == "low"  => sink to mqtt_edge :: "low_priority"
        default => sink to mqtt_edge :: "dead_letter"
    end
end
```

**Key Takeaways:**
Routing replaces the sink stage. It's the standard way to implement conditional logic at the end of a pipeline.

### 17_annotations_and_explain.sigil
**New Concepts:**
- Stage annotations (@warn, @error)
- Documentation within code

Annotations let you add metadata to stages. While they don't change the runtime behavior, they help the compiler provide better diagnostics and improve the output of `sigil explain`.

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline annotated_pipe
    source from local :: "input"
    @warn("Filtering might drop important spikes")
    |> filter msg.value < 100
    @error("Aggregation is required for the dashboard")
    |> window tumbling(1 m)
    |> aggregate {
        total = sum(msg.value)
    }
    |> sink to local :: "output"
end
```

**Key Takeaways:**
Use annotations to document why certain logic exists. They're especially helpful in large teams where many people work on the same DSL files.

### 18_rate_limiting.sigil
**New Concepts:**
- Rate stage
- Excess handling (drop, warn, error)

Rate limiting prevents downstream systems from being overwhelmed. You can choose to drop excess messages, log a warning, or raise an error.

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline rate_limited
    source from local :: "high_volume/input"
    |> rate 100 per 1 s
       on_excess: drop
    |> sink to local :: "throttled/output"
end

pipeline warning_rate
    source from local :: "api/requests"
    |> rate 10 per 1 s
       on_excess: warn
    |> sink to local :: "monitored/output"
end
```

**Key Takeaways:**
The `rate` stage is essential for protecting databases or external APIs. `on_excess: drop` is the most common setting for high-frequency sensor data.

### 19_accumulate.sigil
**New Concepts:**
- Accumulate stage
- EMA (Exponential Moving Average)
- Cumulative sum and delta

The accumulate stage maintains state across windows or messages. It's perfect for smoothing data with EMA or calculating running totals.

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline smoothed_temp
    source from local :: "sensors/temp"
    |> window tumbling(10 s)
    |> aggregate {
        avg_temp = avg(msg.value)
    }
    |> accumulate {
        smooth = ema(avg_temp, 0.3)
    }
    |> map {
        temp = smooth
    }
    |> sink to local :: "analytics/smooth"
end

pipeline energy_total
    source from local :: "meters/power"
    |> accumulate {
        total_energy = cumsum(msg.watts * (1/3600));
        change = delta(msg.watts)
    }
    |> sink to local :: "analytics/energy"
end
```

**Key Takeaways:**
Accumulations persist for the lifetime of the application. They allow for complex time-series analysis that simple windows can't handle.

### 20_fmt_and_types.sigil
**New Concepts:**
- String formatting (fmt)
- Type annotations in aggregate

You can create human-readable strings using `fmt()`. You can also add type annotations to aggregations to ensure the output matches what your downstream systems expect.

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline formatted_output
    source from local :: "sensors/+"
    |> window tumbling(1 m)
    |> aggregate {
        avg_val: float = avg(msg.value)
        total: int = count()
    }
    |> map {
        message = fmt("Sensor {} average is {:.2f} over {} samples", 
                      msg.sensor_id, avg_val, total);
        value = avg_val
    }
    |> sink to local :: "notifications"
end
```

**Key Takeaways:**
Type annotations help prevent runtime errors in generated code. The `fmt` function uses Python-style format strings.

### 21_schema_declarations.sigil
**New Concepts:**
- Schema definitions
- Strict vs Lenient validation
- Nested objects and lists

Schemas let you define the structure of your data. You can enforce these schemas on the source stage to ensure only valid data enters your pipeline.

```sigil
schema SensorReading strict
    id: str
    value: float
    timestamp: int
    tags: [str]
end

schema EnrichedReading
    location: {
        lat: float,
        lon: float
    }
    label: str?
end

broker local
    host: "localhost"
    port: 1883
end

pipeline validated_pipeline
    source from local :: "sensors/data" schema: SensorReading
    |> filter msg.value > 0
    |> sink to local :: "validated/data"
end
```

**Key Takeaways:**
Strict schemas reject any message that doesn't match exactly. Lenient schemas allow extra fields but still validate the ones you've defined.

---

## Tier 4: Expert (22-25). Joins, error handling, new stages, multi-window.

### 22_stream_join.sigil
**New Concepts:**
- Stream joins
- Join types (inner, left)
- Null coalescing (??)

Joins let you combine two different streams of data. This example joins a sensor stream with a customer profile stream based on an ID.

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline customer_enrichment
    source from local :: "sensors/readings"
    |> join local :: "customers/profiles" 
       on msg.customer_id == other.id 
       within 30 s 
       type: inner
    end
    |> map {
        reading = msg.value;
        customer_name = other.name ?? "Unknown";
        zone = other.zone ?? "Default"
    }
    |> sink to local :: "enriched/readings"
end
```

**Key Takeaways:**
The `within` clause is required for joins to prevent memory leaks. Use `??` to provide default values when a join doesn't find a match (in left joins).

### 23_on_error_dlq.sigil
**New Concepts:**
- Error handling (on_error)
- Dead Letter Queues (DLQ)

Errors happen. The `on_error` clause lets you decide what to do when a stage fails. You can drop the message or send it to a special "dead letter" topic for debugging.

```sigil
broker local
    host: "localhost"
    port: 1883
end

broker redis_errors
    host: "localhost"
    port: 6379
    transport: redis
end

pipeline robust_sensor
    source from local :: "sensors/+"
    |> filter msg.value / msg.divisor > 0
    |> map {
        val = msg.value
    }
    |> sink to local :: "output"
    on_error: sink to redis_errors :: "calc_errors"
end

pipeline silent_fail
    source from local :: "unreliable/source"
    |> filter msg.x > 0
    |> sink to local :: "output"
    on_error: drop
end
```

**Key Takeaways:**
Error envelopes include the original message, the error message, and the stage where it happened. This makes debugging production issues much easier.

### 24_new_stages.sigil
**New Concepts:**
- Flatten stage
- Distinct stage
- Throttle stage
- Sample stage

This example demonstrates several specialized stages. `flatten` turns one message into many. `distinct` drops repeated values. `throttle` slows down the flow using a buffer. `sample` only lets a percentage of messages through.

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline data_sampler
    source from local :: "high_frequency/data"
    |> sample 10%
    |> sink to local :: "sampled/data"
end

pipeline list_flattener
    source from local :: "batches"
    |> flatten msg.items
    |> sink to local :: "individual_items"
end

pipeline event_dedup
    source from local :: "events"
    |> distinct msg.event_id within 10 m
    |> sink to local :: "unique_events"
end

pipeline smooth_flow
    source from local :: "bursty/source"
    |> throttle 5 per 1 s 
       overflow: buffer 
       buffer: 200
    |> sink to local :: "smooth/output"
end
```

**Key Takeaways:**
These stages help you handle specific data patterns like bursts, duplicates, or nested lists.

### 25_retry_and_multi_window.sigil
**New Concepts:**
- Publish retries
- Named multi-windows
- Aggregating over specific windows

The final example shows how to make your pipeline resilient with retries. It also shows how to run multiple windows at the same time, allowing you to calculate short-term and long-term averages in one go.

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline resilient_analytics
    retry: 5 with backoff 500 ms
    source from local :: "sensors/temp"
    |> window short tumbling(1 m)
    |> window long tumbling(5 m)
    |> aggregate {
        avg_1m = avg(msg.value) over short
        avg_5m = avg(msg.value) over long
    }
    |> map {
        current_avg = avg_1m;
        trend_avg = avg_5m;
        diff = avg_1m - avg_5m
    }
    |> sink to local :: "analytics/trends"
end
```

**Key Takeaways:**
Named windows let you compare different time scales. The `retry` clause ensures that temporary network issues don't cause data loss when publishing.

---

## What's Next?

Now that you've seen the examples, you can:
- Read the [Language Reference](reference.md) for a deep dive into every stage and expression.
- Follow the [Getting Started Tutorial](tutorial.md) to build your first custom pipeline.
- Explore the `examples/simple.sigil` and `examples/multi_pipeline.sigil` files for more complex real-world patterns.

Happy coding with Sigil!
