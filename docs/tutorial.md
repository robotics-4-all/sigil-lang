# Sigil Tutorial: From Hello World to Production Pipelines

Welcome to the Sigil tutorial. This guide takes you from a basic message relay to complex, production-ready aggregation pipelines. Sigil is a domain-specific language (DSL) designed for building stream processing applications that talk to message brokers like MQTT, Redis, or Kafka.

## Prerequisites

Before starting, ensure you have:
- Python 3.10 or newer.
- The Sigil package installed in your environment: `pip install -e .`
- A message broker (like Mosquitto for MQTT) running locally.

## Lesson 1: Hello World

The simplest Sigil pipeline is a relay. It subscribes to one topic and publishes every message it receives to another topic.

### Concepts Introduced
- **Broker**: Defines connection details for a message transport.
- **Pipeline**: A named processing flow.
- **Source**: Where data enters the pipeline.
- **Sink**: Where data leaves the pipeline.

```sigil
broker local
    host: "localhost"
    port: 1883
    transport: mqtt
end

pipeline hello_world
    source from local :: "sensors/raw"
    |> sink to local :: "sensors/relay"
end
```

### What happens?
The `local` broker block tells Sigil how to connect to your MQTT server. Your `hello_world` pipeline starts by listening to the `sensors/raw` topic. Every message that arrives is immediately sent to `sensors/relay` without any changes.

### Try it
Check your syntax:
```bash
sigil check tutorial.sigil
```

Next, generate the Python application:
```bash
sigil generate tutorial.sigil -o ./app
```

Finally, explain the pipeline logic:
```bash
sigil explain tutorial.sigil
```

### Visualizing the Pipeline
Sigil can generate diagrams of your pipelines. This helps you verify the data flow before running any code.

Generate a Mermaid diagram:
```bash
sigil visualize tutorial.sigil --format mermaid
```
You can paste the output into the [Mermaid Live Editor](https://mermaid.live/) to see the graph.

## Lesson 2: Filtering

Most pipelines don't need every single message. You can use the filter stage to drop data that doesn't meet your criteria.

### Concepts Introduced
- **Filter stage**: Uses boolean expressions to decide which messages continue.
- **msg.field**: References fields in the incoming JSON message.
- **Comparison operators**: `>`, `<`, `==`, `!=`, `>=`, `<=`.

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline high_temp_filter
    source from local :: "sensors/temp"
    |> filter msg.value > 25.0 and msg.status == "active"
    |> sink to local :: "alerts/high_temp"
end
```

### What happens?
The pipeline only passes messages where the `value` field is greater than 25.0 and the `status` field is exactly "active". If a message fails this check, it's dropped immediately.

### Try it
After generating and running the app, send a test message using `mosquitto_pub`:
```bash
mosquitto_pub -t "sensors/temp" -m '{"value": 28.5, "status": "active"}'
```
You should see the message appear on `alerts/high_temp`. If you send a value of 20.0, nothing will happen.

## Lesson 3: Windows & Aggregation

Aggregation allows you to summarize data over time. Sigil uses windows to group messages into time-based buckets.

### Concepts Introduced
- **Tumbling windows**: Fixed-size, non-overlapping time intervals.
- **Aggregate stage**: Performs calculations on all messages in a window.
- **Functions**: `avg`, `sum`, `min`, `max`, `count`.

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline hourly_stats
    source from local :: "power/usage"
    |> window tumbling(1 h)
    |> aggregate {
        average_load = avg(msg.load)
        peak_load = max(msg.load)
        total_readings = count()
    }
    |> sink to local :: "stats/hourly"
end
```

### What happens?
The pipeline collects messages for one hour. When the hour ends, it calculates the average load, the maximum load, and the total number of messages received. It then sends a single JSON object containing these results to the sink.

### Try it
For testing, you can use shorter windows like `tumbling(10 s)`. Send several messages within 10 seconds:
```bash
mosquitto_pub -t "power/usage" -m '{"load": 100}'
mosquitto_pub -t "power/usage" -m '{"load": 200}'
```
After 10 seconds, the app will publish `{"average_load": 150.0, "peak_load": 200, "total_readings": 2}`.

## Lesson 4: Group By + Map

Sometimes you want to calculate statistics per device or category. You can also reshape the output message using the map stage.

### Concepts Introduced
- **Group By**: Partitions the stream so aggregations run independently for each key.
- **Map stage**: Creates a new output message from aggregated values.
- **Let bindings**: Temporary variables inside a map block.
- **Semicolons**: Used to separate assignments in map blocks.

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline device_metrics
    source from local :: "devices/data"
    |> group_by msg.device_id
    |> window tumbling(5 m)
    |> aggregate {
        val_sum = sum(msg.value)
        val_count = count()
    }
    |> map {
        id = msg.device_id;
        average = val_sum / val_count;
        source = "device_metrics"
    }
    |> sink to local :: "metrics/summary"
end
```

### What happens?
Messages are grouped by `device_id`. Every 5 minutes, the pipeline calculates the sum and count for each device. The map stage then builds a new object with the device ID, the calculated average, and a source label. Note that aggregate assignments don't use semicolons, but map assignments do.

### Try it
Send data for two different devices:
```bash
mosquitto_pub -t "devices/data" -m '{"device_id": "A1", "value": 10}'
mosquitto_pub -t "devices/data" -m '{"device_id": "B2", "value": 20}'
```
The output will contain two separate messages, one for each device ID.

## Lesson 5: Sliding Windows & Advanced Aggs

Sliding windows move forward in smaller steps than their total duration, allowing for overlapping intervals.

### Concepts Introduced
- **Sliding windows**: Defined by a duration and a step.
- **Advanced functions**: `stddev`, `first`, `last`.

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline rolling_average
    source from local :: "stock/prices"
    |> window sliding(10 m, step: 1 m)
    |> aggregate {
        moving_avg = avg(msg.price)
        volatility = stddev(msg.price)
        opening = first(msg.price)
        closing = last(msg.price)
    }
    |> sink to local :: "stock/rolling"
end
```

### What happens?
Every minute, the pipeline looks back at the last 10 minutes of data. It calculates the moving average, standard deviation, and the first and last prices seen in that window.

### Try it
Sliding windows are great for detecting trends. If you use `sliding(1 m, step: 10 s)`, you'll get an update every 10 seconds based on the last minute of data.

## Lesson 6: Constants, Env Vars, Expressions

Sigil supports powerful expressions to make your pipelines dynamic and readable.

### Concepts Introduced
- **Constants**: Reusable values defined at the top level.
- **env()**: Reads values from environment variables.
- **Case expressions**: Conditional logic (if-then-else).
- **fmt()**: String formatting.

```sigil
const THRESHOLD = 100.0
const APP_NAME = env("SIGIL_APP_NAME", "default_app")

broker local
    host: "localhost"
    port: 1883
end

pipeline smart_alerts
    source from local :: "system/load"
    |> map {
        status = case {
            when msg.value > THRESHOLD => "CRITICAL"
            when msg.value > THRESHOLD * 0.8 => "WARNING"
            else => "OK"
        };
        message = fmt("App {} reported {} status", APP_NAME, status)
    }
    |> sink to local :: "system/alerts"
end
```

### What happens?
The pipeline uses a constant for the threshold and an environment variable for the app name. It evaluates the message value through a case block to determine a status string. Finally, it formats a human-readable message for the alert.

### Try it
Set the environment variable before running your app:
```bash
export SIGIL_APP_NAME="ProductionMonitor"
python app/main.py
```

## Lesson 7: Pipeline Chaining & Routing

You can connect pipelines together or send data to different destinations based on conditions.

### Concepts Introduced
- **Pipe sources**: Subscribing to the output of another pipeline.
- **Route stage**: Conditional fan-out to multiple sinks.

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline raw_processor
    source from local :: "raw/data"
    |> filter msg.valid == true
    |> sink to local :: "processed/data"
end

pipeline router
    source from pipe raw_processor
    |> route
        when msg.type == "metric" => sink to local :: "metrics/inbox"
        when msg.type == "event" => sink to local :: "events/inbox"
        default => sink to local :: "trash"
    end
end
```

### What happens?
The `raw_processor` pipeline filters incoming data. Your `router` pipeline takes the output of `raw_processor` directly in memory. It then routes messages to different topics based on the `type` field.

### Try it
Chaining pipelines is more efficient than using intermediate broker topics because data stays within the same process.

## Lesson 8: Joins & Enrichment

Joins allow you to combine two different data streams based on a common key.

### Concepts Introduced
- **Join stage**: Merges a primary stream with a secondary stream.
- **other.field**: References fields from the joined stream.
- **Null coalesce (??)**: Provides a default value if a field is missing.

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline enriched_telemetry
    source from local :: "telemetry/raw"
    |> join local :: "metadata/updates" 
       on msg.sensor_id == other.id 
       within 5 s 
       type: left
    end
    |> map {
        sensor = msg.sensor_id;
        reading = msg.value;
        location = other.loc ?? "unknown"
    }
    |> sink to local :: "telemetry/enriched"
end
```

### What happens?
When a telemetry message arrives, Sigil looks for a metadata message with a matching ID that arrived within 5 seconds. Because it's a `left` join, the telemetry message passes through even if no metadata is found. The map stage uses the `??` operator to handle cases where the location is missing.

### Try it
Send the metadata first, then the telemetry:
```bash
mosquitto_pub -t "metadata/updates" -m '{"id": "S1", "loc": "Warehouse"}'
mosquitto_pub -t "telemetry/raw" -m '{"sensor_id": "S1", "value": 22}'
```

## Lesson 9: Production Features

Real-world pipelines need error handling, retries, and lifecycle management.

### Concepts Introduced
- **Schemas**: Validate incoming message structure.
- **on_error**: Defines what to do when processing fails.
- **Retry policy**: Configures automatic retries for publishing.
- **Lifecycle hooks**: Code that runs when the pipeline starts or stops.
- **Stage annotations**: Mark stages with warnings or errors for documentation.

```sigil
schema sensor_schema strict
    id: str
    value: float
    timestamp: int
end

broker local
    host: "localhost"
    port: 1883
end

pipeline production_pipeline
    @initially { publish { event: "pipeline_started", version: "1.0" } }
    
    source from local :: "prod/input"
    
    @warn("Filtering out low values might hide sensor drift")
    |> filter msg.value > 0.1
    
    |> sink to local :: "prod/output"
    on_error: sink to local :: "prod/dlq"
    retry: 5 with backoff 1 s
    
    @finally { publish { event: "pipeline_stopped", clean: true } }
end
```

### What happens?
The pipeline validates incoming messages against a strict schema. If any stage fails, the message goes to a Dead Letter Queue (DLQ) topic. It also attempts to republish failed messages up to 5 times. The `@initially` and `@finally` hooks send lifecycle events to the broker.

### Try it
Try sending a message that violates the schema (e.g., missing a field). You'll see it appear on the `prod/dlq` topic instead of `prod/output`.

## Lesson 10: Advanced

Sigil includes specialized stages for complex stream processing patterns.

### Concepts Introduced
- **Named multi-window**: Multiple windows in one pipeline using `over`.
- **Accumulate**: State-based calculations like EMA or cumulative sums.
- **Temporal operators**: `old()` and `changed()`.
- **Quantifiers**: `forall()` and `exists()`.
- **Rate limiting & Throttling**: Controlling data flow.

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline advanced_analytics
    source from local :: "factory/sensors"
    
    |> deduplicate msg.msg_id within 1 m
    
    |> window fast tumbling(10 s)
    |> window slow tumbling(1 m)
    
    |> aggregate {
        short_avg = avg(msg.temp) over fast
        long_avg = avg(msg.temp) over slow
    }
    
    |> accumulate {
        trend = ema(short_avg, 0.1)
        total_energy = cumsum(msg.power)
    }
    
    |> filter changed(msg.status) or exists(x in msg.errors => x.code > 500)
    
    |> throttle 1 per 1 s
    
    |> map {
        is_stable = forall(m => abs(m.temp - trend) < 5.0);
        energy = total_energy;
        prev_status = old(msg.status)
    }
    
    |> sink to local :: "factory/alerts"
       batch: { size: 10, timeout: 5 s }
end
```

### What happens?
This pipeline demonstrates several advanced features:
1. **Deduplicate**: Removes duplicate messages based on an ID.
2. **Named Windows**: Calculates averages over two different time scales simultaneously.
3. **Accumulate**: Tracks an Exponential Moving Average (EMA) and a running total.
4. **Temporal/Quantifiers**: Filters for status changes or specific error codes in arrays.
5. **Throttling**: Ensures alerts aren't sent more than once per second.
6. **Batching**: Buffers output messages to send them in groups of 10.

### Try it
The `forall` quantifier is particularly useful for checking if all messages in a window satisfy a condition. Your `exists` quantifier checks if at least one message (or one element in an array) satisfies it.

## Understanding the Generated Code

When you run `sigil generate`, the compiler creates a Python project. Here's what's inside:
- `main.py`: The entry point that initializes brokers and starts pipelines.
- `pipelines/`: Contains one Python file per pipeline defined in your `.sigil` file.
- `utils.py`: Helper functions for expressions and message handling.

The generated code uses `commlib-py` for broker communication. It handles JSON serialization, logging, and graceful shutdown automatically. You can customize the behavior using environment variables like `LOG_LEVEL=DEBUG`.

## Troubleshooting

If your pipeline isn't working as expected:
1. **Check the logs**: The generated app logs errors if it can't connect to the broker or if message parsing fails.
2. **Use `sigil explain`**: This command prints a human-readable summary of your pipeline. It's great for verifying that your logic matches your intent.
3. **Verify broker connectivity**: Ensure your MQTT/Redis/Kafka broker is reachable from the machine running the app.
4. **Schema mismatches**: If you use a strict schema, any message missing a field or having the wrong type will be dropped or sent to the DLQ.

## What's Next?

You've now seen the full range of Sigil's capabilities. To dive deeper into specific syntax details, check the **Language Reference** or explore the `examples/` directory in the repository. Happy piping!
