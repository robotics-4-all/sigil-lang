# Sigil Architecture & Internals

This document provides a deep dive into the architecture and internal mechanics of the Sigil DSL compiler. It's designed for contributors who want to understand how the compiler transforms declarative pipeline definitions into production-ready Python applications.

## Compiler Pipeline Overview

The Sigil compiler operates as a multi-stage pipeline. Each stage has a clear responsibility and passes its output to the next. This modular approach ensures that the compiler is easy to test, maintain, and extend.

```text
.sigil file
    ↓
[Parsing Stage]
grammar.tx (textX) → Parses source text into an Abstract Syntax Tree (AST).
    ↓
[Resolution Stage]
language.py (parse) → Handles file loading, multi-file imports, and circular dependency checks.
    ↓
[Validation Stage]
semantics.py (validate) → Performs over 40 semantic checks to ensure the model is valid.
    ↓
[Context Building Stage]
codegen.py (generate) → Maps the AST to intermediate "Context" dataclasses.
    ↓
[Rendering Stage]
templates/*.j2 (Jinja2) → Renders the final Python source files using the context objects.
    ↓
generated Python app (app.py, pipelines/*.py, requirements.txt)
```

### 1. Parsing
The process starts with `grammar.tx`. This file uses textX (a PEG-based parser) to define the language syntax. In Sigil, the grammar *is* the metamodel. There's no separate class hierarchy; textX generates Python classes directly from the grammar rules. This means that the structure of your AST is always in sync with your syntax. The parser is case-sensitive and uses standard PEG rules for whitespace and comments.

### 2. Resolution
The `language.py` module manages the entry points. It uses `@lru_cache` on `build_metamodel()` to avoid redundant grammar loading, which significantly speeds up the CLI for large projects. The module also handles `import "other.sigil"` statements by recursively parsing files and merging their brokers, constants, and pipelines into a single model. Furthermore, it includes logic to detect and prevent circular imports, ensuring that the final model is a directed acyclic graph of dependencies.

### 3. Validation
Before any code is generated, `semantics.py` runs a comprehensive suite of checks. It verifies that stages appear in the correct order (e.g., `source` must come before `sink`), that field references are valid in their specific context, and that types are compatible. The validator returns a list of errors and warnings rather than stopping at the first issue, allowing developers to fix multiple problems in one pass. This stage is critical for catching logical errors that the parser cannot detect.

### 4. Context Building
The `codegen.py` module acts as a bridge. It doesn't pass the raw AST to the templates. Instead, the module extracts and transforms data into specialized dataclasses like `PipelineCtx` and `AggregationCtx`. This keeps the templates clean and focused on code structure, while also providing a layer of abstraction that makes the generator more robust. The builder handles the mapping of DSL concepts to Python-specific implementation details.

### 5. Rendering
Finally, Jinja2 templates in `sigil/templates/` use the context objects to produce the output files. This stage handles the boilerplate of broker connections, message serialization, and threading for windowed operations. The templates are designed to produce readable, PEP 8 compliant Python code that is easy to debug and maintain.

## Module Map

The following table outlines the core modules and their responsibilities within the compiler.

| Module | Role | Key Exports & AST Classes |
|--------|------|---------------------------|
| `grammar.tx` | Defines the language syntax and the resulting AST structure. | `Model`, `Broker`, `Pipeline`, `ConstDef`, `SchemaDef`, `Stage` variants, `Expression` tree, `AnnotatedStage`, `RouteBlock`, `JoinClause`. |
| `language.py` | Orchestrates parsing and handles the multi-file import system. | `build_metamodel()`, `parse(path)`, `parse_string(src)`, `_resolve_imports()`, `CircularImportError`. |
| `semantics.py` | Implements semantic validation rules and enforces stage ordering. | `validate()`, `validate_or_raise()`, `STAGE_ORDER`, `FUNCS_REQUIRING_FIELD`, `SCALAR_FUNC_ARGS`. |
| `expressions.py` | Compiles the Sigil expression AST into valid Python code strings. | `compile_expr()`, `ExprContext` enum (FILTER, MAP, AGGREGATE, HAVING, QUANTIFIER). |
| `codegen.py` | Transforms the validated model into intermediate context dataclasses. | `generate()`, `BrokerCtx`, `PipelineCtx`, `AggregationCtx`, `AccumCtx`, `WindowCtx`, `JoinCtx`, `RouteCtx`. |
| `explain.py` | Generates a human-readable text description of the pipeline logic. | `explain_model()`. |
| `visualize.py` | Produces DOT and Mermaid diagram strings for pipeline visualization. | `visualize_dot()`, `visualize_mermaid()`. |
| `cli.py` | The main entry point for the `sigil` command-line tool. | Subcommands: `check`, `generate`, `explain`, `visualize`. |
| `templates/app.py.j2` | Template for the main application runner and signal management. | JSON logging setup, graceful shutdown logic, pipeline instantiation. |
| `templates/pipeline.py.j2` | Template for the core processing logic of each pipeline. | Message handlers, window buffers, aggregation loops, retry logic, DLQ routing. |
| `templates/requirements.txt.j2` | Template for the project's dependency list. | `commlib-py` with transport-specific extras. |

## Metamodel Structure

The Sigil metamodel is defined by the rules in `grammar.tx`. Here are some of the most important rules and their attributes:

- **Model**: The root of the AST. It contains lists of `brokers`, `pipelines`, `constants`, `schemas`, and `imports`.
- **Broker**: Defines a message broker connection. Attributes include `name`, `host`, `port`, `transport` (mqtt, redis, amqp, kafka), and optional `auth` and `ssl` blocks.
- **Pipeline**: The core processing unit. It has a `name`, a `source`, a list of `stages`, and either a `sink` or a `route` block. It also supports optional `retry` and `dlq` configurations.
- **Stage**: An abstract rule that encompasses all processing steps. Concrete implementations include `FilterStage`, `WindowStage`, `AggregateStage`, `MapStage`, `JoinStage`, etc.
- **Expression**: A recursive rule for boolean and arithmetic logic. It supports binary operations, unary operations, function calls, and field references.
- **AnnotatedStage**: A wrapper for stages that allows for optional `@warn` and `@error` annotations. These are used to provide feedback to the user during validation.

## Semantic Validation Details

The `semantics.py` module performs over 40 distinct checks. Some of the key validations include:

- **Stage Ordering**: Ensures that stages follow the fixed order defined in `STAGE_ORDER`. For example, a `filter` cannot come after an `aggregate`.
- **Duplicate Stages**: Verifies that each stage type (except `window`) appears at most once in a pipeline.
- **Field Reference Context**: Checks that field references are valid for the current stage. For example, `msg.x` is valid in a `filter`, but a bare identifier like `total` is only valid in `having` or `map` after an `aggregate` stage.
- **Broker Existence**: Ensures that all `source`, `sink`, and `join` stages refer to brokers that are actually defined in the model.
- **Circular Imports**: Detects if a file tries to import itself or if there's a chain of imports that leads back to the original file.
- **Type Compatibility**: Checks that function arguments match their expected types (e.g., `sum()` requires a numeric field).
- **Window Binding**: Verifies that aggregations using the `over` keyword refer to a window that is defined in the same pipeline.

## Key Design Decisions

Several architectural choices define how Sigil works and how it's maintained.

### 1. Grammar as Metamodel
Sigil avoids the overhead of maintaining a separate class hierarchy for the AST. By letting textX generate classes directly from `grammar.tx`, the language remains highly flexible. If you change a rule in the grammar, the corresponding Python objects update automatically. This reduces boilerplate and ensures consistency between the language definition and its implementation.

### 2. String-Based Stage Dispatch
The compiler often needs to perform different actions based on the type of stage. Instead of using `isinstance(stage, FilterStage)`, it uses `stage.__class__.__name__ == 'FilterStage'`. This decouples the logic from the specific class instances and makes it easier to handle stages wrapped in `AnnotatedStage` containers, which provide metadata like warnings and errors.

### 3. Recursive Expression Compilation
The `expressions.py` module uses a recursive approach to walk the expression tree. Each node type (like `BinaryOp` or `FunctionCall`) is handled by a specific branch of logic that emits the equivalent Python code. This keeps the compiler logic close to the structure of the language and makes it easy to add new operators or functions without modifying a complex visitor pattern.

### 4. Non-Raising Validation
Validation in `semantics.py` is designed to be exhaustive. Each `_check_*` function returns a list of `SemanticError` objects. The main `validate()` function aggregates these lists. This approach allows users to see all syntax and logic errors in one go, which is much more user-friendly than failing on the first error and requiring multiple compile cycles.

### 5. Intermediate Codegen Contexts
The use of dataclasses in `codegen.py` provides a clean API for the Jinja2 templates. These contexts pre-calculate complex values, such as transport-specific broker configurations and window flush intervals, so the templates can stay simple. It also makes it easier to unit test the code generation logic without rendering full files.

### 6. PEG Parser and Right-Recursion
Because textX uses a PEG parser, binary expressions are naturally right-recursive. To ensure that operators like subtraction or division are left-associative in the generated Python code, the compiler uses a `_flatten_binary()` helper. This function transforms the right-leaning tree into a flat list that can be correctly reconstructed into a left-associative Python expression.

### 7. Attribute Defaults
The metamodel is built with `auto_init_attributes=True`. This is a textX feature that ensures all attributes defined in the grammar have default values (like `None` or `[]`) if they aren't present in the source file. This prevents `AttributeError` exceptions during validation and code generation, making the code more robust.

### 8. Transport Agnosticism
Sigil uses `commlib-py` to handle all message broker interactions. This allows the DSL to support MQTT, Redis, AMQP, and Kafka using the same syntax. The compiler handles the mapping of DSL broker definitions to the specific connection parameters required by each transport, including authentication and SSL settings.

## How To Extend Sigil

Adding new features to Sigil follows a predictable pattern. Here's how to handle common extension tasks.

### 4a: Adding a New Stage
Adding a new processing stage is the most involved task. You must update six different files to ensure the stage is fully integrated.
1. **grammar.tx**: Define the syntax for the new stage (e.g., `|> my_stage: expression`). Add this rule to the `Stage` abstract rule.
2. **semantics.py**: Add the new stage name to the `STAGE_ORDER` dictionary to define where it sits in the pipeline. Implement a `_check_my_stage` function to validate its parameters and check for conflicts with other stages.
3. **codegen.py**: Update the `_build_pipeline_ctx` function. Extract the data from your new stage and add it to the `PipelineCtx` dataclass.
4. **templates/pipeline.py.j2**: Add a block in the message processing loop (usually in `_process_immediate`) to execute the logic for your stage. Ensure it handles errors gracefully and respects the pipeline's state.
5. **explain.py**: Add a case to `explain_model` so the `sigil explain` command can describe what your stage does in plain English.
6. **visualize.py**: Define how the stage should appear in DOT and Mermaid diagrams, including its label, color, and shape.

### 4b: Adding a New Aggregation Function
Aggregation functions (like `sum`, `avg`, or `count`) operate within the `aggregate` stage.
1. **grammar.tx**: Add the function name to the `AggFunc` rule.
2. **semantics.py**: If the function requires a field (like `sum(msg.x)`), add it to the `FUNCS_REQUIRING_FIELD` set. If it takes specific arguments, update the validation logic.
3. **templates/pipeline.py.j2**: Update the aggregation logic in the template. You'll need to add the math for your new function inside the window processing loop, ensuring it handles empty buffers and null values correctly.

### 4c: Adding a New Scalar Function
Scalar functions (like `abs`, `round`, or `len`) can be used anywhere an expression is allowed.
1. **grammar.tx**: Add the function name to the `ScalarFunc` rule.
2. **semantics.py**: Add the function to the `SCALAR_FUNC_ARGS` dictionary, specifying how many arguments it expects.
3. **expressions.py**: Map the Sigil function name to its Python equivalent in the `SCALAR_FUNC_MAP` dictionary.

### 4d: Adding a New Validation Rule
Validation rules ensure the pipeline makes sense before it's compiled.
1. **semantics.py**: Write a new private function (e.g., `_check_unreachable_stages`) that takes the `model` as an argument.
2. **semantics.py**: Have the function return a list of `SemanticError` objects if it finds issues. Use descriptive error messages and include the line number from the AST.
3. **semantics.py**: Call your new function inside the main `validate()` function and append its results to the error list.

## Expression Compilation Deep Dive

The expression system is the heart of Sigil's logic. It transforms DSL expressions into Python code that runs efficiently in the generated app.

### Precedence Chain
The compiler follows a strict precedence chain to ensure expressions are evaluated correctly.
- **OR**: `a or b`
- **AND**: `a and b`
- **Comparison**: `==`, `!=`, `>`, `<`, `>=`, `<=`, `is`, `is not`, `in`, `not in`
- **Addition/Subtraction**: `+`, `-`
- **Multiplication/Division/Modulo**: `*`, `/`, `%`
- **Null Coalescing**: `??` (compiles to `a if a is not None else b`)
- **Unary**: `not`, `-`
- **Atoms**: Literals, field references, function calls, grouped expressions

### Context-Dependent Field References
The `ExprContext` enum controls how field references are compiled.
- **FILTER/AGGREGATE**: References like `msg.value` are compiled to `_get_nested(msg, 'value')`. This helper handles missing keys and nested dictionaries safely, returning `None` instead of raising a `KeyError`.
- **HAVING/MAP**: In these stages, you can refer to aggregated results by their name. A bare identifier like `total` becomes `aggregated['total']`. If you use `msg.value`, it still uses `_get_nested`.
- **Joins**: When a join is active, you can use `other.field`. This always compiles to `_get_nested(other, 'field')`, where `other` is the message from the joined stream.

### Special Functions
Sigil includes several built-in functions that have special compilation rules.
- `env("VAR", "default")`: Compiles to `os.environ.get("VAR", "default")`.
- `fmt("Value is {}", msg.x)`: Compiles to `"Value is {}".format(_get_nested(msg, 'x'))`.
- `old(msg.x)`: Accesses the value of `msg.x` from the *previous* message processed by the pipeline. This is useful for detecting changes or calculating deltas.
- `changed(msg.x)`: A shortcut for `msg.x != old(msg.x)`.
- `forall(m => m.val > 0)`: Used in windowed aggregations to check if a condition holds for all messages in the buffer. It compiles to a Python generator expression inside `all()`.
- `exists(x in msg.list => x > 10)`: Checks if any element in a list matches a predicate. It compiles to a generator expression inside `any()`.

## Generated Code Structure

When you run `sigil generate`, the compiler produces a structured Python project.

### app.py
The `app.py` file is the orchestrator. It performs the following tasks:
- Configures JSON-based logging using the `LOG_LEVEL` and `LOG_FORMAT` environment variables.
- Creates instances of every pipeline defined in the Sigil model.
- Sets up signal handlers for `SIGTERM` and `SIGINT` to ensure the app shuts down gracefully, draining all buffers and closing broker connections before exiting.
- Starts all pipelines, which in turn connect to their respective brokers and start listening for messages.

### pipelines/NAME.py
Each pipeline is generated as a separate Python class in the `pipelines/` directory.
- **`_on_message`**: The callback function triggered by the broker. It handles initial JSON decoding, schema validation, and passes the message to the processing chain.
- **`_process_immediate`**: Executes stages that don't require a window, such as `filter`, `rate`, `deduplicate`, and `map`.
- **`_process_window`**: Manages the buffering of messages for windowed stages like `aggregate`. It supports multiple named windows per pipeline, each with its own configuration.
- **`_flush_window`**: Runs in a separate thread for each window. It calculates aggregations when a window closes (e.g., every 60 seconds) and publishes the results.
- **`drain`**: A method called during shutdown to ensure any messages remaining in buffers or batches are processed and sent.

### requirements.txt
The generated `requirements.txt` includes `commlib-py` and any necessary transport libraries. This ensures the generated app has everything it needs to run in a production environment, whether it's using MQTT, Redis, or Kafka.

## Testing Strategy

Sigil maintains a high level of code quality through an extensive test suite.

### Unit Tests
The project has 518 unit tests that cover every module in the compiler.
- **Grammar Tests**: Ensure that valid Sigil code parses into the expected AST and that invalid code produces clear syntax errors.
- **Semantic Tests**: Verify that the validator correctly identifies logic errors, such as out-of-order stages or invalid field references.
- **Codegen Tests**: Check that the generated Python code is syntactically correct and follows the expected structure.
- **Visualization Tests**: Ensure that DOT and Mermaid outputs are valid and accurately represent the pipeline.

### Integration Tests
There are 26 integration tests that run the generated applications against real message brokers. These tests use Docker to spin up MQTT, Redis, RabbitMQ, and Kafka services. The suite verifies that the entire system—from DSL source to running application—works as intended in a real-world environment, including edge cases like broker disconnections and high message volumes.

### Lifecycle Hooks
Pipelines can define `@initially` and `@finally` hooks. These are executed when the pipeline starts and stops, respectively. The hooks are commonly used to publish "online" or "offline" status messages to a broker, or to perform initial setup tasks like clearing a cache.


### Schema Validation
Sigil supports schema declarations that can be used to validate incoming messages. You can define schemas with specific fields and types, and then apply them to a pipeline's source. This ensures that only well-formed data enters the processing pipeline, reducing the risk of runtime errors.

### Error Handling and DLQ
When a message fails validation or causes an error during processing, Sigil can route it to a Dead Letter Queue (DLQ). This allows developers to inspect failed messages without stopping the entire pipeline. DLQs are configured per-pipeline and can point to any supported broker.

### Pipeline Chaining
Pipelines can be connected together using the `source from pipe` syntax. This allows you to build complex processing graphs by breaking them down into smaller, reusable pipelines. Messages are passed between pipelines in-memory, which is much faster than going through an external broker.

## Broker and Transport Mapping

Sigil supports multiple message transports through `commlib-py`. The following table shows how DSL broker definitions map to transport-specific options:

| Transport | Default Port | Key Options |
|-----------|--------------|-------------|
| `mqtt` | 1883 | `client_id`, `clean_session`, `keepalive` |
| `redis` | 6379 | `db`, `password`, `ssl` |
| `amqp` | 5672 | `vhost`, `username`, `password`, `heartbeat` |
| `kafka` | 9092 | `group_id`, `auto_offset_reset`, `bootstrap_servers` |

The compiler automatically handles the generation of the correct connection parameters based on the `transport` field in the broker definition.

## Windowing and Aggregation Logic

The windowing logic in Sigil is designed to be both flexible and efficient.

- **Tumbling Windows**: Fixed-size, non-overlapping windows. A new window starts as soon as the previous one ends.
- **Sliding Windows**: Overlapping windows defined by a size and a step interval.
- **Flush Mechanism**: Each window has a dedicated background thread that monitors the time or message count. When a window's criteria are met, the thread triggers the `_flush_window` method.
- **Thread Safety**: All window buffers use thread-safe collections to ensure that messages can be added while a flush is in progress.
- **Aggregation Loop**: During a flush, the pipeline iterates over the messages in the buffer and applies the defined aggregation functions. The results are then passed to the `having` and `map` stages.

## Performance Considerations

While Sigil is designed for ease of use, it also includes several features to ensure good performance:

- **In-Memory Chaining**: Using `source from pipe` avoids the overhead of external brokers for internal pipeline communication.
- **Batching**: Sinks can be configured to batch messages together, reducing the number of network calls.
- **Efficient Expressions**: Sigil expressions are compiled directly to Python, which is much faster than interpreting them at runtime.
- **Asynchronous Windows**: Window flushing happens in background threads, so it doesn't block the main message processing loop.

## Future Work

The Sigil project is constantly evolving. Some planned features include:

- **Stateful Processing**: Support for maintaining state across pipeline restarts.
- **More Aggregation Functions**: Adding support for median, mode, and standard deviation.
- **Enhanced Visualization**: Adding support for interactive diagrams and real-time monitoring.
- **LSP Support**: Providing a Language Server Protocol implementation for better IDE integration.

## Glossary

- **AST (Abstract Syntax Tree)**: The tree-like representation of the Sigil source code generated by the parser.
- **Broker**: A message middleware (like MQTT or Kafka) that handles message distribution.
- **Metamodel**: The set of rules that define the structure of the AST. In Sigil, this is defined by the textX grammar.
- **Pipeline**: A sequence of processing stages that transform a stream of messages.
- **Stage**: A single processing step within a pipeline, such as a filter or an aggregation.
- **Window**: A buffer that collects messages over a period of time or a specific count for aggregation.
- **DLQ (Dead Letter Queue)**: A destination for messages that fail processing or validation.
- **Sink**: The final destination for messages processed by a pipeline.
- **Source**: The entry point for messages into a pipeline.



### Schema Validation
Sigil supports schema declarations that can be used to validate incoming messages. You can define schemas with specific fields and types, and then apply them to a pipeline's source. This ensures that only well-formed data enters the processing pipeline, reducing the risk of runtime errors.

### Error Handling and DLQ
When a message fails validation or causes an error during processing, Sigil can route it to a Dead Letter Queue (DLQ). This allows developers to inspect failed messages without stopping the entire pipeline. DLQs are configured per-pipeline and can point to any supported broker.

### Pipeline Chaining
Pipelines can be connected together using the `source from pipe` syntax. This allows you to build complex processing graphs by breaking them down into smaller, reusable pipelines. Messages are passed between pipelines in-memory, which is much faster than going through an external broker.

## Glossary

- **AST (Abstract Syntax Tree)**: The tree-like representation of the Sigil source code generated by the parser.
- **Broker**: A message middleware (like MQTT or Kafka) that handles message distribution.
- **Metamodel**: The set of rules that define the structure of the AST. In Sigil, this is defined by the textX grammar.
- **Pipeline**: A sequence of processing stages that transform a stream of messages.
- **Stage**: A single processing step within a pipeline, such as a filter or an aggregation.
- **Window**: A buffer that collects messages over a period of time or a specific count for aggregation.
- **DLQ (Dead Letter Queue)**: A destination for messages that fail processing or validation.
- **Sink**: The final destination for messages processed by a pipeline.
- **Source**: The entry point for messages into a pipeline.


