# Contributing to Sigil

Welcome to the Sigil project. We're glad you're here to help build this DSL compiler for aggregation pipelines. This guide will help you set up your environment and understand how to add new features.

## Getting Started

Before you start, make sure you have Python 3.10 or newer installed. You'll also need Docker and docker-compose to run the integration tests.

1. Clone the repository from GitHub:
   `git clone https://github.com/robotics-4-all/sigil-lang`

2. Install the package in development mode:
   `pip install -e .`

3. Install the testing tools:
   `pip install pytest`

4. Start the required Docker services. These include EMQX (1883), Redis (6379), RabbitMQ (5672), and Kafka (9092):
   `docker-compose up -d`

## Project Structure

The project is organized into several key directories:

- `sigil/`: This is the core package. It contains the grammar, parser, semantic validator, and code generator.
- `sigil/grammar.tx`: This file defines the textX grammar. It serves as the metamodel for the entire language.
- `sigil/templates/`: These are the Jinja2 templates used to generate the final Python applications.
- `examples/`: You'll find over 25 educational example files here that demonstrate various DSL features.
- `tests/`: The test suite includes more than 500 unit tests and 25 integration tests.
- `docs/`: This directory holds the language reference, tutorials, and architecture guides.

## Running Tests

We use pytest for all our testing needs. You can run tests in several ways depending on what you're working on.

### Unit Tests
Run these if you don't have Docker services running. They cover grammar and semantic validation:
`pytest tests/ -k "not integration"`

### All Tests
Run the full suite including integration tests:
`pytest tests/`

### Integration Tests Only
These require the Docker services to be active:
`pytest tests/test_integration.py`

### Using the Makefile
The Makefile provides several convenient targets:
- `make test`: Runs the entire test suite.
- `make test-unit`: Runs only the unit tests.
- `make test-integration`: Runs only the integration tests.

### Using Docker
You can also run everything inside containers:
`make build && make test`

## Coding Conventions

We follow a few specific patterns to keep the codebase clean and predictable.

- Use Python 3.10+ type hints. Always use the `str | None` syntax instead of `Optional`.
- We don't enforce a specific linter, but please keep your style consistent with the existing code.
- When dispatching logic based on a pipeline stage, use string comparison on `stage.__class__.__name__`.
- Validation functions should start with `_check_` and return a `list[SemanticError]`. They should never raise exceptions.
- Codegen context dataclasses are the only way to pass data to Jinja2 templates. Don't pass raw AST objects to templates.
- Map assignments use a semicolon (`;`) as a separator. Aggregate assignments do not use semicolons.

## How to Add a New Feature

Adding features usually involves a cascading set of changes across the compiler.

### 5a: Adding a new stage
1. Update `sigil/grammar.tx` with the new stage rule.
2. Add the stage to `STAGE_ORDER` in `sigil/semantics.py` and implement its validation.
3. Update `_build_pipeline_ctx` in `sigil/codegen.py` to include the new stage data.
4. Modify `sigil/templates/pipeline.py.j2` to generate the corresponding Python code.
5. Update `sigil/explain.py` and `sigil/visualize.py` to support the new stage.
6. Add tests and a new example file to demonstrate the feature.

### 5b: Adding a new aggregation function
1. Add the function name to the `AggFunc` rule in `sigil/grammar.tx`.
2. Update the `FUNCS_*` sets in `sigil/semantics.py` to include the new function.
3. Implement the function logic in `sigil/templates/pipeline.py.j2`.
4. Add unit tests to verify the aggregation works as expected.

### 5c: Adding a new scalar function
1. Add the function to the `ScalarFunc` rule in `sigil/grammar.tx`.
2. Update `SCALAR_FUNC_ARGS` in `sigil/semantics.py` with the expected argument count.
3. Map the function to its Python equivalent in `SCALAR_FUNC_MAP` within `sigil/expressions.py`.
4. Add tests to check the expression compilation.

### 5d: Adding a new validation rule
1. Create a new `_check_*` function in `sigil/semantics.py`.
2. Call this function within the main `validate()` loop.
3. Add tests that trigger the new validation error.

## Writing Tests

Good tests are vital for a DSL compiler. Here's how we categorize them:

- **Grammar tests**: These parse DSL strings and assert that the resulting AST has the correct structure.
- **Semantics tests**: These parse and validate strings, then check that the expected `SemanticError` messages are returned.
- **Codegen tests**: These parse a model, generate code to a temporary directory, and check the file contents or try to compile them.
- **Visualize tests**: These ensure that the DOT or Mermaid diagrams are generated correctly.
- **Integration tests**: These use real brokers and require the `@mqtt_available`, `@redis_available`, `@amqp_available`, or `@kafka_available` decorators.
- **Helpers**: Use the `_model(src)` and `_errors(src)` helpers in `test_semantics.py` to simplify your test code.

## Submitting Changes

When you're ready to contribute your work, follow these steps:

1. Fork the repository and create a new branch from `main`.
2. Run the full test suite to make sure everything still passes.
3. Write new tests for any features or bug fixes you've added.
4. If you've added a user-facing feature, include a new example `.sigil` file.
5. Update `docs/language-reference.md` if your changes affect the DSL syntax.
6. Submit a pull request with a clear description of your changes.

## Example File Guidelines

Examples are the best way for users to learn Sigil. Please follow these rules when adding them:

- Use progressive numbering for filenames, such as `01_basic_filter.sigil` or `02_window_agg.sigil`.
- Every example must start with a block comment header. Include the title, concepts introduced, what the pipeline does, and a realistic scenario.
- Use scenarios from domains like IoT, fleet management, energy monitoring, or industrial sensors.
- Keep examples self-contained. They should include all necessary broker definitions so they can run without external dependencies.

Thank you for contributing to Sigil. Your help makes this project better for everyone.

License: Apache 2.0
